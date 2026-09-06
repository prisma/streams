//! Ops-bucket control plane: stream registry (CAS'd JSON descriptors, D18/D21)
//! and the dynamic shard topology (D3, §3.2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::crypto::hex;

/// A conditional update may never silently become an unconditional PUT.
/// Keep the storage token opaque so every existing-object writer shares this
/// fail-closed boundary (creation uses PutMode::Create independently).
#[derive(Debug)]
struct ConditionalUpdateToken(UpdateVersion);

#[derive(Debug)]
struct MissingConditionalToken;
impl std::fmt::Display for MissingConditionalToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("existing descriptor has no conditional-update token")
    }
}
impl std::error::Error for MissingConditionalToken {}

impl ConditionalUpdateToken {
    fn from_etag(etag: Option<String>) -> Result<Self, object_store::Error> {
        match etag.filter(|value| !value.is_empty()) {
            Some(etag) => Ok(Self(UpdateVersion {
                e_tag: Some(etag),
                version: None,
            })),
            None => Err(object_store::Error::Generic {
                store: "registry",
                source: Box::new(MissingConditionalToken),
            }),
        }
    }
    fn mode(self) -> PutMode {
        PutMode::Update(self.0)
    }
}

#[cfg(test)]
fn retryable_cas_error(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<object_store::Error>(),
        Some(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDescriptor {
    pub name: String,
    /// Billing tenant boundary (docs/OBSERVABILITY-BILLING.md §3.2):
    /// captured from the deployment's authenticated context at creation
    /// and immutable for the incarnation. `None` only on descriptors
    /// created before the telemetry cutover (pre-launch data; billed
    /// under the deployment default at meter time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// MANDATORY stable tenant identity (MULTITENANCY §10.2): an input
    /// to the registry path and every layout-4 identity hash, verified
    /// against the path at decode. The mutable workspace_id is
    /// deliberately NOT persisted in the immutable descriptor.
    pub project_id: crate::tenant::ProjectId,
    /// 16-byte hex; minted per creation, bound into HKDF (V9 mandate).
    pub stream_epoch: String,
    /// One-way key fingerprint; wrong-key requests are rejected with 403.
    pub key_fingerprint: String,
    pub created_ms: i64,
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
    #[serde(default)]
    pub deleted: bool,
    /// Fork lifecycle (pinned DS protocol): the stream's data is
    /// retained for its live forks, but direct access answers 410 and
    /// re-creation is blocked while references exist.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub soft_deleted: bool,
    /// Round-22 item 7: the logical close instant, stamped in the SAME
    /// registry write that tombstones (`deleted = true`). Billing
    /// closure is a saga and this is its durable debt record — however
    /// late the committer op finally lands (crash, full queue, closed
    /// shard, ownership move), the storage clock stops HERE, not at
    /// "whenever the closure happened to run".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_close_ms: Option<i64>,
    /// Fork parentage (pinned DS protocol): records below `fork_offset`
    /// are served from the ancestor chain; this stream's own records
    /// begin at `fork_offset` (a binary sub-offset materializes the
    /// partial record there at creation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ForkRef>,
    /// Live direct forks of THIS stream, by unique fork id (audit P0:
    /// an anonymous counter cannot be released idempotently — a retried
    /// delete double-decremented and a lost update leaked a reference
    /// forever). Install and release are set operations, so both are
    /// naturally idempotent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fork_children: Vec<String>,
    /// Durable creation state (audit P0). A descriptor is published
    /// BEFORE its initial content, fork tail, and source reference
    /// exist; without this, a replayed PUT could observe the
    /// descriptor, skip initialization, and answer success for a
    /// stream whose initial body never landed (the 2026-07-31 field
    /// anomaly). `Initializing` names the operation so a retry with the
    /// same request joins/resumes it instead of returning early, and a
    /// different request conflicts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init: Option<InitState>,
    /// Configured content type (create-time config; appends must match).
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Raw TTL seconds as configured (config-compare + HEAD reporting).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// ROUTING-V3: the descriptor-resident segment map. `None` is the
    /// implicit single-segment map — segment 0 covers the whole
    /// keyspace and its engine identity IS `storage_hash()`, so a fresh
    /// stream (and every existing total-order stream) carries zero map
    /// bytes and pays zero extra requests. Materialized by the first
    /// split (CAS on the registry object). See docs/ROUTING-V3.md §2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segments: Option<crate::segmap::SegmentMap>,
    /// Product-surface vNext (spec Stage 1/3): the collection's durable
    /// seal state. Monotonic; set only through the seal lifecycle.
    /// `sealed` is the terminal bit; `sealing` names an in-flight seal
    /// (audit P0) so a crash between the final append, the segment
    /// closes and publication is resumable and never leaves a
    /// descriptor claiming sealed over writable segments.
    #[serde(default)]
    pub sealed: bool,
    /// Monotonic allocator for seal-claim generations (and topology
    /// transition closes). Never reset, never reused: a fence set from
    /// a reservation that later aborted must stay below every future
    /// allocation, or the fence would block a legitimate later claim.
    #[serde(default)]
    pub seal_gen_counter: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealing: Option<SealState>,
    /// The operation id of the seal that COMPLETED (audit P0): a
    /// repeated seal-with-final carrying the same final record is
    /// idempotent success, while a different final record against a
    /// sealed collection is a conflict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal_op: Option<String>,
    /// Immutable watch definitions (spec Stage 2/7). Empty for streams
    /// created without watches; JSON streams only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch_definitions: Vec<WatchDefinition>,
    /// Cleanup this tombstone still owes its parent. Deletion marks the
    /// child dead and then releases the parent's reference; a crash in
    /// between used to leave the parent holding a reference to a fork
    /// that no longer exists, with no way to notice — a retry bounced
    /// off the "already dead" check before reaching the release. The
    /// flag survives on the tombstone, so any later delete finishes it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub parent_ref_pending: bool,
    /// Verifier for signed watch-observation URLs, base64 of
    /// `crypto::wait_sig_key`. Written once at create, from the key the
    /// creator presented; the server never holds the stream key itself,
    /// so without this it could not check a signature at all. It is a
    /// verifier for an OBSERVATION capability and nothing more:
    /// possession forges "this key changed" notifications, never
    /// decryption, append, consumer or management rights. Persisting it
    /// is what makes a signed URL outlive the process that issued it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_sig_key: Option<String>,
    /// Storage-layout generation (spec: pre-launch clean switch). New
    /// descriptors write LAYOUT_VERSION; a reader that finds any OTHER
    /// value — including 0, the serde default every pre-cutover
    /// descriptor deserializes to — refuses the namespace with
    /// `unsupported_storage_layout` instead of decoding, translating,
    /// or rewriting it.
    #[serde(default)]
    pub layout_version: u32,
}

/// The storage-layout generation this binary writes and the ONLY one it
/// reads. There are no layout bridges: opening a namespace written by a
/// different layout is refused (pre-launch hard cutover).
pub const LAYOUT_VERSION: u32 = 4;

/// Seal-in-progress marker (audit P0). Present = Sealing: normal
/// appends are refused, only the matching seal operation may write its
/// final record, and any request observing it resumes the transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SealState {
    /// Identifies the sealing request (its final-record identity), so a
    /// retry resumes rather than appending a second final record.
    pub operation_id: String,
    /// WHAT this seal promised. Without it a plain `:seal` arriving
    /// after a crashed seal-with-final would close every segment and
    /// publish Sealed — permanently dropping the final record the first
    /// operation was committed to writing.
    #[serde(default)]
    pub intent: SealIntent,
    pub claimed_ms: i64,
    /// The claim's EXECUTION TOKEN, allocated from the descriptor's
    /// monotonic [`StreamDesc::seal_gen_counter`]. `claimed_ms` alone is
    /// a race timer, not a lease: the old operation's final append can
    /// still be queued inside a committer when its wall-clock window
    /// lapses, and nothing about a timestamp stops that append from
    /// closing the physical segment after somebody else has taken the
    /// claim over. Every claim-authorized append carries its
    /// generation; a takeover FENCES the old generation through the
    /// committer (which reports whether the segment already closed)
    /// before the new claim installs; the committer refuses any
    /// claim-carrying append below the fence before writing a record
    /// or closing a segment. A same-operation retry re-allocates (and
    /// thereby renews) its generation, so an actively-retrying owner
    /// can never be fenced out by an aborted takeover's reservation.
    #[serde(default)]
    pub claim_generation: u64,
}

/// What an in-flight seal owes the collection before it may publish
/// `Sealed`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum SealIntent {
    /// Close the segments; nothing else outstanding.
    #[default]
    Empty,
    /// One final record must be durable first.
    Final {
        routing_key: String,
        /// Identity of the request that owes the record, so only an
        /// exact retry of THAT request may complete the transition.
        #[serde(default)]
        request_hash: String,
        /// Set — durably, before any segment closes — once the final
        /// record is committed. Only then may the seal complete.
        final_committed: bool,
    },
}

impl SealState {
    /// A seal that still owes a final record. Any OTHER seal request
    /// must refuse to finish it.
    pub fn owes_final(&self) -> bool {
        matches!(
            self.intent,
            SealIntent::Final {
                final_committed: false,
                ..
            }
        )
    }
}

/// Creation-in-progress marker (audit P0). Absent = Ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitState {
    /// Identifies the creating request; a retry carrying the same hash
    /// resumes, a different one conflicts.
    pub request_hash: String,
    /// The key fingerprint this initialization was claimed under, so a
    /// resume cannot complete the work with a different key than the
    /// descriptor was created for.
    #[serde(default)]
    pub key_fingerprint: String,
    /// Wall clock of the last claim — a creator that dies leaves this
    /// stale and the next retry takes over. Staleness governs TAKEOVER
    /// only: it never means "ready" (see `http::initializing`).
    pub claimed_ms: i64,
}

/// How long an Initializing claim is honored before another request may
/// take it over (the creator is a single in-process task; a crash or
/// cancellation must not wedge the name forever).
pub const INIT_CLAIM_MS: i64 = 15_000;

/// How long a seal claim holds the collection before ANOTHER seal may
/// take it over.
///
/// A seal intent is deliberately not torn down by an ordering verdict
/// (a producer gap, a stale epoch): the missing predecessor may already
/// be inside the server, and an exact retry must still be able to
/// finish the transition it started. That leaves one hazard — an
/// operation that is simply gone, holding a collection Sealing over a
/// record it will never deliver. A claim older than this is treated as
/// abandoned, so recovery is a timeout, never a guess about whether a
/// verdict was terminal.
///
/// The window only has to outlast a request that is genuinely in
/// flight between its intent CAS and its committer verdict; past that,
/// every client has long since given up.
pub const SEAL_CLAIM_MS: i64 = 15_000;

/// A pure mutation decision for [`Registry::mutate_incarnation`].
pub enum Mutation<T> {
    /// Leave the descriptor unchanged; carry a typed reason out.
    Decline(T),
    /// Replace the descriptor with this one; carry a typed result out.
    Write(PersistedDescriptor, T),
}

/// The outcome of a [`Registry::mutate_incarnation`] call. Every
/// terminal state is named — no bool that conflates "declined" with
/// "wrong incarnation" with "gone".
#[derive(Debug, Clone, PartialEq)]
pub enum MutationResult<T> {
    Applied(T),
    Declined(T),
    IncarnationChanged,
    Missing,
}

/// Fork parentage (pinned DS protocol fork contract).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkRef {
    /// Source stream NAME (path form normalized away at parse).
    pub source: String,
    /// Source incarnation at fork time (stale references are integrity
    /// errors, not silent cross-incarnation reads).
    pub source_epoch: String,
    /// The fork boundary in the SHARED record numbering: ancestor chain
    /// serves [0, fork_offset), own records start at fork_offset.
    pub fork_offset: u64,
    /// The sub-offset as REQUESTED (idempotent-PUT identity only;
    /// its effect is baked into fork_offset / the materialized record).
    #[serde(default)]
    pub fork_sub: u64,
    /// This fork's unique id in the SOURCE's child set — the handle for
    /// idempotent reference install/release.
    #[serde(default)]
    pub fork_id: String,
}

impl ForkRef {
    /// Identity for the idempotent-PUT compare: everything the CALLER
    /// specified. `fork_id` is server-generated (this incarnation's
    /// epoch) and must not make a replayed create look different.
    pub fn same_identity(&self, other: &ForkRef) -> bool {
        // Exact, including the incarnation. The empty-epoch wildcard
        // existed for descriptors written before forks carried one;
        // under the layout-3 clean namespace there are none, and it let
        // a fork of a RECREATED source compare equal to a fork of the
        // original.
        self.source == other.source
            && self.source_epoch == other.source_epoch
            && self.fork_offset == other.fork_offset
            && self.fork_sub == other.fork_sub
    }
}

/// One immutable watch definition (spec Stage 2 §3.2): a name plus the
/// ordered JSON-pointer fields whose canonical extracted values derive
/// the watch key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchDefinition {
    pub name: String,
    pub fields: Vec<String>,
}

/// One resolved append/read target under the unified routing model:
/// which segment of the stream owns a routing key right now.
#[derive(Debug, Clone, PartialEq)]
pub struct SegRoute {
    pub seg_id: u32,
    /// Engine identity (record keyspace / history `inc` hash).
    pub identity: [u8; 16],
    /// Shard-routing hash: which shard's engine takes the write. A
    /// segment's persisted route_hash wins; zeros (and the implicit
    /// map) mean the parent stream's default route.
    pub shard_route: [u8; 16],
    /// The segment is sealed (mid-transition): the caller refreshes the
    /// descriptor once and re-resolves before erroring.
    pub sealed: bool,
    /// The routing key's fixed-point position (sketch feeding + splits).
    pub point: u64,
    /// The routing key's 128-bit hash (postings/sketch identity) —
    /// computed once here so hot paths never re-hash.
    pub key_hash: crate::crypto::RoutingKeyHash,
    /// This segment's key-point range (sketch bin domain).
    pub lo: u64,
    pub hi: u64,
}

/// Decode a stored descriptor, enforcing the pre-launch clean-switch
/// layout gate: any layout_version other than LAYOUT_VERSION — including
/// 0, which every pre-cutover descriptor deserializes to — refuses the
/// namespace rather than decoding it (spec §0: no legacy decoders).
/// Immutable serving state. Persisted JSON is never itself a serving
/// descriptor: conversion validates identity, lifecycle and topology once.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "PersistedDescriptor", into = "PersistedDescriptor")]
pub struct StreamDesc {
    persisted: PersistedDescriptor,
    epoch: [u8; 16],
}

impl std::ops::Deref for StreamDesc {
    type Target = PersistedDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.persisted
    }
}

impl From<StreamDesc> for PersistedDescriptor {
    fn from(desc: StreamDesc) -> Self {
        desc.persisted
    }
}

impl TryFrom<PersistedDescriptor> for StreamDesc {
    type Error = object_store::Error;
    fn try_from(persisted: PersistedDescriptor) -> Result<Self, Self::Error> {
        let epoch = validate_descriptor(&persisted)?;
        Ok(Self { persisted, epoch })
    }
}

#[derive(Debug)]
pub enum Lifecycle<'a> {
    Active,
    Initializing(&'a InitState),
    Sealing(&'a SealState),
    Sealed,
    RetainedForks,
    Deleted { parent_ref_pending: bool },
}

impl StreamDesc {
    pub fn key_point(routing_key: &str) -> u64 {
        PersistedDescriptor::key_point(routing_key)
    }
    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        Some(self.epoch)
    }
    pub fn epoch(&self) -> [u8; 16] {
        self.epoch
    }
    pub fn to_persisted(&self) -> PersistedDescriptor {
        self.persisted.clone()
    }
    pub fn lifecycle(&self) -> Lifecycle<'_> {
        if self.deleted {
            Lifecycle::Deleted {
                parent_ref_pending: self.parent_ref_pending,
            }
        } else if self.soft_deleted {
            Lifecycle::RetainedForks
        } else if let Some(init) = &self.init {
            Lifecycle::Initializing(init)
        } else if let Some(seal) = &self.sealing {
            Lifecycle::Sealing(seal)
        } else if self.sealed {
            Lifecycle::Sealed
        } else {
            Lifecycle::Active
        }
    }
}

fn invalid_descriptor(name: &str, reason: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "registry",
        source: format!("descriptor '{name}' corruption: {reason}").into(),
    }
}

fn validate_descriptor(d: &PersistedDescriptor) -> Result<[u8; 16], object_store::Error> {
    // The sref() invariant: a decoded name must be structurally
    // canonical, or every downstream identity derivation is unsound.
    if crate::tenant::CanonicalStreamName::new(&d.name).is_err() {
        return Err(object_store::Error::Generic {
            store: "registry",
            source: format!("descriptor name {:?} is not canonical — corruption", d.name).into(),
        });
    }
    if d.layout_version != LAYOUT_VERSION {
        return Err(object_store::Error::Generic {
            store: "registry",
            source: format!(
                "unsupported_storage_layout: descriptor '{}' has layout {} (this binary \
                 reads only {}); this namespace was written by a different implementation \
                 — deploy against a fresh bucket/PATH_PREFIX",
                d.name, d.layout_version, LAYOUT_VERSION
            )
            .into(),
        });
    }
    // WP-03/PR 5 (descriptor conversion rules): stored-REFERENCE
    // invariants the typed reconstructions rely on. Corruption REFUSES
    // here — never a downstream panic, never a silent repair.
    if let Some(f) = &d.forked_from {
        if crate::tenant::CanonicalStreamName::new(&f.source).is_err() {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: format!(
                    "descriptor '{}' fork source {:?} is not canonical — corruption \
                     (stored references are project-relative canonical names by contract)",
                    d.name, f.source
                )
                .into(),
            });
        }
        if crate::crypto::unhex(&f.source_epoch).map(|b| b.len()) != Some(16) {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: format!(
                    "descriptor '{}' fork source_epoch {:?} does not decode to 16 bytes \
                     — corruption",
                    d.name, f.source_epoch
                )
                .into(),
            });
        }
    }
    for child in &d.fork_children {
        if crate::tenant::CanonicalStreamName::new(child).is_err() {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: format!(
                    "descriptor '{}' fork child {:?} is not canonical — corruption",
                    d.name, child
                )
                .into(),
            });
        }
    }
    let epoch: [u8; 16] = crate::crypto::unhex(&d.stream_epoch)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            invalid_descriptor(&d.name, "stream_epoch must be exactly 16 hex-encoded bytes")
        })?;
    if let Some(map) = &d.segments {
        map.validate()
            .map_err(|error| invalid_descriptor(&d.name, &format!("topology: {error}")))?;
    }
    if d.sealed && d.sealing.is_some() {
        return Err(invalid_descriptor(
            &d.name,
            "sealed descriptor retains a sealing claim",
        ));
    }
    if d.init.is_some() && (d.sealed || d.sealing.is_some()) {
        return Err(invalid_descriptor(
            &d.name,
            "initializing descriptor cannot be sealing or sealed",
        ));
    }
    if let Some(claim) = &d.sealing {
        if claim.claim_generation > d.seal_gen_counter {
            return Err(invalid_descriptor(
                &d.name,
                "seal claim generation exceeds its allocator",
            ));
        }
        if d.segments.as_ref().is_some_and(|map| map.pending.is_some()) {
            return Err(invalid_descriptor(
                &d.name,
                "seal and topology claims are mutually exclusive",
            ));
        }
    }
    Ok(epoch)
}

pub(crate) fn decode_desc(
    raw: &[u8],
    expect: Option<&crate::tenant::TenantStreamRef>,
) -> Result<StreamDesc, object_store::Error> {
    // Layout gate FIRST, on a minimal probe: a legacy descriptor must
    // be refused as unsupported_storage_layout — the precise
    // operator-facing diagnostic — not as a parse error on the fields
    // layout 4 made mandatory.
    #[derive(serde::Deserialize)]
    struct LayoutProbe {
        #[serde(default)]
        layout_version: u32,
        #[serde(default)]
        name: String,
    }
    let probe: LayoutProbe =
        serde_json::from_slice(raw).map_err(|e| object_store::Error::Generic {
            store: "registry",
            source: format!("descriptor parse: {e}").into(),
        })?;
    if probe.layout_version != LAYOUT_VERSION {
        return Err(object_store::Error::Generic {
            store: "registry",
            source: format!(
                "unsupported_storage_layout: descriptor '{}' has layout {} (this binary \
                 reads only {}); this namespace was written by a different implementation \
                 — deploy against a fresh bucket/PATH_PREFIX",
                probe.name, probe.layout_version, LAYOUT_VERSION
            )
            .into(),
        });
    }
    let persisted: PersistedDescriptor = serde_json::from_slice(raw)
        .map_err(|error| invalid_descriptor("<unknown>", &format!("parse: {error}")))?;
    let desc = StreamDesc::try_from(persisted)?;
    if let Some(expected) = expect {
        if desc.project_id != *expected.project_id() || desc.name != expected.name().as_str() {
            return Err(invalid_descriptor(
                &desc.name,
                &format!("identity mismatch with path {expected}"),
            ));
        }
    }
    Ok(desc)
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

impl PersistedDescriptor {
    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }

    /// The project-qualified identity of this stream. Names are
    /// validated at decode/create, so reconstructing the checked type
    /// is an invariant, not a convenience.
    pub fn sref(&self) -> crate::tenant::TenantStreamRef {
        crate::tenant::TenantStreamRef::new(
            self.project_id.clone(),
            crate::tenant::CanonicalStreamName::new(&self.name)
                .expect("descriptor name is canonical (verified at decode/create)"),
        )
    }

    /// Registry identity of a stream REFERENCED by this descriptor —
    /// fork parentage (`ForkRef.source`) and dead-letter targets
    /// (`ConsumerConfig.dead_letter_stream`) store bare names by
    /// contract, and those names bind inside the REFERRING stream's
    /// project. Every stored-reference resolution goes through here,
    /// which is what makes a cross-project reference unrepresentable
    /// rather than merely checked (MULTITENANCY Stage 4 same-project
    /// fork/DLQ rule).
    // mt-lint: allow(name-param-shared-core): THE sanctioned constructor — derives the ref from the descriptor's OWN project (stored references resolve only through here)
    pub fn ref_in_project(&self, name: &str) -> crate::tenant::TenantStreamRef {
        crate::tenant::TenantStreamRef::new(
            self.project_id.clone(),
            crate::tenant::CanonicalStreamName::new(name)
                .expect("stored stream references are canonical (validated when written)"),
        )
    }

    /// Storage identity (layout 4, MULTITENANCY §2.1):
    /// storage-v1 + project_id + name + stream_epoch — so a recreated
    /// stream gets a fresh keyspace (delete/recreate isolation) and two
    /// projects sharing a name never share a byte.
    pub fn storage_hash(&self) -> [u8; 16] {
        crate::crypto::SegmentHash::for_stream(&self.sref(), &self.stream_epoch).0
    }

    pub fn is_json(&self) -> bool {
        media_type(&self.content_type) == "application/json"
    }

    /// Fixed-point position of a routing key in the [0,1) keyspace —
    /// the coordinate the segment map partitions. The empty/default key
    /// is an ordinary key at stream_hash("")'s position.
    pub fn key_point(routing_key: &str) -> u64 {
        let h = crate::crypto::stream_hash(routing_key);
        u64::from_be_bytes(h[..8].try_into().expect("hash prefix"))
    }

    /// Engine identity of a dynamic-map segment (ROUTING-V3 §2).
    /// Segment 0 is ALWAYS `storage_hash()` — that equality is what
    /// makes every pre-v3 total-order stream already-migrated, with its
    /// whole history as segment 0 and zero data movement.
    pub fn dynamic_segment_identity(&self, seg_id: u32) -> [u8; 16] {
        if seg_id == 0 {
            return self.storage_hash();
        }
        crate::crypto::SegmentHash::for_segment(&self.sref(), &self.stream_epoch, seg_id).0
    }

    /// THE unified routing resolution (ROUTING-V3 §1-2): routing key →
    /// the segment that owns it right now. Handles every layout:
    ///
    /// - descriptor-resident dynamic map (`segments: Some`) — the v3
    ///   model; selects the live segment containing the key point;
    /// - everything else — the implicit single-segment map: segment 0,
    ///   identity `storage_hash()`, parent's shard route. This arm IS
    ///   the old total-order behavior, unchanged to the byte.
    ///
    /// Legacy `scaling` descriptors are routed by their child-stream
    /// machinery upstream of this call until PR4 folds them in; this
    /// function never sees their parent appends.
    /// The physical shard route of one segment: its persisted
    /// route_hash when assigned (split children get real, independent
    /// routes — review blocker 1: a split must add capacity, not just
    /// lineage), the shard-prefix hash for prefix-pinned segments, and
    /// the parent stream route for the implicit/seg-0 case.
    pub fn segment_route(&self, seg: &crate::segmap::SegmentDesc) -> [u8; 16] {
        if seg.route_hash != [0u8; 16] {
            seg.route_hash
        } else if seg.shard_prefix.is_empty() {
            crate::crypto::RouteHash::for_stream(&self.sref()).0
        } else {
            crate::crypto::stream_hash(&seg.shard_prefix)
        }
    }

    /// Unknown explicit segments have no routing authority. The parent
    /// route belongs only to the absent-map implicit segment zero.
    pub fn segment_route_by_id(&self, seg_id: u32) -> Option<[u8; 16]> {
        match &self.segments {
            Some(map) => map.get(seg_id).map(|segment| self.segment_route(segment)),
            None if seg_id == 0 => Some(crate::crypto::RouteHash::for_stream(&self.sref()).0),
            None => None,
        }
    }

    pub fn resolve_segment(&self, routing_key: &str) -> SegRoute {
        let parent_route = crate::crypto::RouteHash::for_stream(&self.sref()).0;
        let key_hash = crate::crypto::RoutingKeyHash::of(routing_key);
        let point = u64::from_be_bytes(key_hash.0[..8].try_into().expect("hash prefix"));
        if let Some(map) = &self.segments {
            // Live segment containing the point; a well-formed map has
            // exactly one. A malformed map (no live cover) falls back to
            // sealed-any-cover so the caller's refresh path can heal.
            let live = map
                .segments
                .iter()
                .find(|s| s.is_live() && s.contains(point));
            // No live cover = mid-transition (a seal published before
            // its successors, or a scaler died between the two): pick
            // the NEWEST sealed cover — the deepest lineage point, the
            // one whose successor the refresh will reveal — never a
            // long-superseded ancestor.
            let chosen = live.or_else(|| {
                map.segments
                    .iter()
                    .filter(|s| s.contains(point))
                    .max_by_key(|s| (s.created_ms, s.seg_id))
            });
            if let Some(seg) = chosen {
                let shard_route = self.segment_route(seg);
                return SegRoute {
                    seg_id: seg.seg_id,
                    identity: self.dynamic_segment_identity(seg.seg_id),
                    shard_route,
                    sealed: !seg.is_live(),
                    point,
                    key_hash,
                    lo: seg.lo,
                    hi: seg.hi,
                };
            }
            unreachable!("validated explicit topology covers every routing point");
        }
        SegRoute {
            seg_id: 0,
            identity: self.storage_hash(),
            shard_route: parent_route,
            sealed: false,
            point,
            key_hash,
            lo: 0,
            hi: crate::segmap::KEYSPACE_END,
        }
    }
}

/// Media type with parameters stripped, lowercased.
pub fn media_type(ct: &str) -> String {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// The failed phase is part of the API, not inferred from Display text.
/// Only Conflict is retried internally. A PUT failure without an explicit
/// conditional conflict may have committed and is therefore ambiguous.
#[derive(Debug)]
pub enum MutationError {
    ReadUnavailable(object_store::Error),
    InvalidData(object_store::Error),
    MissingConditionalToken(object_store::Error),
    Conflict(object_store::Error),
    AmbiguousCompletion(object_store::Error),
}
impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = match self {
            Self::ReadUnavailable(e)
            | Self::InvalidData(e)
            | Self::MissingConditionalToken(e)
            | Self::Conflict(e)
            | Self::AmbiguousCompletion(e) => e,
        };
        std::fmt::Display::fmt(error, f)
    }
}
impl std::error::Error for MutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::ReadUnavailable(e)
            | Self::InvalidData(e)
            | Self::MissingConditionalToken(e)
            | Self::Conflict(e)
            | Self::AmbiguousCompletion(e) => e,
        })
    }
}

pub struct Registry {
    store: Arc<dyn ObjectStore>,
    /// §10.4 system-root scoping; validated cell id from config.
    cell: Arc<str>,
    cache: Mutex<HashMap<crate::tenant::TenantStreamRef, CachedDesc>>,
    cache_ttl: Duration,
    /// Test-only one-shot: the NEXT `get` for a listed name returns a
    /// store error (round-18 fail-closed refresh probe).
    #[cfg(test)]
    // mt-lint: allow(name-keyed-map): test failpoint set, canonical names of the rig's own streams
    fail_next_get: Mutex<std::collections::HashSet<String>>,
    /// SR3-2 test failpoint: the next list_page for this project fails.
    // mt-lint: allow(name-keyed-map): test failpoint set, project ids armed by the rig
    fail_next_list: Mutex<std::collections::HashSet<String>>,
    /// Round-4 review: one-shot descriptor-put failure (the deterministic
    /// stand-in for an etag-precondition conflict), keyed by stream name.
    #[cfg(test)]
    // mt-lint: allow(name-keyed-map): test failpoint set, canonical names of the rig's own streams
    fail_next_put: Mutex<std::collections::HashSet<String>>,
}

struct CachedDesc {
    desc: Option<StreamDesc>,
    at: Instant,
    /// Store ETag of the object this entry was read from. TTL refreshes
    /// revalidate with If-None-Match instead of refetching: descriptors
    /// are immutable for the life of an incarnation, so almost every
    /// refresh is a 304 — uncharged on Tigris — instead of a billable
    /// GET (object-store cost review, item 5).
    etag: Option<String>,
}

/// ORDER-PRESERVING descriptor path (audit P0). The catalog must
/// paginate a million streams with provider continuation and page-local
/// GETs, which requires the object key to sort exactly as the stream
/// name does. Hex of the name's UTF-8 bytes is order-preserving
/// (fixed-width per byte, and '0'..'9' < 'a'..'f'), unambiguous, and
/// legal in every object key — at the cost of readability in the
/// bucket, which the descriptor's own `name` field restores. A
/// hash-keyed path (the pre-audit scheme) sorts randomly, which is why
/// listing had to scan and sort everything.
fn desc_path(cell: &str, sref: &crate::tenant::TenantStreamRef) -> ObjPath {
    if sref.project_id().is_system() {
        // MULTITENANCY §10.4: system streams (_usage, _ops_*) live
        // OUTSIDE every customer project root, under the cell.
        ObjPath::from(format!(
            "system/v1/cells/{}/{}.json",
            cell,
            hex(sref.name().as_str().as_bytes())
        ))
    } else {
        ObjPath::from(format!(
            "registry/v4/projects/{}/streams/{}.json",
            hex(sref.project_id().as_bytes()),
            hex(sref.name().as_str().as_bytes())
        ))
    }
}

/// The catalog scan root for one project (§10.3): a project catalog
/// never sees another project's keys, by prefix construction.
fn project_streams_prefix(project: &crate::tenant::ProjectId) -> String {
    format!("registry/v4/projects/{}/streams/", hex(project.as_bytes()))
}

/// One page of the stream catalog.
/// Why a generation-fenced mutation did not apply.
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg(test)]
pub enum IncarnationCas {
    Applied,
    /// The mutation itself declined (its own precondition failed).
    Declined,
    /// The name now holds a DIFFERENT stream: this operation belongs to
    /// an incarnation that no longer exists, and must not touch it.
    IncarnationChanged,
}

pub struct CatalogPage {
    pub streams: Vec<StreamDesc>,
    /// Name to continue after, when more may follow.
    pub next_after: Option<String>,
    /// The provider listing ran out. This — NOT the page being
    /// underfull — is what ends a catalog walk: a page can come back
    /// short because it crossed a run of tombstones, expirations or
    /// half-built streams, and treating that as the end makes every
    /// live stream after the run unreachable.
    pub exhausted: bool,
}

/// Recover the stream name from a descriptor object key, so a page can
/// continue past entries it could not read.
fn name_from_desc_path(p: &ObjPath) -> Option<String> {
    let last = p.as_ref().rsplit('/').next()?;
    let hexed = last.strip_suffix(".json")?;
    let bytes = crate::crypto::unhex(hexed)?;
    String::from_utf8(bytes).ok()
}

impl Registry {
    /// PR 3.2.1: takes the PROVEN [`crate::tenant::CellId`] — the old
    /// `&str` signature re-validated here and `expect`-ed, contradicting
    /// the validated-configuration boundary.
    pub fn new(store: Arc<dyn ObjectStore>, cell: &crate::tenant::CellId) -> Registry {
        Registry {
            store,
            cell: Arc::from(cell.as_str()),
            cache: Mutex::new(HashMap::new()),
            cache_ttl: Duration::from_secs(5),
            #[cfg(test)]
            fail_next_get: Mutex::new(std::collections::HashSet::new()),
            fail_next_list: Mutex::new(std::collections::HashSet::new()),
            #[cfg(test)]
            fail_next_put: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Bounded cache insert: the descriptor cache previously grew with
    /// every distinct name ever touched (static-audit memory finding —
    /// creates alone put 100k entries in it). At the cap, expired
    /// entries purge first (TTL is seconds, so this is almost always
    /// enough), then the oldest entry falls out.
    fn cache_insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        const REGISTRY_CACHE_MAX: usize = 65_536;
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= REGISTRY_CACHE_MAX && !cache.contains_key(&sref) {
            let ttl = self.cache_ttl;
            cache.retain(|_, e| e.at.elapsed() < ttl);
            if cache.len() >= REGISTRY_CACHE_MAX
                && let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, e)| e.at)
                    .map(|(n, _)| n.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(sref, entry);
    }

    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Test-only: plant a descriptor in the cache as if this instance
    /// had read it moments ago — the cross-instance stale-descriptor
    /// shape (another instance CAS'd a transition we have not seen).
    #[cfg(test)]
    pub fn test_poison_cache(&self, sref: &crate::tenant::TenantStreamRef, desc: StreamDesc) {
        self.cache_insert(
            sref.clone(),
            CachedDesc {
                desc: Some(desc),
                at: Instant::now(),
                etag: None,
            },
        );
    }

    pub async fn get(
        &self,
        sref: &crate::tenant::TenantStreamRef,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        #[cfg(test)]
        if self
            .fail_next_get
            .lock()
            .unwrap()
            .remove(sref.name().as_str())
        {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: "injected registry get failure".into(),
            });
        }
        let revalidate = {
            let cache = self.cache.lock().unwrap();
            match cache.get(sref) {
                Some(e) if e.at.elapsed() < self.cache_ttl => return Ok(e.desc.clone()),
                Some(e) => e.etag.clone().map(|t| (t, e.desc.clone())),
                None => None,
            }
        };
        // TTL expired on a descriptor we hold an ETag for: conditional
        // refresh. Unchanged (the overwhelmingly common case — a
        // descriptor changes only on delete/recreate/config update) comes
        // back 304 and only renews the TTL; a real change pays for a body.
        let opts = |etag: Option<String>| object_store::GetOptions {
            if_none_match: etag,
            ..Default::default()
        };
        let (etag_sent, cached_desc) = match revalidate {
            Some((t, d)) => (Some(t), d),
            None => (None, None),
        };
        let fetched = match self
            .store
            .get_opts(&desc_path(&self.cell, sref), opts(etag_sent.clone()))
            .await
        {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match decode_desc(&raw, Some(sref)) {
                    Ok(d) => (Some(d), etag),
                    Err(e) => {
                        return Err(object_store::Error::Generic {
                            store: "registry",
                            source: format!("descriptor for {sref}: {e}").into(),
                        });
                    }
                }
            }
            Err(object_store::Error::NotModified { .. }) => (cached_desc, etag_sent),
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e),
        };
        self.cache_insert(
            sref.clone(),
            CachedDesc {
                desc: fetched.0.clone(),
                at: Instant::now(),
                etag: fetched.1,
            },
        );
        Ok(fetched.0)
    }

    /// Create a descriptor; on a lost CAS race, return the winner's.
    pub async fn create(
        &self,
        desc: impl Into<PersistedDescriptor>,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        let desc = StreamDesc::try_from(desc.into())?;
        let sref = desc.sref();
        let raw = serde_json::to_vec(&desc).expect("desc json");
        match self
            .store
            .put_opts(
                &desc_path(&self.cell, &sref),
                PutPayload::from(raw),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(put) => {
                self.cache_insert(
                    sref.clone(),
                    CachedDesc {
                        desc: Some(desc.clone()),
                        at: Instant::now(),
                        etag: put.e_tag,
                    },
                );
                Ok((true, desc))
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.invalidate(&sref);
                let existing =
                    self.get(&sref)
                        .await?
                        .ok_or_else(|| object_store::Error::NotFound {
                            path: sref.to_string(),
                            source: "raced create then missing".into(),
                        })?;
                Ok((false, existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Replace a dead (deleted/expired) descriptor with a fresh incarnation.
    /// Predicated CAS: the replacement applies only while the current
    /// descriptor is still dead per `still_dead`. Racing recreators get
    /// exactly one winner; a loser observes the winner's live descriptor
    /// (`(false, winner)`) instead of overwriting its incarnation.
    pub async fn recreate(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        fresh: impl Into<PersistedDescriptor>,
        still_dead: impl Fn(&StreamDesc) -> bool,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        let fresh = StreamDesc::try_from(fresh.into())?;
        if fresh.sref() != *sref {
            return Err(invalid_descriptor(
                &fresh.name,
                "recreation identity mismatch",
            ));
        }
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(&self.cell, sref)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(object_store::Error::NotFound {
                        path: sref.to_string(),
                        source: "recreate on missing descriptor".into(),
                    });
                }
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let current: StreamDesc = decode_desc(&raw, Some(sref))?;
            if !still_dead(&current) {
                self.cache_insert(
                    sref.clone(),
                    CachedDesc {
                        desc: Some(current.clone()),
                        at: Instant::now(),
                        etag: etag.clone(),
                    },
                );
                return Ok((false, current));
            }
            let body = serde_json::to_vec(&fresh).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(&self.cell, sref),
                    PutPayload::from(body),
                    PutOptions::from(ConditionalUpdateToken::from_etag(etag)?.mode()),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(sref);
                    return Ok((true, fresh));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "descriptor CAS retries exhausted".into(),
        })
    }

    /// CAS-update the descriptor (delete = tombstone).
    #[allow(dead_code)]
    // production callers converted to fenced APIs; kept as the corruption fail-closed probe (tests) pending a Stage-4 cleanup decision
    #[cfg(test)]
    pub async fn update<F: Fn(&mut PersistedDescriptor)>(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        apply: F,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(&self.cell, sref)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            // Fail CLOSED on corruption (was: treated as missing).
            let mut desc = decode_desc(&raw, Some(sref))?.to_persisted();
            apply(&mut desc);
            let desc = StreamDesc::try_from(desc)?;
            let body = serde_json::to_vec(&desc).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(&self.cell, sref),
                    PutPayload::from(body),
                    PutOptions::from(ConditionalUpdateToken::from_etag(etag)?.mode()),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(sref);
                    return Ok(Some(desc));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "descriptor CAS retries exhausted".into(),
        })
    }

    /// Test compatibility adapter for old incarnation-outcome fixtures.
    #[cfg(test)]
    pub async fn cas_update_incarnation_outcome(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        expected_epoch: &str,
        mut mutate: impl FnMut(&mut PersistedDescriptor) -> bool,
    ) -> anyhow::Result<IncarnationCas> {
        let mut moved = false;
        let applied = self
            .cas_update_retry(sref, |d| {
                if d.stream_epoch != expected_epoch {
                    moved = true;
                    return false;
                }
                moved = false;
                mutate(d)
            })
            .await?;
        Ok(if applied {
            IncarnationCas::Applied
        } else if moved {
            IncarnationCas::IncarnationChanged
        } else {
            IncarnationCas::Declined
        })
    }

    /// Typed, side-effect-free incarnation-fenced mutation — the
    /// primitive the audit history kept asking for. The `decide`
    /// closure is `Fn` over an IMMUTABLE descriptor and returns a
    /// value, not a bool over `&mut` with captured out-parameters:
    /// re-running it across an object-store precondition retry is safe
    /// BY CONSTRUCTION, so a decision from a lost attempt can never
    /// survive into a winning one (the round-14 `release_fork_ref`
    /// bug is unrepresentable here). Only the successful attempt's
    /// result is returned. Incarnation mismatch and a missing
    /// descriptor are distinct outcomes, never silent declines.
    /// Tombstones are visible to `decide` (fork-debt cleanup writes
    /// them) under the same identity discipline.
    pub async fn mutate_incarnation<T>(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        expected_epoch: &str,
        decide: impl Fn(&StreamDesc) -> Mutation<T>,
    ) -> Result<MutationResult<T>, MutationError> {
        let path = desc_path(&self.cell, sref);
        let mut last: Option<object_store::Error> = None;
        for attempt in 0..5u32 {
            self.invalidate(sref);
            let got = match self.store.get(&path).await {
                Ok(g) => g,
                Err(object_store::Error::NotFound { .. }) => {
                    return Ok(MutationResult::Missing);
                }
                Err(e) => return Err(MutationError::ReadUnavailable(e)),
            };
            let etag = got.meta.e_tag.clone();
            let bytes = got.bytes().await.map_err(MutationError::ReadUnavailable)?;
            let desc: StreamDesc =
                decode_desc(&bytes, Some(sref)).map_err(MutationError::InvalidData)?;
            if desc.stream_epoch != expected_epoch {
                return Ok(MutationResult::IncarnationChanged);
            }
            let (next, result) = match decide(&desc) {
                Mutation::Decline(t) => return Ok(MutationResult::Declined(t)),
                Mutation::Write(next, t) => (next, t),
            };
            let next = StreamDesc::try_from(next).map_err(MutationError::InvalidData)?;
            if next.sref() != *sref || next.stream_epoch != expected_epoch {
                return Err(MutationError::InvalidData(invalid_descriptor(
                    &next.name,
                    "mutation changed incarnation identity",
                )));
            }
            let body = serde_json::to_vec(&next).map_err(|error| {
                MutationError::InvalidData(invalid_descriptor(&next.name, &error.to_string()))
            })?;
            let mode = ConditionalUpdateToken::from_etag(etag)
                .map_err(MutationError::MissingConditionalToken)?
                .mode();
            let write = async {
                #[cfg(test)]
                if self.take_fail_next_put(sref) {
                    return Err(object_store::Error::Precondition {
                        path: path.to_string(),
                        source: "injected registry put conflict".into(),
                    });
                }
                self.store
                    .put_opts(
                        &path,
                        PutPayload::from(body),
                        PutOptions {
                            mode,
                            ..Default::default()
                        },
                    )
                    .await
            }
            .await;
            match write {
                Ok(_) => {
                    self.invalidate(sref);
                    return Ok(MutationResult::Applied(result));
                }
                // Precondition conflict: another writer moved the
                // descriptor. Re-read and re-decide from scratch —
                // `decide` is pure, so this is always safe.
                Err(e @ object_store::Error::Precondition { .. }) => {
                    last = Some(e);
                    if attempt < 4 {
                        tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
                    }
                }
                Err(e) => return Err(MutationError::AmbiguousCompletion(e)),
            }
        }
        Err(MutationError::Conflict(last.expect(
            "five exhausted attempts each returned a precondition conflict",
        )))
    }

    #[cfg(test)]
    pub async fn cas_update_retry(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        mut mutate: impl FnMut(&mut PersistedDescriptor) -> bool,
    ) -> anyhow::Result<bool> {
        let mut last = None;
        for attempt in 0..5u32 {
            self.invalidate(sref);
            match self.cas_update(sref, &mut mutate).await {
                Ok(v) => return Ok(v),
                Err(e) if retryable_cas_error(&e) => {
                    last = Some(e);
                    if attempt < 4 {
                        tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("cas_update_retry exhausted")))
    }

    #[cfg(test)]
    pub async fn cas_update(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        mut mutate: impl FnMut(&mut PersistedDescriptor) -> bool,
    ) -> anyhow::Result<bool> {
        let path = desc_path(&self.cell, sref);
        let got = match self.store.get(&path).await {
            Ok(g) => g,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let etag = got.meta.e_tag.clone();
        let bytes = got.bytes().await?;
        let mut desc = decode_desc(&bytes, Some(sref))?.to_persisted();
        if desc.deleted {
            return Ok(false);
        }
        if !mutate(&mut desc) {
            return Ok(false);
        }
        let desc = StreamDesc::try_from(desc)?;
        if desc.sref() != *sref {
            return Err(
                invalid_descriptor(&desc.name, "mutation changed descriptor identity").into(),
            );
        }
        let body = serde_json::to_vec(&desc)?;
        let mode = ConditionalUpdateToken::from_etag(etag)?.mode();
        #[cfg(test)]
        if self.take_fail_next_put(sref) {
            // One-shot injected put failure standing in for the etag
            // precondition conflict a concurrent descriptor writer
            // produces — the same Err class `cas_update_retry` exists
            // to absorb (this function's anyhow boundary is where the
            // store error lands anyway).
            return Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: "injected registry put conflict".into(),
            }
            .into());
        }
        self.store
            .put_opts(
                &path,
                PutPayload::from(body),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        self.invalidate(sref);
        Ok(true)
    }

    pub fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        self.cache.lock().unwrap().remove(sref);
    }

    /// Arm a ONE-SHOT store error for the next `get(name)` (round 18:
    /// the deletion saga must fail CLOSED when it cannot re-read the
    /// segment map after a sweep).
    #[cfg(test)]
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub fn fail_next_get(&self, name: &str) {
        self.fail_next_get.lock().unwrap().insert(name.to_string());
    }

    /// SR3-2 test failpoint: fail the next catalog page walk for this
    /// project (drives the fail-closed seed path).
    #[cfg(test)]
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub fn fail_next_list(&self, project: &str) {
        self.fail_next_list
            .lock()
            .unwrap()
            .insert(project.to_string());
    }

    /// Round-4 review failpoint: fail the next DESCRIPTOR PUT for this
    /// stream name exactly once — the deterministic stand-in for the
    /// etag-precondition conflict a concurrent descriptor writer (a
    /// touch_ttl slide, a fork release, any late CAS) produces when it
    /// lands inside another mutator's read-modify-write window.
    #[cfg(test)]
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub fn fail_next_put(&self, name: &str) {
        self.fail_next_put.lock().unwrap().insert(name.to_string());
    }

    #[cfg(test)]
    fn take_fail_next_put(&self, sref: &crate::tenant::TenantStreamRef) -> bool {
        self.fail_next_put
            .lock()
            .unwrap()
            .remove(sref.name().as_str())
    }

    /// Force a cached entry past its TTL so tests can exercise the
    /// refresh path without sleeping through the real TTL.
    #[cfg(test)]
    fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
        if let Some(e) = self.cache.lock().unwrap().get_mut(sref) {
            e.at -= self.cache_ttl + Duration::from_secs(1);
        }
    }

    /// Visible and reconciliation catalogs share provider progress,
    /// bounded ordered fetches, decoding and continuation semantics.
    pub async fn list_page(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage, object_store::Error> {
        if self.fail_next_list.lock().unwrap().remove(project.as_str()) {
            return Err(catalog_error("armed list failpoint"));
        }
        self.catalog_page(project, after, limit, false).await
    }

    pub async fn list_page_raw(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage, object_store::Error> {
        self.catalog_page(project, after, limit, true).await
    }

    async fn catalog_page(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<&str>,
        limit: usize,
        include_inactive: bool,
    ) -> Result<CatalogPage, object_store::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        if limit == 0 {
            return Err(catalog_error("catalog limit must be positive"));
        }
        let limit = limit.min(1000);
        let max_scan = limit.saturating_mul(8) + 64;
        const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
        const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
        let root = project_streams_prefix(project);
        let prefix = ObjPath::from(root.trim_end_matches('/'));
        let offset = after.map(|n| ObjPath::from(format!("{root}{}.json", hex(n.as_bytes()))));
        let listing = match &offset {
            Some(o) => self.store.list_with_offset(Some(&prefix), o),
            None => self.store.list(Some(&prefix)),
        };
        let pass = async {
            let mut reads = listing
                .take(max_scan)
                .map(|meta| async move {
                    let meta = meta?;
                    let name = name_from_desc_path(&meta.location)
                        .ok_or_else(|| catalog_error("non-canonical catalog key"))?;
                    if meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                        return Err(catalog_error("descriptor exceeds catalog byte budget"));
                    }
                    let raw = match self.store.get(&meta.location).await {
                        Ok(result) => {
                            let mut chunks = result.into_stream();
                            let mut raw = Vec::new();
                            while let Some(chunk) = chunks.try_next().await? {
                                if raw.len().saturating_add(chunk.len()) > MAX_DESCRIPTOR_BYTES {
                                    return Err(catalog_error(
                                        "descriptor exceeds catalog byte budget",
                                    ));
                                }
                                raw.extend_from_slice(&chunk);
                            }
                            raw
                        }
                        Err(object_store::Error::NotFound { .. }) => return Ok((name, None, 0)),
                        Err(error) => return Err(error),
                    };
                    if raw.len() > MAX_DESCRIPTOR_BYTES {
                        return Err(catalog_error("descriptor exceeds catalog byte budget"));
                    }
                    let canonical = crate::tenant::CanonicalStreamName::new(&name)
                        .map_err(|_| catalog_error("non-canonical catalog name"))?;
                    let expect = crate::tenant::TenantStreamRef::new(project.clone(), canonical);
                    let desc = decode_desc(&raw, Some(&expect)).map_err(|error| {
                        catalog_error(&format!(
                            "catalog: undecodable descriptor at {}: {error}",
                            meta.location
                        ))
                    })?;
                    Ok((name, Some(desc), raw.len()))
                })
                .buffered(8);
            let (mut out, mut last_name, mut scanned, mut bytes) =
                (Vec::new(), None, 0usize, 0usize);
            let now = crate::shard::now_ms();
            let mut exhausted = false;
            while out.len() < limit {
                let Some((name, desc, size)) = reads.try_next().await? else {
                    exhausted = scanned < max_scan;
                    break;
                };
                if bytes.saturating_add(size) > MAX_PAGE_BYTES {
                    break;
                }
                bytes += size;
                scanned += 1;
                // Advance only through consumed provider results. Prefetched
                // results beyond the output/byte limit are retried next page.
                last_name = Some(name);
                if let Some(desc) = desc {
                    let active = !desc.deleted
                        && !desc.soft_deleted
                        && desc.init.is_none()
                        && !desc.expires_at_ms.is_some_and(|expires| now >= expires);
                    if include_inactive || active {
                        out.push(desc);
                    }
                }
            }
            Ok(CatalogPage {
                streams: out,
                next_after: last_name,
                exhausted,
            })
        };
        tokio::time::timeout(Duration::from_secs(10), pass)
            .await
            .map_err(|_| catalog_error("catalog page deadline exceeded"))?
    }
}

fn catalog_error(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "registry",
        source: message.to_string().into(),
    }
}

// ---- shard topology ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub version: u64,
    /// Complete binary prefix code over the stream-hash bit space. "" = one
    /// shard covering everything.
    pub shards: Vec<String>,
    /// The request-body ceiling this namespace was created with.
    ///
    /// R23-2: the absorber sizes its worst-frame reservation from the
    /// CURRENT process setting, so a deployment that lowers
    /// MAX_REQUEST_BODY_BYTES on a namespace already holding a large
    /// unabsorbed record would under-reserve for it — reintroducing
    /// exactly the under-reservation the process-wide budget exists to
    /// prevent. Recording it here lets startup refuse the mismatch.
    ///
    /// `None` on topologies written before this field existed; those
    /// namespaces were created at the 32 MiB protocol pin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_body_bytes: Option<usize>,
}

const TOPOLOGY_PATH: &str = "topology.json";

pub async fn load_or_init_topology(
    store: &Arc<dyn ObjectStore>,
    initial_shards: crate::config::validation::InitialShards,
    body_ceiling: usize,
) -> Result<Topology, object_store::Error> {
    let initial_shards = initial_shards.get();
    let path = ObjPath::from(TOPOLOGY_PATH);
    match store.get(&path).await {
        Ok(r) => {
            let raw = r.bytes().await?;
            // Fail CLOSED: a corrupt topology must abort boot. Panicking is
            // wrong (crash loop) and treating it as missing would be far
            // worse (re-initializing re-shards the whole keyspace).
            return serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: format!("corrupt topology object: {e}").into(),
            });
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    // PR 3.2.1: nonzero power-of-two is proven by the InitialShards
    // type; no assertion is repeated here.
    let bits = (initial_shards as f64).log2() as usize;
    let shards: Vec<String> = if bits == 0 {
        vec![String::new()]
    } else {
        (0..initial_shards)
            .map(|i| format!("{:0width$b}", i, width = bits))
            .collect()
    };
    let topo = Topology {
        version: 1,
        shards,
        max_request_body_bytes: Some(body_ceiling),
    };
    let raw = serde_json::to_vec(&topo).expect("topology json");
    match store
        .put_opts(
            &path,
            PutPayload::from(raw),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(topo),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let r = store.get(&path).await?;
            let raw = r.bytes().await?;
            serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: format!("corrupt topology object: {e}").into(),
            })
        }
        Err(e) => Err(e),
    }
}

pub(crate) fn hash_bits(hash: &[u8; 16]) -> String {
    let mut bits = String::with_capacity(24);
    for byte in hash.iter().take(3) {
        bits.push_str(&format!("{byte:08b}"));
    }
    bits
}

/// Longest-prefix match of the stream hash's leading bits against the shard
/// set. `shards` must form a complete prefix code.
pub fn shard_for_hash(shards: &[String], hash: &[u8; 16]) -> String {
    let bits = hash_bits(hash);
    shards
        .iter()
        .filter(|p| bits.starts_with(p.as_str()))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

/// Does `hash` fall inside the shard identified by bit-prefix `prefix`?
pub fn shard_prefix_matches(prefix: &str, hash: &[u8; 16]) -> bool {
    hash_bits(hash).starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::stream_hash;
    use object_store::ObjectStoreExt;

    #[test]
    fn r08_missing_conditional_token_fails_closed() {
        for token in [None, Some(String::new())] {
            let error = ConditionalUpdateToken::from_etag(token).unwrap_err();
            assert!(matches!(error, object_store::Error::Generic { .. }));
        }
        assert!(matches!(
            ConditionalUpdateToken::from_etag(Some("etag".into()))
                .unwrap()
                .mode(),
            PutMode::Update(_)
        ));
    }

    #[test]
    fn r08_only_precondition_conflicts_are_retried() {
        let conflict = anyhow::Error::from(object_store::Error::Precondition {
            path: "descriptor".into(),
            source: "wording is irrelevant".into(),
        });
        assert!(retryable_cas_error(&conflict));
        for error in [
            anyhow::anyhow!("precondition conflict"),
            anyhow::Error::from(ConditionalUpdateToken::from_etag(None).unwrap_err()),
        ] {
            assert!(!retryable_cas_error(&error));
        }
    }

    #[tokio::test]
    async fn r04_invalid_descriptors_cannot_reach_storage() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let registry = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let valid = desc("validated", "00112233445566778899aabbccddeeff", false);
        let mut invalid = Vec::new();
        let mut d = valid.clone();
        d.stream_epoch = "00".into();
        invalid.push(d);
        let mut d = valid.clone();
        d.stream_epoch = "z".repeat(32);
        invalid.push(d);
        let mut d = valid.clone();
        d.segments = Some(crate::segmap::SegmentMap {
            version: 1,
            next_seg_id: 1,
            segments: vec![],
            pending: None,
        });
        invalid.push(d);
        let map = crate::segmap::SegmentMap::initial("", 1);
        let mut d = valid.clone();
        let mut m = map.clone();
        m.segments[0].lo = 1;
        d.segments = Some(m);
        invalid.push(d);
        let mut d = valid.clone();
        let mut m = map.clone();
        m.segments.push(m.segments[0].clone());
        d.segments = Some(m);
        invalid.push(d);
        let mut d = valid.clone();
        let mut m = map.clone();
        m.segments[0].predecessors.push(77);
        d.segments = Some(m);
        invalid.push(d);
        let mut d = valid.clone();
        d.sealed = true;
        d.sealing = Some(SealState {
            operation_id: "op".into(),
            claimed_ms: 1,
            claim_generation: 0,
            intent: SealIntent::Empty,
        });
        invalid.push(d);
        for (index, descriptor) in invalid.into_iter().enumerate() {
            assert!(
                StreamDesc::try_from(descriptor.clone()).is_err(),
                "invalid case {index}"
            );
            assert!(
                registry.create(descriptor).await.is_err(),
                "invalid case {index} reached create"
            );
            assert!(matches!(
                store.get(&desc_path("test-cell", &valid.sref())).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
    }

    #[test]
    fn r04_valid_transition_and_sealed_predecessor_snapshots() {
        let mut dto = desc("validated", "00112233445566778899aabbccddeeff", false);
        let mut map = crate::segmap::SegmentMap::initial("", 1);
        map.pending = Some(crate::segmap::PendingTransition {
            kind: "split".into(),
            segs: vec![0],
            split_at: u64::MAX / 2,
            started_ms: 1,
            seal_gen: 0,
        });
        dto.segments = Some(map.clone());
        assert!(
            StreamDesc::try_from(dto.clone()).is_ok(),
            "pending split is a valid recovery state"
        );
        map.pending = None;
        let (a, b) = map.split(0, u64::MAX / 2, 7, [1; 16], [2; 16], 2).unwrap();
        dto.segments = Some(map.clone());
        assert!(StreamDesc::try_from(dto.clone()).is_ok());
        map.merge(a, b, 3, 4, [3; 16], 3).unwrap();
        dto.segments = Some(map.clone());
        assert!(StreamDesc::try_from(dto.clone()).is_ok());
        for segment in map.segments.iter_mut().filter(|segment| segment.is_live()) {
            segment.sealed_ms = Some(4);
            segment.sealed_next_offset = Some(5);
        }
        dto.segments = Some(map);
        dto.sealed = true;
        let descriptor = StreamDesc::try_from(dto).unwrap();
        assert!(matches!(descriptor.lifecycle(), Lifecycle::Sealed));
        assert_eq!(
            descriptor.epoch(),
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ]
        );
        assert!(descriptor.segment_route_by_id(999).is_none());
        assert!(descriptor.resolve_segment("key").sealed);
    }

    fn tp() -> crate::tenant::ProjectId {
        crate::tenant::ProjectId::new("proj-test").unwrap()
    }
    /// MULTITENANCY §19 "Identity": two projects owning the same name
    /// share NOTHING — paths, route hashes, storage hashes, segment
    /// identities all differ — and the workspace is not an input to
    /// any of them (transfer changes none of these values).
    #[test]
    fn same_name_two_projects_share_no_identity() {
        let pa = crate::tenant::ProjectId::new("proj-a").unwrap();
        let pb = crate::tenant::ProjectId::new("proj-b").unwrap();
        let mk = |p: &crate::tenant::ProjectId| {
            let mut d = desc("orders", "00000000000000000000000000000001", false);
            d.project_id = p.clone();
            d
        };
        let (da, db) = (mk(&pa), mk(&pb));
        // Registry paths.
        assert_ne!(
            desc_path("cell", &da.sref()).to_string(),
            desc_path("cell", &db.sref()).to_string()
        );
        // Route, storage, and dynamic-segment identities.
        assert_ne!(
            crate::crypto::RouteHash::for_stream(&da.sref()),
            crate::crypto::RouteHash::for_stream(&db.sref())
        );
        assert_ne!(da.storage_hash(), db.storage_hash());
        assert_ne!(
            da.dynamic_segment_identity(3),
            db.dynamic_segment_identity(3)
        );
        // resolve_segment end to end: same key, disjoint physical
        // coordinates.
        let (ra, rb) = (da.resolve_segment("user-1"), db.resolve_segment("user-1"));
        assert_ne!(ra.identity, rb.identity);
        assert_ne!(ra.shard_route, rb.shard_route);
        // Same project + name + epoch = identical (the stable identity).
        assert_eq!(mk(&pa).storage_hash(), da.storage_hash());
        // And the catalog scan prefixes are disjoint by construction.
        assert_ne!(project_streams_prefix(&pa), project_streams_prefix(&pb));
        assert!(!project_streams_prefix(&pa).starts_with(&project_streams_prefix(&pb)));
    }

    fn ts(name: &str) -> crate::tenant::TenantStreamRef {
        crate::tenant::TenantStreamRef::new(
            tp(),
            crate::tenant::CanonicalStreamName::new(name).unwrap(),
        )
    }

    fn desc(name: &str, epoch: &str, deleted: bool) -> PersistedDescriptor {
        PersistedDescriptor {
            seal_gen_counter: 0,
            account_id: None,
            project_id: tp(),
            name: name.into(),
            stream_epoch: epoch.into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            layout_version: LAYOUT_VERSION,
        }
    }

    /// Pre-launch clean switch: a descriptor written by the previous
    /// experimental layout (no layout_version, or any other value) is
    /// REFUSED — never decoded, translated, or rewritten.
    #[tokio::test]
    async fn layout_gate_refuses_foreign_namespaces() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        // Old-shape descriptor: valid JSON, no layout_version field.
        let old = serde_json::json!({
            "name": "legacy",
            "stream_epoch": "00000000000000000000000000000000",
            "key_fingerprint": "fp",
            "created_ms": 1,
            "profile": "queue",
            "content_type": "application/json",
        });
        put_raw(&store, "legacy", old.to_string().as_bytes()).await;
        let err = reg.get(&ts("legacy")).await.expect_err("gate must refuse");
        assert!(
            err.to_string().contains("unsupported_storage_layout"),
            "wrong refusal: {err}"
        );
        // A current-layout descriptor round-trips.
        let d = desc("fresh", "00000000000000000000000000000001", false);
        put_raw(&store, "fresh", &serde_json::to_vec(&d).unwrap()).await;
        assert!(reg.get(&ts("fresh")).await.unwrap().is_some());
    }

    async fn put_raw(store: &Arc<dyn ObjectStore>, name: &str, body: &[u8]) {
        store
            .put(
                &desc_path("test-cell", &ts(name)),
                object_store::PutPayload::from(body.to_vec()),
            )
            .await
            .unwrap();
    }

    /// Wrapper that counts get traffic and how it resolved, so the
    /// conditional-refresh path is provable rather than assumed.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: std::sync::atomic::AtomicU64,
        conditional: std::sync::atomic::AtomicU64,
        not_modified: std::sync::atomic::AtomicU64,
        puts: std::sync::atomic::AtomicU64,
        omit_etag: std::sync::atomic::AtomicBool,
        lose_put_reply: std::sync::atomic::AtomicBool,
    }
    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            use std::sync::atomic::Ordering::SeqCst;
            self.puts.fetch_add(1, SeqCst);
            let result = self.inner.put_opts(location, payload, opts).await?;
            if self.lose_put_reply.swap(false, SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "arbitrary message after accepted PUT".into(),
                });
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            use std::sync::atomic::Ordering::Relaxed;
            self.gets.fetch_add(1, Relaxed);
            if options.if_none_match.is_some() {
                self.conditional.fetch_add(1, Relaxed);
            }
            let mut r = self.inner.get_opts(location, options).await;
            if self.omit_etag.load(Relaxed)
                && let Ok(result) = &mut r
            {
                result.meta.e_tag = None;
            }
            if matches!(&r, Err(object_store::Error::NotModified { .. })) {
                self.not_modified.fetch_add(1, Relaxed);
            }
            r
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn r08_mutation_preserves_conditional_metadata_and_classifies_ambiguous_completion() {
        use std::sync::atomic::Ordering::SeqCst;
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let store = Arc::new(CountingStore {
            inner: inner.clone(),
            gets: Default::default(),
            conditional: Default::default(),
            not_modified: Default::default(),
            puts: Default::default(),
            omit_etag: Default::default(),
            lose_put_reply: Default::default(),
        });
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let epoch = "00000000000000000000000000000001";
        reg.create(desc("conditional", epoch, false)).await.unwrap();
        let decide = |current: &StreamDesc| {
            let mut next = current.to_persisted();
            next.seal_gen_counter += 1;
            Mutation::Write(next, ())
        };
        let puts = store.puts.load(SeqCst);
        store.omit_etag.store(true, SeqCst);
        assert!(matches!(
            reg.mutate_incarnation(&ts("conditional"), epoch, decide)
                .await,
            Err(MutationError::MissingConditionalToken(_))
        ));
        assert_eq!(
            store.puts.load(SeqCst),
            puts,
            "missing metadata cannot issue a PUT"
        );
        store.omit_etag.store(false, SeqCst);
        store.lose_put_reply.store(true, SeqCst);
        assert!(matches!(
            reg.mutate_incarnation(&ts("conditional"), epoch, decide)
                .await,
            Err(MutationError::AmbiguousCompletion(_))
        ));
        assert_eq!(
            store.puts.load(SeqCst),
            puts + 1,
            "an ambiguous write must not be retried"
        );
        reg.invalidate(&ts("conditional"));
        assert_eq!(
            reg.get(&ts("conditional"))
                .await
                .unwrap()
                .unwrap()
                .seal_gen_counter,
            1,
            "the lost reply's PUT actually landed"
        );
        put_raw(&inner, "conditional", b"malformed persisted state").await;
        assert!(matches!(
            reg.mutate_incarnation(&ts("conditional"), epoch, decide)
                .await,
            Err(MutationError::InvalidData(_))
        ));
        assert_eq!(
            store.puts.load(SeqCst),
            puts + 1,
            "corruption cannot replace data"
        );
        assert!(matches!(
            reg.mutate_incarnation(&ts("missing"), epoch, decide).await,
            Ok(MutationResult::Missing)
        ));
    }

    /// MUTATION CANARY for the typed incarnation API. A real
    /// first-attempt CAS conflict — with the descriptor genuinely
    /// changed underneath — must make `decide` re-run against the NEW
    /// state, and only the winning attempt's verdict may escape. This
    /// is the round-14 `release_fork_ref` bug, made structurally
    /// impossible: `decide` is pure, so there is no out-parameter to
    /// leak from the lost attempt.
    ///
    /// Scenario: a soft-deleted source with one child C. Attempt 1
    /// removes C, sees no children, decides to TOMBSTONE — and loses
    /// the CAS to a concurrent install of child D. Attempt 2 removes C,
    /// sees D remain, decides NOT to tombstone. The result must be
    /// "removed, not tombstoned"; the source must survive for D.
    #[tokio::test]
    async fn typed_mutation_never_leaks_a_lost_attempts_decision() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let conflict = Arc::new(ConflictOnceStore {
            inner: inner.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
            inject: std::sync::Mutex::new(None),
        });
        let reg = Registry::new(
            conflict.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let mut src = desc("src", "00000000000000000000000000000001", false);
        src.soft_deleted = true;
        src.fork_children = vec!["C".into()];
        reg.create(src).await.unwrap();

        // The concurrent install that attempt 1 will lose to: the same
        // descriptor with child D added (and C still present, since
        // attempt 1 hasn't committed its removal).
        let mut installed = desc("src", "00000000000000000000000000000001", false);
        installed.soft_deleted = true;
        installed.fork_children = vec!["C".into(), "D".into()];
        *conflict.inject.lock().unwrap() = Some(serde_json::to_vec(&installed).unwrap());
        conflict
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let outcome = reg
            .mutate_incarnation(&ts("src"), "00000000000000000000000000000001", |x| {
                let mut next = x.to_persisted();
                let before = next.fork_children.len();
                next.fork_children.retain(|c| c != "C");
                let removed = next.fork_children.len() != before;
                let should_tombstone =
                    next.fork_children.is_empty() && next.soft_deleted && !next.deleted;
                if should_tombstone {
                    next.deleted = true;
                    next.soft_deleted = false;
                }
                Mutation::Write(next, (removed, should_tombstone))
            })
            .await
            .unwrap();

        match outcome {
            MutationResult::Applied((removed, tombstoned)) => {
                assert!(removed, "C should have been removed");
                assert!(
                    !tombstoned,
                    "the lost attempt's tombstone decision leaked into the result"
                );
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        let after = reg.get(&ts("src")).await.unwrap().unwrap();
        assert!(
            !after.deleted,
            "the source was tombstoned while D still forks it"
        );
        assert_eq!(after.fork_children, vec!["D".to_string()]);
    }

    /// Wraps a store to fail the FIRST `put_opts` with a precondition
    /// error, after first writing an injected body directly to the
    /// backend — simulating a concurrent writer that wins the CAS.
    #[derive(Debug)]
    struct ConflictOnceStore {
        inner: Arc<dyn ObjectStore>,
        armed: std::sync::atomic::AtomicBool,
        inject: std::sync::Mutex<Option<Vec<u8>>>,
    }
    impl std::fmt::Display for ConflictOnceStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ConflictOnceStore")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for ConflictOnceStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            use std::sync::atomic::Ordering::SeqCst;
            if self.armed.swap(false, SeqCst) {
                // The concurrent winner lands first... (take the body
                // out of the lock BEFORE awaiting — a MutexGuard may
                // not cross an await point).
                let body = self.inject.lock().unwrap().take();
                if let Some(body) = body {
                    self.inner
                        .put(location, object_store::PutPayload::from(body))
                        .await?;
                }
                // ...so our conditional put loses.
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "conflict-once".into(),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A TTL refresh of an unchanged descriptor must be a conditional GET
    /// answered 304 (uncharged on Tigris), never a billable body fetch —
    /// and a genuinely changed descriptor must still come through.
    #[tokio::test]
    async fn ttl_refresh_of_unchanged_descriptor_is_a_free_304() {
        use std::sync::atomic::Ordering::Relaxed;
        let counting = Arc::new(CountingStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
            gets: Default::default(),
            conditional: Default::default(),
            not_modified: Default::default(),
            puts: Default::default(),
            omit_etag: Default::default(),
            lose_put_reply: Default::default(),
        });
        let reg = Registry::new(
            counting.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let (created, _) = reg
            .create(desc("s", "00000000000000000000000000000001", false))
            .await
            .unwrap();
        assert!(created);

        // Warm read: cache hit, no store traffic at all.
        assert_eq!(
            reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
            "00000000000000000000000000000001"
        );
        assert_eq!(
            counting.gets.load(Relaxed),
            0,
            "warm read touched the store"
        );

        // TTL expiry on an unchanged descriptor: exactly one conditional
        // GET, answered 304, still serving the cached descriptor.
        reg.expire_for_tests(&ts("s"));
        assert_eq!(
            reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
            "00000000000000000000000000000001"
        );
        assert_eq!(
            counting.conditional.load(Relaxed),
            1,
            "refresh was not conditional"
        );
        assert_eq!(
            counting.not_modified.load(Relaxed),
            1,
            "refresh paid for a body"
        );

        // The 304 renews the TTL: the next read is a cache hit again.
        let gets_now = counting.gets.load(Relaxed);
        assert_eq!(
            reg.get(&ts("s")).await.unwrap().unwrap().stream_epoch,
            "00000000000000000000000000000001"
        );
        assert_eq!(
            counting.gets.load(Relaxed),
            gets_now,
            "304 did not renew the TTL"
        );

        // A real change (delete tombstone) must come through on the next
        // refresh — the conditional path must never pin a stale view.
        reg.update(&ts("s"), |d| d.deleted = true).await.unwrap();
        reg.expire_for_tests(&ts("s"));
        // update() invalidates, so re-prime the cache then expire it.
        assert!(reg.get(&ts("s")).await.unwrap().unwrap().deleted);
        reg.expire_for_tests(&ts("s"));
        assert!(reg.get(&ts("s")).await.unwrap().unwrap().deleted);
    }

    #[tokio::test]
    async fn catalog_provider_progress_survives_empty_filtered_pages_and_prefetch() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store,
            &crate::tenant::CellId::new("catalog-budget").unwrap(),
        );
        for n in 0..100 {
            reg.create(desc(
                &format!("stream-{n:03}"),
                "00000000000000000000000000000001",
                n < 90,
            ))
            .await
            .unwrap();
        }
        let first = reg.list_page(&tp(), None, 2).await.unwrap();
        assert!(first.streams.is_empty());
        assert!(first.next_after.is_some());
        assert!(!first.exhausted);
        let mut after = first.next_after;
        let mut names = Vec::new();
        loop {
            let page = reg.list_page(&tp(), after.as_deref(), 2).await.unwrap();
            names.extend(page.streams.iter().map(|d| d.name.clone()));
            if page.exhausted {
                break;
            }
            assert_ne!(page.next_after, after);
            after = page.next_after;
        }
        assert_eq!(
            names,
            (90..100)
                .map(|n| format!("stream-{n:03}"))
                .collect::<Vec<_>>()
        );
        assert!(reg.list_page_raw(&tp(), None, 0).await.is_err());
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = reg.list_page_raw(&tp(), after.as_deref(), 3).await.unwrap();
            all.extend(page.streams.iter().map(|d| d.name.clone()));
            if page.exhausted {
                break;
            }
            after = page.next_after;
        }
        assert_eq!(
            all,
            (0..100)
                .map(|n| format!("stream-{n:03}"))
                .collect::<Vec<_>>()
        );
    }

    /// Round-22 item 7: the tombstone write carries the logical close
    /// stamp durably, and the RAW catalog page — the reconciler's view
    /// — returns tombstoned and expired descriptors that the customer
    /// catalog hides.
    #[tokio::test]
    async fn tombstone_stamp_persists_and_raw_page_sees_terminals() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        reg.create(desc("alive", "00000000000000000000000000000001", false))
            .await
            .unwrap();
        reg.create(desc("gone", "00000000000000000000000000000002", false))
            .await
            .unwrap();
        let mut ex = desc("expired", "00000000000000000000000000000003", false);
        ex.expires_at_ms = Some(1); // long past
        reg.create(ex).await.unwrap();
        // Tombstone with the stamp in the SAME write.
        reg.update(&ts("gone"), |d| {
            d.deleted = true;
            d.logical_close_ms = Some(1_786_000_000_000);
        })
        .await
        .unwrap();
        reg.invalidate(&ts("gone"));
        let got = reg.get(&ts("gone")).await.unwrap().unwrap();
        assert!(got.deleted);
        assert_eq!(
            got.logical_close_ms,
            Some(1_786_000_000_000),
            "the debt survives on the tombstone"
        );
        // Customer catalog: only the live stream.
        let visible = reg.list_page(&tp(), None, 10).await.unwrap();
        assert_eq!(visible.streams.len(), 1);
        assert_eq!(visible.streams[0].name, "alive");
        // Reconciler view: everything, terminals included.
        let raw = reg.list_page_raw(&tp(), None, 10).await.unwrap();
        assert_eq!(raw.streams.len(), 3, "raw page hides nothing");
        assert!(raw.streams.iter().any(|d| d.deleted));
        assert!(
            raw.streams
                .iter()
                .any(|d| d.expires_at_ms.is_some_and(|e| e < 1000)),
            "expired descriptor present"
        );
    }

    /// A corrupt descriptor must surface as an ERROR — treating it as
    /// absent lets a create/recreate overwrite a live stream's identity.
    #[tokio::test]
    async fn corrupt_descriptor_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        put_raw(&store, "s1", b"{ not json").await;
        assert!(
            reg.get(&ts("s1")).await.is_err(),
            "corrupt descriptor returned as absent/ok"
        );
        // update() must also refuse (was: Ok(None), i.e. missing).
        assert!(reg.update(&ts("s1"), |_| {}).await.is_err());
    }

    /// WP-03/PR 5 decode invariants: a descriptor whose stored
    /// identities do not decode REFUSES at the boundary — never a
    /// downstream `expect` panic, never a silent repair. Each case
    /// starts from a VALID descriptor and corrupts one field, so the
    /// refusal is attributable to that invariant alone.
    #[tokio::test]
    async fn corrupt_stored_identities_fail_closed_at_decode() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        reg.create(desc("base", "00000000000000000000000000000001", false))
            .await
            .unwrap();
        let raw = store
            .get(&desc_path("test-cell", &ts("base")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let valid: serde_json::Value = serde_json::from_slice(&raw).unwrap();

        type Corruption = (&'static str, Box<dyn Fn(&mut serde_json::Value)>);
        let cases: Vec<Corruption> = vec![
            (
                "fork source not canonical",
                Box::new(|d| {
                    d["forked_from"] = serde_json::json!({
                        "source": "__ds/reserved",
                        "source_epoch": "00".repeat(16),
                        "fork_offset": 0,
                        "fork_sub": 0,
                        "fork_id": "f1",
                    });
                }),
            ),
            (
                "fork source_epoch short",
                Box::new(|d| {
                    d["forked_from"] = serde_json::json!({
                        "source": "ok-name",
                        "source_epoch": "0000",
                        "fork_offset": 0,
                        "fork_sub": 0,
                        "fork_id": "f1",
                    });
                }),
            ),
            (
                "fork child not canonical",
                Box::new(|d| {
                    d["fork_children"] = serde_json::json!(["bad//child"]);
                }),
            ),
        ];
        for (why, mutate) in cases {
            let mut c = valid.clone();
            mutate(&mut c);
            put_raw(&store, "base", c.to_string().as_bytes()).await;
            // A FRESH registry per case: the writer registry's 5s
            // descriptor cache would otherwise serve the pre-corruption
            // value and mask the decode check.
            let fresh = Registry::new(
                store.clone(),
                &crate::tenant::CellId::new("test-cell").unwrap(),
            );
            let err = fresh.get(&ts("base")).await;
            assert!(err.is_err(), "{why}: corrupt descriptor must refuse");
            let msg = format!("{}", err.err().unwrap());
            assert!(
                msg.contains("corruption"),
                "{why}: refusal must name corruption: {msg}"
            );
        }
        // And the untouched valid form still decodes (the checks refuse
        // corruption, not legitimate descriptors).
        put_raw(&store, "base", valid.to_string().as_bytes()).await;
        let fresh = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        assert!(fresh.get(&ts("base")).await.is_ok());
    }

    /// A corrupt topology must abort boot, never panic and NEVER be treated
    /// as missing (re-initializing re-shards the whole keyspace).
    #[tokio::test]
    async fn corrupt_topology_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &ObjPath::from(TOPOLOGY_PATH),
                object_store::PutPayload::from(b"garbage".to_vec()),
            )
            .await
            .unwrap();
        assert!(
            load_or_init_topology(
                &store,
                crate::config::validation::InitialShards::new(4).unwrap(),
                crate::protocol_pin::MAX_BODY_BYTES,
            )
            .await
            .is_err()
        );
        // The corrupt object must still be there — not replaced by a fresh
        // initialization.
        let raw = store
            .get(&ObjPath::from(TOPOLOGY_PATH))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&raw[..], b"garbage");
    }

    /// Racing recreators of a dead incarnation: exactly one winner; the
    /// loser observes the winner's descriptor instead of overwriting it.
    #[tokio::test]
    async fn recreate_race_has_one_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(
            store.clone(),
            &crate::tenant::CellId::new("test-cell").unwrap(),
        );
        let (created, _) = reg
            .create(desc("s", "00000000000000000000000000000006", true))
            .await
            .unwrap();
        assert!(created);

        let alive = |d: &StreamDesc| !d.deleted;
        let (won_a, got_a) = reg
            .recreate(
                &ts("s"),
                desc("s", "00000000000000000000000000000007", false),
                |d| !alive(d),
            )
            .await
            .unwrap();
        assert!(won_a, "first recreate must win");
        assert_eq!(got_a.stream_epoch, "00000000000000000000000000000007");

        // Second recreator raced and lost: descriptor is now alive, so the
        // predicate fails and it must observe epoch-a, not install epoch-b.
        let (won_b, got_b) = reg
            .recreate(
                &ts("s"),
                desc("s", "00000000000000000000000000000008", false),
                |d| !alive(d),
            )
            .await
            .unwrap();
        assert!(!won_b, "second recreate must lose");
        assert_eq!(got_b.stream_epoch, "00000000000000000000000000000007");

        reg.invalidate(&ts("s"));
        let stored = reg.get(&ts("s")).await.unwrap().unwrap();
        assert_eq!(
            stored.stream_epoch, "00000000000000000000000000000007",
            "loser overwrote the winner"
        );
    }

    // ---- ROUTING-V3 resolution (docs/ROUTING-V3.md §1-2) ------------

    /// Total-order streams (segments: None, no legacy fields) resolve
    /// every key to segment 0 with identity == storage_hash() — the
    /// zero-move migration guarantee.
    #[test]
    fn implicit_map_is_the_old_total_order_layout() {
        let d = desc("t", "00000000000000000000000000000001", false);
        for rk in ["", "a", "user-42", "\u{1F600}"] {
            let r = d.resolve_segment(rk);
            assert_eq!(r.seg_id, 0);
            assert_eq!(r.identity, d.storage_hash());
            // Layout 4: the parent route is project-qualified and
            // domain-separated — NOT the bare name hash.
            assert_eq!(
                r.shard_route,
                crate::crypto::RouteHash::for_stream(&d.sref()).0
            );
            assert_ne!(r.shard_route, stream_hash("t"));
            assert!(!r.sealed);
        }
    }

    /// Descriptor-resident dynamic maps: the live segment containing
    /// the key point wins; seg 0's identity is storage_hash() (old data
    /// stays addressable); sealed segments surface `sealed` so the
    /// caller refreshes; a segment with its own shard_prefix routes to
    /// that shard.
    #[test]
    fn dynamic_map_resolution_selects_the_live_cover() {
        let mut d = desc("dyn", "00000000000000000000000000000003", false);
        let mut map = crate::segmap::SegmentMap::initial("", 1);
        // Split the keyspace in half: seg 0 sealed, children 1 and 2.
        let mid = u64::MAX / 2;
        map.segments[0].sealed_ms = Some(2);
        map.segments[0].sealed_next_offset = Some(10);
        map.segments.push(crate::segmap::SegmentDesc {
            seg_id: 1,
            lo: 0,
            hi: mid,
            shard_prefix: String::new(),
            route_hash: [0u8; 16],
            created_ms: 2,
            predecessors: vec![0],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        map.segments.push(crate::segmap::SegmentDesc {
            seg_id: 2,
            lo: mid,
            hi: crate::segmap::KEYSPACE_END,
            shard_prefix: "shard-07".into(),
            route_hash: [0u8; 16],
            created_ms: 2,
            predecessors: vec![0],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        map.next_seg_id = 3;
        map.version = 2;
        d.segments = Some(map);

        assert_eq!(d.dynamic_segment_identity(0), d.storage_hash());

        // Find one key on each side of the midpoint.
        let mut lo_key = None;
        let mut hi_key = None;
        for i in 0..64 {
            let k = format!("k{i}");
            if StreamDesc::key_point(&k) < mid {
                lo_key.get_or_insert(k);
            } else {
                hi_key.get_or_insert(k);
            }
            if lo_key.is_some() && hi_key.is_some() {
                break;
            }
        }
        let (lo_key, hi_key) = (lo_key.unwrap(), hi_key.unwrap());

        let r = d.resolve_segment(&lo_key);
        assert_eq!((r.seg_id, r.sealed), (1, false));
        assert_eq!(r.identity, d.dynamic_segment_identity(1));
        assert_ne!(r.identity, d.storage_hash());
        assert_eq!(
            r.shard_route,
            crate::crypto::RouteHash::for_stream(&d.sref()).0,
            "empty prefix = parent route"
        );

        let r = d.resolve_segment(&hi_key);
        assert_eq!((r.seg_id, r.sealed), (2, false));
        assert_eq!(r.shard_route, stream_hash("shard-07"));

        // Seal child 1 with no successor yet (mid-transition crash
        // shape): resolution surfaces sealed=true instead of failing.
        d.segments.as_mut().unwrap().segments[1].sealed_ms = Some(3);
        let r = d.resolve_segment(&lo_key);
        assert_eq!((r.seg_id, r.sealed), (1, true));
    }

    /// Serde: fresh descriptors stay byte-lean (no "segments" key);
    /// pre-v3 JSON (no field at all) parses; a materialized map
    /// round-trips.
    #[test]
    fn descriptor_segments_serde_roundtrip() {
        let d = desc("s", "00000000000000000000000000000004", false);
        let j = serde_json::to_string(&d).unwrap();
        assert!(
            !j.contains("\"segments\""),
            "implicit map must cost zero bytes"
        );
        let legacy: StreamDesc = serde_json::from_str(&j.replace("\"name\"", "\"name\"")).unwrap();
        assert!(legacy.segments.is_none());

        let mut with_map = desc("s2", "00000000000000000000000000000005", false);
        with_map.segments = Some(crate::segmap::SegmentMap::initial("sh", 7));
        let j2 = serde_json::to_string(&with_map).unwrap();
        let back: StreamDesc = serde_json::from_str(&j2).unwrap();
        assert_eq!(back.segments, with_map.segments);
    }
}
