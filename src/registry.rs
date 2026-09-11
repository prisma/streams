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
pub(crate) const LAYOUT_VERSION: u32 = 4;

/// Seal-in-progress marker (audit P0). Present = Sealing: normal
/// appends are refused, only the matching seal operation may write its
/// final record, and any request observing it resumes the transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SealState {
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
pub(crate) enum SealIntent {
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
    pub(crate) fn owes_final(&self) -> bool {
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
pub(crate) struct InitState {
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
pub(crate) const INIT_CLAIM_MS: i64 = 15_000;

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
pub(crate) const SEAL_CLAIM_MS: i64 = 15_000;

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
pub(crate) enum MutationResult<T> {
    Applied(T),
    Declined(T),
    IncarnationChanged,
    Missing,
}

/// Fork parentage (pinned DS protocol fork contract).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ForkRef {
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
    pub(crate) fn same_identity(&self, other: &ForkRef) -> bool {
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
pub(crate) struct WatchDefinition {
    pub name: String,
    pub fields: Vec<String>,
}

/// One resolved append/read target under the unified routing model:
/// which segment of the stream owns a routing key right now.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SegRoute {
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
pub(crate) struct StreamDesc {
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
pub(crate) enum Lifecycle<'a> {
    Active,
    Initializing(&'a InitState),
    Sealing,
    Sealed,
    RetainedForks,
    Deleted { parent_ref_pending: bool },
}

impl StreamDesc {
    pub(crate) fn key_point(routing_key: &str) -> u64 {
        PersistedDescriptor::key_point(routing_key)
    }
    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        Some(self.epoch)
    }
    pub(crate) fn epoch(&self) -> [u8; 16] {
        self.epoch
    }
    pub(crate) fn to_persisted(&self) -> PersistedDescriptor {
        self.persisted.clone()
    }
    pub(crate) fn lifecycle(&self) -> Lifecycle<'_> {
        if self.deleted {
            Lifecycle::Deleted {
                parent_ref_pending: self.parent_ref_pending,
            }
        } else if self.soft_deleted {
            Lifecycle::RetainedForks
        } else if let Some(init) = &self.init {
            Lifecycle::Initializing(init)
        } else if self.sealing.is_some() {
            Lifecycle::Sealing
        } else if self.sealed {
            Lifecycle::Sealed
        } else {
            Lifecycle::Active
        }
    }
}
// mt-lint: allow(name-param-shared-core): corruption diagnostic only; formats the already-loaded descriptor name without deriving identity
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
    if let Some(expected) = expect
        && (desc.project_id != *expected.project_id() || desc.name != expected.name().as_str())
    {
        return Err(invalid_descriptor(
            &desc.name,
            &format!("identity mismatch with path {expected}"),
        ));
    }
    Ok(desc)
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

impl PersistedDescriptor {
    pub(crate) fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }

    /// The project-qualified identity of this stream. Names are
    /// validated at decode/create, so reconstructing the checked type
    /// is an invariant, not a convenience.
    #[expect(
        clippy::expect_used,
        reason = "PersistedDescriptor::sref; descriptor names are validated at decode and create, so reconstructing the checked name cannot fail; a fallible reconstruction would let an already stored descriptor be reported as invalid"
    )]
    pub(crate) fn sref(&self) -> crate::tenant::TenantStreamRef {
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
    #[expect(
        clippy::expect_used,
        reason = "PersistedDescriptor::ref_in_project; stored stream references are validated when written, so reconstructing the checked name cannot fail; a fallible reconstruction would let a stored reference be reported as invalid"
    )]
    // mt-lint: allow(name-param-shared-core): THE sanctioned constructor — derives the ref from the descriptor's OWN project (stored references resolve only through here)
    pub(crate) fn ref_in_project(&self, name: &str) -> crate::tenant::TenantStreamRef {
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
    pub(crate) fn storage_hash(&self) -> [u8; 16] {
        crate::crypto::SegmentHash::for_stream(&self.sref(), &self.stream_epoch).0
    }

    pub(crate) fn is_json(&self) -> bool {
        media_type(&self.content_type) == "application/json"
    }

    /// Fixed-point position of a routing key in the [0,1) keyspace —
    /// the coordinate the segment map partitions. The empty/default key
    /// is an ordinary key at stream_hash("")'s position.
    #[expect(
        clippy::expect_used,
        reason = "PersistedDescriptor::key_point; the first eight bytes of a sixteen-byte hash always form a u64 prefix; a fallible conversion would add an error path no input can reach"
    )]
    pub(crate) fn key_point(routing_key: &str) -> u64 {
        let h = crate::crypto::stream_hash(routing_key);
        u64::from_be_bytes(h[..8].try_into().expect("hash prefix"))
    }

    /// Engine identity of a dynamic-map segment (ROUTING-V3 §2).
    /// Segment 0 is ALWAYS `storage_hash()` — that equality is what
    /// makes every pre-v3 total-order stream already-migrated, with its
    /// whole history as segment 0 and zero data movement.
    pub(crate) fn dynamic_segment_identity(&self, seg_id: u32) -> [u8; 16] {
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
    pub(crate) fn segment_route(&self, seg: &crate::segmap::SegmentDesc) -> [u8; 16] {
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
    pub(crate) fn segment_route_by_id(&self, seg_id: u32) -> Option<[u8; 16]> {
        match &self.segments {
            Some(map) => map.get(seg_id).map(|segment| self.segment_route(segment)),
            None if seg_id == 0 => Some(crate::crypto::RouteHash::for_stream(&self.sref()).0),
            None => None,
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "PersistedDescriptor::resolve_segment; the first eight bytes of a sixteen-byte hash always form a u64 prefix; a fallible conversion would add an error path no input can reach"
    )]
    pub(crate) fn resolve_segment(&self, routing_key: &str) -> SegRoute {
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
pub(crate) fn media_type(ct: &str) -> String {
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
pub(crate) enum MutationError {
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

pub(crate) struct Registry {
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
pub(crate) enum IncarnationCas {
    Applied,
    /// The mutation itself declined (its own precondition failed).
    Declined,
    /// The name now holds a DIFFERENT stream: this operation belongs to
    /// an incarnation that no longer exists, and must not touch it.
    IncarnationChanged,
}

pub(crate) struct CatalogPage {
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
    pub(crate) fn new(store: Arc<dyn ObjectStore>, cell: &crate::tenant::CellId) -> Registry {
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
    #[expect(
        clippy::unwrap_used,
        reason = "Registry::cache_insert; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
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

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::cache_len; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) fn cache_len(&self) -> usize {
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

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::get; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) async fn get(
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
    #[expect(
        clippy::expect_used,
        reason = "Registry::create; the value serializes to JSON from plain fields with string keys, so encoding it cannot fail; a fallible encode would report a storage error for a value the registry itself produced"
    )]
    pub(crate) async fn create(
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
    #[expect(
        clippy::expect_used,
        reason = "Registry::recreate; the value serializes to JSON from plain fields with string keys, so encoding it cannot fail; a fallible encode would report a storage error for a value the registry itself produced"
    )]
    pub(crate) async fn recreate(
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

    /// CAS-update the descriptor (delete = tombstone). Production callers
    /// converted to fenced APIs; kept as the corruption fail-closed probe
    /// (tests) pending a Stage-4 cleanup decision.
    #[cfg(test)]
    pub(crate) async fn update<F: Fn(&mut PersistedDescriptor)>(
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
    pub(crate) async fn cas_update_incarnation_outcome(
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
    #[expect(
        clippy::expect_used,
        reason = "Registry::mutate_incarnation; five exhausted attempts each recorded their precondition conflict, so the last one is always present; a fallible read would turn a bounded retry loop into an error no attempt produced"
    )]
    pub(crate) async fn mutate_incarnation<T>(
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
    pub(crate) async fn cas_update_retry(
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
    pub(crate) async fn cas_update(
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

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::invalidate; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        self.cache.lock().unwrap().remove(sref);
    }

    /// Arm a ONE-SHOT store error for the next `get(name)` (round 18:
    /// the deletion saga must fail CLOSED when it cannot re-read the
    /// segment map after a sweep).
    #[cfg(test)]
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub(crate) fn fail_next_get(&self, name: &str) {
        self.fail_next_get.lock().unwrap().insert(name.to_string());
    }

    /// SR3-2 test failpoint: fail the next catalog page walk for this
    /// project (drives the fail-closed seed path).
    #[cfg(test)]
    // mt-lint: allow(name-param-shared-core): test failpoint arming, no identity derived
    pub(crate) fn fail_next_list(&self, project: &str) {
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
    pub(crate) fn fail_next_put(&self, name: &str) {
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
    #[expect(
        clippy::unwrap_used,
        reason = "Registry::list_page; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) async fn list_page(
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

    pub(crate) async fn list_page_raw(
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
pub(crate) struct Topology {
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

#[expect(
    clippy::expect_used,
    reason = "load_or_init_topology; the value serializes to JSON from plain fields with string keys, so encoding it cannot fail; a fallible encode would report a storage error for a value the registry itself produced"
)]
pub(crate) async fn load_or_init_topology(
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
pub(crate) fn shard_for_hash(shards: &[String], hash: &[u8; 16]) -> String {
    let bits = hash_bits(hash);
    shards
        .iter()
        .filter(|p| bits.starts_with(p.as_str()))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

/// Does `hash` fall inside the shard identified by bit-prefix `prefix`?
pub(crate) fn shard_prefix_matches(prefix: &str, hash: &[u8; 16]) -> bool {
    hash_bits(hash).starts_with(prefix)
}

#[cfg(test)]
mod resolution_tests;
#[cfg(test)]
mod tests;
