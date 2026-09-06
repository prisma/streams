//! Typed append contract. No protocol response is an application result.
use crate::shard::AppendErr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Invalid,
    Denied,
    Missing,
    Gone,
    Conflict,
    Capacity,
    Unavailable,
    Timeout,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendCode {
    TargetIncarnationChanged,
    Creating,
    NotFound,
    Gone,
    MissingKey,
    WrongKey,
    Overloaded,
    InvalidProducer,
    BodyTooLarge,
    TooLarge,
    SealSuperseded,
    Internal,
    UnknownField,
    StreamClosed,
    MissingContentType,
    ContentTypeMismatch,
    EmptyBody,
    InvalidJson,
    InvalidBody,
    RecordTooLarge,
    PayloadTooLarge,
    Sealed,
    Failpoint,
    SegmentTransition,
    StreamOverloaded,
    MaintenanceBackpressure,
    EngineBackpressure,
    AppendTimeout,
    SealIncomplete,
    SeqConflict,
    ProducerGap,
    ProducerStale,
    ProducerEpochSeq,
    ProducerSequenceReused,
    ShardMoving,
    NotOwner,
    ShardOpening,
    ShardOpen,
    RateLimited(&'static str),
}
impl AppendCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TargetIncarnationChanged => "target_incarnation_changed",
            Self::Creating => "creating",
            Self::NotFound => "not_found",
            Self::Gone => "gone",
            Self::MissingKey => "missing_key",
            Self::WrongKey => "wrong_key",
            Self::Overloaded => "overloaded",
            Self::InvalidProducer => "invalid_producer",
            Self::BodyTooLarge => "body_too_large",
            Self::TooLarge => "too_large",
            Self::SealSuperseded => "seal_superseded",
            Self::Internal => "internal",
            Self::UnknownField => "unknown_field",
            Self::StreamClosed => "stream_closed",
            Self::MissingContentType => "missing_content_type",
            Self::ContentTypeMismatch => "content_type_mismatch",
            Self::EmptyBody => "empty_body",
            Self::InvalidJson => "invalid_json",
            Self::InvalidBody => "invalid_body",
            Self::RecordTooLarge => "record_too_large",
            Self::PayloadTooLarge => "payload_too_large",
            Self::Sealed => "sealed",
            Self::Failpoint => "failpoint",
            Self::SegmentTransition => "segment_transition",
            Self::StreamOverloaded => "stream_overloaded",
            Self::MaintenanceBackpressure => "maintenance_backpressure",
            Self::EngineBackpressure => "engine_backpressure",
            Self::AppendTimeout => "append_timeout",
            Self::SealIncomplete => "seal_incomplete",
            Self::SeqConflict => "seq_conflict",
            Self::ProducerGap => "producer_seq_gap",
            Self::ProducerStale => "producer_stale_epoch",
            Self::ProducerEpochSeq => "producer_epoch_seq",
            Self::ProducerSequenceReused => "producer_sequence_reused",
            Self::ShardMoving => "shard_moving",
            Self::NotOwner => "not_ring_owner",
            Self::ShardOpening => "shard_opening",
            Self::ShardOpen => "shard_open",
            Self::RateLimited(code) => code,
        }
    }
}

#[derive(Debug)]
pub(crate) struct AppendFailure {
    pub(crate) class: FailureClass,
    pub(crate) code: AppendCode,
    pub(crate) message: String,
    pub(crate) retry_after: Option<u64>,
    pub(crate) owner: Option<String>,
    pub(crate) closed_at: Option<(u32, u64, bool)>,
    pub(crate) expected: Option<u64>,
    pub(crate) received: Option<u64>,
    pub(crate) producer_epoch: Option<u64>,
}
impl AppendFailure {
    pub(crate) fn new(class: FailureClass, code: AppendCode, message: impl Into<String>) -> Self {
        Self {
            class,
            code,
            message: message.into(),
            retry_after: None,
            owner: None,
            closed_at: None,
            expected: None,
            received: None,
            producer_epoch: None,
        }
    }
    pub(crate) fn retry(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
    }
    pub(crate) fn from_resolve(error: crate::shard_directory::ResolveError) -> Self {
        use crate::shard_directory::ResolveError;
        match error {
            ResolveError::NotOwner { prefix, owner } => {
                let mut e = Self::new(
                    FailureClass::Conflict,
                    AppendCode::NotOwner,
                    format!("shard {prefix} belongs to {owner}"),
                );
                e.owner = Some(owner);
                e
            }
            ResolveError::Opening {
                code,
                retry_after_secs,
                ..
            } => Self::new(
                FailureClass::Unavailable,
                AppendCode::RateLimited(code),
                "shard not currently serving here; retry",
            )
            .retry(retry_after_secs),
            ResolveError::OpenFailed { prefix, error } => Self::new(
                FailureClass::Internal,
                AppendCode::ShardOpen,
                format!("open shard {prefix}: {error}"),
            ),
        }
    }
    pub(crate) fn from_commit(segment: u32, materialized: bool, error: AppendErr) -> Self {
        use FailureClass::*;
        match error {
            AppendErr::SeqConflict { current } => Self::new(
                Conflict,
                AppendCode::SeqConflict,
                format!("Stream-Seq must exceed {}", current.unwrap_or_default()),
            ),
            AppendErr::SealSuperseded => Self::new(
                Conflict,
                AppendCode::SealSuperseded,
                "the seal claim authorizing this write was taken over; retry the close to re-enter the claim",
            ),
            AppendErr::Closed { next_offset } => {
                let mut e = Self::new(Conflict, AppendCode::StreamClosed, "stream is closed");
                e.closed_at = Some((segment, next_offset, materialized));
                e
            }
            AppendErr::ProducerGap { expected, received } => {
                let mut e = Self::new(Conflict, AppendCode::ProducerGap, "sequence gap");
                e.expected = Some(expected);
                e.received = Some(received);
                e
            }
            AppendErr::ProducerStale { current_epoch } => {
                let mut e = Self::new(Denied, AppendCode::ProducerStale, "stale epoch");
                e.producer_epoch = Some(current_epoch);
                e
            }
            AppendErr::ProducerEpochSeq => Self::new(
                Invalid,
                AppendCode::ProducerEpochSeq,
                "a new epoch must start at seq 0",
            ),
            AppendErr::ProducerSeqReused => Self::new(
                Conflict,
                AppendCode::ProducerSequenceReused,
                "same producer sequence with a different request",
            ),
            AppendErr::CtMismatch => Self::new(
                Conflict,
                AppendCode::ContentTypeMismatch,
                "content type mismatch",
            ),
            AppendErr::BadBody(message) => Self::new(Invalid, AppendCode::InvalidBody, message),
            AppendErr::Internal(message) => Self::new(Internal, AppendCode::Internal, message),
            AppendErr::Moved => Self::new(
                Unavailable,
                AppendCode::ShardMoving,
                "shard fenced by a new owner; retry",
            )
            .retry(1),
        }
    }
    pub(crate) fn definitively_rejected(&self) -> bool {
        matches!(
            self.class,
            FailureClass::Invalid
                | FailureClass::Denied
                | FailureClass::Missing
                | FailureClass::Gone
                | FailureClass::Conflict
        ) && !matches!(
            self.code,
            AppendCode::ProducerGap | AppendCode::ProducerEpochSeq | AppendCode::NotOwner
        )
    }
}
impl std::fmt::Display for AppendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}
impl std::error::Error for AppendFailure {}

#[derive(Debug)]
pub(crate) struct AppendOutcome {
    pub(crate) seg_id: u32,
    pub(crate) materialized: bool,
    pub(crate) next_offset: u64,
    pub(crate) last_offset: u64,
    pub(crate) duplicate: bool,
    pub(crate) closed: bool,
    pub(crate) producer: Option<(u64, u64)>,
    pub(crate) appended_records: usize,
}
pub(crate) type AppendResult = Result<AppendOutcome, AppendFailure>;
pub(crate) fn fail(class: FailureClass, code: AppendCode, message: &str) -> AppendResult {
    Err(AppendFailure::new(class, code, message))
}

use crate::application::lifecycle::SealAuthz;
use crate::crypto::{StreamKey, derive_subkey};
use crate::registry::{Registry, StreamDesc};
use crate::shard::{AppendReq, now_ms};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::oneshot;
const APPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) enum AppendKey {
    Missing,
    Invalid,
    Provided(StreamKey),
}
pub(crate) struct AuthorizedAppend {
    descriptor: StreamDesc,
    key: StreamKey,
}
impl AuthorizedAppend {
    pub(crate) fn descriptor(&self) -> &StreamDesc {
        &self.descriptor
    }
    pub(crate) fn key(&self) -> &StreamKey {
        &self.key
    }
}

/// Every semantic input is explicit. Payload bytes are retained until enqueue;
/// no internal HTTP headers or serialization stands between caller and owner.
pub(crate) struct AppendCommand {
    pub(crate) sref: crate::tenant::TenantStreamRef,
    pub(crate) expected_epoch: Option<[u8; 16]>,
    pub(crate) key: StreamKey,
    pub(crate) body: Bytes,
    pub(crate) producer: Option<crate::shard::ProducerReq>,
    pub(crate) content_type: Option<String>,
    pub(crate) routing_key: String,
    pub(crate) close: bool,
    pub(crate) seal_auth: Option<SealAuthz>,
    pub(crate) request_hash: Option<[u8; 16]>,
    pub(crate) sequence: Option<String>,
    pub(crate) ts_hint_ms: Option<i64>,
    pub(crate) key_version: u32,
    pub(crate) close_identity: Option<String>,
    pub(crate) body_charge: Option<crate::quota::BufferedBodyGuard>,
}
pub(crate) struct AppendService {
    pub(crate) registry: Arc<Registry>,
    pub(crate) shards: crate::shard_directory::ShardDirectory,
    pub(crate) admission: crate::admission::AdmissionController,
    pub(crate) quotas: crate::quota::QuotaRegistry,
    pub(crate) history: Arc<crate::history::HistoryResources>,
    pub(crate) scaler: Arc<crate::scaler3::Scaler>,
    pub(crate) keys: Arc<crate::history::KeyCache>,
    pub(crate) watches: Arc<crate::application::watch::WatchService>,
    pub(crate) lifecycle: crate::application::lifecycle::LifecycleService,
    pub(crate) creation: Arc<crate::application::creation::CreationService>,
    pub(crate) auth: Arc<crate::auth::AuthService>,
    pub(crate) deployment: crate::deployment::DeploymentIdentity,
    pub(crate) admission_config: crate::config::AdmissionConfig,
    pub(crate) meter_enabled: bool,
}
impl AppendService {
    fn alive(&self, desc: &StreamDesc) -> bool {
        !desc.deleted
            && !desc.soft_deleted
            && desc.expires_at_ms.is_none_or(|expiry| now_ms() < expiry)
    }
    fn maintenance_limits(&self) -> crate::backpressure::Limits {
        crate::backpressure::Limits::from_config(&self.admission_config)
    }
    pub(crate) async fn prepare(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        key: AppendKey,
    ) -> Result<AuthorizedAppend, AppendFailure> {
        let descriptor = match self.registry.get(sref).await {
            Ok(Some(desc)) if self.alive(&desc) && desc.init.is_some() => {
                return Err(AppendFailure::new(
                    FailureClass::Unavailable,
                    AppendCode::Creating,
                    "stream is still being created; retry",
                )
                .retry(1));
            }
            Ok(Some(desc)) if self.alive(&desc) => desc,
            Ok(desc) => {
                let gone = desc.as_ref().is_some_and(|d| {
                    d.soft_deleted
                        || (!d.deleted
                            && !d.fork_children.is_empty()
                            && d.expires_at_ms.is_some_and(|expiry| now_ms() >= expiry))
                });
                return Err(AppendFailure::new(
                    if gone {
                        FailureClass::Gone
                    } else {
                        FailureClass::Missing
                    },
                    if gone {
                        AppendCode::Gone
                    } else {
                        AppendCode::NotFound
                    },
                    if gone {
                        "stream deleted; live forks remain"
                    } else {
                        "stream not found"
                    },
                ));
            }
            Err(error) => {
                return Err(AppendFailure::new(
                    FailureClass::Internal,
                    AppendCode::Internal,
                    error.to_string(),
                ));
            }
        };
        let key = match key {
            AppendKey::Provided(key)
                if key.fingerprint(&descriptor.epoch()) == descriptor.key_fingerprint =>
            {
                key
            }
            AppendKey::Missing => {
                return Err(AppendFailure::new(
                    FailureClass::Invalid,
                    AppendCode::MissingKey,
                    "Stream-Encryption-Key required",
                ));
            }
            _ => {
                return Err(AppendFailure::new(
                    FailureClass::Denied,
                    AppendCode::WrongKey,
                    "key mismatch",
                ));
            }
        };
        if self.admission.admit_write_inflight().is_err() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::Overloaded,
                "instance at admission capacity; retry",
            )
            .retry(1));
        }
        Ok(AuthorizedAppend { descriptor, key })
    }
    pub(crate) async fn check_memory(&self) -> Result<(), AppendFailure> {
        if self
            .admission
            .admit_write_memory(self.history.budget.reserved_bytes())
            .is_err()
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::Overloaded,
                "instance memory pressure; retry",
            )
            .retry(2));
        }
        Ok(())
    }
    pub(crate) async fn execute(&self, command: AppendCommand) -> AppendResult {
        let prepared = self
            .prepare(&command.sref, AppendKey::Provided(command.key.clone()))
            .await?;
        self.check_memory().await?;
        self.execute_prepared(prepared, command).await
    }
    pub(crate) async fn execute_prepared(
        &self,
        prepared: AuthorizedAppend,
        mut command: AppendCommand,
    ) -> AppendResult {
        if prepared.descriptor.sref() != command.sref || prepared.key.0 != command.key.0 {
            return fail(
                FailureClass::Internal,
                AppendCode::Internal,
                "append preparation mismatch",
            );
        }
        if command.expected_epoch.is_none() {
            command.expected_epoch = Some(prepared.descriptor.epoch());
        }
        let wrapped = prepared
            .descriptor
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some());
        let mut prepared = prepared;
        for attempt in 0..if wrapped { 4 } else { 1 } {
            let result = execute_once(self, prepared, &mut command).await;
            if !wrapped || !matches!(&result,Err(error)if error.code==AppendCode::StreamClosed) {
                return result;
            }
            self.registry.invalidate(&command.sref);
            let Ok(Some(desc)) = self.registry.get(&command.sref).await else {
                return result;
            };
            let seg = desc.resolve_segment(&command.routing_key);
            if desc.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                crate::application::topology::resume(&self.lifecycle.topology, &command.sref).await;
            } else if !seg.sealed {
                return result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10 * (attempt + 1))).await;
            prepared = self
                .prepare(&command.sref, AppendKey::Provided(command.key.clone()))
                .await?;
        }
        fail(
            FailureClass::Unavailable,
            AppendCode::SegmentTransition,
            "segment map transition did not converge; retry",
        )
    }
}

async fn execute_once(
    state: &AppendService,
    prepared: AuthorizedAppend,
    command: &mut AppendCommand,
) -> AppendResult {
    let mut desc = prepared.descriptor;
    if command
        .expected_epoch
        .is_some_and(|epoch| epoch != desc.epoch())
    {
        return fail(
            FailureClass::Conflict,
            AppendCode::TargetIncarnationChanged,
            "append target incarnation changed",
        );
    }
    let key = prepared.key;
    let epoch = desc.epoch();
    let sref = desc.sref();
    let name = sref.name().as_str().to_string();
    let body = command.body.clone();
    let close = command.close;
    let product_key = Some(command.routing_key.clone());
    let product_hash = command.request_hash;
    let seal_auth = command.seal_auth.clone();
    let mut producer = command.producer.clone();
    if let (Some(producer), Some(hash)) = (producer.as_mut(), product_hash) {
        producer.request_hash = Some(hash);
    }
    let close_only = close && body.is_empty();
    // This request's own seal identity: content hash + routing key, the
    // same envelope the intent stores. An exact retry of a crashed raw
    // close therefore recognises ITSELF as the owed final — no private
    // header, no producer opt-in required.
    // The identity of THIS close: the whole semantic request, not just
    // its payload. Two closes with the same body and routing key but
    // different producer coordination are different operations — sharing
    // one id let a request that was refused tear down the intent another
    // one owned, and the promised final record was lost.
    let this_close_op = command.close_identity.clone().unwrap_or_else(|| {
        let producer_fields = producer
            .as_ref()
            .map(|p| [p.id.clone(), p.epoch.to_string(), p.seq.to_string()])
            .unwrap_or_default();
        crate::application::lifecycle::seal_op_id_semantic(
            &crate::application::creation::create_request_hash(
                &desc.content_type,
                None,
                None,
                true,
                &body,
                None,
            ),
            &command.routing_key,
            &[
                producer_fields[0].clone(),
                producer_fields[1].clone(),
                producer_fields[2].clone(),
                command.sequence.clone().unwrap_or_default(),
                String::new(),
                command.content_type.clone().unwrap_or_default(),
                String::new(),
            ],
        )
    });
    // Owed-final authorization: either this request IS the intent's
    // record (computed identity matches) or an internal caller passed
    // the trusted operation id. Nothing a client sends can assert it.
    // A TRUSTED product final proves its whole execution token before
    // anything else: same incarnation, same claim, same generation. It
    // owns a typed transition — if the token no longer matches (the
    // collection was deleted and recreated, or the claim was taken
    // over), the append must not run at all: not write the record, not
    // close a segment, and never fall through into the raw-close claim
    // path on a stranger's descriptor.
    if let Some(auth) = &seal_auth {
        let holds = desc.stream_epoch == auth.epoch
            && desc.sealing.as_ref().is_some_and(|sl| {
                sl.operation_id == auth.op_id && sl.claim_generation == auth.generation
            });
        if !holds {
            return fail(
                FailureClass::Conflict,
                AppendCode::SealSuperseded,
                "the seal this final record belongs to no longer holds its claim",
            );
        }
    }
    let is_owed_final = desc.sealing.as_ref().is_some_and(|sl| {
        sl.owes_final()
            && (sl.operation_id == this_close_op
                || Some(sl.operation_id.as_str()) == seal_auth.as_ref().map(|a| a.op_id.as_str()))
    });
    // The generation this request's claim-authorized writes will carry.
    // Filled by whichever path holds the claim: the trusted internal
    // seal (its ticket), an owed-final resume (renewed below), or a
    // fresh close (begin_sealing_for_close's install).
    let mut raw_seal_gen: Option<u64> = seal_auth.as_ref().map(|a| a.generation);
    if is_owed_final && seal_auth.is_none() {
        // RESUME of a crashed close: renew the claim before appending.
        // Renewal re-allocates the generation, so the resume can never
        // be fenced out by a takeover reservation that aborted after
        // this operation's original attempt.
        match crate::application::lifecycle::renew_owed_claim(
            &state.lifecycle,
            &desc.sref(),
            &this_close_op,
            &desc.stream_epoch,
        )
        .await
        {
            Ok(Some(g)) => raw_seal_gen = Some(g),
            Ok(None) => {
                // The claim moved between the descriptor read and the
                // renewal: whoever holds it now decides. Answer as a
                // conflict rather than write under a claim we lost.
                return fail(
                    FailureClass::Conflict,
                    AppendCode::Sealed,
                    "the seal this close was resuming has been superseded",
                );
            }
            Err(e) => {
                return fail(
                    FailureClass::Unavailable,
                    AppendCode::Internal,
                    &e.to_string(),
                );
            }
        }
    }

    // A raw close that carries content and brings no producer of its own
    // gets a SYNTHETIC one, derived from its operation identity. Without
    // it the second crash boundary is unrecoverable: once the records
    // are durable and the segment is closed, an exact retry reaches the
    // committer's closed-stream check (which only forgives an empty
    // close-only) and is refused — so `final_committed` is never
    // written and the collection stays Sealing over records it already
    // holds. With it, the retry is recognised as a duplicate BEFORE the
    // closed check, and can finish the transition.
    let synthetic_producer = close && !body.is_empty() && producer.is_none();
    if synthetic_producer {
        producer = Some(crate::shard::ProducerReq {
            id: format!(
                "{}rawseal.{this_close_op}",
                crate::shard::INTERNAL_PRODUCER_PREFIX
            ),
            epoch: 1,
            seq: 0,
            request_hash: None,
        });
    }

    // Collection lifecycle (audit P0): the DESCRIPTOR is authoritative,
    // so a sealed collection refuses NEW records even when a segment
    // engine has not observed its close yet. Requests whose pinned
    // answer is idempotent success — close-only retries and producer
    // requests, whose duplicate check must still return 204 — are
    // deferred to the committer, which owns that decision and answers
    // 409 with Stream-Next-Offset when they are genuinely new writes.
    // A producer request rides through so the committer can recognise a
    // retry and answer it with its original result — but it carries the
    // refusal with it, and the committer applies it to anything that
    // turns out to be a NEW sequence. Without that, a novel producer
    // write was accepted while the descriptor said Sealing or Sealed.
    // The seal's OWN final record is the one write a Sealing collection
    // still owes. Its identity is COMPUTED from this request — content
    // hash and routing key — and compared with the durable intent.
    //
    // It used to be asserted by an `x-seal-final` request header, which
    // was wrong twice over: any caller could send it (knowing the id was
    // enough to smuggle an arbitrary record into a sealing collection),
    // and no ordinary client sends it, so an exact retry after a crash
    // was rejected as a new write and the collection stayed stuck owing
    // a record nobody could deliver.
    let sealed_reject_new =
        if (desc.sealed || desc.sealing.is_some()) && !close_only && !is_owed_final {
            Some(if desc.sealed {
                crate::shard::SealedReject::Sealed
            } else {
                crate::shard::SealedReject::Sealing
            })
        } else {
            None
        };
    if sealed_reject_new.is_some() && producer.is_none() {
        // The pinned closure contract requires Stream-Next-Offset on
        // the 409, so read the sealed tail before answering.
        let seg0 = desc.resolve_segment("");
        let engine = match state
            .shards
            .resolve(
                &seg0.shard_route,
                crate::shard_directory::Adoption::External,
            )
            .await
        {
            Ok(e) => e,
            Err(e) => return Err(AppendFailure::from_resolve(e)),
        };
        let handle = match engine.stream_handle(seg0.identity).await {
            Ok(h) => h,
            Err(e) => {
                return fail(
                    FailureClass::Unavailable,
                    AppendCode::Internal,
                    &e.to_string(),
                );
            }
        };
        let next = handle.state.lock().unwrap().durable.next;
        return Err(AppendFailure::from_commit(
            seg0.seg_id,
            desc.segments.is_some(),
            AppendErr::Closed { next_offset: next },
        ));
    }

    // A raw close seals the whole COLLECTION, so the intent has to be
    // durable before any physical segment closes. Publishing it
    // afterwards left a window where other routing keys' segments were
    // still writable while this one was already closed, and a failure
    // in between produced a permanently split-brained collection that
    // still answered the close with success.

    // (the raw close intent is published further down, once every
    // deterministic error has been ruled out — see `close_intent`)

    // Content-Type: required on POST with a body; must match the stream's
    // configured media type (case-insensitive; parameters ignored). A
    // close-only POST ignores content type entirely. With producer headers
    // the mismatch is deferred so duplicates still return 204.
    let ct = command.content_type.clone();
    let mut deferred: Option<crate::shard::DeferredErr> = None;
    if !close_only {
        match &ct {
            None => {
                if producer.is_some() {
                    deferred = Some(crate::shard::DeferredErr::BadBody(
                        "missing Content-Type".into(),
                    ));
                } else {
                    return fail(
                        FailureClass::Invalid,
                        AppendCode::MissingContentType,
                        "Content-Type required",
                    );
                }
            }
            Some(c) => {
                if crate::registry::media_type(c) != crate::registry::media_type(&desc.content_type)
                {
                    if producer.is_some() {
                        deferred = Some(crate::shard::DeferredErr::CtMismatch);
                    } else {
                        return fail(
                            FailureClass::Conflict,
                            AppendCode::ContentTypeMismatch,
                            "content type mismatch",
                        );
                    }
                }
            }
        }
    }

    // Body -> entries (batching rules); errors deferred with producers.
    let mut entries: Vec<Bytes> = Vec::new();
    if !close_only && deferred.is_none() {
        if body.is_empty() {
            if producer.is_some() {
                deferred = Some(crate::shard::DeferredErr::BadBody("empty body".into()));
            } else {
                return fail(FailureClass::Invalid, AppendCode::EmptyBody, "empty body");
            }
        } else if desc.is_json() {
            match crate::application::creation::json_entries(&body, false) {
                Ok(v) => entries = v,
                Err(m) => {
                    if producer.is_some() {
                        deferred = Some(crate::shard::DeferredErr::BadBody(m));
                    } else {
                        return fail(FailureClass::Invalid, AppendCode::InvalidJson, &m);
                    }
                }
            }
        } else {
            entries = vec![body.clone()];
        }
        if deferred.is_none()
            && let Some(over) = crate::application::creation::over_record_ceiling(
                state.admission.record_ceiling(),
                &entries,
            )
        {
            let m = format!(
                "record of {over} bytes exceeds the per-record ceiling \
                 (MAX_RECORD_PAYLOAD_BYTES)"
            );
            if producer.is_some() {
                deferred = Some(crate::shard::DeferredErr::BadBody(m));
            } else {
                return fail(FailureClass::Invalid, AppendCode::RecordTooLarge, &m);
            }
        }
    }

    let close_carries_content = !entries.is_empty();
    // A body larger than the ingest bucket's CAPACITY can never be
    // admitted — that is a permanent 413, and it must be decided BEFORE
    // the lifecycle intent, or the collection is left sealing forever
    // owing a record the limiter will always refuse.
    if close && close_carries_content && deferred.is_none() {
        // Bytes AND records: a batched close with more records than the
        // record bucket can ever hold is just as permanently refused as
        // an oversized body, and publishing an intent for it stranded
        // the collection at 429 forever.
        if let Some(kind) =
            crate::usage::permanently_unadmittable(body.len() as u64, entries.len() as u64)
        {
            return fail(
                FailureClass::Invalid,
                AppendCode::PayloadTooLarge,
                &format!("request exceeds the per-stream ingest {kind} capacity"),
            );
        }
    }
    // The raw close publishes its lifecycle intent HERE: after content
    // type, body parsing and every other deterministic refusal, so a
    // request that answers 400 can never leave the collection stuck in
    // Sealing owing a record nobody will write.
    //
    // A close that CARRIES CONTENT is a final-bearing seal, exactly like
    // the product's seal-with-final: the promise is "these records, then
    // closed". Publishing Empty for it meant a crash after the intent
    // let a later close-only finish the seal without them.
    if close && !desc.sealed && !is_owed_final && deferred.is_none() && seal_auth.is_none() {
        let intent = if entries.is_empty() {
            crate::registry::SealIntent::Empty
        } else {
            crate::registry::SealIntent::Final {
                routing_key: product_key.clone().unwrap_or_default(),
                // THE operation id — the same semantic identity the
                // append computes for itself, so a retry recognises its
                // own intent and nothing else can claim it.
                request_hash: this_close_op.clone(),
                final_committed: false,
            }
        };
        match crate::application::lifecycle::begin_sealing_for_close(
            &state.lifecycle,
            &desc.sref(),
            intent,
            &desc.stream_epoch,
        )
        .await
        {
            Ok(g) => {
                if let Some(g) = g {
                    raw_seal_gen = Some(g);
                }
            }
            Err(e) => return fail(FailureClass::Conflict, AppendCode::Sealed, &e.to_string()),
        }
        #[cfg(test)]
        if crate::failpoints::should_stop_after_seal_intent(&name) {
            // The crash boundary: intent durable, records not written.
            return fail(
                FailureClass::Unavailable,
                AppendCode::Failpoint,
                "stopped after the seal intent",
            );
        }
    }

    // Per-shard service limits (usage.rs): token buckets over request rate,
    // record rate, and ingest bytes. Reject-whole with the limit named.
    // Admission also picks THE counters object for this request — one Arc
    // carried through both count sites (here and the committer), so a
    // concurrent eviction/promotion can never split one request's
    // accounting across two objects (review round 4).
    let name_hash = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    let usage_c = if !close_only && deferred.is_none() {
        match crate::usage::admit_append(&name_hash, body.len() as u64, entries.len() as u64) {
            Err(hit) => {
                crate::usage::note_limit_refusal(&hit);
                let l = crate::usage::limits();
                if matches!(hit, crate::usage::LimitHit::Bytes { .. })
                    && body.len() as f64 > l.bytes_per_sec * l.burst_secs
                {
                    // Larger than the bucket's CAPACITY: no retry can
                    // ever admit it — that is 413, not 429.
                    return fail(
                        FailureClass::Invalid,
                        AppendCode::PayloadTooLarge,
                        "request exceeds the per-stream ingest capacity",
                    );
                }
                return fail(
                    FailureClass::Capacity,
                    AppendCode::RateLimited(hit.code()),
                    &hit.message(),
                )
                .map_err(|e| e.retry(hit.retry_ms().div_ceil(1000).max(1)));
            }
            Ok(c) => {
                c.requests
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                c.records
                    .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
                c.bytes_in
                    .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
                c
            }
        }
    } else {
        // Close-only / deferred-error requests skip admission; a single
        // resolve here still beats the old two-site double resolve.
        crate::usage::counters(&name_hash)
    };

    // STANDARDS ISOLATION (audit P0): the singular route IS the
    // default-key Durable Stream — one strict sequence before and after
    // any product split. Routing keys belong to the plural product
    // route; the removed extension is rejected, never honored, so the
    // raw sequence can never absorb another key's records.
    // The product route passes its routing key as an internal
    // PARAMETER; the raw route has none and is therefore always the
    // default-key sequence.
    let routing_key = product_key.clone().unwrap_or_default();
    // ROUTING-V3 §1: an absent key is the empty/default key, and the
    // sole ordering guarantee is per-routing-key order. Resolution
    // picks the owning segment — the implicit single segment for every
    // stream born under the unified model (splits arrive with the
    // sketch scaler), the ordinal segment for legacy per-key layouts.
    let mut seg = desc.resolve_segment(&routing_key);
    if seg.sealed {
        // Mid-transition (a split/merge sealed this segment): refresh
        // the descriptor once and re-resolve; the successor is in the
        // CAS'd map. Still sealed after a fresh read = the transition
        // is mid-publish — tell the client to retry rather than hang.
        // Søren review blocker 2: the refresh must retain the request's
        // project-qualified identity AND its incarnation. Rebuilding
        // state.sref(name) adopted the DEPLOYMENT tenant's same-named
        // descriptor mid-flight, writing this project's ciphertext into
        // the other project's segments. An epoch change means the
        // collection was replaced — fall through to the 503 retry and
        // let the client re-authorize against the new incarnation.
        state.registry.invalidate(&sref);
        match state.registry.get(&sref).await {
            Ok(Some(d2)) if state.alive(&d2) && d2.stream_epoch == desc.stream_epoch => {
                desc = d2;
                seg = desc.resolve_segment(&routing_key);
            }
            _ => {}
        }
        if seg.sealed {
            return fail(
                FailureClass::Unavailable,
                AppendCode::SegmentTransition,
                "segment map transition in progress; retry",
            )
            .map_err(|e| e.retry(1));
        }
    }
    let seg = seg;
    let hash = seg.identity;
    // Per-SEGMENT capacity slot (see the note at the removed per-stream
    // acquisition above).
    let _stream_slot = match state.admission.stream_slot(seg.identity) {
        Ok(s) => s,
        Err(_) => {
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::StreamOverloaded,
                "too many concurrent requests for this stream",
            )
            .retry(1));
        }
    };
    // Predecessor identities for this routing key (nearest-first) —
    // only multi-segment dynamic maps have any.
    let producer_lineage: Vec<[u8; 16]> = match &desc.segments {
        Some(map) if map.segments.len() > 1 => {
            let mut preds: Vec<&crate::segmap::SegmentDesc> = map
                .segments
                .iter()
                .filter(|sg| sg.seg_id != seg.seg_id && sg.contains(seg.point) && !sg.is_live())
                .collect();
            preds.sort_by_key(|sg| std::cmp::Reverse((sg.created_ms, sg.seg_id)));
            preds
                .into_iter()
                .map(|sg| desc.dynamic_segment_identity(sg.seg_id))
                .collect()
        }
        _ => Vec::new(),
    };
    // Unified-scaler sketch feed (spec §5.1): admitted appends only.
    if !close_only && deferred.is_none() {
        let fed: usize = entries.iter().map(|e| e.len()).sum();
        state
            .scaler
            .note_append(&desc, &seg, fed as u64, entries.len() as u64);
    }
    // Usage counters key by the name hash; the absorber keys lag by this
    // engine hash. Record the alias so /v1/debug/usage can join them.
    crate::usage::link_storage(
        crate::crypto::RouteHash::for_stream(&desc.sref()),
        crate::crypto::SegmentHash(hash),
    );
    let kv = command.key_version;
    let subkey = derive_subkey(&key, &epoch, &routing_key, kv);
    state.keys.put(hash, key, epoch);

    let touch = state.watches.append_touch(&desc, &entries);

    let bytes = entries.iter().map(|e| e.len()).sum();
    let _metric_bytes = bytes as u64;
    #[cfg(test)]
    if !close {
        crate::failpoints::pause_append_before_enqueue(&name).await;
    } else {
        crate::failpoints::pause_close_before_enqueue(&name).await;
    }
    // Round-13 transfer point: the parsed entries are the queued
    // representation from here on — the buffering charge ends so the
    // queued/committer accounting (and the shard ledger) own the
    // bytes without a transient double charge.
    drop(command.body_charge.take());
    let (tx, rx) = oneshot::channel();
    let has_entries = !entries.is_empty();
    let appended_records = entries.len();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        // The SEGMENT's physical route, not the parent's: frames, tail
        // state and postings group under the shard that owns them, and
        // for split children that is a different shard than the parent
        // (usage counters stay keyed by the stream name above).
        route: seg.shard_route,
        entries,
        usage: usage_c,
        routing_key,
        key_hash: seg.key_hash.0,
        // Producer state resolves through the key's sealed predecessors
        // after a split (ROUTING-V3 §3.6); single-segment streams carry
        // an empty chain.
        producer_lineage: producer_lineage.clone(),
        key_version: kv,
        subkey,
        ts_hint_ms: command.ts_hint_ms,
        seq: command.sequence.clone(),
        bytes,
        finish: if close {
            crate::shard::AppendFinish::Close
        } else {
            crate::shard::AppendFinish::Open
        },
        producer: producer.clone(),
        deferred_error: deferred,
        sealed_reject_new,
        touch,
        seal_gen: raw_seal_gen,
        // Reserved system streams bill nothing (§8.4) — without this,
        // every `_usage` emission would dirty `_usage` itself and the
        // drainer would feed back forever. BILLING_METER=off exists for
        // A/B isolation in benchmarks only.
        billing: (!crate::billing::is_reserved_stream(&desc.name) && state.meter_enabled).then(
            || {
                std::sync::Arc::new(crate::billing::BillingRef {
                    identity: crate::billing::identity_with_capabilities(
                        &state.auth,
                        &state.deployment,
                        &desc,
                        true,
                    ),
                    segment_id: seg.seg_id,
                })
            },
        ),
        resp: tx,
    };
    let engine = match state
        .shards
        .resolve(&seg.shard_route, crate::shard_directory::Adoption::External)
        .await
    {
        Ok(e) => e,
        Err(e) => return Err(AppendFailure::from_resolve(e)),
    };
    // Round-13: bind this SEGMENT's durable-write pressure attribution
    // to the project's admission entry (once per resident handle
    // incarnation; seeded from the applied tail's exact
    // unabsorbed_bytes — never from zero when durable debt exists).
    if let Some(adm) = state.quotas.pressure_handle(sref.project_id())
        && let Ok(h) = engine.stream_handle(hash).await
    {
        h.bind_pressure(adm);
    }
    // R25-C: THE maintenance admission point — one, in the shared append
    // core, after `engine_for` resolved ownership. A non-owner already
    // received its Streams-Replay-To above and never reaches this, so a
    // stale local latch cannot answer for someone else's backlog. Both
    // public append surfaces converge here (raw /v1/stream/{*name}
    // including hierarchical names, product append and appendMany, every
    // routing key, split children on their own shard routes), so there
    // is no second copy of the route grammar to drift.
    //
    // Skips: close-only operations carry no entries and must stay
    // admitted (an operator closing a stream is REDUCING future work),
    // and reserved system streams stay admitted because overload
    // recovery must not deadlock on its own system-of-record writes.
    if !close_only && has_entries && !crate::billing::is_reserved_stream(&name) {
        let limits = state.maintenance_limits();
        if let Some(cause) = state.admission.admit_maintenance(&engine, &limits) {
            state.admission.note_maintenance_shed();
            return fail(
                FailureClass::Unavailable,
                AppendCode::MaintenanceBackpressure,
                &format!("{}; retry after maintenance catches up", cause.as_str()),
            )
            .map_err(|e| e.retry(5));
        }
    }
    // Wedge shed: if the shard's durability pipeline is stalled — either
    // the commit db.write is blocked (unflushed-full) or committed groups
    // have waited on the durable watermark beyond the threshold (WAL flush
    // stalled behind L0-full) — reject with a retryable 429 instead of
    // queueing. Without this, appends hang until the platform front door
    // kills them at ~30 s (8-minute wedge, 2026-07-21; detector missed the
    // stale-durability mode on 2026-07-22 when it watched db.write only).
    // 5 s: healthy durable waits under load peak ~1.5 s; a real wedge
    // climbs to 30 s+, so 5 s discriminates cleanly without false sheds.
    let blocked = engine.wedge_ms();
    if blocked > 5_000 {
        state.admission.note_wedge_shed();
        return fail(
            FailureClass::Capacity,
            AppendCode::EngineBackpressure,
            "commit pipeline blocked (compaction lag); retry",
        )
        .map_err(|e| e.retry(2));
    }
    if engine.try_enqueue(req).is_err() {
        return fail(
            FailureClass::Capacity,
            AppendCode::Overloaded,
            "append queue full",
        );
    }
    let outcome = match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
        Ok(Ok(o)) => o,
        _ => {
            return fail(
                FailureClass::Timeout,
                AppendCode::AppendTimeout,
                "append timed out; outcome unknown",
            );
        }
    };

    if outcome.is_ok() {
        state.creation.touch_ttl(&desc);
    }
    if close && seal_auth.is_none() {
        crate::application::lifecycle::complete_raw_close(
            &state.lifecycle,
            &desc,
            crate::application::lifecycle::RawClose {
                operation: &this_close_op,
                generation: raw_seal_gen,
                carries_content: close_carries_content,
                resumes_owed_final: is_owed_final,
            },
            &outcome,
        )
        .await
        .map_err(|error| {
            AppendFailure::new(
                FailureClass::Unavailable,
                AppendCode::SealIncomplete,
                error.to_string(),
            )
        })?;
    }
    match outcome {
        Ok(ack) => Ok(crate::application::append::AppendOutcome {
            seg_id: seg.seg_id,
            materialized: desc.segments.is_some(),
            next_offset: ack.next_offset,
            last_offset: ack.last_offset,
            duplicate: ack.duplicate,
            closed: ack.closed,
            producer: ack.producer.filter(|_| !synthetic_producer),
            appended_records,
        }),
        Err(error) => Err(AppendFailure::from_commit(
            seg.seg_id,
            desc.segments.is_some(),
            error,
        )),
    }
}

pub(crate) fn parse_producer(
    id: Option<String>,
    epoch: Option<String>,
    seq: Option<String>,
) -> Result<Option<crate::shard::ProducerReq>, String> {
    match (id, epoch, seq) {
        (None, None, None) => Ok(None),
        (Some(id), Some(e), Some(s)) => {
            if id.is_empty() {
                return Err("Producer-Id must not be empty".into());
            }
            // The seal machinery synthesizes producer identities for
            // records a client never coordinates itself. They share the
            // durable producer keyspace, so the wire must not be able to
            // name one: a caller who pre-created `prisma.seal.<op>` at
            // sequence 0 would make a later seal's final append look
            // like a duplicate — the seal would then "complete" without
            // ever writing its record.
            if id.starts_with(crate::shard::INTERNAL_PRODUCER_PREFIX) {
                return Err(format!(
                    "Producer-Id must not begin with '{}' (reserved)",
                    crate::shard::INTERNAL_PRODUCER_PREFIX
                ));
            }
            let epoch = parse_uint(&e).ok_or("invalid Producer-Epoch")?;
            let seq = parse_uint(&s).ok_or("invalid Producer-Seq")?;
            Ok(Some(crate::shard::ProducerReq {
                id,
                epoch,
                seq,
                request_hash: None,
            }))
        }
        _ => Err("Producer-Id, Producer-Epoch and Producer-Seq must be sent together".into()),
    }
}

fn parse_uint(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        None
    } else {
        value.parse().ok()
    }
}

pub(crate) fn product_request_hash(
    batch: bool,
    routing_key: &str,
    content_type: &str,
    body: &[u8],
    seal: bool,
) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let mut hx = Sha256::new();
    hx.update(if batch {
        b"\x01batch\x00".as_slice()
    } else {
        b"\x01single\x00".as_slice()
    });
    hx.update((routing_key.len() as u64).to_le_bytes());
    hx.update(routing_key.as_bytes());
    hx.update(content_type.as_bytes());
    hx.update([u8::from(seal)]); // seal flag (spec Stage 5 §7)
    hx.update(&body);
    hx.finalize()[..16].try_into().unwrap()
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    #[test]
    fn rejection_policy_uses_variants_not_display_text() {
        let mut gap = AppendFailure::from_commit(
            0,
            false,
            AppendErr::ProducerGap {
                expected: 1,
                received: 2,
            },
        );
        assert!(!gap.definitively_rejected());
        gap.message = "permanent forbidden stale request".into();
        assert!(!gap.definitively_rejected());
        let mut stale =
            AppendFailure::from_commit(0, false, AppendErr::ProducerStale { current_epoch: 3 });
        stale.message = "retry later while temporarily unavailable".into();
        assert!(stale.definitively_rejected());
    }
    #[test]
    fn producer_parser_rejects_partial_reserved_and_noncanonical_numbers() {
        for (id, epoch, seq) in [
            (Some("p"), None, Some("0")),
            (Some("p"), Some("+1"), Some("0")),
            (Some("p"), Some("1"), Some("-0")),
            (
                Some(crate::shard::INTERNAL_PRODUCER_PREFIX),
                Some("1"),
                Some("0"),
            ),
        ] {
            assert!(
                parse_producer(
                    id.map(str::to_string),
                    epoch.map(str::to_string),
                    seq.map(str::to_string)
                )
                .is_err()
            );
        }
        let accepted = parse_producer(Some("p".into()), Some("001".into()), Some("0".into()))
            .unwrap()
            .unwrap();
        assert_eq!(accepted.epoch, 1);
    }
}
