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
    TtlRenewal,
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
    #[cfg(test)]
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
    ShardOpen,
    RateLimited(&'static str),
}
impl AppendCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TtlRenewal => "ttl_renewal_unavailable",
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
            #[cfg(test)]
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
            Self::ShardOpen => "shard_open",
            Self::RateLimited(code) => code,
        }
    }
}

#[derive(Debug)]
enum AppendConflict {
    Closed {
        segment: u32,
        next: u64,
        materialized: bool,
    },
    ProducerGap {
        expected: u64,
        received: u64,
    },
    ProducerEpoch(u64),
}

#[derive(Debug)]
pub(crate) struct AppendFailure {
    pub(crate) class: FailureClass,
    pub(crate) code: AppendCode,
    pub(crate) message: String,
    pub(crate) retry_after: Option<u64>,
    pub(crate) owner: Option<String>,
    conflict: Option<Box<AppendConflict>>,
}
impl AppendFailure {
    pub(crate) fn new(class: FailureClass, code: AppendCode, message: impl Into<String>) -> Self {
        Self {
            class,
            code,
            message: message.into(),
            retry_after: None,
            owner: None,
            conflict: None,
        }
    }
    pub(crate) fn closed_at(&self) -> Option<(u32, u64, bool)> {
        match self.conflict.as_deref() {
            Some(AppendConflict::Closed {
                segment,
                next,
                materialized,
            }) => Some((*segment, *next, *materialized)),
            _ => None,
        }
    }
    pub(crate) fn expected(&self) -> Option<u64> {
        match self.conflict.as_deref() {
            Some(AppendConflict::ProducerGap { expected, .. }) => Some(*expected),
            _ => None,
        }
    }
    pub(crate) fn received(&self) -> Option<u64> {
        match self.conflict.as_deref() {
            Some(AppendConflict::ProducerGap { received, .. }) => Some(*received),
            _ => None,
        }
    }
    pub(crate) fn producer_epoch(&self) -> Option<u64> {
        match self.conflict.as_deref() {
            Some(AppendConflict::ProducerEpoch(epoch)) => Some(*epoch),
            _ => None,
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
                e.conflict = Some(Box::new(AppendConflict::Closed {
                    segment,
                    next: next_offset,
                    materialized,
                }));
                e
            }
            AppendErr::ProducerGap { expected, received } => {
                let mut e = Self::new(Conflict, AppendCode::ProducerGap, "sequence gap");
                e.conflict = Some(Box::new(AppendConflict::ProducerGap { expected, received }));
                e
            }
            AppendErr::ProducerStale { current_epoch } => {
                let mut e = Self::new(Denied, AppendCode::ProducerStale, "stale epoch");
                e.conflict = Some(Box::new(AppendConflict::ProducerEpoch(current_epoch)));
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
pub(crate) fn fail<T>(
    class: FailureClass,
    code: AppendCode,
    message: &str,
) -> Result<T, AppendFailure> {
    Err(AppendFailure::new(class, code, message))
}

use crate::application::lifecycle::SealAuthz;
use crate::crypto::StreamKey;
use crate::registry::StreamDesc;
use bytes::Bytes;

pub(crate) enum AppendKey {
    Missing,
    Invalid,
    Provided(StreamKey),
}
pub(crate) struct AuthorizedAppend {
    pub(super) descriptor: StreamDesc,
    pub(super) key: StreamKey,
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
    hx.update(body);
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
