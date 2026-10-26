//! Native queue workflows (docs/QUEUES-DESIGN.md §4, §6, §7, §11). A queue
//! is one stream of the queue kind (`native_queue::QUEUE_CONTENT_TYPE`),
//! created through the product creation path, whose queue rows and
//! one-message pages the shard committer owns (`NqOp`). These workflows
//! resolve the queue's stream and engine, check the stream key, seal and
//! open message envelopes, and drive the committer's operations; the
//! product adapter (`product::queues`) only parses and renders.
mod admin;
mod deletion;
mod delivery;
mod envelope;
mod wire;

pub(crate) use admin::{PageAt, View};
pub(crate) use deletion::QueueDeletion;
pub(crate) use envelope::Message;
pub(crate) use wire::{
    ClaimInput, IdsInput, RedriveInput, SendBatchInput, SendInput, SettleInput, WakeupInput,
};

use std::sync::Arc;

use crate::crypto::StreamKey;
use crate::native_queue::{NqOp, NqOut, NqRefusal, QueueConfig, is_queue_kind};
use crate::registry::StreamDesc;
use crate::tenant::TenantStreamRef;

/// The capabilities queue workflows use. Built per request by the
/// composition (`AppState::queue_service`).
#[derive(Clone)]
pub(crate) struct QueueService {
    pub(crate) registry: Arc<crate::registry::Registry>,
    pub(crate) shards: crate::shard_directory::ShardDirectory,
    pub(crate) creation: Arc<super::creation::CreationService>,
    pub(crate) work: Arc<super::request_work::RequestWork>,
    pub(crate) touch: Arc<crate::touch::TouchRegistry>,
    /// The root of wake-up secrets (`native_queue::wakeup`), when the
    /// deployment holds one.
    pub(crate) wake_root: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Invalid,
    Denied,
    Missing,
    Conflict,
    TooLarge,
    Exhausted,
    Unavailable,
    Internal,
}

/// A refused queue request: its class (the adapter's status), its stable
/// code, and whether a retry can succeed.
#[derive(Debug)]
pub(crate) struct QueueFailure {
    pub(crate) class: FailureClass,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) retryable: bool,
    /// The owning instance when another one owns the queue's shard.
    pub(crate) owner: Option<String>,
    /// The seconds a client waits before its retry (`Retry-After`), when
    /// the refusal states one.
    pub(crate) retry_after: Option<u64>,
}

pub(crate) fn fail(
    class: FailureClass,
    code: &'static str,
    message: impl Into<String>,
) -> QueueFailure {
    QueueFailure {
        class,
        code,
        message: message.into(),
        retryable: matches!(
            class,
            FailureClass::Exhausted | FailureClass::Unavailable | FailureClass::Internal
        ),
        owner: None,
        retry_after: None,
    }
}

impl From<NqRefusal> for QueueFailure {
    fn from(refusal: NqRefusal) -> Self {
        use FailureClass as C;
        match refusal {
            NqRefusal::NotFound => not_found(),
            NqRefusal::Deleting => {
                fail(C::Conflict, "queue_deleting", "the queue is being deleted")
            }
            NqRefusal::ConfigConflict => fail(
                C::Conflict,
                "queue_config_conflict",
                "the queue exists with different fixed configuration",
            ),
            NqRefusal::InvalidConfig(m) => fail(C::Invalid, "invalid_config", m),
            NqRefusal::QueueFull => fail(
                C::Exhausted,
                "queue_full",
                "the queue holds its maxQueuedBytes or maxQueuedMessages; retry",
            ),
            NqRefusal::DedupFull => fail(
                C::Exhausted,
                "dedup_full",
                "the queue holds maxDedupEntries dedup keys; retry",
            ),
            NqRefusal::DeliverAtBeyondRetention => fail(
                C::Invalid,
                "deliver_at_beyond_retention",
                "deliverAt is later than the queue's retention from now",
            ),
            NqRefusal::Reclaimed => fail(C::Missing, "reclaimed", "the message was reclaimed"),
            NqRefusal::Busy => QueueFailure {
                retry_after: Some(1),
                ..fail(
                    C::Unavailable,
                    "shard_busy",
                    "the queue's shard is at its committer capacity; nothing was written; retry",
                )
            },
            NqRefusal::Internal(m) => fail(C::Unavailable, "queue_unavailable", m),
        }
    }
}

pub(crate) fn not_found() -> QueueFailure {
    fail(FailureClass::Missing, "queue_not_found", "queue not found")
}

pub(crate) fn message_not_found() -> QueueFailure {
    fail(
        FailureClass::Missing,
        "message_not_found",
        "no such message in this queue",
    )
}

fn unexpected(out: &NqOut) -> QueueFailure {
    fail(
        FailureClass::Internal,
        "internal",
        format!("unexpected queue reply {out:?}"),
    )
}

/// A creation refused by the product creation path: a queue failure, or
/// the project quota refusal the adapter renders as it renders a stream's.
pub(crate) enum CreateFailure {
    Queue(QueueFailure),
    Quota(crate::quota::QuotaRefusal),
}

impl From<QueueFailure> for CreateFailure {
    fn from(f: QueueFailure) -> Self {
        Self::Queue(f)
    }
}

/// One live queue: its descriptor, its storage hash (the engine identity
/// its rows and pages live under) and the engine that owns it.
pub(crate) struct QueueTarget {
    pub(crate) desc: StreamDesc,
    pub(crate) hash: [u8; 16],
    engine: Arc<crate::shard::ShardEngine>,
}

impl QueueTarget {
    pub(crate) fn id(&self, offset: u64) -> String {
        crate::native_queue::keys::message_id(&self.hash, offset)
    }
    /// The offsets of `ids` that name messages of this incarnation;
    /// anything else names no message here.
    pub(crate) fn offsets(&self, ids: &[String]) -> Vec<u64> {
        ids.iter()
            .filter_map(|id| crate::native_queue::keys::parse_message_id(&self.hash, id))
            .collect()
    }
}

/// The stream key of one request, checked against the queue's descriptor,
/// and the page subkey its messages are sealed under (routing key "",
/// key version 0, as every product append).
pub(crate) struct QueueKey {
    key: StreamKey,
    epoch: [u8; 16],
    subkey: [u8; 32],
}

/// The request's stream key, decoded: `missing_key` or `invalid_key` (400).
pub(crate) fn decode_key(raw: Option<&str>) -> Result<StreamKey, QueueFailure> {
    let Some(raw) = raw else {
        return Err(fail(
            FailureClass::Invalid,
            "missing_key",
            "Streams-Encryption-Key required",
        ));
    };
    StreamKey::from_b64(raw).map_err(|m| fail(FailureClass::Invalid, "invalid_key", m))
}

impl QueueService {
    /// The live queue `sref` names: 404 `queue_not_found` for a missing,
    /// deleted or non-queue stream.
    pub(crate) async fn target(&self, sref: &TenantStreamRef) -> Result<QueueTarget, QueueFailure> {
        let desc = match self.registry.get(sref).await {
            Ok(Some(d)) if super::creation::desc_alive(&d) && is_queue_kind(&d.content_type) => d,
            Ok(_) => return Err(not_found()),
            Err(e) => {
                return Err(fail(
                    FailureClass::Unavailable,
                    "registry_unavailable",
                    e.to_string(),
                ));
            }
        };
        if desc.init.is_some() {
            return Err(fail(
                FailureClass::Unavailable,
                "creating",
                "queue is still being created; retry",
            ));
        }
        let seg = desc.resolve_segment("");
        let engine = self
            .shards
            .resolve(&seg.shard_route, crate::shard_directory::Adoption::External)
            .await
            .map_err(ownership)?;
        Ok(QueueTarget {
            hash: seg.identity,
            desc,
            engine,
        })
    }

    /// Run a request's operation in the queue's committer: refused at once
    /// (`NqRefusal::Busy`) when the committer's queue is full.
    pub(crate) async fn op(&self, t: &QueueTarget, op: NqOp) -> Result<NqOut, QueueFailure> {
        Ok(t.engine.try_submit_native_queue(t.hash, op).await?)
    }

    /// The request's key for `t`: 403 `wrong_key` unless it is the stream's.
    pub(crate) fn key(t: &QueueTarget, raw: Option<&str>) -> Result<QueueKey, QueueFailure> {
        let key = decode_key(raw)?;
        let epoch = t.desc.epoch();
        if key.fingerprint(&epoch) != t.desc.key_fingerprint {
            return Err(fail(
                FailureClass::Denied,
                "wrong_key",
                "encryption key mismatch",
            ));
        }
        let subkey = crate::crypto::derive_subkey(&key, &epoch, "", 0);
        Ok(QueueKey { key, epoch, subkey })
    }

    /// The queue's state row: configuration, counts and counters.
    pub(crate) async fn state(
        &self,
        t: &QueueTarget,
    ) -> Result<crate::native_queue::rows::StateRow, QueueFailure> {
        match self.op(t, NqOp::Get).await? {
            NqOut::Stats(row) => Ok(*row),
            other => Err(unexpected(&other)),
        }
    }

    /// Create the queue's stream (the product creation path, the queue
    /// kind) and then the queue. Idempotent: an existing queue with the
    /// same fixed fields answers its stored configuration, `created` false.
    pub(crate) async fn create(
        &self,
        sref: &TenantStreamRef,
        key: StreamKey,
        cfg: QueueConfig,
        quotas: Option<&crate::project_policy::ProjectQuotas>,
    ) -> Result<(bool, QueueConfig), CreateFailure> {
        cfg.validate()
            .map_err(|m| fail(FailureClass::Invalid, "invalid_config", m))?;
        let stream = super::creation::ProductCreateConfig {
            content_type: crate::native_queue::QUEUE_CONTENT_TYPE.to_string(),
            ttl_secs: None,
            expires_at_ms: None,
            watches: Vec::new(),
        };
        match self
            .creation
            .create_product(sref.clone(), key, stream, quotas)
            .await
        {
            Ok(_) => {}
            Err(super::creation::ProductCreateError::Quota(r)) => {
                return Err(CreateFailure::Quota(r));
            }
            Err(super::creation::ProductCreateError::Creation(e)) => {
                return Err(creation_failure(e).into());
            }
        }
        let t = self.target(sref).await?;
        match self.op(&t, NqOp::Create { cfg }).await? {
            NqOut::Config { cfg, created } => Ok((created, cfg)),
            other => Err(unexpected(&other).into()),
        }
    }

    /// One page of the project's queue names, from after `after`: walks at
    /// most eight catalog pages, so `next` continues a long catalog.
    pub(crate) async fn list(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), QueueFailure> {
        let mut names = Vec::new();
        let mut after = after;
        for _ in 0..8 {
            let page = self
                .registry
                .list_page(project, after.as_deref(), 512)
                .await
                .map_err(|e| {
                    fail(
                        FailureClass::Unavailable,
                        "catalog_unavailable",
                        e.to_string(),
                    )
                })?;
            names.extend(
                page.streams
                    .iter()
                    .filter(|d| super::creation::desc_alive(d) && is_queue_kind(&d.content_type))
                    .map(|d| d.name.clone()),
            );
            if page.exhausted || page.next_after.is_none() {
                return Ok((names, None));
            }
            after = page.next_after;
        }
        Ok((names, after))
    }
}

/// A creation failure as a queue refusal: a stream of another kind under
/// the name is the queue's configuration conflict.
fn creation_failure(e: super::creation::CreationError) -> QueueFailure {
    use super::creation::CreationFailure as F;
    use FailureClass as C;
    let class = match e.kind {
        F::Invalid => C::Invalid,
        F::Conflict | F::Gone => C::Conflict,
        F::Missing => C::Missing,
        F::WrongKey => C::Denied,
        F::Storage => C::Internal,
        F::TooLarge => C::TooLarge,
        F::Overloaded => C::Exhausted,
        F::Ambiguous | F::Opening => C::Unavailable,
    };
    let code = match e.code {
        "config_mismatch" => "queue_config_conflict",
        code => code,
    };
    let mut f = fail(class, code, e.message);
    f.owner = e.owner;
    f
}

fn ownership(error: crate::shard_directory::ResolveError) -> QueueFailure {
    use crate::shard_directory::ResolveError;
    match error {
        ResolveError::NotOwner { owner, .. } => {
            let mut f = fail(
                FailureClass::Conflict,
                "not_stream_owner",
                "another instance owns the queue's shard; retry through the router",
            );
            f.retryable = true;
            f.owner = Some(owner);
            f
        }
        ResolveError::Opening { code, .. } => fail(
            FailureClass::Unavailable,
            code,
            "shard not currently serving here; retry",
        ),
        ResolveError::OpenFailed { prefix, error } => fail(
            FailureClass::Internal,
            "shard_open",
            format!("open shard {prefix}: {error}"),
        ),
    }
}
