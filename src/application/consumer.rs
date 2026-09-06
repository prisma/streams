//! Consumer workflows. Each operation holds a project/stream incarnation and
//! consumer generation before touching a queue. Protocol adapters only parse and render.
mod deletion;
mod delivery;
use crate::application::read_remote::InternalTarget;
use crate::crypto::StreamKey;
use crate::registry::StreamDesc;
pub(crate) use deletion::delete;
pub(crate) use delivery::{pull, settle};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ConsumerService {
    pub(crate) registry: Arc<crate::registry::Registry>,
    pub(crate) shards: crate::shard_directory::ShardDirectory,
    pub(crate) peer: crate::peer::PeerClient,
    pub(crate) keys: Arc<crate::history::KeyCache>,
    pub(crate) append: Arc<super::append::AppendService>,
}
impl ConsumerService {
    async fn engine_for(
        &self,
        route: &[u8; 16],
    ) -> Result<Arc<crate::shard::ShardEngine>, ConsumerFailure> {
        self.shards
            .resolve(route, crate::shard_directory::Adoption::External)
            .await
            .map_err(ConsumerFailure::ownership)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FailureClass {
    Invalid,
    Denied,
    Missing,
    Conflict,
    Unavailable,
    Internal,
}
#[derive(Debug)]
pub(crate) struct ConsumerFailure {
    pub(crate) class: FailureClass,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) details: Option<serde_json::Value>,
    pub(crate) retryable: bool,
    pub(crate) version: Option<String>,
    pub(crate) owner: Option<String>,
    pub(crate) auth: Option<crate::auth::AuthError>,
    pub(crate) deletion_debt: Option<DeletionDebt>,
}
fn failure(
    class: FailureClass,
    code: &'static str,
    message: &str,
    details: Option<serde_json::Value>,
    retryable: bool,
) -> ConsumerFailure {
    ConsumerFailure {
        class,
        code,
        message: message.to_string(),
        details,
        retryable,
        version: None,
        owner: None,
        auth: None,
        deletion_debt: None,
    }
}
impl ConsumerFailure {
    fn ownership(error: crate::shard_directory::ResolveError) -> Self {
        use crate::shard_directory::ResolveError;
        match error {
            ResolveError::NotOwner { prefix: _, owner } => {
                let mut e = failure(
                    FailureClass::Conflict,
                    "not_stream_owner",
                    "another instance owns the target segment; retry through the router",
                    None,
                    true,
                );
                e.owner = Some(owner);
                e
            }
            ResolveError::Opening { code, .. } => failure(
                FailureClass::Unavailable,
                code,
                "shard not currently serving here; retry",
                None,
                true,
            ),
            ResolveError::OpenFailed { prefix, error } => failure(
                FailureClass::Internal,
                "shard_open",
                &format!("open shard {prefix}: {error}"),
                None,
                true,
            ),
        }
    }
    fn authorization(error: crate::auth::AuthError) -> Self {
        let mut e = failure(
            FailureClass::Denied,
            "unauthorized",
            "consumer authorization refused",
            None,
            false,
        );
        e.auth = Some(error);
        e
    }
}
pub(crate) enum ConsumerAccess<'a> {
    Account(&'a crate::auth::RequestPrincipal),
    Deployment,
}
impl ConsumerAccess<'_> {
    pub(crate) fn require(
        &self,
        stream: &crate::tenant::TenantStreamRef,
        scope: crate::tenant::Scope,
    ) -> Result<(), ConsumerFailure> {
        if let Self::Account(p) = self {
            if &p.project_id != stream.project_id() {
                return Err(failure(
                    FailureClass::Denied,
                    "unauthorized",
                    "consumer project mismatch",
                    None,
                    false,
                ));
            }
            p.require(scope).map_err(ConsumerFailure::authorization)?;
            p.require_stream(stream.name().as_str())
                .map_err(ConsumerFailure::authorization)?;
        }
        Ok(())
    }
}
/// Constructed only after key verification and authorization of the named source.
pub(crate) struct AuthorizedStreamContext {
    service: Arc<ConsumerService>,
    desc: StreamDesc,
    key: StreamKey,
    epoch: [u8; 16],
    consumer: String,
}
/// Queue mutations bind the active record generation as well as stream incarnation.
pub(crate) struct AuthorizedConsumerContext {
    stream: AuthorizedStreamContext,
    record: crate::queue::ConsumerRecord,
}
impl ConsumerService {
    pub(crate) async fn authorize(
        self: &Arc<Self>,
        sref: &crate::tenant::TenantStreamRef,
        consumer: String,
        key_b64: &str,
        access: &ConsumerAccess<'_>,
        scope: crate::tenant::Scope,
    ) -> Result<AuthorizedStreamContext, ConsumerFailure> {
        access.require(sref, scope)?;
        if super::names::valid_consumer_name(&consumer).is_none() {
            return Err(failure(
                FailureClass::Invalid,
                "invalid_consumer",
                "invalid consumer name",
                None,
                false,
            ));
        }
        let desc = match self.registry.get(sref).await {
            Ok(Some(d)) if desc_alive(&d) => d,
            Ok(_) => {
                return Err(failure(
                    FailureClass::Missing,
                    "not_found",
                    "stream not found",
                    None,
                    false,
                ));
            }
            Err(e) => {
                return Err(failure(
                    FailureClass::Internal,
                    "internal",
                    &e.to_string(),
                    None,
                    true,
                ));
            }
        };
        if initializing(&desc) {
            return Err(failure(
                FailureClass::Unavailable,
                "creating",
                "stream is still being created; retry",
                None,
                true,
            ));
        }
        let (key, epoch) = match check_key(Some(key_b64), &desc) {
            KeyCheck::Ok(k, e) => (k, e),
            _ => {
                return Err(failure(
                    FailureClass::Denied,
                    "wrong_key",
                    "encryption key mismatch",
                    None,
                    false,
                ));
            }
        };
        Ok(AuthorizedStreamContext {
            service: self.clone(),
            desc,
            key,
            epoch,
            consumer,
        })
    }
    pub(crate) async fn active(
        self: &Arc<Self>,
        stream: AuthorizedStreamContext,
    ) -> Result<AuthorizedConsumerContext, ConsumerFailure> {
        let record = load_consumer_record(&stream.service, &stream.desc, &stream.consumer).await?;
        Ok(AuthorizedConsumerContext { stream, record })
    }
}
use super::creation::desc_alive;
fn initializing(desc: &StreamDesc) -> bool {
    desc.init.is_some()
}
enum KeyCheck {
    Ok(StreamKey, [u8; 16]),
    Wrong,
}
fn check_key(raw: Option<&str>, desc: &StreamDesc) -> KeyCheck {
    let Some(key) = raw.and_then(|raw| StreamKey::from_b64(raw).ok()) else {
        return KeyCheck::Wrong;
    };
    let epoch = desc.epoch();
    if key.fingerprint(&epoch) == desc.key_fingerprint {
        KeyCheck::Ok(key, epoch)
    } else {
        KeyCheck::Wrong
    }
}
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ConfigInput {
    pub(crate) visibility_timeout_ms: Option<u32>,
    pub(crate) max_attempts: Option<u32>,
    pub(crate) dead_letter_stream: Option<String>,
    pub(crate) max_batch_records: Option<u16>,
}
pub(crate) struct ConfigOutcome {
    pub(crate) created: bool,
    pub(crate) record: crate::queue::ConsumerRecord,
    pub(crate) epoch: [u8; 16],
}
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PullInput {
    pub(crate) max: Option<usize>,
    pub(crate) wait_ms: Option<u64>,
    pub(crate) visibility_ms: Option<u64>,
}
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeliveryMessage {
    pub(crate) id: String,
    pub(crate) routing_key: String,
    pub(crate) attempts: u32,
    pub(crate) lease_token: String,
    pub(crate) value: serde_json::Value,
}
pub(crate) struct PullOutcome {
    pub(crate) messages: Vec<DeliveryMessage>,
    pub(crate) backlog: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) descriptor: StreamDesc,
}
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SettleItem {
    pub(crate) lease_token: String,
    #[serde(default)]
    pub(crate) delay_ms: Option<u64>,
    #[serde(default)]
    pub(crate) visibility_ms: Option<u64>,
}
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SettleInput {
    #[serde(default)]
    pub(crate) acks: Vec<SettleItem>,
    #[serde(default)]
    pub(crate) retries: Vec<SettleItem>,
    #[serde(default)]
    pub(crate) extends: Vec<SettleItem>,
}
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SettleOutcome {
    acked: usize,
    retried: usize,
    extended: usize,
    dlq: usize,
    stale: usize,
    backlog: u64,
    dlq_blocked: usize,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DeleteOutcome {
    TargetGone,
    Cleaned,
}
/// Durable parent lifecycle remains Deleting while this incarnation has cleanup debt.
/// A retry resumes the same target; it never rebinds to a replacement by name.
#[derive(Debug, Clone)]
pub(crate) struct DeletionDebt {
    pub(crate) stream: crate::tenant::TenantStreamRef,
    pub(crate) epoch: [u8; 16],
    pub(crate) consumer: String,
    pub(crate) generation: u64,
}
struct AuthorizedDeletionContext {
    service: Arc<ConsumerService>,
    descriptor: StreamDesc,
    target: DeletionDebt,
    lifecycle: crate::queue::ConsumerLifecycle,
}
const CONSUMER_DELETE_STEP_ROWS: usize = 4096;
const CONSUMER_DELETE_STEP_BYTES: usize = 1 << 20;
const CONSUMER_DELETE_REQUEST_STEPS: u32 = 512;
const CONSUMER_DELETE_SEGMENT_CONCURRENCY: usize = 8;

pub(crate) fn consumer_version_token(epoch: &[u8; 16], generation: u64) -> String {
    use base64::Engine;
    let mut v = [0u8; 24];
    v[..16].copy_from_slice(epoch);
    v[16..].copy_from_slice(&generation.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v)
}

pub(crate) fn parse_consumer_version(tok: &str) -> Option<([u8; 16], u64)> {
    use base64::Engine;
    let v = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tok.as_bytes())
        .ok()?;
    if v.len() != 24 {
        return None;
    }
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(&v[..16]);
    let generation = u64::from_be_bytes(v[16..].try_into().ok()?);
    Some((epoch, generation))
}

fn consumer_segments(desc: &StreamDesc) -> Vec<(u32, [u8; 16], [u8; 16], Option<u64>)> {
    match &desc.segments {
        Some(map) if !map.segments.is_empty() => {
            let mut v: Vec<_> = map.segments.iter().collect();
            v.sort_by_key(|sg| (sg.created_ms, sg.seg_id));
            v.iter()
                .map(|sg| {
                    (
                        sg.seg_id,
                        desc.dynamic_segment_identity(sg.seg_id),
                        desc.segment_route(sg),
                        sg.sealed_next_offset,
                    )
                })
                .collect()
        }
        _ => {
            let ro = desc.resolve_segment("");
            vec![(ro.seg_id, ro.identity, ro.shard_route, None)]
        }
    }
}

async fn consumer_config_op(
    state: &Arc<ConsumerService>,
    desc: &StreamDesc,
    op: crate::queue::QueueOp,
) -> Result<crate::queue::QueueOut, ConsumerFailure> {
    let route = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    let engine = state.engine_for(&route).await?;
    engine
        .submit_queue(desc.storage_hash(), op)
        .await
        .map_err(|m| failure(FailureClass::Internal, "internal", &m, None, true))
}

async fn load_consumer_record(
    state: &Arc<ConsumerService>,
    desc: &StreamDesc,
    cname: &str,
) -> Result<crate::queue::ConsumerRecord, ConsumerFailure> {
    match consumer_config_op(
        state,
        desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.to_string(),
        },
    )
    .await?
    {
        crate::queue::QueueOut::Config { rec: Some(r), .. }
            if r.state == crate::queue::ConsumerLifecycle::Active =>
        {
            Ok(r)
        }
        crate::queue::QueueOut::Config { rec: Some(r), .. }
            if r.state == crate::queue::ConsumerLifecycle::Deleting =>
        {
            Err(failure(
                FailureClass::Conflict,
                "consumer_deleting",
                "this consumer is being deleted",
                None,
                false,
            ))
        }
        _ => Err(failure(
            FailureClass::Missing,
            "unknown_consumer",
            "no such consumer; create it first",
            None,
            false,
        )),
    }
}

pub(crate) async fn put(
    context: AuthorizedStreamContext,
    doc: ConfigInput,
    access: ConsumerAccess<'_>,
) -> Result<ConfigOutcome, ConsumerFailure> {
    let AuthorizedStreamContext {
        service: state,
        desc,
        key,
        epoch,
        consumer: cname,
    } = context;
    let name = desc.name.clone();
    let mut cfg = crate::queue::ConsumerConfig::default();
    if let Some(v) = doc.visibility_timeout_ms {
        cfg.visibility_timeout_ms = v.clamp(1_000, 12 * 3600 * 1000);
    }
    if let Some(v) = doc.max_attempts {
        cfg.max_attempts = v.clamp(1, 1_000);
    }
    if let Some(v) = doc.max_batch_records {
        cfg.max_batch_records = v.clamp(1, 1_000);
    }
    if let Some(d) = doc.dead_letter_stream {
        // §6.1 review item 4 — the compound rule, each leg independent:
        // the GATE already authorized the source consumer
        // (consumers.configure + prefix over the source); wiring a
        // dead-letter target additionally takes dlq.configure AND the
        // credential's prefix grant over the DESTINATION stream —
        // same-project lookup alone is not authorization.
        if let ConsumerAccess::Account(p) = access {
            if let Err(e) = p.require(crate::tenant::Scope::DlqConfigure) {
                return Err(ConsumerFailure::authorization(e));
            }
            if let Err(e) = p.require_stream(&d) {
                return Err(ConsumerFailure::authorization(e));
            }
        }
        if super::names::ProductStreamName::try_from(d.as_str()).is_err() {
            return Err(failure(
                FailureClass::Invalid,
                "invalid_config",
                "deadLetterStream is not a valid stream name",
                None,
                false,
            ));
        }
        // DLQ capability model. A dead-letter record is written with the
        // SOURCE stream's encryption key, because that is the only key
        // the delivery path holds — there is no key-exchange step and
        // the server never stores stream keys. So the target must be a
        // real, writable collection under THAT key, and configuring the
        // link requires presenting a key valid for both. Validating it
        // here turns a silent, permanent delivery block (the poisoned
        // key stays leased forever while every DLQ append 403s) into an
        // error the caller sees while it can still fix it.
        if d == name {
            return Err(failure(
                FailureClass::Invalid,
                "invalid_config",
                "deadLetterStream must not be the source collection",
                None,
                false,
            ));
        }
        // The link binds inside the SOURCE collection's project — a
        // dead-letter target in another project is unrepresentable.
        let target = match state.registry.get(&desc.ref_in_project(&d)).await {
            Ok(Some(t)) if desc_alive(&t) && !initializing(&t) => t,
            Ok(_) => {
                return Err(failure(
                    FailureClass::Invalid,
                    "unknown_dead_letter_stream",
                    "deadLetterStream does not exist; create it first, with the same encryption key",
                    None,
                    false,
                ));
            }
            Err(_) => {
                return Err(failure(
                    FailureClass::Unavailable,
                    "unavailable",
                    "registry unavailable",
                    None,
                    true,
                ));
            }
        };
        if target.sealed || target.sealing.is_some() {
            return Err(failure(
                FailureClass::Invalid,
                "dead_letter_sealed",
                "deadLetterStream is sealed and cannot accept dead-letter records",
                None,
                false,
            ));
        }
        let same_key = key.fingerprint(&target.epoch()) == target.key_fingerprint;
        if !same_key {
            return Err(failure(
                FailureClass::Invalid,
                "dead_letter_key_mismatch",
                "deadLetterStream uses a different encryption key; dead-letter delivery writes with the source collection's key",
                None,
                false,
            ));
        }
        cfg.dead_letter_epoch = Some(target.stream_epoch.clone());
        cfg.dead_letter_stream = Some(d);
    }

    let out = consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigPut {
            consumer: cname.clone(),
            cfg,
        },
    )
    .await?;
    match out {
        crate::queue::QueueOut::Config {
            conflict: true,
            rec: Some(existing),
            ..
        } if existing.state == crate::queue::ConsumerLifecycle::Deleting => {
            let mut e = failure(
                FailureClass::Conflict,
                "consumer_deleting",
                "a deletion of this consumer is in progress; resume it by retrying DELETE with the Prisma-Consumer-Version on this response, or retry this create shortly",
                None,
                true,
            );
            e.version = Some(consumer_version_token(&epoch, existing.generation));
            Err(e)
        }
        crate::queue::QueueOut::Config {
            conflict: true,
            rec: Some(existing),
            ..
        } => Err(failure(
            FailureClass::Conflict,
            "consumer_config_conflict",
            "consumer exists with different configuration",
            Some(config_value(&cname, &existing.config)),
            false,
        )),
        crate::queue::QueueOut::Config {
            rec: Some(record),
            created,
            ..
        } => Ok(ConfigOutcome {
            created,
            record,
            epoch,
        }),
        _ => Err(failure(
            FailureClass::Internal,
            "internal",
            "unexpected config outcome",
            None,
            true,
        )),
    }
}
pub(crate) fn config_value(cname: &str, cfg: &crate::queue::ConsumerConfig) -> serde_json::Value {
    json!({"name":cname,"visibilityTimeoutMs":cfg.visibility_timeout_ms,"maxAttempts":cfg.max_attempts,"deadLetterStream":cfg.dead_letter_stream,"maxBatchRecords":cfg.max_batch_records})
}
pub(crate) async fn get(
    context: AuthorizedStreamContext,
) -> Result<ConfigOutcome, ConsumerFailure> {
    let AuthorizedStreamContext {
        service: state,
        desc,
        epoch,
        consumer,
        ..
    } = context;
    match consumer_config_op(&state, &desc, crate::queue::QueueOp::ConfigGet { consumer }).await? {
        crate::queue::QueueOut::Config {
            rec: Some(record), ..
        } if record.state == crate::queue::ConsumerLifecycle::Active => Ok(ConfigOutcome {
            created: false,
            record,
            epoch,
        }),
        _ => Err(failure(
            FailureClass::Missing,
            "unknown_consumer",
            "no such consumer",
            None,
            false,
        )),
    }
}

#[derive(serde::Serialize)]
pub(crate) struct SweepOutcome {
    complete: bool,
    steps: i64,
}
#[derive(serde::Serialize)]
pub(crate) struct QueuePosition {
    cursor: u64,
    tail: u64,
}
/// Internal callers provide the incarnation-bound target only after fleet authorization.
impl ConsumerService {
    pub(crate) async fn sweep_local(
        &self,
        target: InternalTarget,
        route: [u8; 16],
        consumer: String,
        fence_below: u64,
        max_steps: i64,
    ) -> Result<SweepOutcome, ConsumerFailure> {
        let engine = self.engine_for(&route).await?;
        let mut steps = 0;
        loop {
            if steps >= max_steps.clamp(1, CONSUMER_DELETE_REQUEST_STEPS as i64) {
                return Ok(SweepOutcome {
                    complete: false,
                    steps,
                });
            }
            steps += 1;
            match engine
                .submit_queue(
                    target.identity,
                    crate::queue::QueueOp::ConfigDeleteStep {
                        consumer: consumer.clone(),
                        fence_below,
                        max_rows: CONSUMER_DELETE_STEP_ROWS,
                        max_bytes: CONSUMER_DELETE_STEP_BYTES,
                    },
                )
                .await
            {
                Ok(crate::queue::QueueOut::DeleteStep { complete: true, .. }) => {
                    return Ok(SweepOutcome {
                        complete: true,
                        steps,
                    });
                }
                Ok(crate::queue::QueueOut::DeleteStep {
                    complete: false, ..
                }) => continue,
                other => {
                    return Err(failure(
                        FailureClass::Unavailable,
                        "segment_cleanup_failed",
                        &format!("relayed cleanup step failed: {other:?}"),
                        None,
                        true,
                    ));
                }
            }
        }
    }
    pub(crate) async fn queue_position(
        &self,
        target: InternalTarget,
        route: [u8; 16],
        consumer: &str,
        generation: u64,
    ) -> Result<QueuePosition, ConsumerFailure> {
        let engine = self.engine_for(&route).await?;
        let cursor = engine
            .queue_cursor(target.identity, consumer, generation)
            .await
            .map_err(|e| {
                failure(
                    FailureClass::Unavailable,
                    "queue_unavailable",
                    &e.to_string(),
                    None,
                    true,
                )
            })?;
        let handle = engine.stream_handle(target.identity).await.map_err(|e| {
            failure(
                FailureClass::Unavailable,
                "queue_unavailable",
                &e.to_string(),
                None,
                true,
            )
        })?;
        let local = handle.state.lock().unwrap().durable.next;
        let (remote, _) = engine
            .durable_absorbed(&target.identity)
            .await
            .map_err(|e| {
                failure(
                    FailureClass::Unavailable,
                    "queue_unavailable",
                    &e.to_string(),
                    None,
                    true,
                )
            })?;
        Ok(QueuePosition {
            cursor,
            tail: local.max(remote),
        })
    }
}
