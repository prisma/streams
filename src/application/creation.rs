//! Creation, fork anchoring and deletion own their persisted initialization/debt transitions.
use crate::crypto::{StreamKey, derive_subkey, hex};
use crate::registry::{Mutation, MutationResult, Registry, StreamDesc};
use crate::shard::{AppendReq, ShardEngine, now_ms};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::oneshot;

const APPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreationFailure {
    Invalid,
    Conflict,
    Missing,
    Gone,
    WrongKey,
    Storage,
    TooLarge,
    Overloaded,
    Ambiguous,
    Opening,
}
#[derive(Debug)]
pub(crate) struct CreationError {
    pub kind: CreationFailure,
    pub code: &'static str,
    pub message: String,
    pub owner: Option<String>,
    pub retry_after: Option<u64>,
}
impl CreationError {
    fn new(kind: CreationFailure, code: &'static str, message: &str) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            owner: None,
            retry_after: None,
        }
    }
    fn gone(desc: Option<&StreamDesc>) -> Self {
        if desc.is_some_and(|d| {
            d.soft_deleted
                || (!d.deleted
                    && !d.fork_children.is_empty()
                    && d.expires_at_ms.is_some_and(|expires| now_ms() >= expires))
        }) {
            Self::new(
                CreationFailure::Gone,
                "gone",
                "stream deleted; live forks remain",
            )
        } else {
            Self::new(CreationFailure::Missing, "not_found", "stream not found")
        }
    }
}
#[derive(Clone)]
pub(crate) struct ForkCommand {
    pub source: crate::tenant::TenantStreamRef,
    pub offset: Option<u64>,
    pub sub_offset: Option<u64>,
}
pub(crate) struct CreateCommand {
    pub sref: crate::tenant::TenantStreamRef,
    pub key: StreamKey,
    pub content_type: Option<String>,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub close: bool,
    pub body: Bytes,
    pub fork: Option<ForkCommand>,
}
pub(crate) struct CreateOutcome {
    pub created: bool,
    pub desc: StreamDesc,
    pub next: u64,
    pub closed: bool,
}
#[derive(Clone)]
pub(crate) struct CreationService {
    pub registry: Arc<Registry>,
    pub shards: crate::shard_directory::ShardDirectory,
    pub ownership: crate::ownership::OwnershipService,
    pub reads: Arc<crate::application::read::ReadService>,
    pub keys: Arc<crate::history::KeyCache>,
    pub runtime: crate::runtime::RuntimeCaps,
    pub deployment: crate::deployment::DeploymentIdentity,
    pub auth: Arc<crate::auth::AuthService>,
    pub quotas: crate::quota::QuotaRegistry,
    pub admission: crate::admission::AdmissionController,
    pub sliding: Arc<std::sync::Mutex<std::collections::HashSet<crate::tenant::TenantStreamRef>>>,
}
mod anchor;
mod claim;
mod deletion;
mod fork;
mod initialization;
mod product;
mod raw;
pub(crate) use product::{ProductCreateConfig, ProductCreateError};

impl CreationService {
    pub(crate) async fn resolve(
        &self,
        route: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, CreationError> {
        use crate::shard_directory::ResolveError;
        self.shards
            .resolve(route, crate::shard_directory::Adoption::External)
            .await
            .map_err(|e| match e {
                ResolveError::NotOwner { prefix, owner } => CreationError {
                    owner: Some(owner.clone()),
                    ..CreationError::new(
                        CreationFailure::Conflict,
                        "not_ring_owner",
                        &format!("shard {prefix} belongs to {owner}"),
                    )
                },
                ResolveError::Opening {
                    code,
                    retry_after_secs,
                    ..
                } => CreationError {
                    retry_after: Some(retry_after_secs),
                    ..CreationError::new(
                        CreationFailure::Opening,
                        code,
                        "shard not currently serving here; retry",
                    )
                },
                ResolveError::OpenFailed { prefix, error } => CreationError::new(
                    CreationFailure::Storage,
                    "shard_open",
                    &format!("open shard {prefix}: {error}"),
                ),
            })
    }
}
pub(crate) fn desc_alive(desc: &crate::registry::PersistedDescriptor) -> bool {
    !desc.deleted
        && !desc.soft_deleted
        && desc.expires_at_ms.is_none_or(|expires| now_ms() < expires)
}
fn init_claim_stale(desc: &crate::registry::PersistedDescriptor) -> bool {
    desc.init
        .as_ref()
        .is_some_and(|init| now_ms() - init.claimed_ms > crate::registry::INIT_CLAIM_MS)
}
pub(crate) fn create_request_hash(
    content_type: &str,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    close: bool,
    body: &[u8],
    fork: Option<&crate::registry::ForkRef>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content_type.as_bytes());
    h.update([0u8]);
    h.update(ttl_secs.unwrap_or(0).to_le_bytes());
    h.update(expires_at_ms.unwrap_or(0).to_le_bytes());
    h.update([u8::from(close)]);
    h.update((body.len() as u64).to_le_bytes());
    h.update(body);
    if let Some(fr) = fork {
        h.update(fr.source.as_bytes());
        h.update([0u8]);
        // The source INCARNATION is part of the identity. Without it, a
        // retry against a recreated source hashed the same as the
        // original, so it resumed an initialization whose stored
        // forked_from still pointed at the previous epoch — reference
        // installed on incarnation B, child recorded against A, and
        // stitched reads later failing the epoch check.
        h.update(fr.source_epoch.as_bytes());
        h.update([0u8]);
        h.update(fr.fork_offset.to_le_bytes());
        h.update(fr.fork_sub.to_le_bytes());
    }
    hex(&h.finalize()[..16])
}
pub(crate) fn fresh_desc(
    service: &CreationService,
    sref: &crate::tenant::TenantStreamRef,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
) -> crate::registry::PersistedDescriptor {
    let epoch = service.runtime.epoch();
    crate::registry::PersistedDescriptor {
        name: sref.name().as_str().to_string(),
        account_id: Some(service.deployment.account_id().to_string()),
        project_id: sref.project_id().clone(),
        stream_epoch: hex(&epoch),
        seal_gen_counter: 0,
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs
            .map(|t| now_ms() + (t as i64) * 1000)
            .or(expires_at_ms),
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type,
        ttl_secs,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
}

pub(crate) fn json_entries(body: &[u8], allow_empty_array: bool) -> Result<Vec<Bytes>, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "invalid JSON body".to_string())?;
    match v {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() && !allow_empty_array {
                return Err("empty JSON array".to_string());
            }
            Ok(arr
                .iter()
                .map(|e| Bytes::from(serde_json::to_vec(e).expect("json")))
                .collect())
        }
        other => Ok(vec![Bytes::from(serde_json::to_vec(&other).expect("json"))]),
    }
}
pub(crate) fn over_record_ceiling(cap: usize, entries: &[Bytes]) -> Option<usize> {
    if cap == 0 {
        return None;
    }
    entries.iter().map(|e| e.len()).find(|l| *l > cap)
}
impl CreationService {
    pub(crate) fn touch_ttl(self: &Arc<Self>, desc: &StreamDesc) {
        let Some(ttl) = desc.ttl_secs else { return };
        let Some(exp) = desc.expires_at_ms else {
            return;
        };
        let window_ms = (ttl as i64).saturating_mul(1000);
        let now = now_ms();
        if exp.saturating_sub(now) >= window_ms - window_ms / 4 {
            return; // window still fresh
        }
        // One in-flight slide per stream: without this, every request in
        // the window between spawn and CAS completion spawns ANOTHER CAS —
        // a herd against the registry under rapid op sequences.
        // Søren review: keyed by the PROJECT-QUALIFIED ref — a bare-name
        // set let same-name projects suppress one another's slides, and
        // the spawned CAS below extended (or epoch-fenced into a no-op
        // against) the deployment tenant's descriptor instead of this one.
        let sref = desc.sref();
        if !self.sliding.lock().unwrap().insert(sref.clone()) {
            return; // a slide is already in flight
        }
        let target = now + window_ms;
        let state = self.clone();
        let expect_epoch = desc.stream_epoch.clone();
        let slide = TtlSlide {
            sliding: self.sliding.clone(),
            sref: sref.clone(),
        };
        tokio::spawn(async move {
            let _slide = slide;
            // The registry re-decides each typed mutation after a conditional conflict.
            // Incarnation-fenced: a slide spawned against incarnation A must
            // not extend the expiry of a replacement created under the same
            // name while the task sat on the runtime.
            if let Err(e) = state
                .registry
                .mutate_incarnation(&sref, &expect_epoch, |current| {
                    if current.deleted
                        || current.ttl_secs.is_none()
                        || !current.expires_at_ms.is_some_and(|e| e < target)
                    {
                        return Mutation::Decline(());
                    }
                    let mut next = current.to_persisted();
                    next.expires_at_ms = Some(target);
                    Mutation::Write(next, ())
                })
                .await
            {
                tracing::warn!(
                    project = %sref.project_id().as_str(),
                    stream = %sref.name().as_str(),
                    "ttl slide lost: {e}"
                );
            }
            state.registry.invalidate(&sref);
        });
    }
}

struct TtlSlide {
    sliding: Arc<std::sync::Mutex<std::collections::HashSet<crate::tenant::TenantStreamRef>>>,
    sref: crate::tenant::TenantStreamRef,
}
impl Drop for TtlSlide {
    fn drop(&mut self) {
        self.sliding.lock().unwrap().remove(&self.sref);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn r05_cancelled_ttl_attempt_releases_only_its_owned_slot() {
        let project = crate::tenant::ProjectId::new("creation-test").unwrap();
        let source = project.stream_ref("source");
        let other = project.stream_ref("other");
        let sliding = Arc::new(std::sync::Mutex::new(std::collections::HashSet::from([
            source.clone(),
            other.clone(),
        ])));
        let guard = TtlSlide {
            sliding: sliding.clone(),
            sref: source.clone(),
        };
        let (entered, waiting) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        waiting.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            *sliding.lock().unwrap(),
            std::collections::HashSet::from([other])
        );
        assert!(
            sliding.lock().unwrap().insert(source),
            "retry can claim the released slot"
        );
    }
}
