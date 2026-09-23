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
    alive_at(desc, now_ms())
}
/// Liveness at the caller's instant: the recreate CAS and the classification
/// of the winner it declines on must judge one instant, or a winner expiring
/// between the two would be neither live nor retained.
fn alive_at(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !desc.deleted && !desc.soft_deleted && desc.expires_at_ms.is_none_or(|expires| at_ms < expires)
}
/// A dead incarnation whose epoch and records its live forks still read
/// through (pinned fork lifecycle): soft-deleted, or expired with children.
/// Creation, append and delete answer it as gone and nothing may replace it
/// until the last fork releases it, so this is the one place that decides it.
pub(crate) fn retained_for_forks(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    desc.soft_deleted
        || (!desc.deleted
            && !desc.fork_children.is_empty()
            && desc.expires_at_ms.is_some_and(|expires| at_ms >= expires))
}
/// The recreate CAS predicate. It judges the STORED descriptor, because a
/// cached snapshot can predate a fork another instance anchored, and replacing
/// a retained source strands every fork reading through it.
pub(crate) fn recreatable(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !alive_at(desc, at_ms) && !retained_for_forks(desc, at_ms)
}
fn init_claim_stale(desc: &crate::registry::PersistedDescriptor) -> bool {
    desc.init
        .as_ref()
        .is_some_and(|init| now_ms() - init.claimed_ms > crate::registry::INIT_CLAIM_MS)
}
#[expect(
    clippy::too_many_arguments,
    reason = "create_request_hash; the request hash covers every field a client can vary, taken separately as the handler parsed them; a request struct would exist only to be hashed"
)]
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
#[expect(
    clippy::too_many_arguments,
    reason = "fresh_desc; a fresh descriptor is built from the resolved name, epoch, policy and fork parts separately as creation decided them; a builder would restate the descriptor's own fields"
)]
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
        expires_at_ms: ttl_secs.map(ttl::expiry_from_now).or(expires_at_ms),
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

#[expect(
    clippy::expect_used,
    reason = "json_entries; a serde_json::Value serialises infallibly; a fallible path would add a branch no value reaches"
)]
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
mod ttl;
pub(crate) use ttl::{TtlMutation, admit_ttl};

#[cfg(test)]
mod tests {
    use crate::application::request_work::{Action, Key, Kind, RequestWork, WorkError};
    use std::{sync::Arc, time::Duration};

    #[expect(
        clippy::excessive_nesting,
        reason = "r05_cancelled_ttl_attempt_releases_only_its_owned_slot; the fixture nests the drain wait inside the timeout that bounds it inside the test; flattening it would separate the wait from the bound it must respect"
    )]
    #[tokio::test]
    async fn r05_cancelled_ttl_attempt_releases_only_its_owned_slot() {
        let project = crate::tenant::ProjectId::new("creation-test").unwrap();
        let source = Key {
            stream: project.stream_ref("source"),
            epoch: "first".into(),
            kind: Kind::Ttl,
        };
        let other = Key {
            stream: project.stream_ref("other"),
            epoch: "first".into(),
            kind: Kind::Ttl,
        };
        let work = Arc::new(RequestWork::default());
        let tasks = crate::tasks::TaskSupervisor::new();
        work.start(&tasks).unwrap();
        let first = work
            .test_admit(
                source.clone(),
                Action::Held(Box::pin(std::future::pending())),
                tokio::time::Instant::now() + Duration::from_millis(10),
            )
            .unwrap();
        let _other = work
            .submit(
                other.clone(),
                Action::Held(Box::pin(std::future::pending())),
            )
            .unwrap();
        assert_eq!(first.wait().await, Err(WorkError::TimedOut));
        tokio::time::timeout(Duration::from_secs(1), async {
            while work.test_keys().contains(&source) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            work.test_keys(),
            std::collections::HashSet::from([other.clone()])
        );
        let retry = work
            .submit(source, Action::Held(Box::pin(async { Ok(()) })))
            .expect("retry can claim the released slot");
        retry.wait().await.unwrap();
        assert_eq!(work.test_keys(), std::collections::HashSet::from([other]));
        tasks.shutdown(Duration::from_secs(1)).await;
        assert!(work.test_keys().is_empty());
    }

    /// The recreate CAS and the classification of the winner it declines on
    /// share one instant, so the three name verdicts must partition every
    /// descriptor at that instant, the expiry instant itself included: live,
    /// retained for forks, or recreatable, never two and never none.
    #[test]
    fn a_name_is_live_retained_or_recreatable_at_one_instant() {
        use super::{alive_at, recreatable, retained_for_forks};
        let at = 1_000;
        let base = crate::registry::PersistedDescriptor {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("creation-test").unwrap(),
            name: "verdict".into(),
            stream_epoch: "0123456789abcdef0123456789abcdef".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
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
            layout_version: crate::registry::LAYOUT_VERSION,
        };
        // (expiry, deleted, soft-deleted, children) -> (live, retained, recreatable)
        let cases = [
            (None, false, false, 1, (true, false, false)),
            (Some(at + 1), false, false, 0, (true, false, false)),
            (Some(at), false, false, 0, (false, false, true)),
            (Some(at), false, false, 1, (false, true, false)),
            (Some(at - 1), false, false, 1, (false, true, false)),
            (None, false, true, 1, (false, true, false)),
            (None, true, false, 0, (false, false, true)),
        ];
        for (expiry, deleted, soft_deleted, children, want) in cases {
            let mut d = base.clone();
            d.expires_at_ms = expiry;
            d.deleted = deleted;
            d.soft_deleted = soft_deleted;
            d.fork_children = (0..children).map(|i| format!("child-{i}")).collect();
            let got = (
                alive_at(&d, at),
                retained_for_forks(&d, at),
                recreatable(&d, at),
            );
            assert_eq!(got, want, "{expiry:?} {deleted} {soft_deleted} {children}");
        }
    }
}
