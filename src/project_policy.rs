//! Project policy and credential-grant caches (docs/MULTITENANCY.md
//! §11, Stage 2b/5).
//!
//! The data plane authorizes every request from LOCAL memory: the
//! request path reads lock-free `arc_swap` snapshots that background
//! refreshers (Stage 5) publish. No request performs a synchronous
//! Control Plane lookup, and a snapshot older than the accepted
//! staleness window fails CLOSED (§7.1) — availability of stale-but-
//! bounded policy beats silent authorization against dead data.
//!
//! Retrieval is behind injectable traits (`PolicySource`,
//! `GrantSource`) so Stage 2b tests run against in-memory fixtures and
//! Stage 5 wires the real snapshot/delta feed without touching the
//! verification logic.

#![allow(dead_code)] // consumed across MT Stages 2b/4/5 as wiring lands

use std::collections::HashMap;
use std::sync::Arc;

use crate::tenant::{ProjectId, ScopeSet, StreamGrant, WorkspaceId};

/// §11. Every status except `Active` fails closed at authorization;
/// `TransferPending` additionally means the issuer has stopped minting
/// tokens, so a verified token seen in that state is pre-transfer and
/// must not act.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    Active,
    Suspended,
    TransferPending,
    Deleting,
    Deleted,
}

/// §17 quota fields. Stage 2b carries them through the policy feed;
/// Stage 6 enforces them. `0` means "no limit configured" at this
/// level (cell safety limits still apply).
#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct ProjectQuotas {
    #[serde(default)]
    pub requests_per_sec: u64,
    #[serde(default)]
    pub append_bytes_per_sec: u64,
    #[serde(default)]
    pub append_records_per_sec: u64,
    #[serde(default)]
    pub read_bytes_per_sec: u64,
    #[serde(default)]
    pub max_inflight_requests: u64,
    #[serde(default)]
    pub max_live_subscriptions: u64,
    #[serde(default)]
    pub max_streams: u64,
    /// SR2-4: ceiling on this project's bytes sitting in committer
    /// queues awaiting a decision (charged before enqueue, released
    /// when the append is decided). 0 = not configured.
    #[serde(default)]
    pub queued_append_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ProjectPolicy {
    pub project_id: ProjectId,
    pub workspace_id: WorkspaceId,
    pub cell_id: Arc<str>,
    pub project_policy_version: u64,
    pub ownership_version: u64,
    pub status: ProjectStatus,
    pub quotas: ProjectQuotas,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    Active,
    Disabled,
    Revoked,
    Expired,
}

/// §11: the credential-grant cache entry. The data plane never infers
/// workspace ownership from credential metadata — ownership comes from
/// the current `ProjectPolicy` only.
#[derive(Clone, Debug)]
pub struct CredentialGrant {
    pub credential_id: Arc<str>,
    pub project_id: ProjectId,
    pub grant_version: u64,
    pub status: CredentialStatus,
    pub scopes: ScopeSet,
    pub grant: StreamGrant,
    pub expires_at: Option<i64>,
}

/// One published generation of project policies. `fetched_at_unix`
/// drives the fail-closed staleness window; `feed_version` is the
/// producer's `project_policy_version` high-water mark, for
/// observability and delta ordering.
#[derive(Clone, Debug)]
pub struct PolicySnapshot {
    pub projects: HashMap<ProjectId, ProjectPolicy>,
    pub fetched_at_unix: i64,
    pub feed_version: u64,
}

impl PolicySnapshot {
    pub fn empty() -> Self {
        Self {
            projects: HashMap::new(),
            fetched_at_unix: 0,
            feed_version: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrantSnapshot {
    // mt-lint: allow(name-keyed-map): credential id -> grant (the feed snapshot itself)
    pub credentials: HashMap<Arc<str>, CredentialGrant>,
    pub fetched_at_unix: i64,
    pub feed_version: u64,
}

impl GrantSnapshot {
    pub fn empty() -> Self {
        Self {
            credentials: HashMap::new(),
            fetched_at_unix: 0,
            feed_version: 0,
        }
    }
}

/// Injectable snapshot producers (review round: retrieval behind
/// traits). Stage 5 implements these against the Control Plane
/// snapshot/delta feed; tests implement them in memory. The refresher
/// task — not the request path — calls `fetch`, then publishes the
/// result into the `AuthService`'s arc-swap slots.
#[async_trait::async_trait]
pub trait PolicySource: Send + Sync {
    async fn fetch(&self) -> anyhow::Result<PolicySnapshot>;
}

#[async_trait::async_trait]
pub trait GrantSource: Send + Sync {
    async fn fetch(&self) -> anyhow::Result<GrantSnapshot>;
}

/// In-memory source for tests and local rigs.
pub struct StaticPolicySource(pub std::sync::Mutex<PolicySnapshot>);

#[async_trait::async_trait]
impl PolicySource for StaticPolicySource {
    async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
        Ok(self.0.lock().unwrap().clone())
    }
}

pub struct StaticGrantSource(pub std::sync::Mutex<GrantSnapshot>);

#[async_trait::async_trait]
impl GrantSource for StaticGrantSource {
    async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
        Ok(self.0.lock().unwrap().clone())
    }
}
