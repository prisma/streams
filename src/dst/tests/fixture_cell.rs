//! Fixture cell: ONE enforce-mode cell serving many projects, the
//! shared-cell Layer A rig (shared-cells PLAN section 4.1).
//!
//! Every project reuses the same stream names, routing keys, producer
//! id, consumer group and ONE encryption key. That is the adversarial
//! choice: a frame served to the wrong project would decrypt and show up
//! as foreign plaintext in the oracle, instead of failing closed as an
//! AEAD error. Each record names its project, stream, key and sequence,
//! and the test keeps the exact ledger of what it appended.

use super::fixture_auth::{RIG_PRIV, RIG_PUB};
use super::fixture_http::{HttpRigOptions, http_rig_build, install_rollup};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::auth::ceiling::{CellCeiling, SharedBounds};
use crate::config::Environment;
use crate::project_policy::{
    CredentialGrant, CredentialStatus, GrantSnapshot, PolicySnapshot, ProjectPolicy, ProjectQuotas,
    ProjectStatus,
};
use crate::tenant::{ProjectId, Scope, ScopeSet, StreamGrant, WorkspaceId};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

/// The names every project reuses (one is hierarchical).
pub(super) const NAMES: [&str; 3] = ["orders", "events", "a/b/c"];
/// The routing keys every project reuses; "" is the default key.
pub(super) const KEYS: [&str; 4] = ["", "k0", "k1", "k2"];
/// Projects share workspaces, as real accounts do.
pub(super) const WORKSPACES: usize = 16;
/// Concurrent requests the rig drives at once.
const FANOUT: usize = 32;
const SUITE_PROJECTS: usize = 128;
pub(super) const CREATE: &[u8] = br#"{"format":{"kind":"json"}}"#;

/// The cell's project count: 128 in the suite; the at-scale leg sets
/// `MT_CERT_PROJECTS=1000`, the knob the shared-cell smoke already reads
/// (one code path, two postures). Read through the process environment
/// adapter, never `std::env` directly.
pub(super) fn cell_scale() -> usize {
    crate::config::ProcessEnvironment
        .get("MT_CERT_PROJECTS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(SUITE_PROJECTS)
}

pub(super) fn project(i: usize) -> String {
    format!("proj-sc{i:04}")
}

pub(super) fn workspace(i: usize) -> String {
    format!("ws-sc{:02}", i % WORKSPACES)
}

pub(super) fn credential(i: usize) -> String {
    format!("c-sc{i:04}")
}

/// Every scope a customer credential can hold.
fn all_scopes() -> String {
    Scope::ALL.map(Scope::as_str).join(" ")
}

/// The policy a fresh cell publishes for project `i`.
pub(super) fn policy(i: usize, quotas: ProjectQuotas) -> ProjectPolicy {
    ProjectPolicy {
        project_id: ProjectId::new(&project(i)).unwrap(),
        workspace_id: WorkspaceId::new(&workspace(i)).unwrap(),
        cell_id: Arc::from("test-cell"),
        project_policy_version: 1,
        ownership_version: 1,
        status: ProjectStatus::Active,
        quotas,
    }
}

/// The every-name, every-scope grant of project `i`'s credential.
pub(super) fn grant(i: usize, version: u64) -> CredentialGrant {
    CredentialGrant {
        credential_id: Arc::from(credential(i).as_str()),
        project_id: ProjectId::new(&project(i)).unwrap(),
        grant_version: version,
        status: CredentialStatus::Active,
        scopes: ScopeSet::parse(&all_scopes()).0,
        grant: StreamGrant::All,
        expires_at: None,
    }
}

/// The claims of one customer token; `of(i)` is project `i`'s first.
#[derive(serde::Serialize)]
pub(super) struct Token {
    iss: &'static str,
    aud: &'static str,
    sub: &'static str,
    pub(super) credential_id: String,
    pub(super) project_id: String,
    pub(super) workspace_id: String,
    cell_id: &'static str,
    pub(super) ownership_version: u64,
    pub(super) grant_version: u64,
    scope: String,
    jti: &'static str,
    iat: i64,
    exp: i64,
    /// The signing key id (a header, not a claim).
    #[serde(skip)]
    pub(super) kid: &'static str,
}

impl Token {
    /// Project `i`'s credential, every scope, ownership and grant 1.
    pub(super) fn of(i: usize) -> Self {
        let now = crate::shard::now_ms() / 1000;
        Self {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: credential(i),
            project_id: project(i),
            workspace_id: workspace(i),
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: all_scopes(),
            jti: "cell",
            iat: now - 60,
            exp: now + 3600,
            kid: "rig-1",
        }
    }

    /// The signed `Bearer` header value (the fixture RSA key).
    pub(super) fn bearer(&self) -> String {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(self.kid.to_string());
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(RIG_PRIV.as_bytes()).unwrap();
        format!(
            "Bearer {}",
            jsonwebtoken::encode(&header, self, &key).unwrap()
        )
    }
}

/// The fixture public key under key id `kid`.
pub(super) fn jwks_key(kid: &str) -> (String, crate::auth::JwksKey) {
    let key = crate::auth::JwksKey {
        alg: jsonwebtoken::Algorithm::RS256,
        key: jsonwebtoken::DecodingKey::from_rsa_pem(RIG_PUB.as_bytes()).unwrap(),
        fp: crate::auth::key_fp(RIG_PUB.as_bytes()),
    };
    (kid.to_string(), key)
}

/// The divisor every fixture cell shares its bounds by (`PROJECT_SHARE_K`,
/// the owner's default of 2026-10-07): a project's ceiling on each shared
/// bound is the bound / 8.
pub(super) const SHARE_K: usize = 8;

/// How a cell is built: its size, each project's quotas, the scenario's
/// command line over the hermetic fixture, its shard configuration and
/// prefixes, its maintenance admission bounds, and the shared bounds its
/// projects' ceilings divide (none by default: the rig's admission sets
/// no inflight or SSE bound unless `bounds` gives one).
pub(super) struct CellSpec {
    pub(super) projects: usize,
    pub(super) quotas: fn(usize) -> ProjectQuotas,
    pub(super) cli: fn(&mut crate::config::CliArgs),
    pub(super) shard: crate::shard::ShardConfig,
    pub(super) prefixes: Vec<String>,
    pub(super) admission: Option<crate::config::AdmissionConfig>,
    pub(super) bounds: SharedBounds,
}

impl CellSpec {
    /// `projects` projects with no quotas and the default command line.
    pub(super) fn open(projects: usize) -> Self {
        Self {
            projects,
            quotas: |_| ProjectQuotas::default(),
            cli: |_| {},
            shard: crate::shard::ShardConfig::default(),
            prefixes: vec!["00".to_string()],
            admission: None,
            bounds: SharedBounds::default(),
        }
    }
}

/// Give the rig the spec's shared bounds and the cell its ceiling over
/// them, reserving the rig deployment's identities, before the first
/// policy snapshot: the instance inflight bound and the ceiling derive
/// from the one value, as boot derives both from `ADMIT_MAX_INFLIGHT`.
fn share_cell(svc: &crate::auth::AuthService, state: &crate::http::AppState, bounds: SharedBounds) {
    if bounds.inflight > 0 {
        state
            .admission
            .set_max_inflight(i64::try_from(bounds.inflight).unwrap());
    }
    let ceiling = CellCeiling::shared(u64::try_from(SHARE_K).unwrap(), bounds);
    svc.install_cell_ceiling(ceiling.reserving(&state.deployment))
        .unwrap();
}

/// A running shared cell and the feeds it was given.
pub(super) struct Cell {
    pub(super) svc: Arc<crate::auth::AuthService>,
    pub(super) state: Arc<crate::http::AppState>,
    pub(super) addr: std::net::SocketAddr,
    pub(super) projects: usize,
    pub(super) bearers: Vec<String>,
    pub(super) policies: HashMap<ProjectId, ProjectPolicy>,
    pub(super) grants: HashMap<Arc<str>, CredentialGrant>,
    feed_version: u64,
}

/// Build the cell: keys (kid `rig-1`), one policy and one credential per
/// project, an enforce-mode rig over a fresh memory store, its rollup.
pub(super) async fn open_cell(spec: CellSpec) -> Cell {
    let now = crate::shard::now_ms() / 1000;
    let svc = Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    svc.publish_jwks(crate::auth::JwksSnapshot {
        keys: HashMap::from([jwks_key("rig-1")]),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let policies: HashMap<_, _> = (0..spec.projects)
        .map(|i| {
            (
                ProjectId::new(&project(i)).unwrap(),
                policy(i, (spec.quotas)(i)),
            )
        })
        .collect();
    let grants: HashMap<_, _> = (0..spec.projects)
        .map(|i| (Arc::from(credential(i).as_str()), grant(i, 1)))
        .collect();
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            auth_service: Some(svc.clone()),
            cli: spec.cli,
            shard: spec.shard,
            prefixes: spec.prefixes,
            admission: spec.admission,
            ..Default::default()
        },
    )
    .await;
    let (state, addr) = rig.parts();
    share_cell(&svc, &state, spec.bounds);
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let bearers = (0..spec.projects).map(|i| Token::of(i).bearer()).collect();
    let mut cell = Cell {
        svc,
        state,
        addr,
        projects: spec.projects,
        bearers,
        policies,
        grants,
        feed_version: 0,
    };
    cell.publish();
    cell
}

impl Cell {
    /// Publish the current policies and grants as the next feed version.
    pub(super) fn publish(&mut self) {
        self.feed_version += 1;
        let now = crate::shard::now_ms() / 1000;
        self.svc
            .publish_policies(PolicySnapshot {
                projects: self.policies.clone(),
                fetched_at_unix: now,
                feed_version: self.feed_version,
            })
            .unwrap();
        self.svc
            .publish_grants(GrantSnapshot {
                credentials: self.grants.clone(),
                fetched_at_unix: now,
                feed_version: self.feed_version,
            })
            .unwrap();
    }

    /// The policy of project `i`, for an edit before `publish`.
    pub(super) fn policy_mut(&mut self, i: usize) -> &mut ProjectPolicy {
        self.policies
            .get_mut(&ProjectId::new(&project(i)).unwrap())
            .unwrap()
    }

    /// One request as project `i`: its bearer and the shared stream key.
    pub(super) async fn call(
        &self,
        i: usize,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> (u16, HashMap<String, String>, Vec<u8>) {
        self.call_with(&self.bearers[i], method, path, body).await
    }

    /// One request under an explicit bearer and the shared stream key.
    pub(super) async fn call_with(
        &self,
        bearer: &str,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> (u16, HashMap<String, String>, Vec<u8>) {
        let headers = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", bearer),
        ];
        preq(self.addr, method, path, &headers, body).await
    }

    /// One request as project `i` that must answer 200 with JSON.
    pub(super) async fn json(&self, i: usize, method: &str, path: &str) -> serde_json::Value {
        let (st, _, b) = self.call(i, method, path, b"").await;
        assert_eq!(
            st,
            200,
            "{} {method} {path}: {}",
            project(i),
            String::from_utf8_lossy(&b)
        );
        serde_json::from_slice(&b).unwrap()
    }

    /// Every project creates every name in `NAMES`.
    pub(super) async fn create_everywhere(&self) {
        for_each(self.projects, |i| async move {
            for name in NAMES {
                let path = format!("/v1/streams/{name}");
                let (st, _, b) = self.call(i, "PUT", &path, CREATE).await;
                assert_eq!(
                    st,
                    201,
                    "{} {name}: {}",
                    project(i),
                    String::from_utf8_lossy(&b)
                );
            }
        })
        .await;
    }
}

/// Run `f` for every project index, `FANOUT` at a time.
pub(super) async fn for_each<F, Fut>(projects: usize, f: F)
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    futures_util::stream::iter(0..projects)
        .map(f)
        .buffer_unordered(FANOUT)
        .collect::<Vec<()>>()
        .await;
}

/// Run `f` for every project index and keep each answer, by index.
pub(super) async fn map_each<F, Fut, T>(projects: usize, f: F) -> Vec<T>
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = (usize, T)>,
{
    let mut out: Vec<(usize, T)> = futures_util::stream::iter(0..projects)
        .map(f)
        .buffer_unordered(FANOUT)
        .collect()
        .await;
    out.sort_by_key(|(i, _)| *i);
    out.into_iter().map(|(_, t)| t).collect()
}

/// The record project `p` appends as sequence `q` on (`name`, `key`):
/// every field a leak could carry names its origin.
pub(super) fn record(p: usize, name: &str, key: &str, q: usize) -> serde_json::Value {
    let text =
        format!(r#"{{"p":{p},"s":"{name}","k":"{key}","q":{q},"t":"p{p:04}/{name}/{key}/{q}"}}"#);
    serde_json::from_str(&text).unwrap()
}

/// The marker every record carries, for transcripts that are not JSON.
pub(super) fn marker(value: &serde_json::Value) -> String {
    value["t"].as_str().unwrap().to_string()
}

/// One project in eight writes; the rest stay idle with empty streams.
pub(super) fn active(i: usize) -> bool {
    i % 8 == 3
}

/// The exact record count project `i` appends on (name, key): 1 to 4,
/// distinct across neighbours, so a swapped answer cannot match.
fn volume(i: usize, name_ix: usize, key_ix: usize) -> usize {
    1 + (i / 8 + 2 * name_ix + 3 * key_ix) % 4
}

/// The exact client-side record of what each project appended.
#[derive(Default)]
pub(super) struct Ledger {
    /// (project, name index, key index) -> the records, in order.
    records: HashMap<(usize, usize, usize), Vec<serde_json::Value>>,
}

impl Ledger {
    /// What project `p` holds on (name, key), in append order.
    pub(super) fn of(&self, p: usize, name_ix: usize, key_ix: usize) -> Vec<serde_json::Value> {
        self.records
            .get(&(p, name_ix, key_ix))
            .cloned()
            .unwrap_or_default()
    }

    /// Every record of project `p` on `name_ix`, keyed, sorted by marker.
    pub(super) fn stream(&self, p: usize, name_ix: usize) -> Vec<(String, serde_json::Value)> {
        let mut out: Vec<_> = (0..KEYS.len())
            .flat_map(|k| {
                self.of(p, name_ix, k)
                    .into_iter()
                    .map(move |v| (KEYS[k].to_string(), v))
            })
            .collect();
        out.sort_by_key(|(_, v)| marker(v));
        out
    }

    /// The append requests project `p` made: one per record, one per batch.
    pub(super) fn append_requests(&self, p: usize) -> u64 {
        self.records
            .iter()
            .filter(|((q, _, _), _)| *q == p)
            .map(|((_, _, k), rs)| if KEYS[*k] == "k1" { 1 } else { rs.len() as u64 })
            .sum()
    }

    /// Records and JSON payload bytes project `p` appended in all.
    pub(super) fn totals(&self, p: usize) -> (u64, u64) {
        self.records
            .iter()
            .filter(|((q, _, _), _)| *q == p)
            .flat_map(|(_, rs)| rs.iter())
            .fold((0, 0), |(n, b), v| (n + 1, b + v.to_string().len() as u64))
    }
}

/// Every active project appends its volume to every (name, key): single
/// appends, except key `k1`, which goes as one `:batch`.
pub(super) async fn seed(cell: &Cell) -> Ledger {
    let appended = map_each(cell.projects, |i| async move {
        let mut mine = Vec::new();
        if !active(i) {
            return (i, mine);
        }
        for (s, name) in NAMES.iter().enumerate() {
            for (k, key) in KEYS.iter().enumerate() {
                let rs: Vec<_> = (0..volume(i, s, k))
                    .map(|q| record(i, name, key, q))
                    .collect();
                append_all(cell, i, name, key, &rs).await;
                mine.push(((i, s, k), rs));
            }
        }
        (i, mine)
    })
    .await;
    Ledger {
        records: appended.into_iter().flatten().collect(),
    }
}

/// Append `rs` to (`name`, `key`) as project `i`; `k1` batches them.
pub(super) async fn append_all(
    cell: &Cell,
    i: usize,
    name: &str,
    key: &str,
    rs: &[serde_json::Value],
) {
    if key == "k1" {
        let body = serde_json::Value::Array(rs.to_vec()).to_string();
        let path = format!("/v1/streams/{name}/records:batch");
        let (st, _, b) = keyed(cell, i, &path, key, body.as_bytes()).await;
        assert_eq!(
            st,
            200,
            "{} batch: {}",
            project(i),
            String::from_utf8_lossy(&b)
        );
        return;
    }
    for r in rs {
        let path = format!("/v1/streams/{name}/records");
        let (st, _, b) = keyed(cell, i, &path, key, r.to_string().as_bytes()).await;
        assert_eq!(
            st,
            200,
            "{} append: {}",
            project(i),
            String::from_utf8_lossy(&b)
        );
    }
}

/// POST `body` to `path` as project `i` under routing key `key`.
async fn keyed(
    cell: &Cell,
    i: usize,
    path: &str,
    key: &str,
    body: &[u8],
) -> (u16, HashMap<String, String>, Vec<u8>) {
    let mut headers = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", cell.bearers[i].as_str()),
    ];
    if !key.is_empty() {
        headers.push(("prisma-routing-key", key));
    }
    preq(cell.addr, "POST", path, &headers, body).await
}

/// The query string selecting routing key `key` ("" = the default).
pub(super) fn key_query(key: &str) -> String {
    if key.is_empty() {
        String::new()
    } else {
        format!("?routingKey={key}")
    }
}

/// The `code` of an error answer, if the body carries one.
pub(super) fn error_code(body: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(body);
    let rest = text.split_once("\"code\":\"")?.1;
    rest.split('"').next().map(str::to_string)
}

/// Drain this runtime's denial queue into `_audit_events` and read the
/// whole journal back through the system path.
pub(super) async fn journal(state: &Arc<crate::http::AppState>) -> Vec<serde_json::Value> {
    for _ in 0..200 {
        if crate::audit::drain_audit_once(state).await.unwrap() == 0 {
            break;
        }
    }
    let key = state.billing.usage_key().expect("rig usage key");
    let mut events = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let Some((page, next)) = crate::billing::system_read(
            state,
            crate::billing::AUDIT_EVENTS_STREAM,
            &key,
            cursor.clone(),
        )
        .await
        .unwrap() else {
            break;
        };
        let values: Vec<serde_json::Value> = serde_json::from_slice(&page).unwrap_or_default();
        if values.is_empty() {
            break;
        }
        events.extend(values);
        if cursor.as_deref() == Some(next.as_str()) {
            break;
        }
        cursor = Some(next);
    }
    events
}

pub(super) type Answers = Vec<(u16, Option<String>)>;

/// `count` requests as project `i`, one after another or all at once.
pub(super) async fn burst(
    cell: &Cell,
    i: usize,
    req: (&str, &str, &[u8]),
    count: usize,
    together: bool,
) -> Answers {
    let (method, path, body) = req;
    let one = || async move {
        let (st, _, b) = cell.call(i, method, path, body).await;
        (st, error_code(&b))
    };
    if together {
        return futures_util::future::join_all((0..count).map(|_| one())).await;
    }
    let mut out = Vec::new();
    for _ in 0..count {
        out.push(one().await);
    }
    out
}

/// A JSON record padded to about `bytes` bytes.
pub(super) fn padded(bytes: usize) -> Vec<u8> {
    format!(r#"{{"pad":"{}"}}"#, "x".repeat(bytes.saturating_sub(10))).into_bytes()
}
