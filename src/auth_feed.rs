//! Snapshot feeds for the auth data plane (docs/MULTITENANCY.md §7.1,
//! Stage 5).
//!
//! The request path never fetches anything: a background refresher
//! publishes whole snapshots into the `AuthService`'s arc-swap slots,
//! and `verify_customer` fails CLOSED when a snapshot's age exceeds
//! `POLICY_STALENESS_MAX_SECS` — so a dead refresher degrades to
//! refusal, never to authorizing against stale data forever.
//!
//! This module ships the FILE-backed sources: operator-authored JSON
//! documents for field trials, DST rigs, and the shadow campaign. The
//! Control Plane snapshot/delta feed implements the same
//! `PolicySource`/`GrantSource`/`KeySource` traits when the platform
//! side (Stage 1) lands; nothing on the request path changes then.
//!
//! Parsing is STRICT. These files are authorization inputs: an
//! unknown scope name, an empty `stream_prefixes` array (§4.2: that
//! means "none", which no operator writes on purpose), or an
//! unsupported algorithm each fail the whole fetch — the previous
//! snapshot stays published and ages toward the staleness refusal,
//! which is exactly the §7.1 posture: loud, bounded, fail-closed.

use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{AuthService, JwksSnapshot};
use crate::project_policy::{
    CredentialGrant, CredentialStatus, GrantSnapshot, GrantSource, PolicySnapshot, PolicySource,
    ProjectPolicy, ProjectQuotas, ProjectStatus,
};
use crate::tenant::{ProjectId, ScopeSet, StreamGrant, WorkspaceId};

/// Verification keys, mirroring `PolicySource`/`GrantSource`.
#[async_trait::async_trait]
pub trait KeySource: Send + Sync {
    async fn fetch(&self) -> anyhow::Result<JwksSnapshot>;
}

// ---------------------------------------------------------------------
// Wire formats (operator-authored JSON)

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysDoc {
    #[serde(default)]
    feed_version: u64,
    keys: Vec<KeyDoc>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyDoc {
    kid: String,
    /// "RS256" | "EdDSA" — the verify-side allowlist, restated here so
    /// a key that could never verify fails at LOAD time, not silently
    /// at request time.
    alg: String,
    /// PEM public key (SPKI).
    pem: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PoliciesDoc {
    feed_version: u64,
    projects: Vec<PolicyDoc>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDoc {
    project_id: ProjectId,
    workspace_id: WorkspaceId,
    cell_id: String,
    project_policy_version: u64,
    ownership_version: u64,
    status: ProjectStatus,
    #[serde(default)]
    quotas: ProjectQuotas,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantsDoc {
    feed_version: u64,
    credentials: Vec<GrantDoc>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantDoc {
    credential_id: String,
    project_id: ProjectId,
    grant_version: u64,
    status: CredentialStatus,
    /// Space-separated, same shape as the token's `scope` claim.
    scopes: String,
    /// Same claim semantics as the token (§4.2): absent/null = all
    /// streams; an EMPTY array is refused as an authoring error.
    #[serde(default)]
    stream_prefixes: Option<Vec<String>>,
    #[serde(default)]
    expires_at: Option<i64>,
}

// ---------------------------------------------------------------------
// Strict parsers (pure; the sources wrap file I/O around them)

pub fn parse_keys(json: &str, now: i64) -> anyhow::Result<JwksSnapshot> {
    let doc: KeysDoc = serde_json::from_str(json)?;
    anyhow::ensure!(!doc.keys.is_empty(), "keys file lists no keys");
    let mut keys = HashMap::new();
    for k in doc.keys {
        anyhow::ensure!(!k.kid.is_empty(), "key with empty kid");
        let (alg, dk) = match k.alg.as_str() {
            "RS256" => (
                jsonwebtoken::Algorithm::RS256,
                jsonwebtoken::DecodingKey::from_rsa_pem(k.pem.as_bytes())
                    .map_err(|e| anyhow::anyhow!("kid {:?}: bad RSA pem: {e}", k.kid))?,
            ),
            "EdDSA" => (
                jsonwebtoken::Algorithm::EdDSA,
                jsonwebtoken::DecodingKey::from_ed_pem(k.pem.as_bytes())
                    .map_err(|e| anyhow::anyhow!("kid {:?}: bad Ed25519 pem: {e}", k.kid))?,
            ),
            other => anyhow::bail!("kid {:?}: alg {other:?} is not in [RS256, EdDSA]", k.kid),
        };
        anyhow::ensure!(
            keys.insert(k.kid.clone(), crate::auth::JwksKey { alg, key: dk })
                .is_none(),
            "duplicate kid {:?}",
            k.kid
        );
    }
    Ok(JwksSnapshot {
        keys,
        fetched_at_unix: now,
        feed_version: doc.feed_version,
    })
}

pub fn parse_policies(json: &str, now: i64) -> anyhow::Result<PolicySnapshot> {
    let doc: PoliciesDoc = serde_json::from_str(json)?;
    let mut projects = HashMap::new();
    for p in doc.projects {
        crate::tenant::validate_cell_id(&p.cell_id)
            .map_err(|e| anyhow::anyhow!("project {:?}: bad cell_id: {e}", p.project_id))?;
        let policy = ProjectPolicy {
            project_id: p.project_id.clone(),
            workspace_id: p.workspace_id,
            cell_id: Arc::from(p.cell_id.as_str()),
            project_policy_version: p.project_policy_version,
            ownership_version: p.ownership_version,
            status: p.status,
            quotas: p.quotas,
        };
        anyhow::ensure!(
            projects.insert(p.project_id.clone(), policy).is_none(),
            "duplicate project {:?}",
            p.project_id
        );
    }
    Ok(PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: doc.feed_version,
    })
}

pub fn parse_grants(json: &str, now: i64) -> anyhow::Result<GrantSnapshot> {
    let doc: GrantsDoc = serde_json::from_str(json)?;
    let mut credentials = HashMap::new();
    for g in doc.credentials {
        anyhow::ensure!(!g.credential_id.is_empty(), "credential with empty id");
        let (scopes, unknown) = ScopeSet::parse(&g.scopes);
        anyhow::ensure!(
            unknown == 0,
            "credential {:?}: {unknown} unknown scope(s) in {:?} — a typo \
             here silently narrows authority, refuse it at load",
            g.credential_id,
            g.scopes
        );
        let grant = match g.stream_prefixes {
            None => StreamGrant::All,
            Some(v) if v.is_empty() => anyhow::bail!(
                "credential {:?}: stream_prefixes is an EMPTY array (grants \
                 nothing); omit the field for all-streams or list prefixes",
                g.credential_id
            ),
            Some(v) => {
                // Review item 6: the same SET normalizer as tokens —
                // per-prefix grammar, the 64-prefix cap, and
                // redundancy pruning.
                let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
                let set = crate::tenant::normalize_prefix_set(&refs).map_err(|e| {
                    anyhow::anyhow!("credential {:?}: stream_prefixes: {e:?}", g.credential_id)
                })?;
                StreamGrant::Prefixes(set.into())
            }
        };
        let cred = CredentialGrant {
            credential_id: Arc::from(g.credential_id.as_str()),
            project_id: g.project_id,
            grant_version: g.grant_version,
            status: g.status,
            scopes,
            grant,
            expires_at: g.expires_at,
        };
        anyhow::ensure!(
            credentials
                .insert(Arc::from(g.credential_id.as_str()), cred)
                .is_none(),
            "duplicate credential {:?}",
            g.credential_id
        );
    }
    Ok(GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: doc.feed_version,
    })
}

// ---------------------------------------------------------------------
// File-backed sources

pub struct FileKeySource(pub std::path::PathBuf);
pub struct FilePolicySource(pub std::path::PathBuf);
pub struct FileGrantSource(pub std::path::PathBuf);

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[async_trait::async_trait]
impl KeySource for FileKeySource {
    async fn fetch(&self) -> anyhow::Result<JwksSnapshot> {
        let raw = tokio::fs::read_to_string(&self.0).await?;
        parse_keys(&raw, unix_now())
    }
}

#[async_trait::async_trait]
impl PolicySource for FilePolicySource {
    async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
        let raw = tokio::fs::read_to_string(&self.0).await?;
        parse_policies(&raw, unix_now())
    }
}

#[async_trait::async_trait]
impl GrantSource for FileGrantSource {
    async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
        let raw = tokio::fs::read_to_string(&self.0).await?;
        parse_grants(&raw, unix_now())
    }
}

// ---------------------------------------------------------------------
// The refresher

/// Fetch all three feeds once, publishing whatever succeeds. Each feed
/// fails independently: a broken grants file must not stop key
/// rotation from landing.
pub async fn refresh_once(
    auth: &AuthService,
    keys: &dyn KeySource,
    policies: &dyn PolicySource,
    grants: &dyn GrantSource,
) {
    match keys.fetch().await {
        Ok(s) => {
            if let Err(why) = auth.publish_jwks(s) {
                tracing::warn!("auth feed: keys snapshot REFUSED ({why}); previous ages");
            }
        }
        Err(e) => tracing::warn!("auth feed: keys fetch failed (previous snapshot ages): {e}"),
    }
    match policies.fetch().await {
        Ok(s) => {
            if let Err(why) = auth.publish_policies(s) {
                tracing::warn!("auth feed: policy snapshot REFUSED ({why}); previous ages");
            }
        }
        Err(e) => tracing::warn!("auth feed: policy fetch failed (previous snapshot ages): {e}"),
    }
    match grants.fetch().await {
        Ok(s) => {
            if let Err(why) = auth.publish_grants(s) {
                tracing::warn!("auth feed: grants snapshot REFUSED ({why}); previous ages");
            }
        }
        Err(e) => tracing::warn!("auth feed: grants fetch failed (previous snapshot ages): {e}"),
    }
}

/// Background refresher: an immediate first fetch (so shadow counters
/// are meaningful from the first request), then a fixed cadence. The
/// cadence must be well inside `POLICY_STALENESS_MAX_SECS` or the cell
/// oscillates into staleness refusals; boot enforces that.
pub fn spawn_refresher(
    auth: Arc<AuthService>,
    keys: Box<dyn KeySource>,
    policies: Box<dyn PolicySource>,
    grants: Box<dyn GrantSource>,
    every: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // SR-4: the cadence is the FALLBACK; an unknown-kid nudge
            // (rate-limited in AuthService) refreshes immediately so a
            // rotated key's first token doesn't wait a full interval.
            tokio::select! {
                _ = tick.tick() => {}
                _ = auth.kid_wakeup.notified() => {}
            }
            refresh_once(&auth, keys.as_ref(), policies.as_ref(), grants.as_ref()).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refresh_once_publishes_from_files_and_survives_a_broken_one() {
        let dir = std::env::temp_dir().join(format!("mt-feed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let kp = dir.join("keys.json");
        let pp = dir.join("policies.json");
        let gp = dir.join("grants.json");
        std::fs::write(&kp, keys_json()).unwrap();
        std::fs::write(
            &pp,
            serde_json::json!({
                "feed_version": 1,
                "projects": [{
                    "project_id": "proj_456", "workspace_id": "ws_789",
                    "cell_id": "fra-cell-07", "project_policy_version": 1,
                    "ownership_version": 1, "status": "active"
                }]
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &gp,
            serde_json::json!({
                "feed_version": 1,
                "credentials": [{
                    "credential_id": "c1", "project_id": "proj_456",
                    "grant_version": 1, "status": "active",
                    "scopes": "streams.records.read"
                }]
            })
            .to_string(),
        )
        .unwrap();

        let svc = AuthService::new(
            crate::auth::AuthMode::Shadow,
            "https://auth.prisma.io".into(),
            "fra-cell-07",
        )
        .unwrap();
        let (ks, ps, gs) = (
            FileKeySource(kp.clone()),
            FilePolicySource(pp.clone()),
            FileGrantSource(gp.clone()),
        );
        refresh_once(&svc, &ks, &ps, &gs).await;
        let f = svc.feed_json(unix_now());
        assert_eq!(f["jwks"]["keys"], 1);
        assert_eq!(f["policies"]["projects"], 1);
        assert_eq!(f["grants"]["credentials"], 1);
        assert_eq!(f["policies"]["stale"], false);

        // Feeds fail independently: a broken grants file must not stop
        // key/policy refreshes, and the PREVIOUS grants snapshot stays
        // published (aging toward the staleness refusal).
        std::fs::write(&gp, "{ not json").unwrap();
        refresh_once(&svc, &ks, &ps, &gs).await;
        let f = svc.feed_json(unix_now());
        assert_eq!(f["grants"]["credentials"], 1, "old snapshot retained");
        assert_eq!(f["jwks"]["keys"], 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    const PUB: &str = include_str!("dst/fixtures/mt-test-rsa.pub.pem");
    const NOW: i64 = 1_786_600_600;

    fn keys_json() -> String {
        serde_json::json!({
            "keys": [{ "kid": "test-1", "alg": "RS256", "pem": PUB }]
        })
        .to_string()
    }

    #[test]
    fn keys_parse_and_reject() {
        let s = parse_keys(&keys_json(), NOW).unwrap();
        assert!(s.keys.contains_key("test-1"));
        assert_eq!(s.fetched_at_unix, NOW);
        // HMAC must fail at LOAD, per the §7 alg allowlist.
        let hmac = serde_json::json!({
            "keys": [{ "kid": "h", "alg": "HS256", "pem": PUB }]
        });
        assert!(parse_keys(&hmac.to_string(), NOW).is_err());
        assert!(parse_keys(r#"{"keys":[]}"#, NOW).is_err());
    }

    #[test]
    fn policies_parse_strictly() {
        let ok = serde_json::json!({
            "feed_version": 40,
            "projects": [{
                "project_id": "proj_456",
                "workspace_id": "ws_789",
                "cell_id": "fra-cell-07",
                "project_policy_version": 40,
                "ownership_version": 12,
                "status": "active"
            }]
        });
        let s = parse_policies(&ok.to_string(), NOW).unwrap();
        let pid = ProjectId::new("proj_456").unwrap();
        assert_eq!(s.projects[&pid].ownership_version, 12);
        assert_eq!(s.projects[&pid].status, ProjectStatus::Active);
        assert_eq!(s.feed_version, 40);
        // Unknown fields refuse: an operator typo (say "stauts") must
        // not silently leave the real field defaulted.
        let typo = ok.to_string().replace("\"status\"", "\"stauts\"");
        assert!(parse_policies(&typo, NOW).is_err());
    }

    #[test]
    fn grants_parse_strictly() {
        let base = serde_json::json!({
            "feed_version": 7,
            "credentials": [{
                "credential_id": "strcred_123",
                "project_id": "proj_456",
                "grant_version": 7,
                "status": "active",
                "scopes": "streams.records.read streams.records.append",
                "stream_prefixes": ["customers/acme"]
            }]
        });
        let s = parse_grants(&base.to_string(), NOW).unwrap();
        let c = &s.credentials[&Arc::from("strcred_123")];
        assert_eq!(c.grant_version, 7);
        assert!(matches!(&c.grant, StreamGrant::Prefixes(p) if p.len() == 1));

        // Unknown scope: refuse the load (typos must not silently
        // narrow authority).
        let bad_scope = base.to_string().replace(
            "streams.records.read",
            "streams.recods.read", // typo
        );
        assert!(parse_grants(&bad_scope, NOW).is_err());

        // EMPTY prefixes array: authoring error, refuse (§4.2).
        let empty = base.to_string().replace("[\"customers/acme\"]", "[]");
        assert!(parse_grants(&empty, NOW).is_err());

        // Absent prefixes = all streams.
        let absent = serde_json::json!({
            "feed_version": 7,
            "credentials": [{
                "credential_id": "c2",
                "project_id": "proj_456",
                "grant_version": 1,
                "status": "active",
                "scopes": "streams.records.read"
            }]
        });
        let s = parse_grants(&absent.to_string(), NOW).unwrap();
        assert!(matches!(
            s.credentials[&Arc::from("c2")].grant,
            StreamGrant::All
        ));
    }
}
