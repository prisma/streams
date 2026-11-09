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
//! Freshness belongs to the refresher, never to a source. A feed's age
//! counts from the pass that last asked its source and published the
//! answer, so no source can post-date its data or keep alive a feed it did
//! not fetch. For a FILE feed that means "read and parsed then", not "its
//! author is still publishing": an unchanged file stays fresh while it
//! stays readable and valid, and whether its author has published a newer
//! generation shows only as the policy and grant `feedVersion` on
//! `/v1/debug/auth`. A source therefore answers `Ok` only with what this
//! call obtained from its origin: a cached copy standing in for an
//! unreachable origin would restart the age the fail-closed window
//! measures, while an `Err` leaves the previous snapshot ageing toward
//! refusal.
//!
//! Parsing is STRICT. These files are authorization inputs: an
//! unknown scope name, an empty `stream_prefixes` array (§4.2: that
//! means "none", which no operator writes on purpose), or an
//! unsupported algorithm each fail the whole fetch — the previous
//! snapshot stays published and ages toward the staleness refusal,
//! which is exactly the §7.1 posture: loud, bounded, fail-closed.

use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{
    AUD_CUSTOMER, AUD_INTERNAL, AuthService, JwksKey, JwksSnapshot, KeyAudience, audience_shared,
};
use crate::project_policy::{
    CredentialStatus, GrantSnapshot, GrantSource, PolicySnapshot, PolicySource, ProjectQuotas,
    ProjectStatus,
};
use crate::tenant::ProjectId;

mod compact;

/// Verification keys, mirroring `PolicySource`/`GrantSource`.
#[async_trait::async_trait]
pub(crate) trait KeySource: Send + Sync {
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
    /// Shared-cells H6: the ONE token audience this key may sign for,
    /// `prisma-streams-data` (customer) or `prisma-streams-internal`
    /// (fleet workload). Required: a key the feed does not pin is refused.
    aud: String,
    /// PEM public key (SPKI).
    pem: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDoc {
    #[serde(rename = "container_id")]
    project_id: ProjectId,
    cell_id: String,
    #[serde(rename = "policy_version")]
    project_policy_version: u64,
    #[serde(rename = "generation")]
    ownership_version: u64,
    status: ProjectStatus,
    /// Required, as in the contract (shared-cells R3): a policy without
    /// quotas would be an unlimited project. Each field may be absent.
    quotas: ProjectQuotas,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantDoc {
    credential_id: String,
    #[serde(rename = "container_id")]
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
// Strict parsers (pure; the sources wrap file I/O around them). A parsed
// snapshot is unstamped (0, the never-fetched age): only the refresher
// stamps, so one that skipped it reads as unpublished and fails closed.

pub(crate) fn parse_keys(json: &str) -> anyhow::Result<JwksSnapshot> {
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
        let aud = KeyAudience::parse(&k.aud).ok_or_else(|| {
            anyhow::anyhow!(
                "kid {:?}: aud {:?} is not in [{AUD_CUSTOMER}, {AUD_INTERNAL}]",
                k.kid,
                k.aud
            )
        })?;
        anyhow::ensure!(
            keys.insert(k.kid.clone(), JwksKey::new(alg, dk, k.pem.as_bytes(), aud))
                .is_none(),
            "duplicate kid {:?}",
            k.kid
        );
    }
    if let Some((first, second)) = audience_shared(&keys) {
        anyhow::bail!("kids {first:?} and {second:?} name one key material under two audiences");
    }
    Ok(JwksSnapshot {
        keys,
        fetched_at_unix: 0,
        feed_version: doc.feed_version,
    })
}

/// A policy document's snapshot, parsed as the files are (`read_policy_file`).
#[cfg(test)]
pub(crate) fn parse_policies(json: impl AsRef<[u8]>) -> anyhow::Result<PolicySnapshot> {
    compact::read_policies(json.as_ref())
}

/// A grant document's snapshot, parsed as the files are (`read_grant_file`).
#[cfg(test)]
pub(crate) fn parse_grants(json: impl AsRef<[u8]>) -> anyhow::Result<GrantSnapshot> {
    compact::read_grants(json.as_ref())
}

// ---------------------------------------------------------------------
// File-backed sources

pub(crate) struct FileKeySource(pub std::path::PathBuf);
pub(crate) struct FilePolicySource(pub std::path::PathBuf);
pub(crate) struct FileGrantSource(pub std::path::PathBuf);

#[async_trait::async_trait]
impl KeySource for FileKeySource {
    async fn fetch(&self) -> anyhow::Result<JwksSnapshot> {
        let raw = tokio::fs::read_to_string(&self.0).await?;
        parse_keys(&raw)
    }
}

#[async_trait::async_trait]
impl PolicySource for FilePolicySource {
    async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
        let path = self.0.clone();
        on_a_blocking_thread(move || read_policy_file(&path)).await
    }
}

#[async_trait::async_trait]
impl GrantSource for FileGrantSource {
    async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
        let path = self.0.clone();
        on_a_blocking_thread(move || read_grant_file(&path)).await
    }
}

/// `read`'s answer, run on a blocking thread and joined: a feed file is
/// parsed as it is read, which must not hold an executor thread.
async fn on_a_blocking_thread<T: Send + 'static>(
    read: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    #[expect(
        clippy::disallowed_methods,
        reason = "auth feed file read; each refresh pass reads each feed file once and joins it, as tokio::fs::read's own blocking read did before the parse moved onto it; a pass past its deadline abandons it as it abandoned that read, and the read ends with the file"
    )]
    let read = tokio::task::spawn_blocking(read);
    read.await?
}

/// The policy snapshot the file at `path` holds, parsed as it is read on
/// the calling (blocking) thread: the file is never resident whole.
pub(crate) fn read_policy_file(path: &std::path::Path) -> anyhow::Result<PolicySnapshot> {
    compact::read_policies(std::io::BufReader::new(std::fs::File::open(path)?))
}

/// The grant snapshot the file at `path` holds, parsed as it is read on
/// the calling (blocking) thread.
pub(crate) fn read_grant_file(path: &std::path::Path) -> anyhow::Result<GrantSnapshot> {
    compact::read_grants(std::io::BufReader::new(std::fs::File::open(path)?))
}

// ---------------------------------------------------------------------
// The refresher

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A single pass has exactly three independent operations and a fixed
/// per-source deadline. Each successful source publishes immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefreshOutcome {
    Published,
    Refused,
    Unavailable,
    TimedOut,
}

#[derive(Debug)]
pub(crate) struct RefreshReport {
    pub keys: RefreshOutcome,
    pub policies: RefreshOutcome,
    pub grants: RefreshOutcome,
}

const SOURCE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// The one place freshness is stamped: the instant the pass asked,
/// attached only to a snapshot the source returned inside the deadline.
/// Asking time rather than answering time, because the origin's answer is
/// only known to be at least that new.
async fn refresh_source<T>(
    source_label: &str,
    fetch: impl std::future::Future<Output = anyhow::Result<T>>,
    publish: impl FnOnce(T, i64) -> Result<(), String>,
) -> RefreshOutcome {
    let asked_at = unix_now();
    match tokio::time::timeout(SOURCE_DEADLINE, fetch).await {
        Ok(Ok(snapshot)) => match publish(snapshot, asked_at) {
            Ok(()) => RefreshOutcome::Published,
            Err(why) => {
                tracing::warn!(source = source_label, %why, "auth snapshot refused; previous snapshot retained");
                RefreshOutcome::Refused
            }
        },
        Ok(Err(error)) => {
            tracing::warn!(source = source_label, %error, "auth fetch failed; previous snapshot retained");
            RefreshOutcome::Unavailable
        }
        Err(_) => {
            tracing::warn!(
                source = source_label,
                "auth fetch timed out; previous snapshot retained"
            );
            RefreshOutcome::TimedOut
        }
    }
}

pub(crate) async fn refresh_once(
    auth: &AuthService,
    keys: &dyn KeySource,
    policies: &dyn PolicySource,
    grants: &dyn GrantSource,
) -> RefreshReport {
    let (keys, policies, grants) = tokio::join!(
        refresh_source("keys", keys.fetch(), |s, fetched_at_unix| auth
            .publish_jwks(JwksSnapshot {
                fetched_at_unix,
                ..s
            })
            .map_err(|e| e.to_string())),
        refresh_source("policies", policies.fetch(), |s, fetched_at_unix| auth
            .publish_policies(PolicySnapshot {
                fetched_at_unix,
                ..s
            })
            .map_err(|e| e.to_string())),
        refresh_source("grants", grants.fetch(), |s, fetched_at_unix| auth
            .publish_grants(GrantSnapshot {
                fetched_at_unix,
                ..s
            })
            .map_err(|e| e.to_string())),
    );
    RefreshReport {
        keys,
        policies,
        grants,
    }
}

/// Background refresher: an immediate first fetch (so shadow counters
/// are meaningful from the first request), then a fixed cadence. The
/// cadence must be well inside `POLICY_STALENESS_MAX_SECS` or the cell
/// oscillates into staleness refusals; boot enforces that.
#[expect(
    clippy::too_many_arguments,
    reason = "spawn_refresher; the refresher takes the auth service, its three sources, the cadence and the supervisor as boot wires them; a bundle struct would exist only for this signature"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "spawn_refresher; the supervisor rejects a spawn only while it is stopping, when no refresh is owed; a rejected refresher has nothing left to fetch"
)]
pub(crate) fn spawn_refresher(
    auth: Arc<AuthService>,
    keys: Box<dyn KeySource>,
    policies: Box<dyn PolicySource>,
    grants: Box<dyn GrantSource>,
    every: std::time::Duration,
    tasks: &crate::tasks::TaskSupervisor,
) {
    let _ = tasks.spawn(
        "auth-refresher",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                // SR-4: the cadence is the FALLBACK; an unknown-kid nudge
                // (rate-limited in AuthService) refreshes immediately so a
                // rotated key's first token doesn't wait a full interval.
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tick.tick() => {}
                    _ = auth.kid_wakeup.notified() => {}
                }
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    report = refresh_once(&auth, keys.as_ref(), policies.as_ref(), grants.as_ref()) => {
                        tracing::debug!(keys = ?report.keys, policies = ?report.policies,
                            grants = ?report.grants, "authentication feed pass completed");
                    }
                }
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::StreamGrant;

    struct ParkedKeys;
    #[async_trait::async_trait]
    impl KeySource for ParkedKeys {
        async fn fetch(&self) -> anyhow::Result<JwksSnapshot> {
            std::future::pending().await
        }
    }
    struct EmptyPolicies;
    #[async_trait::async_trait]
    impl PolicySource for EmptyPolicies {
        async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
            parse_policies(r#"{"feed_version":1,"containers":[]}"#)
        }
    }
    struct EmptyGrants;
    #[async_trait::async_trait]
    impl GrantSource for EmptyGrants {
        async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
            parse_grants(r#"{"feed_version":1,"credentials":[]}"#)
        }
    }

    #[tokio::test(start_paused = true, crate = "crate::test_rt")]
    async fn slow_keys_do_not_delay_other_publications_and_pass_is_bounded() {
        let svc = AuthService::new(crate::auth::AuthMode::Shadow, "issuer".into(), "cell").unwrap();
        let pass = refresh_once(&svc, &ParkedKeys, &EmptyPolicies, &EmptyGrants);
        tokio::pin!(pass);
        tokio::select! {
            _ = &mut pass => panic!("parked source must not complete yet"),
            _ = tokio::task::yield_now() => {}
        }
        let feeds = svc.feed_json(unix_now());
        assert!(!feeds["policies"]["ageSecs"].is_null());
        assert!(!feeds["grants"]["ageSecs"].is_null());
        tokio::time::advance(SOURCE_DEADLINE).await;
        let report = pass.await;
        assert_eq!(report.keys, RefreshOutcome::TimedOut);
        assert_eq!(report.policies, RefreshOutcome::Published);
        assert_eq!(report.grants, RefreshOutcome::Published);
    }

    #[tokio::test(crate = "crate::test_rt")]
    async fn cancelling_refresh_does_not_wait_for_source_deadline() {
        let svc = Arc::new(
            AuthService::new(crate::auth::AuthMode::Shadow, "issuer".into(), "cell").unwrap(),
        );
        let tasks = crate::tasks::TaskSupervisor::new();
        spawn_refresher(
            svc,
            Box::new(ParkedKeys),
            Box::new(EmptyPolicies),
            Box::new(EmptyGrants),
            std::time::Duration::from_secs(1),
            &tasks,
        );
        tokio::task::yield_now().await;
        let report = tasks.shutdown(std::time::Duration::from_millis(100)).await;
        assert!(report.aborted.is_empty());
        assert_eq!(report.finished(), vec!["auth-refresher"]);
    }

    #[expect(
        clippy::let_underscore_must_use,
        reason = "refresh_once_publishes_from_files_and_survives_a_broken_one; the fixture removes its temporary directory on the way out; a failed removal leaves nothing the assertions depend on"
    )]
    #[tokio::test(crate = "crate::test_rt")]
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
                "containers": [{
                    "container_id": "proj_456", "cell_id": "fra-cell-07", "policy_version": 1,
                    "generation": 1, "status": "active", "quotas": {}
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
                    "credential_id": "c1", "container_id": "proj_456",
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

    /// A file feed's freshness is its last successful read, not its
    /// author's liveness: a failed read refreshes nothing, and the same
    /// generation read again is fresh although its author wrote nothing.
    #[tokio::test(crate = "crate::test_rt")]
    async fn file_feed_freshness_means_last_successful_read_not_publisher_liveness() {
        let dir = std::env::temp_dir().join(format!("mt-feed-freshness-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (kp, pp, gp) = (
            dir.join("keys.json"),
            dir.join("policies.json"),
            dir.join("grants.json"),
        );
        let svc = AuthService::new(crate::auth::AuthMode::Shadow, "issuer".into(), "cell").unwrap();
        // The author last wrote generation 7 about 250 s ago.
        let written = unix_now() - 250;
        svc.publish_policies(PolicySnapshot {
            fetched_at_unix: written,
            feed_version: 7,
            ..PolicySnapshot::empty()
        })
        .unwrap();
        svc.publish_grants(GrantSnapshot {
            fetched_at_unix: written,
            feed_version: 7,
            ..GrantSnapshot::empty()
        })
        .unwrap();
        let (ks, ps, gs) = (
            FileKeySource(kp.clone()),
            FilePolicySource(pp.clone()),
            FileGrantSource(gp.clone()),
        );
        for path in [&kp, &pp, &gp] {
            std::fs::write(path, "{ not json").unwrap();
        }
        let report = refresh_once(&svc, &ks, &ps, &gs).await;
        assert_eq!(
            [report.policies, report.grants],
            [RefreshOutcome::Unavailable; 2]
        );
        let feeds = svc.feed_json(unix_now());
        for feed in ["policies", "grants"] {
            let age = feeds[feed]["ageSecs"].as_i64();
            assert!(
                age.is_some_and(|a| a >= 250),
                "{feed}: a failed read refreshes nothing"
            );
        }
        std::fs::write(&pp, r#"{"feed_version":7,"containers":[]}"#).unwrap();
        std::fs::write(&gp, r#"{"feed_version":7,"credentials":[]}"#).unwrap();
        let report = refresh_once(&svc, &ks, &ps, &gs).await;
        assert_eq!(
            [report.policies, report.grants],
            [RefreshOutcome::Published; 2]
        );
        let feeds = svc.feed_json(unix_now());
        for feed in ["policies", "grants"] {
            let age = feeds[feed]["ageSecs"].as_i64();
            assert!(
                age.is_some_and(|a| (0..=60).contains(&a)),
                "{feed}: re-reading the same generation is fresh"
            );
            assert_eq!(feeds[feed]["feedVersion"], 7, "{feed}: no newer generation");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Stands in for a cached origin that stamps its own reads: the
    /// refresher must not take a source's word for how fresh its answer is.
    struct SelfStamped(i64);
    #[async_trait::async_trait]
    impl KeySource for SelfStamped {
        async fn fetch(&self) -> anyhow::Result<JwksSnapshot> {
            Ok(JwksSnapshot {
                fetched_at_unix: self.0,
                ..JwksSnapshot::empty()
            })
        }
    }
    #[async_trait::async_trait]
    impl PolicySource for SelfStamped {
        async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
            Ok(PolicySnapshot {
                fetched_at_unix: self.0,
                ..PolicySnapshot::empty()
            })
        }
    }
    #[async_trait::async_trait]
    impl GrantSource for SelfStamped {
        async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
            Ok(GrantSnapshot {
                fetched_at_unix: self.0,
                ..GrantSnapshot::empty()
            })
        }
    }

    /// Item 93: freshness belongs to the refresher. An ancient source stamp
    /// would refuse fresh data; a post-dated one would keep a dead feed
    /// authorizing past the fail-closed window.
    #[tokio::test(crate = "crate::test_rt")]
    async fn the_refresher_not_the_source_stamps_feed_freshness() {
        for stamp in [1, unix_now() + 86_400] {
            let svc =
                AuthService::new(crate::auth::AuthMode::Shadow, "issuer".into(), "cell").unwrap();
            let source = SelfStamped(stamp);
            let report = refresh_once(&svc, &source, &source, &source).await;
            assert_eq!(
                [report.keys, report.policies, report.grants],
                [RefreshOutcome::Published; 3]
            );
            assert_eq!(
                stale_feeds(&svc),
                Vec::<&str>::new(),
                "a published fetch is fresh whatever the source stamped ({stamp})"
            );
        }
    }

    /// The feeds whose age is outside the fresh window right now.
    fn stale_feeds(svc: &AuthService) -> Vec<&'static str> {
        let feeds = svc.feed_json(unix_now());
        ["jwks", "policies", "grants"]
            .into_iter()
            .filter(|f| {
                !feeds[*f]["ageSecs"]
                    .as_i64()
                    .is_some_and(|a| (0..=60).contains(&a))
            })
            .collect()
    }

    const PUB: &str = include_str!("dst/fixtures/mt-test-rsa.pub.pem");

    fn keys_json() -> String {
        serde_json::json!({
            "keys": [{ "kid": "test-1", "alg": "RS256", "aud": AUD_CUSTOMER, "pem": PUB }]
        })
        .to_string()
    }

    #[test]
    fn keys_parse_and_reject() {
        let s = parse_keys(&keys_json()).unwrap();
        assert!(s.keys.contains_key("test-1"));
        assert_eq!(
            s.fetched_at_unix, 0,
            "parsing never stamps freshness; the refresher does"
        );
        // HMAC must fail at LOAD, per the §7 alg allowlist.
        let hmac = serde_json::json!({
            "keys": [{ "kid": "h", "alg": "HS256", "aud": AUD_CUSTOMER, "pem": PUB }]
        });
        assert!(parse_keys(&hmac.to_string()).is_err());
        assert!(parse_keys(r#"{"keys":[]}"#).is_err());
    }

    /// Isolation review F2: a keys feed that names one public key under a
    /// customer kid and again under an internal kid pins nothing: the
    /// customer issuer's key would sign workload tokens under the second
    /// kid. Such a feed is refused whole, as a key without `aud` is.
    #[test]
    fn a_key_signs_for_one_audience_whatever_kid_names_it() {
        let mut doc: serde_json::Value = serde_json::from_str(&keys_json()).unwrap();
        let mut fleet = doc["keys"][0].clone();
        fleet["kid"] = "fleet-1".into();
        fleet["aud"] = AUD_INTERNAL.into();
        doc["keys"].as_array_mut().unwrap().push(fleet);
        let parsed = parse_keys(&doc.to_string()).map(|s| s.keys.len());
        assert!(
            parsed.is_err(),
            "one PEM under both audiences parsed as {parsed:?}"
        );
    }

    /// Shared-cells H6: every key names the ONE audience it may sign for,
    /// and a key that names none, or one this cell does not verify, refuses
    /// the whole feed (the previous key set stays and ages toward refusal).
    #[test]
    fn keys_pin_each_kid_to_one_audience() {
        let doc = |aud: Option<&str>| {
            let mut doc: serde_json::Value = serde_json::from_str(&keys_json()).unwrap();
            let key = doc["keys"][0].as_object_mut().unwrap();
            match aud {
                Some(aud) => key.insert("aud".into(), aud.into()),
                None => key.remove("aud"),
            };
            doc.to_string()
        };
        for (case, aud) in [
            ("a key without aud", None),
            ("an unknown aud", Some("prisma-streams-admin")),
            ("an empty aud", Some("")),
        ] {
            let parsed = parse_keys(&doc(aud)).map(|s| s.keys.len());
            assert!(parsed.is_err(), "{case} parsed as {parsed:?}");
        }
        for (aud, pinned) in [
            (AUD_CUSTOMER, KeyAudience::Customer),
            (AUD_INTERNAL, KeyAudience::Internal),
        ] {
            let parsed = parse_keys(&doc(Some(aud))).unwrap();
            let key = &parsed.keys["test-1"];
            assert_eq!(key.aud, pinned, "{aud}");
            assert_eq!(key.fp, crate::auth::key_fp(PUB.as_bytes(), pinned));
        }
    }

    /// The platform contract's golden key vectors parse here exactly as
    /// its schema judges them (comments stripped): the valid snapshot pins
    /// its key to the customer audience, and each hostile one is refused.
    #[test]
    fn golden_key_vectors_parse_as_the_schema_judges_them() {
        let golden = |json: &str| {
            let mut doc: serde_json::Value = serde_json::from_str(json).unwrap();
            doc.as_object_mut().unwrap().remove("_comment");
            parse_keys(&doc.to_string())
        };
        let valid = golden(include_str!(
            "../contracts/streams-platform/v1/golden/keys.valid.json"
        ))
        .unwrap();
        let pins: Vec<KeyAudience> = valid.keys.values().map(|k| k.aud).collect();
        assert_eq!(pins, [KeyAudience::Customer]);
        for (case, json) in [
            (
                "missing aud",
                include_str!(
                    "../contracts/streams-platform/v1/golden/keys.hostile-missing-aud.json"
                ),
            ),
            (
                "unknown alg",
                include_str!(
                    "../contracts/streams-platform/v1/golden/keys.hostile-unknown-alg.json"
                ),
            ),
        ] {
            let parsed = golden(json).map(|s| s.keys.len());
            assert!(parsed.is_err(), "{case} parsed as {parsed:?}");
        }
    }

    /// The platform contract's golden policy vectors parse here exactly as
    /// its schema judges them (comments stripped): the valid snapshot, which
    /// sets a project's stored-bytes quota, parses, and the one with a
    /// negative quota is refused whole.
    #[test]
    fn golden_policy_vectors_parse_as_the_schema_judges_them() {
        let golden = |json: &str| {
            let mut doc: serde_json::Value = serde_json::from_str(json).unwrap();
            doc.as_object_mut().unwrap().remove("_comment");
            parse_policies(doc.to_string())
        };
        let valid = golden(include_str!(
            "../contracts/streams-platform/v1/golden/container-policies.valid.json"
        ))
        .unwrap();
        let pid = ProjectId::new("proj-golden").unwrap();
        assert_eq!(valid.projects[&pid].status, ProjectStatus::Active);
        let hostile = golden(include_str!(
            "../contracts/streams-platform/v1/golden/container-policies.hostile-negative-stored.json"
        ))
        .map(|s| s.projects.len());
        assert!(hostile.is_err(), "a negative quota parsed as {hostile:?}");
    }

    /// A policy carries the project's stored-bytes quota; one without it
    /// parses at 0, "not configured", which the cell's ceiling resolves.
    #[test]
    fn a_policy_feed_carries_max_stored_bytes() {
        let stored = |quotas: &str| {
            let doc = r#"{"feed_version": 1, "containers": [{"container_id": "proj_456",
                "cell_id": "fra-cell-07",
                "policy_version": 1, "generation": 1,
                "status": "active", "quotas": QUOTAS}]}"#;
            let s = parse_policies(doc.replace("QUOTAS", quotas)).unwrap();
            s.projects[&ProjectId::new("proj_456").unwrap()]
                .quotas
                .max_stored_bytes
        };
        assert_eq!(stored(r#"{"max_stored_bytes": 1073741824}"#), 1 << 30);
        assert_eq!(stored("{}"), 0);
    }

    /// Edge #253: the feed names a container and no workspace or owner. A
    /// policy that still names its workspace, or a token-side owner field,
    /// is refused whole (unknown field), as the old key `projects` is.
    #[test]
    fn a_policy_naming_a_workspace_is_refused() {
        let feed = |entry: &str| {
            parse_policies(format!(
                r#"{{"feed_version": 1, "containers": [{{"container_id": "ct_1",
                "cell_id": "fra-cell-07", "policy_version": 1, "generation": 1,
                "status": "active", "quotas": {{}}{entry}}}]}}"#
            ))
            .map(|s| s.projects.len())
        };
        assert_eq!(feed("").unwrap(), 1);
        assert!(feed(r#", "workspace_id": "ws_789""#).is_err());
        let old_key = parse_policies(r#"{"feed_version": 1, "projects": []}"#);
        assert!(old_key.is_err(), "the old key parsed");
    }

    #[test]
    fn policies_parse_strictly() {
        let ok = serde_json::json!({
            "feed_version": 40,
            "containers": [{
                "container_id": "proj_456",
                "cell_id": "fra-cell-07",
                "policy_version": 40,
                "generation": 12,
                "status": "active",
                "quotas": { "requests_per_sec": 100 }
            }]
        });
        let s = parse_policies(ok.to_string()).unwrap();
        let pid = ProjectId::new("proj_456").unwrap();
        assert_eq!(s.projects[&pid].ownership_version, 12);
        assert_eq!(s.projects[&pid].status, ProjectStatus::Active);
        assert_eq!(s.projects[&pid].quotas.requests_per_sec, 100);
        assert_eq!(s.feed_version, 40);
        // Unknown fields refuse: an operator typo (say "stauts") must
        // not silently leave the real field defaulted.
        let typo = ok.to_string().replace("\"status\"", "\"stauts\"");
        assert!(parse_policies(&typo).is_err());
        // Quotas fail closed (shared-cells R3): a misspelled quota key, or
        // no quotas object at all, would leave every field 0, unlimited.
        let mut bare = ok.clone();
        bare["containers"][0]
            .as_object_mut()
            .unwrap()
            .remove("quotas");
        for (case, doc) in [
            (
                "a misspelled quota key",
                ok.to_string().replace("requests_", "request_"),
            ),
            ("a policy without quotas", bare.to_string()),
        ] {
            let parsed = parse_policies(&doc).map(|s| format!("{:?}", s.projects[&pid].quotas));
            assert!(parsed.is_err(), "{case} parsed as {parsed:?}");
        }
    }

    #[test]
    fn grants_parse_strictly() {
        let base = serde_json::json!({
            "feed_version": 7,
            "credentials": [{
                "credential_id": "strcred_123",
                "container_id": "proj_456",
                "grant_version": 7,
                "status": "active",
                "scopes": "streams.records.read streams.records.append",
                "stream_prefixes": ["customers/acme"]
            }]
        });
        let s = parse_grants(base.to_string()).unwrap();
        let c = &s.credentials[&Arc::from("strcred_123")];
        assert_eq!(c.grant_version, 7);
        assert!(matches!(&c.grant, StreamGrant::Prefixes(p) if p.len() == 1));

        // Unknown scope: refuse the load (typos must not silently
        // narrow authority).
        let bad_scope = base.to_string().replace(
            "streams.records.read",
            "streams.recods.read", // typo
        );
        assert!(parse_grants(&bad_scope).is_err());

        // EMPTY prefixes array: authoring error, refuse (§4.2).
        let empty = base.to_string().replace("[\"customers/acme\"]", "[]");
        assert!(parse_grants(&empty).is_err());

        // Absent prefixes = all streams.
        let absent = serde_json::json!({
            "feed_version": 7,
            "credentials": [{
                "credential_id": "c2",
                "container_id": "proj_456",
                "grant_version": 1,
                "status": "active",
                "scopes": "streams.records.read"
            }]
        });
        let s = parse_grants(absent.to_string()).unwrap();
        assert!(matches!(
            s.credentials[&Arc::from("c2")].grant,
            StreamGrant::All
        ));
    }

    /// A feed whose list is not a list is refused, and the refusal names
    /// what the feed should hold there.
    #[test]
    fn a_feed_whose_list_is_not_a_list_names_what_it_expected() {
        let policies = parse_policies(r#"{"feed_version":1,"containers":5}"#).unwrap_err();
        let grants = parse_grants(r#"{"feed_version":1,"credentials":"x"}"#).unwrap_err();
        let (policies, grants) = (policies.to_string(), grants.to_string());
        assert!(
            policies.contains("expected a list of container policies"),
            "{policies}"
        );
        assert!(
            grants.contains("expected a list of credential grants"),
            "{grants}"
        );
    }

    /// A feed of `n` projects, each with one credential, on one cell and
    /// one quota set, as the policy and grant documents.
    fn feed_of(n: usize) -> (String, String) {
        let quotas =
            r#"{"requests_per_sec":100,"append_bytes_per_sec":1048576,"max_streams":1000}"#;
        let (mut policies, mut grants) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for i in 0..n {
            policies.push(format!(
                r#"{{"container_id":"proj_{i:08}","cell_id":"test-cell","policy_version":1,"generation":1,"status":"active","quotas":{quotas}}}"#
            ));
            grants.push(format!(
                r#"{{"credential_id":"strcred_{i:08}","container_id":"proj_{i:08}","grant_version":1,"status":"active","scopes":"streams.records.read streams.records.append"}}"#
            ));
        }
        (
            format!(
                r#"{{"feed_version":1,"containers":[{}]}}"#,
                policies.join(",")
            ),
            format!(
                r#"{{"feed_version":1,"credentials":[{}]}}"#,
                grants.join(",")
            ),
        )
    }

    /// Tuning item 3c, step 1: parsing a 100,000-tenant feed (a policy and
    /// a credential a project) holds at most 60 MB at once beyond its input,
    /// the two snapshots it returns included, and returns every entry.
    #[test]
    fn a_100k_tenant_feed_parses_within_60_mb() {
        let (policies, grants) = feed_of(100_000);
        let ((p, g), peak) = crate::test_alloc::peak_bytes(|| {
            let p = parse_policies(&policies).unwrap();
            (p, parse_grants(&grants).unwrap())
        });
        assert_eq!((p.projects.len(), g.credentials.len()), (100_000, 100_000));
        assert!(peak <= 60 << 20, "the parse held {peak} bytes at once");
    }

    /// Tuning item 3c, step 1: reading a 100,000-tenant feed's files
    /// never holds a whole file: each file's read holds at most what its
    /// parse holds plus 1 MiB, its snapshot included.
    #[test]
    fn a_100k_tenant_feed_file_is_read_without_holding_the_file() {
        let (policies, grants) = feed_of(100_000);
        let dir = std::env::temp_dir().join(format!("feed-read-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (pp, gp) = (dir.join("policies.json"), dir.join("grants.json"));
        std::fs::write(&pp, &policies).unwrap();
        std::fs::write(&gp, &grants).unwrap();
        let (parsed, parse_peak) = crate::test_alloc::peak_bytes(|| parse_policies(&policies));
        let (read, read_peak) = crate::test_alloc::peak_bytes(|| read_policy_file(&pp));
        let (grants_parsed, grants_parse_peak) =
            crate::test_alloc::peak_bytes(|| parse_grants(&grants));
        let (grants_read, grants_read_peak) =
            crate::test_alloc::peak_bytes(|| read_grant_file(&gp));
        std::fs::remove_dir_all(&dir).unwrap();
        let counts =
            |p: &PolicySnapshot, g: &GrantSnapshot| (p.projects.len(), g.credentials.len());
        let (parsed, grants_parsed) = (parsed.unwrap(), grants_parsed.unwrap());
        let (read, grants_read) = (read.unwrap(), grants_read.unwrap());
        assert_eq!(counts(&read, &grants_read), counts(&parsed, &grants_parsed));
        assert_eq!(counts(&read, &grants_read), (100_000, 100_000));
        assert!(
            read_peak <= parse_peak + (1 << 20)
                && grants_read_peak <= grants_parse_peak + (1 << 20),
            "policies: read {read_peak}, parse {parse_peak}; grants: read {grants_read_peak}, parse {grants_parse_peak}"
        );
    }
}
