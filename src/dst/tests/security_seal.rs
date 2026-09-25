//! Seal authorization: a seal carrying a final record appends that record,
//! so it needs `streams.records.append` as well as the
//! `streams.lifecycle.manage` every seal needs (owner decision, second
//! external review of 9813d1cb). A refused final-record seal changes
//! nothing: no record, no producer state, no seal intent or owed final.
use super::fixture_auth::{auth_rig, rig_bearer, rig_create};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::project_policy::{CredentialGrant, CredentialStatus, GrantSnapshot};
use crate::tenant::{ProjectId, Scope, ScopeSet, StreamGrant};
use std::sync::Arc;

const PROJECT: (&str, &str) = ("proj-seal", "ws-seal");
const LIFECYCLE: &str = "streams.lifecycle.manage streams.records.read streams.metadata.read";
const APPEND: &str = "streams.records.append streams.records.read";

/// Grant version 2 of the rig's credentials: `c-full` every scope,
/// `c-life` lifecycle without append, `c-append` append without
/// lifecycle. Returns a bearer for each, in that order.
fn bearers(svc: &crate::auth::AuthService) -> [String; 3] {
    let all = Scope::ALL.map(Scope::as_str).join(" ");
    let grants = [
        ("c-full", all.as_str()),
        ("c-life", LIFECYCLE),
        ("c-append", APPEND),
    ];
    let credentials = grants.iter().map(|&(cred, scopes)| {
        let credential = CredentialGrant {
            credential_id: Arc::from(cred),
            project_id: ProjectId::new(PROJECT.0).unwrap(),
            grant_version: 2,
            status: CredentialStatus::Active,
            scopes: ScopeSet::parse(scopes).0,
            grant: StreamGrant::All,
            expires_at: None,
        };
        (Arc::from(cred), credential)
    });
    svc.publish_grants(GrantSnapshot {
        credentials: credentials.collect(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    grants.map(|(cred, scopes)| rig_bearer(PROJECT, cred, scopes, 2))
}

/// POST `body` to `name`:seal as `bearer`, with the stream key and, when
/// `producer` is set, a producer triple: the status and error code.
async fn seal(
    addr: std::net::SocketAddr,
    bearer: &str,
    name: &str,
    body: &str,
    producer: bool,
) -> (u16, String) {
    let mut headers = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", bearer),
    ];
    if producer {
        headers.extend([
            ("producer-id", "p-final"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ]);
    }
    let path = format!("/v1/streams/{name}:seal");
    let (st, _, resp) = preq(addr, "POST", &path, &headers, body.as_bytes()).await;
    let answer: serde_json::Value = serde_json::from_slice(&resp).unwrap_or_default();
    (
        st,
        answer["error"]["code"].as_str().unwrap_or("").to_string(),
    )
}

/// Append one record to `name` as `bearer`, as producer `p-final` seq 0
/// when `producer` is set: the status.
async fn append(addr: std::net::SocketAddr, bearer: &str, name: &str, producer: bool) -> u16 {
    let mut headers = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", bearer),
    ];
    if producer {
        headers.extend([
            ("producer-id", "p-final"),
            ("producer-epoch", "0"),
            ("producer-seq", "0"),
        ]);
    }
    let path = format!("/v1/streams/{name}/records");
    preq(addr, "POST", &path, &headers, br#"{"n":1}"#).await.0
}

/// The records `name` holds and whether it is sealed, read as `bearer`.
async fn state_of(addr: std::net::SocketAddr, bearer: &str, name: &str) -> (usize, bool) {
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", bearer),
    ];
    let path = format!("/v1/streams/{name}/records");
    let (st, _, body) = preq(addr, "GET", &path, &headers, b"").await;
    assert_eq!(st, 200, "read {name}");
    let records: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let path = format!("/v1/streams/{name}");
    let (st, _, body) = preq(addr, "GET", &path, &headers, b"").await;
    assert_eq!(st, 200, "metadata {name}");
    let meta: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (records.len(), meta["sealed"].as_bool().unwrap())
}

/// Red before the change: `c-life` (lifecycle, no append) sealed with a
/// final record (200) and the record landed. Now a final record, present
/// even as `null`, needs append too: 403 `missing_scope`, and the stream
/// keeps its record count, stays unsealed and open to ordinary appends,
/// and the refused request's producer triple is still unused. A body
/// without `final` is a plain seal and needs lifecycle alone; an append
/// credential never gets lifecycle authority through a final record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_final_record_seal_needs_append_as_well_as_lifecycle() {
    let (svc, state, addr) = auth_rig(
        PROJECT.0,
        PROJECT.1,
        &["c-full", "c-life", "c-append"],
        None,
    )
    .await;
    let [full, life, append_only] = bearers(&svc);
    rig_create(addr, "a/refused", &full).await;
    assert_eq!(append(addr, &full, "a/refused", false).await, 200);

    for body in [r#"{"final":{"n":2}}"#, r#"{"final":null}"#] {
        for producer in [false, true] {
            let refused = seal(addr, &life, "a/refused", body, producer).await;
            assert_eq!(refused, (403, "missing_scope".into()), "{body}");
        }
    }
    for body in ["", "{}", r#"{"final":{"n":2}}"#] {
        let refused = seal(addr, &append_only, "a/refused", body, false).await;
        assert_eq!(
            refused,
            (403, "missing_scope".into()),
            "append-only {body:?}"
        );
    }
    // Nothing changed: one record, not sealed, ordinary appends still
    // land (no seal intent owes a final), and the refused producer triple
    // commits as new.
    assert_eq!(state_of(addr, &full, "a/refused").await, (1, false));
    assert_eq!(append(addr, &full, "a/refused", true).await, 200);
    assert_eq!(state_of(addr, &full, "a/refused").await, (2, false));

    // A plain seal (no body, or a body without `final`) needs lifecycle
    // alone.
    for (name, body) in [("a/plain", ""), ("a/plain-doc", "{}")] {
        rig_create(addr, name, &full).await;
        assert_eq!(
            seal(addr, &life, name, body, false).await,
            (200, String::new())
        );
        assert_eq!(state_of(addr, &full, name).await, (0, true), "{name}");
    }
    // Both scopes: the final record lands and seals.
    rig_create(addr, "a/final", &full).await;
    let sealed = seal(addr, &full, "a/final", r#"{"final":{"n":2}}"#, false).await;
    assert_eq!(sealed, (200, String::new()));
    assert_eq!(state_of(addr, &full, "a/final").await, (1, true));
    engine_shutdown(&state).await;
}
