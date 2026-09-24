//! Product-operation authorization: the §6.1 scope each dispatched
//! operation demands, and the authentication that precedes a route refusal
//! (404/405) for a request naming no operation.
use super::fixture_auth::{auth_rig, rig_scoped_bearer};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::tenant::Scope;

const PROJECT: (&str, &str) = ("proj-ops", "ws-ops");
const CREDENTIAL: &str = "c-ops";

/// Every operation the product entry dispatches, on a stream that never
/// exists (every handler refuses fast and nothing is created), with the one
/// scope the gate must demand.
const OPERATIONS: [(&str, &str, Option<Scope>); 19] = [
    ("PUT", "/v1/streams/opgrid", Some(Scope::Create)),
    ("GET", "/v1/streams/opgrid", Some(Scope::MetadataRead)),
    ("DELETE", "/v1/streams/opgrid", Some(Scope::LifecycleManage)),
    (
        "POST",
        "/v1/streams/opgrid:seal",
        Some(Scope::LifecycleManage),
    ),
    ("GET", "/v1/streams/opgrid:scan", Some(Scope::RecordsRead)),
    (
        "POST",
        "/v1/streams/opgrid/records",
        Some(Scope::RecordsAppend),
    ),
    (
        "POST",
        "/v1/streams/opgrid/records:batch",
        Some(Scope::RecordsAppend),
    ),
    (
        "GET",
        "/v1/streams/opgrid/records",
        Some(Scope::RecordsRead),
    ),
    (
        "GET",
        "/v1/streams/opgrid/records:long-poll",
        Some(Scope::RecordsRead),
    ),
    (
        "GET",
        "/v1/streams/opgrid/records:sse",
        Some(Scope::RecordsRead),
    ),
    (
        "PUT",
        "/v1/streams/opgrid/consumers/c",
        Some(Scope::ConsumersConfigure),
    ),
    (
        "GET",
        "/v1/streams/opgrid/consumers/c",
        Some(Scope::MetadataRead),
    ),
    (
        "DELETE",
        "/v1/streams/opgrid/consumers/c",
        Some(Scope::ConsumersConfigure),
    ),
    (
        "POST",
        "/v1/streams/opgrid/consumers/c:pull",
        Some(Scope::ConsumersPull),
    ),
    (
        "POST",
        "/v1/streams/opgrid/consumers/c:settle",
        Some(Scope::ConsumersSettle),
    ),
    (
        "GET",
        "/v1/streams/opgrid/watches",
        Some(Scope::MetadataRead),
    ),
    (
        "GET",
        "/v1/streams/opgrid/watches/w",
        Some(Scope::MetadataRead),
    ),
    ("GET", "/v1/streams/opgrid/usage", Some(Scope::UsageRead)),
    (
        "GET",
        "/v1/streams/opgrid/watches/w/keys/0011223344556677",
        None,
    ),
];

/// Requests the entry refuses by route, with the refusal a fully scoped
/// credential gets.
const UNDISPATCHABLE: [(&str, &str, u16, &str); 8] = [
    ("POST", "/v1/streams/opgrid", 404, "unknown_route"),
    ("PUT", "/v1/streams/opgrid:seal", 404, "unknown_route"),
    (
        "PATCH",
        "/v1/streams/opgrid/records",
        405,
        "method_not_allowed",
    ),
    (
        "GET",
        "/v1/streams/opgrid/records:batch",
        405,
        "method_not_allowed",
    ),
    (
        "GET",
        "/v1/streams/opgrid/consumers/c:pull",
        405,
        "method_not_allowed",
    ),
    (
        "DELETE",
        "/v1/streams/opgrid/watches",
        405,
        "method_not_allowed",
    ),
    (
        "POST",
        "/v1/streams/opgrid/usage",
        405,
        "method_not_allowed",
    ),
    (
        "PUT",
        "/v1/streams/opgrid/watches/w/keys/0011223344556677",
        405,
        "method_not_allowed",
    ),
];

fn error_code(body: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Space-separated scope claim for `scopes`.
fn claim(scopes: impl Iterator<Item = Scope>) -> String {
    scopes.map(Scope::as_str).collect::<Vec<_>>().join(" ")
}

/// One request under a credential republished at `grant_version` holding
/// exactly `scopes`. The consumer version header lets a consumer DELETE
/// reach its own scope check.
async fn scoped_call(
    svc: &crate::auth::AuthService,
    addr: std::net::SocketAddr,
    request: (&str, &str),
    scopes: &str,
    grant_version: u64,
) -> (u16, String) {
    let bearer = rig_scoped_bearer(svc, PROJECT, CREDENTIAL, scopes, grant_version);
    let version = crate::product::consumer_version_token(&[0; 16], 1);
    let headers = [
        ("authorization", bearer.as_str()),
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-consumer-version", version.as_str()),
    ];
    let (method, path) = request;
    let (status, _, body) = preq(addr, method, path, &headers, b"").await;
    (status, error_code(&body))
}

/// Item 73: each operation the entry dispatches is refused without its §6.1
/// scope and passes the gate with that scope alone. The watch wait carries
/// no scope: its handler verifies a capability instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_dispatched_operation_demands_exactly_its_scope() {
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &[CREDENTIAL], None).await;
    let mut grant_version = 1;
    for (method, path, scope) in OPERATIONS {
        let others = Scope::ALL.into_iter().filter(|s| Some(*s) != scope);
        let without = if scope.is_some() {
            claim(others)
        } else {
            String::new()
        };
        grant_version += 1;
        let answer = scoped_call(&svc, addr, (method, path), &without, grant_version).await;
        let Some(scope) = scope else {
            assert_ne!(answer.1, "missing_scope", "{method} {path} needs no scope");
            continue;
        };
        let missing = (403, "missing_scope".to_string());
        assert_eq!(
            answer,
            missing,
            "{method} {path} without {}",
            scope.as_str()
        );
        grant_version += 1;
        let only = scoped_call(&svc, addr, (method, path), scope.as_str(), grant_version).await;
        assert_ne!(
            only.1,
            "missing_scope",
            "{method} {path} with only {}",
            scope.as_str()
        );
    }
    engine_shutdown(&state).await;
}

/// Item 73: a request that names no operation is still authenticated before
/// the entry refuses its route, so an anonymous caller learns nothing about
/// the route table. Grammar refusals come first, from the route parser.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_naming_no_operation_authenticates_before_its_404_or_405() {
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &[CREDENTIAL], None).await;
    let consumer = "/v1/streams/opgrid/consumers/c:x";
    let (status, _, body) = preq(addr, "GET", consumer, &[], b"").await;
    let grammar = (status, error_code(&body));
    assert_eq!(grammar, (400, "invalid_consumer_name".to_string()));
    let everything = claim(Scope::ALL.into_iter());
    for (method, path, status, code) in UNDISPATCHABLE {
        let (anonymous, _, body) = preq(addr, method, path, &[], b"").await;
        let anonymous = (anonymous, error_code(&body));
        let unauthorized = (401, "unauthorized".to_string());
        assert_eq!(
            anonymous, unauthorized,
            "{method} {path} before authentication"
        );
        let full = scoped_call(&svc, addr, (method, path), &everything, 2).await;
        assert_eq!(
            full,
            (status, code.to_string()),
            "{method} {path} with every scope"
        );
    }
    engine_shutdown(&state).await;
}
