#![cfg(test)]

use super::super::{FleetView, Lb, RotatingClient, epoch_millis};
use super::{UpStat, proxy};
use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::FutureExt;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, UNIX_EPOCH};

fn stat() -> UpStat {
    UpStat {
        reqs: AtomicU64::new(0),
        errs: AtomicU64::new(0),
        window: AtomicU64::new(0),
        ewma_us: AtomicU64::new(0),
        last_us: AtomicU64::new(0),
        cold_starts: AtomicU64::new(0),
        last_seen_ms: AtomicU64::new(0),
        replays: AtomicU64::new(0),
        unmarked: AtomicU64::new(0),
        eject_until_ms: AtomicU64::new(0),
    }
}

fn balancer(upstreams: Vec<String>) -> Arc<Lb> {
    let stats = upstreams.iter().map(|_| stat()).collect();
    let fleet = FleetView {
        active: (1..=upstreams.len())
            .map(|i| format!("streams-{i}"))
            .collect(),
        topology: vec![String::new()],
        overrides: [(String::new(), "streams-1".to_string())].into(),
        ..Default::default()
    };
    Arc::new(Lb {
        upstreams: RwLock::new(upstreams),
        stats,
        history: Mutex::new(VecDeque::new()),
        gen_stats: Mutex::new(serde_json::Value::Null),
        fleet: Mutex::new(fleet),
        http: RotatingClient::new(),
    })
}

#[test]
fn routing_keeps_active_overrides_ejection_and_last_candidate_fallback() {
    let lb = balancer(vec!["one".into(), "two".into(), "three".into()]);
    lb.fleet
        .lock()
        .unwrap()
        .overrides
        .insert(String::new(), "streams-2".to_string());
    assert_eq!(lb.select(Some("customers/acme"), 0), 1);
    lb.stats[1].eject_for(0, 100);
    assert_ne!(lb.select(Some("customers/acme"), 0), 1);
    lb.stats[0].eject_for(0, 100);
    lb.stats[2].eject_for(0, 100);
    assert_eq!(
        lb.select(Some("customers/acme"), 0),
        1,
        "an entirely ejected fleet keeps its original candidates"
    );
    assert_eq!(
        lb.select(None, 0),
        0,
        "registry paths use the first active ordinal"
    );
    lb.fleet.lock().unwrap().active.clear();
    assert_eq!(lb.select(Some("customers/acme"), 0), 0);
}

#[test]
fn clock_ejection_and_weighted_latency_cover_integer_boundaries() {
    assert_eq!(epoch_millis(UNIX_EPOCH - Duration::from_secs(1)), 0);
    assert_eq!(epoch_millis(UNIX_EPOCH + Duration::from_millis(42)), 42);
    assert_eq!(
        epoch_millis(UNIX_EPOCH + Duration::from_secs(u64::MAX / 1000 + 1)),
        u64::MAX
    );
    let stat = stat();
    stat.eject_for(42, u64::MAX);
    assert_eq!(stat.eject_until_ms.load(Ordering::Relaxed), u64::MAX);
    stat.record_served(u64::MAX, 0);
    stat.record_served(u64::MAX, 9000);
    assert_eq!(stat.ewma_us.load(Ordering::Relaxed), u64::MAX);
    stat.record_served(0, 0);
    assert_eq!(
        stat.ewma_us.load(Ordering::Relaxed),
        u64::try_from(u128::from(u64::MAX) * 9 / 10).unwrap()
    );
    assert_eq!(stat.reqs.load(Ordering::Relaxed), 3);
    assert_eq!(stat.cold_starts.load(Ordering::Relaxed), 1);
}

struct Captured {
    method: Method,
    target: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone, Copy)]
enum Reply {
    ReplayTwice,
    AlwaysBounce,
    Unmarked,
}

struct Upstream {
    reply: Reply,
    requests: Mutex<Vec<Captured>>,
}

async fn upstream(State(owner): State<Arc<Upstream>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 4096).await.unwrap();
    let mut requests = owner.requests.lock().unwrap();
    requests.push(Captured {
        method: parts.method,
        target: parts.uri.to_string(),
        headers: parts.headers,
        body,
    });
    let count = requests.len();
    let target = match owner.reply {
        Reply::ReplayTwice if count <= 2 => Some(format!("streams-{}", count + 1)),
        Reply::AlwaysBounce => Some("streams-1".to_string()),
        Reply::Unmarked => return (StatusCode::NOT_FOUND, "platform page").into_response(),
        Reply::ReplayTwice => None,
    };
    if let Some(target) = target {
        return (
            StatusCode::CONFLICT,
            [
                ("prisma-streams-origin", "yes"),
                ("streams-replay-to", &target),
            ],
            "moved",
        )
            .into_response();
    }
    Response::builder()
        .status(StatusCode::CREATED)
        .header("prisma-streams-origin", "yes")
        .header("stream-next-offset", "next")
        .header("set-cookie", "first=1")
        .header("set-cookie", "second=2")
        .body(Body::from("served"))
        .unwrap()
}

async fn with_upstream<F, Fut>(reply: Reply, case: F) -> Arc<Upstream>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let owner = Arc::new(Upstream {
        reply,
        requests: Mutex::new(Vec::new()),
    });
    let router = Router::new().fallback(upstream).with_state(owner.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (done, wait) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            drop(wait.await);
        })
        .into_future();
    let test = async {
        let result = AssertUnwindSafe(case(format!("http://{address}")))
            .catch_unwind()
            .await;
        done.send(()).unwrap();
        result
    };
    let (served, result) = tokio::join!(server, test);
    served.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    owner
}

#[tokio::test]
async fn two_replays_preserve_the_wire_request_and_charge_the_serving_upstream() {
    let owner = with_upstream(Reply::ReplayTwice, |url| async move {
        let lb = balancer((1..=3).map(|i| format!("{url}/u{i}")).collect());
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/streams/customers/acme:append?project=1")
            .header("authorization", "Bearer test")
            .header("stream-encryption-key", "test-key")
            .header("host", "wrong.invalid")
            .header("connection", "close")
            .header("content-length", "999")
            .body(Body::from("samebody"))
            .unwrap();
        let response = proxy(State(lb.clone()), request).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["stream-next-offset"], "next");
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
        assert_eq!(
            to_bytes(response.into_body(), 4096).await.unwrap(),
            "served"
        );
        assert_eq!(
            lb.stats
                .iter()
                .map(|s| s.reqs.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            [0, 0, 1]
        );
        assert_eq!(
            lb.stats
                .iter()
                .map(|s| s.replays.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            [1, 1, 0]
        );
    })
    .await;
    let requests = owner.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (i, request) in requests.iter().enumerate() {
        assert_eq!(request.method, Method::POST);
        assert_eq!(
            request.target,
            format!("/u{}/v1/streams/customers/acme:append?project=1", i + 1)
        );
        assert_eq!(request.headers["authorization"], "Bearer test");
        assert_eq!(request.headers["stream-encryption-key"], "test-key");
        assert_ne!(request.headers["host"], "wrong.invalid");
        assert_eq!(request.headers["content-length"], "8");
        assert_eq!(request.body, "samebody");
    }
}

#[tokio::test]
async fn an_ownership_loop_stops_after_two_follows() {
    let owner = with_upstream(Reply::AlwaysBounce, |url| async move {
        let lb = balancer(vec![url]);
        let request = Request::builder()
            .uri("/v1/stream/a")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            proxy(State(lb.clone()), request).await.status(),
            StatusCode::CONFLICT
        );
        assert_eq!(lb.stats[0].replays.load(Ordering::Relaxed), 3);
    })
    .await;
    assert_eq!(owner.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn an_unmarked_platform_response_is_retryable_and_ejected() {
    with_upstream(Reply::Unmarked, |url| async move {
        let lb = balancer(vec![url]);
        let request = Request::builder()
            .uri("/v1/stream/a")
            .body(Body::empty())
            .unwrap();
        let response = proxy(State(lb.clone()), request).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], "upstream_unavailable");
        assert_eq!(body["error"]["retryable"], true);
        assert_eq!(lb.stats[0].unmarked.load(Ordering::Relaxed), 1);
        assert_eq!(lb.stats[0].reqs.load(Ordering::Relaxed), 0);
        assert_ne!(lb.stats[0].eject_until_ms.load(Ordering::Relaxed), 0);
    })
    .await;
}

#[tokio::test]
async fn body_ceiling_and_non_stream_routes_fail_before_forwarding() {
    let lb = balancer(vec!["not-a-valid-upstream".into()]);
    let request = Request::builder()
        .uri("/v1/stream/a")
        .body(Body::from(vec![0; 32 * 1024 * 1024 + 1]))
        .unwrap();
    assert_eq!(
        proxy(State(lb.clone()), request).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let request = Request::builder()
        .uri("/unrelated")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        proxy(State(lb), request).await.status(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn poisoned_route_or_url_publication_cannot_be_used() {
    let lb = balancer(vec!["one".into()]);
    let failed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut fleet = lb.fleet.lock().unwrap();
        fleet.active.clear();
        panic!("partial route publication");
    }));
    assert!(failed.is_err());
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| lb.select(Some("a"), 0))).is_err());
    let lb = balancer(vec!["one".into()]);
    let failed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut urls = lb.upstreams.write().unwrap();
        urls.clear();
        panic!("partial URL publication");
    }));
    assert!(failed.is_err());
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| lb.upstream_url(0))).is_err());
}
