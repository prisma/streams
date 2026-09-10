#![cfg(test)]

use super::{AttemptKind, Generator, GeneratorConfig, Workload, pick, retry_delay};
use axum::body::{Bytes, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Router, extract::State};
use futures_util::FutureExt;
use std::future::{IntoFuture, pending};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;

fn config(changes: &[(&str, &str)]) -> anyhow::Result<GeneratorConfig> {
    let mut values: std::collections::HashMap<_, _> = [
        ("AUTH_TOKEN", "token"),
        ("STREAM_KEY", "secret"),
        ("LB_URL", "http://127.0.0.1:1"),
    ]
    .into();
    values.extend(changes.iter().copied());
    GeneratorConfig::load(|name| values.get(name).map(|value| (*value).to_string()))
}

fn owner(changes: &[(&str, &str)]) -> Generator {
    Generator {
        workload: Arc::new(Workload::new(config(changes).unwrap()).unwrap()),
        workers: JoinSet::new(),
    }
}

#[test]
fn configuration_rejects_invalid_dimensions_without_inheriting_lb_upstreams() {
    for name in ["STREAMS", "CONC_START", "CONC_MAX", "RAMP_SECS", "BATCH"] {
        assert!(config(&[(name, "0")]).is_err(), "{name}");
        assert!(config(&[(name, "wrong")]).is_err(), "{name}");
    }
    assert!(config(&[("GEN_UPSTREAMS", " , ; ")]).is_err());
    assert!(config(&[("PORT", "65536")]).is_err());
    assert!(GeneratorConfig::load(|_| None).is_err());
    let c = config(&[
        ("UPSTREAMS", "unrelated"),
        ("READ_EVERY", "0"),
        ("RECORD_PAD", "0"),
    ])
    .unwrap();
    assert_eq!(c.targets, ["http://127.0.0.1:1"]);
    assert_eq!(c.streams.get(), 32);
    assert_eq!(c.batch.get(), 1);
    assert_eq!(c.read_every, 0);
    assert_eq!(c.record_pad, 0);
    assert_eq!(c.desired(Duration::ZERO), 8);
    assert_eq!(c.desired(Duration::from_secs(300)), 16);
    assert_eq!(c.desired(Duration::MAX), 4096);
}

#[test]
fn nonce_modulo_and_attribution_keep_their_independent_routes() {
    let g = owner(&[
        ("STREAMS", "3"),
        ("GEN_UPSTREAMS", "first, second"),
        ("ATTR_UPSTREAMS", "a;b;c"),
        ("READ_EVERY", "2"),
    ]);
    for nonce in [0, 1, u64::from(u32::MAX) + 1, u64::MAX] {
        let a = g.workload.attempt(nonce).unwrap();
        assert_eq!(a.stream, usize::try_from(nonce % 3).unwrap());
        assert_eq!(a.target, pick(&a.name, &["first".into(), "second".into()]));
        assert_eq!(
            a.attribution,
            pick(&a.name, &["a".into(), "b".into(), "c".into()])
        );
        assert_eq!(
            a.kind,
            if nonce % 2 == 1 {
                AttemptKind::Read
            } else {
                AttemptKind::Append
            }
        );
    }
}

#[tokio::test]
async fn drain_counts_admitted_unpolled_workers_and_forbids_late_admission() {
    let mut g = owner(&[]);
    assert!(g.launch());
    g.workload.state.close();
    let stats = g.workload.state.snapshot();
    assert_eq!(stats["draining"], true);
    assert_eq!(stats["activeWorkers"], 1);
    assert!(!g.launch());
    g.workers.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(g.workload.state.snapshot()["activeWorkers"], 0);
    assert_eq!(g.workload.sequence.load(Ordering::Relaxed), 0);
    assert!(!g.launch());
}

#[tokio::test]
async fn cancelled_unpolled_worker_releases_its_reserved_membership() {
    let mut g = owner(&[]);
    assert!(g.launch());
    g.workload.state.close();
    g.workers.abort_all();
    assert!(
        g.workers
            .join_next()
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert_eq!(g.workload.state.snapshot()["activeWorkers"], 0);
}

#[test]
fn successful_accounting_separates_reads_and_preserves_wrapping_ack_ids() {
    let g = owner(&[("STREAMS", "1"), ("READ_EVERY", "2")]);
    let state = &g.workload.state;
    for nonce in [u64::MAX - 1, 2, 3] {
        state
            .record(
                &g.workload.attempt(nonce).unwrap(),
                Duration::from_micros(1000),
            )
            .unwrap();
    }
    state.rotate_rates();
    state.set_concurrency(42);
    state.close();
    let s = state.snapshot();
    assert_eq!(s["ok"], 3);
    assert_eq!(s["okAppends"], 2);
    assert_eq!(s["okReads"], 1);
    assert_eq!(
        s["ledger"][0],
        serde_json::json!({"count":2,"sum":0,"xor":(u64::MAX - 1) ^ 2})
    );
    assert_eq!(s["achievedPerSec"], 3);
    assert_eq!(s["perUpstreamPerSec"], serde_json::json!([3]));
    assert_eq!(s["concurrency"], 42);
    assert_eq!(s["winSamples"], 0);
    assert_eq!(s["p50Ms"], 1.0);
    let actual: std::collections::BTreeSet<_> =
        s.as_object().unwrap().keys().map(String::as_str).collect();
    let expected = [
        "mode",
        "winP50Ms",
        "winP99Ms",
        "winSamples",
        "concurrency",
        "achievedPerSec",
        "perUpstreamPerSec",
        "ok",
        "okAppends",
        "okReads",
        "ledger",
        "errs",
        "throttled",
        "draining",
        "activeWorkers",
        "meanMs",
        "p50Ms",
        "p99Ms",
        "maxMs",
        "elapsedMin",
        "lastErr",
    ]
    .into_iter()
    .collect();
    assert_eq!(actual, expected);
}

#[test]
fn rejected_latency_cannot_publish_a_successful_measurement() {
    let g = owner(&[]);
    let state = &g.workload.state;
    for duration in [Duration::from_secs(121), Duration::MAX] {
        assert!(
            state
                .record(&g.workload.attempt(0).unwrap(), duration)
                .is_err()
        );
    }
    let s = state.snapshot();
    for key in ["ok", "okAppends", "okReads", "winSamples"] {
        assert_eq!(s[key], 0, "{key}");
    }
    assert_eq!(s["ledger"][0]["count"], 0);
    assert_eq!(state.hist.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn closing_interrupts_retry_backoff_and_covers_already_closed_waits() {
    let g = owner(&[]);
    let wait = g.workload.state.backoff(Duration::from_secs(3600));
    tokio::pin!(wait);
    assert!(futures_util::poll!(&mut wait).is_pending());
    g.workload.state.close();
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        g.workload.state.backoff(Duration::from_secs(3600)),
    )
    .await
    .unwrap();
}

#[test]
fn retry_after_conversion_and_jitter_saturate() {
    let mut headers = HeaderMap::new();
    assert_eq!(retry_delay(&headers, 399), Duration::from_millis(899));
    headers.insert("retry-after", "2".parse().unwrap());
    assert_eq!(retry_delay(&headers, 401), Duration::from_millis(2001));
    headers.insert("retry-after", u64::MAX.to_string().parse().unwrap());
    assert_eq!(retry_delay(&headers, 399), Duration::from_millis(u64::MAX));
}

#[tokio::test]
async fn every_server_exit_joins_all_admitted_workers() {
    let mut g = owner(&[]);
    for _ in 0..3 {
        assert!(g.launch());
    }
    let result = g
        .serve(
            async { Err(std::io::Error::other("server stopped")) },
            pending(),
        )
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("generator stats server")
    );
    assert!(g.workers.is_empty());
    assert_eq!(g.workload.state.snapshot()["activeWorkers"], 0);
    assert!(!g.launch());
}

#[tokio::test]
async fn nonce_exhaustion_fails_the_run_and_joins_sibling_workers() {
    let mut g = owner(&[]);
    g.workload.sequence.store(u64::MAX, Ordering::Relaxed);
    for _ in 0..3 {
        assert!(g.launch());
    }
    let result = g.serve(pending(), pending()).await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("nonce space exhausted")
    );
    assert!(g.workers.is_empty());
    assert_eq!(g.workload.state.snapshot()["activeWorkers"], 0);
    assert!(!g.launch());
}

struct Captured {
    method: Method,
    target: String,
    headers: HeaderMap,
    body: Bytes,
}

async fn capture(State(requests): State<Arc<Mutex<Vec<Captured>>>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 8192).await.unwrap();
    let nonce = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v[0]["i"].as_u64());
    requests.lock().unwrap().push(Captured {
        method: parts.method,
        target: parts.uri.to_string(),
        headers: parts.headers,
        body,
    });
    match nonce {
        Some(2) => (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "0")]).into_response(),
        Some(4) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        _ => StatusCode::OK.into_response(),
    }
}

async fn with_upstream<F, Fut>(case: F) -> Arc<Mutex<Vec<Captured>>>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new().fallback(capture).with_state(requests.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (done, wait) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            drop(wait.await);
        })
        .into_future();
    let test = async {
        let result = AssertUnwindSafe(case(url)).catch_unwind().await;
        done.send(()).unwrap();
        result
    };
    let (served, result) = tokio::join!(server, test);
    served.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    requests
}

#[tokio::test]
async fn real_http_preserves_workload_bodies_credentials_read_mix_and_outcomes() {
    let requests = with_upstream(|url| async move {
        let g = owner(&[
            ("LB_URL", &url),
            ("STREAMS", "1"),
            ("READ_EVERY", "2"),
            ("BATCH", "2"),
            ("RECORD_PAD", "3"),
        ]);
        g.workload.create_streams().await;
        for n in [0, 1, 2, 4] {
            g.workload
                .execute(&g.workload.attempt(n).unwrap())
                .await
                .unwrap();
        }
        let s = g.workload.state.snapshot();
        assert_eq!(s["ok"], 2);
        assert_eq!(s["okAppends"], 1);
        assert_eq!(s["okReads"], 1);
        assert_eq!(s["throttled"], 1);
        assert_eq!(s["errs"], 1);
        assert_eq!(s["ledger"][0]["count"], 1);
    })
    .await;
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].method, Method::PUT);
    assert_eq!(requests[1].method, Method::POST);
    let records: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(records.as_array().unwrap().len(), 2);
    assert_eq!(records[0]["i"], 0);
    assert_eq!(records[0]["b"], 0);
    assert_eq!(records[1]["b"], 1);
    assert_eq!(records[0]["pad"], "xxx");
    assert!(records[0]["t"].as_u64().unwrap() > 0);
    assert_eq!(requests[2].method, Method::GET);
    assert_eq!(requests[2].target, "/v1/stream/pilot-0?offset=now");
    for r in requests.iter() {
        assert_eq!(r.headers["authorization"], "Bearer token");
        assert_eq!(r.headers["stream-encryption-key"], "secret");
    }
}

#[tokio::test]
async fn worker_panic_is_observed_and_all_other_workers_are_joined() {
    with_upstream(|url| async move {
        let mut g = owner(&[("LB_URL", &url), ("STREAMS", "1"), ("READ_EVERY", "0")]);
        let state = &g.workload.state;
        assert!(
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _hist = state.hist.lock().unwrap();
                panic!("interrupted measurement");
            }))
            .is_err()
        );
        assert!(g.launch());
        let result = tokio::time::timeout(Duration::from_secs(5), g.serve(pending(), pending()))
            .await
            .unwrap();
        assert!(result.is_err());
        assert!(g.workers.is_empty());
        assert!(g.workload.state.membership.is_closed());
        assert_eq!(g.workload.state.membership.snapshot().1, 0);
    })
    .await;
}

#[tokio::test]
async fn stats_and_drain_routes_keep_serving_the_same_final_ledger() {
    let g = owner(&[("STREAMS", "1"), ("READ_EVERY", "0")]);
    g.workload
        .state
        .record(&g.workload.attempt(7).unwrap(), Duration::from_millis(1))
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (done, wait) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, super::router(g.workload.state.clone()))
        .with_graceful_shutdown(async {
            drop(wait.await);
        })
        .into_future();
    let case = async {
        let http = super::super::client();
        let response = http.get(format!("{url}/stats")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["draining"],
            false
        );
        for method in [Method::POST, Method::GET] {
            let drained = http
                .request(method, format!("{url}/drain"))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
            assert_eq!(
                drained,
                serde_json::json!({"draining": true, "activeWorkers": 0})
            );
        }
        for path in ["/", "/stats"] {
            let stats = http
                .get(format!("{url}{path}"))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
            assert_eq!(stats["okAppends"], 1);
            assert_eq!(
                stats["ledger"][0],
                serde_json::json!({"count": 1, "sum": 7, "xor": 7})
            );
            assert_eq!(stats["draining"], true);
        }
    };
    let client = async {
        let result = AssertUnwindSafe(case).catch_unwind().await;
        done.send(()).unwrap();
        result
    };
    let (served, result) = tokio::join!(server, client);
    served.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
