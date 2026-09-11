#![cfg(test)]

use super::{Args, bench_read};
use axum::Router;
use axum::body::{Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::http::{HeaderMap, Method};
use axum::response::{IntoResponse, Response};
use clap::Parser;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

#[test]
fn zero_work_or_measurement_counts_are_rejected_before_startup() {
    for option in ["--concurrency", "--streams", "--entries", "--duration-secs"] {
        assert!(
            Args::try_parse_from(["bench", option, "0"]).is_err(),
            "{option}"
        );
    }
    assert!(Args::try_parse_from(["bench", "--warmup-secs", "0"]).is_ok());
}

async fn with_server<F, Fut>(router: Router, test: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (done, wait) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            drop(wait.await);
        })
        .into_future();
    let case = async {
        let result = AssertUnwindSafe(test(format!("http://{address}")))
            .catch_unwind()
            .await;
        done.send(()).unwrap();
        result
    };
    let (served, result) = tokio::join!(server, case);
    served.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn failed_read_cannot_be_reported_as_successful_partial_throughput() {
    let router = Router::new().fallback(|| async { StatusCode::SERVICE_UNAVAILABLE });
    with_server(router, |url| async move {
        let args = Args::try_parse_from(["bench", "--url", &url, "--streams", "1"]).unwrap();
        assert!(
            bench_read(args).await.is_err(),
            "failed replay must fail the benchmark"
        );
    })
    .await;
}

#[expect(
    clippy::disallowed_methods,
    reason = "Benchmark terminal-ownership regression owns the JoinSet under test; a held sibling must drop before finish_workers returns either failure; detached tasks would let this assertion race cleanup"
)]
#[tokio::test]
async fn failed_worker_aborts_and_joins_entered_siblings() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    for panic in [false, true] {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let (entered, wait) = tokio::sync::oneshot::channel();
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(async move {
            let _guard = guard;
            entered.send(()).unwrap();
            std::future::pending::<anyhow::Result<()>>().await
        });
        wait.await.unwrap();
        workers.spawn(async move {
            assert!(!panic, "injected benchmark worker panic");
            anyhow::bail!("injected benchmark worker failure")
        });
        assert!(super::finish_workers(workers).await.is_err());
        assert!(
            dropped.load(Ordering::SeqCst),
            "failed run returned with a live sibling"
        );
    }
}

#[tokio::test]
async fn rejected_latency_sample_cannot_publish_success_counters() {
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    let shared = super::Shared::new().unwrap();
    shared
        .record_success(Duration::from_micros(42), 2, 64)
        .await
        .unwrap();
    for invalid in [Duration::from_secs(1000), Duration::from_secs(u64::MAX)] {
        assert!(shared.record_success(invalid, 9, 999).await.is_err());
        assert_eq!(shared.ok.load(Ordering::Relaxed), 1);
        assert_eq!(shared.entries_ok.load(Ordering::Relaxed), 2);
        assert_eq!(shared.bytes_ok.load(Ordering::Relaxed), 64);
        assert_eq!(shared.hist.lock().await.len(), 1);
    }
}

struct RecordedRequest {
    method: Method,
    target: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Default)]
struct Recorder {
    requests: Mutex<Vec<RecordedRequest>>,
}

impl Recorder {
    async fn record(&self, request: Request) -> usize {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, 4096).await.unwrap();
        let mut requests = self.requests.lock().unwrap();
        requests.push(RecordedRequest {
            method: parts.method,
            target: parts.uri.to_string(),
            headers: parts.headers,
            body,
        });
        requests.len()
    }
}

async fn replay_page(State(recorder): State<Arc<Recorder>>, request: Request) -> Response {
    let count = recorder.record(request).await;
    let body = if count == 1 { "a" } else { "bc" };
    ([("stream-next-offset", "next")], body).into_response()
}

async fn acknowledge_append(State(recorder): State<Arc<Recorder>>, request: Request) -> StatusCode {
    recorder.record(request).await;
    StatusCode::OK
}

#[tokio::test]
async fn replay_keeps_offsets_credentials_and_exact_completed_byte_totals() {
    let recorder = Arc::new(Recorder::default());
    let router = Router::new()
        .fallback(replay_page)
        .with_state(recorder.clone());
    with_server(router, |url| async move {
        let client = super::make_client(1).unwrap();
        let totals = super::read_stream(
            &client,
            &format!("{url}/v1/stream/s"),
            &Some("credential".to_string()),
        )
        .await
        .unwrap();
        assert_eq!(
            totals,
            super::ReadTotals {
                bytes: 3,
                requests: 2
            }
        );
    })
    .await;
    let seen = recorder.requests.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].target, "/v1/stream/s?offset=-1");
    assert_eq!(seen[1].target, "/v1/stream/s?offset=next");
    assert!(
        seen.iter()
            .all(|request| request.headers["stream-encryption-key"] == "credential")
    );
}

#[tokio::test]
async fn append_run_preserves_the_http_workload() {
    let recorder = Arc::new(Recorder::default());
    let router = Router::new()
        .fallback(acknowledge_append)
        .with_state(recorder.clone());
    with_server(router, |url| async move {
        let args = Args::try_parse_from([
            "bench",
            "--url",
            &url,
            "--streams",
            "2",
            "--concurrency",
            "2",
            "--warmup-secs",
            "0",
            "--duration-secs",
            "1",
            "--entries",
            "2",
            "--payload-bytes",
            "32",
            "--key",
            "credential",
        ])
        .unwrap();
        super::bench_append(args).await.unwrap();
    })
    .await;
    let observed = recorder.requests.lock().unwrap();
    let created: Vec<_> = observed
        .iter()
        .filter(|request| request.method == Method::PUT)
        .map(|request| request.target.as_str())
        .collect();
    assert_eq!(created, ["/v1/stream/bench-0", "/v1/stream/bench-1"]);
    let appended: Vec<_> = observed
        .iter()
        .filter(|request| request.method == Method::POST)
        .collect();
    assert!(!appended.is_empty());
    for request in appended {
        assert!(request.target == "/v1/stream/bench-0" || request.target == "/v1/stream/bench-1");
        assert_eq!(request.headers["stream-encryption-key"], "credential");
        assert_eq!(request.headers["content-type"], "application/json");
        assert_eq!(
            request.body.as_ref(),
            br#"[{"v":"xxxxxxxxxxxx"},{"v":"xxxxxxxxxxxx"}]"#
        );
    }
}

#[tokio::test]
async fn truncated_replay_body_is_a_failed_measurement() {
    use axum::body::{Body, Bytes};
    use axum::response::Response;
    let router = Router::new().fallback(|| async {
        let stream = futures_util::stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("injected body failure")),
        ]);
        Response::new(Body::from_stream(stream))
    });
    with_server(router, |url| async move {
        let client = super::make_client(1).unwrap();
        assert!(super::read_stream(&client, &url, &None).await.is_err());
    })
    .await;
}
