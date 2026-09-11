#![cfg(test)]
use super::{
    Config, Point, Progress, Results, Snapshot, Sweep, Verb, Window, Workload, app, collapse,
    preview, ramp, row,
};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    routing::any,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;

fn config(extra: &[(&str, &str)]) -> Config {
    Config::load(|key| {
        extra
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.to_string())
            .or_else(|| match key {
                "TARGET" => Some("http://127.0.0.1:1".into()),
                "AUTH_TOKEN" => Some("test-auth".into()),
                "STREAM_KEY" => Some("test-key".into()),
                "SIZES" | "BATCHES" | "MAX_WORKERS" => Some("1".into()),
                "WARMUP_SECS" | "INTER_POINT_SECS" => Some("0".into()),
                _ => None,
            })
    })
    .unwrap()
}

#[test]
fn configuration_rejects_invalid_inputs_and_preserves_explicit_workload() {
    assert!(Config::load(|_| None).is_err());
    for (key, value) in [
        ("TARGET", "not a url"),
        ("TARGET", "file:///tmp/test"),
        ("MAX_WORKERS", "0"),
        ("FIXED_SIZE", "0"),
        ("SIZES", "1;bad"),
        ("BATCHES", "1;0"),
        ("SIZES", ""),
        ("MEASURE_SECS", "0"),
        ("MAX_INFLIGHT_MB", "18446744073709551615"),
        ("INTER_POINT_SECS", "18446744073709551615"),
        ("BENCH_VERB", "typo"),
        ("PORT", "65536"),
    ] {
        assert!(
            Config::load(|name| match name {
                n if n == key => Some(value.into()),
                "TARGET" => Some("http://localhost:8080".into()),
                "AUTH_TOKEN" | "STREAM_KEY" => Some("test".into()),
                _ => None,
            })
            .is_err(),
            "{key}={value} must fail before work starts"
        );
    }
    let cfg = config(&[
        ("SIZES", "64;256"),
        ("BATCHES", "4;16"),
        ("FIXED_SIZE", "128"),
        ("MAX_WORKERS", "2"),
        ("BENCH_STREAM", "orders"),
        ("PORT", "1234"),
    ]);
    assert_eq!(
        cfg.plan
            .iter()
            .map(|p| (p.sweep, p.event_bytes, p.batch))
            .collect::<Vec<_>>(),
        [
            ("size", 64, 1),
            ("size", 256, 1),
            ("batch", 128, 4),
            ("batch", 128, 16)
        ]
    );
    assert_eq!(cfg.url(), "http://127.0.0.1:1/v1/stream/orders");
    assert_eq!(cfg.port, 1234);
    assert_eq!(cfg.concurrency(cfg.plan[0], usize::MAX), 2);
    assert_eq!(cfg.concurrency(cfg.plan[0], 1), 2);
}

#[test]
fn bodies_and_concurrency_use_exact_payloads_and_probe_modes_send_no_events() {
    let point = Point {
        sweep: "batch",
        event_bytes: 64,
        batch: 7,
    };
    let body = point.body(Verb::Append).unwrap();
    let decoded: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.len(), 7 * 65 + 1);
    assert_eq!(decoded.len(), 7);
    assert!(
        decoded
            .iter()
            .all(|row| row["p"].as_str().unwrap() == "x".repeat(56))
    );
    assert_eq!(
        Point {
            event_bytes: 1,
            ..point
        }
        .body(Verb::Append)
        .unwrap()
        .len(),
        71
    );
    assert!(
        Point {
            event_bytes: usize::MAX,
            ..point
        }
        .body(Verb::Append)
        .is_err()
    );
    assert!(
        Point {
            batch: usize::MAX,
            ..point
        }
        .body(Verb::Append)
        .is_err()
    );
    let cfg = config(&[("MAX_WORKERS", "10000")]);
    assert_eq!(
        cfg.concurrency(point, body.len()),
        2 * 1024 * 1024 / body.len()
    );
    for (verb, route) in [("health", "/health"), ("sleep", "/v1/debug/sleep?ms=17")] {
        let cfg = config(&[
            ("BENCH_VERB", verb),
            ("SLEEP_MS", "17"),
            ("MAX_WORKERS", "5"),
        ]);
        assert!(point.body(cfg.verb).unwrap().is_empty());
        assert_eq!(cfg.concurrency(point, 0), 5);
        assert!(cfg.url().ends_with(route));
    }
    assert_eq!(preview(&"🦀".repeat(201)), "🦀".repeat(200));
}

#[test]
fn output_schema_accounts_for_actual_append_or_probe_work() {
    let point = Point {
        sweep: "batch",
        event_bytes: 16,
        batch: 4,
    };
    let snapshot = Snapshot {
        ok: 10,
        errors: 3,
        throttles: 7,
        bytes: 200,
        p50_ms: 1.5,
        p99_ms: 7.5,
    };
    let append = row(
        point,
        Verb::Append,
        3,
        Duration::from_secs(2),
        snapshot,
        true,
    );
    assert_eq!(
        append,
        serde_json::json!({ "collapsed": true, "sweep": "batch", "event_bytes": 16, "batch": 4, "conc": 3, "secs": 2.0, "requests_per_s": 5.0, "events_per_s": 20.0, "mb_per_s": 0.0001, "p50_ms": 1.5, "p99_ms": 7.5, "errs": 3, "throttles": 7 })
    );
    let probe = row(
        point,
        Verb::Health,
        3,
        Duration::from_secs(2),
        Snapshot {
            bytes: 0,
            ..snapshot
        },
        false,
    );
    assert_eq!(probe["requests_per_s"], 5.0);
    assert_eq!(probe["events_per_s"], 0.0);
    assert_eq!(probe["mb_per_s"], 0.0);
    assert_eq!(probe["collapsed"], false);
}

struct Request {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}
struct Http {
    target: String,
    requests: mpsc::UnboundedReceiver<Request>,
    release: Arc<Semaphore>,
    server: JoinHandle<()>,
}

#[derive(Clone)]
struct ServerState {
    requests: mpsc::UnboundedSender<Request>,
    release: Arc<Semaphore>,
    status: StatusCode,
}

impl Http {
    #[expect(
        clippy::disallowed_methods,
        reason = "benchmark loopback fixture; each server is explicitly aborted and joined by close; real concurrent HTTP is required to hold request completion"
    )]
    async fn new(status: StatusCode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let state = ServerState {
            requests: tx,
            release: release.clone(),
            status,
        };
        let router = Router::new()
            .fallback(any(
                |State(state): State<ServerState>,
                 method: Method,
                 uri: Uri,
                 headers: HeaderMap,
                 body: Bytes| async move {
                    state
                        .requests
                        .send(Request {
                            method,
                            uri,
                            headers,
                            body,
                        })
                        .unwrap();
                    state.release.acquire().await.unwrap().forget();
                    (state.status, "🦀".repeat(201))
                },
            ))
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            target,
            requests,
            release,
            server,
        }
    }

    /// Bounded so a mutant that stops issuing requests fails instead of hanging.
    async fn request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(10), self.requests.recv())
            .await
            .expect("fixture request within ten seconds")
            .expect("fixture server alive")
    }

    async fn close(self) {
        self.server.abort();
        assert!(self.server.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn requests_preserve_credentials_bodies_and_count_all_http_outcomes() {
    for (verb, status) in [
        ("append", StatusCode::OK),
        ("health", StatusCode::TOO_MANY_REQUESTS),
        ("sleep", StatusCode::SERVICE_UNAVAILABLE),
        ("append", StatusCode::BAD_REQUEST),
    ] {
        let mut http = Http::new(status).await;
        let cfg = Arc::new(config(&[
            ("TARGET", &http.target),
            ("BENCH_VERB", verb),
            ("SLEEP_MS", "7"),
        ]));
        let workload = Workload::new(
            cfg,
            super::RotatingClient::new(),
            Point {
                sweep: "batch",
                event_bytes: 16,
                batch: 3,
            },
        )
        .unwrap();
        let mut attempt = Box::pin(workload.attempt());
        let request = tokio::select! {
            request = http.requests.recv() => request.unwrap(),
            _ = &mut attempt => panic!("request completed before fixture release"),
        };
        match verb {
            "append" => {
                assert_eq!(request.uri.path(), "/v1/stream/bench-ordered");
                assert_eq!(request.headers["authorization"], "Bearer test-auth");
                assert_eq!(request.headers["stream-encryption-key"], "test-key");
                assert_eq!(request.headers["content-type"], "application/json");
                assert_eq!(
                    serde_json::from_slice::<Vec<serde_json::Value>>(&request.body)
                        .unwrap()
                        .len(),
                    3
                );
            }
            "sleep" => {
                assert_eq!(request.uri.to_string(), "/v1/debug/sleep?ms=7");
                assert_eq!(request.headers["authorization"], "Bearer test-auth");
                assert!(request.body.is_empty());
            }
            "health" => {
                assert_eq!(request.uri.path(), "/health");
                assert!(request.body.is_empty());
            }
            _ => unreachable!(),
        }
        http.release.add_permits(1);
        let (outcome, _) = attempt.await;
        assert!(matches!(
            (status, outcome),
            (StatusCode::OK, super::Outcome::Success)
                | (
                    StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE,
                    super::Outcome::Throttle
                )
                | (StatusCode::BAD_REQUEST, super::Outcome::Error)
        ));
        http.close().await;
    }
}

#[tokio::test]
async fn point_freezes_before_drain_and_joins_held_requests_before_next_point() {
    let mut http = Http::new(StatusCode::OK).await;
    let mut cfg = config(&[("TARGET", &http.target)]);
    cfg.measure = Duration::from_secs(30);
    let point = cfg.plan[0];
    let mut sweep = Sweep::new(cfg);
    let mut run = Box::pin(sweep.point(point));
    tokio::select! { request = http.requests.recv() => { request.unwrap(); }, _ = &mut run => panic!("no request was issued") }
    // Only advance once real HTTP has reached its held response. The request's
    // 120-second timeout stays beyond the complete 30-second measurement.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::select! { _ = tokio::time::sleep(Duration::from_millis(1)) => {}, _ = &mut run => panic!("point returned without joining the held request") }
    assert!(http.requests.try_recv().is_err());
    http.release.add_permits(1);
    tokio::time::resume();
    let (first, collapsed) = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first["requests_per_s"], 0.0,
        "completion after freeze cannot enter the result"
    );
    assert!(!collapsed);
    assert!(sweep.workers.is_empty());
    // A new point owns a fresh ledger and no worker from its predecessor.
    let mut run = Box::pin(sweep.point(point));
    tokio::select! { request = http.requests.recv() => { request.unwrap(); }, _ = &mut run => panic!("next point did not start") }
    http.release.add_permits(1);
    tokio::select! { request = http.requests.recv() => { request.unwrap(); }, _ = &mut run => panic!("point exited before its measurement request") }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::select! { _ = tokio::time::sleep(Duration::from_millis(1)) => {}, _ = &mut run => panic!("point skipped draining") }
    http.release.add_permits(1);
    tokio::time::resume();
    let (second, _) = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    assert!(second["requests_per_s"].as_f64().unwrap() > 0.0);
    assert!(sweep.workers.is_empty());
    assert_eq!(first["requests_per_s"], 0.0);
    http.close().await;
}

#[tokio::test]
async fn server_exit_aborts_and_joins_workers_even_during_preparation() {
    let mut http = Http::new(StatusCode::OK).await;
    let mut sweep = Sweep::new(config(&[("TARGET", &http.target)]));
    let workload = Arc::new(
        Workload::new(
            sweep.config.clone(),
            sweep.client.clone(),
            sweep.config.plan[0],
        )
        .unwrap(),
    );
    let weak = Arc::downgrade(&workload);
    let (_, stop) = watch::channel(false);
    sweep.launch(&workload, &stop, 1);
    drop(workload);
    let server = async {
        http.request().await;
        Err(std::io::Error::other("fixture server exit"))
    };
    assert!(
        sweep
            .serve(server)
            .await
            .unwrap_err()
            .to_string()
            .contains("server failed")
    );
    assert!(sweep.workers.is_empty());
    assert!(weak.upgrade().is_none());
    http.close().await;
}

#[expect(
    clippy::disallowed_methods,
    reason = "benchmark worker failure regression; the sweep owns and joins this deliberately panicking worker and its sibling; observing panic cleanup requires a real task failure"
)]
#[tokio::test]
async fn worker_panic_fails_the_sweep_and_joins_all_siblings() {
    let mut sweep = Sweep::new(config(&[]));
    sweep
        .workers
        .spawn(async { panic!("fixture worker panic") });
    sweep.workers.spawn(std::future::pending());
    let failed = sweep.wait(Duration::from_secs(1)).await;
    assert!(failed.unwrap_err().to_string().contains("panicked"));
    sweep.serve(async { Ok(()) }).await.unwrap();
    assert!(sweep.workers.is_empty());
}

#[tokio::test]
async fn done_results_remain_available_with_the_original_json_contract() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let results = Arc::new(Mutex::new(Results {
        rows: vec![serde_json::json!({"sweep": "size"})],
        done: true,
    }));
    let server = axum::serve(listener, app(results)).into_future();
    tokio::pin!(server);
    let request = async {
        super::RotatingClient::new()
            .get()
            .get(url)
            .send()
            .await
            .unwrap()
    };
    let response = tokio::select! { _ = &mut server => panic!("server stopped"), response = request => response };
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    let body = tokio::select! { _ = server => panic!("server stopped"), body = response.json::<serde_json::Value>() => body.unwrap() };
    assert_eq!(
        body,
        serde_json::json!({"done":true,"points":1,"results":[{"sweep":"size"}]})
    );
}

#[test]
fn quality_loom_completion_and_freeze_use_the_actual_window_transitions() {
    use loom::sync::{Arc as LoomArc, Mutex as LoomMutex};
    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.check(|| {
        let mut window = Window::new().unwrap();
        window.start().unwrap();
        let window = LoomArc::new(LoomMutex::new(window));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let window = window.clone();
            handles.push(loom::thread::spawn(move || {
                window
                    .lock()
                    .unwrap()
                    .record(super::Outcome::Success, 7, Duration::from_micros(10))
                    .unwrap()
            }));
        }
        let frozen = window.lock().unwrap().freeze();
        for handle in handles {
            handle.join().unwrap();
        }
        let final_state = window.lock().unwrap().snapshot();
        assert_eq!(final_state, frozen);
        assert!(final_state.ok <= 2);
        assert_eq!(final_state.bytes, final_state.ok * 7);
    });
}

#[test]
fn warmup_ramp_spreads_targets_across_six_equal_pauses_with_a_floor_of_four() {
    let seconds =
        |steps: [(usize, u64); 6]| steps.map(|(target, s)| (target, Duration::from_secs(s)));
    assert_eq!(
        ramp(13, Duration::from_secs(6)),
        seconds([(4, 1), (4, 1), (6, 1), (8, 1), (10, 1), (13, 1)])
    );
    assert_eq!(
        ramp(1024, Duration::from_secs(12)),
        seconds([(170, 2), (341, 2), (512, 2), (682, 2), (853, 2), (1024, 2)])
    );
    assert_eq!(ramp(2, Duration::ZERO), [(2, Duration::ZERO); 6]);
    assert_eq!(
        ramp(5, Duration::from_secs(3)).map(|(target, _)| target),
        [4, 4, 4, 4, 4, 5]
    );
}

#[test]
fn collapse_requires_45_measured_seconds_of_errors_without_success() {
    let snapshot = |ok, errors| Snapshot {
        ok,
        errors,
        throttles: 0,
        bytes: 0,
        p50_ms: 0.0,
        p99_ms: 0.0,
    };
    let limit = Duration::from_secs(45);
    assert!(collapse(limit, &snapshot(0, 51)));
    assert!(collapse(Duration::from_secs(46), &snapshot(0, 1000)));
    assert!(!collapse(
        limit - Duration::from_millis(1),
        &snapshot(0, 51)
    ));
    assert!(!collapse(limit, &snapshot(1, 51)));
    assert!(!collapse(limit, &snapshot(0, 50)));
    assert!(!collapse(limit, &snapshot(0, 0)));
    assert!(!collapse(Duration::from_secs(100), &snapshot(1, 0)));
}

#[test]
fn progress_lines_are_due_every_15_seconds_and_report_deltas_since_the_last_line() {
    let point = Point {
        sweep: "batch",
        event_bytes: 16,
        batch: 4,
    };
    let snapshot = |ok, bytes, errors| Snapshot {
        ok,
        errors,
        throttles: 0,
        bytes,
        p50_ms: 0.0,
        p99_ms: 0.0,
    };
    let started = tokio::time::Instant::now();
    let mut progress = Progress::new(point, Verb::Append, started, snapshot(10, 1_000_000, 1));
    assert!(!progress.due(started + Duration::from_millis(14_999)));
    assert!(progress.due(started + Duration::from_secs(15)));
    let first = started + Duration::from_secs(20);
    assert_eq!(
        progress.report(first, snapshot(30, 21_000_000, 4)),
        "bench window t=   20s: 1 req/s 4 ev/s 1.00 MB/s errs+3"
    );
    assert!(!progress.due(first + Duration::from_millis(14_999)));
    assert!(progress.due(first + Duration::from_secs(15)));
    assert_eq!(
        progress.report(first + Duration::from_secs(10), snapshot(50, 31_000_000, 4)),
        "bench window t=   30s: 2 req/s 8 ev/s 1.00 MB/s errs+0"
    );
    let mut probe = Progress::new(point, Verb::Health, started, snapshot(0, 0, 0));
    assert_eq!(
        probe.report(started + Duration::from_secs(15), snapshot(30, 0, 2)),
        "bench window t=   15s: 2 req/s 0 ev/s 0.00 MB/s errs+2"
    );
}

#[tokio::test]
async fn error_reports_are_bounded_over_the_workload_lifetime() {
    let workload = Workload::new(
        Arc::new(config(&[])),
        super::RotatingClient::new(),
        Point {
            sweep: "size",
            event_bytes: 8,
            batch: 1,
        },
    )
    .unwrap();
    assert!(workload.report_error());
    assert!(workload.report_error());
    assert!(workload.report_error());
    assert!(!workload.report_error());
    workload.window.lock().unwrap().start().unwrap();
    assert!(!workload.report_error());
}

#[tokio::test]
async fn prepare_creates_the_stream_for_append_and_warms_only_the_configured_route() {
    let stream = "/v1/stream/bench-ordered";
    for (verb, expected) in [
        (
            "append",
            vec![(Method::PUT, stream, 0), (Method::POST, stream, 14)],
        ),
        ("health", vec![(Method::GET, "/health", 0)]),
    ] {
        let mut http = Http::new(StatusCode::OK).await;
        http.release.add_permits(expected.len());
        let sweep = Sweep::new(config(&[("TARGET", &http.target), ("BENCH_VERB", verb)]));
        tokio::time::timeout(Duration::from_secs(10), sweep.prepare())
            .await
            .unwrap()
            .unwrap();
        for (method, path, body) in expected {
            assert_prepared(&http.request().await, &method, path, body);
        }
        assert!(
            http.requests.try_recv().is_err(),
            "{verb}: nothing beyond the create and warm requests"
        );
        http.close().await;
    }
}

fn assert_prepared(request: &Request, method: &Method, path: &str, body: usize) {
    assert_eq!(
        (&request.method, request.uri.path(), request.body.len()),
        (method, path, body)
    );
    if *method != Method::GET {
        assert_eq!(request.headers["authorization"], "Bearer test-auth");
        assert_eq!(request.headers["stream-encryption-key"], "test-key");
        assert_eq!(request.headers["content-type"], "application/json");
    }
}

#[tokio::test]
async fn run_serves_completed_results_on_the_configured_port() {
    let mut http = Http::new(StatusCode::OK).await;
    http.release.add_permits(Semaphore::MAX_PERMITS);
    let port = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let target = http.target.clone();
    let mut run = Box::pin(super::run(move |key| match key {
        "TARGET" => Some(target.clone()),
        "AUTH_TOKEN" | "STREAM_KEY" => Some("test".into()),
        "SIZES" | "BATCHES" | "MAX_WORKERS" | "MEASURE_SECS" => Some("1".into()),
        "WARMUP_SECS" | "INTER_POINT_SECS" => Some("0".into()),
        "PORT" => Some(port.to_string()),
        _ => None,
    }));
    let create = tokio::select! {
        request = http.request() => request,
        result = &mut run => panic!("benchmark entry returned before creating the stream: {result:?}"),
    };
    assert_eq!(
        (create.method, create.uri.path()),
        (Method::PUT, "/v1/stream/bench-ordered")
    );
    let warm = tokio::select! {
        request = http.request() => request,
        result = &mut run => panic!("benchmark entry returned before warming: {result:?}"),
    };
    assert_eq!((warm.method, warm.body.len()), (Method::POST, 14));
    // Real HTTP has reached the fixture. The fixed pauses and the one-second
    // measurements may now elapse on the paused clock; outcomes are not asserted.
    tokio::time::pause();
    let client = super::RotatingClient::new();
    let url = format!("http://127.0.0.1:{port}/");
    let mut polls = 0;
    let results = loop {
        polls += 1;
        assert!(polls < 10_000, "sweep never published completed results");
        if let Some(results) = published_results(&mut run, &client, &url).await {
            break results;
        }
        tokio::select! {
            result = &mut run => panic!("benchmark entry returned before serving results: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    };
    assert_eq!(results["points"], 2);
    let rows = results["results"].as_array().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row["sweep"].as_str().unwrap(), row["collapsed"] == false))
            .collect::<Vec<_>>(),
        [("size", true), ("batch", true)]
    );
    http.close().await;
}

/// One results poll while the entry keeps running; `None` until the sweep
/// reports itself done or while the results server is not yet answering.
async fn published_results(
    run: &mut (impl std::future::Future<Output = anyhow::Result<()>> + Unpin),
    client: &super::RotatingClient,
    url: &str,
) -> Option<serde_json::Value> {
    let response = tokio::select! {
        result = &mut *run => panic!("benchmark entry returned before serving results: {result:?}"),
        response = client.get().get(url).send() => response.ok()?,
    };
    let body = tokio::select! {
        result = &mut *run => panic!("benchmark entry returned while serving results: {result:?}"),
        body = response.json::<serde_json::Value>() => body.ok()?,
    };
    (body["done"] == true).then_some(body)
}
