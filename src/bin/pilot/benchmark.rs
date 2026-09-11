//! Single-stream sweep with one joined worker set and immutable point ownership.
#![warn(clippy::wildcard_enum_match_arm)]

use super::RotatingClient;
use anyhow::Context;
use axum::{Router, extract::State, routing::get};
use bytes::Bytes;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

#[path = "benchmark/config.rs"]
mod config;
#[path = "benchmark/window.rs"]
mod window;
use config::{Config, Point, Verb};
use window::{Outcome, Snapshot, Window};

#[derive(Default)]
struct Results {
    rows: Vec<serde_json::Value>,
    done: bool,
}

struct Workload {
    config: Arc<Config>,
    client: RotatingClient,
    body: Bytes,
    bytes: u64,
    window: Mutex<Window>,
}

impl Workload {
    fn new(config: Arc<Config>, client: RotatingClient, point: Point) -> anyhow::Result<Self> {
        let body = point.body(config.verb)?;
        Ok(Self {
            bytes: u64::try_from(body.len())?,
            config,
            client,
            body,
            window: Mutex::new(Window::new()?),
        })
    }

    fn request(&self) -> reqwest::RequestBuilder {
        let client = self.client.get();
        let url = self.config.url();
        let request = match self.config.verb {
            Verb::Health => client.get(url),
            Verb::Sleep(_) => client
                .get(url)
                .header("authorization", format!("Bearer {}", self.config.auth)),
            Verb::Append => client
                .post(url)
                .header("authorization", format!("Bearer {}", self.config.auth))
                .header("stream-encryption-key", &self.config.key)
                .header("content-type", "application/json")
                .body(self.body.clone()),
        };
        request.timeout(Duration::from_secs(120))
    }

    async fn attempt(&self) -> (Outcome, Duration) {
        let start = Instant::now();
        let outcome = match self.request().send().await {
            Ok(response) if response.status().is_success() => {
                // Header success is the ACK convention; latency includes draining.
                drop(response.bytes().await);
                Outcome::Success
            }
            Ok(response) if matches!(response.status().as_u16(), 429 | 503) => Outcome::Throttle,
            Ok(response) => {
                if self.report_error() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    println!("bench err: HTTP {status}: {}", preview(&text));
                }
                Outcome::Error
            }
            Err(error) => {
                if self.report_error() {
                    println!("bench err: transport: {error}");
                }
                Outcome::Error
            }
        };
        (outcome, start.elapsed())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "benchmark error observation; poison marks an interrupted measurement; recovering its counters would publish a misleading result"
    )]
    fn report_error(&self) -> bool {
        self.window.lock().unwrap().report_error()
    }

    #[expect(
        clippy::unwrap_used,
        reason = "benchmark measurement owner; completion and freeze share one ledger lock; silent poison recovery would certify partial statistics"
    )]
    async fn worker(self: Arc<Self>, mut stop: watch::Receiver<bool>) -> anyhow::Result<()> {
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let (outcome, elapsed) = self.attempt().await;
            let pause = match outcome {
                Outcome::Success => Duration::ZERO,
                Outcome::Throttle => Duration::from_millis(10),
                Outcome::Error => Duration::from_millis(50),
            };
            self.window
                .lock()
                .unwrap()
                .record(outcome, self.bytes, elapsed)?;
            tokio::select! {
                _ = stop.wait_for(|closed| *closed) => return Ok(()),
                _ = tokio::time::sleep(pause) => {},
            }
        }
    }
}

fn preview(text: &str) -> String {
    text.chars().take(200).collect()
}

struct Sweep {
    config: Arc<Config>,
    client: RotatingClient,
    results: Arc<Mutex<Results>>,
    workers: JoinSet<anyhow::Result<()>>,
}

impl Sweep {
    fn new(config: Config) -> Self {
        Self {
            config: Arc::new(config),
            client: RotatingClient::new(),
            results: Arc::new(Mutex::new(Results::default())),
            workers: JoinSet::new(),
        }
    }

    async fn serve(
        &mut self,
        server: impl Future<Output = std::io::Result<()>>,
    ) -> anyhow::Result<()> {
        tokio::pin!(server);
        let result = tokio::select! {
            result = &mut server => result.context("benchmark results server failed"),
            result = self.run() => match result {
                Ok(()) => server.await.context("benchmark results server failed"),
                Err(error) => Err(error),
            },
        };
        self.workers.abort_all();
        while self.workers.join_next().await.is_some() {}
        result
    }

    async fn prepare(&self) -> anyhow::Result<()> {
        if self.config.verb == Verb::Append {
            let create = self
                .client
                .get()
                .put(self.config.url())
                .header("authorization", format!("Bearer {}", self.config.auth))
                .header("stream-encryption-key", &self.config.key)
                .header("content-type", "application/json")
                .send()
                .await;
            println!("bench stream create: {:?}", create.map(|r| r.status()));
        }
        let warm = Workload::new(
            self.config.clone(),
            self.client.clone(),
            Point {
                sweep: "warm",
                event_bytes: 12,
                batch: 1,
            },
        )?;
        for attempt in 0..42 {
            if warm
                .request()
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                println!("bench target warm after {attempt} retries");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        println!("bench target NOT warm after 42 attempts; proceeding anyway");
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "benchmark result publication; each frozen point is published once after all its workers join; poison cannot safely be recovered as a valid sweep"
    )]
    async fn run(&mut self) -> anyhow::Result<()> {
        self.prepare().await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        for point in self.config.plan.clone() {
            let (row, collapsed) = self.point(point).await?;
            println!("bench point done: {row}");
            self.results.lock().unwrap().rows.push(row);
            let drain = if collapsed {
                self.config
                    .drain
                    .checked_mul(3)
                    .context("collapsed drain overflows")?
            } else {
                self.config.drain
            };
            self.wait(drain).await?;
        }
        self.results.lock().unwrap().done = true;
        println!("bench sweep COMPLETE");
        Ok(())
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "benchmark sweep task owner; every worker is retained in this set and joined before advancing or returning from serve; a detached pool loses point and failure ownership"
    )]
    fn launch(&mut self, workload: &Arc<Workload>, stop: &watch::Receiver<bool>, desired: usize) {
        while self.workers.len() < desired {
            self.workers.spawn(workload.clone().worker(stop.clone()));
        }
    }

    async fn wait(&mut self, duration: Duration) -> anyhow::Result<()> {
        if duration.is_zero() {
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(duration)
            .context("benchmark deadline overflows")?;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => Ok(()),
            result = self.workers.join_next(), if !self.workers.is_empty() => {
                result.context("benchmark worker set unexpectedly empty")?.context("benchmark worker panicked")??;
                anyhow::bail!("benchmark worker exited before point close")
            },
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "benchmark point owner; start and freeze serialize with every completion on the point's unique ledger; poisoned measurement cannot be used for a result"
    )]
    async fn point(&mut self, point: Point) -> anyhow::Result<(serde_json::Value, bool)> {
        anyhow::ensure!(
            self.workers.is_empty(),
            "previous point workers still active"
        );
        let workload = Arc::new(Workload::new(
            self.config.clone(),
            self.client.clone(),
            point,
        )?);
        let concurrency = self.config.concurrency(point, workload.body.len());
        let (stop, receiver) = watch::channel(false);
        for (target, pause) in ramp(concurrency, self.config.warmup) {
            self.launch(&workload, &receiver, target);
            self.wait(pause).await?;
        }
        workload.window.lock().unwrap().start()?;
        let started = Instant::now();
        let mut collapsed = false;
        let mut progress = Progress::new(
            point,
            self.config.verb,
            started,
            workload.window.lock().unwrap().snapshot(),
        );
        loop {
            let remaining = self.config.measure.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            self.wait(Duration::from_secs(5).min(remaining)).await?;
            let snapshot = workload.window.lock().unwrap().snapshot();
            if collapse(started.elapsed(), &snapshot) {
                collapsed = true;
                break;
            }
            let now = Instant::now();
            if progress.due(now) {
                println!("{}", progress.report(now, snapshot));
            }
        }
        let snapshot = workload.window.lock().unwrap().freeze();
        let elapsed = started.elapsed();
        stop.send_replace(true);
        while let Some(result) = self.workers.join_next().await {
            result.context("benchmark worker panicked while draining")??;
        }
        Ok((
            row(
                point,
                self.config.verb,
                concurrency,
                elapsed,
                snapshot,
                collapsed,
            ),
            collapsed,
        ))
    }
}

/// Warmup schedule: six equal pauses whose worker targets grow linearly,
/// spreading the division remainder across the steps and never launching
/// fewer than min(concurrency, 4) workers.
fn ramp(concurrency: usize, warmup: Duration) -> [(usize, Duration); 6] {
    std::array::from_fn(|index| {
        let step = index + 1;
        let target = concurrency / 6 * step + concurrency % 6 * step / 6;
        (target.max(concurrency.min(4)), warmup / 6)
    })
}

/// A point collapses once 45 measured seconds produced errors and no success.
fn collapse(elapsed: Duration, snapshot: &Snapshot) -> bool {
    elapsed >= Duration::from_secs(45) && snapshot.ok == 0 && snapshot.errors > 50
}

/// Progress lines between measurement snapshots, at most one per 15 seconds,
/// each reporting the rates since the previous line.
struct Progress {
    point: Point,
    verb: Verb,
    started: Instant,
    since: Instant,
    last: Snapshot,
}

impl Progress {
    fn new(point: Point, verb: Verb, started: Instant, snapshot: Snapshot) -> Self {
        Self {
            point,
            verb,
            started,
            since: started,
            last: snapshot,
        }
    }

    fn due(&self, now: Instant) -> bool {
        now.duration_since(self.since) >= Duration::from_secs(15)
    }

    fn report(&mut self, now: Instant, snapshot: Snapshot) -> String {
        let dt = now
            .duration_since(self.since)
            .as_secs_f64()
            .max(f64::MIN_POSITIVE);
        let requests = snapshot.ok.saturating_sub(self.last.ok) as f64 / dt;
        let events = if self.verb == Verb::Append {
            requests * self.point.batch as f64
        } else {
            0.0
        };
        let line = format!(
            "bench window t={:>5.0}s: {requests:.0} req/s {events:.0} ev/s {:.2} MB/s errs+{}",
            now.duration_since(self.started).as_secs_f64(),
            snapshot.bytes.saturating_sub(self.last.bytes) as f64 / dt / 1e6,
            snapshot.errors.saturating_sub(self.last.errors)
        );
        self.since = now;
        self.last = snapshot;
        line
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "benchmark output schema; these six immutable values form the final JSON row; an argument bag would duplicate the serialized result without owning behavior"
)]
fn row(
    point: Point,
    verb: Verb,
    concurrency: usize,
    elapsed: Duration,
    snapshot: Snapshot,
    collapsed: bool,
) -> serde_json::Value {
    let dt = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let requests = snapshot.ok as f64 / dt;
    serde_json::json!({
        "collapsed": collapsed, "sweep": point.sweep, "event_bytes": point.event_bytes,
        "batch": point.batch, "conc": concurrency, "secs": dt,
        "requests_per_s": requests,
        "events_per_s": if verb == Verb::Append { requests * point.batch as f64 } else { 0.0 },
        "mb_per_s": snapshot.bytes as f64 / dt / 1e6,
        "p50_ms": snapshot.p50_ms, "p99_ms": snapshot.p99_ms,
        "errs": snapshot.errors, "throttles": snapshot.throttles,
    })
}

#[expect(
    clippy::unwrap_used,
    reason = "benchmark results endpoint; poison means publication was interrupted; serving a recovered partial sweep would report untrustworthy measurements"
)]
fn app(results: Arc<Mutex<Results>>) -> Router {
    Router::new().route("/", get(|State(results): State<Arc<Mutex<Results>>>| async move {
        let results = results.lock().unwrap();
        ([("access-control-allow-origin", "*")], axum::Json(serde_json::json!({ "done": results.done, "points": results.rows.len(), "results": results.rows })))
    })).with_state(results)
}

pub(super) async fn run(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<()> {
    let config = Config::load(lookup)?;
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, config.port)).await?;
    let mut sweep = Sweep::new(config);
    let server = axum::serve(listener, app(sweep.results.clone())).into_future();
    sweep.serve(server).await
}

#[cfg(test)]
#[path = "benchmark/tests.rs"]
mod tests;
