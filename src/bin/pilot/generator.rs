//! Closed-loop generator configuration, attempt accounting and worker custody.

use super::{RotatingClient, env, now_ms, pick};
use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use hdrhistogram::Histogram;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinSet;

// The terminal boundary is compiled from the same file under Loom's primitives.
mod synchronization {
    pub(super) use std::sync::Mutex;
    pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
}
#[path = "generator/membership.rs"]
mod membership;
use membership::Membership;

struct GeneratorConfig {
    auth: String,
    key: String,
    targets: Vec<String>,
    attribution: Vec<String>,
    streams: NonZeroUsize,
    concurrency_start: NonZeroU64,
    concurrency_max: NonZeroU64,
    ramp_secs: NonZeroU64,
    batch: NonZeroUsize,
    read_every: u64,
    record_pad: usize,
    prefix: String,
    port: u16,
}

impl GeneratorConfig {
    fn load(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let auth = lookup("AUTH_TOKEN").context("AUTH_TOKEN required")?;
        let key = lookup("STREAM_KEY").context("STREAM_KEY required")?;
        let split = |raw: String| {
            raw.split([',', ';'])
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        // UPSTREAMS is deliberately not an alias: it belongs to the LB process.
        let targets = match lookup("GEN_UPSTREAMS") {
            Some(raw) => split(raw),
            None => vec![lookup("LB_URL").context("LB_URL or GEN_UPSTREAMS required")?],
        };
        anyhow::ensure!(
            !targets.is_empty(),
            "generator requires at least one target"
        );
        let attribution = lookup("ATTR_UPSTREAMS").map(split).unwrap_or_default();
        let concurrency_max: NonZeroU64 = number(&lookup, "CONC_MAX", "4096")?;
        usize::try_from(concurrency_max.get()).context("CONC_MAX exceeds addressable workers")?;
        Ok(Self {
            auth,
            key,
            targets,
            attribution,
            concurrency_max,
            streams: number(&lookup, "STREAMS", "32")?,
            concurrency_start: number(&lookup, "CONC_START", "8")?,
            ramp_secs: number(&lookup, "RAMP_SECS", "300")?,
            batch: number(&lookup, "BATCH", "1")?,
            read_every: number(&lookup, "READ_EVERY", "10")?,
            record_pad: number(&lookup, "RECORD_PAD", "200")?,
            prefix: lookup("STREAM_PREFIX").unwrap_or_else(|| "pilot".to_string()),
            port: number(&lookup, "PORT", "8080")?,
        })
    }

    fn desired(&self, elapsed: Duration) -> u64 {
        let level = elapsed.as_secs() / self.ramp_secs.get();
        self.concurrency_max.get().min(
            self.concurrency_start
                .get()
                .saturating_mul(1 << level.min(30)),
        )
    }
}

fn number<T: FromStr>(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    fallback: &str,
) -> anyhow::Result<T> {
    lookup(key)
        .as_deref()
        .unwrap_or(fallback)
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid {key}"))
}

#[derive(Default)]
struct UpstreamRate {
    window: AtomicU64,
    rate: AtomicU64,
}

#[derive(Default)]
struct AckLedger {
    count: AtomicU64,
    sum: AtomicU64,
    xor: AtomicU64,
}

struct GeneratorState {
    ok: AtomicU64,
    ok_appends: AtomicU64,
    ok_reads: AtomicU64,
    errors: AtomicU64,
    window: AtomicU64,
    achieved: AtomicU64,
    throttled: AtomicU64,
    concurrency: AtomicU64,
    membership: Membership,
    wake: watch::Sender<()>,
    hist: Mutex<Histogram<u64>>,
    hist_window: Mutex<Histogram<u64>>,
    last_error: Mutex<String>,
    start: Instant,
    rates: Vec<UpstreamRate>,
    ledger: Vec<AckLedger>,
}

impl GeneratorState {
    fn new(config: &GeneratorConfig) -> anyhow::Result<Self> {
        let (wake, _) = watch::channel(());
        Ok(Self {
            ok: AtomicU64::new(0),
            ok_appends: AtomicU64::new(0),
            ok_reads: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            window: AtomicU64::new(0),
            achieved: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            concurrency: AtomicU64::new(0),
            membership: Membership::new(),
            wake,
            hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3)?),
            hist_window: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3)?),
            last_error: Mutex::new(String::new()),
            start: Instant::now(),
            rates: (0..config.attribution.len().max(1))
                .map(|_| UpstreamRate::default())
                .collect(),
            ledger: (0..config.streams.get())
                .map(|_| AckLedger::default())
                .collect(),
        })
    }

    fn admit(self: &Arc<Self>) -> Option<WorkerHold> {
        self.membership.reserve().then(|| WorkerHold(self.clone()))
    }

    fn close(&self) {
        self.membership.close();
        self.wake.send_replace(());
    }

    fn rotate_rates(&self) {
        self.achieved
            .store(self.window.swap(0, Ordering::Relaxed), Ordering::Relaxed);
        for rate in &self.rates {
            rate.rate
                .store(rate.window.swap(0, Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Generator ramp owns its measurement-window reset; a poisoned histogram cannot represent the new concurrency window; recovering partial samples would mislabel the measured workload"
    )]
    fn set_concurrency(&self, desired: u64) {
        if self.concurrency.swap(desired, Ordering::Relaxed) != desired {
            self.hist_window.lock().unwrap().reset();
        }
    }
}

/// Constructed before spawn; the active count includes even an unpolled child.
struct WorkerHold(Arc<GeneratorState>);
impl Drop for WorkerHold {
    fn drop(&mut self) {
        self.0.membership.release();
    }
}

impl GeneratorState {
    #[expect(
        clippy::unwrap_used,
        reason = "Generator measurement owns the ACK histograms; poisoned samples cannot describe a successful measurement; substituting empty histograms would conceal a failed run"
    )]
    fn record(&self, attempt: &Attempt, elapsed: Duration) -> anyhow::Result<()> {
        let micros =
            u64::try_from(elapsed.as_micros()).context("latency exceeds u64 microseconds")?;
        let mut hist = self.hist.lock().unwrap();
        let mut window = self.hist_window.lock().unwrap();
        // Validate both fixed ranges before either histogram or the ACK ledger changes.
        anyhow::ensure!(
            micros <= hist.high() && micros <= window.high(),
            "latency exceeds histogram range"
        );
        hist.record(micros)?;
        window.record(micros)?;
        self.ok.fetch_add(1, Ordering::Relaxed);
        match attempt.kind {
            AttemptKind::Read => {
                self.ok_reads.fetch_add(1, Ordering::Relaxed);
            }
            AttemptKind::Append => {
                self.ok_appends.fetch_add(1, Ordering::Relaxed);
                let ledger = &self.ledger[attempt.stream];
                ledger.count.fetch_add(1, Ordering::Relaxed);
                ledger.sum.fetch_add(attempt.nonce, Ordering::Relaxed);
                ledger.xor.fetch_xor(attempt.nonce, Ordering::Relaxed);
            }
        }
        self.window.fetch_add(1, Ordering::Relaxed);
        // Workload::attempt chooses from this exact attribution set (or index
        // zero when absent); clamping would hide a broken owner invariant.
        self.rates[attempt.attribution]
            .window
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Generator error observation owns its latest failure; poisoning means that observation was interrupted; silent recovery would conceal a corrupt run"
    )]
    fn record_error(&self, error: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().unwrap() = error;
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Generator snapshot owns measured histogram and error observations; poisoned observations cannot certify a run; empty replacement values would falsely validate measurements"
    )]
    fn snapshot(&self) -> serde_json::Value {
        // Acquire terminal membership BEFORE reading counters. After closed + zero,
        // every worker's accounting precedes its final release and no admission is possible.
        let (draining, active_workers) = self.membership.snapshot();
        let hist = self.hist.lock().unwrap();
        let window = self.hist_window.lock().unwrap();
        let per_up: Vec<_> = self
            .rates
            .iter()
            .map(|r| r.rate.load(Ordering::Relaxed))
            .collect();
        let ledger: Vec<_> = self
            .ledger
            .iter()
            .map(|a| {
                serde_json::json!({
                    "count": a.count.load(Ordering::Relaxed),
                    "sum": a.sum.load(Ordering::Relaxed),
                    "xor": a.xor.load(Ordering::Relaxed),
                })
            })
            .collect();
        serde_json::json!({
            "mode": "closed-loop", "winP50Ms": window.value_at_quantile(0.5) as f64 / 1000.0,
            "winP99Ms": window.value_at_quantile(0.99) as f64 / 1000.0, "winSamples": window.len(),
            "concurrency": self.concurrency.load(Ordering::Relaxed),
            "achievedPerSec": self.achieved.load(Ordering::Relaxed), "perUpstreamPerSec": per_up,
            "ok": self.ok.load(Ordering::Relaxed), "okAppends": self.ok_appends.load(Ordering::Relaxed),
            "okReads": self.ok_reads.load(Ordering::Relaxed), "ledger": ledger,
            "errs": self.errors.load(Ordering::Relaxed), "throttled": self.throttled.load(Ordering::Relaxed),
            "draining": draining, "activeWorkers": active_workers, "meanMs": hist.mean() / 1000.0,
            "p50Ms": hist.value_at_quantile(0.5) as f64 / 1000.0,
            "p99Ms": hist.value_at_quantile(0.99) as f64 / 1000.0,
            "maxMs": hist.max() as f64 / 1000.0,
            "elapsedMin": self.start.elapsed().as_secs_f64() / 60.0,
            "lastErr": self.last_error.lock().unwrap().clone(),
        })
    }

    async fn backoff(&self, delay: Duration) {
        let mut wake = self.wake.subscribe();
        if self.membership.is_closed() {
            return;
        }
        tokio::select! {
            () = tokio::time::sleep(delay) => {},
            _ = wake.changed() => {},
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AttemptKind {
    Read,
    Append,
}

struct Attempt {
    nonce: u64,
    stream: usize,
    name: String,
    target: usize,
    attribution: usize,
    kind: AttemptKind,
}

struct Workload {
    config: GeneratorConfig,
    http: RotatingClient,
    state: Arc<GeneratorState>,
    sequence: AtomicU64,
}

impl Workload {
    fn new(config: GeneratorConfig) -> anyhow::Result<Self> {
        Ok(Self {
            state: Arc::new(GeneratorState::new(&config)?),
            config,
            http: RotatingClient::new(),
            sequence: AtomicU64::new(0),
        })
    }

    fn attempt(&self, nonce: u64) -> anyhow::Result<Attempt> {
        let count =
            u64::try_from(self.config.streams.get()).context("stream count exceeds nonce range")?;
        let stream =
            usize::try_from(nonce % count).context("stream index exceeds address space")?;
        let name = format!("{}-{stream}", self.config.prefix);
        let target = pick(&name, &self.config.targets);
        // Without an attribution list the public rate vector has one aggregate
        // bucket, independent of how many request-routing targets are present.
        let attribution = if self.config.attribution.is_empty() {
            0
        } else {
            pick(&name, &self.config.attribution)
        };
        let read_every = self.config.read_every;
        let kind = if read_every > 0 && nonce % read_every == read_every - 1 {
            AttemptKind::Read
        } else {
            AttemptKind::Append
        };
        Ok(Attempt {
            nonce,
            stream,
            name,
            target,
            attribution,
            kind,
        })
    }

    async fn create_streams(&self) {
        let http = self.http.get();
        for i in 0..self.config.streams.get() {
            let name = format!("{}-{i}", self.config.prefix);
            let target = &self.config.targets[pick(&name, &self.config.targets)];
            let result = http
                .put(format!("{target}/v1/stream/{name}"))
                .header("authorization", format!("Bearer {}", self.config.auth))
                .header("stream-encryption-key", &self.config.key)
                .header("content-type", "application/json")
                .send()
                .await;
            if let Err(error) = result {
                eprintln!("create {name}: {error}");
            }
        }
    }

    async fn execute(&self, attempt: &Attempt) -> anyhow::Result<()> {
        let started = Instant::now();
        let http = self.http.get();
        let url = format!(
            "{}/v1/stream/{}",
            self.config.targets[attempt.target], attempt.name
        );
        let request = match attempt.kind {
            AttemptKind::Read => http.get(format!("{url}?offset=now")),
            AttemptKind::Append => {
                let records: Vec<_> = (0..self.config.batch.get()).map(|batch| serde_json::json!({
                    "i": attempt.nonce, "b": batch, "t": now_ms(), "pad": "x".repeat(self.config.record_pad),
                })).collect();
                http.post(url).json(&records)
            }
        }.header("authorization", format!("Bearer {}", self.config.auth))
            .header("stream-encryption-key", &self.config.key);
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                // The service ACK is the success status. Drain its body for pool reuse
                // and preserve the generator's body-inclusive latency convention.
                drop(response.bytes().await);
                self.state.record(attempt, started.elapsed())?;
            }
            Ok(response) if matches!(response.status().as_u16(), 429 | 503) => {
                self.state.throttled.fetch_add(1, Ordering::Relaxed);
                let delay = retry_delay(response.headers(), attempt.nonce);
                drop(response);
                self.state.backoff(delay).await;
            }
            Ok(response) => {
                self.state
                    .record_error(format!("status {} on {}", response.status(), attempt.name))
            }
            Err(error) => self.state.record_error(error.to_string()),
        }
        Ok(())
    }

    async fn worker(&self, _hold: WorkerHold) -> anyhow::Result<()> {
        while !self.state.membership.is_closed() {
            let nonce = self
                .sequence
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| anyhow::anyhow!("operation nonce space exhausted"))?;
            self.execute(&self.attempt(nonce)?).await?;
        }
        Ok(())
    }
}

fn retry_delay(headers: &reqwest::header::HeaderMap, nonce: u64) -> Duration {
    let millis = headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(500, |seconds| seconds.saturating_mul(1000));
    Duration::from_millis(millis.saturating_add(nonce % 400))
}

/// Owns every worker directly. No controller task can outlive or detach children.
struct Generator {
    workload: Arc<Workload>,
    workers: JoinSet<anyhow::Result<()>>,
}

impl Generator {
    #[expect(
        clippy::disallowed_methods,
        reason = "Generator owns the complete worker set; membership is reserved before spawn and every terminal exit aborts then joins the set; a detached worker could invalidate final drain accounting"
    )]
    fn launch(&mut self) -> bool {
        let Some(hold) = self.workload.state.admit() else {
            return false;
        };
        let workload = self.workload.clone();
        self.workers
            .spawn(async move { workload.worker(hold).await });
        true
    }

    fn ramp(&mut self) {
        let desired = self
            .workload
            .config
            .desired(self.workload.state.start.elapsed());
        self.workload.state.set_concurrency(desired);
        for _ in self.workers.len()..usize::try_from(desired).unwrap_or(usize::MAX) {
            if !self.launch() {
                break;
            }
        }
    }

    async fn serve(
        &mut self,
        server: impl std::future::Future<Output = std::io::Result<()>>,
        signal: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let mut rate = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let mut ramp = tokio::time::interval(Duration::from_secs(1));
        rate.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ramp.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(server, signal);
        let result = loop {
            tokio::select! {
                result = &mut server => break result.context("generator stats server"),
                () = &mut signal, if !self.workload.state.membership.is_closed() => {
                    self.workload.state.close();
                    println!("pilot gen: SIGTERM -> draining");
                }
                _ = rate.tick() => self.workload.state.rotate_rates(),
                _ = ramp.tick(), if !self.workload.state.membership.is_closed() => {
                    self.ramp();
                }
                Some(result) = self.workers.join_next(), if !self.workers.is_empty() => {
                    match result {
                        Ok(Ok(())) if self.workload.state.membership.is_closed() => {},
                        Ok(Ok(())) => break Err(anyhow::anyhow!("generator worker exited before drain")),
                        Ok(Err(error)) => break Err(error),
                        Err(error) => break Err(error.into()),
                    }
                }
            }
        };
        self.workload.state.close();
        self.workers.abort_all();
        while self.workers.join_next().await.is_some() {}
        result
    }
}

async fn stats(State(state): State<Arc<GeneratorState>>) -> impl axum::response::IntoResponse {
    (
        [("access-control-allow-origin", "*")],
        axum::Json(state.snapshot()),
    )
}

async fn drain(State(state): State<Arc<GeneratorState>>) -> axum::Json<serde_json::Value> {
    state.close();
    axum::Json(
        serde_json::json!({"draining": true, "activeWorkers": state.membership.snapshot().1}),
    )
}

fn router(state: Arc<GeneratorState>) -> Router {
    Router::new()
        .route("/", get(stats))
        .route("/stats", get(stats))
        .route("/drain", get(drain).post(drain))
        .with_state(state)
}

pub(super) async fn run() -> anyhow::Result<()> {
    let workload = Arc::new(Workload::new(GeneratorConfig::load(env)?)?);
    workload.create_streams().await;
    let port = workload.config.port;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).await?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let signal = async move {
        #[cfg(unix)]
        {
            terminate.recv().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
    };
    println!(
        "pilot gen: {} stream(s), conc {}→{} doubling every {}s, batch {}, {} target(s)",
        workload.config.streams,
        workload.config.concurrency_start,
        workload.config.concurrency_max,
        workload.config.ramp_secs,
        workload.config.batch,
        workload.config.targets.len()
    );
    println!("pilot gen stats on :{port}");
    let app = router(workload.state.clone());
    let mut generator = Generator {
        workload,
        workers: JoinSet::new(),
    };
    generator
        .serve(
            std::future::IntoFuture::into_future(axum::serve(listener, app)),
            signal,
        )
        .await
}

#[cfg(test)]
#[path = "generator/tests.rs"]
mod tests;
