//! HTTP load driver for Durable Streams servers (old Bun/SQLite or new
//! SlateDB implementation — same protocol).
//!
//! Modes:
//!   append     - concurrent appends, measures ACK latency + throughput
//!   read       - replays streams from offset -1, measures read throughput
//!   durability - appends one record, then polls until it is durable in
//!                object storage (old server: /_details uploaded_through;
//!                new server: durable at ACK by construction)

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Parser, Debug, Clone)]
#[command(name = "bench")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    #[arg(long, default_value = "append")]
    mode: String,
    /// Concurrent in-flight requests.
    #[arg(long, default_value = "64")]
    concurrency: NonZeroUsize,
    /// Distinct streams to spread appends across.
    #[arg(long, default_value = "16")]
    streams: NonZeroUsize,
    #[arg(long, default_value_t = 256)]
    payload_bytes: usize,
    /// Entries per append request (JSON array mode when > 1).
    #[arg(long, default_value = "1")]
    entries: NonZeroUsize,
    #[arg(long, default_value = "15")]
    duration_secs: NonZeroU64,
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    /// Stream name prefix (change between runs to write fresh streams).
    #[arg(long, default_value = "bench")]
    prefix: String,
    /// Emit machine-readable JSON summary line at the end.
    #[arg(long, default_value_t = false)]
    json: bool,
    /// Label included in the JSON summary.
    #[arg(long, default_value = "")]
    label: String,
    /// Stream encryption key (base64url, 32 bytes) sent as
    /// Stream-Encryption-Key on every request. Omit for servers that don't
    /// require it (the old TS implementation).
    #[arg(long, env = "STREAM_KEY")]
    key: Option<String>,
}

fn keyed(mut rb: reqwest::RequestBuilder, key: &Option<String>) -> reqwest::RequestBuilder {
    if let Some(k) = key {
        rb = rb.header("stream-encryption-key", k.as_str());
    }
    rb
}

struct Shared {
    hist: Mutex<Histogram<u64>>,
    ok: AtomicU64,
    errors: AtomicU64,
    entries_ok: AtomicU64,
    bytes_ok: AtomicU64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.mode.as_str() {
        "append" => bench_append(args).await,
        "read" => bench_read(args).await,
        "durability" => bench_durability(args).await,
        other => anyhow::bail!("unknown mode: {other}"),
    }
}

fn make_client(concurrency: usize) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency.saturating_add(8))
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(30))
        .http1_only()
        .build()
}

fn payload(bytes: usize, entries: usize) -> (Vec<u8>, &'static str) {
    if entries > 1 {
        // JSON array of objects, each with a payload of roughly `bytes` chars.
        let filler = "x".repeat(bytes.saturating_sub(20).max(1));
        let one = format!("{{\"v\":\"{filler}\"}}");
        let body = format!(
            "[{}]",
            std::iter::repeat_n(one, entries)
                .collect::<Vec<_>>()
                .join(",")
        );
        (body.into_bytes(), "application/json")
    } else {
        (vec![b'x'; bytes], "application/octet-stream")
    }
}

impl Shared {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3)?),
            ok: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            entries_ok: AtomicU64::new(0),
            bytes_ok: AtomicU64::new(0),
        })
    }

    async fn record_success(
        &self,
        elapsed: Duration,
        entries: u64,
        bytes: u64,
    ) -> anyhow::Result<()> {
        // Do not publish throughput for a sample the latency histogram rejected.
        let micros = u64::try_from(elapsed.as_micros())?;
        self.hist.lock().await.record(micros)?;
        self.ok.fetch_add(1, Ordering::Relaxed);
        self.entries_ok.fetch_add(entries, Ordering::Relaxed);
        self.bytes_ok.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}

/// Owns the common append window, payload and measurements shared by all workers.
struct AppendRun {
    args: Args,
    client: reqwest::Client,
    shared: Shared,
    body: Vec<u8>,
    content_type: &'static str,
    entries: u64,
    bytes: u64,
    measure_start: Instant,
    stop_at: Instant,
}

impl AppendRun {
    async fn start(args: Args) -> anyhow::Result<Self> {
        let client = make_client(args.concurrency.get())?;
        let shared = Shared::new()?;
        let entries = u64::try_from(args.entries.get())?;
        let bytes = u64::try_from(args.payload_bytes)?
            .checked_mul(entries)
            .context("requested bytes per append overflow the measurement counter")?;
        let (body, content_type) = payload(args.payload_bytes, args.entries.get());
        // Old servers reject appends to absent streams. Start the window only
        // after all stream creation has succeeded, as in the original workload.
        for stream in 0..args.streams.get() {
            let response = keyed(
                client.put(format!("{}/v1/stream/{}-{stream}", args.url, args.prefix)),
                &args.key,
            )
            .send()
            .await?;
            anyhow::ensure!(
                response.status().is_success(),
                "create stream failed: {}",
                response.status()
            );
        }
        let measure_start = Instant::now()
            .checked_add(Duration::from_secs(args.warmup_secs))
            .context("warmup exceeds the monotonic clock range")?;
        let stop_at = measure_start
            .checked_add(Duration::from_secs(args.duration_secs.get()))
            .context("duration exceeds the monotonic clock range")?;
        Ok(Self {
            args,
            client,
            shared,
            body,
            content_type,
            entries,
            bytes,
            measure_start,
            stop_at,
        })
    }

    async fn worker(&self, index: usize) -> anyhow::Result<()> {
        let url = format!(
            "{}/v1/stream/{}-{}",
            self.args.url,
            self.args.prefix,
            index % self.args.streams.get()
        );
        while Instant::now() < self.stop_at {
            let started = Instant::now();
            let response = keyed(self.client.post(&url), &self.args.key)
                .header("content-type", self.content_type)
                .body(self.body.clone())
                .send()
                .await;
            // The existing benchmark measures response-header ACK latency.
            // Drain the body for pool reuse without changing that definition.
            let elapsed = started.elapsed();
            let success = match response {
                Ok(response) => {
                    let success = response.status().is_success();
                    drop(response.bytes().await);
                    success
                }
                Err(_) => false,
            };
            if started < self.measure_start {
                continue;
            }
            if success {
                self.shared
                    .record_success(elapsed, self.entries, self.bytes)
                    .await?;
            } else {
                self.shared.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

/// Retains every worker until success or terminal cancellation. An error aborts
/// and joins the remaining tasks before it can escape to a benchmark caller.
async fn finish_workers<T: Send + 'static>(
    mut workers: JoinSet<anyhow::Result<T>>,
) -> anyhow::Result<Vec<T>> {
    let mut results = Vec::with_capacity(workers.len());
    while let Some(joined) = workers.join_next().await {
        let result = joined
            .map_err(anyhow::Error::from)
            .and_then(|result| result);
        match result {
            Ok(value) => results.push(value),
            Err(error) => {
                workers.shutdown().await;
                return Err(error);
            }
        }
    }
    Ok(results)
}

#[expect(
    clippy::disallowed_methods,
    reason = "Append benchmark owns this finite worker group; finish_workers aborts and joins all siblings on failure, and JoinSet aborts on cancellation; detached handles would lose terminal ownership"
)]
async fn bench_append(args: Args) -> anyhow::Result<()> {
    let run = Arc::new(AppendRun::start(args).await?);
    let mut workers = JoinSet::new();
    for index in 0..run.args.concurrency.get() {
        let run = run.clone();
        workers.spawn(async move { run.worker(index).await });
    }
    finish_workers(workers).await?;
    let args = &run.args;
    let shared = &run.shared;
    let hist = shared.hist.lock().await;
    let ok = shared.ok.load(Ordering::Relaxed);
    let errors = shared.errors.load(Ordering::Relaxed);
    let entries_ok = shared.entries_ok.load(Ordering::Relaxed);
    let secs = args.duration_secs.get() as f64;
    let summary = serde_json::json!({
        "label": args.label,
        "mode": "append",
        "concurrency": args.concurrency.get(),
        "streams": args.streams.get(),
        "payload_bytes": args.payload_bytes,
        "entries_per_req": args.entries.get(),
        "duration_secs": secs,
        "requests_ok": ok,
        "errors": errors,
        "req_per_sec": ok as f64 / secs,
        "entries_per_sec": entries_ok as f64 / secs,
        "mb_per_sec": shared.bytes_ok.load(Ordering::Relaxed) as f64 / secs / 1e6,
        "latency_ms": {
            "p50": hist.value_at_quantile(0.50) as f64 / 1000.0,
            "p90": hist.value_at_quantile(0.90) as f64 / 1000.0,
            "p99": hist.value_at_quantile(0.99) as f64 / 1000.0,
            "p999": hist.value_at_quantile(0.999) as f64 / 1000.0,
            "max": hist.max() as f64 / 1000.0,
            "mean": hist.mean() / 1000.0,
        },
    });
    if args.json {
        println!("{summary}");
    } else {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    }
    Ok(())
}

#[derive(Default, Debug, PartialEq, Eq)]
struct ReadTotals {
    bytes: u64,
    requests: u64,
}

async fn read_stream(
    client: &reqwest::Client,
    url: &str,
    key: &Option<String>,
) -> anyhow::Result<ReadTotals> {
    let mut offset = "-1".to_string();
    let mut totals = ReadTotals::default();
    loop {
        let response = keyed(client.get(format!("{url}?offset={offset}")), key)
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "read failed: {}",
            response.status()
        );
        let next = response
            .headers()
            .get("stream-next-offset")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await?;
        totals.requests += 1;
        totals.bytes += u64::try_from(body.len())?;
        let Some(next) = next else { return Ok(totals) };
        if body.is_empty() || next == offset {
            return Ok(totals);
        }
        offset = next;
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "Read benchmark owns all stream replays; finish_workers aborts and joins siblings before returning any failure, and JoinSet aborts on cancellation; detached tasks could outlive a failed measurement"
)]
async fn bench_read(args: Args) -> anyhow::Result<()> {
    let client = make_client(args.concurrency.get())?;
    let t0 = Instant::now();
    let mut workers = JoinSet::new();
    for stream in 0..args.streams.get() {
        let client = client.clone();
        let url = format!("{}/v1/stream/{}-{stream}", args.url, args.prefix);
        let key = args.key.clone();
        workers.spawn(async move { read_stream(&client, &url, &key).await });
    }
    let mut total_bytes = 0;
    let mut total_reqs = 0;
    for result in finish_workers(workers).await? {
        total_bytes += result.bytes;
        total_reqs += result.requests;
    }
    let secs = t0.elapsed().as_secs_f64();
    let summary = serde_json::json!({
        "label": args.label,
        "mode": "read",
        "streams": args.streams.get(),
        "requests": total_reqs,
        "total_mb": total_bytes as f64 / 1e6,
        "secs": secs,
        "mb_per_sec": total_bytes as f64 / secs / 1e6,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

/// Measures the gap between append ACK and object-store durability.
async fn bench_durability(args: Args) -> anyhow::Result<()> {
    let client = make_client(4)?;
    let stream = format!("{}-dur-{}", args.prefix, std::process::id());
    let url = format!("{}/v1/stream/{}", args.url, stream);
    let r = keyed(client.put(&url), &args.key).send().await?;
    anyhow::ensure!(r.status().is_success(), "create failed: {}", r.status());
    let mut lags_ms = [0.0f64; 10];
    for sample in &mut lags_ms {
        let t0 = Instant::now();
        let res = keyed(client.post(&url), &args.key)
            .header("content-type", "application/octet-stream")
            .body(vec![b'x'; args.payload_bytes])
            .send()
            .await?;
        anyhow::ensure!(res.status().is_success(), "append failed: {}", res.status());
        let ack = t0.elapsed();
        // Poll the details endpoint until uploaded_through covers next_offset.
        // The new server has no lag by construction (ACK == durable); its
        // /_details endpoint doesn't exist, which reports as lag 0.
        let details_url = format!("{url}/_details");
        let mut lag = 0.0f64;
        let ack_at = Instant::now();
        loop {
            let r = client.get(&details_url).send().await?;
            if r.status() == reqwest::StatusCode::NOT_FOUND {
                break; // new server: durable at ACK
            }
            let v: serde_json::Value = r.json().await?;
            let s = &v["stream"];
            let next: i64 = s["next_offset"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let uploaded: i64 = s["uploaded_through"]
                .as_str()
                .unwrap_or("-1")
                .parse()
                .unwrap_or(-1);
            if next > 0 && uploaded >= next - 1 {
                lag = ack_at.elapsed().as_secs_f64() * 1000.0;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        *sample = lag;
        println!(
            "append ack: {:.1}ms, ack->durable lag: {:.1}ms",
            ack.as_secs_f64() * 1000.0,
            lag
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    lags_ms.sort_by(f64::total_cmp);
    println!(
        "durability lag ms: min={:.1} median={:.1} max={:.1}",
        lags_ms[0],
        lags_ms[lags_ms.len() / 2],
        lags_ms[9]
    );
    Ok(())
}

#[cfg(test)]
#[path = "bench/tests.rs"]
mod tests;
