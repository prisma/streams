//! HTTP load driver for Durable Streams servers (old Bun/SQLite or new
//! SlateDB implementation — same protocol).
//!
//! Modes:
//!   append     - concurrent appends, measures ACK latency + throughput
//!   read       - replays streams from offset -1, measures read throughput
//!   durability - appends one record, then polls until it is durable in
//!                object storage (old server: /_details uploaded_through;
//!                new server: durable at ACK by construction)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;

#[derive(Parser, Debug, Clone)]
#[command(name = "bench")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    #[arg(long, default_value = "append")]
    mode: String,
    /// Concurrent in-flight requests.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Distinct streams to spread appends across.
    #[arg(long, default_value_t = 16)]
    streams: usize,
    #[arg(long, default_value_t = 256)]
    payload_bytes: usize,
    /// Entries per append request (JSON array mode when > 1).
    #[arg(long, default_value_t = 1)]
    entries: usize,
    #[arg(long, default_value_t = 15)]
    duration_secs: u64,
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

fn make_client(concurrency: usize) -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency + 8)
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(30))
        .http1_only()
        .build()
        .expect("client")
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

async fn bench_append(args: Args) -> anyhow::Result<()> {
    let client = make_client(args.concurrency);
    let shared = Arc::new(Shared {
        hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
        ok: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        entries_ok: AtomicU64::new(0),
        bytes_ok: AtomicU64::new(0),
    });
    let (body, content_type) = payload(args.payload_bytes, args.entries);
    let body = Arc::new(body);

    // Pre-create the target streams (the old server 404s appends to
    // non-existent streams).
    for s in 0..args.streams {
        let r = keyed(
            client.put(format!("{}/v1/stream/{}-{}", args.url, args.prefix, s)),
            &args.key,
        )
        .send()
        .await?;
        anyhow::ensure!(
            r.status().is_success(),
            "create stream failed: {}",
            r.status()
        );
    }

    let warmup_until = Instant::now() + Duration::from_secs(args.warmup_secs);
    let stop_at = warmup_until + Duration::from_secs(args.duration_secs);
    let measure_start = warmup_until;

    let mut handles = Vec::new();
    for w in 0..args.concurrency {
        let client = client.clone();
        let shared = shared.clone();
        let body = body.clone();
        let url = format!(
            "{}/v1/stream/{}-{}",
            args.url,
            args.prefix,
            w % args.streams
        );
        let entries = args.entries as u64;
        let bytes = args.payload_bytes as u64 * entries;
        let content_type = content_type.to_string();
        let key = args.key.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let now = Instant::now();
                if now >= stop_at {
                    break;
                }
                let t0 = Instant::now();
                let res = keyed(client.post(&url), &key)
                    .header("content-type", content_type.as_str())
                    .body(body.as_ref().clone())
                    .send()
                    .await;
                let elapsed = t0.elapsed();
                let in_window = t0 >= measure_start;
                match res {
                    Ok(r) if r.status().is_success() => {
                        let _ = r.bytes().await;
                        if in_window {
                            shared.ok.fetch_add(1, Ordering::Relaxed);
                            shared.entries_ok.fetch_add(entries, Ordering::Relaxed);
                            shared.bytes_ok.fetch_add(bytes, Ordering::Relaxed);
                            shared
                                .hist
                                .lock()
                                .await
                                .record(elapsed.as_micros() as u64)
                                .ok();
                        }
                    }
                    Ok(r) => {
                        let _ = r.bytes().await;
                        if in_window {
                            shared.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        if in_window {
                            shared.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await?;
    }

    let hist = shared.hist.lock().await;
    let ok = shared.ok.load(Ordering::Relaxed);
    let errors = shared.errors.load(Ordering::Relaxed);
    let entries_ok = shared.entries_ok.load(Ordering::Relaxed);
    let secs = args.duration_secs as f64;
    let summary = serde_json::json!({
        "label": args.label,
        "mode": "append",
        "concurrency": args.concurrency,
        "streams": args.streams,
        "payload_bytes": args.payload_bytes,
        "entries_per_req": args.entries,
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

async fn bench_read(args: Args) -> anyhow::Result<()> {
    let client = make_client(args.concurrency);
    let t0 = Instant::now();
    let mut total_bytes = 0u64;
    let mut total_reqs = 0u64;
    let mut handles = Vec::new();
    for s in 0..args.streams {
        let client = client.clone();
        let url = format!("{}/v1/stream/{}-{}", args.url, args.prefix, s);
        let key = args.key.clone();
        handles.push(tokio::spawn(async move {
            let mut offset = "-1".to_string();
            let mut bytes = 0u64;
            let mut reqs = 0u64;
            loop {
                let res = keyed(client.get(format!("{url}?offset={offset}")), &key)
                    .send()
                    .await;
                let Ok(r) = res else { break };
                if !r.status().is_success() {
                    break;
                }
                let next = r
                    .headers()
                    .get("stream-next-offset")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let body = r.bytes().await.unwrap_or_default();
                reqs += 1;
                bytes += body.len() as u64;
                let Some(next) = next else { break };
                if body.is_empty() || next == offset {
                    break;
                }
                offset = next;
            }
            (bytes, reqs)
        }));
    }
    for h in handles {
        let (b, r) = h.await?;
        total_bytes += b;
        total_reqs += r;
    }
    let secs = t0.elapsed().as_secs_f64();
    let summary = serde_json::json!({
        "label": args.label,
        "mode": "read",
        "streams": args.streams,
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
    let client = make_client(4);
    let stream = format!("{}-dur-{}", args.prefix, std::process::id());
    let url = format!("{}/v1/stream/{}", args.url, stream);
    let r = keyed(client.put(&url), &args.key).send().await?;
    anyhow::ensure!(r.status().is_success(), "create failed: {}", r.status());
    let mut lags_ms: Vec<f64> = Vec::new();
    let iterations = 10usize;
    for _ in 0..iterations {
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
        lags_ms.push(lag);
        println!(
            "append ack: {:.1}ms, ack->durable lag: {:.1}ms",
            ack.as_secs_f64() * 1000.0,
            lag
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    lags_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "durability lag ms: min={:.1} median={:.1} max={:.1}",
        lags_ms.first().unwrap(),
        lags_ms[lags_ms.len() / 2],
        lags_ms.last().unwrap()
    );
    Ok(())
}
