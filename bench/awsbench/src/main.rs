//! awsbench: single-ordered-unit benchmark harness for the AWS comparison
//! campaign (bench/aws-comparison-plan.md). One binary drives all three
//! systems — Kinesis (1 shard), SQS FIFO (1 message group), Prisma Streams
//! (1 stream) — with identical closed-loop shapes and identical accounting,
//! so harness variance is eliminated from the comparison.
//!
//! Accounting rules (the plan's): every response is {ack, throttle, error};
//! SDK retries are DISABLED so throttles are measured, never hidden. Kinesis
//! PutRecords partial failures count delivered records exactly and classify
//! the request as a throttle.
//!
//! Runs standalone (CLI flags) or on Prisma Compute (all flags have env
//! aliases; a minimal HTTP listener on $PORT serves the collected JSONL so
//! the platform sees liveness and the operator can scrape results).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;

#[derive(Parser, Debug, Clone)]
#[command(name = "awsbench", about = "Kinesis/SQS/Prisma single-unit benchmark shapes")]
struct Args {
    /// kinesis | sqs | prisma
    #[arg(long, env = "BENCH_SYSTEM")]
    system: String,
    /// a=latency-floor, b=record-ceiling sweep, c=byte-ceiling, d=tail, e=overload
    /// Shape selector. Has a default because it is a REQUIRED clap arg
    /// otherwise: a deploy that forgets BENCH_SHAPE makes the binary exit
    /// instantly at startup, and Compute still reports the version
    /// "running" while its domain 404s — which looks exactly like a boot
    /// failure and cost a soak window to diagnose (2026-07-26).
    #[arg(long, env = "BENCH_SHAPE", default_value = "a")]
    shape: String,
    /// Kinesis stream name | SQS queue URL | Prisma base URL
    #[arg(long, env = "BENCH_TARGET")]
    target: String,
    /// Fixed concurrency (shapes a/c/d/e); shape b sweeps 2..=128
    #[arg(long, env = "BENCH_CONC", default_value_t = 1)]
    conc: usize,
    /// Records per request (SQS caps at 10 — actual sent count is recorded)
    #[arg(long, env = "BENCH_BATCH", default_value_t = 1)]
    batch: usize,
    /// Payload bytes per record
    #[arg(long, env = "BENCH_RECORD_BYTES", default_value_t = 200)]
    record_bytes: usize,
    /// Run duration seconds (per sweep step for shape b)
    #[arg(long, env = "BENCH_SECS", default_value_t = 300)]
    secs: u64,
    /// Shape d: run the consumer side and measure producer->receive
    #[arg(long, env = "BENCH_CONSUME", default_value_t = false)]
    consume: bool,
    /// JSONL output path
    #[arg(long, env = "BENCH_OUT", default_value = "awsbench.jsonl")]
    out: String,
    /// Prisma auth token (system=prisma)
    #[arg(long, env = "AUTH_TOKEN", default_value = "")]
    auth: String,
    /// Prisma stream encryption key (system=prisma)
    #[arg(long, env = "STREAM_KEY", default_value = "")]
    stream_key: String,
    /// Prisma stream name (system=prisma)
    #[arg(long, env = "BENCH_STREAM", default_value = "cmp-1")]
    stream: String,
}

struct Stats {
    ok: AtomicU64,
    throttled: AtomicU64,
    errs: AtomicU64,
    records: AtomicU64,
    window_ok: AtomicU64,
    window_records: AtomicU64,
    hist: Mutex<Histogram<u64>>,
    hist_win: Mutex<Histogram<u64>>,
    tail_win: Mutex<Histogram<u64>>,
    last_err: Mutex<String>,
    lines: Mutex<Vec<String>>,
}

impl Stats {
    fn new() -> Arc<Stats> {
        Arc::new(Stats {
            ok: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            errs: AtomicU64::new(0),
            records: AtomicU64::new(0),
            window_ok: AtomicU64::new(0),
            window_records: AtomicU64::new(0),
            hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            hist_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            tail_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            last_err: Mutex::new(String::new()),
            lines: Mutex::new(Vec::new()),
        })
    }
    fn record_ok(&self, lat_us: u64, records: u64) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        self.window_ok.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
        self.window_records.fetch_add(records, Ordering::Relaxed);
        let _ = self.hist.lock().unwrap().record(lat_us.max(1));
        let _ = self.hist_win.lock().unwrap().record(lat_us.max(1));
    }
    fn record_throttle(&self, delivered: u64) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(delivered, Ordering::Relaxed);
        self.window_records.fetch_add(delivered, Ordering::Relaxed);
    }
    fn record_err(&self, msg: String) {
        self.errs.fetch_add(1, Ordering::Relaxed);
        *self.last_err.lock().unwrap() = msg;
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Binary payload: 8-byte big-endian producer timestamp + filler.
fn payload(record_bytes: usize) -> Vec<u8> {
    let mut p = vec![b'x'; record_bytes.max(8)];
    p[..8].copy_from_slice(&now_ms().to_be_bytes());
    p
}

enum Outcome {
    /// All records accepted.
    Ok { records: u64 },
    /// Request throttled (possibly partially delivered — Kinesis).
    Throttle { delivered: u64 },
    Err(String),
}

#[derive(Clone)]
enum Client {
    Kinesis(aws_sdk_kinesis::Client, String),
    Sqs(aws_sdk_sqs::Client, String),
    /// (http, base_url, stream_name, auth, key)
    Prisma(reqwest::Client, String, String, String, String),
}

impl Client {
    async fn send(&self, batch: usize, record_bytes: usize, seq: u64) -> Outcome {
        match self {
            Client::Kinesis(c, stream) => {
                let entries: Vec<_> = (0..batch)
                    .map(|_| {
                        aws_sdk_kinesis::types::PutRecordsRequestEntry::builder()
                            .data(aws_sdk_kinesis::primitives::Blob::new(payload(record_bytes)))
                            .partition_key("slate-cmp") // one shard, one ordered unit
                            .build()
                            .unwrap()
                    })
                    .collect();
                match c.put_records().stream_name(stream).set_records(Some(entries)).send().await {
                    Ok(out) => {
                        let failed = out.failed_record_count().unwrap_or(0) as u64;
                        if failed > 0 {
                            Outcome::Throttle { delivered: batch as u64 - failed }
                        } else {
                            Outcome::Ok { records: batch as u64 }
                        }
                    }
                    Err(e) => {
                        // The structured error code, not Display: SdkError's
                        // Display for service errors is the opaque "service
                        // error" (532k SQS throttles were misclassified as
                        // errors in the first campaign run before this).
                        let code = aws_sdk_kinesis::error::ProvideErrorMetadata::code(&e)
                            .unwrap_or_default()
                            .to_string();
                        if code.contains("ProvisionedThroughputExceeded")
                            || code.contains("Throttl")
                            || code.contains("LimitExceeded")
                        {
                            Outcome::Throttle { delivered: 0 }
                        } else {
                            Outcome::Err(format!("{code}: {e}"))
                        }
                    }
                }
            }
            Client::Sqs(c, queue_url) => {
                let n = batch.min(10); // SQS batch cap
                let entries: Vec<_> = (0..n)
                    .map(|i| {
                        let ts = format!("{:016x}", now_ms());
                        let body_len = record_bytes.max(16);
                        let mut body = ts;
                        body.push_str(&"x".repeat(body_len - 16));
                        aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                            .id(format!("m{i}"))
                            .message_body(body)
                            .message_group_id("slate-cmp")
                            .message_deduplication_id(format!("{seq}-{i}"))
                            .build()
                            .unwrap()
                    })
                    .collect();
                match c.send_message_batch().queue_url(queue_url).set_entries(Some(entries)).send().await {
                    Ok(out) => {
                        let failed = out.failed();
                        if failed.is_empty() {
                            Outcome::Ok { records: n as u64 }
                        } else {
                            let delivered = n as u64 - failed.len() as u64;
                            let throttle = failed
                                .iter()
                                .any(|f| f.code().contains("Throttl") || f.code().contains("RequestThrottled"));
                            if throttle {
                                Outcome::Throttle { delivered }
                            } else {
                                Outcome::Err(format!("batch failures: {}", failed[0].code()))
                            }
                        }
                    }
                    Err(e) => {
                        let code = aws_sdk_sqs::error::ProvideErrorMetadata::code(&e)
                            .unwrap_or_default()
                            .to_string();
                        if code.contains("Throttl") || code.contains("RequestThrottled") {
                            Outcome::Throttle { delivered: 0 }
                        } else {
                            Outcome::Err(format!("{code}: {e}"))
                        }
                    }
                }
            }
            Client::Prisma(http, base, stream, auth, key) => {
                let recs: Vec<serde_json::Value> = (0..batch)
                    .map(|b| {
                        serde_json::json!({
                            "t": now_ms(),
                            "b": b,
                            "pad": "x".repeat(record_bytes.saturating_sub(40).max(1)),
                        })
                    })
                    .collect();
                match http
                    .post(format!("{base}/v1/stream/{stream}"))
                    .header("authorization", format!("Bearer {auth}"))
                    .header("stream-encryption-key", key.clone())
                    .header("content-type", "application/json")
                    .json(&recs)
                    .send()
                    .await
                {
                    Ok(r) => {
                        let code = r.status().as_u16();
                        if (200..300).contains(&code) {
                            Outcome::Ok { records: batch as u64 }
                        } else if code == 429 || code == 503 {
                            Outcome::Throttle { delivered: 0 }
                        } else {
                            Outcome::Err(format!("status {code}"))
                        }
                    }
                    Err(e) => Outcome::Err(e.to_string()),
                }
            }
        }
    }
}

async fn run_load(
    client: Client,
    stats: Arc<Stats>,
    conc: usize,
    batch: usize,
    record_bytes: usize,
    secs: u64,
    label: &str,
    out: &mut std::fs::File,
) -> anyhow::Result<()> {
    use std::io::Write;
    let stop = Arc::new(AtomicU64::new(0));
    let seq = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..conc {
        let client = client.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let seq = seq.clone();
        workers.push(tokio::spawn(async move {
            while stop.load(Ordering::Relaxed) == 0 {
                let s = seq.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                match client.send(batch, record_bytes, s).await {
                    Outcome::Ok { records } => {
                        stats.record_ok(t0.elapsed().as_micros() as u64, records)
                    }
                    Outcome::Throttle { delivered } => stats.record_throttle(delivered),
                    Outcome::Err(m) => stats.record_err(m),
                }
            }
        }));
    }
    let t_end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < t_end {
        let left = t_end - Instant::now();
        tokio::time::sleep(Duration::from_secs(20).min(left)).await;
        let win_ok = stats.window_ok.swap(0, Ordering::Relaxed);
        let win_recs = stats.window_records.swap(0, Ordering::Relaxed);
        let (p50, p99, mean) = {
            let mut hw = stats.hist_win.lock().unwrap();
            let r = (
                hw.value_at_quantile(0.5) as f64 / 1000.0,
                hw.value_at_quantile(0.99) as f64 / 1000.0,
                hw.mean() / 1000.0,
            );
            hw.reset();
            r
        };
        let tail = {
            let mut tw = stats.tail_win.lock().unwrap();
            let r = if tw.is_empty() {
                None
            } else {
                Some((
                    tw.value_at_quantile(0.5) as f64 / 1000.0,
                    tw.value_at_quantile(0.99) as f64 / 1000.0,
                ))
            };
            tw.reset();
            r
        };
        let line = serde_json::json!({
            "ts": now_ms() / 1000,
            "label": label,
            "conc": conc,
            "batch": batch,
            "recordBytes": record_bytes,
            "achievedPerSec": win_ok / 20,
            "recordsPerSec": win_recs / 20,
            "winP50Ms": p50,
            "winP99Ms": p99,
            "meanMs": mean,
            "ok": stats.ok.load(Ordering::Relaxed),
            "errs": stats.errs.load(Ordering::Relaxed),
            "throttled": stats.throttled.load(Ordering::Relaxed),
            "tailP50Ms": tail.map(|t| t.0),
            "tailP99Ms": tail.map(|t| t.1),
            "lastErr": stats.last_err.lock().unwrap().clone(),
        })
        .to_string();
        writeln!(out, "{line}")?;
        out.flush()?;
        eprintln!("{line}");
        stats.lines.lock().unwrap().push(line);
    }
    stop.store(1, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    Ok(())
}

/// Shape D consumer: read the ordered unit, extract producer timestamps,
/// record producer->receive latency.
async fn run_consumer(client: Client, stats: Arc<Stats>, stop: Arc<AtomicU64>) {
    match client {
        Client::Kinesis(c, stream) => {
            let shard = c
                .list_shards()
                .stream_name(&stream)
                .send()
                .await
                .ok()
                .and_then(|o| o.shards().first().map(|s| s.shard_id().to_string()));
            let Some(shard_id) = shard else { return };
            let mut iter = match c
                .get_shard_iterator()
                .stream_name(&stream)
                .shard_id(&shard_id)
                .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::Latest)
                .send()
                .await
            {
                Ok(o) => o.shard_iterator().map(String::from),
                Err(_) => None,
            };
            while stop.load(Ordering::Relaxed) == 0 {
                let Some(it) = iter.clone() else { break };
                match c.get_records().shard_iterator(it).limit(10_000).send().await {
                    Ok(out) => {
                        let now = now_ms();
                        for rec in out.records() {
                            let d = rec.data().as_ref();
                            if d.len() >= 8 {
                                let ts = u64::from_be_bytes(d[..8].try_into().unwrap());
                                let _ = stats
                                    .tail_win
                                    .lock()
                                    .unwrap()
                                    .record((now.saturating_sub(ts) * 1000).max(1));
                            }
                        }
                        iter = out.next_shard_iterator().map(String::from);
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
                // 5 GetRecords/s per-shard budget: poll at 4/s.
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        Client::Sqs(c, queue_url) => {
            while stop.load(Ordering::Relaxed) == 0 {
                match c
                    .receive_message()
                    .queue_url(&queue_url)
                    .max_number_of_messages(10)
                    .wait_time_seconds(20)
                    .send()
                    .await
                {
                    Ok(out) => {
                        let now = now_ms();
                        let msgs = out.messages();
                        let mut del = Vec::new();
                        for (i, m) in msgs.iter().enumerate() {
                            if let Some(body) = m.body() {
                                if body.len() >= 16 {
                                    if let Ok(ts) = u64::from_str_radix(&body[..16], 16) {
                                        let _ = stats
                                            .tail_win
                                            .lock()
                                            .unwrap()
                                            .record((now.saturating_sub(ts) * 1000).max(1));
                                    }
                                }
                            }
                            if let Some(rh) = m.receipt_handle() {
                                del.push(
                                    aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                                        .id(format!("d{i}"))
                                        .receipt_handle(rh)
                                        .build()
                                        .unwrap(),
                                );
                            }
                        }
                        if !del.is_empty() {
                            let _ = c
                                .delete_message_batch()
                                .queue_url(&queue_url)
                                .set_entries(Some(del))
                                .send()
                                .await;
                        }
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
        }
        Client::Prisma(http, base, stream, auth, key) => {
            // Long-poll from `now`, chasing the tail via opaque
            // Stream-Next-Offset tokens. Arrival time is the metric:
            // JSON reads return an ARRAY of record payloads, each carrying
            // {"t": producer_ms}.
            let mut offset: Option<String> = None;
            while stop.load(Ordering::Relaxed) == 0 {
                let url = match &offset {
                    None => format!("{base}/v1/stream/{stream}?offset=now"),
                    Some(tok) => format!(
                        "{base}/v1/stream/{stream}?offset={tok}&live=long-poll&timeout=20s"
                    ),
                };
                match http
                    .get(&url)
                    .header("authorization", format!("Bearer {auth}"))
                    .header("stream-encryption-key", key.clone())
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        let next = r
                            .headers()
                            .get("stream-next-offset")
                            .and_then(|v| v.to_str().ok())
                            .map(String::from);
                        let now = now_ms();
                        if offset.is_some() {
                            if let Ok(body) = r.text().await {
                                if let Ok(vals) =
                                    serde_json::from_str::<Vec<serde_json::Value>>(&body)
                                {
                                    for v in vals {
                                        if let Some(ts) = v.get("t").and_then(|t| t.as_u64()) {
                                            let _ = stats
                                                .tail_win
                                                .lock()
                                                .unwrap()
                                                .record((now.saturating_sub(ts) * 1000).max(1));
                                        }
                                    }
                                }
                            }
                        }
                        if next.is_some() {
                            offset = next;
                        }
                    }
                    _ => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
        }
    }
}

/// Minimal HTTP listener: platform liveness + scrapeable results.
async fn stats_server(stats: Arc<Stats>) {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let Ok(listener) = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await else {
        return;
    };
    eprintln!("awsbench stats on :{port}");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let lines = stats.lines.lock().unwrap().join(",");
        let body = format!("[{lines}]");
        let resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 2048];
            let _ = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf)).await;
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let stats = Stats::new();
    tokio::spawn(stats_server(stats.clone()));

    let client = match args.system.as_str() {
        "kinesis" | "sqs" => {
            // Explicit static credentials: the Compute runtime leaks its own
            // AWS_WEB_IDENTITY_TOKEN_FILE into service env, and the default
            // provider chain panics on it. BENCH_AWS_* are ours alone.
            let id = std::env::var("BENCH_AWS_KEY_ID").context("BENCH_AWS_KEY_ID")?;
            let secret =
                std::env::var("BENCH_AWS_SECRET").context("BENCH_AWS_SECRET")?;
            let creds = aws_sdk_kinesis::config::Credentials::new(
                id, secret, None, None, "static",
            );
            let conf = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .credentials_provider(creds)
                .region(aws_config::Region::new(
                    std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".into()),
                ))
                .retry_config(aws_config::retry::RetryConfig::disabled())
                .load()
                .await;
            if args.system == "kinesis" {
                Client::Kinesis(aws_sdk_kinesis::Client::new(&conf), args.target.clone())
            } else {
                Client::Sqs(aws_sdk_sqs::Client::new(&conf), args.target.clone())
            }
        }
        "prisma" => {
            let http = reqwest::Client::builder()
                .pool_max_idle_per_host(4096)
                // <5 s: Compute kills flows idle past ~5 s (RUNBOOK §3.1).
                // NOTE the AWS SDK clients below still use hyper's default
                // pool idle (~90 s) — acceptable for bench shapes under
                // continuous load, documented as a gap.
                .pool_idle_timeout(Duration::from_secs(4))
                .tcp_nodelay(true)
                .timeout(Duration::from_secs(30))
                .build()?;
            // Create the stream up front (idempotent).
            let _ = http
                .put(format!("{}/v1/stream/{}", args.target, args.stream))
                .header("authorization", format!("Bearer {}", args.auth))
                .header("stream-encryption-key", args.stream_key.clone())
                .header("content-type", "application/json")
                .send()
                .await;
            Client::Prisma(
                http,
                args.target.clone(),
                args.stream.clone(),
                args.auth.clone(),
                args.stream_key.clone(),
            )
        }
        other => anyhow::bail!("unknown system {other}"),
    };

    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out)
        .context("open out")?;

    let consumer_stop = Arc::new(AtomicU64::new(0));
    let consumer = if args.consume {
        let c = client.clone();
        let s = stats.clone();
        let stop = consumer_stop.clone();
        Some(tokio::spawn(async move { run_consumer(c, s, stop).await }))
    } else {
        None
    };

    // Explicit tier ramp: BENCH_TIERS="1,2,4,..." runs each concurrency
    // for BENCH_SECS. The "b" sweep only doubles 2..=128 (7 tiers); a
    // regional comparison wants a chosen ladder of arbitrary length.
    if let Ok(tiers) = std::env::var("BENCH_TIERS") {
        let list: Vec<usize> = tiers
            .split(',')
            .filter_map(|t| t.trim().parse().ok())
            .filter(|c| *c > 0)
            .collect();
        eprintln!("tier ramp: {list:?} x {}s each", args.secs);
        for (i, conc) in list.iter().enumerate() {
            run_load(
                client.clone(),
                stats.clone(),
                *conc,
                args.batch,
                args.record_bytes,
                args.secs,
                &format!("t{:02}-conc{conc}", i + 1),
                &mut out,
            )
            .await?;
        }
        consumer_stop.store(1, Ordering::Relaxed);
        if let Some(c) = consumer {
            let _ = tokio::time::timeout(Duration::from_secs(25), c).await;
        }
        if std::env::var("BENCH_HOLD").as_deref() == Ok("1") {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
        return Ok(());
    }

    match args.shape.as_str() {
        "b" => {
            let mut conc = 2usize;
            while conc <= 128 {
                run_load(
                    client.clone(),
                    stats.clone(),
                    conc,
                    args.batch,
                    args.record_bytes,
                    args.secs,
                    &format!("b-conc{conc}"),
                    &mut out,
                )
                .await?;
                conc *= 2;
            }
        }
        s => {
            run_load(
                client.clone(),
                stats.clone(),
                args.conc,
                args.batch,
                args.record_bytes,
                args.secs,
                s,
                &mut out,
            )
            .await?;
        }
    }

    consumer_stop.store(1, Ordering::Relaxed);
    if let Some(c) = consumer {
        let _ = tokio::time::timeout(Duration::from_secs(25), c).await;
    }
    // Keep serving stats so the platform scrape can read the full result
    // set after the shapes finish (the wrapper keeps the process alive).
    if std::env::var("BENCH_HOLD").as_deref() == Ok("1") {
        eprintln!("BENCH_DONE (holding for scrape)");
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
    eprintln!("BENCH_DONE");
    Ok(())
}
