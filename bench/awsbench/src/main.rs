//! awsbench: single-ordered-unit benchmark harness for the AWS comparison
//! campaign (bench/aws-comparison-plan.md). Implements the same closed-loop
//! shapes and the same JSONL stats surface as the pilot generator, against
//! one Kinesis shard or one SQS FIFO message group.
//!
//! SDK retries are DISABLED: throttles are a first-class measurement
//! (counted, never hidden behind retries), matching the plan's accounting
//! rules — {ack, throttle, error} per response.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;

#[derive(Parser, Debug, Clone)]
#[command(name = "awsbench", about = "Kinesis/SQS single-unit benchmark shapes")]
struct Args {
    /// kinesis | sqs
    #[arg(long)]
    system: String,
    /// a=latency-floor, b=record-ceiling sweep, c=byte-ceiling, e=overload
    /// (shape d, tail freshness, is `--shape d` with --consume)
    #[arg(long)]
    shape: String,
    /// Kinesis stream name or SQS queue URL
    #[arg(long)]
    target: String,
    /// Fixed concurrency (shapes a/c/d/e); shape b sweeps 2..=128 on its own
    #[arg(long, default_value_t = 1)]
    conc: usize,
    /// Records per request (Kinesis PutRecords entries; SQS caps at 10)
    #[arg(long, default_value_t = 1)]
    batch: usize,
    /// Payload bytes per record
    #[arg(long, default_value_t = 200)]
    record_bytes: usize,
    /// Run duration seconds (per sweep step for shape b)
    #[arg(long, default_value_t = 300)]
    secs: u64,
    /// Shape d: also run the consumer side and measure producer->receive
    #[arg(long, default_value_t = false)]
    consume: bool,
    /// JSONL output path
    #[arg(long, default_value = "awsbench.jsonl")]
    out: String,
}

struct Stats {
    ok: AtomicU64,
    throttled: AtomicU64,
    errs: AtomicU64,
    records: AtomicU64,
    window_ok: AtomicU64,
    hist: Mutex<Histogram<u64>>,      // cumulative, µs
    hist_win: Mutex<Histogram<u64>>,  // per-window, µs
    tail_win: Mutex<Histogram<u64>>,  // shape d: producer->receive, µs
    last_err: Mutex<String>,
}

impl Stats {
    fn new() -> Arc<Stats> {
        Arc::new(Stats {
            ok: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            errs: AtomicU64::new(0),
            records: AtomicU64::new(0),
            window_ok: AtomicU64::new(0),
            hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            hist_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            tail_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            last_err: Mutex::new(String::new()),
        })
    }
    fn record_ok(&self, lat_us: u64, records: u64) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        self.window_ok.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
        let _ = self.hist.lock().unwrap().record(lat_us.max(1));
        let _ = self.hist_win.lock().unwrap().record(lat_us.max(1));
    }
    fn record_throttle(&self) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
    }
    fn record_err(&self, msg: String) {
        self.errs.fetch_add(1, Ordering::Relaxed);
        *self.last_err.lock().unwrap() = msg;
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Payload: 8-byte producer timestamp header (for shape d) + filler.
fn payload(record_bytes: usize) -> Vec<u8> {
    let mut p = vec![b'x'; record_bytes.max(8)];
    p[..8].copy_from_slice(&now_ms().to_be_bytes());
    p
}

enum Outcome {
    Ok,
    Throttle,
    Err(String),
}

#[derive(Clone)]
enum Client {
    Kinesis(aws_sdk_kinesis::Client, String),
    Sqs(aws_sdk_sqs::Client, String),
}

impl Client {
    async fn send(&self, batch: usize, record_bytes: usize, seq: u64) -> Outcome {
        match self {
            Client::Kinesis(c, stream) => {
                let entries: Vec<_> = (0..batch)
                    .map(|_| {
                        aws_sdk_kinesis::types::PutRecordsRequestEntry::builder()
                            .data(aws_sdk_kinesis::primitives::Blob::new(payload(record_bytes)))
                            // one shard: constant partition key = one ordered unit
                            .partition_key("slate-cmp")
                            .build()
                            .unwrap()
                    })
                    .collect();
                match c
                    .put_records()
                    .stream_name(stream)
                    .set_records(Some(entries))
                    .send()
                    .await
                {
                    Ok(out) => {
                        // Per-record throttles surface as FailedRecordCount
                        // with per-entry error codes, NOT as a top-level
                        // error. Count a request with any failed record as
                        // one throttle (the plan's accounting rule).
                        if out.failed_record_count().unwrap_or(0) > 0 {
                            Outcome::Throttle
                        } else {
                            Outcome::Ok
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("ProvisionedThroughputExceeded")
                            || msg.contains("Throttl")
                            || msg.contains("LimitExceeded")
                        {
                            Outcome::Throttle
                        } else {
                            Outcome::Err(msg)
                        }
                    }
                }
            }
            Client::Sqs(c, queue_url) => {
                // SQS caps batches at 10 entries / 256 KB total.
                let n = batch.min(10);
                let entries: Vec<_> = (0..n)
                    .map(|i| {
                        // Binary-safe: SQS bodies are strings; hex the
                        // timestamp header, pad with 'x' to size.
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
                match c
                    .send_message_batch()
                    .queue_url(queue_url)
                    .set_entries(Some(entries))
                    .send()
                    .await
                {
                    Ok(out) => {
                        let failed = out.failed();
                        if !failed.is_empty() {
                            let throttle = failed.iter().any(|f| {
                                f.code().contains("Throttl") || f.code().contains("RequestThrottled")
                            });
                            if throttle {
                                Outcome::Throttle
                            } else {
                                Outcome::Err(format!("batch failures: {}", failed[0].code()))
                            }
                        } else {
                            Outcome::Ok
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("Throttl") || msg.contains("RequestThrottled") {
                            Outcome::Throttle
                        } else {
                            Outcome::Err(msg)
                        }
                    }
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
                    Outcome::Ok => {
                        stats.record_ok(t0.elapsed().as_micros() as u64, batch as u64)
                    }
                    Outcome::Throttle => stats.record_throttle(),
                    Outcome::Err(m) => stats.record_err(m),
                }
            }
        }));
    }
    let t_end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < t_end {
        tokio::time::sleep(Duration::from_secs(20).min(t_end - Instant::now())).await;
        let win_ok = stats.window_ok.swap(0, Ordering::Relaxed);
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
            "recordsPerSec": win_ok * batch as u64 / 20,
            "winP50Ms": p50,
            "winP99Ms": p99,
            "meanMs": mean,
            "ok": stats.ok.load(Ordering::Relaxed),
            "errs": stats.errs.load(Ordering::Relaxed),
            "throttled": stats.throttled.load(Ordering::Relaxed),
            "tailP50Ms": tail.map(|t| t.0),
            "tailP99Ms": tail.map(|t| t.1),
            "lastErr": stats.last_err.lock().unwrap().clone(),
        });
        writeln!(out, "{line}")?;
        out.flush()?;
        eprintln!("{line}");
    }
    stop.store(1, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    Ok(())
}

/// Shape D consumer: read the unit, extract the producer timestamp header,
/// record producer->receive latency into tail_win.
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
                                let lat_us = now.saturating_sub(ts) * 1000;
                                let _ =
                                    stats.tail_win.lock().unwrap().record(lat_us.max(1));
                            }
                        }
                        iter = out.next_shard_iterator().map(String::from);
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
                // 5 GetRecords/s per shard budget: poll at 4/s.
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
                                        let lat_us = now.saturating_sub(ts) * 1000;
                                        let _ = stats
                                            .tail_win
                                            .lock()
                                            .unwrap()
                                            .record(lat_us.max(1));
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
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let conf = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .retry_config(aws_config::retry::RetryConfig::disabled())
        .load()
        .await;
    let client = match args.system.as_str() {
        "kinesis" => Client::Kinesis(aws_sdk_kinesis::Client::new(&conf), args.target.clone()),
        "sqs" => Client::Sqs(aws_sdk_sqs::Client::new(&conf), args.target.clone()),
        other => anyhow::bail!("unknown system {other}"),
    };
    let stats = Stats::new();
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

    match args.shape.as_str() {
        "b" => {
            // Record-ceiling sweep: conc doubles every args.secs.
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
    Ok(())
}
