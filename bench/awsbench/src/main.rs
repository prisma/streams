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
    /// Prisma stream name (system=prisma). With --streams-n > 1 this
    /// is the PREFIX: streams are "<stream>-0" .. "<stream>-{n-1}".
    #[arg(long, env = "BENCH_STREAM", default_value = "cmp-1")]
    stream: String,
    /// R26-9: number of streams to spray (prisma only). One stream
    /// exercises one segment of one shard; a multi-shard campaign needs
    /// enough streams that route hashes cover every physical shard.
    /// Records go to stream op %% n, so the reconciler can verify the
    /// union without a per-op stream map.
    #[arg(long, env = "BENCH_STREAMS_N", default_value_t = 1)]
    streams_n: usize,
    // R27-3: BENCH_INCOMPRESSIBLE ("1"/"true") switches record padding
    // to base64 of a splitmix64 stream keyed by (op, batch position) —
    // reproducible byte-for-byte, no structure for frame compression to
    // exploit. Read directly from env in send() (clap's bool parser
    // rejects "1", which killed the first local validation instantly).
    /// R26-9: hold BEFORE the first tier until the campaign POSTs
    /// /start on the stats port. All regions deploy sequentially over
    /// minutes; a synchronized release gives every generator a common
    /// t0 so per-region windows are comparable and the recovery window
    /// after the ramp is a controlled, shared measurement.
    #[arg(long, env = "BENCH_START_GATED", default_value_t = false)]
    start_gated: bool,
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
    /// Exactly-once ledger for the tail chaser: records decoded, and
    /// bodies that failed mid-download/decode (each forcing a retry from
    /// the committed cursor).
    records_decoded: AtomicU64,
    body_failures: AtomicU64,
    /// Server-reported live-read stages (STREAMS_DEBUG_TIMING): arm->wake
    /// (waited responses only) and wake->records-built, in µs.
    dbg_wake_win: Mutex<Histogram<u64>>,
    dbg_read_win: Mutex<Histogram<u64>>,
    /// producer -> record DECODED at the consumer (the honest roundtrip;
    /// tail_win keeps the historical producer -> response-headers metric
    /// for comparability with earlier soaks, which understates by body
    /// download + parse time).
    tail_dec_win: Mutex<Histogram<u64>>,
    /// headers -> next long-poll issued (the rearm gap; ~0 when the
    /// pipelined consumer is doing its job).
    rearm_win: Mutex<Histogram<u64>>,
    last_err: Mutex<String>,
    lines: Mutex<Vec<String>>,
    /// R26-7: cumulative throttles split by the server's typed error
    /// code and by HTTP status. One merged "throttled" number cannot
    /// distinguish the ordinary per-stream limiter (429
    /// limit_records_per_sec) from maintenance shedding (503
    /// maintenance_backpressure) — the 2026-08-11 soak plateau was
    /// misattributed for exactly this reason.
    throttled_by_code: Mutex<std::collections::HashMap<String, u64>>,
    throttled_by_status: Mutex<std::collections::HashMap<String, u64>>,
    /// R26-8 exact op ledger. Every request carries its sequence number
    /// in every record ({"op": seq, "b": batch_pos}); the outcome files
    /// the op under exactly one disposition. `/ledger` serves the three
    /// sets as compressed [start, end] ranges so the reconciler can
    /// verify EXACT integrity: every acked op present exactly once with
    /// all batch positions, rejected ops absent, ambiguous 0-or-1.
    acked_ops: Mutex<Vec<u64>>,
    rejected_ops: Mutex<Vec<u64>>,
    ambiguous_ops: Mutex<Vec<u64>>,
    /// Process-global op sequence: op ids must be unique across TIERS,
    /// not just within one run_load call, or the exactly-once check
    /// sees legitimate cross-tier duplicates.
    op_seq: AtomicU64,
    /// R26-9: set by POST /start on the stats port; a gated run parks
    /// until it flips.
    released: AtomicU64,
    /// R27-5: remote end-of-load — /stop finishes the current shape
    /// cleanly (workers join, final cumulative line emitted, ledger
    /// preserved) without waiting out BENCH_SECS.
    stop_requested: AtomicU64,
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
            records_decoded: AtomicU64::new(0),
            body_failures: AtomicU64::new(0),
            dbg_wake_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            dbg_read_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            tail_dec_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            rearm_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
            last_err: Mutex::new(String::new()),
            lines: Mutex::new(Vec::new()),
            throttled_by_code: Mutex::new(std::collections::HashMap::new()),
            throttled_by_status: Mutex::new(std::collections::HashMap::new()),
            acked_ops: Mutex::new(Vec::new()),
            rejected_ops: Mutex::new(Vec::new()),
            ambiguous_ops: Mutex::new(Vec::new()),
            op_seq: AtomicU64::new(0),
            released: AtomicU64::new(0),
            stop_requested: AtomicU64::new(0),
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
    fn record_throttle(&self, delivered: u64, status: u16, code: &str) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(delivered, Ordering::Relaxed);
        self.window_records.fetch_add(delivered, Ordering::Relaxed);
        *self
            .throttled_by_code
            .lock()
            .unwrap()
            .entry(if code.is_empty() { "unknown".into() } else { code.to_string() })
            .or_insert(0) += 1;
        *self
            .throttled_by_status
            .lock()
            .unwrap()
            .entry(status.to_string())
            .or_insert(0) += 1;
    }
    fn record_err(&self, msg: String) {
        self.errs.fetch_add(1, Ordering::Relaxed);
        *self.last_err.lock().unwrap() = msg;
    }
}

/// R27-3: the Prisma target, swappable at runtime via POST
/// /retarget?url=... on the stats port. A single-instance restart leg
/// replaces the platform VERSION, and version-scoped preview domains
/// die with their version — the R26 run's post-restart offered load
/// collapsed to ~25 req/s against the retired domain. Swapping the
/// target keeps the op ledger intact (same process) while restoring
/// full offered load.
fn target() -> &'static std::sync::RwLock<String> {
    static T: std::sync::OnceLock<std::sync::RwLock<String>> = std::sync::OnceLock::new();
    T.get_or_init(|| std::sync::RwLock::new(String::new()))
}

/// Deterministic incompressible pad: splitmix64 keyed by (op, b),
/// base64-encoded to stay JSON-safe. ~6 bits/char of entropy defeats
/// zstd (measured locally; the exact achieved frame ratio is read off
/// the server's exact frame-byte counters during the campaign).
fn incompressible_pad(op: u64, b: usize, len: usize) -> String {
    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut state = op
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((b as u64).wrapping_mul(0xBF58476D1CE4E5B9))
        .wrapping_add(0x94D049BB133111EB);
    let mut next = move || {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    };
    let mut out = String::with_capacity(len);
    while out.len() < len {
        let mut v = next();
        for _ in 0..10 {
            out.push(B64[(v & 63) as usize] as char);
            v >>= 6;
            if out.len() == len {
                break;
            }
        }
    }
    out
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
    Throttle { delivered: u64, status: u16, code: String },
    /// Definitive refusal or failure: a PARSED server response says the
    /// request did not commit.
    Err(String),
    /// Transport failure after the request left (timeout, reset): the
    /// server may have committed 0 or 1 times. The reconciler treats
    /// these ops as allowed-but-not-required to appear.
    Ambiguous(String),
}

#[derive(Clone)]
enum Client {
    Kinesis(aws_sdk_kinesis::Client, String),
    Sqs(aws_sdk_sqs::Client, String),
    /// (http, base_url, stream_names, auth, key). Requests spray
    /// streams by op id (op %% n); stream 0 is the consumer's target.
    Prisma(reqwest::Client, String, Arc<Vec<String>>, String, String),
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
                            Outcome::Throttle {
                                delivered: batch as u64 - failed,
                                status: 0,
                                code: "kinesis_partial_throughput".into(),
                            }
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
                            Outcome::Throttle { delivered: 0, status: 0, code }
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
                                Outcome::Throttle {
                                    delivered,
                                    status: 0,
                                    code: failed[0].code().to_string(),
                                }
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
                            Outcome::Throttle { delivered: 0, status: 0, code }
                        } else {
                            Outcome::Err(format!("{code}: {e}"))
                        }
                    }
                }
            }
            Client::Prisma(http, _base, streams, auth, key) => {
                let stream = &streams[(seq % streams.len() as u64) as usize];
                let incompressible = std::env::var("BENCH_INCOMPRESSIBLE")
                    .map(|v| v == "1" || v == "true")
                    .unwrap_or(false);
                let recs: Vec<serde_json::Value> = (0..batch)
                    .map(|b| {
                        let padlen = record_bytes.saturating_sub(40).max(1);
                        serde_json::json!({
                            "t": now_ms(),
                            // R26-8 op identity: request sequence + batch
                            // position, the exact-reconciliation key.
                            "op": seq,
                            "b": b,
                            "pad": if incompressible {
                                incompressible_pad(seq, b, padlen)
                            } else {
                                "x".repeat(padlen)
                            },
                        })
                    })
                    .collect();
                let base = target().read().unwrap().clone();
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
                        let status = r.status().as_u16();
                        // R27-3: a refusal is DEFINITIVE only when a
                        // Streams process said it (prisma-streams-origin
                        // stamped on every server response). A 5xx/429/
                        // 408 without the origin header is the platform
                        // edge talking — the request may or may not have
                        // reached a Streams process, so the op is
                        // AMBIGUOUS (allowed 0-or-1 in the exact
                        // reconciliation), never assumed rejected. The
                        // R26 restart leg produced 484k origin-less 503s
                        // that happened to be true rejections; the
                        // harness must not assume that in advance.
                        let from_streams = r.headers().contains_key("prisma-streams-origin");
                        if (200..300).contains(&status) {
                            Outcome::Ok { records: batch as u64 }
                        } else if (status == 429 || status == 503) && from_streams {
                            // The server names WHICH limiter refused
                            // (error.code): limit_records_per_sec vs
                            // maintenance_backpressure vs overloaded...
                            let code = r
                                .text()
                                .await
                                .ok()
                                .and_then(|b| serde_json::from_str::<serde_json::Value>(&b).ok())
                                .and_then(|v| {
                                    v.pointer("/error/code")
                                        .and_then(|c| c.as_str())
                                        .map(str::to_string)
                                })
                                .unwrap_or_default();
                            Outcome::Throttle { delivered: 0, status, code }
                        } else if !from_streams && (status >= 500 || status == 429 || status == 408)
                        {
                            Outcome::Ambiguous(format!("infrastructure status {status}"))
                        } else {
                            Outcome::Err(format!("status {status}"))
                        }
                    }
                    Err(e) => Outcome::Ambiguous(e.to_string()),
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
    let mut workers = Vec::new();
    for _ in 0..conc {
        let client = client.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        workers.push(tokio::spawn(async move {
            while stop.load(Ordering::Relaxed) == 0 {
                let s = stats.op_seq.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                match client.send(batch, record_bytes, s).await {
                    Outcome::Ok { records } => {
                        stats.record_ok(t0.elapsed().as_micros() as u64, records);
                        stats.acked_ops.lock().unwrap().push(s);
                    }
                    Outcome::Throttle { delivered, status, code } => {
                        stats.record_throttle(delivered, status, &code);
                        stats.rejected_ops.lock().unwrap().push(s);
                    }
                    Outcome::Err(m) => {
                        stats.record_err(m);
                        stats.rejected_ops.lock().unwrap().push(s);
                    }
                    Outcome::Ambiguous(m) => {
                        stats.record_err(m);
                        stats.ambiguous_ops.lock().unwrap().push(s);
                    }
                }
            }
        }));
    }
    let t_end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < t_end && stats.stop_requested.load(Ordering::Relaxed) == 0 {
        let left = t_end - Instant::now();
        // 2 s ticks so a remote /stop lands promptly; report every 20 s.
        let mut slept = Duration::ZERO;
        let report_gap = Duration::from_secs(20).min(left);
        while slept < report_gap && stats.stop_requested.load(Ordering::Relaxed) == 0 {
            let step = Duration::from_secs(2).min(report_gap - slept);
            tokio::time::sleep(step).await;
            slept += step;
        }
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
        let tail_dec = {
            let mut tw = stats.tail_dec_win.lock().unwrap();
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
        let rearm = {
            let mut tw = stats.rearm_win.lock().unwrap();
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
        let dbg_wake_p50 = {
            let mut w = stats.dbg_wake_win.lock().unwrap();
            let v = if w.is_empty() { None } else { Some(w.value_at_quantile(0.5)) };
            w.reset();
            v
        };
        let dbg_read_p50 = {
            let mut w = stats.dbg_read_win.lock().unwrap();
            let v = if w.is_empty() { None } else { Some(w.value_at_quantile(0.5)) };
            w.reset();
            v
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
            "throttledByCode": &*stats.throttled_by_code.lock().unwrap(),
            "throttledByStatus": &*stats.throttled_by_status.lock().unwrap(),
            "ambiguous": stats.ambiguous_ops.lock().unwrap().len(),
            "tailP50Ms": tail.map(|t| t.0),
            "tailP99Ms": tail.map(|t| t.1),
            "tailDecP50Ms": tail_dec.map(|t| t.0),
            "tailDecP99Ms": tail_dec.map(|t| t.1),
            "rearmP50Ms": rearm.map(|t| t.0),
            "rearmP99Ms": rearm.map(|t| t.1),
            "recordsDecoded": stats.records_decoded.load(Ordering::Relaxed),
            "bodyFailures": stats.body_failures.load(Ordering::Relaxed),
            "dbgWakeP50Us": dbg_wake_p50,
            "dbgReadP50Us": dbg_read_p50,
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
    // R26-8: one FINAL record with post-join cumulative counters. The
    // periodic lines are snapshots taken while workers were mid-flight;
    // requests completing after the last window were invisible (exactly
    // one per worker in the 2026-08-11 soak: the +640 excess). The
    // reconciler reads the last line, which is now this one; rate and
    // latency fields are deliberately null so harvest medians skip it.
    let fin = serde_json::json!({
        "ts": now_ms() / 1000,
        "label": label,
        "conc": conc,
        "batch": batch,
        "recordBytes": record_bytes,
        "final": true,
        "ok": stats.ok.load(Ordering::Relaxed),
        "errs": stats.errs.load(Ordering::Relaxed),
        "throttled": stats.throttled.load(Ordering::Relaxed),
        "throttledByCode": &*stats.throttled_by_code.lock().unwrap(),
        "throttledByStatus": &*stats.throttled_by_status.lock().unwrap(),
        "ambiguous": stats.ambiguous_ops.lock().unwrap().len(),
        "binSha256": std::env::var("APP_BINARY_SHA256").unwrap_or_default(),
        "recordsDecoded": stats.records_decoded.load(Ordering::Relaxed),
        "bodyFailures": stats.body_failures.load(Ordering::Relaxed),
        "lastErr": stats.last_err.lock().unwrap().clone(),
    })
    .to_string();
    writeln!(out, "{fin}")?;
    out.flush()?;
    eprintln!("{fin}");
    stats.lines.lock().unwrap().push(fin);
    Ok(())
}

/// Sorted inclusive [start, end] ranges from raw op ids (R26-8): the
/// ledger stays a few KB even at millions of ops because worker
/// sequences are dense.
fn compress_ranges(ids: &mut Vec<u64>) -> Vec<(u64, u64)> {
    ids.sort_unstable();
    ids.dedup();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &id in ids.iter() {
        match out.last_mut() {
            Some((_, e)) if *e + 1 == id => *e = id,
            _ => out.push((id, id)),
        }
    }
    out
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
        Client::Prisma(http, _base, streams, auth, key) => {
            // The chaser follows stream 0; with streams-n > 1 the
            // roundtrip metric samples 1/n of the traffic — recorded in
            // the campaign report, not silently. Reads the LIVE target
            // so a /retarget mid-run moves the consumer with the
            // producers.
            let base = target().read().unwrap().clone();
            prisma_tail_loop(&http, &base, &streams[0], &auth, &key, &stats, &stop).await;
        }
    }
}

/// Long-poll tail chaser with SPLIT cursors (2026-07-27 review #2):
///
/// `committed` — the offset token whose preceding response body has been
/// fully read AND decoded; the only safe restart point. One speculative
/// request may run ahead using the last response's Stream-Next-Offset,
/// and the previous body's decode is overlapped into that request's RTT.
/// If a body fails mid-stream (reset, truncation, bad JSON), the
/// speculative response is drained and DISCARDED and the loop re-polls
/// from `committed` — accepting it would silently skip the failed
/// response's records, an integrity bug no HTTP status ever surfaces.
async fn prisma_tail_loop(
    http: &reqwest::Client,
    base: &str,
    stream: &str,
    auth: &str,
    key: &str,
    stats: &Arc<Stats>,
    stop: &AtomicU64,
) {
    #[derive(serde::Deserialize)]
    struct TailRec {
        t: Option<u64>,
    }
    async fn decode(resp: reqwest::Response, hdr_ms: u64, stats: &Arc<Stats>) -> Result<(), ()> {
        let body = resp.bytes().await.map_err(|_| ())?;
        let vals = serde_json::from_slice::<Vec<TailRec>>(&body).map_err(|_| ())?;
        let dec_ms = now_ms();
        let mut tw = stats.tail_win.lock().unwrap();
        let mut dw = stats.tail_dec_win.lock().unwrap();
        for v in vals {
            if let Some(ts) = v.t {
                stats.records_decoded.fetch_add(1, Ordering::Relaxed);
                let _ = tw.record((hdr_ms.saturating_sub(ts) * 1000).max(1));
                let _ = dw.record((dec_ms.saturating_sub(ts) * 1000).max(1));
            }
        }
        Ok(())
    }

    let mut committed: Option<String> = None;
    let mut inflight: Option<(reqwest::Response, u64, Option<String>)> = None;
    while stop.load(Ordering::Relaxed) == 0 {
        let from = inflight
            .as_ref()
            .and_then(|(_, _, next)| next.clone())
            .or_else(|| committed.clone());
        let url = match &from {
            None => format!("{base}/v1/stream/{stream}?offset=now"),
            Some(tok) => {
                format!("{base}/v1/stream/{stream}?offset={tok}&live=long-poll&timeout=20s")
            }
        };
        let fut = http
            .get(&url)
            .header("authorization", format!("Bearer {auth}"))
            .header("stream-encryption-key", key.to_string())
            .send();
        tokio::pin!(fut);

        // Overlap the previous body's decode into this request's RTT.
        if let Some((prev, prev_hdr_ms, prev_next)) = inflight.take() {
            match decode(prev, prev_hdr_ms, stats).await {
                Ok(()) => {
                    if prev_next.is_some() {
                        committed = prev_next;
                    }
                }
                Err(()) => {
                    stats.body_failures.fetch_add(1, Ordering::Relaxed);
                    // The speculative request ran past a failed body:
                    // drain and discard it, retry from committed.
                    if let Ok(r) = fut.await {
                        let _ = r.bytes().await;
                    }
                    continue;
                }
            }
        }

        match fut.await {
            Ok(r) if r.status().as_u16() == 204 => {
                let next = r
                    .headers()
                    .get("stream-next-offset")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                if next.is_some() {
                    committed = next;
                }
            }
            Ok(r) if r.status().is_success() => {
                let hdr_ms = now_ms();
                if let Some(dbg) = r
                    .headers()
                    .get("streams-debug-wait")
                    .and_then(|v| v.to_str().ok())
                {
                    // "waited=1 arm_us=N read_us=M" (STREAMS_DEBUG_TIMING).
                    let mut arm = None;
                    let mut read = None;
                    let mut waited = false;
                    for part in dbg.split_whitespace() {
                        if let Some(v) = part.strip_prefix("arm_us=") {
                            arm = v.parse::<u64>().ok();
                        } else if let Some(v) = part.strip_prefix("read_us=") {
                            read = v.parse::<u64>().ok();
                        } else if part == "waited=1" {
                            waited = true;
                        }
                    }
                    if waited {
                        if let Some(a) = arm {
                            let _ = stats.dbg_wake_win.lock().unwrap().record(a.max(1));
                        }
                    }
                    if let Some(rd) = read {
                        let _ = stats.dbg_read_win.lock().unwrap().record(rd.max(1));
                    }
                }
                let next = r
                    .headers()
                    .get("stream-next-offset")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                if from.is_none() {
                    // offset=now bootstrap: no records in this response.
                    committed = next;
                    continue;
                }
                let _ = stats
                    .rearm_win
                    .lock()
                    .unwrap()
                    .record(((now_ms().saturating_sub(hdr_ms)) * 1000).max(1));
                inflight = Some((r, hdr_ms, next));
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    if let Some((prev, prev_hdr_ms, _)) = inflight.take() {
        let _ = decode(prev, prev_hdr_ms, stats).await;
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
        let stats = stats.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 2048];
            let n = match tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                _ => 0,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .to_string();
            let body = if path.starts_with("/start") {
                stats.released.store(1, Ordering::Relaxed);
                "{\"started\":true}".to_string()
            } else if path.starts_with("/stop") {
                stats.stop_requested.store(1, Ordering::Relaxed);
                "{\"stopping\":true}".to_string()
            } else if path.starts_with("/retarget") {
                // R27-3: swap the Prisma base URL in place — the restart
                // leg's replacement version gets full offered load and
                // the op ledger survives (same process).
                match path.split_once("url=").map(|(_, u)| u.to_string()) {
                    Some(u) if u.starts_with("http") => {
                        let decoded = u.replace("%3A", ":").replace("%2F", "/");
                        *target().write().unwrap() = decoded.clone();
                        eprintln!("RETARGET -> {decoded}");
                        format!("{{\"target\":\"{decoded}\"}}")
                    }
                    _ => "{\"error\":\"url= required\"}".to_string(),
                }
            } else if path.starts_with("/ledger") {
                // R26-8: the exact op ledger, as compressed ranges.
                let acked = compress_ranges(&mut stats.acked_ops.lock().unwrap());
                let rejected = compress_ranges(&mut stats.rejected_ops.lock().unwrap());
                let ambiguous = compress_ranges(&mut stats.ambiguous_ops.lock().unwrap());
                serde_json::json!({
                    "acked": acked,
                    "rejected": rejected,
                    "ambiguous": ambiguous,
                })
                .to_string()
            } else {
                format!("[{}]", stats.lines.lock().unwrap().join(","))
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.shape == "cert" {
        // Workload-cert shape (bench/WORKLOAD-CERT-PLAN.md): rotating
        // writers + parked SSE subscriber swarm + fan-out overlap,
        // with an EXACT offered-ops denominator for shed accounting.
        let stats = Stats::new();
        tokio::spawn(stats_server(stats.clone()));
        return run_cert(&args, stats).await;
    }
    if args.shape == "wide" {
        // MT campaigns deploy wide: the stats server must serve the
        // JSONL (and platform liveness) exactly like the tiered shapes.
        let stats = Stats::new();
        tokio::spawn(stats_server(stats.clone()));
        return run_wide(&args, stats).await;
    }
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
            // Create the stream up front, and REFUSE to proceed until it
            // exists. The original fire-and-forget create cost a soak
            // region: one platform-edge blip during a version migration
            // failed the create, and every subsequent append 404'd for
            // the whole run while the generator looked merely unlucky
            // (soak2 run 1, ap-southeast-1, 2026-07-27).
            let names: Vec<String> = if args.streams_n <= 1 {
                vec![args.stream.clone()]
            } else {
                (0..args.streams_n)
                    .map(|i| format!("{}-{i}", args.stream))
                    .collect()
            };
            for name in &names {
                let mut created = false;
                for attempt in 0..30u32 {
                    match http
                        .put(format!("{}/v1/stream/{}", args.target, name))
                        .header("authorization", format!("Bearer {}", args.auth))
                        .header("stream-encryption-key", args.stream_key.clone())
                        .header("content-type", "application/json")
                        .send()
                        .await
                    {
                        // 2xx = created; 409 = already exists (a rerun) —
                        // both mean appends will not 404.
                        Ok(r) if r.status().is_success() || r.status().as_u16() == 409 => {
                            created = true;
                            break;
                        }
                        Ok(r) => eprintln!(
                            "stream create attempt {attempt}: status {}",
                            r.status()
                        ),
                        Err(e) => eprintln!("stream create attempt {attempt}: {e}"),
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                if !created {
                    anyhow::bail!(
                        "stream {name} could not be created after 30 attempts; \
                         refusing to run a benchmark whose every append would 404"
                    );
                }
            }
            *target().write().unwrap() = args.target.clone();
            Client::Prisma(
                http,
                args.target.clone(),
                Arc::new(names),
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

    if args.start_gated {
        eprintln!("BENCH_GATED: waiting for POST /start on the stats port");
        while stats.released.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        eprintln!("BENCH_RELEASED");
    }

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

// ---- wide-cardinality shape (BENCH_SHAPE=wide, prisma only) ----------
//
// Many streams, few active — the cost review's "many lightly used
// streams" regime. Three phases:
//
//   1. create BENCH_WIDE_STREAMS streams;
//   2. seed each with ONE record (so every stream exists in the shard
//      log and eventually crosses the absorber's age threshold — the
//      per-stream history tax is the thing under test);
//   3. a BENCH_WIDE_SECS steady window: the first BENCH_WIDE_ACTIVE
//      streams append batch×record_bytes every
//      BENCH_WIDE_APPEND_INTERVAL_MS, while a scanner cold-reads random
//      INACTIVE streams from offset 0 at BENCH_WIDE_SCAN_RPS (the
//      history-reader cardinality path).
//
// Emits "SETUP_DONE ..." on stderr between phases so the runner can
// split the store ledger into setup vs steady, one JSONL window line
// per 20 s, and "WIDE_DONE" at the end.

struct WideHist(std::sync::Mutex<Vec<u64>>);

impl WideHist {
    fn new() -> Arc<Self> {
        Arc::new(WideHist(std::sync::Mutex::new(Vec::new())))
    }
    fn rec(&self, us: u64) {
        self.0.lock().unwrap().push(us);
    }
    fn drain_sorted(&self) -> Vec<u64> {
        let mut v = std::mem::take(&mut *self.0.lock().unwrap());
        v.sort_unstable();
        v
    }
}

fn pctl_ms(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx] as f64 / 1000.0
}

fn wide_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn wide_batch(batch: usize, record_bytes: usize) -> Vec<serde_json::Value> {
    (0..batch)
        .map(|b| {
            serde_json::json!({
                "t": now_ms(),
                "b": b,
                "pad": "x".repeat(record_bytes.saturating_sub(40).max(1)),
            })
        })
        .collect()
}


// ---- workload-cert shape (BENCH_SHAPE=cert, prisma product only) ----------
// Tenants [0, SUB_TENANTS) are SUBSCRIBER tenants: each owns one hot
// stream "{prefix}s{t}" carrying its parked :sse subscriptions, and
// fan-out writes land there. Tenants [SUB_TENANTS, TENANTS) own plain
// writer streams "{prefix}w{t}". Every window of WINDOW_MS, ACTIVE
// writers are active: FANOUT_ACTIVE drawn round-robin from the
// subscriber range, the rest round-robin from the plain range; each
// active tenant offers WPS/ACTIVE writes/s. `offered` increments at
// SCHEDULE time — the shed denominator counts intent, not sends.
async fn run_cert(args: &Args, stats: Arc<Stats>) -> anyhow::Result<()> {
    use futures_util::StreamExt;
    let tenants: usize = wide_env("BENCH_CERT_TENANTS", 10_000);
    let sub_tenants: usize = wide_env("BENCH_CERT_SUB_TENANTS", 1_000);
    let subs_n: usize = wide_env("BENCH_CERT_SUBS_N", 0);
    let active: usize = wide_env("BENCH_CERT_ACTIVE", 100);
    let fanout_active: usize = wide_env("BENCH_CERT_FANOUT_ACTIVE", 10);
    let window_ms: u64 = wide_env("BENCH_CERT_WINDOW_MS", 5_000);
    let wps: u64 = wide_env("BENCH_CERT_WPS", 1_000);
    let secs: u64 = wide_env("BENCH_CERT_SECS", 300);
    let setup_conc: usize = wide_env("BENCH_WIDE_SETUP_CONC", 64);
    anyhow::ensure!(sub_tenants <= tenants && fanout_active <= active);
    anyhow::ensure!(fanout_active == 0 || sub_tenants > 0);
    let record_bytes = args.record_bytes;

    let tokens_path =
        std::env::var("BENCH_TOKENS_FILE").context("cert shape requires BENCH_TOKENS_FILE")?;
    let doc: Vec<serde_json::Value> = serde_json::from_str(&std::fs::read_to_string(&tokens_path)?)?;
    let tokens: Arc<Vec<String>> = Arc::new(
        doc.iter()
            .map(|e| e["token"].as_str().unwrap_or_default().to_string())
            .collect(),
    );
    anyhow::ensure!(tokens.len() >= tenants, "need >= {tenants} tokens");

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(4096)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()?;
    let base = args.target.clone();
    let key = args.stream_key.clone();
    let prefix = args.stream.clone();
    // L2 post-mortem: the platform edge throttles concurrent TLS
    // handshakes per client. An unpaced subscriber swarm (5k tasks,
    // 500 ms retries) melts into a permanent connect-timeout storm
    // that also starves the writer pool (L2a/L2b: ~96% "apErr" with
    // zero server-side rejections). Pace connection ESTABLISHMENT;
    // requests on already-established conns are unaffected.
    let connect_conc: usize = std::env::var("BENCH_CERT_CONNECT_CONC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(48);
    let connect_sem = Arc::new(tokio::sync::Semaphore::new(connect_conc));
    // Parked SSE conns need fd headroom past a 1024 container default.
    #[cfg(target_os = "linux")]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
        eprintln!("CERT: nofile={}", lim.rlim_cur);
    }
    // Typed error classes: one opaque "err" bucket hid the edge storm
    // for two full runs. Count by class, sample-print the first three.
    let err_connect = Arc::new(AtomicU64::new(0));
    let err_timeout = Arc::new(AtomicU64::new(0));
    let err_status = Arc::new(AtomicU64::new(0));
    let err_other = Arc::new(AtomicU64::new(0));
    let sub_err_connect = Arc::new(AtomicU64::new(0));
    let sub_err_status = Arc::new(AtomicU64::new(0));
    fn bump(class: &'static str, ctr: &AtomicU64, msg: impl std::fmt::Display) {
        if ctr.fetch_add(1, Ordering::Relaxed) < 3 {
            eprintln!("CERT-ERR[{class}]: {msg}");
        }
    }
    let mut out = std::fs::File::create(&args.out)?;
    use std::io::Write as _;
    let stream_of = |t: usize| -> String {
        if t < sub_tenants {
            format!("{prefix}s{t}")
        } else {
            format!("{prefix}w{t}")
        }
    };

    // Phase 1: create every stream the run can touch.
    eprintln!("CERT: creating {tenants} streams (conc {setup_conc})");
    let t_create = Instant::now();
    for chunk in (0..tenants).collect::<Vec<_>>().chunks(10_000) {
        let fails: usize = futures_util::stream::iter(chunk.iter().map(|&t| {
            let http = http.clone();
            let url = format!("{base}/v1/streams/{}", stream_of(t));
            let auth = format!("Bearer {}", tokens[t]);
            let key = key.clone();
            async move {
                for attempt in 0..4u32 {
                    let r = http
                        .put(&url)
                        .header("authorization", auth.clone())
                        .header("prisma-encryption-key", key.clone())
                        .header("content-type", "application/json")
                        .json(&serde_json::json!({"format": {"kind": "json"}}))
                        .send()
                        .await;
                    if matches!(&r, Ok(resp) if resp.status().is_success()) {
                        return 0usize;
                    }
                    tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                }
                1usize
            }
        }))
        .buffer_unordered(setup_conc)
        .fold(0usize, |a, b| async move { a + b })
        .await;
        anyhow::ensure!(fails == 0, "{fails} creates failed");
    }
    let create_ms = t_create.elapsed().as_millis();
    eprintln!("CERT: creates done in {create_ms}ms");

    // Phase 2: park the subscriber swarm (durable-cursor :sse at tail).
    let delivered = Arc::new(AtomicU64::new(0));
    let reconnects = Arc::new(AtomicU64::new(0));
    let subs_open = Arc::new(AtomicU64::new(0));
    let lag_hist = WideHist::new();
    let stop = Arc::new(AtomicU64::new(0));
    for j in 0..subs_n {
        let t = j % sub_tenants;
        let http = http.clone();
        let base = base.clone();
        let name = stream_of(t);
        let auth = format!("Bearer {}", tokens[t]);
        let key = key.clone();
        let (delivered, reconnects, subs_open, lag_hist, stop) = (
            delivered.clone(),
            reconnects.clone(),
            subs_open.clone(),
            lag_hist.clone(),
            stop.clone(),
        );
        let connect_sem = connect_sem.clone();
        let sub_err_connect = sub_err_connect.clone();
        let sub_err_status = sub_err_status.clone();
        tokio::spawn(async move {
            let mut first = true;
            // Jittered backoff: 5k tasks on a fixed 500 ms retry are a
            // synchronized handshake storm against the edge limiter.
            let backoff_ms = 1500 + (j as u64).wrapping_mul(2654435761) % 1500;
            while stop.load(Ordering::Relaxed) == 0 {
                // Hold a connect permit across tail-learn + SSE
                // establishment; release once parked (or on failure).
                let permit = connect_sem.clone().acquire_owned().await.unwrap();
                // (Re)learn the tail, then park.
                let tail = match http
                    .get(format!("{base}/v1/streams/{name}/records"))
                    .header("authorization", auth.clone())
                    .header("prisma-encryption-key", key.clone())
                    .send()
                    .await
                {
                    Ok(r) => r
                        .headers()
                        .get("prisma-next-cursor")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string(),
                    Err(e) => {
                        bump("sub-connect", &sub_err_connect, &e);
                        drop(permit);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                };
                let resp = http
                    .get(format!(
                        "{base}/v1/streams/{name}/records:sse?cursor={}",
                        urlencoding_min(&tail)
                    ))
                    .header("authorization", auth.clone())
                    .header("prisma-encryption-key", key.clone())
                    .send()
                    .await;
                let r = match resp {
                    Ok(r) => r,
                    Err(e) => {
                        bump("sub-connect", &sub_err_connect, &e);
                        drop(permit);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                };
                if r.status().as_u16() != 200 {
                    bump("sub-status", &sub_err_status, r.status());
                    drop(permit);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                drop(permit);
                if first {
                    subs_open.fetch_add(1, Ordering::Relaxed);
                    first = false;
                } else {
                    reconnects.fetch_add(1, Ordering::Relaxed);
                }
                let mut r = r;
                while let Ok(Some(b)) = r.chunk().await {
                    // Delivery lag: writers embed {"t":<unix_ms>}.
                    let text = String::from_utf8_lossy(&b);
                    for m in text.match_indices("\"t\":") {
                        let rest = &text[m.0 + 4..];
                        let end = rest
                            .find(|c: char| !c.is_ascii_digit())
                            .unwrap_or(rest.len());
                        if let Ok(sent_ms) = rest[..end].parse::<u128>() {
                            let lag = (now_ms() as u128).saturating_sub(sent_ms) as u64;
                            lag_hist.rec(lag * 1000); // hist is in micros
                            delivered.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if stop.load(Ordering::Relaxed) != 0 {
                        break;
                    }
                }
            }
        });
        if j % 200 == 199 {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }
    if subs_n > 0 {
        // Give the swarm a moment to park before offering load.
        for _ in 0..100 {
            if subs_open.load(Ordering::Relaxed) as usize >= subs_n * 95 / 100 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        eprintln!("CERT: {} subscriptions parked", subs_open.load(Ordering::Relaxed));
    }

    // Phase 3: rotating writers.
    let offered = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let thr = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let ap_hist = WideHist::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(768));
    let per_tenant_interval =
        Duration::from_millis((1000 * active as u64 / wps.max(1)).max(1));
    let plain = tenants - sub_tenants;
    let deadline = Instant::now() + Duration::from_secs(secs);
    let writer = {
        let (http, base, key, tokens, offered, ok, thr, err, ap_hist, sem, stop) = (
            http.clone(),
            base.clone(),
            key.clone(),
            tokens.clone(),
            offered.clone(),
            ok.clone(),
            thr.clone(),
            err.clone(),
            ap_hist.clone(),
            sem.clone(),
            stop.clone(),
        );
        let (err_connect, err_timeout, err_status, err_other) = (
            err_connect.clone(),
            err_timeout.clone(),
            err_status.clone(),
            err_other.clone(),
        );
        let stream_of = move |t: usize| -> String {
            if t < sub_tenants {
                format!("{prefix}s{t}")
            } else {
                format!("{prefix}w{t}")
            }
        };
        tokio::spawn(async move {
            let mut window: u64 = 0;
            while Instant::now() < deadline && stop.load(Ordering::Relaxed) == 0 {
                // Deterministic active set for this window.
                let mut set = Vec::with_capacity(active);
                for i in 0..fanout_active {
                    set.push(((window as usize * fanout_active) + i) % sub_tenants.max(1));
                }
                for i in 0..active - fanout_active {
                    set.push(
                        sub_tenants
                            + (((window as usize) * (active - fanout_active) + i) % plain.max(1)),
                    );
                }
                let window_end = Instant::now() + Duration::from_millis(window_ms);
                let mut next_fire = Instant::now();
                while Instant::now() < window_end
                    && Instant::now() < deadline
                    && stop.load(Ordering::Relaxed) == 0
                {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(next_fire)).await;
                    next_fire += per_tenant_interval;
                    for &t in &set {
                        offered.fetch_add(1, Ordering::Relaxed);
                        let http = http.clone();
                        let url = format!("{base}/v1/streams/{}/records", stream_of(t));
                        let auth = format!("Bearer {}", tokens[t]);
                        let key = key.clone();
                        let (ok, thr, err, ap_hist) =
                            (ok.clone(), thr.clone(), err.clone(), ap_hist.clone());
                        let (err_connect, err_timeout, err_status, err_other) = (
                            err_connect.clone(),
                            err_timeout.clone(),
                            err_status.clone(),
                            err_other.clone(),
                        );
                        let pad = record_bytes.saturating_sub(40);
                        let permit = sem.clone().acquire_owned().await.unwrap();
                        tokio::spawn(async move {
                            let _p = permit;
                            let body = format!(
                                "{{\"t\":{},\"pad\":\"{}\"}}",
                                now_ms(),
                                "x".repeat(pad)
                            );
                            let t0 = Instant::now();
                            match http
                                .post(&url)
                                .header("authorization", auth)
                                .header("prisma-encryption-key", key)
                                .header("content-type", "application/json")
                                .body(body)
                                .send()
                                .await
                            {
                                Ok(r) if r.status().is_success() => {
                                    ok.fetch_add(1, Ordering::Relaxed);
                                    ap_hist.rec(t0.elapsed().as_micros() as u64);
                                }
                                Ok(r) if r.status().as_u16() == 429 || r.status().as_u16() == 503 => {
                                    thr.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(r) => {
                                    bump("ap-status", &err_status, r.status());
                                    err.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    if e.is_connect() {
                                        bump("ap-connect", &err_connect, &e);
                                    } else if e.is_timeout() {
                                        bump("ap-timeout", &err_timeout, &e);
                                    } else {
                                        bump("ap-other", &err_other, &e);
                                    }
                                    err.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                    }
                }
                window += 1;
            }
        })
    };

    // Reporter until the deadline.
    let started = Instant::now();
    while Instant::now() < deadline {
        let left = deadline - Instant::now();
        tokio::time::sleep(Duration::from_secs(20).min(left)).await;
        let ap = ap_hist.drain_sorted();
        let lags = lag_hist.drain_sorted();
        let offered_v = offered.load(Ordering::Relaxed);
        let thr_v = thr.load(Ordering::Relaxed);
        let line = serde_json::json!({
            "phase": "cert",
            "offered": offered_v,
            "apOk": ok.load(Ordering::Relaxed),
            "apThr": thr_v,
            "apErr": err.load(Ordering::Relaxed),
            "shedPctCum": if offered_v > 0 { 100.0 * thr_v as f64 / offered_v as f64 } else { 0.0 },
            "apWinP50Ms": pctl_ms(&ap, 0.5),
            "apWinP99Ms": pctl_ms(&ap, 0.99),
            "subsOpen": subs_open.load(Ordering::Relaxed),
            "delivered": delivered.load(Ordering::Relaxed),
            "reconnects": reconnects.load(Ordering::Relaxed),
            "errConnect": err_connect.load(Ordering::Relaxed),
            "errTimeout": err_timeout.load(Ordering::Relaxed),
            "errStatus": err_status.load(Ordering::Relaxed),
            "errOther": err_other.load(Ordering::Relaxed),
            "subErrConnect": sub_err_connect.load(Ordering::Relaxed),
            "subErrStatus": sub_err_status.load(Ordering::Relaxed),
            "connectConc": connect_conc,
            "lagWinP50Ms": pctl_ms(&lags, 0.5),
            "lagWinP99Ms": pctl_ms(&lags, 0.99),
            "tenants": tenants, "subTenants": sub_tenants, "subsN": subs_n,
            "active": active, "fanoutActive": fanout_active,
            "windowMs": window_ms, "wps": wps, "recordBytes": record_bytes,
            "elapsedS": started.elapsed().as_secs(),
            "ts": now_ms()/1000,
        });
        eprintln!("{line}");
        writeln!(out, "{line}")?;
        out.flush()?;
        stats.lines.lock().unwrap().push(line.to_string());
    }
    stop.store(1, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
    let offered_v = offered.load(Ordering::Relaxed);
    let thr_v = thr.load(Ordering::Relaxed);
    let done = serde_json::json!({
        "phase": "cert_done",
        "offered": offered_v,
        "apOk": ok.load(Ordering::Relaxed),
        "apThr": thr_v,
        "apErr": err.load(Ordering::Relaxed),
        "shedPct": if offered_v > 0 { 100.0 * thr_v as f64 / offered_v as f64 } else { 0.0 },
        "delivered": delivered.load(Ordering::Relaxed),
        "reconnects": reconnects.load(Ordering::Relaxed),
        "subsOpen": subs_open.load(Ordering::Relaxed),
        "errConnect": err_connect.load(Ordering::Relaxed),
        "errTimeout": err_timeout.load(Ordering::Relaxed),
        "errStatus": err_status.load(Ordering::Relaxed),
        "errOther": err_other.load(Ordering::Relaxed),
        "subErrConnect": sub_err_connect.load(Ordering::Relaxed),
        "subErrStatus": sub_err_status.load(Ordering::Relaxed),
        "connectConc": connect_conc,
        "createMs": create_ms,
        "steadySecs": secs,
        "tenants": tenants, "subTenants": sub_tenants, "subsN": subs_n,
        "active": active, "fanoutActive": fanout_active,
        "windowMs": window_ms, "wps": wps, "recordBytes": record_bytes,
        "binSha256": std::env::var("APP_BINARY_SHA256").unwrap_or_default(),
        "ts": now_ms()/1000,
    });
    eprintln!("{done}");
    writeln!(out, "{done}")?;
    out.flush()?;
    stats.lines.lock().unwrap().push(done.to_string());
    eprintln!("CERT_DONE");
    if std::env::var("BENCH_HOLD").as_deref() == Ok("1") {
        eprintln!("CERT_DONE (holding for scrape)");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
    Ok(())
}

/// Percent-encode the handful of cursor bytes that matter in a query
/// value (base64url cursors only ever need '=' handled, but be safe).
fn urlencoding_min(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
            _ => format!("%{:02X}", c as u32).chars().collect(),
        })
        .collect()
}

async fn run_wide(args: &Args, stats: Arc<Stats>) -> anyhow::Result<()> {
    use futures_util::StreamExt;
    let n: usize = std::env::var("BENCH_WIDE_STREAMS")
        .context("BENCH_WIDE_STREAMS")?
        .parse()?;
    let active: usize = wide_env("BENCH_WIDE_ACTIVE", 100);
    let secs: u64 = wide_env("BENCH_WIDE_SECS", 900);
    let interval_ms: u64 = wide_env("BENCH_WIDE_APPEND_INTERVAL_MS", 500);
    let scan_rps: u64 = wide_env("BENCH_WIDE_SCAN_RPS", 2);
    let setup_conc: usize = wide_env("BENCH_WIDE_SETUP_CONC", 64);
    anyhow::ensure!(active <= n, "BENCH_WIDE_ACTIVE must be <= BENCH_WIDE_STREAMS");
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(4096)
        .pool_idle_timeout(Duration::from_secs(4))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()?;
    let base = args.target.clone();
    let auth = format!("Bearer {}", args.auth);
    let key = args.stream_key.clone();
    let prefix = args.stream.clone();
    let mut out = std::fs::File::create(&args.out)?;
    use std::io::Write as _;

    // ---- MT (multi-tenant, PRODUCT surface) ------------------------------
    // BENCH_MT=1 drives the product surface with per-project customer
    // JWTs: stream i belongs to project i % BENCH_PROJECTS_ACTIVE and
    // every request carries that project's Bearer from
    // BENCH_TOKENS_FILE (JSON [{"project":..,"token":..}, ...]).
    // Everything else — pacing, stream count, stats — is the same
    // experiment, so tenant cardinality is the ONLY moving axis.
    let mt = std::env::var("BENCH_MT").as_deref() == Ok("1");
    let mt_active: usize = if mt {
        std::env::var("BENCH_PROJECTS_ACTIVE")
            .context("BENCH_MT=1 requires BENCH_PROJECTS_ACTIVE")?
            .parse()?
    } else {
        0
    };
    let mt_tokens: Vec<String> = if mt {
        let path = std::env::var("BENCH_TOKENS_FILE").context("BENCH_TOKENS_FILE")?;
        let doc: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(&path)?)?;
        let toks: Vec<String> = doc
            .iter()
            .map(|e| e["token"].as_str().unwrap_or_default().to_string())
            .collect();
        anyhow::ensure!(
            mt_active >= 1 && mt_active <= toks.len(),
            "BENCH_PROJECTS_ACTIVE {mt_active} outside 1..={}",
            toks.len()
        );
        anyhow::ensure!(args.batch == 1, "MT mode appends single records; set BENCH_BATCH=1");
        toks
    } else {
        Vec::new()
    };
    let auth_for = |i: usize| -> String {
        if mt {
            format!("Bearer {}", mt_tokens[i % mt_active])
        } else {
            auth.clone()
        }
    };
    let enc_header: &'static str = if mt { "prisma-encryption-key" } else { "stream-encryption-key" };
    let entry_url = |i: usize| -> String {
        if mt {
            format!("{base}/v1/streams/{prefix}{i}")
        } else {
            format!("{base}/v1/stream/{prefix}{i}")
        }
    };
    let append_url = |i: usize| -> String {
        if mt {
            format!("{base}/v1/streams/{prefix}{i}/records")
        } else {
            format!("{base}/v1/stream/{prefix}{i}")
        }
    };

    eprintln!(
        "WIDE: {n} streams, {active} active, {secs}s steady, append every {interval_ms}ms, scan {scan_rps}/s, mt={mt} projectsActive={mt_active}"
    );

    // Phase 1a: create. Every stream must exist or the regime is void.
    let t_create = Instant::now();
    for chunk_start in (0..n).step_by(10_000) {
        let chunk_end = (chunk_start + 10_000).min(n);
        let fails: usize = futures_util::stream::iter(
            (chunk_start..chunk_end).map(|i| {
                let http = http.clone();
                let url = entry_url(i);
                let auth = auth_for(i);
                let key = key.clone();
                // Product create takes the typed creation document.
                let create_body = mt.then(|| serde_json::json!({"format": {"kind": "json"}}));
                async move {
                    for attempt in 0..4u32 {
                        let mut req = http
                            .put(&url)
                            .header("authorization", auth.clone())
                            .header(enc_header, key.clone())
                            .header("content-type", "application/json");
                        if let Some(b) = &create_body {
                            req = req.json(b);
                        }
                        let r = req.send().await;
                        if matches!(&r, Ok(resp) if resp.status().is_success()) {
                            return 0usize;
                        }
                        tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                    }
                    1usize
                }
            }),
        )
        .buffer_unordered(setup_conc)
        .fold(0usize, |a, b| async move { a + b })
        .await;
        anyhow::ensure!(fails == 0, "{fails} creates failed in chunk at {chunk_start}");
        eprintln!("WIDE: created {chunk_end}/{n}");
    }
    let create_ms = t_create.elapsed().as_millis();

    // Phase 1b: seed one record per stream.
    let t_seed = Instant::now();
    let record_bytes = args.record_bytes;
    for chunk_start in (0..n).step_by(10_000) {
        let chunk_end = (chunk_start + 10_000).min(n);
        let fails: usize = futures_util::stream::iter(
            (chunk_start..chunk_end).map(|i| {
                let http = http.clone();
                let url = append_url(i);
                let auth = auth_for(i);
                let key = key.clone();
                async move {
                    let body = if mt {
                        wide_batch(1, record_bytes).pop().unwrap_or_default()
                    } else {
                        serde_json::Value::Array(wide_batch(1, record_bytes))
                    };
                    for attempt in 0..4u32 {
                        let r = http
                            .post(&url)
                            .header("authorization", auth.clone())
                            .header(enc_header, key.clone())
                            .header("content-type", "application/json")
                            .json(&body)
                            .send()
                            .await;
                        if matches!(&r, Ok(resp) if resp.status().is_success()) {
                            return 0usize;
                        }
                        tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                    }
                    1usize
                }
            }),
        )
        .buffer_unordered(setup_conc)
        .fold(0usize, |a, b| async move { a + b })
        .await;
        anyhow::ensure!(fails == 0, "{fails} seeds failed in chunk at {chunk_start}");
        eprintln!("WIDE: seeded {chunk_end}/{n}");
    }
    let seed_ms = t_seed.elapsed().as_millis();
    eprintln!("SETUP_DONE streams={n} create_ms={create_ms} seed_ms={seed_ms}");
    let setup_line = serde_json::json!({
        "phase": "setup", "streams": n, "active": active,
        "createMs": create_ms, "seedMs": seed_ms,
        "projectsActive": mt_active,
        "surface": if mt { "product" } else { "raw" },
        "recordBytes": record_bytes, "intervalMs": interval_ms,
        "ts": now_ms()/1000,
    });
    writeln!(out, "{setup_line}")?;
    out.flush()?;
    stats.lines.lock().unwrap().push(setup_line.to_string());

    // Phase 2: steady window.
    let stop = Arc::new(AtomicU64::new(0));
    let ap_hist = WideHist::new();
    let sc_hist = WideHist::new();
    let ap_ok = Arc::new(AtomicU64::new(0));
    let ap_thr = Arc::new(AtomicU64::new(0));
    let ap_err = Arc::new(AtomicU64::new(0));
    let sc_ok = Arc::new(AtomicU64::new(0));
    let sc_err = Arc::new(AtomicU64::new(0));
    let sc_records = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::new();
    for j in 0..active {
        let http = http.clone();
        let url = append_url(j);
        let auth = auth_for(j);
        let key = key.clone();
        let stop = stop.clone();
        let (hist, ok, thr, err) =
            (ap_hist.clone(), ap_ok.clone(), ap_thr.clone(), ap_err.clone());
        let batch = args.batch;
        tasks.push(tokio::spawn(async move {
            // Stagger starts so the herd doesn't align on one instant.
            let interval = Duration::from_millis(interval_ms);
            let mut next = Instant::now() + interval * j as u32 / active.max(1) as u32;
            while stop.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep_until(tokio::time::Instant::from_std(next)).await;
                next += interval;
                let body = if mt {
                    wide_batch(1, record_bytes).pop().unwrap_or_default()
                } else {
                    serde_json::Value::Array(wide_batch(batch, record_bytes))
                };
                let t0 = Instant::now();
                match http
                    .post(&url)
                    .header("authorization", auth.clone())
                    .header(enc_header, key.clone())
                    .header("content-type", "application/json")
                    .json(&body)
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        hist.rec(t0.elapsed().as_micros() as u64);
                    }
                    Ok(r) if r.status().as_u16() == 429 || r.status().as_u16() == 503 => {
                        thr.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    // Scanner: cold catch-up reads of random INACTIVE streams. One task,
    // paced; a splitmix64 keeps it dependency-free.
    if n > active && scan_rps > 0 {
        let http = http.clone();
        let base = base.clone();
        let auth = auth.clone();
        let key = key.clone();
        let prefix = prefix.clone();
        let stop = stop.clone();
        let (hist, ok, err, recs) =
            (sc_hist.clone(), sc_ok.clone(), sc_err.clone(), sc_records.clone());
        tasks.push(tokio::spawn(async move {
            let mut seed = now_ms() as u64 | 1;
            let mut rng = move || {
                seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = seed;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^ (z >> 31)
            };
            let gap = Duration::from_millis(1000 / scan_rps.max(1));
            while stop.load(Ordering::Relaxed) == 0 {
                // No offset param = read from the beginning (a canonical
                // offset token is not a bare integer).
                let idx = active + (rng() as usize % (n - active));
                let url = format!("{base}/v1/stream/{prefix}{idx}");
                let t0 = Instant::now();
                match http
                    .get(&url)
                    .header("authorization", auth.clone())
                    .header("stream-encryption-key", key.clone())
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => match r.bytes().await {
                        Ok(body) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                            hist.rec(t0.elapsed().as_micros() as u64);
                            if let Ok(v) =
                                serde_json::from_slice::<Vec<serde_json::Value>>(&body)
                            {
                                recs.fetch_add(v.len() as u64, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    _ => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(gap).await;
            }
        }));
    }

    // Window reporter until the deadline.
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let left = deadline - Instant::now();
        tokio::time::sleep(Duration::from_secs(20).min(left)).await;
        let ap = ap_hist.drain_sorted();
        let sc = sc_hist.drain_sorted();
        let line = serde_json::json!({
            "phase": "steady",
            "apOk": ap_ok.load(Ordering::Relaxed),
            "apThr": ap_thr.load(Ordering::Relaxed),
            "apErr": ap_err.load(Ordering::Relaxed),
            "apWinP50Ms": pctl_ms(&ap, 0.5),
            "apWinP99Ms": pctl_ms(&ap, 0.99),
            "scOk": sc_ok.load(Ordering::Relaxed),
            "scErr": sc_err.load(Ordering::Relaxed),
            "scWinP50Ms": pctl_ms(&sc, 0.5),
            "scWinP99Ms": pctl_ms(&sc, 0.99),
            "scRecords": sc_records.load(Ordering::Relaxed),
            "projectsActive": mt_active,
            "surface": if mt { "product" } else { "raw" },
            "streams": n, "active": active,
            "recordBytes": record_bytes, "intervalMs": interval_ms,
            "ts": now_ms()/1000,
        });
        eprintln!("{line}");
        writeln!(out, "{line}")?;
        out.flush()?;
        stats.lines.lock().unwrap().push(line.to_string());
    }
    stop.store(1, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
    }
    let done = serde_json::json!({
        "phase": "wide_done",
        "apOk": ap_ok.load(Ordering::Relaxed),
        "apThr": ap_thr.load(Ordering::Relaxed),
        "apErr": ap_err.load(Ordering::Relaxed),
        "scOk": sc_ok.load(Ordering::Relaxed),
        "scErr": sc_err.load(Ordering::Relaxed),
        "projectsActive": mt_active,
        "surface": if mt { "product" } else { "raw" },
        "streams": n, "active": active,
        "recordBytes": record_bytes, "intervalMs": interval_ms,
        "steadySecs": secs,
        "binSha256": std::env::var("APP_BINARY_SHA256").unwrap_or_default(),
        "ts": now_ms()/1000,
    });
    eprintln!("{done}");
    writeln!(out, "{done}")?;
    out.flush()?;
    stats.lines.lock().unwrap().push(done.to_string());
    eprintln!("WIDE_DONE");
    if std::env::var("BENCH_HOLD").as_deref() == Ok("1") {
        eprintln!("WIDE_DONE (holding for scrape)");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tail_cursor_tests {
    use super::*;
    use std::io::{Read as _, Write as _};

    /// Review #2's required integrity test: a response whose HEADERS
    /// succeed (carrying a valid next offset) but whose BODY dies
    /// mid-stream must NOT advance the cursor. The consumer must discard
    /// its speculative lookahead, retry from the committed cursor, and
    /// decode every record exactly once.
    ///
    /// Server script (offset -> behavior):
    ///   now  -> 200, next=A, body []
    ///   A #1 -> 200, next=B, body "[{\"t\":1},{\"t\":2}" TRUNCATED
    ///          (Content-Length lies; connection closed early)
    ///   B #1 -> the speculative lookahead the consumer must DISCARD:
    ///          200, next=C, body [{"t":99}]  <- the canary: if 99 is
    ///          ever decoded, the consumer accepted a lookahead past a
    ///          failed body (the skip bug)
    ///   A #2 -> 200, next=B, body [{"t":1},{"t":2}]   (the retry)
    ///   B #2 -> 200, next=C, body [{"t":3}]
    ///   C    -> 204, next=C (idle; loop parks until stop)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_body_retries_from_the_committed_cursor() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let slog = log.clone();

        std::thread::spawn(move || {
            let mut a_count = 0;
            let mut b_count = 0;
            for conn in listener.incoming() {
                let Ok(mut sock) = conn else { break };
                loop {
                    let mut buf = [0u8; 4096];
                    let mut req = Vec::new();
                    // read until end of headers
                    let ok = loop {
                        match sock.read(&mut buf) {
                            Ok(0) => break false,
                            Ok(n) => {
                                req.extend_from_slice(&buf[..n]);
                                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break true;
                                }
                            }
                            Err(_) => break false,
                        }
                    };
                    if !ok {
                        break;
                    }
                    let line = String::from_utf8_lossy(&req);
                    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let offset = path
                        .split("offset=")
                        .nth(1)
                        .map(|t| t.split('&').next().unwrap_or("").to_string())
                        .unwrap_or_default();
                    slog.lock().unwrap().push(offset.clone());
                    let respond = |sock: &mut std::net::TcpStream,
                                   status: &str,
                                   next: &str,
                                   body: &[u8],
                                   lie_len: Option<usize>| {
                        let len = lie_len.unwrap_or(body.len());
                        let head = format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\nstream-next-offset: {next}\r\ncontent-length: {len}\r\n\r\n"
                        );
                        let _ = sock.write_all(head.as_bytes());
                        let _ = sock.write_all(body);
                    };
                    match offset.as_str() {
                        "now" => respond(&mut sock, "200 OK", "A", b"[]", None),
                        "A" => {
                            a_count += 1;
                            if a_count == 1 {
                                // Truncated: promise 40 bytes, send 16, kill.
                                respond(
                                    &mut sock,
                                    "200 OK",
                                    "B",
                                    b"[{\"t\":1},{\"t\":2}",
                                    Some(40),
                                );
                                let _ = sock.shutdown(std::net::Shutdown::Both);
                                break;
                            }
                            respond(&mut sock, "200 OK", "B", b"[{\"t\":1},{\"t\":2}]", None);
                        }
                        "B" => {
                            b_count += 1;
                            if b_count == 1 {
                                // The speculative canary.
                                respond(&mut sock, "200 OK", "C", b"[{\"t\":99}]", None);
                            } else {
                                respond(&mut sock, "200 OK", "C", b"[{\"t\":3}]", None);
                            }
                        }
                        _ => respond(&mut sock, "204 No Content", "C", b"", None),
                    }
                }
            }
        });

        let stats = Stats::new();
        let stop = Arc::new(AtomicU64::new(0));
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let s2 = stats.clone();
        let st2 = stop.clone();
        let base = format!("http://{addr}");
        let run = tokio::spawn(async move {
            prisma_tail_loop(&http, &base, "t", "tok", "key", &s2, &st2).await;
        });
        // Let the script play out, then stop.
        for _ in 0..100 {
            if stats.records_decoded.load(Ordering::Relaxed) >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop.store(1, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(10), run).await;

        assert_eq!(
            stats.records_decoded.load(Ordering::Relaxed),
            3,
            "records 1,2,3 exactly once — a 99 or a count of 5 means the \
             speculative response was accepted past a failed body"
        );
        assert!(
            stats.body_failures.load(Ordering::Relaxed) >= 1,
            "the truncated body must have been detected"
        );
        let seen = log.lock().unwrap().clone();
        let a_polls = seen.iter().filter(|o| o.as_str() == "A").count();
        assert!(
            a_polls >= 2,
            "the consumer must have re-polled A from the committed cursor (log: {seen:?})"
        );
    }
}
