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
use std::{path::PathBuf, sync::RwLock};

use base64::Engine;
use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;

#[path = "../offsets.rs"]
#[allow(dead_code)]
mod offsets;

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
    /// After append load, HEAD every stream and require its durable next
    /// offset to equal the exact number of successful entries generated.
    #[arg(long, default_value_t = false)]
    verify_offsets: bool,
    /// Stream encryption key (base64url, 32 bytes) sent as
    /// Stream-Encryption-Key on every request. Omit for servers that don't
    /// require it (the old TS implementation).
    #[arg(long, env = "STREAM_KEY")]
    key: Option<String>,
    /// Bearer token for production-authenticated targets.
    #[arg(long, env = "STREAMS_AUTH_TOKEN")]
    auth_token: Option<String>,
    /// Mode-0600 file containing the current bearer token. The file is
    /// re-read off the request path so a >=24-hour production soak can rotate
    /// short-lived JWTs. Replace it atomically; malformed reads retain the
    /// last good token and increment the reported failure counter.
    #[arg(long, env = "STREAMS_AUTH_TOKEN_FILE", conflicts_with = "auth_token")]
    auth_token_file: Option<PathBuf>,
    #[arg(long, default_value_t = 30)]
    auth_token_refresh_secs: u64,
    /// Expected JWT `sub` for both the initial and every refreshed token.
    /// The evidence records only whether subject pinning was enabled.
    #[arg(long, env = "STREAMS_AUTH_TOKEN_SUBJECT")]
    auth_token_subject: Option<String>,
}

#[derive(Clone)]
struct AuthToken {
    current: Arc<RwLock<Option<String>>>,
    source: &'static str,
    refresh_successes: Arc<AtomicU64>,
    token_changes: Arc<AtomicU64>,
    refresh_failures: Arc<AtomicU64>,
    subject_pinned: bool,
}

impl AuthToken {
    async fn new(args: &Args) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=3600).contains(&args.auth_token_refresh_secs),
            "auth token refresh interval must be 1..=3600 seconds"
        );
        let refresh_successes = Arc::new(AtomicU64::new(0));
        let token_changes = Arc::new(AtomicU64::new(0));
        let refresh_failures = Arc::new(AtomicU64::new(0));
        let (initial, source) = match &args.auth_token_file {
            Some(path) => (
                Some(read_token_file(path, args.auth_token_subject.as_deref()).await?),
                "file",
            ),
            None => {
                if let (Some(token), Some(subject)) = (
                    args.auth_token.as_deref(),
                    args.auth_token_subject.as_deref(),
                ) {
                    anyhow::ensure!(
                        jwt_subject(token).as_deref() == Some(subject),
                        "fixed auth JWT does not match the expected subject"
                    );
                }
                (args.auth_token.clone(), "fixed")
            }
        };
        let auth = Self {
            current: Arc::new(RwLock::new(initial)),
            source,
            refresh_successes,
            token_changes,
            refresh_failures,
            subject_pinned: args.auth_token_subject.is_some(),
        };
        if let Some(path) = args.auth_token_file.clone() {
            let refresh = Duration::from_secs(args.auth_token_refresh_secs);
            let current = auth.current.clone();
            let successes = auth.refresh_successes.clone();
            let changes = auth.token_changes.clone();
            let failures = auth.refresh_failures.clone();
            let expected_subject = args.auth_token_subject.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(refresh).await;
                    match read_token_file(&path, expected_subject.as_deref()).await {
                        Ok(next) => {
                            successes.fetch_add(1, Ordering::Relaxed);
                            let mut slot = current.write().unwrap();
                            if slot.as_ref() != Some(&next) {
                                *slot = Some(next);
                                changes.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
        Ok(auth)
    }

    fn current(&self) -> Option<String> {
        self.current.read().unwrap().clone()
    }

    fn evidence(&self) -> serde_json::Value {
        serde_json::json!({
            "source": self.source,
            "refresh_successes": self.refresh_successes.load(Ordering::Relaxed),
            "token_changes": self.token_changes.load(Ordering::Relaxed),
            "refresh_failures": self.refresh_failures.load(Ordering::Relaxed),
            "subject_pinned": self.subject_pinned,
        })
    }
}

fn jwt_subject(token: &str) -> Option<String> {
    let mut segments = token.split('.');
    let _header = segments.next()?;
    let payload = segments.next()?;
    let _signature = segments.next()?;
    if segments.next().is_some() || payload.len() > 16 * 1024 {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    if decoded.len() > 16 * 1024 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let subject = value.get("sub")?.as_str()?;
    if subject.is_empty()
        || subject.len() > 128
        || !subject
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return None;
    }
    Some(subject.to_string())
}

async fn read_token_file(path: &PathBuf, expected_subject: Option<&str>) -> anyhow::Result<String> {
    let metadata = tokio::fs::metadata(path).await?;
    anyhow::ensure!(metadata.is_file(), "auth token path is not a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "auth token file must not be accessible by group or other"
        );
    }
    anyhow::ensure!(metadata.len() <= 16 * 1024, "auth token file is too large");
    let token = tokio::fs::read_to_string(path).await?;
    let token = token.trim();
    anyhow::ensure!(
        !token.is_empty() && token.len() <= 16 * 1024 && !token.chars().any(char::is_whitespace),
        "auth token file is empty, oversized, or contains whitespace"
    );
    if let Some(expected) = expected_subject {
        anyhow::ensure!(
            jwt_subject(token).as_deref() == Some(expected),
            "auth token JWT does not match the expected subject"
        );
    }
    Ok(token.to_string())
}

fn authorized(
    mut rb: reqwest::RequestBuilder,
    key: &Option<String>,
    auth_token: &AuthToken,
) -> reqwest::RequestBuilder {
    if let Some(k) = key {
        rb = rb.header("stream-encryption-key", k.as_str());
    }
    if let Some(token) = auth_token.current() {
        rb = rb.bearer_auth(token);
    }
    rb
}

struct Shared {
    hist: Mutex<Histogram<u64>>,
    ok: AtomicU64,
    errors: AtomicU64,
    entries_ok: AtomicU64,
    bytes_ok: AtomicU64,
    per_stream_entries_ok: Vec<AtomicU64>,
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
    let auth_token = AuthToken::new(&args).await?;
    let shared = Arc::new(Shared {
        hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
        ok: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        entries_ok: AtomicU64::new(0),
        bytes_ok: AtomicU64::new(0),
        per_stream_entries_ok: (0..args.streams).map(|_| AtomicU64::new(0)).collect(),
    });
    let (body, content_type) = payload(args.payload_bytes, args.entries);
    let body = Arc::new(body);

    // Pre-create the target streams (the old server 404s appends to
    // non-existent streams).
    for s in 0..args.streams {
        let r = authorized(
            client.put(format!("{}/v1/stream/{}-{}", args.url, args.prefix, s)),
            &args.key,
            &auth_token,
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
        let stream_index = w % args.streams;
        let entries = args.entries as u64;
        let bytes = args.payload_bytes as u64 * entries;
        let content_type = content_type.to_string();
        let key = args.key.clone();
        let auth_token = auth_token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let now = Instant::now();
                if now >= stop_at {
                    break;
                }
                let t0 = Instant::now();
                let res = authorized(client.post(&url), &key, &auth_token)
                    .header("content-type", content_type.as_str())
                    .body(body.as_ref().clone())
                    .send()
                    .await;
                let elapsed = t0.elapsed();
                let in_window = t0 >= measure_start;
                match res {
                    Ok(r) if r.status().is_success() => {
                        let _ = r.bytes().await;
                        // Offset reconciliation covers the full process run,
                        // including warmup. Latency/throughput counters remain
                        // measurement-window only.
                        shared.per_stream_entries_ok[stream_index]
                            .fetch_add(entries, Ordering::Relaxed);
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
    let mut offset_mismatches = Vec::new();
    if args.verify_offsets {
        for stream_index in 0..args.streams {
            let url = format!("{}/v1/stream/{}-{}", args.url, args.prefix, stream_index);
            let response = authorized(client.head(url), &args.key, &auth_token)
                .send()
                .await?;
            let observed = response
                .headers()
                .get("stream-next-offset")
                .and_then(|header| header.to_str().ok())
                .and_then(|token| offsets::Offset::parse(token).ok())
                .map(offsets::Offset::scan_from);
            let expected = shared.per_stream_entries_ok[stream_index].load(Ordering::Relaxed);
            if observed != Some(expected) {
                offset_mismatches.push(serde_json::json!({
                    "stream": stream_index,
                    "expected_next": expected,
                    "observed_next": observed,
                }));
            }
        }
    }
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
        "offset_verification": {
            "enabled": args.verify_offsets,
            "passed": !args.verify_offsets || offset_mismatches.is_empty(),
            "streams_verified": if args.verify_offsets { args.streams } else { 0 },
            "mismatches": offset_mismatches,
        },
        "auth": auth_token.evidence(),
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
    anyhow::ensure!(
        !args.verify_offsets || offset_mismatches.is_empty(),
        "durable stream offsets did not match successful entries"
    );
    Ok(())
}

async fn bench_read(args: Args) -> anyhow::Result<()> {
    let client = make_client(args.concurrency);
    let auth_token = AuthToken::new(&args).await?;
    let t0 = Instant::now();
    let mut total_bytes = 0u64;
    let mut total_reqs = 0u64;
    let mut handles = Vec::new();
    for s in 0..args.streams {
        let client = client.clone();
        let url = format!("{}/v1/stream/{}-{}", args.url, args.prefix, s);
        let key = args.key.clone();
        let auth_token = auth_token.clone();
        handles.push(tokio::spawn(async move {
            let mut offset = "-1".to_string();
            let mut bytes = 0u64;
            let mut reqs = 0u64;
            loop {
                let res = authorized(
                    client.get(format!("{url}?offset={offset}")),
                    &key,
                    &auth_token,
                )
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
        "auth": auth_token.evidence(),
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

/// Measures the gap between append ACK and object-store durability.
async fn bench_durability(args: Args) -> anyhow::Result<()> {
    let client = make_client(4);
    let auth_token = AuthToken::new(&args).await?;
    let stream = format!("{}-dur-{}", args.prefix, std::process::id());
    let url = format!("{}/v1/stream/{}", args.url, stream);
    let r = authorized(client.put(&url), &args.key, &auth_token)
        .send()
        .await?;
    anyhow::ensure!(r.status().is_success(), "create failed: {}", r.status());
    let mut lags_ms: Vec<f64> = Vec::new();
    let iterations = 10usize;
    for _ in 0..iterations {
        let t0 = Instant::now();
        let res = authorized(client.post(&url), &args.key, &auth_token)
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
            let r = authorized(client.get(&details_url), &None, &auth_token)
                .send()
                .await?;
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
