//! Stress test for the state-protocol profile: simulates a Postgres logical-
//! decoding adapter ingesting WAL change records into a state-protocol
//! stream, with live-query consumers driving invalidation via /touch/wait.
//!
//! Model:
//! - "Postgres": T tenants, each with a running row-version counter (the
//!   in-process ground truth standing in for the queryable database state).
//! - WAL generator workers append batches of State Protocol change records
//!   ({type, key, value, old_value, headers:{operation,txid,timestamp}}) and
//!   bump the tenant's truth counter after the durable ACK.
//! - Fine consumers: one live query per tenant
//!   (`SELECT .. WHERE tenantId = $1`), template (entity, [tenantId]),
//!   waiting on the watch key; on touched -> "re-run the query" (read the
//!   truth counter).
//! - Coarse consumers wait on the table key.
//!
//! Validation:
//! - MISSED invalidations: consumers only refresh on touched/stale. At the
//!   end every consumer's observed value must equal the tenant's final
//!   truth. Any shortfall = a missed invalidation (hard failure).
//! - SPURIOUS wakes: touched=true but the tenant's truth didn't change.
//! - LATENCY: time from the first un-observed durable ACK for a tenant to
//!   the consumer's wake.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use serde_json::json;
use tokio::sync::Mutex;

#[path = "../crypto.rs"]
mod crypto;
#[path = "../touch_keys.rs"]
mod touch_keys;

#[derive(Parser, Debug, Clone)]
#[command(name = "livebench")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    #[arg(long, env = "STREAM_KEY")]
    key: String,
    #[arg(long, default_value = "pgwal")]
    stream: String,
    #[arg(long, default_value = "public.todos")]
    entity: String,
    #[arg(long, default_value_t = 500)]
    tenants: usize,
    /// Fine waiters (one live query per waiter, tenant = waiter % tenants).
    #[arg(long, default_value_t = 500)]
    fine_waiters: usize,
    #[arg(long, default_value_t = 50)]
    coarse_waiters: usize,
    #[arg(long, default_value_t = 8)]
    gen_workers: usize,
    /// Change records per WAL transaction (JSON array append).
    #[arg(long, default_value_t = 10)]
    batch: usize,
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,
    #[arg(long, default_value_t = 5000)]
    wait_timeout_ms: u64,
    /// Route waits through this edge simulator / CDN base URL (appends and
    /// control traffic always go direct to --url).
    #[arg(long)]
    edge_url: Option<String>,
}

struct Tenant {
    truth: AtomicU64,
    /// ns timestamp of the first durable ACK not yet observed by the fine
    /// consumer (0 = none pending). Drives invalidation-latency samples.
    first_unobserved_ns: AtomicU64,
}

struct Shared {
    tenants: Vec<Tenant>,
    stop: AtomicBool,
    appends_ok: AtomicU64,
    appends_err: AtomicU64,
    changes: AtomicU64,
    wakes: AtomicU64,
    spurious: AtomicU64,
    stales: AtomicU64,
    waits: AtomicU64,
    lat: Mutex<Histogram<u64>>,
    t0: Instant,
}

impl Shared {
    fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(4096)
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(30))
        .http1_only()
        .build()
        .unwrap()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let http = client();
    let base = format!("{}/v1/stream/{}", args.url, args.stream);

    // Create the state-protocol stream with the query family PINNED in the
    // descriptor (durable; survives restarts and moves by construction).
    let r = http
        .put(&base)
        .header("stream-encryption-key", &args.key)
        .header("stream-profile", "state-protocol")
        .header(
            "stream-touch-templates",
            json!([{"entity": args.entity, "fields": ["tenantId"]}]).to_string(),
        )
        .send()
        .await?;
    anyhow::ensure!(r.status().is_success(), "create failed: {}", r.status());

    // Derive the touch capability token (consumers hold ONLY this — they can
    // observe invalidations but not decrypt payloads).
    let epoch_hex = {
        let streams: serde_json::Value = http
            .get(format!("{}/v1/streams", args.url))
            .send()
            .await?
            .json()
            .await?;
        streams
            .as_array()
            .and_then(|a| a.iter().find(|s| s["name"] == args.stream.as_str()))
            .and_then(|s| s["stream_epoch"].as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("stream epoch not found"))?
    };
    let epoch: [u8; 16] = crypto::unhex(&epoch_hex)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| anyhow::anyhow!("bad epoch"))?;
    let stream_key = crypto::StreamKey::from_b64(&args.key).map_err(anyhow::Error::msg)?;
    let token = crypto::touch_token(&stream_key, &epoch);
    let sig_key = crypto::wait_sig_key(&token, &epoch);
    let tpl_id = touch_keys::template_id(&args.entity, &["tenantId".to_string()]);
    println!(
        "pinned template {:016x} for {} ([tenantId])",
        tpl_id, args.entity
    );

    // A wait without any credential (no sig, no token) must be rejected.
    let r = http
        .get(format!(
            "{base}/touch/key/0000000000000000?cursor=now&timeout=1ms"
        ))
        .send()
        .await?;
    anyhow::ensure!(
        r.status() == reqwest::StatusCode::FORBIDDEN,
        "unauthenticated wait should be 403, got {}",
        r.status()
    );
    println!("touch auth: unauthenticated wait correctly rejected (403)");

    // Waits go through the edge (CDN simulator) when configured.
    let wait_base = args.edge_url.clone().unwrap_or_else(|| args.url.clone());

    let shared = Arc::new(Shared {
        tenants: (0..args.tenants)
            .map(|_| Tenant {
                truth: AtomicU64::new(0),
                first_unobserved_ns: AtomicU64::new(0),
            })
            .collect(),
        stop: AtomicBool::new(false),
        appends_ok: AtomicU64::new(0),
        appends_err: AtomicU64::new(0),
        changes: AtomicU64::new(0),
        wakes: AtomicU64::new(0),
        spurious: AtomicU64::new(0),
        stales: AtomicU64::new(0),
        waits: AtomicU64::new(0),
        lat: Mutex::new(Histogram::new_with_bounds(1, 300_000_000_000, 3).unwrap()),
        t0: Instant::now(),
    });

    // ---- consumers ----
    let mut consumers = Vec::new();
    let final_observed: Arc<Vec<AtomicU64>> =
        Arc::new((0..args.fine_waiters).map(|_| AtomicU64::new(0)).collect());
    for w in 0..args.fine_waiters {
        let tenant_idx = w % args.tenants;
        let watch = touch_keys::key_hex(touch_keys::watch_key(tpl_id, &[format!("t{tenant_idx}")]));
        let sig = crypto::wait_url_sig(&sig_key, &watch);
        let http = http.clone();
        let shared = shared.clone();
        let observed_slot = final_observed.clone();
        let timeout_ms = args.wait_timeout_ms;
        let wait_url = format!(
            "{}/v1/stream/{}/touch/key/{}",
            wait_base, args.stream, watch
        );
        consumers.push(tokio::spawn(async move {
            let mut cursor = "now".to_string();
            let mut observed = 0u64;
            loop {
                shared.waits.fetch_add(1, Ordering::Relaxed);
                // Collapsible wait: cohort members share this exact URL.
                let res = http
                    .get(format!(
                        "{wait_url}?cursor={cursor}&sig={sig}&timeout={timeout_ms}ms"
                    ))
                    .send()
                    .await;
                let Ok(r) = res else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let Ok(v) = r.json::<serde_json::Value>().await else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                cursor = v["cursor"].as_str().unwrap_or("now").to_string();
                let touched = v["touched"].as_bool().unwrap_or(false);
                let stale = v["stale"].as_bool().unwrap_or(false);
                if touched || stale {
                    // "Re-run the query": read the current tenant state.
                    let t = &shared.tenants[tenant_idx];
                    let now = t.truth.load(Ordering::Acquire);
                    if stale {
                        shared.stales.fetch_add(1, Ordering::Relaxed);
                    }
                    if touched {
                        shared.wakes.fetch_add(1, Ordering::Relaxed);
                        if now == observed {
                            shared.spurious.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if now != observed {
                        observed = now;
                        let first = t.first_unobserved_ns.swap(0, Ordering::AcqRel);
                        if first != 0 {
                            let lat = shared.now_ns().saturating_sub(first);
                            shared.lat.lock().await.record(lat.max(1)).ok();
                        }
                    }
                }
                // Strict rule: `observed` only ever advances on touched/stale.
                // stop is set 1.5s after the last durable append, far beyond
                // the 25ms bucket flush, so any owed invalidation has already
                // arrived through a wait by the time we break.
                if shared.stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            observed_slot[w].store(observed, Ordering::Release);
        }));
    }
    let table_key = touch_keys::key_hex(touch_keys::table_key(&args.entity));
    let table_sig = crypto::wait_url_sig(&sig_key, &table_key);
    for _ in 0..args.coarse_waiters {
        let http = http.clone();
        let shared = shared.clone();
        let timeout_ms = args.wait_timeout_ms;
        let wait_url = format!(
            "{}/v1/stream/{}/touch/key/{}",
            wait_base, args.stream, table_key
        );
        let sig = table_sig.clone();
        consumers.push(tokio::spawn(async move {
            let mut cursor = "now".to_string();
            loop {
                let res = http
                    .get(format!(
                        "{wait_url}?cursor={cursor}&sig={sig}&timeout={timeout_ms}ms"
                    ))
                    .send()
                    .await;
                if let Ok(r) = res {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        cursor = v["cursor"].as_str().unwrap_or("now").to_string();
                    }
                }
                if shared.stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        }));
    }

    // ---- WAL generators ----
    tokio::time::sleep(Duration::from_millis(300)).await; // waiters settle
    let mut generators = Vec::new();
    let gen_until = Instant::now() + Duration::from_secs(args.duration_secs);
    for g in 0..args.gen_workers {
        let http = http.clone();
        let base = base.clone();
        let shared = shared.clone();
        let args = args.clone();
        generators.push(tokio::spawn(async move {
            let mut txid = (g as u64) << 32;
            let mut rowseq = 0u64;
            while Instant::now() < gen_until {
                txid += 1;
                let tenant_idx = (txid as usize * 2654435761) % args.tenants;
                let tenant = format!("t{tenant_idx}");
                let mut records = Vec::with_capacity(args.batch);
                for i in 0..args.batch {
                    rowseq += 1;
                    let row_id = format!("r{g}-{rowseq}");
                    let op = match i % 3 {
                        0 => "insert",
                        1 => "update",
                        _ => "delete",
                    };
                    let mut rec = json!({
                        "type": args.entity,
                        "key": row_id,
                        "value": {"id": row_id, "tenantId": tenant, "status": "open", "n": rowseq},
                        "headers": {
                            "operation": op,
                            "txid": txid.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                        },
                    });
                    if op == "update" {
                        rec["old_value"] =
                            json!({"id": row_id, "tenantId": tenant, "status": "done", "n": rowseq - 1});
                    }
                    if op == "delete" {
                        rec["value"] = serde_json::Value::Null;
                        rec["old_value"] =
                            json!({"id": row_id, "tenantId": tenant, "status": "open", "n": rowseq});
                    }
                    records.push(rec);
                }
                // Model the real ordering: Postgres commits FIRST (the new
                // row versions are queryable), then logical decoding emits
                // the change and the adapter appends it — retrying until it
                // lands, like a WAL-position-tracking adapter would.
                {
                    let t = &shared.tenants[tenant_idx];
                    t.truth.fetch_add(args.batch as u64, Ordering::AcqRel);
                    let now = shared.now_ns();
                    let _ = t.first_unobserved_ns.compare_exchange(
                        0,
                        now,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );
                }
                loop {
                    let res = http
                        .post(&base)
                        .header("stream-encryption-key", &args.key)
                        .header("content-type", "application/json")
                        .json(&records)
                        .send()
                        .await;
                    match res {
                        Ok(r) if r.status().is_success() => {
                            shared.appends_ok.fetch_add(1, Ordering::Relaxed);
                            shared.changes.fetch_add(args.batch as u64, Ordering::Relaxed);
                            break;
                        }
                        _ => {
                            shared.appends_err.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }));
    }
    for g in generators {
        g.await?;
    }
    println!("generators done; draining consumers...");
    // Grace: let in-flight buckets flush and waiters observe final state.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    shared.stop.store(true, Ordering::Relaxed);
    // Consumers exit after their current wait returns (bounded by timeout).
    for c in consumers {
        let _ = c.await;
    }

    // ---- verdict ----
    let mut missed = 0usize;
    for w in 0..args.fine_waiters {
        let tenant_idx = w % args.tenants;
        let expect = shared.tenants[tenant_idx].truth.load(Ordering::Acquire);
        let got = final_observed[w].load(Ordering::Acquire);
        if got < expect {
            missed += 1;
            if missed <= 5 {
                println!("MISSED: waiter {w} tenant {tenant_idx} observed {got} < {expect}");
            }
        }
    }
    let lat = shared.lat.lock().await;
    let secs = args.duration_secs as f64;
    println!("\n=== livebench summary ===");
    println!(
        "appends ok={} err={} | changes={} ({:.0}/s)",
        shared.appends_ok.load(Ordering::Relaxed),
        shared.appends_err.load(Ordering::Relaxed),
        shared.changes.load(Ordering::Relaxed),
        shared.changes.load(Ordering::Relaxed) as f64 / secs
    );
    println!(
        "fine waiters={} coarse={} | waits={} wakes={} spurious={} stales={}",
        args.fine_waiters,
        args.coarse_waiters,
        shared.waits.load(Ordering::Relaxed),
        shared.wakes.load(Ordering::Relaxed),
        shared.spurious.load(Ordering::Relaxed),
        shared.stales.load(Ordering::Relaxed),
    );
    println!(
        "invalidation latency ms: p50={:.1} p95={:.1} p99={:.1} max={:.1} (n={})",
        lat.value_at_quantile(0.50) as f64 / 1e6,
        lat.value_at_quantile(0.95) as f64 / 1e6,
        lat.value_at_quantile(0.99) as f64 / 1e6,
        lat.max() as f64 / 1e6,
        lat.len(),
    );
    println!(
        "missed invalidations: {missed} {}",
        if missed == 0 { "✓" } else { "✗ FAIL" }
    );
    if let Some(edge) = &args.edge_url {
        if let Ok(r) = http.get(format!("{edge}/_edge/stats")).send().await {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                let down = v["downstream"].as_u64().unwrap_or(0);
                let up = v["upstream"].as_u64().unwrap_or(0);
                println!(
                    "edge: downstream={} upstream={} coalesced={} cache_hits={} | ORIGIN LOAD REDUCTION: {:.1}x",
                    down,
                    up,
                    v["coalesced"].as_u64().unwrap_or(0),
                    v["cache_hits"].as_u64().unwrap_or(0),
                    if up > 0 { down as f64 / up as f64 } else { 0.0 }
                );
            }
        }
    }
    anyhow::ensure!(missed == 0, "{missed} missed invalidations");
    Ok(())
}
