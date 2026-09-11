// Pilot harness, run 2: high-concurrency Rust replacements for the Bun
// load balancer and workload generator (which capped run 1 at ~256
// in-flight requests each — see EXPERIMENT-PILOT.md).
//
//   MODE=lb   rendezvous-hash reverse proxy + live dashboard
//   MODE=gen  closed-loop generator: concurrency doubles every RAMP_SECS
//
// The generator is closed-loop (workers issue the next request only after
// the previous completes), so offered load self-paces to what the fleet
// can absorb and congestion collapse is impossible by construction.

#[path = "pilot/client.rs"]
mod http_client;
use http_client::{RotatingClient, client};

#[path = "pilot/proxy.rs"]
mod routing;
use routing::proxy;

#[path = "pilot/benchmark.rs"]
mod benchmark;

#[path = "pilot/generator.rs"]
mod workload_generator;

use axum::Router;
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use object_store::ObjectStoreExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[expect(
    clippy::disallowed_methods,
    reason = "pilot process configuration; this independent workload binary owns its explicit environment inputs; routing them through server runtime state would couple separate executables"
)]
fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}
fn now_ms() -> u64 {
    epoch_millis(SystemTime::now())
}

fn epoch_millis(now: SystemTime) -> u64 {
    u64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

// Matches the JS FNV-1a in the run-1 Bun LB so stream→server pinning is
// unchanged across harnesses.
fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 2166136261;
    for b in s.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h
}
/// Log an upstream transport error at most once a second: a dead
/// instance under load produces tens of thousands of identical errors.
fn tracing_warn_once(e: &reqwest::Error) {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = now_ms();
    let prev = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(prev) > 1000
        && LAST
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        eprintln!("lb: upstream transport error: {e}");
    }
}

/// How long an upstream stays locally ejected after an unmarked
/// (platform-edge) response. Long enough to cover a redeploy gap,
/// short enough that a revived instance rejoins quickly.
fn eject_ms() -> u64 {
    env("LB_EJECT_MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000)
}

fn pick(stream: &str, upstreams: &[String]) -> usize {
    let mut best = 0usize;
    let mut best_score = 0u32;
    for (i, u) in upstreams.iter().enumerate() {
        let score = fnv1a(&format!("{stream} {u}"));
        if i == 0 || score > best_score {
            best_score = score;
            best = i;
        }
    }
    best
}

// ---- fleet view (COMPUTE-SPEC §2/§4): read desired.json + heartbeats ----

#[derive(Clone, Default)]
struct FleetView {
    desired: usize,
    /// Per-upstream-index: (rps, ack_p50_ms, live, cpu_pct) from heartbeats.
    heartbeats: Vec<(f64, f64, bool, f64)>,
    /// Ring active set (instance names): first `desired` ordinal instances
    /// minus any heartbeat-dark >30 s (mirrors the servers' R2 view), with
    /// an unfiltered fallback so a fully-asleep fleet still gets woken.
    active: Vec<String>,
    /// Shard bit-prefixes from the data namespace's topology.json.
    topology: Vec<String>,
    /// Rebalancer shard moves (fleet/overrides.json): prefix -> instance.
    /// Routing consults these before the rendezvous pick, mirroring the
    /// servers' effective_owner — without this every request for an
    /// override-owned shard takes a 409 Streams-Replay-To double hop
    /// (FLEET-CAMPAIGN.md: streams-3's entire steady-state load).
    overrides: std::collections::HashMap<String, String>,
}

#[expect(
    clippy::expect_used,
    reason = "pilot object-store startup; missing credentials or invalid store configuration must stop the selected workload; defaults could publish measurements to an unintended store"
)]
fn fleet_store(prefix: &str) -> Arc<dyn object_store::ObjectStore> {
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(env("S3_ENDPOINT").expect("S3_ENDPOINT"))
        .with_bucket_name(env("S3_BUCKET").expect("S3_BUCKET"))
        .with_region(env("S3_REGION").unwrap_or_else(|| "auto".into()))
        .with_access_key_id(env("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID"))
        .with_secret_access_key(env("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY"))
        // Local fleets use plain-http object stores (s3lite); without
        // this every heartbeat/desired read fails silently and the LB
        // routes the whole fleet to instance 1 (docker staircase find).
        .with_allow_http(true)
        .build()
        .expect("s3 store");
    Arc::new(object_store::prefix::PrefixStore::new(s3, prefix))
}

/// The COLLECTION NAME a request targets, decoded, mirroring the
/// server's ProductRoute grammar. Names are hierarchical UTF-8
/// ("customers/acme/orders"), so hashing only the first path segment
/// routed every stream under a shared prefix to one instance and made
/// the 409-replay path carry traffic that should never have missed
/// (round-19 fleet-contract finding). Sub-resources and the known final
/// action verbs terminate the name; a ':' anywhere else is part of it.
fn collection_name(path: &str) -> Option<String> {
    const SUBRESOURCES: [&str; 5] = ["records", "consumers", "producers", "watches", "forks"];
    let rest = path
        .strip_prefix("/v1/streams/")
        .or_else(|| path.strip_prefix("/v1/stream/"))?;
    let rest = rest.split('?').next().unwrap_or(rest);
    // Trim the trailing action verb, if any: ".../name:seal".
    let (head, _verb) = match rest.rsplit_once(':') {
        Some((h, v)) if !v.contains('/') => (h, Some(v)),
        _ => (rest, None),
    };
    // Trim a trailing sub-resource path ("<name>/records", "<name>/
    // consumers/<c>"): everything from the FIRST segment that is a known
    // sub-resource keyword onward belongs to the route, not the name.
    let segs: Vec<&str> = head.split('/').collect();
    let end = segs
        .iter()
        .position(|s| SUBRESOURCES.contains(s))
        .unwrap_or(segs.len());
    let name = segs[..end].join("/");
    let name = percent_decode(&name);
    if name.is_empty() { None } else { Some(name) }
}

/// One-shot percent-decoding of a path (the SDK encodes each name
/// segment). '+' is NOT a space — same rule as the server.
fn percent_decode(v: &str) -> String {
    let b = v.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = |c: u8| match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            };
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| v.to_string())
}

/// Mirrors crypto::stream_hash — shard choice keys off the stream name.
fn name_hash(name: &str) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let d = Sha256::digest(name.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&d[..16]);
    out
}

/// Mirrors registry::hash_bits + shard_for_hash (longest-prefix match).
fn shard_for(topology: &[String], hash: &[u8; 16]) -> String {
    let mut bits = String::with_capacity(24);
    for byte in hash.iter().take(3) {
        bits.push_str(&format!("{byte:08b}"));
    }
    topology
        .iter()
        .filter(|p| bits.starts_with(p.as_str()))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = env("MODE").unwrap_or_else(|| std::env::args().nth(1).unwrap_or_default());
    match mode.as_str() {
        "lb" => lb().await,
        "gen" => workload_generator::run().await?,
        "bench" => benchmark::run(env).await?,
        m => {
            eprintln!("unknown MODE '{m}' (want lb|gen)");
            std::process::exit(1);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- LB ----

struct UpStat {
    reqs: AtomicU64,
    errs: AtomicU64,
    window: AtomicU64,
    ewma_us: AtomicU64,
    last_us: AtomicU64,
    cold_starts: AtomicU64,
    last_seen_ms: AtomicU64,
    /// 409 Streams-Replay-To bounces THIS upstream issued — its ownership
    /// view disagreed with the router's pick. Nonzero steady-state means
    /// the router's override/topology view is stale.
    replays: AtomicU64,
    /// Unmarked (platform-edge) responses seen from this upstream: it is
    /// dead or unpublished, not answering as a Streams server.
    unmarked: AtomicU64,
    /// Local ejection deadline (ms since epoch). Set the instant an
    /// unmarked response arrives — the router must stop sending NEW
    /// requests here immediately rather than waiting out the ~30 s
    /// heartbeat-dark window (round-19 must-fix 4).
    eject_until_ms: AtomicU64,
}

struct Lb {
    /// Live per-ordinal upstream URLs. Seeded from UPSTREAMS; the fleet
    /// poller overwrites entries from fleet/urls.json so a replaced
    /// instance (new preview URL after redeploy) is reachable without
    /// redeploying this router — env-frozen URLs turn every instance
    /// replacement into a full redeploy cascade.
    upstreams: std::sync::RwLock<Vec<String>>,
    stats: Vec<UpStat>,
    history: Mutex<VecDeque<serde_json::Value>>,
    gen_stats: Mutex<serde_json::Value>,
    fleet: Mutex<FleetView>,
    http: RotatingClient,
}

async fn lb() {
    let upstreams: Vec<String> = env("UPSTREAMS")
        .expect("UPSTREAMS required")
        .split([',', ';'])
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    let stats = upstreams
        .iter()
        .map(|_| UpStat {
            reqs: AtomicU64::new(0),
            errs: AtomicU64::new(0),
            window: AtomicU64::new(0),
            ewma_us: AtomicU64::new(0),
            last_us: AtomicU64::new(0),
            cold_starts: AtomicU64::new(0),
            last_seen_ms: AtomicU64::new(0),
            replays: AtomicU64::new(0),
            unmarked: AtomicU64::new(0),
            eject_until_ms: AtomicU64::new(0),
        })
        .collect();
    let n_up = upstreams.len();
    let lb = Arc::new(Lb {
        upstreams: std::sync::RwLock::new(upstreams),
        stats,
        history: Mutex::new(VecDeque::new()),
        gen_stats: Mutex::new(serde_json::json!(null)),
        fleet: Mutex::new(FleetView {
            desired: 1,
            ..Default::default()
        }),
        http: RotatingClient::new(),
    });

    // Router load report: the servers' ack latency cannot see edge-side
    // queueing (run 7: clients at p50 1.6-2 s while server acks sat at
    // 60-80 ms and the fleet SHRANK mid-congestion). Publish what the
    // router observes — client-experienced latency + delivered rps — to
    // the fleet prefix; the servers fold it into desired-count.
    {
        let lb = lb.clone();
        let rstore = fleet_store(&env("FLEET_PREFIX").expect("FLEET_PREFIX"));
        let router_name = env("ROUTER_NAME").unwrap_or_else(|| "router-1".into());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let (mut worst_ewma_us, mut total_window) = (0u64, 0u64);
                for s in lb.stats.iter() {
                    let seen = s.last_seen_ms.load(Ordering::Relaxed);
                    let fresh = now_ms().saturating_sub(seen) < 10_000;
                    if fresh {
                        worst_ewma_us = worst_ewma_us.max(s.ewma_us.load(Ordering::Relaxed));
                    }
                    total_window += s.window.load(Ordering::Relaxed);
                }
                let _ = total_window; // window is reset by the 1 s ticker; rps comes from it there
                let body = serde_json::json!({
                    "router": router_name,
                    "ts_ms": now_ms(),
                    "client_p50_ms": worst_ewma_us as f64 / 1000.0,
                });
                let _ = rstore
                    .put(
                        &object_store::path::Path::from(format!("routers/{router_name}.json")),
                        object_store::PutPayload::from(serde_json::to_vec(&body).unwrap()),
                    )
                    .await;
            }
        });
    }

    // Fleet poller: desired.json + heartbeats every 2 s, topology every 60 s.
    // The LB emulates the platform: it routes to only the first `desired`
    // upstreams, so the rest idle and scale to zero.
    {
        let lb = lb.clone();
        let fstore = fleet_store(&env("FLEET_PREFIX").expect("FLEET_PREFIX"));
        let dstore = fleet_store(&env("DATA_PREFIX").expect("DATA_PREFIX"));
        tokio::spawn(async move {
            let mut topo_age = 0u32;
            loop {
                // Single guard: two lock() temporaries in one expression
                // would self-deadlock the std Mutex.
                let (prev_desired, prev_topo, prev_ov) = {
                    let f = lb.fleet.lock().unwrap();
                    (f.desired, f.topology.clone(), f.overrides.clone())
                };
                let mut view = FleetView {
                    desired: prev_desired,
                    heartbeats: Vec::new(),
                    active: Vec::new(),
                    topology: prev_topo,
                    overrides: prev_ov,
                };
                if let Ok(r) = fstore
                    .get(&object_store::path::Path::from("fleet/desired.json"))
                    .await
                    && let Ok(raw) = r.bytes().await
                    && let Ok(d) = serde_json::from_slice::<serde_json::Value>(&raw)
                {
                    view.desired = (d["count"].as_u64().unwrap_or(1) as usize).clamp(1, n_up);
                }
                // Rebalancer overrides: a successful read replaces the map
                // (absent file = no overrides); a transient error keeps the
                // previous view rather than flapping routing on a blip.
                match fstore
                    .get(&object_store::path::Path::from("fleet/overrides.json"))
                    .await
                {
                    Ok(r) => {
                        if let Ok(raw) = r.bytes().await
                            && let Ok(o) = serde_json::from_slice::<serde_json::Value>(&raw)
                        {
                            view.overrides = o["entries"]
                                .as_object()
                                .map(|m| {
                                    m.iter()
                                        .filter_map(|(k, v)| {
                                            v["to"].as_str().map(|t| (k.clone(), t.to_string()))
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                        }
                    }
                    Err(object_store::Error::NotFound { .. }) => view.overrides.clear(),
                    Err(_) => {}
                }
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let mut ages_ms: Vec<i64> = Vec::new();
                for i in 0..n_up {
                    let p = object_store::path::Path::from(format!("fleet/streams-{}.json", i + 1));
                    let mut entry = (0.0, 0.0, false, 0.0);
                    let mut age = i64::MAX;
                    if let Ok(r) = fstore.get(&p).await
                        && let Ok(raw) = r.bytes().await
                        && let Ok(h) = serde_json::from_slice::<serde_json::Value>(&raw)
                    {
                        let ts = h["ts_ms"].as_i64().unwrap_or(0);
                        age = now_ms - ts;
                        let live = age < 10_000;
                        entry = (
                            if live {
                                h["rps"].as_f64().unwrap_or(0.0)
                            } else {
                                0.0
                            },
                            if live {
                                h["ack_p50_ms"].as_f64().unwrap_or(0.0)
                            } else {
                                0.0
                            },
                            live,
                            if live {
                                h["cpu_pct"].as_f64().unwrap_or(0.0)
                            } else {
                                0.0
                            },
                        );
                    }
                    ages_ms.push(age);
                    view.heartbeats.push(entry);
                }
                // Ring active set: first `desired` ordinal instances minus
                // the >30s-dark (same rule as the servers' R2 check).
                // Fallback: everyone asleep → unfiltered, so the first
                // request wakes the ordinal owner.
                let d = view.desired.clamp(1, n_up);
                let mut active: Vec<String> = (1..=d)
                    .filter(|i| ages_ms.get(i - 1).map(|a| *a < 30_000).unwrap_or(false))
                    .map(|i| format!("streams-{i}"))
                    .collect();
                if active.is_empty() {
                    active = (1..=d).map(|i| format!("streams-{i}")).collect();
                }
                view.active = active;
                // Platform emulation: on real infrastructure, scale-out
                // means the platform STARTS instance N+1. Here an instance
                // starts on first request — but the live-set ring only
                // routes to heartbeating instances, so a newly-desired
                // sleeping ordinal would deadlock dark (found in run 5:
                // desired=4, live=1 forever). Ping desired-but-stale
                // ordinals out of band; one /health GET wakes them.
                for i in 1..=d {
                    if ages_ms.get(i - 1).map(|a| *a >= 8_000).unwrap_or(true) {
                        let url = format!("{}/health", lb.upstreams.read().unwrap()[i - 1]);
                        let c = lb.http.get();
                        tokio::spawn(async move {
                            let _ = c.get(url).timeout(Duration::from_secs(20)).send().await;
                        });
                    }
                }
                if (topo_age == 0 || view.topology.is_empty())
                    && let Ok(r) = dstore
                        .get(&object_store::path::Path::from("topology.json"))
                        .await
                    && let Ok(raw) = r.bytes().await
                    && let Ok(t) = serde_json::from_slice::<serde_json::Value>(&raw)
                    && let Some(shards) = t["shards"].as_array()
                {
                    view.topology = shards
                        .iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect();
                }
                // Replaced instances publish their new preview URLs to
                // fleet/urls.json (deploy step `urls`); adopt them so a
                // kill+redeploy rejoins without touching this router.
                if let Ok(r) = fstore
                    .get(&object_store::path::Path::from("fleet/urls.json"))
                    .await
                    && let Ok(raw) = r.bytes().await
                    && let Ok(m) =
                        serde_json::from_slice::<std::collections::HashMap<String, String>>(&raw)
                {
                    let mut ups = lb.upstreams.write().unwrap();
                    for (name, url) in m {
                        if let Some(i) = name
                            .strip_prefix("streams-")
                            .and_then(|n| n.parse::<usize>().ok())
                            .and_then(|n| n.checked_sub(1))
                            && i < ups.len()
                            && !url.is_empty()
                            && ups[i] != url
                        {
                            println!("lb: upstream {name} -> {url}");
                            ups[i] = url;
                        }
                    }
                }
                topo_age = (topo_age + 1) % 30;
                *lb.fleet.lock().unwrap() = view;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // 1s ticker: roll the per-upstream request window into history and
    // poll the generator's stats endpoint for the dashboard header.
    {
        let lb = lb.clone();
        let gen_url = env("GEN_STATS_URL");
        tokio::spawn(async move {
            let poll = client();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let per: Vec<u64> = lb
                    .stats
                    .iter()
                    .map(|s| s.window.swap(0, Ordering::Relaxed))
                    .collect();
                let gv = match &gen_url {
                    Some(u) => match poll
                        .get(u)
                        .timeout(Duration::from_millis(1500))
                        .send()
                        .await
                    {
                        Ok(r) => r
                            .json::<serde_json::Value>()
                            .await
                            .unwrap_or(serde_json::json!(null)),
                        Err(_) => serde_json::json!(null),
                    },
                    None => serde_json::json!(null),
                };
                *lb.gen_stats.lock().unwrap() = gv.clone();
                let fleet = lb.fleet.lock().unwrap().clone();
                let hb_rps: Vec<f64> = fleet.heartbeats.iter().map(|(r, _, _, _)| *r).collect();
                let hb_live: Vec<bool> = fleet.heartbeats.iter().map(|(_, _, l, _)| *l).collect();
                let mut h = lb.history.lock().unwrap();
                h.push_back(serde_json::json!({
                    "t": now_ms(),
                    "perUp": per,
                    "hb": hb_rps,
                    "live": hb_live,
                    "desired": fleet.desired,
                    "conc": gv.get("concurrency"),
                    "ach": gv.get("achievedPerSec"),
                }));
                if h.len() > 900 {
                    h.pop_front();
                }
            }
        });
    }

    let app = Router::new()
        .route("/", get(|| async { Html(DASH) }))
        .route(
            "/stats",
            get(|State(lb): State<Arc<Lb>>| async move {
                let stats: Vec<serde_json::Value> = lb
                    .stats
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "reqs": s.reqs.load(Ordering::Relaxed),
                            "errs": s.errs.load(Ordering::Relaxed),
                            "ewmaMs": s.ewma_us.load(Ordering::Relaxed) as f64 / 1000.0,
                            "lastMs": s.last_us.load(Ordering::Relaxed) as f64 / 1000.0,
                            "coldStarts": s.cold_starts.load(Ordering::Relaxed),
                            "replays": s.replays.load(Ordering::Relaxed),
                            "unmarked": s.unmarked.load(Ordering::Relaxed),
                            "ejected": s.eject_until_ms.load(Ordering::Relaxed) > now_ms(),
                        })
                    })
                    .collect();
                let history: Vec<serde_json::Value> =
                    lb.history.lock().unwrap().iter().cloned().collect();
                let gv = lb.gen_stats.lock().unwrap().clone();
                let fleet = lb.fleet.lock().unwrap().clone();
                (
                    [("access-control-allow-origin", "*")],
                    axum::Json(serde_json::json!({
                        "upstreams": lb.stats.len(),
                        "stats": stats,
                        "gen": gv,
                        "desired": fleet.desired,
                        "heartbeats": fleet.heartbeats.iter().map(|(r, p50, l, cpu)| serde_json::json!({"rps": r, "ackMs": p50, "live": l, "cpu": cpu})).collect::<Vec<_>>(),
                        "topology": fleet.topology,
                        "overrides": fleet.overrides,
                        "history": history,
                    })),
                )
            }),
        )
        .fallback(proxy)
        .with_state(lb);

    let port = env("PORT").unwrap_or_else(|| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    println!("pilot lb listening on :{port}");
    axum::serve(listener, app).await.unwrap();
}

// --------------------------------------------------------- dashboard ----

const DASH: &str = r##"<!doctype html><meta charset="utf-8"><title>Streams pilot</title>
<style>body{font:14px system-ui;background:#0b0e14;color:#d8dee9;margin:24px}h1{font-size:18px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.card{background:#141926;border-radius:8px;padding:12px}.big{font-size:26px;font-weight:600}
.dim{color:#7b8496;font-size:12px}canvas{width:100%;height:260px;background:#141926;border-radius:8px}
.slp{opacity:0.45}</style>
<h1>Prisma Streams pilot (run 3): fleet-coordinated autoscaling</h1>
<div class="dim" id="ramp">loading</div>
<div class="grid" id="cards"></div>
<canvas id="chart" width="1200" height="260"></canvas>
<div class="dim">req/s per server from fleet heartbeats (stacked), 15-minute window. The fleet publishes its own desired count (fleet/desired.json); the LB routes to only that many servers — the rest go stale and scale to zero.</div>
<script>
const colors=["#5e81ac","#a3be8c","#ebcb8b","#bf616a"];
async function tick(){
  const d=await (await fetch("/stats")).json();
  const g=d.gen||{};
  document.getElementById("ramp").textContent=
    "fleet desired: "+(d.desired??"?")+" of "+d.upstreams+" | gen concurrency: "+(g.concurrency??"n/a")+
    " | achieved: "+(g.achievedPerSec??"n/a")+"/s | win p50 "+(g.winP50Ms??0).toFixed(0)+
    "ms p99 "+(g.winP99Ms??0).toFixed(0)+"ms | errs "+(g.errs??0)+
    " | updated "+new Date().toLocaleTimeString();
  document.getElementById("cards").innerHTML=(d.heartbeats||[]).map(function(s,i){
    const active=i<(d.desired||1);
    const state=s.live?(active?"ACTIVE":"live, leaving ring"):"SLEEPING";
    return '<div class="card'+(s.live?"":" slp")+'"><div class="dim">server '+(i+1)+' — '+state+'</div><div class="big">'+
    (s.live?s.rps.toFixed(0):"0")+' req/s</div>'+
    '<div class="dim">'+(s.live?('ack p50 '+(s.ackMs||0).toFixed(0)+'ms'):'heartbeat stale (scaled to zero)')+'</div></div>';
  }).join("");
  const c=document.getElementById("chart").getContext("2d");
  c.clearRect(0,0,1200,260);
  const h=d.history; if(!h.length) return;
  let max=4;
  h.forEach(function(p){max=Math.max(max,(p.hb||[]).reduce(function(a,b){return a+b},0))});
  const w=1200/Math.max(900,h.length);
  h.forEach(function(p,x){
    let y=260;
    (p.hb||[]).forEach(function(v,i){const hh=v/max*250;c.fillStyle=colors[i];c.fillRect(x*w,y-hh,Math.max(1,w-0.5),hh);y-=hh;});
  });
}
setInterval(tick,1000);tick();
</script>"##;
