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

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use futures_util::TryStreamExt;
use hdrhistogram::Histogram;
use object_store::ObjectStoreExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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
}

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

/// A client handle that is rebuilt every 60 s: the platform pins existing
/// keep-alive connections to whatever replica/version first accepted them,
/// so long-lived pools can stay stuck on stale replicas after a redeploy.
/// Rotating the client closes the pool and re-resolves within a minute.
#[derive(Clone)]
struct RotatingClient(Arc<Mutex<reqwest::Client>>);

impl RotatingClient {
    fn new() -> Self {
        let rc = RotatingClient(Arc::new(Mutex::new(client())));
        let inner = rc.0.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                *inner.lock().unwrap() = client();
            }
        });
        rc
    }
    fn get(&self) -> reqwest::Client {
        self.0.lock().unwrap().clone()
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        // http1_only is load-bearing: the platform edge negotiates h2 via
        // ALPN, and h2 multiplexes everything over ONE TCP connection per
        // host (bounded by the server's max-concurrent-streams and pinned
        // to a single LB replica) — measured throughput FELL as workers
        // doubled. HTTP/1.1 with a big pool gets one connection per
        // in-flight request and spreads across replicas.
        .http1_only()
        .pool_max_idle_per_host(8192)
        // <5 s: Compute suspends idle VMs after ~5 s and silently kills
        // flows; a pooled socket idle past that is a corpse the next
        // request eats. Same rule as the server's store client (RUNBOOK
        // §3.1). The 60 s client rotation handles replica pinning; this
        // handles dead sockets.
        .pool_idle_timeout(Duration::from_secs(4))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

#[tokio::main]
async fn main() {
    let mode = env("MODE").unwrap_or_else(|| std::env::args().nth(1).unwrap_or_default());
    match mode.as_str() {
        "lb" => lb().await,
        "gen" => generator().await,
        "bench" => bench().await,
        m => {
            eprintln!("unknown MODE '{m}' (want lb|gen)");
            std::process::exit(1);
        }
    }
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
}

struct Lb {
    upstreams: Vec<String>,
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
        })
        .collect();
    let lb = Arc::new(Lb {
        upstreams,
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
                let (prev_desired, prev_topo) = {
                    let f = lb.fleet.lock().unwrap();
                    (f.desired, f.topology.clone())
                };
                let mut view = FleetView {
                    desired: prev_desired,
                    heartbeats: Vec::new(),
                    active: Vec::new(),
                    topology: prev_topo,
                };
                if let Ok(r) = fstore
                    .get(&object_store::path::Path::from("fleet/desired.json"))
                    .await
                {
                    if let Ok(raw) = r.bytes().await {
                        if let Ok(d) = serde_json::from_slice::<serde_json::Value>(&raw) {
                            view.desired = (d["count"].as_u64().unwrap_or(1) as usize)
                                .clamp(1, lb.upstreams.len());
                        }
                    }
                }
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let mut ages_ms: Vec<i64> = Vec::new();
                for i in 0..lb.upstreams.len() {
                    let p = object_store::path::Path::from(format!("fleet/streams-{}.json", i + 1));
                    let mut entry = (0.0, 0.0, false, 0.0);
                    let mut age = i64::MAX;
                    if let Ok(r) = fstore.get(&p).await {
                        if let Ok(raw) = r.bytes().await {
                            if let Ok(h) = serde_json::from_slice::<serde_json::Value>(&raw) {
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
                        }
                    }
                    ages_ms.push(age);
                    view.heartbeats.push(entry);
                }
                // Ring active set: first `desired` ordinal instances minus
                // the >30s-dark (same rule as the servers' R2 check).
                // Fallback: everyone asleep → unfiltered, so the first
                // request wakes the ordinal owner.
                let d = view.desired.clamp(1, lb.upstreams.len());
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
                        let url = format!("{}/health", lb.upstreams[i - 1]);
                        let c = lb.http.get();
                        tokio::spawn(async move {
                            let _ = c.get(url).timeout(Duration::from_secs(20)).send().await;
                        });
                    }
                }
                if topo_age == 0 || view.topology.is_empty() {
                    if let Ok(r) = dstore
                        .get(&object_store::path::Path::from("topology.json"))
                        .await
                    {
                        if let Ok(raw) = r.bytes().await {
                            if let Ok(t) = serde_json::from_slice::<serde_json::Value>(&raw) {
                                if let Some(shards) = t["shards"].as_array() {
                                    view.topology = shards
                                        .iter()
                                        .filter_map(|s| s.as_str().map(String::from))
                                        .collect();
                                }
                            }
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
                        "upstreams": lb.upstreams.len(),
                        "stats": stats,
                        "gen": gv,
                        "desired": fleet.desired,
                        "heartbeats": fleet.heartbeats.iter().map(|(r, p50, l, cpu)| serde_json::json!({"rps": r, "ackMs": p50, "live": l, "cpu": cpu})).collect::<Vec<_>>(),
                        "topology": fleet.topology,
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

async fn proxy(State(lb): State<Arc<Lb>>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    let stream = match path
        .strip_prefix("/v1/stream/")
        .and_then(|r| r.split(['/', '?']).next())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return (StatusCode::NOT_FOUND, "lb: not a stream route").into_response(),
    };
    // COMPUTE-SPEC R1: route by shard (name-hash longest-prefix against the
    // topology), rendezvous over only the active set (first `desired`
    // upstreams) — instances beyond the desired count receive nothing and
    // scale to zero.
    let (active, shard) = {
        let f = lb.fleet.lock().unwrap();
        let shard = shard_for(&f.topology, &name_hash(&stream));
        let active = if f.active.is_empty() {
            vec!["streams-1".to_string()]
        } else {
            f.active.clone()
        };
        (active, shard)
    };
    // Rendezvous over instance NAMES from the live-filtered active set —
    // the identical computation the servers run for their R2 check.
    let chosen = &active[pick(&shard, &active)];
    let i = chosen
        .strip_prefix("streams-")
        .and_then(|n| n.parse::<usize>().ok())
        .and_then(|n| n.checked_sub(1))
        .filter(|n| *n < lb.upstreams.len())
        .unwrap_or(0);
    let s = &lb.stats[i];
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let url = format!("{}{}{}", lb.upstreams[i], path, query);
    let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes()).unwrap();
    let mut headers = HeaderMap::new();
    for (k, v) in req.headers() {
        let n = k.as_str();
        if n != "host" && n != "connection" && n != "content-length" && n != "transfer-encoding" {
            headers.insert(k.clone(), v.clone());
        }
    }
    let body = match axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response(),
    };

    let idle_ms = now_ms().saturating_sub(s.last_seen_ms.load(Ordering::Relaxed));
    let t0 = Instant::now();
    let http = lb.http.get();
    let mut resp = http
        .request(method.clone(), url)
        .headers(headers.clone())
        .body(body.clone())
        .send()
        .await;
    // R3: an instance that doesn't own the shard answers 409 with
    // Streams-Replay-To: <instance-name>; replay there without involving
    // the client (Fly-Replay pattern).
    if let Ok(r) = &resp {
        if r.status() == 409 {
            if let Some(target) = r
                .headers()
                .get("streams-replay-to")
                .and_then(|v| v.to_str().ok())
                .and_then(|n| n.strip_prefix("streams-"))
                .and_then(|n| n.parse::<usize>().ok())
                .and_then(|n| n.checked_sub(1))
                .filter(|n| *n < lb.upstreams.len())
            {
                let url2 = format!("{}{}{}", lb.upstreams[target], path, query);
                resp = http
                    .request(method, url2)
                    .headers(headers)
                    .body(body)
                    .send()
                    .await;
            }
        }
    }
    let us = t0.elapsed().as_micros() as u64;
    s.last_seen_ms.store(now_ms(), Ordering::Relaxed);
    match resp {
        Ok(r) => {
            s.reqs.fetch_add(1, Ordering::Relaxed);
            s.window.fetch_add(1, Ordering::Relaxed);
            s.last_us.store(us, Ordering::Relaxed);
            let prev = s.ewma_us.load(Ordering::Relaxed);
            s.ewma_us.store(
                if prev == 0 { us } else { (prev * 9 + us) / 10 },
                Ordering::Relaxed,
            );
            if idle_ms > 8000 && us > 1_500_000 {
                s.cold_starts.fetch_add(1, Ordering::Relaxed);
            }
            let mut out = Response::builder().status(r.status().as_u16());
            for (k, v) in r.headers() {
                let n = k.as_str();
                if n != "connection" && n != "transfer-encoding" {
                    out = out.header(k, v);
                }
            }
            out.body(Body::from_stream(
                r.bytes_stream().map_err(std::io::Error::other),
            ))
            .unwrap()
        }
        Err(e) => {
            s.errs.fetch_add(1, Ordering::Relaxed);
            (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
        }
    }
}

// --------------------------------------------------------------- gen ----

struct Gen {
    ok: AtomicU64,
    errs: AtomicU64,
    window: AtomicU64,
    achieved: AtomicU64,
    throttled: AtomicU64,
    concurrency: AtomicU64,
    hist: Mutex<Histogram<u64>>,
    // Windowed histogram, reset at each concurrency level so per-level
    // percentiles aren't polluted by boot cold-starts or earlier levels.
    hist_win: Mutex<Histogram<u64>>,
    last_err: Mutex<String>,
    start: Instant,
    // Per-upstream attribution: which server each stream's requests land
    // on, computed client-side with the same rendezvous hash the LB uses.
    per_up_window: Vec<AtomicU64>,
    per_up_rate: Vec<AtomicU64>,
}

async fn generator() {
    let auth = env("AUTH_TOKEN").expect("AUTH_TOKEN required");
    let key = env("STREAM_KEY").expect("STREAM_KEY required");
    // Route client-side (mimics production router, no LB hop) when
    // GEN_UPSTREAMS is set; otherwise send everything through LB_URL.
    // Deliberately NOT named UPSTREAMS: platform env vars merge across
    // deploys (and appear to leak between services at project scope), and
    // the LB legitimately sets UPSTREAMS — the generator silently flipping
    // into direct mode from inherited env caused two broken runs.
    let upstreams: Vec<String> = match env("GEN_UPSTREAMS") {
        Some(u) => u
            .split([',', ';'])
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect(),
        None => vec![env("LB_URL").expect("LB_URL or GEN_UPSTREAMS required")],
    };
    // Attribution-only server list (when routing via LB_URL): lets the
    // stats report a per-server split without bypassing the LB.
    let attr_upstreams: Vec<String> = env("ATTR_UPSTREAMS")
        .map(|u| {
            u.split([',', ';'])
                .filter(|s| !s.is_empty())
                .map(|s| s.trim().to_string())
                .collect()
        })
        .unwrap_or_default();
    let n_streams: usize = env("STREAMS").and_then(|v| v.parse().ok()).unwrap_or(32);
    let conc_start: u64 = env("CONC_START").and_then(|v| v.parse().ok()).unwrap_or(8);
    let conc_max: u64 = env("CONC_MAX").and_then(|v| v.parse().ok()).unwrap_or(4096);
    let ramp_secs: u64 = env("RAMP_SECS").and_then(|v| v.parse().ok()).unwrap_or(300);
    let batch: usize = env("BATCH").and_then(|v| v.parse().ok()).unwrap_or(1);
    // AWS-comparison knobs (bench/aws-comparison-plan.md): RECORD_PAD sizes
    // records (default 200 B); READ_EVERY mixes one read per N ops
    // (default 10; 0 = pure write so shapes match the awsbench arms).
    let read_every: u64 = env("READ_EVERY").and_then(|v| v.parse().ok()).unwrap_or(10);
    let record_pad: usize = env("RECORD_PAD").and_then(|v| v.parse().ok()).unwrap_or(200);
    // Distinct per-generator stream namespaces: multiple generators over
    // the same streams muddies closed-loop accounting and attribution.
    let stream_prefix: String = env("STREAM_PREFIX").unwrap_or_else(|| "pilot".into());
    let stream_prefix2 = stream_prefix.clone();

    let rc = RotatingClient::new();
    let http = rc.get();
    let base = |stream: &str| -> String {
        let i = if upstreams.len() == 1 {
            0
        } else {
            pick(stream, &upstreams)
        };
        upstreams[i].clone()
    };

    // Create the streams up front (idempotent; matches run-1 config).
    for i in 0..n_streams {
        let name = format!("{stream_prefix}-{i}");
        let r = http
            .put(format!("{}/v1/stream/{name}", base(&name)))
            .header("authorization", format!("Bearer {auth}"))
            .header("stream-encryption-key", key.clone())
            .header("content-type", "application/json")
            .send()
            .await;
        if let Err(e) = r {
            eprintln!("create {name}: {e}");
        }
    }
    println!(
        "pilot gen: {} stream(s), conc {}→{} doubling every {}s, batch {}, {} target(s)",
        n_streams,
        conc_start,
        conc_max,
        ramp_secs,
        batch,
        upstreams.len()
    );

    let attr_n = attr_upstreams.len().max(1);
    let g = Arc::new(Gen {
        ok: AtomicU64::new(0),
        errs: AtomicU64::new(0),
        window: AtomicU64::new(0),
        achieved: AtomicU64::new(0),
        throttled: AtomicU64::new(0),
        concurrency: AtomicU64::new(0),
        hist: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
        hist_win: Mutex::new(Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()),
        last_err: Mutex::new(String::new()),
        start: Instant::now(),
        per_up_window: (0..attr_n).map(|_| AtomicU64::new(0)).collect(),
        per_up_rate: (0..attr_n).map(|_| AtomicU64::new(0)).collect(),
    });

    // 1s ticker: achieved/s window.
    {
        let g = g.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                g.achieved
                    .store(g.window.swap(0, Ordering::Relaxed), Ordering::Relaxed);
                for i in 0..g.per_up_window.len() {
                    g.per_up_rate[i].store(
                        g.per_up_window[i].swap(0, Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                }
            }
        });
    }

    // Controller: step concurrency up every ramp_secs, spawning workers.
    {
        let g = g.clone();
        let rc = rc.clone();
        let upstreams = upstreams.clone();
        let attr_upstreams = attr_upstreams.clone();
        tokio::spawn(async move {
            let mut spawned: u64 = 0;
            let seq = Arc::new(AtomicU64::new(0));
            loop {
                let level = g.start.elapsed().as_secs() / ramp_secs;
                let desired = conc_max.min(conc_start.saturating_mul(1 << level.min(30)));
                if g.concurrency.swap(desired, Ordering::Relaxed) != desired {
                    g.hist_win.lock().unwrap().reset();
                }
                while spawned < desired {
                    spawned += 1;
                    let g = g.clone();
                    let rc = rc.clone();
                    let upstreams = upstreams.clone();
                    let attr_upstreams = attr_upstreams.clone();
                    let auth = auth.clone();
                    let key = key.clone();
                    let seq = seq.clone();
                    let stream_prefix2 = stream_prefix2.clone();
                    let (read_every, pad_n) = (read_every, record_pad);
                    tokio::spawn(async move {
                        loop {
                            let n = seq.fetch_add(1, Ordering::Relaxed);
                            let name = format!("{}-{}", stream_prefix2, n as usize % n_streams);
                            let i = if upstreams.len() == 1 {
                                0
                            } else {
                                pick(&name, &upstreams)
                            };
                            let attr_i = if attr_upstreams.is_empty() {
                                i
                            } else {
                                pick(&name, &attr_upstreams)
                            };
                            let t0 = Instant::now();
                            let http = rc.get();
                            let res = if read_every > 0 && n % read_every == read_every - 1 {
                                http.get(format!("{}/v1/stream/{name}?offset=now", upstreams[i]))
                                    .header("authorization", format!("Bearer {auth}"))
                                    .header("stream-encryption-key", key.clone())
                                    .send()
                                    .await
                            } else {
                                let recs: Vec<serde_json::Value> = (0..batch)
                                    .map(|b| serde_json::json!({"i": n, "b": b, "t": now_ms(), "pad": "x".repeat(pad_n)}))
                                    .collect();
                                http.post(format!("{}/v1/stream/{name}", upstreams[i]))
                                    .header("authorization", format!("Bearer {auth}"))
                                    .header("stream-encryption-key", key.clone())
                                    .header("content-type", "application/json")
                                    .json(&recs)
                                    .send()
                                    .await
                            };
                            match res {
                                Ok(r) if r.status().is_success() => {
                                    let _ = r.bytes().await;
                                    g.ok.fetch_add(1, Ordering::Relaxed);
                                    g.window.fetch_add(1, Ordering::Relaxed);
                                    let ai = attr_i.min(g.per_up_window.len().saturating_sub(1));
                                    g.per_up_window[ai].fetch_add(1, Ordering::Relaxed);
                                    let us = t0.elapsed().as_micros() as u64;
                                    let _ = g.hist.lock().unwrap().record(us);
                                    let _ = g.hist_win.lock().unwrap().record(us);
                                }
                                // §12.2 client contract: back off on 429/503,
                                // honoring Retry-After with jitter. Without
                                // this, closed-loop workers retry instantly
                                // and admission control becomes a reject
                                // storm that starves the whole instance
                                // (docker staircase, 2026-07-15: 2.7M 429s,
                                // goodput ~1/s, /health unresponsive).
                                Ok(r)
                                    if r.status().as_u16() == 429 || r.status().as_u16() == 503 =>
                                {
                                    g.throttled.fetch_add(1, Ordering::Relaxed);
                                    let ra_ms = r
                                        .headers()
                                        .get("retry-after")
                                        .and_then(|v| v.to_str().ok())
                                        .and_then(|v| v.parse::<u64>().ok())
                                        .map(|secs| secs * 1000)
                                        .unwrap_or(500);
                                    let jitter = (n % 400) as u64;
                                    tokio::time::sleep(Duration::from_millis(ra_ms + jitter)).await;
                                }
                                Ok(r) => {
                                    g.errs.fetch_add(1, Ordering::Relaxed);
                                    *g.last_err.lock().unwrap() =
                                        format!("status {} on {name}", r.status());
                                }
                                Err(e) => {
                                    g.errs.fetch_add(1, Ordering::Relaxed);
                                    *g.last_err.lock().unwrap() = format!("{e}");
                                }
                            }
                        }
                    });
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    let app = Router::new()
        .route(
            "/",
            get(|State(g): State<Arc<Gen>>| async move {
                let (win_p50, win_p99, win_n) = {
                    let hw = g.hist_win.lock().unwrap();
                    (
                        hw.value_at_quantile(0.5) as f64 / 1000.0,
                        hw.value_at_quantile(0.99) as f64 / 1000.0,
                        hw.len(),
                    )
                };
                let h = g.hist.lock().unwrap();
                let per_up: Vec<u64> = g
                    .per_up_rate
                    .iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();
                let json = serde_json::json!({
                    "mode": "closed-loop",
                    "winP50Ms": win_p50,
                    "winP99Ms": win_p99,
                    "winSamples": win_n,
                    "concurrency": g.concurrency.load(Ordering::Relaxed),
                    "achievedPerSec": g.achieved.load(Ordering::Relaxed),
                    "perUpstreamPerSec": per_up,
                    "ok": g.ok.load(Ordering::Relaxed),
                    "errs": g.errs.load(Ordering::Relaxed),
                    "throttled": g.throttled.load(Ordering::Relaxed),
                    "meanMs": h.mean() / 1000.0,
                    "p50Ms": h.value_at_quantile(0.5) as f64 / 1000.0,
                    "p99Ms": h.value_at_quantile(0.99) as f64 / 1000.0,
                    "maxMs": h.max() as f64 / 1000.0,
                    "elapsedMin": g.start.elapsed().as_secs_f64() / 60.0,
                    "lastErr": g.last_err.lock().unwrap().clone(),
                });
                drop(h);
                ([("access-control-allow-origin", "*")], axum::Json(json))
            }),
        )
        .with_state(g);

    let port = env("PORT").unwrap_or_else(|| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    println!("pilot gen stats on :{port}");
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

// ------------------------------------------------------------- bench ----
// Single-stream ceiling sweep: how does one totally-ordered stream's
// throughput respond to (1) event size and (2) events per request?
// Closed-loop workers against ONE stream on ONE dedicated server (no LB,
// no fleet, one shard) — measures the commit pipeline itself.

#[derive(Clone, Copy, Debug)]
struct BenchPoint {
    sweep: &'static str, // "size" | "batch"
    event_bytes: usize,
    batch: usize,
}

struct BenchState {
    // live counters for the CURRENT point
    ok: AtomicU64,
    errs: AtomicU64,
    throttles: AtomicU64,
    bytes: AtomicU64,
    hist: Mutex<Histogram<u64>>,
    gen_id: AtomicU64, // bumped per point; workers re-read config
    point: Mutex<Option<BenchPoint>>,
    conc: AtomicU64,
    results: Mutex<Vec<serde_json::Value>>,
    done: AtomicU64,
}

async fn bench() {
    let target = env("TARGET").expect("TARGET (server base url)");
    let auth = env("AUTH_TOKEN").expect("AUTH_TOKEN");
    let key = env("STREAM_KEY").expect("STREAM_KEY");
    let stream = env("BENCH_STREAM").unwrap_or_else(|| "bench-ordered".into());
    let warmup = Duration::from_secs(env("WARMUP_SECS").and_then(|v| v.parse().ok()).unwrap_or(8));
    let measure = Duration::from_secs(
        env("MEASURE_SECS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(40),
    );
    let max_inflight_mb: usize = env("MAX_INFLIGHT_MB")
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let drain = Duration::from_secs(
        env("INTER_POINT_SECS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    );
    let fixed_size: usize = env("FIXED_SIZE")
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);

    let sizes: Vec<usize> = env("SIZES")
        .unwrap_or_else(|| "64;256;1024;4096;16384;65536;262144;1048576".into())
        .split(';')
        .filter_map(|v| v.trim().parse().ok())
        .collect();
    let batches: Vec<usize> = env("BATCHES")
        .unwrap_or_else(|| "1;4;16;64;256;1024;4096".into())
        .split(';')
        .filter_map(|v| v.trim().parse().ok())
        .collect();

    let mut plan: Vec<BenchPoint> = Vec::new();
    for s in &sizes {
        plan.push(BenchPoint {
            sweep: "size",
            event_bytes: *s,
            batch: 1,
        });
    }
    for b in &batches {
        plan.push(BenchPoint {
            sweep: "batch",
            event_bytes: fixed_size,
            batch: *b,
        });
    }

    let rc = RotatingClient::new();
    // Create the stream (single, totally ordered, JSON so top-level-array
    // batching exercises the standard batch path).
    let create = rc
        .get()
        .put(format!("{target}/v1/stream/{stream}"))
        .header("authorization", format!("Bearer {auth}"))
        .header("stream-encryption-key", key.clone())
        .header("content-type", "application/json")
        .send()
        .await;
    println!("bench stream create: {:?}", create.map(|r| r.status()));
    // Pre-warm: a scale-from-0 target wakes with several seconds of shard
    // opening; a cold burst then trips the stall guard and permanently
    // collapses the first point. Hold the plan until appends succeed.
    for attempt in 0u32.. {
        let ok = rc
            .get()
            .post(format!("{target}/v1/stream/{stream}"))
            .header("authorization", format!("Bearer {auth}"))
            .header("stream-encryption-key", key.clone())
            .header("content-type", "application/json")
            .timeout(Duration::from_secs(30))
            .body("[{\"p\":\"warm\"}]")
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        if ok {
            println!("bench target warm after {attempt} retries");
            break;
        }
        if attempt > 40 {
            println!("bench target NOT warm after 40 retries; proceeding anyway");
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    let st = Arc::new(BenchState {
        ok: AtomicU64::new(0),
        errs: AtomicU64::new(0),
        throttles: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        hist: Mutex::new(Histogram::new_with_bounds(1, 300_000_000, 3).unwrap()),
        gen_id: AtomicU64::new(0),
        point: Mutex::new(None),
        conc: AtomicU64::new(0),
        results: Mutex::new(Vec::new()),
        done: AtomicU64::new(0),
    });

    // Worker pool: spawn the max we could ever need; workers park when
    // their id >= current concurrency.
    let max_workers: usize = env("MAX_WORKERS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    for wid in 0..max_workers {
        let st = st.clone();
        let rc = rc.clone();
        let target = target.clone();
        let auth = auth.clone();
        let key = key.clone();
        let stream = stream.clone();
        tokio::spawn(async move {
            let mut body: bytes::Bytes = bytes::Bytes::new();
            let mut my_gen = u64::MAX;
            let mut point = BenchPoint {
                sweep: "size",
                event_bytes: 0,
                batch: 0,
            };
            loop {
                let cur_gen = st.gen_id.load(Ordering::Relaxed);
                if cur_gen != my_gen {
                    my_gen = cur_gen;
                    let p = *st.point.lock().unwrap();
                    let Some(p) = p else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        my_gen = u64::MAX;
                        continue;
                    };
                    point = p;
                    let _ = &point; // silence unused when only body matters
                    // Pre-build the request body: JSON array of `batch`
                    // records, each ~event_bytes serialized.
                    let pad = "x".repeat(point.event_bytes.saturating_sub(8).max(1));
                    let rec = format!("{{\"p\":\"{pad}\"}}");
                    let mut b = String::with_capacity((rec.len() + 1) * point.batch + 2);
                    b.push('[');
                    for i in 0..point.batch {
                        if i > 0 {
                            b.push(',');
                        }
                        b.push_str(&rec);
                    }
                    b.push(']');
                    body = bytes::Bytes::from(b.into_bytes());
                }
                if wid as u64 >= st.conc.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                let t0 = Instant::now();
                let verb = std::env::var("BENCH_VERB").unwrap_or_default();
                let res = match verb.as_str() {
                    "health" => {
                        rc.get()
                            .get(format!("{target}/health"))
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                    }
                    "sleep" => {
                        let ms = std::env::var("SLEEP_MS").unwrap_or_else(|_| "100".into());
                        rc.get()
                            .get(format!("{target}/v1/debug/sleep?ms={ms}"))
                            .header("authorization", format!("Bearer {auth}"))
                            .timeout(Duration::from_secs(120))
                            .send()
                            .await
                    }
                    _ => {
                        rc.get()
                            .post(format!("{target}/v1/stream/{stream}"))
                            .header("authorization", format!("Bearer {auth}"))
                            .header("stream-encryption-key", key.clone())
                            .header("content-type", "application/json")
                            .timeout(Duration::from_secs(120))
                            .body(reqwest::Body::from(body.clone()))
                            .send()
                            .await
                    }
                };
                match res {
                    Ok(r) if r.status().is_success() => {
                        let _ = r.bytes().await;
                        st.ok.fetch_add(1, Ordering::Relaxed);
                        st.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                        let us = t0.elapsed().as_micros().min(u64::MAX as u128) as u64;
                        let _ = st.hist.lock().unwrap().record(us.max(1));
                    }
                    Ok(r) if r.status().as_u16() == 429 || r.status().as_u16() == 503 => {
                        st.throttles.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(r) => {
                        let n = st.errs.fetch_add(1, Ordering::Relaxed);
                        if n < 3 {
                            let status = r.status();
                            let body = r.text().await.unwrap_or_default();
                            let body = &body[..body.len().min(200)];
                            println!("bench err[{n}]: HTTP {status}: {body}");
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => {
                        let n = st.errs.fetch_add(1, Ordering::Relaxed);
                        if n < 3 {
                            println!("bench err[{n}]: transport: {e}");
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
    }

    // Controller: walk the plan.
    {
        let st = st.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            for p in plan {
                let body_bytes = (p.event_bytes + 12) * p.batch;
                // Probe modes (BENCH_VERB=health|sleep): BATCHES is a raw
                // concurrency ladder, not a body multiplier.
                let probe = std::env::var("BENCH_VERB").ok().filter(|v| v != "append");
                let conc = if probe.is_some() {
                    p.batch.clamp(1, max_workers)
                } else {
                    (max_inflight_mb * 1024 * 1024 / body_bytes.max(1)).clamp(4, max_workers)
                };
                *st.point.lock().unwrap() = Some(p);
                st.gen_id.fetch_add(1, Ordering::Relaxed);
                println!(
                    "bench point start: sweep={} event={}B batch={} conc={conc}",
                    p.sweep, p.event_bytes, p.batch
                );
                // Ramp concurrency across the warmup: stepping straight to
                // full conc lands one synchronized burst on a cold pipeline
                // and trips backpressure before steady state can form.
                let steps = 6u32;
                for i in 1..=steps {
                    st.conc.store(
                        (conc as u64 * i as u64 / steps as u64).max(4),
                        Ordering::Relaxed,
                    );
                    tokio::time::sleep(warmup / steps).await;
                }
                // reset measurement window
                st.ok.store(0, Ordering::Relaxed);
                st.errs.store(0, Ordering::Relaxed);
                st.throttles.store(0, Ordering::Relaxed);
                st.bytes.store(0, Ordering::Relaxed);
                st.hist.lock().unwrap().reset();
                let t0 = Instant::now();
                // Stall guard: a collapsed point (zero completions, error
                // churn) is recorded and aborted early instead of deepening
                // the hole for the rest of the sweep.
                let mut collapsed = false;
                // Windowed progress: long sustained points need a time
                // series, not just the final aggregate — degradation over
                // time is the whole question.
                let (mut w_ok, mut w_err, mut w_bytes) = (0u64, 0u64, 0u64);
                let mut w_t = Instant::now();
                while t0.elapsed() < measure {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    if t0.elapsed() >= Duration::from_secs(45)
                        && st.ok.load(Ordering::Relaxed) == 0
                        && st.errs.load(Ordering::Relaxed) > 50
                    {
                        collapsed = true;
                        break;
                    }
                    if w_t.elapsed() >= Duration::from_secs(15) {
                        let (ok, err, by) = (
                            st.ok.load(Ordering::Relaxed),
                            st.errs.load(Ordering::Relaxed),
                            st.bytes.load(Ordering::Relaxed),
                        );
                        let dt = w_t.elapsed().as_secs_f64();
                        println!(
                            "bench window t={:>5.0}s: {:.0} req/s {:.0} ev/s {:.2} MB/s errs+{}",
                            t0.elapsed().as_secs_f64(),
                            (ok - w_ok) as f64 / dt,
                            ((ok - w_ok) as usize * p.batch) as f64 / dt,
                            (by - w_bytes) as f64 / dt / 1e6,
                            err - w_err
                        );
                        (w_ok, w_err, w_bytes) = (ok, err, by);
                        w_t = Instant::now();
                    }
                }
                let dt = t0.elapsed().as_secs_f64();
                let ok = st.ok.load(Ordering::Relaxed);
                let bytes = st.bytes.load(Ordering::Relaxed);
                let (p50, p99) = {
                    let h = st.hist.lock().unwrap();
                    (
                        h.value_at_quantile(0.5) as f64 / 1000.0,
                        h.value_at_quantile(0.99) as f64 / 1000.0,
                    )
                };
                let row = serde_json::json!({
                    "collapsed": collapsed,
                    "sweep": p.sweep,
                    "event_bytes": p.event_bytes,
                    "batch": p.batch,
                    "conc": conc,
                    "secs": dt,
                    "requests_per_s": ok as f64 / dt,
                    "events_per_s": (ok as usize * p.batch) as f64 / dt,
                    "mb_per_s": bytes as f64 / dt / 1e6,
                    "p50_ms": p50,
                    "p99_ms": p99,
                    "errs": st.errs.load(Ordering::Relaxed),
                    "throttles": st.throttles.load(Ordering::Relaxed),
                });
                println!("bench point done: {row}");
                st.results.lock().unwrap().push(row);
                // Drain pause: let L0/compaction backlog clear so the next
                // point measures steady state, not the previous point's debt.
                st.conc.store(0, Ordering::Relaxed);
                tokio::time::sleep(if collapsed { drain * 3 } else { drain }).await;
            }
            st.conc.store(0, Ordering::Relaxed);
            st.done.store(1, Ordering::Relaxed);
            println!("bench sweep COMPLETE");
        });
    }

    let app = Router::new()
        .route(
            "/",
            get(|State(st): State<Arc<BenchState>>| async move {
                let results = st.results.lock().unwrap().clone();
                (
                    [("access-control-allow-origin", "*")],
                    axum::Json(serde_json::json!({
                        "done": st.done.load(Ordering::Relaxed) == 1,
                        "points": results.len(),
                        "results": results,
                    })),
                )
            }),
        )
        .with_state(st);
    let port = env("PORT").unwrap_or_else(|| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    println!("bench results on :{port}");
    axum::serve(listener, app).await.unwrap();
}
