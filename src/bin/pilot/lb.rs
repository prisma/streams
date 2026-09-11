//! MODE=lb: the pilot's emulated platform edge. A rendezvous-hash reverse
//! proxy that follows the fleet's desired count, heartbeats, topology and
//! rebalancer overrides, wakes sleeping ordinals, and publishes what it
//! observes — client-experienced latency and delivered rps — for the
//! dashboard and the servers' scaler.
use super::{
    DASH, FleetView, Lb, RotatingClient, UpStat, client, env, epoch_millis, fleet_store, now_ms, proxy,
};
use axum::Router;
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// The router's one process-wide report of every daemon it owns; each
/// runs until the process exits, so there is nothing to join.
pub(super) async fn run() {
    let lb = Lb::from_env();
    let n_up = lb.stats.len();
    spawn_load_report(lb.clone());
    spawn_fleet_poller(lb.clone(), n_up);
    spawn_ticker(lb.clone());
    serve(lb).await;
}

/// A router input without which nothing can be routed or reported.
fn required(key: &str) -> String {
    let Some(value) = env(key) else {
        eprintln!("lb: {key} required");
        std::process::exit(2);
    };
    value
}

fn path(name: &str) -> object_store::path::Path {
    object_store::path::Path::from(name)
}

/// One JSON document from the store, or None when it is absent,
/// unreadable or malformed.
async fn read_json(store: &Arc<dyn ObjectStore>, name: &str) -> Option<Value> {
    let raw = store.get(&path(name)).await.ok()?.bytes().await.ok()?;
    serde_json::from_slice(&raw).ok()
}

impl Lb {
    fn from_env() -> Arc<Self> {
        let upstreams: Vec<String> = required("UPSTREAMS")
            .split([',', ';'])
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        let stats = upstreams.iter().map(|_| UpStat::default()).collect();
        Arc::new(Lb {
            upstreams: std::sync::RwLock::new(upstreams),
            stats,
            history: Mutex::new(VecDeque::new()),
            gen_stats: Mutex::new(json!(null)),
            fleet: Mutex::new(FleetView {
                desired: 1,
                ..Default::default()
            }),
            http: RotatingClient::new(),
        })
    }
}

// ---------------------------------------------------------- load report --

/// Router load report: the servers' ack latency cannot see edge-side
/// queueing (run 7: clients at p50 1.6-2 s while server acks sat at
/// 60-80 ms and the fleet SHRANK mid-congestion). Publish what the
/// router observes — client-experienced latency — to the fleet prefix;
/// the servers fold it into desired-count.
#[expect(
    clippy::disallowed_methods,
    reason = "lb::spawn_load_report; the report daemon lives as long as the router process and produces nothing to join; a supervisor would only hold a handle that ends with the process"
)]
fn spawn_load_report(lb: Arc<Lb>) {
    let rstore = fleet_store(&required("FLEET_PREFIX"));
    let router_name = env("ROUTER_NAME").unwrap_or_else(|| "router-1".into());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let body = lb.load_report(&router_name);
            let payload = object_store::PutPayload::from(body.to_string().into_bytes());
            // The report is advisory: a failed put is superseded by the
            // next tick's.
            if let Err(e) = rstore
                .put(&path(&format!("routers/{router_name}.json")), payload)
                .await
            {
                eprintln!("lb: load report not published: {e}");
            }
        }
    });
}

impl Lb {
    /// The worst client-experienced latency among upstreams seen in the
    /// last 10 s.
    fn load_report(&self, router_name: &str) -> Value {
        let worst_ewma_us = self
            .stats
            .iter()
            .filter(|s| now_ms().saturating_sub(s.last_seen_ms.load(Ordering::Relaxed)) < 10_000)
            .map(|s| s.ewma_us.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0);
        json!({
            "router": router_name,
            "ts_ms": now_ms(),
            "client_p50_ms": worst_ewma_us as f64 / 1000.0,
        })
    }
}

// ---------------------------------------------------------- fleet poll ---

/// Fleet poller: desired.json + heartbeats every 2 s, topology every 60 s.
/// The LB emulates the platform: it routes to only the first `desired`
/// upstreams, so the rest idle and scale to zero.
#[expect(
    clippy::disallowed_methods,
    reason = "lb::spawn_fleet_poller; the poller lives as long as the router process and produces nothing to join; a supervisor would only hold a handle that ends with the process"
)]
fn spawn_fleet_poller(lb: Arc<Lb>, n_up: usize) {
    let fstore = fleet_store(&required("FLEET_PREFIX"));
    let dstore = fleet_store(&required("DATA_PREFIX"));
    tokio::spawn(async move {
        let mut topo_age = 0u32;
        loop {
            lb.poll_fleet(&fstore, &dstore, n_up, topo_age == 0).await;
            topo_age = (topo_age + 1) % 30;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

impl Lb {
    /// One poll: rebuild the fleet view from the store and publish it.
    #[expect(
        clippy::unwrap_used,
        reason = "lb::Lb::poll_fleet; a poisoned fleet view may hold a half-published poll; recovering it could route by a view no poll completed"
    )]
    async fn poll_fleet(
        &self,
        fstore: &Arc<dyn ObjectStore>,
        dstore: &Arc<dyn ObjectStore>,
        n_up: usize,
        refresh_topology: bool,
    ) {
        // Single guard: two lock() temporaries in one expression would
        // self-deadlock the std Mutex.
        let mut view = {
            let f = self.fleet.lock().unwrap();
            FleetView {
                desired: f.desired,
                heartbeats: Vec::new(),
                active: Vec::new(),
                topology: f.topology.clone(),
                overrides: f.overrides.clone(),
            }
        };
        if let Some(d) = read_json(fstore, "fleet/desired.json").await {
            let count = usize::try_from(d["count"].as_u64().unwrap_or(1)).unwrap_or(1);
            view.desired = count.clamp(1, n_up);
        }
        read_overrides(fstore, &mut view).await;
        let ages_ms = poll_heartbeats(fstore, n_up, &mut view).await;
        let d = view.desired.clamp(1, n_up);
        view.active = active_ring(d, &ages_ms);
        self.wake_stale_ordinals(d, &ages_ms);
        if (refresh_topology || view.topology.is_empty())
            && let Some(t) = read_json(dstore, "topology.json").await
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
        if let Some(published) = read_json(fstore, "fleet/urls.json").await
            && let Ok(published) = serde_json::from_value::<HashMap<String, String>>(published)
        {
            self.adopt_urls(published);
        }
        *self.fleet.lock().unwrap() = view;
    }

    /// Platform emulation: on real infrastructure, scale-out means the
    /// platform STARTS instance N+1. Here an instance starts on first
    /// request — but the live-set ring only routes to heartbeating
    /// instances, so a newly-desired sleeping ordinal would deadlock dark
    /// (found in run 5: desired=4, live=1 forever). Ping desired-but-stale
    /// ordinals out of band; one /health GET wakes them.
    #[expect(
        clippy::disallowed_methods,
        reason = "lb::Lb::wake_stale_ordinals; a wake ping is fire-and-forget: the request itself starts a sleeping ordinal and its response carries nothing; joining it would stall the poll on every cold start"
    )]
    #[expect(
        clippy::let_underscore_must_use,
        reason = "lb::Lb::wake_stale_ordinals; the ping's outcome is irrelevant once the request has left; a cold or dark instance answers nothing worth recording"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "lb::Lb::wake_stale_ordinals; a poisoned upstream table may hold a half-adopted URL; recovering it could ping an address no deploy published"
    )]
    fn wake_stale_ordinals(&self, d: usize, ages_ms: &[i64]) {
        for i in 1..=d {
            if ages_ms.get(i - 1).map(|a| *a >= 8_000).unwrap_or(true) {
                let url = format!("{}/health", self.upstreams.read().unwrap()[i - 1]);
                let c = self.http.get();
                tokio::spawn(async move {
                    let _ = c.get(url).timeout(Duration::from_secs(20)).send().await;
                });
            }
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "lb::Lb::adopt_urls; a poisoned upstream table may hold a half-adopted URL; recovering it could route to an address no deploy published"
    )]
    fn adopt_urls(&self, published: HashMap<String, String>) {
        let mut ups = self.upstreams.write().unwrap();
        for (name, url) in published {
            let Some(i) = ordinal_index(&name) else {
                continue;
            };
            if i < ups.len() && !url.is_empty() && ups[i] != url {
                println!("lb: upstream {name} -> {url}");
                ups[i] = url;
            }
        }
    }
}

/// `streams-N` names the N-th (1-based) upstream.
fn ordinal_index(name: &str) -> Option<usize> {
    name.strip_prefix("streams-")?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}

/// Rebalancer overrides: a successful read replaces the map (absent file
/// = no overrides); a transient error keeps the previous view rather
/// than flapping routing on a blip.
async fn read_overrides(store: &Arc<dyn ObjectStore>, view: &mut FleetView) {
    match store.get(&path("fleet/overrides.json")).await {
        Ok(r) => {
            if let Ok(raw) = r.bytes().await
                && let Ok(o) = serde_json::from_slice::<Value>(&raw)
            {
                view.overrides = overrides_of(&o);
            }
        }
        Err(object_store::Error::NotFound { .. }) => view.overrides.clear(),
        Err(_) => {}
    }
}

fn overrides_of(o: &Value) -> HashMap<String, String> {
    o["entries"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v["to"].as_str().map(|t| (k.clone(), t.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Every ordinal's heartbeat, as (rps, ack_p50_ms, live, cpu_pct) in the
/// view plus its age in ms (i64::MAX when unreadable).
async fn poll_heartbeats(
    store: &Arc<dyn ObjectStore>,
    n_up: usize,
    view: &mut FleetView,
) -> Vec<i64> {
    let now_ms = i64::try_from(epoch_millis(SystemTime::now())).unwrap_or(i64::MAX);
    let mut ages_ms = Vec::with_capacity(n_up);
    for i in 0..n_up {
        let heartbeat = read_json(store, &format!("fleet/streams-{}.json", i + 1)).await;
        let (entry, age) = heartbeat_entry(heartbeat.as_ref(), now_ms);
        ages_ms.push(age);
        view.heartbeats.push(entry);
    }
    ages_ms
}

fn heartbeat_entry(heartbeat: Option<&Value>, now_ms: i64) -> ((f64, f64, bool, f64), i64) {
    let Some(h) = heartbeat else {
        return ((0.0, 0.0, false, 0.0), i64::MAX);
    };
    let age = now_ms - h["ts_ms"].as_i64().unwrap_or(0);
    let live = age < 10_000;
    let gauge = |key: &str| if live { h[key].as_f64().unwrap_or(0.0) } else { 0.0 };
    ((gauge("rps"), gauge("ack_p50_ms"), live, gauge("cpu_pct")), age)
}

/// Ring active set: first `desired` ordinal instances minus the >30s-dark
/// (same rule as the servers' R2 check). Fallback: everyone asleep →
/// unfiltered, so the first request wakes the ordinal owner.
fn active_ring(d: usize, ages_ms: &[i64]) -> Vec<String> {
    let active: Vec<String> = (1..=d)
        .filter(|i| ages_ms.get(i - 1).map(|a| *a < 30_000).unwrap_or(false))
        .map(|i| format!("streams-{i}"))
        .collect();
    if active.is_empty() {
        return (1..=d).map(|i| format!("streams-{i}")).collect();
    }
    active
}

// ------------------------------------------------------------- ticker ---

/// 1s ticker: roll the per-upstream request window into history and
/// poll the generator's stats endpoint for the dashboard header.
#[expect(
    clippy::disallowed_methods,
    reason = "lb::spawn_ticker; the ticker lives as long as the router process and produces nothing to join; a supervisor would only hold a handle that ends with the process"
)]
fn spawn_ticker(lb: Arc<Lb>) {
    let gen_url = env("GEN_STATS_URL");
    tokio::spawn(async move {
        let poll = client();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let gv = generator_stats(&poll, gen_url.as_deref()).await;
            lb.record_tick(gv);
        }
    });
}

async fn generator_stats(poll: &reqwest::Client, url: Option<&str>) -> Value {
    let Some(u) = url else {
        return json!(null);
    };
    match poll.get(u).timeout(Duration::from_millis(1500)).send().await {
        Ok(r) => r.json::<Value>().await.unwrap_or(json!(null)),
        Err(_) => json!(null),
    }
}

impl Lb {
    #[expect(
        clippy::unwrap_used,
        reason = "lb::Lb::record_tick; a poisoned history, generator or fleet view may hold a half-written sample; recovering it could chart a tick no poll completed"
    )]
    fn record_tick(&self, gv: Value) {
        let per: Vec<u64> = self
            .stats
            .iter()
            .map(|s| s.window.swap(0, Ordering::Relaxed))
            .collect();
        *self.gen_stats.lock().unwrap() = gv.clone();
        let fleet = self.fleet.lock().unwrap().clone();
        let hb_rps: Vec<f64> = fleet.heartbeats.iter().map(|(r, _, _, _)| *r).collect();
        let hb_live: Vec<bool> = fleet.heartbeats.iter().map(|(_, _, l, _)| *l).collect();
        let mut h = self.history.lock().unwrap();
        h.push_back(json!({
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

    /// The dashboard's snapshot: per-upstream counters, generator stats,
    /// the fleet view and the rolling history.
    #[expect(
        clippy::unwrap_used,
        reason = "lb::Lb::stats_json; a poisoned history, generator or fleet view may hold a half-written sample; recovering it could report a tick no poll completed"
    )]
    fn stats_json(&self) -> Value {
        let stats: Vec<Value> = self.stats.iter().map(upstream_json).collect();
        let history: Vec<Value> = self.history.lock().unwrap().iter().cloned().collect();
        let gv = self.gen_stats.lock().unwrap().clone();
        let fleet = self.fleet.lock().unwrap().clone();
        let heartbeats: Vec<Value> = fleet
            .heartbeats
            .iter()
            .map(|(r, p50, l, cpu)| json!({"rps": r, "ackMs": p50, "live": l, "cpu": cpu}))
            .collect();
        json!({
            "upstreams": self.stats.len(),
            "stats": stats,
            "gen": gv,
            "desired": fleet.desired,
            "heartbeats": heartbeats,
            "topology": fleet.topology,
            "overrides": fleet.overrides,
            "history": history,
        })
    }
}

fn upstream_json(s: &UpStat) -> Value {
    json!({
        "reqs": s.reqs.load(Ordering::Relaxed),
        "errs": s.errs.load(Ordering::Relaxed),
        "ewmaMs": s.ewma_us.load(Ordering::Relaxed) as f64 / 1000.0,
        "lastMs": s.last_us.load(Ordering::Relaxed) as f64 / 1000.0,
        "coldStarts": s.cold_starts.load(Ordering::Relaxed),
        "replays": s.replays.load(Ordering::Relaxed),
        "unmarked": s.unmarked.load(Ordering::Relaxed),
        "ejected": s.eject_until_ms.load(Ordering::Relaxed) > now_ms(),
    })
}

// -------------------------------------------------------------- serve ---

async fn stats(State(lb): State<Arc<Lb>>) -> ([(&'static str, &'static str); 1], axum::Json<Value>) {
    ([("access-control-allow-origin", "*")], axum::Json(lb.stats_json()))
}

#[expect(
    clippy::unwrap_used,
    reason = "lb::serve; binding the listen port and serving are the router's last acts and have nothing to fall back to; a mapped error would only restate the panic"
)]
async fn serve(lb: Arc<Lb>) {
    let app = Router::new()
        .route("/", get(|| async { Html(DASH) }))
        .route("/stats", get(stats))
        .fallback(proxy)
        .with_state(lb);
    let port = env("PORT").unwrap_or_else(|| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    println!("pilot lb listening on :{port}");
    axum::serve(listener, app).await.unwrap();
}
