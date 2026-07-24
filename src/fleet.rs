//! Fleet coordination (COMPUTE-SPEC §2/§4, pilot-scaled).
//!
//! Every instance heartbeats `fleet/<instance>.json` every 2 s with its load
//! vector, derives the live set from heartbeat freshness (<10 s), and
//! recomputes the desired instance count from the same inputs every other
//! instance sees — CAS conflicts on `fleet/desired.json` are benign no-ops.
//! The pilot load vector is a single dimension (append+read req/s); the
//! production vector is COMPUTE-SPEC §4.2.
//!
//! Sleep interaction: a scale-to-zero'd instance stops heartbeating and ages
//! out of the live set within 10 s — exactly the semantics the ring wants,
//! since a sleeping instance serves nothing. The router waking instance N+1
//! re-adds it to the live set on its next heartbeat.

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::http::AppState;
use crate::shard::now_ms;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    pub instance: String,
    pub ts_ms: i64,
    pub rps: f64,
    /// p50 of commit durable-wait over the last 15 s across owned shards
    /// (ms). The latency dimension of the load vector (§4.2): rps alone
    /// deadlocks — a congested instance caps its own throughput signal.
    #[serde(default)]
    pub ack_p50_ms: f64,
    /// Measured process CPU (user+sys) over the last heartbeat interval,
    /// as % of one core. THE primary scaling signal: assumed-capacity
    /// constants go stale every time the engine changes speed (run 5
    /// scaled out at ~5 % utilization because SCALE_RPS_CAPACITY still
    /// described the pre-pacing engine). Utilization is workload- and
    /// version-independent.
    #[serde(default)]
    pub cpu_pct: f64,
    /// In-flight HTTP requests at heartbeat time / windowed peak since the
    /// last beat. Measures ADMITTED CONCURRENCY — the per-instance
    /// resource the platform edge actually bounds (runs 6–8: the fleet
    /// ceiling at 16–25 % CPU).
    #[serde(default)]
    pub inflight: i64,
    #[serde(default)]
    pub inflight_peak: i64,
    /// Resident set size (MB). The 1 GB boxes die at ~RSS cap (the
    /// block-cache epidemic); scaling and alarms need to see it.
    #[serde(default)]
    pub rss_mb: f64,
    /// O14a: WAL PUT latency at the object_store client over the last 15 s
    /// (ms) — the durable-commit path's raw store cost, same window as
    /// ack_p50_ms so excursions correlate sample-for-sample.
    #[serde(default)]
    pub wal_put_p50_ms: u64,
    #[serde(default)]
    pub wal_put_p99_ms: u64,
    /// Outbound object-store ops in flight now / peak. The platform egress
    /// budget (~50 concurrent per instance) gates these; a pinned peak
    /// during an ack excursion is the egress-exhaustion signature.
    #[serde(default)]
    pub out_inflight: i64,
    #[serde(default)]
    pub out_inflight_peak: i64,
    pub owned_shards: Vec<String>,
    pub draining: bool,
    /// Age of the oldest unabsorbed bytes across owned shards (s). The
    /// rebalance signal: sustained > REBALANCE_LAG_SECS means this host
    /// cannot keep up with its shards' internal machinery and one should
    /// move (SCALING.md §4).
    #[serde(default)]
    pub absorb_lag_max_secs: u64,
}

/// fleet/overrides.json: rebalancer shard moves, CAS-updated by the
/// initiating (laggard) instance, read by everyone each fleet tick.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct Overrides {
    #[serde(default)]
    pub entries: std::collections::HashMap<String, OverrideEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct OverrideEntry {
    pub to: String,
    pub ms: i64,
}

/// Current RSS in bytes. Linux (musl cloud build): /proc/self/statm.
/// macOS dev box: getrusage peak RSS as an approximation.
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = statm
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
            {
                return pages * 4096;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0;
        }
        ru.ru_maxrss as u64 // bytes on macOS
    }
}

/// Process CPU time (user+sys) in seconds via getrusage — portable across
/// the macOS dev box and the musl cloud build.
fn cpu_time_secs() -> f64 {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0.0;
        }
        ru.ru_utime.tv_sec as f64
            + ru.ru_utime.tv_usec as f64 / 1e6
            + ru.ru_stime.tv_sec as f64
            + ru.ru_stime.tv_usec as f64 / 1e6
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Desired {
    pub count: u64,
    pub reason: String,
    pub epoch: u64,
    pub computed_at_ms: i64,
}

pub struct FleetCfg {
    pub instance: String,
    /// Legacy assumed-capacity dimension (req/s per instance). 0 disables
    /// it — measured CPU replaced it as the primary signal.
    pub capacity_rps: u64,
    /// Per-instance admitted-concurrency capacity (edge slots). Measured
    /// on Prisma Compute via a calibrated-latency ladder: the edge admits
    /// ~48-50 concurrent requests per instance and queues the rest
    /// (EXPERIMENT edge probe, 2026-07-15). Utilization = Σ in-flight /
    /// (slots × live); scale-out at target_util of it — i.e. BEFORE the
    /// edge queue forms. 0 disables.
    pub edge_slots: u64,
    /// Utilization target for scale-out capacity planning: desired ≥
    /// ceil(fleet core-equivalents in use / target_util). 0.75 = "scale
    /// when the fleet is at 75 % of maximum".
    pub target_util: f64,
    /// Projected post-shrink utilization ceiling for scale-in: shrinking
    /// to N-1 is allowed only if used/(N-1) stays under this. Must be
    /// meaningfully below target_util or the fleet flaps at the boundary.
    pub scale_in_util: f64,
    /// Hot-instance scale-out: one instance sustaining ≥ this CPU% asks
    /// for one more instance even while fleet-average utilization is low
    /// (skewed shards). Matches target_util by default (75).
    pub hot_cpu_pct: f64,
    /// How long the hot-instance breach must persist (transition churn
    /// briefly spikes CPU during shard handoffs).
    pub cpu_sustain: Duration,
    pub scale_in: Duration,
    /// Ack-p50 threshold (ms) above which a loaded instance demands
    /// scale-out regardless of the other dimensions.
    pub latency_ms: u64,
    /// Router-observed client-latency threshold (ms). The router
    /// publishes what clients actually experience (routers/*.json);
    /// server-side ack latency cannot see edge queueing. Breach ⇒ +1 and
    /// scale-in is blocked while hot.
    pub edge_latency_ms: u64,
    /// How long the latency breach must persist before it scales the fleet
    /// (transition churn spikes ack latency briefly; undamped, every scale
    /// event triggers the next one).
    pub latency_sustain: Duration,
    pub max: u64,
}

pub fn start(state: Arc<AppState>, store: Arc<dyn ObjectStore>, cfg: FleetCfg) {
    tokio::spawn(async move {
        let mut ewma_rps = 0.0f64;
        let mut last_ops = 0u64;
        let mut last_tick = Instant::now();
        let mut below_since: Option<Instant> = None;
        let mut lat_breach_since: Option<Instant> = None;
        let mut cpu_breach_since: Option<Instant> = None;
        // Rebalancer (SCALING.md §4): consecutive ticks with absorb lag
        // over threshold, and the churn guard on shard moves.
        let mut lag_hot_ticks: u32 = 0;
        let mut last_move: Option<Instant> = None;
        let rebalance_lag_secs: u64 = std::env::var("REBALANCE_LAG_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);
        let rebalance_cooldown: u64 = std::env::var("REBALANCE_MOVE_COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);
        let mut last_cpu = cpu_time_secs();
        let mut ewma_cpu = 0.0f64;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let ops = state.fleet_ops.load(Ordering::Relaxed);
            let dt = last_tick.elapsed().as_secs_f64().max(0.001);
            last_tick = Instant::now();
            let inst_rps = (ops - last_ops) as f64 / dt;
            last_ops = ops;
            ewma_rps = if ewma_rps == 0.0 {
                inst_rps
            } else {
                ewma_rps * 0.6 + inst_rps * 0.4
            };
            let cpu_now = cpu_time_secs();
            let inst_cpu = ((cpu_now - last_cpu) / dt * 100.0).max(0.0);
            last_cpu = cpu_now;
            ewma_cpu = if ewma_cpu == 0.0 {
                inst_cpu
            } else {
                ewma_cpu * 0.6 + inst_cpu * 0.4
            };

            // 1. Heartbeat (single writer per object: plain PUT).
            let (owned, ack_p50_ms) = {
                let shards = state.shards.read().unwrap();
                let owned: Vec<String> = shards.keys().cloned().collect();
                let cutoff = now_ms() - 15_000;
                let mut waits: Vec<u32> = Vec::new();
                for eng in shards.values() {
                    waits.extend(
                        eng.timings
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|g| g.ts_ms >= cutoff)
                            .map(|g| g.durable_wait_us),
                    );
                }
                waits.sort_unstable();
                let p50 = waits
                    .get(waits.len() / 2)
                    .map(|us| *us as f64 / 1000.0)
                    .unwrap_or(0.0);
                (owned, (p50 * 10.0).round() / 10.0)
            };
            let inflight_now = state.inflight.load(Ordering::Relaxed);
            let inflight_peak = state.inflight_peak.swap(inflight_now, Ordering::Relaxed);
            let (wal_put_p50_ms, wal_put_p99_ms, out_inflight, out_inflight_peak) =
                crate::store_timing::heartbeat_summary();
            let hb = Heartbeat {
                instance: cfg.instance.clone(),
                ts_ms: now_ms(),
                rps: (ewma_rps * 10.0).round() / 10.0,
                ack_p50_ms,
                cpu_pct: (ewma_cpu * 10.0).round() / 10.0,
                inflight: inflight_now,
                inflight_peak,
                rss_mb: (rss_bytes() as f64 / 1048576.0 * 10.0).round() / 10.0,
                wal_put_p50_ms,
                wal_put_p99_ms,
                out_inflight,
                out_inflight_peak,
                owned_shards: owned,
                draining: false,
                absorb_lag_max_secs: crate::usage::absorb_lag_max(),
            };
            let path = ObjPath::from(format!("fleet/{}.json", cfg.instance));
            if let Err(e) = store
                .put(&path, PutPayload::from(serde_json::to_vec(&hb).unwrap()))
                .await
            {
                tracing::warn!("heartbeat put failed: {e}");
                continue;
            }

            // 2. Live set + fleet load.
            let mut total_rps = 0.0f64;
            let mut total_cores_used = 0.0f64;
            let mut total_inflight = 0.0f64;
            let mut live = 0u64;
            let mut max_loaded_p50 = 0.0f64;
            let mut max_loaded_cpu = 0.0f64;
            let mut hb_age_ms: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            // Fresh peers' load, for rebalance target choice.
            let mut peer_cpu: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            let mut listing = store.list(Some(&ObjPath::from("fleet")));
            use futures_util::StreamExt;
            let mut hb_paths = Vec::new();
            while let Some(meta) = listing.next().await {
                let Ok(meta) = meta else { continue };
                if meta.location.as_ref().ends_with(".json")
                    && !meta.location.as_ref().ends_with("desired.json")
                    && !meta.location.as_ref().ends_with("overrides.json")
                {
                    hb_paths.push(meta.location);
                }
            }
            for p in hb_paths {
                let Ok(r) = store.get(&p).await else { continue };
                let Ok(raw) = r.bytes().await else { continue };
                let Ok(other) = serde_json::from_slice::<Heartbeat>(&raw) else {
                    continue;
                };
                hb_age_ms.insert(other.instance.clone(), now_ms() - other.ts_ms);
                if now_ms() - other.ts_ms < 10_000 && !other.draining {
                    peer_cpu.insert(other.instance.clone(), other.cpu_pct);
                    live += 1;
                    total_rps += other.rps;
                    total_cores_used += other.cpu_pct / 100.0;
                    total_inflight += other.inflight.max(0) as f64;
                    // Load-gated dims count only for instances doing real
                    // work (≥5 rps) so idle blips and cold starts (binary
                    // download, shard replay burn CPU) don't scale us.
                    if other.rps >= 5.0 {
                        if other.ack_p50_ms > max_loaded_p50 {
                            max_loaded_p50 = other.ack_p50_ms;
                        }
                        if other.cpu_pct > max_loaded_cpu {
                            max_loaded_cpu = other.cpu_pct;
                        }
                    }
                }
            }

            // 2b. Router reports: worst client-observed p50 across fresh
            // routers. Edge congestion is invisible to server-side acks.
            let mut edge_p50 = 0.0f64;
            {
                let mut rl = store.list(Some(&ObjPath::from("routers")));
                let mut rpaths = Vec::new();
                while let Some(meta) = rl.next().await {
                    let Ok(meta) = meta else { continue };
                    rpaths.push(meta.location);
                }
                for p in rpaths {
                    let Ok(r) = store.get(&p).await else { continue };
                    let Ok(raw) = r.bytes().await else { continue };
                    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&raw) else {
                        continue;
                    };
                    let fresh = now_ms() - v["ts_ms"].as_i64().unwrap_or(0) < 10_000;
                    if fresh {
                        edge_p50 = edge_p50.max(v["client_p50_ms"].as_f64().unwrap_or(0.0));
                    }
                }
            }

            // 3. Desired count — max over dimensions (§4.1/§4.2):
            //    utilization (primary): ceil(cores-in-use / target_util) —
            //    scale-out begins when the fleet nears 75 % of maximum.
            //    hot instance: one instance sustaining ≥ hot_cpu_pct wants
            //    +1 even when fleet-average is low (shard skew). Damped.
            //    latency: sustained ack-p50 breach wants +1 (congestion
            //    that doesn't show as CPU — e.g. object-store slowness).
            //    rps: legacy assumed-capacity dim, only if capacity_rps>0.
            let need_util = (total_cores_used / cfg.target_util).ceil() as u64;
            let need_rps = if cfg.capacity_rps > 0 {
                (total_rps / (cfg.target_util * cfg.capacity_rps as f64).max(1.0)).ceil() as u64
            } else {
                0
            };
            // Edge-slot dimension: in-flight requests vs the measured
            // per-instance admission budget. This is the resource that
            // actually bound runs 6-8 (servers at 16-25 % CPU): once
            // offered concurrency exceeds slots × live, clients queue at
            // the edge. Scaling at 75 % of the slot budget adds capacity
            // BEFORE the queue forms.
            let need_slots = if cfg.edge_slots > 0 {
                (total_inflight / (cfg.target_util * cfg.edge_slots as f64).max(1.0)).ceil() as u64
            } else {
                0
            };
            let breaching = max_loaded_p50 > cfg.latency_ms as f64;
            let need_latency = if breaching {
                let since = *lat_breach_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= cfg.latency_sustain {
                    live + 1
                } else {
                    0
                }
            } else {
                lat_breach_since = None;
                0
            };
            let edge_hot = edge_p50 > cfg.edge_latency_ms as f64 && total_rps >= 5.0;
            let need_edge = if edge_hot { live + 1 } else { 0 };
            let cpu_hot = max_loaded_cpu >= cfg.hot_cpu_pct;
            let need_hot = if cpu_hot {
                let since = *cpu_breach_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= cfg.cpu_sustain {
                    live + 1
                } else {
                    0
                }
            } else {
                cpu_breach_since = None;
                0
            };
            let need = need_util
                .max(need_rps)
                .max(need_slots)
                .max(need_latency)
                .max(need_hot)
                .max(need_edge)
                .clamp(1, cfg.max);
            // Scale-in target uses the conservative divisor, and edge
            // congestion BLOCKS shrink outright: measured rps falls during
            // client-side queueing, and shrinking on that signal removes
            // capacity mid-collapse (observed in run 7: desired 4→2 while
            // client p50 was 1.6-2 s).
            let need_shrink = if edge_hot {
                cfg.max // sentinel: never below current -> shrink blocked
            } else {
                let slots_shrink = if cfg.edge_slots > 0 {
                    (total_inflight / (cfg.scale_in_util * cfg.edge_slots as f64).max(1.0)).ceil()
                        as u64
                } else {
                    0
                };
                ((total_cores_used / cfg.scale_in_util).ceil() as u64)
                    .max(need_rps)
                    .max(slots_shrink)
                    .max(need_latency)
                    .max(need_hot)
                    .clamp(1, cfg.max)
            };

            let dpath = ObjPath::from("fleet/desired.json");
            let (cur, version): (Option<Desired>, Option<UpdateVersion>) =
                match store.get(&dpath).await {
                    Ok(r) => {
                        let v = UpdateVersion {
                            e_tag: r.meta.e_tag.clone(),
                            version: r.meta.version.clone(),
                        };
                        let raw = r.bytes().await.unwrap_or_default();
                        (serde_json::from_slice(&raw).ok(), Some(v))
                    }
                    Err(object_store::Error::NotFound { .. }) => (None, None),
                    Err(e) => {
                        tracing::warn!("desired.json get failed: {e}");
                        continue;
                    }
                };
            let cur_count = cur.as_ref().map(|d| d.count).unwrap_or(1);
            // Publish the ring's ACTIVE set for the R2 ownership check:
            // the first `desired` ordinal instances, dropping any that have
            // been heartbeat-dark >30 s (wedged — requests would have woken
            // a merely-sleeping one). Self is always fresh (just wrote).
            // Falls back to the unfiltered ordinal set if filtering empties
            // it (bootstrap: everyone asleep, first request must land).
            {
                let ordinal: Vec<String> = (1..=cur_count.max(1))
                    .map(|i| format!("streams-{i}"))
                    .collect();
                let mut active: Vec<String> = ordinal
                    .iter()
                    .filter(|n| {
                        **n == cfg.instance
                            || hb_age_ms.get(*n).map(|a| *a < 30_000).unwrap_or(false)
                    })
                    .cloned()
                    .collect();
                if active.is_empty() {
                    active = ordinal;
                }
                *state.ring_active.write().unwrap() = active;
            }

            // R4 rebalancer (SCALING.md §4). Every instance mirrors
            // fleet/overrides.json into routing state; the laggard itself
            // initiates a move (it alone knows per-shard lag), CAS-guarded.
            {
                let opath = ObjPath::from("fleet/overrides.json");
                let (mut ov, ov_ver): (Overrides, Option<UpdateVersion>) =
                    match store.get(&opath).await {
                        Ok(r) => {
                            let v = UpdateVersion {
                                e_tag: r.meta.e_tag.clone(),
                                version: r.meta.version.clone(),
                            };
                            let raw = r.bytes().await.unwrap_or_default();
                            (serde_json::from_slice(&raw).unwrap_or_default(), Some(v))
                        }
                        Err(object_store::Error::NotFound { .. }) => (Overrides::default(), None),
                        Err(e) => {
                            tracing::warn!("overrides.json get failed: {e}");
                            (Overrides::default(), None)
                        }
                    };
                {
                    let map: std::collections::HashMap<String, String> = ov
                        .entries
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to.clone()))
                        .collect();
                    *state.ring_overrides.write().unwrap() = map;
                }

                let my_lag = hb.absorb_lag_max_secs;
                lag_hot_ticks = if my_lag > rebalance_lag_secs {
                    lag_hot_ticks + 1
                } else {
                    0
                };
                let cooled = last_move
                    .map(|t| t.elapsed().as_secs() >= rebalance_cooldown)
                    .unwrap_or(true);
                if lag_hot_ticks >= 2 && cooled {
                    // Move my laggiest shard to the coolest fresh peer.
                    let target = peer_cpu
                        .iter()
                        .filter(|(n, _)| **n != cfg.instance)
                        .min_by(|a, b| a.1.total_cmp(b.1))
                        .map(|(n, _)| n.clone());
                    let victim = {
                        let mut per_shard: std::collections::HashMap<String, u64> =
                            std::collections::HashMap::new();
                        for (h, lag) in crate::usage::absorb_lag_all() {
                            let p = crate::registry::shard_for_hash(&state.shard_prefixes, &h);
                            let e = per_shard.entry(p).or_insert(0);
                            *e = (*e).max(lag);
                        }
                        per_shard
                            .into_iter()
                            .filter(|(p, _)| {
                                state
                                    .effective_owner(p)
                                    .map(|o| o == cfg.instance)
                                    .unwrap_or(false)
                            })
                            .max_by_key(|(_, lag)| *lag)
                            .map(|(p, _)| p)
                    };
                    if let (Some(to), Some(prefix)) = (target, victim) {
                        ov.entries.insert(
                            prefix.clone(),
                            OverrideEntry {
                                to: to.clone(),
                                ms: now_ms(),
                            },
                        );
                        let payload = PutPayload::from(serde_json::to_vec(&ov).unwrap());
                        let mode = match ov_ver {
                            Some(v) => PutMode::Update(v),
                            None => PutMode::Create,
                        };
                        let res = store
                            .put_opts(
                                &opath,
                                payload,
                                PutOptions::from(mode),
                            )
                            .await;
                        match res {
                            Ok(_) => {
                                tracing::info!(
                                    "rebalancer: moving shard {prefix} -> {to} (absorb lag {my_lag}s)"
                                );
                                state
                                    .ring_overrides
                                    .write()
                                    .unwrap()
                                    .insert(prefix.clone(), to.clone());
                                // Stop serving immediately; the new owner
                                // fences the log on first routed request.
                                state.shards.write().unwrap().remove(&prefix);
                                last_move = Some(Instant::now());
                                lag_hot_ticks = 0;
                            }
                            Err(e) => {
                                tracing::info!("rebalancer: overrides CAS lost ({e}); retry next tick");
                            }
                        }
                    }
                }
            }

            // Scale-out publishes `need` immediately; scale-in publishes
            // the conservative `need_shrink` and only after the sustain
            // window — and only when even the conservative target is
            // below the current count (no flapping at the 75 % boundary).
            let (publish, publish_count) = if cur.is_none() {
                (true, need) // bootstrap: make the count observable
            } else if need > cur_count {
                below_since = None;
                (true, need)
            } else if need_shrink < cur_count {
                let since = *below_since.get_or_insert_with(Instant::now);
                (since.elapsed() >= cfg.scale_in, need_shrink)
            } else {
                below_since = None;
                (false, need)
            };

            if publish {
                let next = Desired {
                    count: publish_count,
                    reason: format!(
                        "cores_used={total_cores_used:.2} util->{need_util} inflight={total_inflight:.0} slots->{need_slots} hot_cpu={max_loaded_cpu:.0}% ({need_hot}) ack_p50={max_loaded_p50:.0}ms ({need_latency}) edge_p50={edge_p50:.0}ms ({need_edge}) rps={total_rps:.0} ({need_rps}) live={live}",
                    ),
                    epoch: cur.as_ref().map(|d| d.epoch + 1).unwrap_or(1),
                    computed_at_ms: now_ms(),
                };
                let mode = match version {
                    Some(v) => PutMode::Update(v),
                    None => PutMode::Create,
                };
                match store
                    .put_opts(
                        &dpath,
                        PutPayload::from(serde_json::to_vec(&next).unwrap()),
                        PutOptions::from(mode),
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!(
                            "fleet desired {} -> {} ({})",
                            cur_count,
                            publish_count,
                            next.reason
                        );
                        below_since = None;
                    }
                    // Lost the CAS: another instance published; converge next tick.
                    Err(object_store::Error::Precondition { .. })
                    | Err(object_store::Error::AlreadyExists { .. }) => {}
                    Err(e) => tracing::warn!("desired.json cas failed: {e}"),
                }
            }
        }
    });
}
