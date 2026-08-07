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
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
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
    /// Worst wedge across owned shards (ms): blocked commit write or
    /// stale durability. Complements absorb lag as the rebalance signal —
    /// a backpressured shard sheds appends BEFORE they commit, so lag
    /// (age of committed-but-unabsorbed bytes) never grows on a wedged
    /// instance (ladder p4b D3: zero-flow vacuous run).
    #[serde(default)]
    pub wedge_max_ms: i64,
    /// This instance's own base URL (SELF_URL env), published so peers
    /// can fan segment-scoped work out to the owner — cross-owner
    /// lineage reads and consumer sweeps need an address, and only the
    /// platform (not the instance's peers) knows it otherwise.
    #[serde(default)]
    pub url: String,
}

/// fleet/overrides.json: rebalancer shard moves, CAS-updated by the
/// initiating (laggard) instance, read by everyone each fleet tick.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct Overrides {
    #[serde(default)]
    pub entries: std::collections::HashMap<String, OverrideEntry>,
    /// Pending-event outbox (§12.4) — same contract as Desired's.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_events: Vec<crate::ops::OpsEvent>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct OverrideEntry {
    pub to: String,
    pub ms: i64,
}

/// Rebalance target: the coolest peer that is itself HEALTHY. Under
/// fleet-wide backlog every instance breaches the lag threshold, and
/// unguarded moves just hand the backlog around (ladder pass 3: 7 moves
/// in 10 minutes of ping-pong). `peers` maps instance -> (cpu_pct,
/// effective_lag_secs) and must exclude nobody; self is filtered here.
pub fn pick_move_target(
    peers: &std::collections::HashMap<String, (f64, u64)>,
    me: &str,
    lag_threshold_secs: u64,
) -> Option<String> {
    peers
        .iter()
        .filter(|(n, (_, lag))| n.as_str() != me && *lag < lag_threshold_secs / 2)
        .min_by(|a, b| a.1.0.total_cmp(&b.1.0))
        .map(|(n, _)| n.clone())
}

/// Validate a peer base URL before it can receive the fleet-internal
/// credential (and, on relays that decrypt, a customer stream key).
/// `fleet/urls.json` and heartbeats are bucket-writable inputs, so an
/// unvalidated string here is an exfiltration primitive (round-19).
///
/// Rules: absolute http(s) origin only — scheme + host + optional port,
/// nothing else. No userinfo (`@`), no path, no query, no fragment. TLS
/// is mandatory unless FLEET_ALLOW_HTTP_PEERS=1 (local rigs and DST
/// use plain http against 127.0.0.1). When FLEET_PEER_DOMAINS is set,
/// the host must equal or be a subdomain of one of its entries.
pub fn valid_peer_url(url: &str) -> bool {
    let allow_http = std::env::var("FLEET_ALLOW_HTTP_PEERS").ok().as_deref() == Some("1");
    let rest = match url.strip_prefix("https://") {
        Some(r) => r,
        None => match url.strip_prefix("http://") {
            Some(r) if allow_http => r,
            _ => return false,
        },
    };
    if rest.is_empty()
        || rest.contains('/')
        || rest.contains('?')
        || rest.contains('#')
        || rest.contains('@')
        || rest.contains('\\')
        || rest.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return false;
    }
    let host = rest.split(':').next().unwrap_or("");
    if host.is_empty() {
        return false;
    }
    // Port, when present, must be numeric — "host:evil" is not an origin.
    if let Some((_, port)) = rest.split_once(':')
        && (port.is_empty() || !port.chars().all(|c| c.is_ascii_digit()))
    {
        return false;
    }
    match std::env::var("FLEET_PEER_DOMAINS") {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .any(|d| host == d || host.ends_with(&format!(".{d}"))),
        _ => true,
    }
}

/// Last successfully-read AND validated fleet/urls.json map. A transient
/// GET failure must not drop back to heartbeat SELF_URLs: on Compute
/// those are stale after any redeploy (each version mints a new preview
/// URL), so losing the published map would point relays at dead
/// addresses until the next successful read (round-19).
fn last_good_urls() -> &'static Mutex<std::collections::HashMap<String, String>> {
    static M: OnceLock<Mutex<std::collections::HashMap<String, String>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Return-home admission: dropping an override hands `gain` shard(s)
/// back to the rendezvous home, which currently owns `owned`. Allowed
/// only while that stays within the fair share ceil(total/active) —
/// above it, the imbalance that caused the original move re-appears
/// immediately and the rebalancer/return-home pair flap on a
/// ~REBALANCE_RETURN_SECS period (FLEET-CAMPAIGN.md, 4 shards over 4
/// instances). A drained home (owns nothing) is always below the mean,
/// so the ladder-p3 refill this block exists for still happens.
pub fn return_home_allowed(owned: usize, gain: usize, total: usize, active_n: usize) -> bool {
    if active_n == 0 {
        return false;
    }
    owned + gain <= total.div_ceil(active_n)
}

/// Rebalance victim: the laggiest shard THIS instance actually serves.
/// Possession, not the ring, is the truth here — and the lag must be
/// keyed by shard prefix, never re-derived from a stream hash (records
/// are keyed by storage_hash while the shard is chosen by
/// stream_hash(name); the two are unrelated, so the derived prefix
/// almost never matched and no move ever fired — ladder pass 6b D3).
pub fn pick_victim_shard(shard_lags: &[(String, u64)], served: &[String]) -> Option<String> {
    shard_lags
        .iter()
        .filter(|(p, _)| served.iter().any(|s| s == p))
        .max_by_key(|(_, lag)| *lag)
        .map(|(p, _)| p.clone())
}

/// Current memory pressure in bytes. Linux (musl cloud build):
/// /proc/self/statm RSS. macOS dev box: task_vm_info.phys_footprint.
///
/// This MUST be a value that can go back DOWN when memory is returned:
/// the admission shed compares it against ADMIT_RSS_SHED_MB, and the
/// wedge liveness gate proved the old macOS reading (getrusage
/// ru_maxrss — the lifetime PEAK) turns one overload spike into a
/// permanent 429 wedge. Plain resident size is also wrong on macOS:
/// mimalloc surrenders freed pages with MADV_FREE_REUSABLE, and Darwin
/// keeps those OS-reclaimable pages in resident_size (measured on the
/// wedge repro: resident 120 MB vs phys_footprint 2 MB after drain).
/// phys_footprint is the metric Darwin's own memory limits use.
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
    #[cfg(target_os = "macos")]
    unsafe {
        // Prefix of task_vm_info through phys_footprint; task_info fills
        // only the count we pass, so the truncated layout is safe.
        #[repr(C)]
        struct TaskVmInfoPrefix {
            virtual_size: u64,
            region_count: i32,
            page_size: i32,
            resident_size: u64,
            resident_size_peak: u64,
            device: u64,
            device_peak: u64,
            internal: u64,
            internal_peak: u64,
            external: u64,
            external_peak: u64,
            reusable: u64,
            reusable_peak: u64,
            purgeable_volatile_pmap: u64,
            purgeable_volatile_resident: u64,
            purgeable_volatile_virtual: u64,
            compressed: u64,
            compressed_peak: u64,
            compressed_lifetime: u64,
            phys_footprint: u64,
        }
        const TASK_VM_INFO: u32 = 22;
        unsafe extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(task: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
        }
        let mut info: TaskVmInfoPrefix = std::mem::zeroed();
        let mut count = (std::mem::size_of::<TaskVmInfoPrefix>() / 4) as u32;
        let kr = task_info(
            mach_task_self(),
            TASK_VM_INFO,
            &mut info as *mut _ as *mut u8,
            &mut count,
        );
        if kr == 0 {
            return info.phys_footprint;
        }
        0
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
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
    /// Pending-event outbox (§12.4): the CAS that commits a desired-
    /// count change records its event HERE; the drainer appends to
    /// `_ops_events` and CAS-clears exactly these ids. Bounded — at the
    /// cap the transition still proceeds and the drop is counted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_events: Vec<crate::ops::OpsEvent>,
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

/// The fleet coordination store, registered by start() so the ops
/// drainer can read/clear the CAS outboxes without threading handles.
fn fleet_store_slot() -> &'static Mutex<Option<Arc<dyn ObjectStore>>> {
    static S: OnceLock<Mutex<Option<Arc<dyn ObjectStore>>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(None))
}

fn fleet_store_handle() -> Option<Arc<dyn ObjectStore>> {
    fleet_store_slot().lock().unwrap().clone()
}

pub fn start(state: Arc<AppState>, store: Arc<dyn ObjectStore>, cfg: FleetCfg) {
    *fleet_store_slot().lock().unwrap() = Some(store.clone());
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
            let (owned, ack_p50_ms, wedge_prefix, wedge_max_ms) = {
                let shards = state.shards.read().unwrap();
                let owned: Vec<String> = shards.keys().cloned().collect();
                let (wedge_prefix, wedge_max_ms) = shards
                    .iter()
                    .map(|(p, e)| (p.clone(), e.wedge_ms()))
                    .max_by_key(|(_, w)| *w)
                    .unwrap_or((String::new(), 0));
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
                (
                    owned,
                    (p50 * 10.0).round() / 10.0,
                    wedge_prefix,
                    wedge_max_ms,
                )
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
                wedge_max_ms,
                url: std::env::var("SELF_URL").unwrap_or_default(),
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
            // Fresh peers' load (cpu, absorb lag), for rebalance targets.
            let mut peer_load: std::collections::HashMap<String, (f64, u64)> =
                std::collections::HashMap::new();
            // Fresh peers' published base URLs, for segment fan-out.
            let mut peer_urls: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            // (cpu, effective lag secs incl. wedge)
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
                    let eff_lag = other
                        .absorb_lag_max_secs
                        .max((other.wedge_max_ms / 1000).max(0) as u64);
                    peer_load.insert(other.instance.clone(), (other.cpu_pct, eff_lag));
                    if !other.url.is_empty() {
                        if valid_peer_url(&other.url) {
                            peer_urls.insert(other.instance.clone(), other.url.clone());
                        } else {
                            tracing::warn!(
                                instance = %other.instance,
                                "rejecting malformed peer URL from heartbeat"
                            );
                        }
                    }
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
            // FLEET_MIN: hard floor on the fleet size (HA / pinned test
            // rings). All dimensions and the shrink target respect it.
            let fleet_min: u64 = std::env::var("FLEET_MIN")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1)
                .max(1);
            let need = need.max(fleet_min);
            let need_shrink = need_shrink.max(fleet_min);
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
                // fleet/urls.json overrides heartbeat-published URLs: on
                // Compute a deploy cannot know its own final preview URL
                // (each version mints a new one), so the deploy script
                // publishes the resolved map AFTER deploying and the
                // fleet reads it here. SELF_URL heartbeats remain the
                // plain-VM path where an instance does know its address.
                let published: Option<std::collections::HashMap<String, String>> =
                    match store.get(&ObjPath::from("fleet/urls.json")).await {
                        Ok(r) => match r.bytes().await {
                            Ok(raw) => serde_json::from_slice::<
                                std::collections::HashMap<String, String>,
                            >(&raw)
                            .ok()
                            .map(|m| {
                                m.into_iter()
                                    .filter(|(inst, url)| {
                                        let ok = valid_peer_url(url);
                                        if !ok {
                                            tracing::warn!(
                                                instance = %inst,
                                                "rejecting malformed peer URL from urls.json"
                                            );
                                        }
                                        ok
                                    })
                                    .collect()
                            }),
                            Err(_) => None,
                        },
                        Err(_) => None,
                    };
                match published {
                    Some(m) => {
                        *last_good_urls().lock().unwrap() = m.clone();
                        peer_urls.extend(m);
                    }
                    // Transient failure: keep the last validated map
                    // rather than falling back to stale heartbeat URLs.
                    None => peer_urls.extend(last_good_urls().lock().unwrap().clone()),
                }
                *state.peer_urls.write().unwrap() = peer_urls.clone();
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

                // Eager handoff: open any shard newly assigned to ME right
                // now instead of waiting for the first routed request —
                // the open fences the loser's db immediately (ladder p3:
                // lazy opening left a moved shard unowned for 92 minutes
                // while the loser's zombie compactor/GC kept running).
                {
                    let mine: Vec<String> = ov
                        .entries
                        .iter()
                        .filter(|(_, e)| e.to == cfg.instance)
                        .map(|(p, _)| p.clone())
                        .collect();
                    for prefix in mine {
                        let have = state.shards.read().unwrap().contains_key(&prefix);
                        if !have {
                            // Through the gate: single-flight with the
                            // request path, honoring the same holdoffs.
                            match state
                                .gate
                                .get_or_open(&prefix, std::time::Duration::from_secs(300))
                                .await
                            {
                                crate::sharddir::OpenOutcome::Ready(_) => {
                                    tracing::info!(
                                        "rebalancer: eagerly opened moved-in shard {prefix}"
                                    );
                                }
                                crate::sharddir::OpenOutcome::Wait { code, .. } => {
                                    tracing::info!("eager open of {prefix} deferred ({code})");
                                }
                                crate::sharddir::OpenOutcome::Failed(e) => {
                                    tracing::warn!("eager open of {prefix} failed: {e}");
                                }
                            }
                        }
                    }
                }

                // Return-home: drop an override once the rendezvous owner
                // is fresh and healthy again and the entry has aged past
                // the hysteresis window — otherwise moves are sticky and
                // an instance that lagged once is drained forever
                // (ladder p3: streams-2 owned nothing by D3).
                {
                    let return_secs: i64 = std::env::var("REBALANCE_RETURN_SECS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(300);
                    let active = state.ring_active.read().unwrap().clone();
                    let mut drop_keys: Vec<String> = Vec::new();
                    let mut pending_returns: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();
                    for (prefix, e) in ov.entries.iter() {
                        if now_ms() - e.ms < return_secs * 1000 {
                            continue;
                        }
                        if active.is_empty() {
                            continue;
                        }
                        let home = active[crate::http::ring_pick(prefix, &active)].clone();
                        if home == e.to {
                            drop_keys.push(prefix.clone()); // override is a no-op
                            continue;
                        }
                        let healthy = peer_load
                            .get(&home)
                            .map(|(_, lag)| *lag == 0)
                            .unwrap_or(home == cfg.instance && crate::usage::absorb_lag_max() == 0);
                        if !healthy {
                            continue;
                        }
                        // Load-aware return: giving the shard back must not
                        // push the home above its fair share, or the
                        // rebalancer moves it right out again and the pair
                        // flap on a ~return_secs period (FLEET-CAMPAIGN.md:
                        // 4 shards over 4 instances). A drained home (the
                        // ladder-p3 case this block exists for) is below
                        // the mean and still gets its shards back.
                        let owned = state
                            .shard_prefixes
                            .iter()
                            .filter(|p| state.effective_owner(p).as_deref() == Some(home.as_str()))
                            .count();
                        let gain = pending_returns.get(&home).copied().unwrap_or(0) + 1;
                        if !return_home_allowed(
                            owned,
                            gain,
                            state.shard_prefixes.len(),
                            active.len(),
                        ) {
                            continue;
                        }
                        *pending_returns.entry(home).or_insert(0) += 1;
                        drop_keys.push(prefix.clone());
                    }
                    if !drop_keys.is_empty() {
                        let mut next = ov.clone();
                        for k in &drop_keys {
                            if next.pending_events.len() < 64 {
                                next.pending_events.push(
                                    crate::ops::OpsEvent::new(
                                        "rebalance_return",
                                        format!("return/{k}/{}", now_ms() / 1000),
                                    )
                                    .shard(k),
                                );
                            }
                            next.entries.remove(k);
                        }
                        let payload = PutPayload::from(serde_json::to_vec(&next).unwrap());
                        let mode = match ov_ver.clone() {
                            Some(v) => PutMode::Update(v),
                            None => PutMode::Create,
                        };
                        if store
                            .put_opts(&opath, payload, PutOptions::from(mode))
                            .await
                            .is_ok()
                        {
                            tracing::info!(
                                "rebalancer: returned {} shard(s) to rendezvous owners: {:?}",
                                drop_keys.len(),
                                drop_keys
                            );
                            ov = next;
                        }
                    }
                }

                let my_lag = hb
                    .absorb_lag_max_secs
                    .max((hb.wedge_max_ms / 1000).max(0) as u64);
                lag_hot_ticks = if my_lag > rebalance_lag_secs {
                    lag_hot_ticks + 1
                } else {
                    0
                };
                let cooled = last_move
                    .map(|t| t.elapsed().as_secs() >= rebalance_cooldown)
                    .unwrap_or(true);
                if lag_hot_ticks >= 2 && cooled {
                    // Move my laggiest shard to the coolest HEALTHY peer.
                    // A peer that is itself lagging must not receive more
                    // work: under global backlog every instance breaches
                    // the threshold and unguarded moves just hand the
                    // backlog around (ladder p3: 7 moves in 10 min, shard
                    // ping-pong, zero net absorption gained).
                    let target = pick_move_target(&peer_load, &cfg.instance, rebalance_lag_secs);
                    if target.is_none() {
                        tracing::info!(
                            "rebalancer: lag {my_lag}s but no healthy peer; holding shards"
                        );
                    }
                    let victim = {
                        // Possession-first, keyed by the shard the absorber
                        // actually serves (never re-derived from a stream
                        // hash — see usage::shard_lag_map).
                        let served: Vec<String> =
                            state.shards.read().unwrap().keys().cloned().collect();
                        pick_victim_shard(&crate::usage::shard_lag_all(), &served)
                            // No committed backlog anywhere (shed-before-
                            // commit): move the WEDGED shard itself.
                            .or_else(|| {
                                if wedge_max_ms > 0 && !wedge_prefix.is_empty() {
                                    Some(wedge_prefix.clone())
                                } else {
                                    None
                                }
                            })
                    };
                    if let (Some(to), Some(prefix)) = (target, victim) {
                        if ov.pending_events.len() < 64 {
                            ov.pending_events.push(
                                crate::ops::OpsEvent::new(
                                    "rebalance_move",
                                    format!("move/{prefix}/{}", now_ms() / 1000),
                                )
                                .shard(&prefix)
                                .fields(serde_json::json!({"to": to.clone()})),
                            );
                        }
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
                            .put_opts(&opath, payload, PutOptions::from(mode))
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
                                // Stop serving immediately and fail all
                                // in-flight work fast (retryable) — the new
                                // owner fences the log on first routed
                                // request.
                                let eng = state.shards.write().unwrap().remove(&prefix);
                                if let Some(e) = eng {
                                    e.begin_close();
                                }
                                last_move = Some(Instant::now());
                                lag_hot_ticks = 0;
                            }
                            Err(e) => {
                                tracing::info!(
                                    "rebalancer: overrides CAS lost ({e}); retry next tick"
                                );
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
                let epoch = cur.as_ref().map(|d| d.epoch + 1).unwrap_or(1);
                // The transition and its event commit in ONE CAS write
                // (§12.4). Deterministic id: (epoch) — a re-published
                // epoch is the same transition. Carried-over pending
                // events survive until the drainer clears them; capped.
                let mut pending_events = cur
                    .as_ref()
                    .map(|d| d.pending_events.clone())
                    .unwrap_or_default();
                if pending_events.len() < 64 {
                    pending_events.push(
                        crate::ops::OpsEvent::new("desired_changed", format!("desired/{epoch}"))
                            .fields(serde_json::json!({
                                "count": publish_count,
                                "prev": cur_count,
                            })),
                    );
                }
                let next = Desired {
                    count: publish_count,
                    reason: format!(
                        "cores_used={total_cores_used:.2} util->{need_util} inflight={total_inflight:.0} slots->{need_slots} hot_cpu={max_loaded_cpu:.0}% ({need_hot}) ack_p50={max_loaded_p50:.0}ms ({need_latency}) edge_p50={edge_p50:.0}ms ({need_edge}) rps={total_rps:.0} ({need_rps}) live={live}",
                    ),
                    epoch,
                    computed_at_ms: now_ms(),
                    pending_events,
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

/// Drain the fleet CAS outboxes (§12.4): append `desired.json` and
/// `overrides.json` pending events to `_ops_events`, then CAS-clear
/// exactly the emitted ids. A lost clear re-emits deterministic ids
/// the downstream rollup deduplicates; a CAS conflict retries next
/// tick. Called from the telemetry task.
pub async fn drain_fleet_events(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(store) = fleet_store_handle() else {
        return Ok(0);
    };
    let mut emitted = 0usize;
    for name in ["fleet/desired.json", "fleet/overrides.json"] {
        let path = ObjPath::from(name);
        let Ok(got) = store.get(&path).await else {
            continue;
        };
        let version = UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let Ok(bytes) = got.bytes().await else {
            continue;
        };
        let pending: Vec<crate::ops::OpsEvent> = if name.ends_with("desired.json") {
            serde_json::from_slice::<Desired>(&bytes)
                .map(|d| d.pending_events)
                .unwrap_or_default()
        } else {
            serde_json::from_slice::<Overrides>(&bytes)
                .map(|o| o.pending_events)
                .unwrap_or_default()
        };
        if pending.is_empty() {
            continue;
        }
        let ids: std::collections::HashSet<String> =
            pending.iter().map(|e| e.event_id.clone()).collect();
        // Round-21 ordering fix: the events reach `_ops_events`
        // DURABLY before the CAS outbox is cleared — a crash between
        // the two re-emits deterministic ids the rollup deduplicates,
        // never loses the transition.
        let Some(sys_key) = state.usage_key.clone() else {
            continue;
        };
        let mut stamped = pending.clone();
        for ev in &mut stamped {
            if ev.cell.is_empty() {
                ev.cell = state.cell_id.clone();
            }
        }
        let body = match serde_json::to_vec(&stamped) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if crate::billing::system_append(state, crate::billing::OPS_EVENTS_STREAM, &sys_key, body)
            .await
            .is_err()
        {
            continue; // outbox stays; retry next tick
        }
        emitted += ids.len();
        // Clear EXACTLY the drained ids under CAS; concurrent writers'
        // new events survive.
        let cleared: Vec<u8> = if name.ends_with("desired.json") {
            let Ok(mut d) = serde_json::from_slice::<Desired>(&bytes) else {
                continue;
            };
            d.pending_events.retain(|e| !ids.contains(&e.event_id));
            serde_json::to_vec(&d).unwrap_or_default()
        } else {
            let Ok(mut o) = serde_json::from_slice::<Overrides>(&bytes) else {
                continue;
            };
            o.pending_events.retain(|e| !ids.contains(&e.event_id));
            serde_json::to_vec(&o).unwrap_or_default()
        };
        let _ = store
            .put_opts(
                &path,
                PutPayload::from(cleared),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn peers(v: &[(&str, f64, u64)]) -> HashMap<String, (f64, u64)> {
        v.iter()
            .map(|(n, c, l)| (n.to_string(), (*c, *l)))
            .collect()
    }

    // Regression: ladder pass 3 did 7 moves in 10 minutes because moves
    // were allowed to peers that were themselves behind.
    #[test]
    fn target_is_the_coolest_healthy_peer() {
        let p = peers(&[("a", 90.0, 0), ("b", 10.0, 0), ("c", 50.0, 0)]);
        assert_eq!(pick_move_target(&p, "a", 60).as_deref(), Some("b"));
    }

    #[test]
    fn target_excludes_self() {
        let p = peers(&[("a", 1.0, 0), ("b", 80.0, 0)]);
        assert_eq!(pick_move_target(&p, "a", 60).as_deref(), Some("b"));
    }

    #[test]
    fn no_target_when_every_peer_is_also_lagging() {
        // fleet-wide backlog: hold shards rather than pass them around
        let p = peers(&[("a", 5.0, 90), ("b", 5.0, 80), ("c", 5.0, 70)]);
        assert_eq!(pick_move_target(&p, "a", 60), None);
    }

    // Regression: FLEET-CAMPAIGN.md — 4 shards over 4 instances drew
    // 1/1/2/0; the rebalancer moved one off the 2-owner, return-home
    // handed it straight back once the home was healthy, and ownership
    // oscillated on a ~300 s period.
    // Round-19: fleet/urls.json and heartbeats are bucket-writable
    // inputs, and relays send the fleet-internal credential (sometimes a
    // customer stream key) to whatever they name. Anything that is not a
    // bare origin must be refused before it is stored.
    #[test]
    fn peer_urls_must_be_bare_origins() {
        unsafe { std::env::set_var("FLEET_ALLOW_HTTP_PEERS", "1") };
        assert!(valid_peer_url("https://cv-abc.fra.prisma.build"));
        assert!(valid_peer_url("http://127.0.0.1:8091"));
        // userinfo, path, query, fragment, backslash, whitespace
        assert!(!valid_peer_url("https://evil@attacker.example"));
        assert!(!valid_peer_url("https://host.example/path"));
        assert!(!valid_peer_url("https://host.example?x=1"));
        assert!(!valid_peer_url("https://host.example#f"));
        assert!(!valid_peer_url("https://host.example\\@evil"));
        assert!(!valid_peer_url("https://host .example"));
        // non-http schemes and bare hosts
        assert!(!valid_peer_url("file:///etc/passwd"));
        assert!(!valid_peer_url("host.example"));
        assert!(!valid_peer_url(""));
        // non-numeric port
        assert!(!valid_peer_url("http://host.example:evil"));
        unsafe { std::env::remove_var("FLEET_ALLOW_HTTP_PEERS") };
        // TLS mandatory once the dev escape hatch is off
        assert!(!valid_peer_url("http://127.0.0.1:8091"));
        assert!(valid_peer_url("https://cv-abc.fra.prisma.build"));
    }

    #[test]
    fn peer_domain_allowlist_is_enforced_when_set() {
        unsafe { std::env::set_var("FLEET_PEER_DOMAINS", "prisma.build") };
        assert!(valid_peer_url("https://cv-abc.fra.prisma.build"));
        assert!(valid_peer_url("https://prisma.build"));
        assert!(!valid_peer_url("https://attacker.example"));
        // suffix-matching must not accept a lookalike domain
        assert!(!valid_peer_url("https://evilprisma.build"));
        unsafe { std::env::remove_var("FLEET_PEER_DOMAINS") };
    }

    #[test]
    fn return_home_suppressed_at_fair_share() {
        // home kept 1 of its 2 shards; mean is ceil(4/4)=1; returning
        // would make 2 > 1 — the exact flap case.
        assert!(!return_home_allowed(1, 1, 4, 4));
    }

    #[test]
    fn drained_home_still_gets_refilled() {
        // ladder p3: the once-lagged instance owns nothing; the mean is
        // 1; refilling to 1 is exactly fair.
        assert!(return_home_allowed(0, 1, 4, 4));
    }

    #[test]
    fn fine_granularity_returns_normally() {
        // 16 shards over 4 instances: home at 3 takes its 4th back.
        assert!(return_home_allowed(3, 1, 16, 4));
        assert!(!return_home_allowed(4, 1, 16, 4));
    }

    #[test]
    fn multiple_pending_returns_share_the_budget() {
        // two overrides pointing at the same healthy home in one pass:
        // the second must count the first (16/4: home at 2, gains 1+1
        // fine; a third would breach).
        assert!(return_home_allowed(2, 1, 16, 4));
        assert!(return_home_allowed(2, 2, 16, 4));
        assert!(!return_home_allowed(2, 3, 16, 4));
    }

    #[test]
    fn empty_active_set_never_returns() {
        assert!(!return_home_allowed(0, 1, 4, 0));
    }

    #[test]
    fn target_must_be_well_under_the_threshold_not_merely_under_it() {
        // threshold/2 gate: a peer at 40s with a 60s threshold is not healthy
        let p = peers(&[("a", 5.0, 0), ("b", 5.0, 40)]);
        assert_eq!(pick_move_target(&p, "a", 60), None);
    }

    // Regression: victim was derived via shard_for_hash(lag_map_key), but
    // the lag map is keyed by storage_hash while shards are chosen by
    // stream_hash(name) - so it almost never matched and D3 never moved.
    #[test]
    fn victim_is_the_laggiest_shard_we_actually_serve() {
        let lags = vec![
            ("000".to_string(), 10),
            ("011".to_string(), 90),
            ("111".to_string(), 40),
        ];
        let served = vec!["000".to_string(), "111".to_string()];
        // 011 is laggier but we do not serve it
        assert_eq!(pick_victim_shard(&lags, &served).as_deref(), Some("111"));
    }

    #[test]
    fn no_victim_when_we_serve_nothing_with_lag() {
        let lags = vec![("011".to_string(), 90)];
        let served = vec!["000".to_string()];
        assert_eq!(pick_victim_shard(&lags, &served), None);
    }

    #[test]
    fn no_victim_when_nothing_lags() {
        let served = vec!["000".to_string()];
        assert_eq!(pick_victim_shard(&[], &served), None);
    }
}
