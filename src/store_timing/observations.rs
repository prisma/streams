//! Process-wide object-store measurements and their diagnostic snapshots.
use super::StoreResources;
use async_trait::async_trait;
use object_store::{Result, path::Path};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) const OPS: [&str; 7] = ["put", "mpu", "get", "head", "delete", "list", "copy"];
pub(crate) const CLASSES: [&str; 5] = ["wal", "manifest", "sst", "fleet", "other"];

const RING_CAP: usize = 16_384;
const SLOW_CAP: usize = 96;
const SLOW_MS: u64 = 300;

#[derive(Clone, Copy)]
struct Ev {
    ts_ms: u64,
    op: u8,
    class: u8,
    dur_us: u32,
    ok: bool,
}

struct SlowOp {
    ts_ms: u64,
    op: u8,
    class: u8,
    dur_ms: u64,
    ok: bool,
    path: String,
}

pub(crate) struct StoreStats {
    ring: Mutex<VecDeque<Ev>>,
    slow: Mutex<VecDeque<SlowOp>>,
    /// Outbound object-store ops in flight right now, instance-wide.
    pub inflight: AtomicI64,
    /// High-water mark; swapped down only by the /v1/debug/store sampler so
    /// heartbeats (which only load it) can't race the window.
    pub inflight_peak: AtomicI64,
}

pub(crate) fn stats() -> &'static StoreStats {
    static S: OnceLock<StoreStats> = OnceLock::new();
    S.get_or_init(|| StoreStats {
        ring: Mutex::new(VecDeque::with_capacity(RING_CAP)),
        slow: Mutex::new(VecDeque::with_capacity(SLOW_CAP)),
        inflight: AtomicI64::new(0),
        inflight_peak: AtomicI64::new(0),
    })
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn classify(path: &str) -> u8 {
    // Paths arrive fully prefixed (we wrap beneath PrefixStore), e.g.
    // pilot12/shards/root-3/wal/00000042.sst — substrings are reliable.
    if path.contains("/wal/") || path.starts_with("wal/") {
        0
    } else if path.contains("manifest") {
        1
    } else if path.contains("/compacted/") || path.ends_with(".sst") {
        2
    } else if path.contains("/fleet/") || path.contains("fleet/") || path.contains("routers/") {
        3
    } else {
        4
    }
}

pub(super) fn record(op: u8, class: u8, start: Instant, path: &str, ok: bool) {
    let dur_us = u32::try_from(start.elapsed().as_micros()).unwrap_or(u32::MAX);
    let ts_ms = now_ms();
    stats().record(
        Ev {
            ts_ms,
            op,
            class,
            dur_us,
            ok,
        },
        path,
    );
}

impl StoreStats {
    #[expect(
        clippy::unwrap_used,
        reason = "process store observation ring; poison may follow an interrupted sample update; refusing a partial sample preserves the diagnostic contract"
    )]
    fn cells(&self, cutoff: u64) -> HashMap<(u8, u8), CellSamples> {
        let ring = self.ring.lock().unwrap();
        let mut cells: HashMap<(u8, u8), CellSamples> = HashMap::new();
        for event in ring.iter().filter(|event| event.ts_ms >= cutoff) {
            let cell = cells.entry((event.op, event.class)).or_default();
            cell.client.push(event.dur_us);
            cell.errors += u64::from(!event.ok);
        }
        cells
    }

    #[expect(
        clippy::unwrap_used,
        reason = "process store observation rings; poison may follow an interrupted sample update; silently recovering would publish partial measurements"
    )]
    fn record(&self, event: Ev, path: &str) {
        let Ev {
            ts_ms,
            op,
            class,
            dur_us,
            ok,
        } = event;
        {
            let mut ring = self.ring.lock().unwrap();
            if ring.len() >= RING_CAP {
                ring.pop_front();
            }
            ring.push_back(Ev {
                ts_ms,
                op,
                class,
                dur_us,
                ok,
            });
        }
        let dur_ms = (dur_us / 1000) as u64;
        // Slow ring is duration-only: routine NotFounds (GC boundary probes,
        // descriptor misses) are counted in the per-cell err field instead.
        if dur_ms >= SLOW_MS {
            let mut slow = self.slow.lock().unwrap();
            if slow.len() >= SLOW_CAP {
                slow.pop_front();
            }
            let tail: String = path
                .chars()
                .rev()
                .take(48)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            slow.push_back(SlowOp {
                ts_ms,
                op,
                class,
                dur_ms,
                ok,
                path: tail,
            });
        }
    }
}

/// RAII: outbound-op guard — gauge up on create, down on drop, and records
/// the latency sample exactly once (on explicit finish or on drop).
pub(super) struct OpGuard {
    op: u8,
    class: u8,
    start: Instant,
    path: String,
    done: bool,
}

impl OpGuard {
    pub(super) fn new(op: u8, path: &Path) -> Self {
        let s = stats();
        let now = s.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        s.inflight_peak.fetch_max(now, Ordering::Relaxed);
        let p = path.as_ref().to_string();
        OpGuard {
            op,
            class: classify(&p),
            start: Instant::now(),
            path: p,
            done: false,
        }
    }
    pub(super) fn finish(mut self, ok: bool) {
        self.done = true;
        stats().inflight.fetch_sub(1, Ordering::Relaxed);
        record(self.op, self.class, self.start, &self.path, ok);
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        if !self.done {
            // Dropped mid-flight (cancelled future / abandoned stream):
            // still a completed outbound episode for our purposes.
            stats().inflight.fetch_sub(1, Ordering::Relaxed);
            record(self.op, self.class, self.start, &self.path, false);
        }
    }
}

/// The retained rings have at most 16,384 samples. Callers use 50, 90 or 99;
/// rounding the rational rank to nearest matches the former floating ranks.
pub(super) fn percentile_index(len: usize, percent: usize) -> usize {
    (len.saturating_sub(1) * percent + 50) / 100
}

fn pct(sorted_us: &[u32], percent: usize) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    u64::from(sorted_us[percentile_index(sorted_us.len(), percent)] / 1000)
}

#[derive(Default)]
struct CellSamples {
    client: Vec<u32>,
    errors: u64,
    server: Vec<u32>,
}

impl CellSamples {
    fn into_json(mut self) -> serde_json::Value {
        self.client.sort_unstable();
        let mut cell = serde_json::Map::from_iter([
            ("n".into(), serde_json::json!(self.client.len())),
            ("err".into(), serde_json::json!(self.errors)),
            ("p50_ms".into(), serde_json::json!(pct(&self.client, 50))),
            ("p90_ms".into(), serde_json::json!(pct(&self.client, 90))),
            ("p99_ms".into(), serde_json::json!(pct(&self.client, 99))),
            (
                "max_ms".into(),
                serde_json::json!(u64::from(self.client.last().copied().unwrap_or(0) / 1000)),
            ),
        ]);
        if !self.server.is_empty() {
            self.server.sort_unstable();
            cell.insert("sn".into(), serde_json::json!(self.server.len()));
            cell.insert("sp50_ms".into(), serde_json::json!(pct(&self.server, 50)));
            cell.insert("sp99_ms".into(), serde_json::json!(pct(&self.server, 99)));
        }
        serde_json::Value::Object(cell)
    }
}

/// Snapshot for /v1/debug/store: per (op,class) percentiles over
/// `window_secs`, the slow-op ring, and the outbound gauge.
#[expect(
    clippy::unwrap_used,
    reason = "process slow-operation ring; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
)]
pub(crate) fn snapshot(
    window_secs: u64,
    swap_peak: bool,
    resources: &StoreResources,
) -> serde_json::Value {
    let s = stats();
    let cutoff = now_ms().saturating_sub(window_secs * 1000);
    let mut cells = s.cells(cutoff);
    let http = http_stats();
    http.collect_server_cells(cutoff, &mut cells);
    let ops: serde_json::Map<String, serde_json::Value> = cells
        .into_iter()
        .map(|((op, class), cell)| {
            (
                format!("{}:{}", OPS[usize::from(op)], CLASSES[usize::from(class)]),
                cell.into_json(),
            )
        })
        .collect();
    let regions = http.region_snapshot();
    let slow: Vec<_> = {
        let sl = s.slow.lock().unwrap();
        let now = now_ms();
        sl.iter()
            .rev()
            .take(40)
            .map(|o| {
                serde_json::json!({
                    "ago_s": (now.saturating_sub(o.ts_ms)) / 1000,
                    "op": OPS[o.op as usize],
                    "class": CLASSES[o.class as usize],
                    "ms": o.dur_ms,
                    "ok": o.ok,
                    "path": o.path,
                })
            })
            .collect()
    };
    let inflight_now = s.inflight.load(Ordering::Relaxed);
    let peak = if swap_peak {
        s.inflight_peak.swap(inflight_now, Ordering::Relaxed)
    } else {
        s.inflight_peak.load(Ordering::Relaxed)
    };
    let timers = super::sentinels::snapshot(cutoff);
    let st = wal_read_storm(window_secs);
    let storm = serde_json::json!({
        "wal_gets": st.wal_gets,
        "sst_puts": st.sst_puts,
        "wal_deletes": st.wal_deletes,
        "stalled": st.stalled,
    });
    serde_json::json!({
        "ts_ms": now_ms(),
        "window_secs": window_secs,
        "out_inflight_now": inflight_now,
        "out_inflight_peak": peak,
        "bulk_gate": resources.bulk_stats(),
        "timer_thread": timers.thread,
        "timer_tokio": timers.tokio,
        "steal_pct": timers.steal_pct,
        "served_from": regions.served_from,
        "served_from_by_class": regions.served_from_by_class,
        "wal_read_storm": storm,
        // Reopen-storm visibility (sharddir.rs): started climbing while
        // completed stays flat = the eu-central-1 wedge shape.
        "shard_opens": crate::sharddir::stats_json(),
        "ops": ops,
        "slow": slow,
    })
}

// ---- Server-Timing sniffer -------------------------------------------------
// The rings above time the whole request from our side; Tigris also reports
// its *internal* processing time per response (`Server-Timing: total;dur=N`)
// plus which region served it (`x-tigris-served-from`). object_store never
// surfaces response headers, so we interpose at its HttpService seam: a
// connector wrapping the stock reqwest one. wall − server ≈ network path
// (TLS, egress NAT, PoP routing) and finally splits provider-internal tail
// from path tail in production, per op class.

struct HttpEv {
    ts_ms: u64,
    op: u8,
    class: u8,
    server_us: u32,
}

#[derive(Hash, PartialEq, Eq)]
struct RegionCell {
    region: String,
    op: u8,
    class: u8,
}

struct RegionSnapshot {
    // mt-lint: allow(name-keyed-map): provider region labels index diagnostic response counts, not tenant stream names
    served_from: BTreeMap<String, u64>,
    // mt-lint: allow(name-keyed-map): provider region and operation labels index diagnostic response counts, not tenant stream names
    served_from_by_class: BTreeMap<String, BTreeMap<String, u64>>,
}

struct HttpStats {
    ring: Mutex<VecDeque<HttpEv>>,
    /// One cumulative count per response. Flat regional totals are derived
    /// from the same snapshot, so the two diagnostic views cannot diverge.
    regions: Mutex<HashMap<RegionCell, u64>>,
}

impl HttpStats {
    fn new() -> Self {
        Self {
            ring: Mutex::new(VecDeque::with_capacity(RING_CAP)),
            regions: Mutex::new(HashMap::new()),
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "process HTTP timing ring; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
    )]
    fn record_timing(&self, event: HttpEv) {
        let mut ring = self.ring.lock().unwrap();
        if ring.len() >= RING_CAP {
            ring.pop_front();
        }
        ring.push_back(event);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "process response-region counters; poison may follow an interrupted count update; recovery would present partial diagnostic state as valid"
    )]
    fn record_region(&self, region: &str, op: u8, class: u8) {
        *self
            .regions
            .lock()
            .unwrap()
            .entry(RegionCell {
                region: region.into(),
                op,
                class,
            })
            .or_default() += 1;
    }

    #[expect(
        clippy::unwrap_used,
        reason = "process HTTP timing ring; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
    )]
    fn collect_server_cells(&self, cutoff: u64, cells: &mut HashMap<(u8, u8), CellSamples>) {
        let ring = self.ring.lock().unwrap();
        for event in ring.iter().filter(|event| event.ts_ms >= cutoff) {
            cells
                .entry((event.op, event.class))
                .or_default()
                .server
                .push(event.server_us);
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "process response-region counters; poison may follow an interrupted count update; both diagnostic views must refuse that state together"
    )]
    fn region_snapshot(&self) -> RegionSnapshot {
        let counts = self.regions.lock().unwrap();
        let mut snapshot = RegionSnapshot {
            served_from: BTreeMap::new(),
            served_from_by_class: BTreeMap::new(),
        };
        for (key, &count) in counts.iter() {
            *snapshot.served_from.entry(key.region.clone()).or_default() += count;
            let cell = format!(
                "{}:{}",
                OPS[usize::from(key.op)],
                CLASSES[usize::from(key.class)]
            );
            snapshot
                .served_from_by_class
                .entry(key.region.clone())
                .or_default()
                .insert(cell, count);
        }
        snapshot
    }
}

fn http_stats() -> &'static HttpStats {
    static H: OnceLock<HttpStats> = OnceLock::new();
    H.get_or_init(HttpStats::new)
}

/// `Server-Timing: total;dur=12.4` (possibly one metric among several) → µs.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Server-Timing decimal input; the existing clamp and cast truncate fractional microseconds and map NaN to zero; integer parsing would reject valid fractional provider durations"
)]
pub(super) fn parse_server_timing_us(v: &str) -> Option<u32> {
    for metric in v.split(',') {
        let mut segs = metric.trim().split(';');
        if segs.next().map(str::trim) != Some("total") {
            continue;
        }
        for seg in segs {
            if let Some(ms) = seg.trim().strip_prefix("dur=")
                && let Ok(ms) = ms.trim().parse::<f64>()
            {
                return Some((ms * 1000.0).clamp(0.0, u32::MAX as f64) as u32);
            }
        }
    }
    None
}

/// Map an HTTP request onto the OPS index. Class granularity is what
/// matters (put:wal vs get:sst); op mapping is best-effort.
pub(super) fn http_op(method: &str, query: Option<&str>) -> u8 {
    let q = query.unwrap_or("");
    match method {
        "PUT" if q.contains("partNumber") => 1,
        "PUT" => 0,
        "POST" => 1, // multipart create/complete
        "GET" if q.contains("list-type") => 5,
        "HEAD" => 3,
        "DELETE" => 4,
        _ => 2,
    }
}

#[derive(Debug)]
struct SniffService {
    inner: object_store::client::HttpClient,
}

#[async_trait]
impl object_store::client::HttpService for SniffService {
    async fn call(
        &self,
        req: object_store::client::HttpRequest,
    ) -> std::result::Result<object_store::client::HttpResponse, object_store::client::HttpError>
    {
        let op = http_op(req.method().as_str(), req.uri().query());
        // LIST carries its path in the query (?prefix=…), not the URL path.
        let class = if op == 5 {
            req.uri()
                .query()
                .and_then(|q| {
                    q.split('&')
                        .find_map(|kv| kv.strip_prefix("prefix="))
                        .map(|p| classify(&p.replace("%2F", "/")))
                })
                .unwrap_or(4)
        } else {
            classify(req.uri().path())
        };
        let resp = self.inner.execute(req).await?;
        let h = http_stats();
        if let Some(us) = resp
            .headers()
            .get("server-timing")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_server_timing_us)
        {
            h.record_timing(HttpEv {
                ts_ms: now_ms(),
                op,
                class,
                server_us: us,
            });
        }
        if let Some(region) = resp
            .headers()
            .get("x-tigris-served-from")
            .and_then(|v| v.to_str().ok())
        {
            h.record_region(region, op, class);
        }
        Ok(resp)
    }
}

/// Install with `AmazonS3Builder::with_http_connector(SniffConnector)`.
#[derive(Debug, Default)]
pub(crate) struct SniffConnector;

impl object_store::client::HttpConnector for SniffConnector {
    fn connect(
        &self,
        options: &object_store::ClientOptions,
    ) -> Result<object_store::client::HttpClient> {
        let inner = object_store::client::ReqwestConnector::default().connect(options)?;
        Ok(object_store::client::HttpClient::new(SniffService {
            inner,
        }))
    }
}

/// Counts behind the compaction-stall signal, over a trailing window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WalReadStorm {
    pub wal_gets: u64,
    pub sst_puts: u64,
    pub wal_deletes: u64,
    /// Readers are hammering the WAL while nothing compacts or trims it.
    pub stalled: bool,
}

/// Minimum WAL GETs in the window before the shape means anything. Below
/// this an idle instance (no compaction because there is nothing to
/// compact) looks identical to a stalled one.
const STORM_MIN_WAL_GETS: u64 = 500;

/// Detect the failure that took eu-central-1 out of the 2026-07-26 soak
/// (docs/SOAK-REGIONS.md): compaction stops, so the WAL is never trimmed;
/// readers then scan an ever-growing set of WAL SSTs directly; those reads
/// consume the outbound budget and starve the appends that would have
/// advanced the WAL. It is self-reinforcing, and by the time throughput
/// dies it has been visible in these counters for minutes.
///
/// The shape is unmistakable and cheap to spot: thousands of `get:wal`
/// against zero `put:sst` and zero `delete:wal`.
#[expect(
    clippy::unwrap_used,
    reason = "process store-stall detector; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
)]
pub(crate) fn wal_read_storm(window_secs: u64) -> WalReadStorm {
    let s = stats();
    let cutoff = now_ms().saturating_sub(window_secs * 1000);
    let ring = s.ring.lock().unwrap();
    tally_storm(
        ring.iter()
            .filter(|ev| ev.ts_ms >= cutoff)
            .map(|ev| (ev.op, ev.class)),
    )
}

/// The decision, split out from the ring so it is testable without touching
/// the process-wide stats singleton.
pub(super) fn tally_storm(ops: impl Iterator<Item = (u8, u8)>) -> WalReadStorm {
    let (mut wal_gets, mut sst_puts, mut wal_deletes) = (0u64, 0u64, 0u64);
    for oc in ops {
        match oc {
            (2, 0) => wal_gets += 1,    // get:wal
            (0, 2) => sst_puts += 1,    // put:sst
            (4, 0) => wal_deletes += 1, // delete:wal
            _ => {}
        }
    }
    WalReadStorm {
        wal_gets,
        sst_puts,
        wal_deletes,
        stalled: wal_gets >= STORM_MIN_WAL_GETS && sst_puts == 0 && wal_deletes == 0,
    }
}

/// Cheap scalar summary for heartbeats: WAL-PUT p50/p99 over the trailing
/// 15 s plus the outbound gauge (non-destructive peak read).
#[expect(
    clippy::unwrap_used,
    reason = "process store heartbeat; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
)]
pub(crate) fn heartbeat_summary() -> (u64, u64, i64, i64) {
    let s = stats();
    let cutoff = now_ms().saturating_sub(15_000);
    let mut wal: Vec<u32> = Vec::new();
    {
        let ring = s.ring.lock().unwrap();
        for ev in ring.iter() {
            if ev.ts_ms >= cutoff && ev.op == 0 && ev.class == 0 {
                wal.push(ev.dur_us);
            }
        }
    }
    wal.sort_unstable();
    (
        pct(&wal, 50),
        pct(&wal, 99),
        s.inflight.load(Ordering::Relaxed),
        s.inflight_peak.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests;
