//! O14a diagnostic: per-operation object-store latency, recorded at the
//! object_store client boundary — the last point that is *ours* before the
//! platform's egress path and Tigris. Splits ack excursions three ways:
//!   - WAL PUT tail spikes alone            → provider (Tigris) tail latency
//!   - every op class spikes together AND the outbound in-flight gauge sits
//!     at the platform egress budget (~50)  → egress-slot exhaustion
//!   - acks spike with no store-side spike  → our scheduling/watermark path
//!
//! One global registry (all stores, all roles) because the egress budget is
//! per *instance*: only the summed outbound concurrency means anything.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};

pub const OPS: [&str; 7] = ["put", "mpu", "get", "head", "delete", "list", "copy"];
pub const CLASSES: [&str; 5] = ["wal", "manifest", "sst", "fleet", "other"];

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

pub struct StoreStats {
    ring: Mutex<VecDeque<Ev>>,
    slow: Mutex<VecDeque<SlowOp>>,
    /// Outbound object-store ops in flight right now, instance-wide.
    pub inflight: AtomicI64,
    /// High-water mark; swapped down only by the /v1/debug/store sampler so
    /// heartbeats (which only load it) can't race the window.
    pub inflight_peak: AtomicI64,
}

/// Cumulative object-store GET count and bytes fetched.
///
/// R23-6: the deepest CHAOS-5 fact is that reading ~4 MiB of absorbable
/// data takes 21-42 s. Concurrency only overlapped those slow reads
/// while multiplying unmodelled memory, so the number that actually
/// explains the ceiling is READ AMPLIFICATION — bytes fetched from the
/// object store per useful frame byte returned. The absorber snapshots
/// these around its read phase; the delta is that gather's cost.
pub static GET_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static GET_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Optional instance-wide cap on concurrent object-store ops
/// (STORE_MAX_CONCURRENT, 0/unset = off). Run-12 found ack excursions are
/// broad client-side slowdowns: HTTP/1.1 to Tigris + 4 s pool pruning means
/// every op burst past the warm set pays a fresh TLS handshake through the
/// egress NAT — the outbound edition of the platform's Conduit bug. Capping
/// concurrency keeps a small connection set continuously busy (never idle,
/// never pruned, no handshake storms); bursts queue for milliseconds
/// instead. List/delete streams are exempt (long-lived, low-volume).
fn sem() -> Option<&'static tokio::sync::Semaphore> {
    static S: OnceLock<Option<tokio::sync::Semaphore>> = OnceLock::new();
    S.get_or_init(|| {
        let n: usize = std::env::var("STORE_MAX_CONCURRENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if n == 0 {
            None
        } else {
            Some(tokio::sync::Semaphore::new(n))
        }
    })
    .as_ref()
}

async fn permit() -> Option<tokio::sync::SemaphorePermit<'static>> {
    match sem() {
        // acquire() only errs on close; we never close it
        Some(s) => s.acquire().await.ok(),
        None => None,
    }
}

pub fn stats() -> &'static StoreStats {
    static S: OnceLock<StoreStats> = OnceLock::new();
    S.get_or_init(|| StoreStats {
        ring: Mutex::new(VecDeque::with_capacity(RING_CAP)),
        slow: Mutex::new(VecDeque::with_capacity(SLOW_CAP)),
        inflight: AtomicI64::new(0),
        inflight_peak: AtomicI64::new(0),
    })
}

/// Run-13 discriminator: excursions are class-agnostic at low CPU and low
/// outbound concurrency (v18 killed the burst/handshake theory), so either
/// the vCPU itself stalls (host steal / descheduling) or the network path
/// queues. Two 10 ms-cadence sentinels tell them apart: a raw OS thread
/// (immune to our event loop) and a tokio task (subject to it), plus
/// /proc/stat steal ticks. Drift spikes on BOTH sentinels co-timed with
/// excursions → the VM stalled; tokio-only → our loop starved; neither →
/// the network path is the queue.
struct DriftRings {
    thread: Mutex<VecDeque<(u64, u32)>>, // (ts_ms, drift_us)
    tokio: Mutex<VecDeque<(u64, u32)>>,
    steal: Mutex<VecDeque<(u64, u64, u64)>>, // (ts_ms, steal_ticks, total_ticks)
}

fn drift() -> &'static DriftRings {
    static D: OnceLock<DriftRings> = OnceLock::new();
    D.get_or_init(|| DriftRings {
        thread: Mutex::new(VecDeque::with_capacity(4096)),
        tokio: Mutex::new(VecDeque::with_capacity(4096)),
        steal: Mutex::new(VecDeque::with_capacity(64)),
    })
}

fn push_drift(ring: &Mutex<VecDeque<(u64, u32)>>, drift_us: u32) {
    let mut r = ring.lock().unwrap();
    if r.len() >= 4096 {
        r.pop_front();
    }
    r.push_back((now_ms(), drift_us));
}

/// Read (steal_ticks, total_ticks) from /proc/stat's aggregate cpu line.
fn read_steal() -> Option<(u64, u64)> {
    let s = std::fs::read_to_string("/proc/stat").ok()?;
    let line = s.lines().next()?;
    let f: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if f.len() < 8 {
        return None;
    }
    Some((f[7], f.iter().sum()))
}

/// Spawn both sentinels; call once at startup.
pub fn spawn_sentinels() {
    std::thread::Builder::new()
        .name("drift-sentinel".into())
        .spawn(|| {
            loop {
                let t0 = Instant::now();
                std::thread::sleep(std::time::Duration::from_millis(10));
                let over = t0
                    .elapsed()
                    .as_micros()
                    .saturating_sub(10_000)
                    .min(u32::MAX as u128);
                push_drift(&drift().thread, over as u32);
            }
        })
        .ok();
    tokio::spawn(async {
        let mut last_steal = read_steal();
        let mut ticks: u32 = 0;
        loop {
            let t0 = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let over = t0
                .elapsed()
                .as_micros()
                .saturating_sub(10_000)
                .min(u32::MAX as u128);
            push_drift(&drift().tokio, over as u32);
            ticks += 1;
            if ticks.is_multiple_of(100) {
                // ~1 s cadence: cumulative steal/total ticks for window deltas
                if let Some((st, tot)) = read_steal() {
                    let mut r = drift().steal.lock().unwrap();
                    if r.len() >= 64 {
                        r.pop_front();
                    }
                    r.push_back((now_ms(), st, tot));
                    last_steal = Some((st, tot));
                }
                let _ = &last_steal;
            }
        }
    });
}

fn drift_stats(ring: &Mutex<VecDeque<(u64, u32)>>, cutoff: u64) -> serde_json::Value {
    let mut v: Vec<u32> = ring
        .lock()
        .unwrap()
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .map(|(_, d)| *d)
        .collect();
    v.sort_unstable();
    let over50 = v.iter().filter(|d| **d >= 50_000).count();
    let idx99 = if v.is_empty() {
        0
    } else {
        ((v.len() as f64 - 1.0) * 0.99).round() as usize
    };
    serde_json::json!({
        "n": v.len(),
        "p50_us": v.get(v.len() / 2).copied().unwrap_or(0),
        "p99_us": v.get(idx99).copied().unwrap_or(0),
        "max_us": v.last().copied().unwrap_or(0),
        "over_50ms": over50,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
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

fn record(op: u8, class: u8, start: Instant, path: &str, ok: bool) {
    let dur_us = start.elapsed().as_micros().min(u32::MAX as u128) as u32;
    let ts_ms = now_ms();
    let s = stats();
    {
        let mut ring = s.ring.lock().unwrap();
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
        let mut slow = s.slow.lock().unwrap();
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

/// RAII: outbound-op guard — gauge up on create, down on drop, and records
/// the latency sample exactly once (on explicit finish or on drop).
struct OpGuard {
    op: u8,
    class: u8,
    start: Instant,
    path: String,
    done: bool,
}

impl OpGuard {
    fn new(op: u8, path: &Path) -> Self {
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
    fn finish(mut self, ok: bool) {
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

/// ObjectStore wrapper that times every operation. Sits *beneath*
/// PrefixStore so it sees final (fully-prefixed) paths.
#[derive(Debug)]
pub struct TimingStore<T: ObjectStore> {
    inner: T,
}

impl<T: ObjectStore> TimingStore<T> {
    pub fn new(inner: T) -> Self {
        TimingStore { inner }
    }
}

impl<T: ObjectStore> std::fmt::Display for TimingStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimingStore({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for TimingStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _p = permit().await;
        let g = OpGuard::new(0, location);
        let r = self.inner.put_opts(location, payload, opts).await;
        g.finish(r.is_ok());
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let _p = permit().await;
        let g = OpGuard::new(1, location);
        match self.inner.put_multipart_opts(location, opts).await {
            Ok(up) => Ok(Box::new(TimedMpu {
                inner: up,
                guard: Some(g),
            })),
            Err(e) => {
                g.finish(false);
                Err(e)
            }
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // 0.14 routes the ext-method `head()` through get_opts(head: true).
        let _p = permit().await;
        let g = OpGuard::new(if options.head { 3 } else { 2 }, location);
        let r = self.inner.get_opts(location, options).await;
        // GetResult still streams the body afterwards; timing to first byte
        // is what the egress path gates on, and it keeps the guard simple.
        // A 304 on a conditional GET is a successful revalidation (the
        // registry's TTL refresh), not an error.
        let ok = r.is_ok() || matches!(&r, Err(object_store::Error::NotModified { .. }));
        g.finish(ok);
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _p = permit().await;
        let g = OpGuard::new(2, location);
        let r = self.inner.get_ranges(location, ranges).await;
        g.finish(r.is_ok());
        // R23-6: range reads are how the absorber pulls frames out of the
        // object-store-backed LSM, so this is where amplification shows.
        if let Ok(parts) = &r {
            GET_COUNT.fetch_add(parts.len() as u64, Ordering::Relaxed);
            GET_BYTES.fetch_add(
                parts.iter().map(|b| b.len() as u64).sum::<u64>(),
                Ordering::Relaxed,
            );
        }
        r
    }

    // 0.14 made single-shot delete an ext method over delete_stream: each
    // yielded path is one completed delete; inter-yield gaps approximate
    // per-delete latency (deletes are never the ack path — class + gauge
    // matter here, exact durations don't).
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let s = stats();
        let now = s.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        s.inflight_peak.fetch_max(now, Ordering::Relaxed);
        TimedDeleteStream {
            inner: self.inner.delete_stream(locations),
            last: Instant::now(),
            open: true,
        }
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let g = OpGuard::new(5, prefix.unwrap_or(&Path::default()));
        TimedStream {
            inner: self.inner.list(prefix),
            guard: Some(g),
        }
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _p = permit().await;
        let g = OpGuard::new(5, prefix.unwrap_or(&Path::default()));
        let r = self.inner.list_with_delimiter(prefix).await;
        g.finish(r.is_ok());
        r
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _p = permit().await;
        let g = OpGuard::new(6, from);
        let r = self.inner.copy_opts(from, to, options).await;
        g.finish(r.is_ok());
        r
    }
}

/// Times a list stream over its whole life (streams hold the outbound
/// connection until exhausted or dropped).
struct TimedStream<S> {
    inner: S,
    guard: Option<OpGuard>,
}

impl<S: Stream<Item = Result<ObjectMeta>> + Unpin> Stream for TimedStream<S> {
    type Item = Result<ObjectMeta>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<ObjectMeta>>> {
        let poll = self.inner.poll_next_unpin(cx);
        if let std::task::Poll::Ready(None) = poll
            && let Some(g) = self.guard.take()
        {
            g.finish(true);
        }
        poll
    }
}

/// delete_stream wrapper: gauge held for the stream's life, one recorded
/// event per completed (yielded) delete.
struct TimedDeleteStream {
    inner: BoxStream<'static, Result<Path>>,
    last: Instant,
    open: bool,
}

impl Stream for TimedDeleteStream {
    type Item = Result<Path>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Path>>> {
        let poll = self.inner.poll_next_unpin(cx);
        match &poll {
            std::task::Poll::Ready(Some(item)) => {
                let start = self.last;
                self.last = Instant::now();
                let (p, ok) = match item {
                    Ok(p) => (p.as_ref().to_string(), true),
                    Err(_) => (String::new(), false),
                };
                record(4, classify(&p), start, &p, ok);
            }
            std::task::Poll::Ready(None) => {
                if self.open {
                    self.open = false;
                    stats().inflight.fetch_sub(1, Ordering::Relaxed);
                }
            }
            _ => {}
        }
        poll
    }
}

impl Drop for TimedDeleteStream {
    fn drop(&mut self) {
        if self.open {
            self.open = false;
            stats().inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct TimedMpu {
    inner: Box<dyn MultipartUpload>,
    guard: Option<OpGuard>,
}

impl std::fmt::Debug for TimedMpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimedMpu({:?})", self.inner)
    }
}

#[async_trait]
impl MultipartUpload for TimedMpu {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let r = self.inner.complete().await;
        if let Some(g) = self.guard.take() {
            g.finish(r.is_ok());
        }
        r
    }
    async fn abort(&mut self) -> Result<()> {
        let r = self.inner.abort().await;
        if let Some(g) = self.guard.take() {
            g.finish(false);
        }
        r
    }
}

/// Percentiles for one (op, class) cell over the trailing window.
fn pct(sorted_us: &[u32], q: f64) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * q).round() as usize;
    (sorted_us[idx] / 1000) as u64
}

/// Snapshot for /v1/debug/store: per (op,class) percentiles over
/// `window_secs`, the slow-op ring, and the outbound gauge.
pub fn snapshot(window_secs: u64, swap_peak: bool) -> serde_json::Value {
    let s = stats();
    let cutoff = now_ms().saturating_sub(window_secs * 1000);
    let mut cells: std::collections::HashMap<(u8, u8), Vec<u32>> = std::collections::HashMap::new();
    let mut errs: std::collections::HashMap<(u8, u8), u64> = std::collections::HashMap::new();
    {
        let ring = s.ring.lock().unwrap();
        for ev in ring.iter() {
            if ev.ts_ms >= cutoff {
                cells.entry((ev.op, ev.class)).or_default().push(ev.dur_us);
                if !ev.ok {
                    *errs.entry((ev.op, ev.class)).or_default() += 1;
                }
            }
        }
    }
    // Server-side (Tigris-internal) durations from the HTTP sniffer, same
    // (op,class) keying. sp* ≈ their processing; p* − sp* ≈ network path.
    let mut scells: std::collections::HashMap<(u8, u8), Vec<u32>> =
        std::collections::HashMap::new();
    {
        let ring = http_stats().ring.lock().unwrap();
        for ev in ring.iter() {
            if ev.ts_ms >= cutoff {
                scells
                    .entry((ev.op, ev.class))
                    .or_default()
                    .push(ev.server_us);
            }
        }
    }
    let mut ops = serde_json::Map::new();
    let mut keys: Vec<_> = cells.keys().chain(scells.keys()).copied().collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        let mut v = cells.remove(&k).unwrap_or_default();
        v.sort_unstable();
        let name = format!("{}:{}", OPS[k.0 as usize], CLASSES[k.1 as usize]);
        let mut cell = serde_json::json!({
            "n": v.len(),
            "err": errs.get(&k).copied().unwrap_or(0),
            "p50_ms": pct(&v, 0.50),
            "p90_ms": pct(&v, 0.90),
            "p99_ms": pct(&v, 0.99),
            "max_ms": (v.last().copied().unwrap_or(0) / 1000) as u64,
        });
        if let Some(mut sv) = scells.remove(&k) {
            sv.sort_unstable();
            let c = cell.as_object_mut().unwrap();
            c.insert("sn".into(), serde_json::json!(sv.len()));
            c.insert("sp50_ms".into(), serde_json::json!(pct(&sv, 0.50)));
            c.insert("sp99_ms".into(), serde_json::json!(pct(&sv, 0.99)));
        }
        ops.insert(name, cell);
    }
    let served_from_by_class = {
        let m = http_stats().served_from_class.lock().unwrap();
        let mut out: std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>> =
            Default::default();
        for ((region, op, class), n) in m.iter() {
            let cell = format!("{}:{}", OPS[*op as usize], CLASSES[*class as usize]);
            *out.entry(region.clone())
                .or_default()
                .entry(cell)
                .or_default() += n;
        }
        serde_json::to_value(out).unwrap_or(serde_json::Value::Null)
    };
    let served_from: std::collections::BTreeMap<String, u64> = http_stats()
        .served_from
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
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
    // steal% over the window: delta of the two cumulative tick samples
    // bracketing the cutoff
    let steal_pct = {
        let r = drift().steal.lock().unwrap();
        let inside: Vec<_> = r.iter().filter(|(ts, _, _)| *ts >= cutoff).collect();
        match (inside.first(), inside.last()) {
            (Some((_, s0, t0)), Some((_, s1, t1))) if t1 > t0 => {
                ((s1 - s0) as f64 / (t1 - t0) as f64 * 1000.0).round() / 10.0
            }
            _ => -1.0,
        }
    };
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
        "timer_thread": drift_stats(&drift().thread, cutoff),
        "timer_tokio": drift_stats(&drift().tokio, cutoff),
        "steal_pct": steal_pct,
        "served_from": served_from,
        "served_from_by_class": served_from_by_class,
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

struct HttpStats {
    ring: Mutex<VecDeque<HttpEv>>,
    served_from: Mutex<std::collections::HashMap<String, u64>>,
    /// (region, op, class) → count. The flat map above answers "how much
    /// went where"; this one answers "WHAT went where" — during the
    /// eu-central-1 ord1 window we could not tell whether the remote PoP
    /// was serving everything (a routing/DNS problem) or only the
    /// CAS/manifest subset (a metadata-placement problem).
    served_from_class: Mutex<std::collections::HashMap<(String, u8, u8), u64>>,
}

fn http_stats() -> &'static HttpStats {
    static H: OnceLock<HttpStats> = OnceLock::new();
    H.get_or_init(|| HttpStats {
        ring: Mutex::new(VecDeque::with_capacity(RING_CAP)),
        served_from: Mutex::new(std::collections::HashMap::new()),
        served_from_class: Mutex::new(std::collections::HashMap::new()),
    })
}

/// `Server-Timing: total;dur=12.4` (possibly one metric among several) → µs.
fn parse_server_timing_us(v: &str) -> Option<u32> {
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
fn http_op(method: &str, query: Option<&str>) -> u8 {
    let q = query.unwrap_or("");
    match method {
        "PUT" if q.contains("partNumber") => 1,
        "PUT" => 0,
        "POST" => 1, // multipart create/complete
        "GET" if q.contains("list-type") => 5,
        "GET" => 2,
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
            let mut ring = h.ring.lock().unwrap();
            if ring.len() >= RING_CAP {
                ring.pop_front();
            }
            ring.push_back(HttpEv {
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
            *h.served_from
                .lock()
                .unwrap()
                .entry(region.to_string())
                .or_default() += 1;
            *h.served_from_class
                .lock()
                .unwrap()
                .entry((region.to_string(), op, class))
                .or_default() += 1;
        }
        Ok(resp)
    }
}

/// Install with `AmazonS3Builder::with_http_connector(SniffConnector)`.
#[derive(Debug, Default)]
pub struct SniffConnector;

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
pub struct WalReadStorm {
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
pub fn wal_read_storm(window_secs: u64) -> WalReadStorm {
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
fn tally_storm(ops: impl Iterator<Item = (u8, u8)>) -> WalReadStorm {
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
pub fn heartbeat_summary() -> (u64, u64, i64, i64) {
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
        pct(&wal, 0.50),
        pct(&wal, 0.99),
        s.inflight.load(Ordering::Relaxed),
        s.inflight_peak.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a window: `n` copies of `(op, class)`.
    fn ops(spec: &[((u8, u8), usize)]) -> Vec<(u8, u8)> {
        spec.iter()
            .flat_map(|(oc, n)| std::iter::repeat(*oc).take(*n))
            .collect()
    }

    const GET_WAL: (u8, u8) = (2, 0);
    const PUT_SST: (u8, u8) = (0, 2);
    const DEL_WAL: (u8, u8) = (4, 0);
    const PUT_WAL: (u8, u8) = (0, 0);

    #[test]
    fn wal_read_storm_fires_on_the_eu_central_shape() {
        // The real 60 s window that took eu-central-1 out of the soak:
        // 12,666 get:wal, no put:sst, no delete:wal (docs/SOAK-REGIONS.md).
        let w = tally_storm(ops(&[(GET_WAL, 12_666), (PUT_WAL, 5)]).into_iter());
        assert!(w.stalled, "{w:?}");
        assert_eq!(w.wal_gets, 12_666);
        assert_eq!((w.sst_puts, w.wal_deletes), (0, 0));
    }

    #[test]
    fn wal_read_storm_stays_quiet_on_a_healthy_window() {
        // ap-northeast-1's window in the same run: reads come from SSTs and
        // the WAL is being trimmed.
        let w = tally_storm(
            ops(&[
                (PUT_WAL, 1_242),
                (PUT_SST, 132),
                (DEL_WAL, 1_255),
                (GET_WAL, 40),
            ])
            .into_iter(),
        );
        assert!(!w.stalled, "{w:?}");
    }

    #[test]
    fn wal_read_storm_ignores_an_idle_instance() {
        // No compaction and no trimming, because there is nothing to do.
        // Without the floor this reads identically to a stall.
        let w = tally_storm(ops(&[(GET_WAL, 12)]).into_iter());
        assert!(!w.stalled, "{w:?}");
    }

    #[test]
    fn wal_read_storm_clears_as_soon_as_trimming_resumes() {
        // A single delete:wal in the window is enough: the loop is turning.
        let w = tally_storm(ops(&[(GET_WAL, 12_666), (DEL_WAL, 1)]).into_iter());
        assert!(!w.stalled, "{w:?}");
    }

    #[test]
    fn server_timing_parse() {
        assert_eq!(parse_server_timing_us("total;dur=12.4"), Some(12_400));
        assert_eq!(
            parse_server_timing_us("cache;desc=hit, total;dur=3"),
            Some(3_000)
        );
        assert_eq!(parse_server_timing_us("total; dur=0.5"), Some(500));
        assert_eq!(parse_server_timing_us("edge;dur=9"), None);
        assert_eq!(parse_server_timing_us("garbage"), None);
    }

    #[test]
    fn http_op_mapping() {
        assert_eq!(http_op("PUT", None), 0);
        assert_eq!(http_op("PUT", Some("partNumber=2&uploadId=x")), 1);
        assert_eq!(http_op("GET", Some("list-type=2&prefix=a")), 5);
        assert_eq!(http_op("GET", None), 2);
        assert_eq!(http_op("DELETE", None), 4);
    }
}
