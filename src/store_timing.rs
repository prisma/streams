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
use std::sync::{Arc, Mutex, OnceLock};
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
            if ticks % 100 == 0 {
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

fn classify(path: &str) -> u8 {
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
        g.finish(r.is_ok());
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _p = permit().await;
        let g = OpGuard::new(2, location);
        let r = self.inner.get_ranges(location, ranges).await;
        g.finish(r.is_ok());
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
        if let std::task::Poll::Ready(None) = poll {
            if let Some(g) = self.guard.take() {
                g.finish(true);
            }
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
    let mut ops = serde_json::Map::new();
    let mut keys: Vec<_> = cells.keys().copied().collect();
    keys.sort();
    for k in keys {
        let mut v = cells.remove(&k).unwrap();
        v.sort_unstable();
        let name = format!("{}:{}", OPS[k.0 as usize], CLASSES[k.1 as usize]);
        ops.insert(
            name,
            serde_json::json!({
                "n": v.len(),
                "err": errs.get(&k).copied().unwrap_or(0),
                "p50_ms": pct(&v, 0.50),
                "p90_ms": pct(&v, 0.90),
                "p99_ms": pct(&v, 0.99),
                "max_ms": (v.last().copied().unwrap_or(0) / 1000) as u64,
            }),
        );
    }
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
    serde_json::json!({
        "ts_ms": now_ms(),
        "window_secs": window_secs,
        "out_inflight_now": inflight_now,
        "out_inflight_peak": peak,
        "timer_thread": drift_stats(&drift().thread, cutoff),
        "timer_tokio": drift_stats(&drift().tokio, cutoff),
        "steal_pct": steal_pct,
        "ops": ops,
        "slow": slow,
    })
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
