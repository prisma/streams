//! History tier (§3.6): per-stream WAL-less SlateDBs under shared-bucket
//! prefixes, block-transformer encrypted with the stream key, block-zstd
//! compressed — plus the absorber that drains shard logs into them.
//!
//! History keyspace:
//!   'r' '!' <offset u64 BE>                       record (plaintext in blocks)
//!   'k' '!' <rk_len u16 BE> <rk> <offset u64 BE>  routing-key index (copy)
//!
//! History record value: [ver u8=1][ts i64 LE][key_version u32 LE]
//!                       [rk_len u16 LE][rk][payload]

mod canonical_span;
mod postings_read;
use postings_read::{execute_postings_plan, execute_postings_plan_scoped};
pub(crate) mod span_cache;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::{CompressionCodec, Settings, WriteOptions};
use slatedb::{Db, WriteBatch};
use tokio::sync::mpsc;

use crate::crypto::{RouteHash, SegmentHash, StreamKey, hex};
use crate::shard::{AbsorbSignal, ShardEngine, read_frames_range};

#[cfg(test)]
mod controller_tests;

// ---- block transformer: AES-256-GCM with a random nonce per block ----

/// Operator pause for the whole absorber (fleet runbook).
/// Scan options for history reads: without readahead, slatedb fetches one
/// (compressed, ~200B) block per sequential GET — thousands of round-trips
/// per page on a 25ms store. 2MB readahead turns that into a few large GETs.
fn hist_scan_opts() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 2 * 1024 * 1024,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    }
}

/// Postings-index scans (spec §7.5): 1 MiB read-ahead reaches ~64
/// buckets of compact pages in one cold load; blocks stay cacheable
/// (the SST index/filter cache still applies), and the decoded slice
/// cache (next PR) is the long-lived home for the result.
pub(crate) fn postings_scan_opts_pub() -> slatedb::config::ScanOptions {
    postings_scan_opts()
}

fn postings_scan_opts() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 1024 * 1024,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    }
}

// ---- shared history v2 keyspace (docs/HISTORY-V2.md) ----
//
// Route hash FIRST so a shard split can clone the partition by key
// range; then the stream incarnation, a tag byte, and the offset.
// Values are raw stream-key-encrypted frames, byte-identical to the
// shard log's — the reader decodes them with the same tail machinery.

pub fn hist2_record_key(route: RouteHash, inc: SegmentHash, offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(41);
    k.extend_from_slice(&route.0);
    k.extend_from_slice(&inc.0);
    k.push(b'r');
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

// ---- settings (D23 maintenance profile + F2 pattern) ----

/// Shared block cache for ALL history DBs (absorber writes + reads):
/// SlateDB's per-DB default is 512 MB, and the absorber opens a DB per
/// absorbed stream — unbounded aggregate cache on a 1 GB box.
/// Per-partition L0 facts from the db's IN-MEMORY manifest snapshot
/// (`Db::manifest()` — no object-store request). The manifest types
/// only expose Serialize at this pin, so this walks the serde view
/// tolerantly: (l0_sst_count, l0_bytes_est, compacted_runs,
/// manifest_id). The review's leading indicator — "history L0
/// approaching 64 × 4 MiB" — is exactly l0_sst_count here.
pub fn history_l0_stats(db: &slatedb::Db) -> (u64, u64, u64, u64) {
    fn sum_sizes(v: &serde_json::Value) -> u64 {
        match v {
            serde_json::Value::Object(m) => m
                .iter()
                .map(|(k, v)| {
                    if v.is_u64() && (k.contains("size") || k.ends_with("_bytes")) {
                        v.as_u64().unwrap_or(0)
                    } else {
                        sum_sizes(v)
                    }
                })
                .sum(),
            serde_json::Value::Array(a) => a.iter().map(sum_sizes).sum(),
            _ => 0,
        }
    }
    fn find<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
        match v {
            serde_json::Value::Object(m) => {
                m.get(key).or_else(|| m.values().find_map(|x| find(x, key)))
            }
            serde_json::Value::Array(a) => a.iter().find_map(|x| find(x, key)),
            _ => None,
        }
    }
    let vm = db.manifest();
    let Ok(j) = serde_json::to_value(&vm) else {
        return (0, 0, 0, 0);
    };
    let id = find(&j, "id").and_then(|v| v.as_u64()).unwrap_or(0);
    let (l0n, l0b) = find(&j, "l0")
        .and_then(|v| v.as_array())
        .map(|a| (a.len() as u64, a.iter().map(sum_sizes).sum()))
        .unwrap_or((0, 0));
    let runs = find(&j, "compacted")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as u64)
        .unwrap_or(0);
    (l0n, l0b, runs, id)
}

// ---------------------------------------------------------------------
// Process-wide absorber memory budget (OOM review item 1)
// ---------------------------------------------------------------------

/// ONE budget for EVERY shard's gathers. `gather_max_bytes` alone is a
/// per-shard packing bound: at 16 open shards it multiplied to 512 MiB
/// of nominal simultaneous gather exposure — the preview.7 kill
/// amplifier — because each gather also transiently holds more than its
/// accounting value (raw frame vectors, the WriteBatch's cloned
/// values+keys, posting-run builders, and SST construction inside
/// SlateDB). Every gather must RESERVE here before it reads or clones a
/// single frame; the reservation covers those transients via the build
/// multiplier, and an estimate above the whole budget clamps to it, so
/// an oversized gather serializes process-wide instead of deadlocking.
/// Budgets are process-wide BY CONSTRUCTION: the semaphores live in one
/// process-level static, not per absorber.
pub struct AbsorbBudget {
    bytes: tokio::sync::Semaphore,
    gathers: tokio::sync::Semaphore,
    capacity: usize,
    gather_cap: usize,
    reserved: AtomicU64,
    inflight: AtomicU64,
}

/// Multiplier from packed batch bytes to transient build memory: raw
/// frames + WriteBatch value/key clones + posting builders + SST
/// encode. Conservative by design — under-reserving is how instances
/// die between RSS samples.
pub const ABSORB_BUILD_MULTIPLIER: usize = 3;

/// Per-stream byte cap for one gather chunk — bounds what a single
/// stream contributes to a batch (and what one wave slot holds in
/// flight).
pub(crate) const GATHER_PER_STREAM_CAP: usize = 4 * 1024 * 1024;

/// Per-frame encoding overhead allowance on top of the raw body:
/// frame header + maximum routing key + length fields + AEAD tag are
/// all well under this; rounding the reservation UP is the safe
/// direction.
pub const FRAME_ENCODING_ALLOWANCE: usize = 64 * 1024;

/// Worst-case MODELED transient for ONE legal oversized frame: the
/// packer deliberately lets a single frame proceed alone (liveness —
/// bodies can reach the API cap), so the DECLARED envelope must cover
/// its modeled build cost (encoded frame × the build multiplier) or
/// the budget's bound is a fiction. The budget floors its capacity
/// here and every gather reserves at least this, so the reservation
/// covers the modeled transient; concurrency degrades (budget ÷ this
/// per gather — ONE at the floor) instead of the bound lying. Field
/// note: arm X survived at exactly that effective one-gather
/// concurrency. The model is validated (not proven) by the
/// acceptance campaign's oversized-frame leg.
/// CHAOS-3 (2026-08-09): this scales with the EFFECTIVE body ceiling,
/// not the pinned protocol maximum. At the 32 MiB pin it is 96.2 MiB —
/// 19% of the 1 GiB posture's 500 MB shed line, held whenever a gather
/// is in flight, measured in Singapore against gathers whose actual
/// size averaged 6 MB. A deployment that lowers MAX_REQUEST_BODY_BYTES
/// shrinks this proportionally and buys the difference back as
/// admission headroom.
#[cfg(test)]
pub fn absorb_worst_frame_transient() -> usize {
    worst_frame_transient_for(crate::protocol_pin::MAX_BODY_BYTES)
}

/// The sizing rule as a pure function of the body ceiling, so it can be
/// asserted without mutating process-wide state under a parallel test
/// harness.
pub fn worst_frame_transient_for(body_limit: usize) -> usize {
    (body_limit + FRAME_ENCODING_ALLOWANCE) * ABSORB_BUILD_MULTIPLIER
}

/// The gather packing limit AS RESOLVED at startup — after the clamp to
/// `capacity / ABSORB_BUILD_MULTIPLIER`. Published so the concurrency
/// arithmetic below matches what the absorber actually does.
/// Runtime-scoped resources shared by every engine of that runtime.
/// Construction captures validated capacities; no first caller can select
/// configuration for a different runtime.
pub struct HistoryResources {
    pub budget: AbsorbBudget,
    pub cache: Arc<slatedb::db_cache::foyer::FoyerCache>,
    pub(crate) spans: span_cache::SpanCache,
    pub paused: std::sync::atomic::AtomicBool,
    pub packing_bytes: usize,
    pub worst_frame_transient: usize,
    pub resolved_memory_config: std::sync::OnceLock<serde_json::Value>,
}
impl std::fmt::Debug for HistoryResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryResources")
            .field("capacity", &self.budget.capacity())
            .field("packing_bytes", &self.packing_bytes)
            .finish_non_exhaustive()
    }
}
impl HistoryResources {
    pub fn new(cfg: &crate::config::HistoryConfig, packing_bytes: usize) -> Self {
        Self::with_body_limit(cfg, packing_bytes, crate::protocol_pin::MAX_BODY_BYTES)
    }
    pub fn with_body_limit(
        cfg: &crate::config::HistoryConfig,
        packing_bytes: usize,
        body_limit: usize,
    ) -> Self {
        let worst_frame_transient = worst_frame_transient_for(body_limit);
        let capacity = cfg
            .absorb_global_budget_bytes
            .max(worst_frame_transient)
            .min(u32::MAX as usize);
        let spans = span_cache::SpanCache::new(
            cfg.canonical_span_cache && cfg.cache_bytes >= span_cache::CAPACITY,
        );
        Self {
            budget: AbsorbBudget::new(capacity, cfg.absorb_global_gathers),
            cache: Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: cfg.cache_bytes.saturating_sub(spans.capacity()) as u64,
                    ..Default::default()
                },
            )),
            spans,
            paused: std::sync::atomic::AtomicBool::new(cfg.absorb_pause_initial),
            packing_bytes: packing_bytes.min(capacity / ABSORB_BUILD_MULTIPLIER),
            worst_frame_transient,
            resolved_memory_config: std::sync::OnceLock::new(),
        }
    }
    pub fn per_gather_reservation_bytes(&self) -> usize {
        self.packing_bytes
            .saturating_mul(ABSORB_BUILD_MULTIPLIER)
            .max(self.worst_frame_transient)
            .clamp(1, self.budget.capacity())
    }
    pub fn effective_gather_concurrency(&self) -> usize {
        self.budget
            .gather_slots()
            .min((self.budget.capacity() / self.per_gather_reservation_bytes().max(1)).max(1))
    }
}

/// Injected history-flush slowdown, ms per gather flush (0 = off).
/// Set via POST /v1/debug/history-stall?ms= — the STALLED-HISTORY-
/// FLUSH acceptance leg's lever: it stalls the actual gather flush
/// path WITH the reservation held (it does not pause the SlateDB
/// compactor itself).
pub static HISTORY_FLUSH_STALL_MS: AtomicU64 = AtomicU64::new(0);

/// The budget floor as a pure function (tested directly): a configured
/// capacity below one worst-case frame build is raised to it.
#[cfg(test)]
pub fn floored_budget_capacity(configured: usize) -> usize {
    configured.max(absorb_worst_frame_transient())
}

/// The shed expression (OOM review P2), factored for a deterministic
/// test: pressure = sampled RSS + absorber bytes ALREADY RESERVED —
/// the reservation is visible the instant it is granted, so admission
/// backs off BEFORE the allocation shows up in an RSS sample.
pub fn memory_pressure_mb(rss_mb: u64, reserved_bytes: u64) -> u64 {
    rss_mb.saturating_add(reserved_bytes / (1024 * 1024))
}

pub struct AbsorbReservation<'a> {
    bytes: usize,
    budget: &'a AbsorbBudget,
    // RAII permits (review: cancellation safety). If reserve() is
    // cancelled mid-acquire — engine shutdown aborting the absorber, a
    // timed-out test dropping the future, a future select! — the
    // already-held permit drops and returns to its semaphore on its
    // own. forget()/add_permits bookkeeping leaked a gather slot
    // permanently in exactly that window.
    _gather: tokio::sync::SemaphorePermit<'a>,
    _bytes: tokio::sync::SemaphorePermit<'a>,
}

impl AbsorbBudget {
    pub fn new(bytes: usize, gathers: usize) -> Self {
        // Permits are addressed as u32 (acquire_many); cap the byte
        // capacity there so conversions are total, never truncating.
        let capacity = bytes.clamp(1, u32::MAX as usize);
        AbsorbBudget {
            bytes: tokio::sync::Semaphore::new(capacity),
            gathers: tokio::sync::Semaphore::new(gathers.max(1)),
            capacity,
            gather_cap: gathers.max(1),
            reserved: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
        }
    }

    /// The effective (floored, clamped) byte capacity — startup
    /// invariants and the campaign's verify-before-load read this.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn gather_slots(&self) -> usize {
        self.gather_cap
    }

    /// Reserve BEFORE any frame is read. Blocks until the process-wide
    /// bytes AND a concurrent-gather slot are available — that wait IS
    /// the backpressure that keeps N shards from building N batches at
    /// once on a 1 GiB instance. Cancellation-safe: permits are RAII,
    /// so a reservation future dropped at ANY await point returns
    /// whatever it already held.
    pub async fn reserve(&self, estimate: usize) -> AbsorbReservation<'_> {
        let want = estimate.clamp(1, self.capacity);
        // capacity <= u32::MAX by construction, so this is total.
        let want_permits = u32::try_from(want).expect("capacity clamped to u32 range");
        let gather = self
            .gathers
            .acquire()
            .await
            .expect("absorb budget semaphore closed");
        let bytes = self
            .bytes
            .acquire_many(want_permits)
            .await
            .expect("absorb budget semaphore closed");
        self.reserved
            .fetch_add(want as u64, std::sync::atomic::Ordering::Relaxed);
        self.inflight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        AbsorbReservation {
            bytes: want,
            budget: self,
            _gather: gather,
            _bytes: bytes,
        }
    }

    pub fn inflight(&self) -> u64 {
        self.inflight.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn gather_slots_free(&self) -> usize {
        self.gathers.available_permits()
    }
}

impl AbsorbReservation<'_> {
    /// Bytes actually GRANTED (post-clamp) — what the gauges report.
    pub fn granted(&self) -> usize {
        self.bytes
    }

    /// Grow this reservation by `additional` transient bytes, waiting
    /// on the pool like reserve() does — the wait IS the cross-shard
    /// backpressure when a gather turns out fatter than its adaptive
    /// estimate (#266). Clamped so the TOTAL never exceeds the pool's
    /// capacity: a single oversized frame always fits (capacity floors
    /// at the worst-frame transient) and a batch can't exceed it
    /// (resolved_gather_packing_bytes caps gather_max at capacity ÷
    /// multiplier), so a shortfall after clamping only occurs in
    /// shapes the pre-adaptive reservation could not cover either.
    pub async fn grow(&mut self, additional: usize) {
        let add = additional.min(self.budget.capacity.saturating_sub(self.bytes));
        if add == 0 {
            return;
        }
        let add_permits = u32::try_from(add).expect("capacity clamped to u32 range");
        let extra = self
            .budget
            .bytes
            .acquire_many(add_permits)
            .await
            .expect("absorb budget semaphore closed");
        self._bytes.merge(extra);
        self.bytes += add;
        self.budget
            .reserved
            .fetch_add(add as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for AbsorbReservation<'_> {
    fn drop(&mut self) {
        // Permits return themselves; only the observability counters
        // need bookkeeping here.
        self.budget
            .reserved
            .fetch_sub(self.bytes as u64, std::sync::atomic::Ordering::Relaxed);
        self.budget
            .inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// Gather observability (OOM review instrumentation): last-gather phase
// timings + reserved-vs-actual, and cumulative absorbed/ingested bytes
// for rate derivation between ops snapshots.
pub static ABSORB_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static INGEST_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_RESERVED: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_ACTUAL: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_READ_MS: AtomicU64 = AtomicU64::new(0);
/// Time the last gather spent PARKED between frame reads (#266 pacing).
/// Included in GATHER_LAST_READ_MS's window; subtract to attribute the
/// read phase between real store reads and deliberate append windows.
pub static GATHER_LAST_PACE_MS: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_WRITE_MS: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_FLUSH_MS: AtomicU64 = AtomicU64::new(0);
pub static HISTORY_FLUSH_WAIT_MS_MAX: AtomicU64 = AtomicU64::new(0);

/// Settings for the SHARED history v2 partition (docs/HISTORY-V2.md).
/// Differences from v1 per-stream DBs, each deliberate: NO compression
/// (values are stream-key-encrypted frames — already compressed before
/// encryption, and ciphertext does not compress) and NO block
/// transformer (the frames are the ciphertext; object storage never
/// sees plaintext either way).
pub(crate) fn history2_settings(
    cfg: &crate::config::HistoryConfig,
    compactor: &slatedb::config::CompactorOptions,
) -> Settings {
    Settings {
        compression_codec: None,
        ..history_settings(cfg, compactor)
    }
}

pub(crate) fn history_settings(
    cfg: &crate::config::HistoryConfig,
    compactor: &slatedb::config::CompactorOptions,
) -> Settings {
    // Bench-only escape hatch: HISTORY_COMPACTOR=off disables the embedded
    // compactor (and lifts the L0 caps so flushes never block on it). Used
    // with the s3lite --discard-substr mode, where history SST bodies are
    // dropped and must never be re-read. Production keeps the compactor.
    let compactor_off = cfg.compactor_off;
    // History DBs (per-stream v1 AND the shared v2 partitions) are
    // quiet most of the time, and their fixed-cadence LISTs were 79% of
    // v2's residual request cost (docs/HISTORY-V2.md scorecard). The
    // fork answered that with quiet-backoff + listing reuse; upstream
    // declined that design (slatedb#1991 -> #1993), so on upstream the
    // same economics come from a LONG STATIC sweep interval — the old
    // backoff CEILING becomes the cadence. The cost of the trade is
    // reclamation latency on a busy history DB (bounded, storage-cheap),
    // not steady-state requests. HISTORY_GC_INTERVAL_SECS (default 600;
    // HISTORY_GC_MAX_INTERVAL_SECS accepted as a legacy alias).
    let gc_interval = cfg.gc_interval;
    let mut gc = Settings::default()
        .garbage_collector_options
        .unwrap_or_default();
    for slot in [
        &mut gc.wal_options,
        &mut gc.manifest_options,
        &mut gc.compacted_options,
        &mut gc.compactions_options,
    ] {
        *slot = Some(slatedb::config::GarbageCollectorDirectoryOptions {
            interval: gc_interval,
            ..slot.unwrap_or_default()
        });
    }
    Settings {
        wal_enabled: false,
        flush_interval: Some(Duration::from_millis(100)),
        manifest_poll_interval: Duration::from_secs(300),
        garbage_collector_options: Some(gc),
        compression_codec: Some(CompressionCodec::Zstd),
        // Upstream default is 512 MB — on a 1 GB instance the absorber sink
        // buffers toward the kernel kill line long before backpressure
        // fires (same finding as the shard tier, 2026-07-14; reproduced as
        // an OOM loop at 10 MB/s absorb on Compute, 2026-07-23). Bound it.
        max_unflushed_bytes: 32 * 1024 * 1024,
        // 4 MB (was 16): each history SST build runs zstd + AES over the
        // whole SST inside SlateDB's flush task ON OUR RUNTIME — 16 MB
        // builds blocked the event loop in 100s-of-ms bursts (run 12
        // timer evidence). Smaller SSTs = shorter bursts; the embedded
        // compactor consolidates them.
        l0_sst_size_bytes: 4 * 1024 * 1024,
        l0_max_ssts: if compactor_off { 1_000_000 } else { 64 },
        l0_max_ssts_per_key: if compactor_off { 1_000_000 } else { 64 },
        compactor_options: if compactor_off {
            None
        } else {
            // Embedded compactor kept for phase 1 so L0s consolidate while
            // the absorber has the DB open; the detached model arrives with
            // the compactor service.
            //
            // R29 release blocker: `Settings::default().compactor_options`
            // silently ran the UPSTREAM worker profile (concurrency 4,
            // 4 subcompactions, 4x2 MiB read-ahead, 256 MiB rolls) on
            // every history and history-v2 partition — the exact
            // defaults the R27-4 posture removes — while
            // MEMPROFILE_CERT reported the process certified. Every
            // production DB uses the ONE resolved profile.
            Some(compactor.clone())
        },
        ..Default::default()
    }
}

pub fn history_db_path(hash: &[u8; 16]) -> String {
    format!("streams/{}", hex(hash))
}

// ---- key cache (transient; fed by keyed requests) ----

pub struct KeyEntry {
    pub key: StreamKey,
    pub epoch: [u8; 16],
    pub at: Instant,
}

#[derive(Default)]
pub struct KeyCache {
    map: Mutex<HashMap<[u8; 16], KeyEntry>>,
}

const KEY_TTL: Duration = Duration::from_secs(900);
/// Cardinality bound: v2 absorption never consults this cache, so on a
/// wide shard it is pure retention — expired entries used to return
/// None but stay resident forever (static-audit memory finding).
const KEY_CACHE_MAX: usize = 65_536;

impl KeyCache {
    pub fn put(&self, hash: [u8; 16], key: StreamKey, epoch: [u8; 16]) {
        let mut map = self.map.lock().unwrap();
        if map.len() >= KEY_CACHE_MAX && !map.contains_key(&hash) {
            map.retain(|_, e| e.at.elapsed() <= KEY_TTL);
            if map.len() >= KEY_CACHE_MAX
                && let Some(oldest) = map.iter().min_by_key(|(_, e)| e.at).map(|(h, _)| *h)
            {
                map.remove(&oldest);
            }
        }
        map.insert(
            hash,
            KeyEntry {
                key,
                epoch,
                at: Instant::now(),
            },
        );
    }

    pub fn get(&self, hash: &[u8; 16]) -> Option<(StreamKey, [u8; 16])> {
        let mut map = self.map.lock().unwrap();
        let expired = map.get(hash).is_some_and(|e| e.at.elapsed() > KEY_TTL);
        if expired {
            map.remove(hash);
            return None;
        }
        let e = map.get(hash)?;
        Some((e.key.clone(), e.epoch))
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

// ---- absorber ----

#[derive(Clone)]
pub struct AbsorberConfig {
    pub threshold_bytes: u64,
    pub threshold_age: Duration,
    pub tick: Duration,
    pub batch_puts: usize,
    /// Upper bound on plaintext bytes buffered per absorb pass. absorb_one
    /// holds the whole pass in memory; without a cap, a pass that starts
    /// behind a high-throughput stream buffers the entire lag (GBs on a
    /// 1 GB instance). The boundary advances per pass, so a capped pass
    /// just means more passes.
    pub pass_bytes: u64,
    /// Streams whose pending bytes are at or under this run in the
    /// CONCURRENT small lane; bigger streams keep the serial full-budget
    /// lane. The split exists so wide sparse backlogs (thousands of
    /// near-empty streams, each pass dominated by ~10 serial store
    /// round-trips) can overlap latency without letting several
    /// full-size passes multiply peak memory (docs/COST-WIDE1.md §1:
    /// the serial grind measured ~4.5 streams/s, pinning both the bill
    /// and backlog completion).
    pub small_pass_bytes: u64,
    /// Concurrent small-lane passes (1 = fully serial, the old behavior).
    /// Peak extra memory is bounded by concurrency × small_pass_bytes of
    /// plaintext.
    pub concurrency: usize,
    /// Every N ticks, re-discover unabsorbed streams from the engine's
    /// resident handles. Signals are the fast path; the sweep closes
    /// their gaps (bounded-channel drops under wide backlogs, restarts).
    pub sweep_every: u32,
    /// Aggregate byte budget for ONE v2 gather WriteBatch (keys + frame
    /// values, keyed index duplicates counted twice). Without it the
    /// lane's nominal exposure is V2_LANE_PER_TICK x per-stream cap
    /// (~4 GiB) held in memory before SlateDB backpressure can apply —
    /// and oversized first frames (bodies up to the 32 MiB API cap) can
    /// exceed even that. Streams that do not fit stay pending and
    /// gather on later ticks; a single over-budget chunk still proceeds
    /// alone so every stream makes progress. This is therefore a SOFT
    /// budget, not an absolute memory ceiling: that one-chunk exception
    /// admits up to a per-stream-cap chunk — and a single oversized
    /// KEYED frame is stored twice (record + index row), so the true
    /// worst case is ~2× the largest admissible frame plus overhead.
    /// Default matches the history DB's max_unflushed_bytes: one
    /// gather ≈ one memtable.
    pub gather_max_bytes: usize,
    /// Duty-cycle the gather READ phase (#266): after any frame read,
    /// if at least `gather_pace_window` has elapsed since the last
    /// park, park `gather_pace`. The reads land on the SHARD db — the
    /// same SlateDB instance serving append WAL writes — and a gather
    /// issues them back to back, seconds of continuous read pressure
    /// per call. The L1 certification ladder showed append shed
    /// tracking absorb gathers monotonically while the streams-side
    /// commit lane was exonerated (DST wave gate) and neither more
    /// slatedb runtime threads nor deferral removed it: the
    /// serialization is inside SlateDB. TIME-based, not count-based
    /// (L1d7): a drain gather fills its 32 MiB batch from ~8 big
    /// chunks — few reads, 20+ s of read time, a count trigger never
    /// fires — and at run-time chunk sizes a count of 32 paced ~0.8%
    /// of the read phase, homeopathic. Defaults 50 ms window / 10 ms
    /// park = ~17% worst-case read-phase overhead and an append window
    /// at least every ~60 ms plus one read. `gather_pace == 0`
    /// disables; `gather_pace_window == 0` parks after every read.
    pub gather_pace_window: Duration,
    pub gather_pace: Duration,
    /// Concurrent per-stream frame reads within one gather (#266).
    /// The read phase is latency-bound (a store round trip per sparse
    /// stream) and append shed scales with its WALL TIME, so waves of
    /// concurrent reads shrink the exposure window directly. Transient
    /// memory is bounded by read_par x the per-stream cap, within the
    /// caller's existing budget reservation. 1 = serial (the pre-#266
    /// behavior).
    pub gather_read_par: usize,
}

impl Default for AbsorberConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 4 * 1024 * 1024,
            threshold_age: Duration::from_secs(300),
            tick: Duration::from_secs(5),
            batch_puts: 4_096,
            pass_bytes: 256 * 1024 * 1024,
            small_pass_bytes: 1024 * 1024,
            concurrency: 6,
            sweep_every: 12,
            gather_max_bytes: 32 * 1024 * 1024,
            gather_pace_window: Duration::from_millis(50),
            // L1d8 falsified pacing (stretching the read phase amplifies
            // the append-service deficit); disabled by default, knob kept
            // for field experiments.
            gather_pace: Duration::ZERO,
            gather_read_par: 8,
        }
    }
}

const MAX_PENDING_STREAMS: usize = 8192;
const DISCOVERY_PAGE_STREAMS: usize = 256;

struct PendingAbsorb {
    bytes: u64,
    since: Instant,
    /// Consecutive non-fence absorb failures; drives exponential backoff so
    /// a persistent error retries at tick·2^n instead of every tick.
    failures: u32,
    /// Earliest next attempt (backoff); zero-delay until the first failure.
    retry_after: Option<Instant>,
}

/// Selection walks a capacity-bounded roster. Hash-order continuation
/// reserves turns for cold streams even while earlier keys stay hot.
fn due_streams(
    pending: &HashMap<[u8; 16], PendingAbsorb>,
    cfg: &AbsorberConfig,
    now: Instant,
    after: Option<[u8; 16]>,
) -> Vec<([u8; 16], u64)> {
    let mut due: Vec<_> = pending
        .iter()
        .filter(|(_, p)| {
            (p.bytes >= cfg.threshold_bytes || now.duration_since(p.since) >= cfg.threshold_age)
                && p.retry_after.is_none_or(|retry| now >= retry)
        })
        .map(|(hash, p)| (*hash, p.bytes))
        .collect();
    due.sort_unstable_by_key(|entry| entry.0);
    if let Some(after) = after {
        let split = due.partition_point(|entry| entry.0 <= after);
        due.rotate_left(split);
    }
    due
}

/// Per-stream classification of one v2 gather (review round 4, P1): the
/// pump must retire ONLY what the gather settled. `advanced` carries
/// (hash, new upto, raw frame bytes copied — the committer's
/// unabsorbed_bytes decrement); `no_work` had nothing durable to absorb;
/// `deferred_budget` did not fit this batch's byte budget and MUST stay
/// pending — with lag and age intact — for the next tick.
#[derive(Default)]
pub(crate) struct GatherOutcome {
    pub(crate) advanced: Vec<([u8; 16], u64, u64)>,
    pub(crate) no_work: Vec<[u8; 16]>,
    pub(crate) deferred_budget: Vec<[u8; 16]>,
    /// Streams whose gather ADVANCED but did not reach the stream's
    /// durable end — the per-stream byte cap truncated the chunk, so
    /// data remains. `(hash, remaining offsets)`. These MUST stay
    /// pending: retiring them (they are also in `advanced`) strands the
    /// remainder until some unrelated event re-discovers it, and for a
    /// stream whose next record exceeds the cap that is effectively
    /// never — 8x100 KiB behind a 64 KiB cap absorbed exactly one
    /// record and then stopped forever (chaos campaign, 2026-08-09).
    pub(crate) partial: Vec<([u8; 16], u64)>,
}

/// Fence-class absorb errors mean this engine lost the shard to a new owner:
/// retrying can never succeed and — worse — keeps evicting the rightful
/// owner's history db in a ping-pong ("the absorption war", 2026-07-20).
/// The correct move is to DROP the claim; the owner accumulates its own
/// signals from its own appends.
fn absorb_error_is_fence(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<slatedb::Error>()
            .is_some_and(|error| matches!(error.kind(), slatedb::ErrorKind::Closed(_)))
    })
}

pub struct Absorber {
    /// Decaying max of observed per-gather transient (batch bytes x
    /// build multiplier). CHAOS-3 measured gathers averaging 6 MB
    /// against a 96 MiB worst-case reservation — 19% of the 1 GiB
    /// posture's shed line held per in-flight gather, and the L1
    /// certification ladder showed that pressure SHEDDING APPENDS for
    /// the reservations' full hold duration (#266). The adaptive
    /// estimate keeps the pressure line honest at sparse shapes while
    /// grow() + the pool keep the OOM bound exact.
    recent_transient: AtomicU64,
    data_store: Arc<dyn ObjectStore>,
    shard: Arc<ShardEngine>,
    keys: Arc<KeyCache>,
    cfg: AbsorberConfig,
    /// History DB handles kept open across passes. The original F2 design
    /// opened and closed per pass ("maintenance-free"), but each open is
    /// 1-2 s of manifest round-trips — at a 32 MB pass that caps absorb
    /// throughput near ~5-8k rec/s, below a loaded stream's ingest, and
    /// the backlog compounds into the OOM spiral (sinmax run 11 marathon).
    /// Small LRU (4) + idle eviction keeps V4's idle-per-DB-overhead
    /// concern bounded; entries are dropped on fence-class errors and on
    /// absorber exit.
    /// Highest `upto` this absorber has submitted per stream, WITH the
    /// lane that submitted it (true = v2 shared partition). The
    /// published handle state only reflects a submit after the committer
    /// batch it landed in is durable AND dispatched, so pacing passes off
    /// the published value alone re-absorbs the same range whenever
    /// dispatch lags a tick — wasted decrypt/write work, and the duplicate
    /// `Absorbed` op it produces used to collapse the deferred-trim lag
    /// (2026-07-27 boundary-race DST failure). LANE-SCOPED (round 4):
    /// each lane trusts only its OWN mark — during the brief pre-seal
    /// window both lanes can claim a stream, and the committer's layout
    /// seal then DROPS one side's advance; if the surviving lane trusted
    /// the dropped lane's floor it would skip a range that only exists
    /// in the dropped tier, permanently hiding acked records. Per-
    /// instance state: a restarted or new-owner absorber starts from
    /// published state again, which is safe because re-absorbing is
    /// idempotent.
    submitted: std::sync::Mutex<HashMap<[u8; 16], (u64, bool)>>,
    discovery_after: std::sync::Mutex<Option<[u8; 16]>>,
}

/// Must exceed the small lane's concurrency, or a tick's concurrent
/// passes evict each other's handles at the end of every tick and the
/// next tick re-opens them (the open IS the per-stream cost being
/// amortized).

impl Absorber {
    /// Construct without starting the pump — DST tests drive gathers
    /// directly for deterministic budget/packing assertions.
    pub(crate) fn new(
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
    ) -> Self {
        let seed = cfg
            .gather_max_bytes
            .saturating_mul(ABSORB_BUILD_MULTIPLIER)
            .max(shard.history_resources.worst_frame_transient) as u64;
        Absorber {
            data_store,
            shard,
            keys,
            cfg,
            submitted: std::sync::Mutex::new(HashMap::new()),
            discovery_after: Default::default(),
            // Seeded at the worst-case est: boot-time gathers (restart
            // rediscovery drains the whole backlog) reserve like the
            // pre-adaptive code and the estimate decays toward observed
            // reality over ~a dozen gathers.
            recent_transient: AtomicU64::new(seed),
        }
    }

    /// The reservation estimate for the next gather: the decaying max
    /// of observed transients, floored at one per-stream chunk's
    /// modeled cost (so steady sparse shapes don't thrash grow()) and
    /// capped at the worst-case est the pre-adaptive code always used.
    pub(crate) fn adaptive_gather_est(&self) -> usize {
        let cap = self
            .cfg
            .gather_max_bytes
            .saturating_mul(ABSORB_BUILD_MULTIPLIER)
            .max(self.shard.history_resources.worst_frame_transient);
        let floor = worst_frame_transient_for(GATHER_PER_STREAM_CAP).min(cap);
        (self
            .recent_transient
            .load(std::sync::atomic::Ordering::Relaxed) as usize)
            .clamp(floor, cap)
    }

    /// Record a gather's observed transient: decaying max — jumps to a
    /// fat observation immediately, decays an eighth per gather so a
    /// one-off burst stops inflating the estimate within ~a dozen
    /// ticks.
    fn observe_gather_transient(&self, batch_bytes: usize) {
        let observed = batch_bytes.saturating_mul(ABSORB_BUILD_MULTIPLIER) as u64;
        let ord = std::sync::atomic::Ordering::Relaxed;
        let prev = self.recent_transient.load(ord);
        let decayed = prev - prev / 8;
        self.recent_transient.store(decayed.max(observed), ord);
    }

    #[cfg(test)]
    pub(crate) fn observe_gather_transient_for_tests(&self, batch_bytes: usize) {
        self.observe_gather_transient(batch_bytes);
    }

    /// Production and composed fixtures register the task before publishing
    /// the engine, so its termination includes absorption as well as commits.
    pub(crate) fn start_owned(
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        rx: mpsc::Receiver<AbsorbSignal>,
    ) {
        let absorber = Self::new(data_store, shard.clone(), keys, cfg);
        shard.spawn_required("absorber", absorber.run(rx));
    }

    #[cfg(test)]
    pub fn start(
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        rx: mpsc::Receiver<AbsorbSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::new(data_store, shard, keys, cfg).run(rx))
    }

    /// Scan the durable dirty index and merge outstanding work:
    /// unabsorbed streams into `pending`, trim debt into the engine's
    /// maintenance set. Pending bytes come from the tail's EXACT
    /// `unabsorbed_bytes` gauge — the old records × 1 KiB estimate
    /// under-sized a single 32 MiB record by 32,000×, putting it below
    /// both default absorption thresholds forever (review round 4).
    /// `or_insert` merge: live entries always win over the scan's view.
    async fn seed_from_dirty_index(
        &self,
        pending: &mut HashMap<[u8; 16], PendingAbsorb>,
    ) -> anyhow::Result<usize> {
        // R25-A: maintenance state is loaded SYNCHRONOUSLY by the
        // engine opener, before the engine is published. Restoring it
        // here — asynchronously, after the engine is already serving —
        // was the R24 defect: the first request after a restart could be
        // admitted before the backlog was known, and a late restore
        // could overwrite state a new append had already advanced.
        let after = *self.discovery_after.lock().unwrap();
        let (dirty, more) = self
            .shard
            .scan_dirty_streams_page(after, DISCOVERY_PAGE_STREAMS)
            .await?;
        let last = dirty.last().map(|entry| entry.0);
        let mut absorb_seeded = 0usize;
        for (h, absorbed, next) in dirty {
            if pending.len() >= MAX_PENDING_STREAMS && !pending.contains_key(&h) {
                continue;
            }
            let (recs, bytes) = match self.shard.tail_fields(&h).await {
                Ok(Some(t)) => {
                    if t.trimmed < t.trim_safe_to {
                        self.shard.note_trim_debt(h);
                    }
                    let recs = t.next.saturating_sub(t.absorbed);
                    let bytes = if t.unabsorbed_bytes > 0 {
                        t.unabsorbed_bytes
                    } else {
                        // Legacy tail without the gauge: keep the estimate.
                        recs.saturating_mul(1024)
                    };
                    (recs, bytes)
                }
                // Tail unreadable right now: fall back to the marker's
                // view rather than failing the whole seed pass.
                _ => {
                    let recs = next.saturating_sub(absorbed);
                    (recs, recs.saturating_mul(1024))
                }
            };
            if recs == 0 {
                continue;
            }
            // R25-D: heal a stranded submitted-watermark. The gather
            // records what it SUBMITTED (fire-and-forget) so an advance
            // in flight to handle state is not re-sent — but if the
            // committer group carrying that advance FAILED, the durable
            // boundary never moved and the mark now fences the range
            // off from every future gather: `from = max(mark, absorbed)
            // >= upto` reads as no_work forever, and the backlog is
            // stranded until a restart. The durable tail is the source
            // of truth: a mark ahead of it at rescan time describes a
            // submission that did not land, so roll it back. A genuine
            // in-flight advance re-submitted after this is harmless —
            // the committer ignores non-advancing boundaries and the
            // history write is idempotent.
            {
                let mut submitted = self.submitted.lock().unwrap();
                if let Some((mark, _v2)) = submitted.get(&h)
                    && *mark > absorbed
                {
                    tracing::warn!(
                        "rolling back stranded absorb mark for {}: submitted={} durable absorbed={}",
                        crate::crypto::hex(&h[..4]),
                        mark,
                        absorbed,
                    );
                    submitted.remove(&h);
                }
            }
            // Backdate by the age threshold so recovered work is eligible
            // promptly rather than a full window later.
            let since = Instant::now()
                .checked_sub(self.cfg.threshold_age)
                .unwrap_or_else(Instant::now);
            pending.entry(h).or_insert(PendingAbsorb {
                bytes,
                since,
                failures: 0,
                retry_after: None,
            });
            absorb_seeded += 1;
        }
        *self.discovery_after.lock().unwrap() = if more { last } else { None };
        Ok(absorb_seeded)
    }

    /// Shared-partition gather pass (history v2): read MANY streams' raw
    /// encrypted frames from the shard log, put them all into ONE
    /// WriteBatch on the shard's shared partition, flush ONCE, then
    /// advance every covered boundary. No decryption, no KeyCache, no
    /// per-stream DB — the per-stream request tax this replaces was ~43
    /// Class A per one-record stream (docs/COST-WIDE1.md §1).
    ///
    /// Classifies every requested stream: `advanced` covered by this
    /// flush (with new upto and the frame bytes copied), `no_work` had
    /// nothing durable to absorb, and `deferred_budget` did not fit the
    /// aggregate byte budget — the CALLER must keep those pending (with
    /// lag and age intact) so they gather on the next tick; dropping
    /// them used to strand their backlog until the ~60 s resident-handle
    /// sweep re-found it. A per-stream byte cap truncates fat streams
    /// mid-range — their boundary still advances over what was written,
    /// and the sweep or the next signal re-drives the remainder.
    /// Test-facing wrapper: reserve adaptively, then gather. The pump
    /// loop calls absorb_gather_v2_with directly because its
    /// reservation must precede the post-budget fence re-check.
    #[cfg(test)]
    pub(crate) async fn absorb_gather_v2(
        &self,
        streams: &[[u8; 16]],
    ) -> anyhow::Result<GatherOutcome> {
        let mut reservation = self
            .shard
            .history_resources
            .budget
            .reserve(self.adaptive_gather_est())
            .await;
        self.absorb_gather_v2_with(streams, &mut reservation).await
    }

    pub(crate) async fn absorb_gather_v2_with(
        &self,
        streams: &[[u8; 16]],
        reservation: &mut AbsorbReservation<'_>,
    ) -> anyhow::Result<GatherOutcome> {
        // Rough WriteBatch bookkeeping cost per entry, on top of key+value.
        const ENTRY_OVERHEAD: usize = 64;
        let part = self.shard.history_partition().await?;
        let t_read = Instant::now();
        let mut wb = WriteBatch::new();
        let mut out = GatherOutcome::default();
        let mut batch_bytes: usize = 0;
        let mut paced = Duration::ZERO;
        let mut last_park = Instant::now();
        // (segment, chunk_from, chunk_to, per-key runs) for write-through
        // cache warming — installed only after the batch flush succeeds.
        type WarmChunk = (
            SegmentHash,
            u64,
            u64,
            Vec<([u8; 16], Vec<crate::postings::AbsRun>)>,
        );
        let mut warm_installs: Vec<WarmChunk> = Vec::new();
        // #266 phase A: plan the reads serially — resident-map lookups
        // and lock reads only. The per-stream frame reads are the
        // latency-bound part of the gather (store round trips), and the
        // L1 ladder showed append shed scales with read-phase WALL TIME
        // (L1d8: stretching the phase via pacing amplified shed 10x),
        // so the reads run in bounded-concurrency waves below while the
        // WriteBatch build stays serial and deterministic in lane order.
        struct ReadPlan {
            hash: [u8; 16],
            handle: Arc<crate::shard::StreamHandle>,
            from: u64,
            upto: u64,
            route: RouteHash,
        }
        let mut plans: Vec<ReadPlan> = Vec::new();
        for hash in streams {
            let handle = self.shard.stream_handle(*hash).await?;
            let (from, upto, route) = {
                let st = handle.state.lock().unwrap();
                (
                    st.durable.absorbed,
                    st.durable.next,
                    RouteHash(st.durable.route),
                )
            };
            // Lane-scoped floor: trust only OUR lane's mark — a v1 mark
            // here may describe an advance the layout seal dropped, and
            // skipping past it would hide that range from the partition.
            let from = {
                let submitted = self.submitted.lock().unwrap();
                submitted
                    .get(hash)
                    .and_then(|(u, v2)| (*v2).then_some(*u))
                    .unwrap_or(0)
                    .max(from)
            };
            if from >= upto {
                out.no_work.push(*hash);
                continue;
            }
            plans.push(ReadPlan {
                hash: *hash,
                handle,
                from,
                upto,
                route,
            });
        }
        const PER_STREAM_CAP: usize = GATHER_PER_STREAM_CAP;
        let read_par = self.cfg.gather_read_par.max(1);
        let per_stream = PER_STREAM_CAP.min(self.cfg.gather_max_bytes);
        let mut pi = 0usize;
        while pi < plans.len() {
            // Aggregate budget: the batch is held in memory until the one
            // flush below, so its size — not the lane's stream count — is
            // what a 1 GiB instance actually feels. Anything deferred here
            // stays in the pending set and gathers on a later tick; the
            // whole-remainder deferral also skips their reads.
            if batch_bytes >= self.cfg.gather_max_bytes {
                for p in &plans[pi..] {
                    out.deferred_budget.push(p.hash);
                }
                break;
            }
            let wave_end = (pi + read_par).min(plans.len());
            let wave = &plans[pi..wave_end];
            pi = wave_end;
            // Transient memory: at most read_par chunks in flight, each
            // capped at per_stream — bounded by the same reservation the
            // caller already holds (gather_max_bytes x build multiplier).
            let shard = &self.shard;
            let mut futs = Vec::with_capacity(wave.len());
            for (k, p) in wave.iter().enumerate() {
                futs.push(async move {
                    (
                        k,
                        read_frames_range(shard, &p.handle, p.from, p.upto, per_stream).await,
                    )
                });
            }
            // The wave is already sized to read_par, so join_all IS the
            // concurrency bound — no stream adapter needed.
            let mut got = futures_util::future::join_all(futs).await;
            got.sort_unstable_by_key(|(k, _)| *k);
            // #266: optional duty-cycle between waves — see the
            // gather_pace_window field doc. L1d8 falsified pacing as a
            // shed fix (default now 0); the knob remains for field
            // experiments. The commit below is untouched — never
            // stretch the durability-critical section.
            if !self.cfg.gather_pace.is_zero() && last_park.elapsed() >= self.cfg.gather_pace_window
            {
                tokio::time::sleep(self.cfg.gather_pace).await;
                paced += self.cfg.gather_pace;
                last_park = Instant::now();
            }
            for (k, res) in got {
                let p = &wave[k];
                let hash = &p.hash;
                let (from, upto, route) = (p.from, p.upto, p.route);
                let inc = SegmentHash(p.hash);
                let chunk = res?;
                if chunk.frames.is_empty() {
                    out.no_work.push(*hash);
                    continue;
                }
                // This chunk's batch contribution (keyed frames store the
                // value twice: record row + routing-key index row, whose key
                // is 2 bytes longer than the record row's for the length
                // prefix), plus the raw frame bytes for the tail's
                // unabsorbed_bytes gauge.
                let mut chunk_bytes = 0usize;
                let mut chunk_raw = 0u64;
                for raw in &chunk.frames {
                    chunk_raw += raw.len() as u64;
                    // Canonical row + a conservative per-record postings
                    // allowance (~key 65 B amortized + a few varints). The
                    // full-frame keyed duplicate is GONE (ROUTING-V3 §3).
                    chunk_bytes += raw.len() + 41 + ENTRY_OVERHEAD + 24;
                }
                // A chunk that would blow the budget waits for a batch of its
                // own — unless the batch is empty, in which case it proceeds
                // alone (one oversized frame must still make progress; frame
                // bodies can reach the 32 MiB API cap).
                if batch_bytes > 0 && batch_bytes + chunk_bytes > self.cfg.gather_max_bytes {
                    out.deferred_budget.push(*hash);
                    continue;
                }
                // #266 adaptive reservation: cover this chunk's modeled
                // transient BEFORE building it. On the steady path the
                // adaptive estimate already covers the batch and this
                // is a no-op; when a gather turns out fatter than
                // recent history, grow() waits on the pool — the same
                // cross-shard backpressure reserve() gives, applied to
                // exactly the bytes that turned real.
                let needed = (batch_bytes + chunk_bytes).saturating_mul(ABSORB_BUILD_MULTIPLIER);
                if needed > reservation.granted() {
                    reservation.grow(needed - reservation.granted()).await;
                }
                batch_bytes += chunk_bytes;
                #[cfg(test)]
                if std::env::var("DST_DRAIN_TRACE").is_ok() {
                    let offs: Vec<u64> = chunk
                        .frames
                        .iter()
                        .map(|raw| raw.view().header.offset)
                        .collect();
                    eprintln!(
                        "GATHER {} from={from} upto={upto} frames={offs:?}",
                        crate::crypto::hex(&hash[..4]),
                    );
                }
                let mut last = from;
                // Postings replace the covering index (ROUTING-V3 §3): the
                // frame is stored once under its canonical offset; every
                // routing key — INCLUDING the empty/default key — gets
                // compact offset-run pages in the SAME WriteBatch, so the
                // index adds no request, manifest, database, namespace or
                // GC surface of its own.
                let mut pages = crate::postings::PageBuilder::default();
                for raw in &chunk.frames {
                    let frame = raw.view();
                    let off = frame.header.offset;
                    wb.put(hist2_record_key(route, inc, off), Bytes::from(raw.clone()));
                    pages.note_frame(
                        crate::postings::rk_hash(frame.header.routing_key),
                        off,
                        raw.len() as u64,
                    );
                    last = off;
                }
                let (emitted, postings_bytes) = pages.finish();
                POSTINGS_PAGES_WRITTEN
                    .fetch_add(emitted.len() as u64, std::sync::atomic::Ordering::Relaxed);
                // Decode what we just encoded (cheap varints, and a free
                // round-trip check) to hand the slice cache exactly the runs
                // a reader would load — write-through warming (spec §7)
                // makes first-read-after-absorb skip the index round trip.
                let mut chunk_runs: std::collections::HashMap<
                    [u8; 16],
                    Vec<crate::postings::AbsRun>,
                > = std::collections::HashMap::new();
                for (kh, bucket, first, value) in emitted {
                    match crate::postings::decode_page_abs(first, &value) {
                        Some(abs) => {
                            POSTINGS_RUNS_WRITTEN
                                .fetch_add(abs.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            crate::postings::append_page_runs(
                                chunk_runs.entry(kh.0).or_default(),
                                abs,
                            )
                            .ok_or_else(|| anyhow::anyhow!("overlapping postings during gather"))?;
                        }
                        None => anyhow::bail!("postings page failed self-decode during gather"),
                    }
                    wb.put(
                        crate::postings::postings_key(route, inc, &kh, bucket, first),
                        value,
                    );
                }
                POSTINGS_BYTES_WRITTEN
                    .fetch_add(postings_bytes, std::sync::atomic::Ordering::Relaxed);
                CANONICAL_BYTES_WRITTEN.fetch_add(chunk_raw, std::sync::atomic::Ordering::Relaxed);
                warm_installs.push((inc, from, last + 1, chunk_runs.into_iter().collect()));
                out.advanced.push((*hash, last + 1, chunk_raw));
                // Truncated by the per-stream cap: more durable data sits
                // below `upto`. The caller must keep this stream pending.
                if last + 1 < upto {
                    out.partial.push((*hash, upto - (last + 1)));
                }
            }
        }
        GATHER_LAST_PACE_MS.store(
            paced.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.observe_gather_transient(batch_bytes);
        if out.advanced.is_empty() {
            return Ok(out);
        }
        let ord = std::sync::atomic::Ordering::Relaxed;
        GATHER_LAST_READ_MS.store(t_read.elapsed().as_millis() as u64, ord);
        GATHER_LAST_ACTUAL.store(batch_bytes as u64, ord);
        // R25-F: the per-gather read-amplification attribution was
        // REMOVED. It snapshotted process-global GET deltas around the
        // read phase, so concurrent customer/registry/billing/fleet
        // traffic contaminated every sample — a number that looks like a
        // measurement and is not is worse than no number. Operation-
        // local attribution needs the metrics handle carried through
        // SlateDB's scan and spawned fetch tasks (deferred fork patch);
        // until then the process-wide transferred-byte counters in
        // store_timing are the only honest read telemetry.
        let t_write = Instant::now();
        part.write_with_options(wb, &WriteOptions::default())
            .await?;
        GATHER_LAST_WRITE_MS.store(t_write.elapsed().as_millis() as u64, ord);
        let t_flush = Instant::now();
        let stall = HISTORY_FLUSH_STALL_MS.load(std::sync::atomic::Ordering::Relaxed);
        if stall > 0 {
            // Stalled-history-flush campaign lever: the stall sits ON
            // the real flush path with the reservation held, and it is
            // INSIDE the flush timing window — the flush-wait metrics
            // must report the delay the campaign injects, or the gate
            // could stall the path while its primary metric shows
            // nothing.
            tokio::time::sleep(Duration::from_millis(stall)).await;
        }
        part.flush().await?; // wal off => memtable -> L0, manifest published
        // Flush wait is the review's leading indicator: when history L0
        // approaches its cap, THIS is what starts blocking.
        let flush_ms = t_flush.elapsed().as_millis() as u64;
        GATHER_LAST_FLUSH_MS.store(flush_ms, ord);
        HISTORY_FLUSH_WAIT_MS_MAX.fetch_max(flush_ms, ord);
        let absorbed_bytes = out.advanced.iter().map(|(_, _, b)| *b).sum::<u64>();
        ABSORB_BYTES_TOTAL.fetch_add(absorbed_bytes, ord);
        // R25-B: NO maintenance retirement here. This task has proved
        // the HISTORY COPY is durable — the backlog is not retired until
        // the shard's absorbed boundary commits, which happens in the
        // committer's common finalization when the AbsorbedBatch group
        // lands (and stages the maintenance row in the same WriteBatch).
        // Retiring here would claim progress a crash between this flush
        // and that commit would revoke.
        // The pages are durable: warm the slice cache with the runs we
        // just wrote. Readers clip to their own durable boundary, so an
        // install racing the boundary advance can never over-serve.
        for (inc, chunk_from, chunk_to, per_key) in warm_installs {
            self.shard
                .postings_cache
                .install_chunk(inc, chunk_from, chunk_to, per_key);
        }
        self.shard
            .submit_absorbed_batch_v2(out.advanced.clone())
            .await;
        {
            let mut submitted = self.submitted.lock().unwrap();
            for (hash, upto, _) in &out.advanced {
                let e = submitted.entry(*hash).or_insert((0, true));
                if e.1 {
                    e.0 = e.0.max(*upto);
                } else {
                    *e = (*upto, true);
                }
            }
        }
        tracing::info!(
            "v2 gather absorbed {} streams into {}/history2 ({} budget-deferred)",
            out.advanced.len(),
            self.shard.prefix,
            out.deferred_budget.len()
        );
        Ok(out)
    }
}

use std::sync::atomic::AtomicU64;

/// Zero-route tails with unabsorbed data (a bug, not a layout — the v1
/// per-stream format was deleted in the pre-launch clean switch).
pub static ABSORB_ZERO_ROUTE_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static POSTINGS_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static POSTINGS_PAGES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static POSTINGS_RUNS_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static CANONICAL_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static READ_SPANS_MAX: AtomicU64 = AtomicU64::new(0);
pub static READ_FRAMES_SCANNED: AtomicU64 = AtomicU64::new(0);
pub static READ_FRAMES_MATCHED: AtomicU64 = AtomicU64::new(0);
pub static POSTINGS_CORRUPT: AtomicU64 = AtomicU64::new(0);

pub async fn read_history2(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    from: u64,
    upto: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    match key_filter {
        Some(rk) => read_history2_keyed(part, route, inc, rk, from, upto, max_bytes).await,
        None => read_history2_scan(part, route, inc, from, upto, max_bytes).await,
    }
}

/// Unfiltered canonical scan (whole-segment replay): unchanged from the
/// covering-index era — the canonical rows ARE the stream.
async fn read_history2_scan(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    let mut frames: Vec<crate::shard::record::CheckedFrame> = Vec::new();
    let mut last: Option<u64> = None;
    let mut completed = true;
    let mut total = 0usize;
    let prefix = hist2_record_key(route, inc, 0);
    let range = hist2_record_key(route, inc, from)..hist2_record_key(route, inc, upto);
    let mut iter = part.scan_with_options(range, &hist_scan_opts()).await?;
    while let Some(kv) = iter.next().await? {
        let frame = crate::shard::record::CheckedFrame::from_row(&kv.key, &prefix[..33], kv.value)?;
        let off = frame.view().header.offset;
        total += frame.len();
        frames.push(frame);
        last = Some(off);
        if total >= max_bytes {
            completed = false;
            break;
        }
    }
    Ok((frames, last, completed))
}

/// Keyed read through the postings planner (ROUTING-V3 §3/§5): decode
/// the key's offset runs for the requested range, plan bounded
/// canonical spans (<= 8 per response, gap-coalesced by BYTES, 16 MiB
/// scan cap), execute each span as ONE canonical range scan, and
/// verify every frame against the exact routing-key bytes — a 128-bit
/// rk-hash collision can add candidates, never another key's data.
///
/// `last` advances to `consumed_to - 1` even when a planned range holds
/// no matches, so cursors move over provably match-free ranges. The
/// per-offset GET pattern is structurally impossible here: reads are
/// range scans only.
///
/// Ranges with ZERO postings pages fall back to the pre-postings
/// covering index (`k!`-era `hist2_index_key` rows / filtered canonical
/// scan for the empty key) — the migration arm for partitions absorbed
/// before postings existed. Partitions that STRADDLE the cutover in one
/// requested range are a dev-rig-only shape and are not served exactly
/// (docs/ROUTING-V3.md §3); production deployments are greenfield.
async fn read_history2_keyed(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    if from >= upto {
        return Ok((Vec::new(), None, true));
    }
    let kh = crate::postings::rk_hash(rk);
    // 1. Collect this key's pages for every bucket the range touches.
    // Greenfield layout (spec §12.4, postings_from = 0): the postings
    // index is authoritative for the WHOLE absorbed range — zero pages
    // means the range provably holds no matches and the cursor advances
    // over it. A page that fails to decode (or disagrees with its key)
    // is corruption: never claim completeness over an unverified range;
    // fall back to ONE bounded canonical envelope scan of the requested
    // range, filtered by exact key bytes (spec §8.6), and count it.
    let (lo, hi) = crate::postings::postings_range(route, inc, &kh, from, upto);
    let mut runs: Vec<crate::postings::AbsRun> = Vec::new();
    let mut corrupt = false;
    {
        let mut iter = part
            .scan_with_options(lo..hi, &postings_scan_opts())
            .await?;
        while let Some(kv) = iter.next().await? {
            if crate::postings::decode_stored_page(route, inc, &kh, &kv.key, &kv.value)
                .and_then(|page| crate::postings::append_page_runs(&mut runs, page))
                .is_none()
            {
                corrupt = true;
                break;
            }
        }
    }
    let admitted = (!corrupt)
        .then(|| crate::postings::ValidatedRuns::new(runs))
        .flatten();
    let Some(runs) = admitted else {
        POSTINGS_CORRUPT.fetch_add(1, Relaxed);
        return read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await;
    };
    let window = crate::postings::RunWindow::new(runs, from, upto);
    execute_postings_plan(part, route, inc, rk, window, upto, upto, max_bytes).await
}

/// Keyed read through the DECODED SLICE CACHE (spec §7): the engine's
/// cache resolves the runs (hit, single-flight cold load, or forward
/// extension), then the shared planner/executor below serves them.
/// `provable_to < upto` (a load window that could not reach the whole
/// range) yields an honest partial at the proven boundary.
#[allow(clippy::too_many_arguments)]
// Public unscoped library compatibility entry; the server's descriptor-bound
// path calls read_history2_keyed_scoped. Canonical regression tests use both.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn read_history2_keyed_cached(
    cache: &Arc<crate::postings_cache::PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    absorbed: u64,
    max_bytes: usize,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    read_history2_keyed_scoped(
        cache, part, route, inc, rk, from, upto, absorbed, max_bytes, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_history2_keyed_scoped(
    cache: &Arc<crate::postings_cache::PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    absorbed: u64,
    max_bytes: usize,
    scope: Option<Arc<span_cache::Scope>>,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    if from >= upto {
        return Ok((Vec::new(), None, true));
    }
    let kh = crate::postings::rk_hash(rk);
    match cache
        .runs_for(part, route, inc, kh, from, upto, absorbed)
        .await?
    {
        crate::postings_cache::CacheRuns::Corrupt => {
            POSTINGS_CORRUPT.fetch_add(1, Relaxed);
            read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await
        }
        crate::postings_cache::CacheRuns::Runs { runs, provable_to } => {
            execute_postings_plan_scoped(
                part,
                route,
                inc,
                rk,
                runs,
                provable_to,
                upto,
                max_bytes,
                scope,
                absorbed,
            )
            .await
        }
    }
}

/// Corruption envelope (spec §8.6): one bounded canonical scan of the
/// requested range, filtered by EXACT routing-key bytes. Never lies
/// about completeness — a byte-truncated envelope returns an honest
/// partial with a resume cursor.
async fn read_history2_keyed_envelope(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)> {
    let mut frames: Vec<crate::shard::record::CheckedFrame> = Vec::new();
    let mut last: Option<u64> = None;
    let mut completed = true;
    let mut total = 0usize;
    let prefix = hist2_record_key(route, inc, 0);
    let range = hist2_record_key(route, inc, from)..hist2_record_key(route, inc, upto);
    let mut iter = part.scan_with_options(range, &hist_scan_opts()).await?;
    while let Some(kv) = iter.next().await? {
        let f = crate::shard::record::CheckedFrame::from_row(&kv.key, &prefix[..33], kv.value)?;
        let off = f.view().header.offset;
        total += f.len();
        if f.view().header.routing_key == rk {
            frames.push(f);
        }
        last = Some(off);
        if total >= max_bytes {
            completed = false;
            break;
        }
    }
    if completed {
        // The whole range was verified frame-by-frame.
        last = Some(last.map_or(upto - 1, |l| l.max(upto - 1)));
    }
    Ok((frames, last, completed))
}

pub fn absorber_channel() -> (mpsc::Sender<AbsorbSignal>, mpsc::Receiver<AbsorbSignal>) {
    mpsc::channel(65_536)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutOptions, PutPayload, PutResult, path::Path as OPath};

    #[derive(Debug)]
    struct SlowPuts(Arc<dyn ObjectStore>);
    impl std::fmt::Display for SlowPuts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowPuts")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SlowPuts {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            // Stall only WAL flushes: setup (manifest writes) stays fast,
            // and the flusher wedges exactly like a slow-store day.
            if location.as_ref().contains("wal") {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<OPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<OPath>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    use crate::shard::{ShardConfig, ShardEngine};
    use slatedb::Db;

    #[test]
    fn r08_fence_disposition_uses_error_kind_through_context() {
        for reason in [slatedb::CloseReason::Fenced, slatedb::CloseReason::Clean] {
            for message in ["arbitrary new wording", ""] {
                let error = anyhow::Error::new(slatedb::Error::closed(message.into(), reason))
                    .context("unrelated gather context");
                assert!(absorb_error_is_fence(&error));
            }
        }
        for message in ["Fenced", "Closed error", "detected newer DB client"] {
            let unavailable = anyhow::Error::new(slatedb::Error::unavailable(message.into()));
            assert!(!absorb_error_is_fence(&unavailable));
            assert!(!absorb_error_is_fence(&anyhow::anyhow!(message)));
        }
    }

    /// The absorption-war regression test: an absorber whose shard engine
    /// is fenced by a second opener must EXIT (it holds the engine Arc, so
    /// nothing else can end it), not retry forever against the dead db.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absorber_exits_when_shard_engine_is_fenced() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db1 = Db::builder("t/shard", store.clone()).build().await.unwrap();
        let (absorb_tx, absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "t".into(),
            Arc::new(db1),
            store.clone(),
            ShardConfig::default(),
            absorb_tx,
            None,
            crate::shard::ShardMaintenance::default(),
        );
        let handle = Absorber::start(
            store.clone(),
            engine.clone(),
            Arc::new(KeyCache::default()),
            AbsorberConfig {
                tick: Duration::from_millis(50),
                ..Default::default()
            },
            absorb_rx,
        );
        // Give it pending work so exit isn't the empty-queue accident.
        engine.stream_handle([7u8; 16]).await.ok();

        // Second opener on the same path fences the first (SlateDB CAS).
        let _db2 = Db::builder("t/shard", store.clone()).build().await.unwrap();

        // The fenced engine flips closed, and the absorber task exits.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !engine.is_closed() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(engine.is_closed(), "engine never observed the fence");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("absorber did not exit after its engine was fenced")
            .unwrap();
    }

    /// Budget PRIMITIVE test (review: named for what it proves, no
    /// more): a held reservation blocks further reservations past the
    /// declared capacity, the shed expression trips on RSS+reserved,
    /// and releasing hands the budget to the waiter. The real-history
    /// stalled-flush scenario (flush held, ingest continuing, shed,
    /// heal, catch-up, survival) is the acceptance campaign's
    /// slow-compactor leg, driven via /v1/debug/history-stall — this
    /// test does NOT claim it.
    /// #266: grow() must account exactly like reserve() — permits
    /// held while granted, clamped at pool capacity, everything
    /// returned on drop — and a grow that cannot fit must WAIT on the
    /// pool (that wait is the cross-shard backpressure when a gather
    /// outruns its adaptive estimate).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reservation_grow_accounts_clamps_and_waits() {
        let budget: &'static AbsorbBudget = Box::leak(Box::new(AbsorbBudget::new(100, 2)));
        let mut r = budget.reserve(30).await;
        assert_eq!(r.granted(), 30);
        r.grow(20).await;
        assert_eq!(r.granted(), 50);
        assert_eq!(budget.reserved_bytes(), 50);
        // Clamp: asking past capacity grants only up to it.
        r.grow(1000).await;
        assert_eq!(r.granted(), 100);
        // A second reservation must WAIT while the first holds all
        // capacity, and complete once it drops.
        let second = budget.reserve(40);
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "second reservation must block while grow holds capacity"
        );
        drop(r);
        let s2 = tokio::time::timeout(Duration::from_millis(200), &mut second)
            .await
            .expect("released capacity must admit the waiter");
        assert_eq!(s2.granted(), 40);
        drop(s2);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    /// #266: a grow() blocked on the pool must also be cancellation
    /// safe — dropping the blocked future releases nothing it didn't
    /// hold and the original grant stays intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reservation_grow_cancel_leaves_grant_intact() {
        let budget: &'static AbsorbBudget = Box::leak(Box::new(AbsorbBudget::new(100, 2)));
        let mut r = budget.reserve(40).await;
        let holder = budget.reserve(60).await; // pool now full
        {
            let g = r.grow(30);
            tokio::pin!(g);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut g)
                    .await
                    .is_err(),
                "grow must block while the pool is full"
            );
            // dropping the pinned future cancels the acquire
        }
        assert_eq!(r.granted(), 40, "cancelled grow must not change the grant");
        drop(holder);
        r.grow(30).await;
        assert_eq!(r.granted(), 70);
        drop(r);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absorb_budget_blocks_waiters_and_recovers_after_release() {
        let budget: &'static AbsorbBudget = Box::leak(Box::new(AbsorbBudget::new(100, 2)));
        // Two reservations that cannot coexist (scaled units): the
        // first holds the budget, the second must WAIT.
        let stalled = budget.reserve(60).await;
        assert_eq!(budget.reserved_bytes(), 60);
        let second = budget.reserve(60); // 60+60 > 100: must wait
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a second gather must NOT proceed while the stall holds the budget"
        );
        // The DECLARED reservation can never exceed capacity — "32 MiB
        // x open shards" of declared reservation is impossible by
        // construction. (Coverage of the real transient is the floor's
        // job, tested separately below.)
        assert!(budget.reserved_bytes() <= 100);
        // While the budget is held, admission pressure includes the
        // reservation (review P2): 500 MB RSS + 128 MiB reserved trips
        // a 600 MB line even though sampled RSS alone would not.
        assert_eq!(memory_pressure_mb(500, 128 * 1024 * 1024), 628);
        assert!(memory_pressure_mb(500, 128 * 1024 * 1024) > 600);
        assert!(memory_pressure_mb(500, 0) <= 600);
        // Release: the holder drops and the waiter proceeds.
        drop(stalled);
        let recovered = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("waiter must be granted after healing");
        assert_eq!(budget.reserved_bytes(), 60);
        drop(recovered);
        assert_eq!(budget.reserved_bytes(), 0, "full release after drop");
    }

    /// The concurrent-gather cap is enforced independently of bytes:
    /// two small gathers pass, the third waits for a slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gather_concurrency_cap_holds() {
        let budget: &'static AbsorbBudget = Box::leak(Box::new(AbsorbBudget::new(1000, 2)));
        let a = budget.reserve(10).await;
        let b = budget.reserve(10).await;
        let third = budget.reserve(10);
        tokio::pin!(third);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut third)
                .await
                .is_err(),
            "third concurrent gather must wait for a slot"
        );
        drop(a);
        let c = tokio::time::timeout(Duration::from_secs(5), third)
            .await
            .expect("slot handoff");
        drop(b);
        drop(c);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    /// Review (disposition item 1): cancelling a reservation mid-
    /// acquire must not leak a gather slot. The waiter has taken its
    /// gather permit and is parked on bytes when it is aborted; RAII
    /// permits return BOTH resources, and after the holder releases,
    /// fresh reservations fill every slot again. Deterministic via
    /// available-permit observation, no sleeps as conditions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_reservation_releases_its_gather_slot() {
        let budget: &'static AbsorbBudget = Box::leak(Box::new(AbsorbBudget::new(100, 2)));
        let holder = budget.reserve(100).await; // slot 1 + ALL bytes
        assert_eq!(budget.gather_slots_free(), 1);
        // The victim takes slot 2, then parks waiting for bytes.
        let victim = tokio::spawn(async { budget.reserve(50).await });
        while budget.gather_slots_free() != 0 {
            tokio::task::yield_now().await;
        }
        victim.abort(); // cancelled while awaiting byte permits
        let _ = victim.await; // join the aborted task
        // The aborted waiter's gather permit must have returned.
        while budget.gather_slots_free() != 1 {
            tokio::task::yield_now().await;
        }
        drop(holder);
        // FULL capacity restored: two fresh reservations coexist.
        let a = budget.reserve(40).await;
        let b = budget.reserve(40).await;
        assert_eq!(budget.gather_slots_free(), 0);
        drop(a);
        drop(b);
        assert_eq!(budget.gather_slots_free(), 2);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    /// Review (disposition item 2): the budget floor covers one
    /// worst-case frame build, every gather's estimate is at least
    /// that, and oversized-class reservations SERIALIZE without
    /// starvation — the declared envelope covers the real transient.
    /// R23-3: the REPORTED concurrency must equal what the budget will
    /// actually admit. The old formula divided capacity by the worst
    /// frame and ignored the packing term, so it could report 2 where
    /// the semaphore allows 1 — a telemetry artifact that reached the
    /// first chaos report as if it were a measured result.
    #[test]
    fn reported_concurrency_matches_what_the_budget_admits() {
        let resources = HistoryResources::new(&crate::config::HistoryConfig::default(), usize::MAX);
        let cap = resources.budget.capacity();
        let per = resources.per_gather_reservation_bytes();
        let reported = resources.effective_gather_concurrency();

        assert!(per <= cap, "a reservation may never exceed capacity");
        assert!(reported >= 1, "at least one gather must always run");
        assert!(
            reported <= resources.budget.gather_slots(),
            "cannot exceed configured slots"
        );
        // The defining identity: this many reservations fit at once.
        assert!(
            per.saturating_mul(reported) <= cap,
            "{reported} x {per} exceeds capacity {cap} — the budget would block"
        );

        // The sizing rule itself, independent of live process state:
        // under the 1 GiB profile the 8 MiB packing cap dominates a
        // 1 MiB worst frame, which is why lowering the body ceiling
        // alone cannot raise concurrency.
        let packing_term = (8 * 1024 * 1024usize) * ABSORB_BUILD_MULTIPLIER;
        assert_eq!(packing_term, 24 * 1024 * 1024);
        assert!(
            packing_term > worst_frame_transient_for(1024 * 1024),
            "packing term must dominate here; if not, the rule changed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worst_frame_floor_serializes_oversized_gathers_without_starvation() {
        assert_eq!(
            absorb_worst_frame_transient(),
            (crate::protocol_pin::MAX_BODY_BYTES + FRAME_ENCODING_ALLOWANCE)
                * ABSORB_BUILD_MULTIPLIER
        );
        // A too-small configured budget floors up to the worst frame.
        assert_eq!(
            floored_budget_capacity(64 * 1024 * 1024),
            absorb_worst_frame_transient()
        );
        // A generous budget is untouched.
        assert_eq!(
            floored_budget_capacity(256 * 1024 * 1024),
            256 * 1024 * 1024
        );
        // At exactly the floor, worst-frame reservations run one at a
        // time and ALL complete (no deadlock, no starvation).
        let budget: &'static AbsorbBudget =
            Box::leak(Box::new(AbsorbBudget::new(floored_budget_capacity(0), 4)));
        let done = std::sync::Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..3 {
            let done = done.clone();
            handles.push(tokio::spawn(async move {
                let r = budget.reserve(absorb_worst_frame_transient()).await;
                assert_eq!(r.granted(), absorb_worst_frame_transient());
                // Serialized by bytes: nothing else can be reserved.
                assert_eq!(
                    budget.reserved_bytes(),
                    absorb_worst_frame_transient() as u64
                );
                done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }));
        }
        for h in handles {
            tokio::time::timeout(Duration::from_secs(10), h)
                .await
                .expect("oversized gathers must not starve")
                .unwrap();
        }
        assert_eq!(done.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    /// Absorber phases are seeded from the shard prefix: different
    /// prefixes yield different phases (co-opened shards do not flush
    /// in lockstep), and the phase always fits inside one tick.
    #[test]
    fn absorber_phase_stagger_is_prefix_seeded() {
        let tick_ms = 5000u64;
        let phase = |prefix: &str| -> u64 {
            let h = crate::crypto::stream_hash(prefix);
            u64::from_le_bytes(h[..8].try_into().unwrap()) % tick_ms
        };
        let prefixes = [
            "0000", "0001", "0010", "0011", "0100", "0101", "0110", "0111", "1000", "1001", "1010",
            "1011", "1100", "1101", "1110", "1111",
        ];
        let phases: Vec<u64> = prefixes.iter().map(|p| phase(p)).collect();
        let distinct: std::collections::HashSet<_> = phases.iter().collect();
        assert!(
            distinct.len() >= 12,
            "16 shard prefixes must spread across the tick, got {distinct:?}"
        );
        assert!(phases.iter().all(|p| *p < tick_ms));
    }

    /// Wedge detector: a stale in-progress db.write reads as blocked;
    /// idle (0) and fresh writes do not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_ms_tracks_stale_writes() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = Db::builder("w/shard", store.clone()).build().await.unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "w".into(),
            Arc::new(db),
            store.clone(),
            ShardConfig::default(),
            absorb_tx,
            None,
            crate::shard::ShardMaintenance::default(),
        );
        assert_eq!(engine.commit_blocked_ms(), 0, "idle engine must read 0");
        engine.set_commit_write_started_ms(crate::shard::now_ms() - 5_000);
        assert!(
            engine.commit_blocked_ms() >= 4_500,
            "stale write must read blocked"
        );
        engine.set_commit_write_started_ms(crate::shard::now_ms());
        assert!(
            engine.commit_blocked_ms() < 2_000,
            "fresh write must not trip the shed"
        );
        engine.set_commit_write_started_ms(0);
        assert_eq!(engine.commit_blocked_ms(), 0);
    }

    /// End-to-end wedge detection under REAL SlateDB byte backpressure: a
    /// store whose PUTs stall blocks the commit db.write once the unflushed
    /// cap fills, and commit_blocked_ms() must cross the shed threshold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_detects_real_flush_stall() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        let db = Db::builder("s/shard", slow.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                max_unflushed_bytes: 8 * 1024,
                // 0.15 validates max_unflushed > l0_sst_size; keep the tiny
                // unflushed cap (the stall trigger) and shrink L0 under it.
                l0_sst_size_bytes: 4 * 1024,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "s".into(),
            Arc::new(db),
            slow.clone(),
            ShardConfig::default(),
            absorb_tx,
            None,
            crate::shard::ShardMaintenance::default(),
        );

        // Continuous feed: a LATER db.write must find the unflushed cap
        // full (the first group is admitted regardless) and block there.
        let feeder = engine.clone();
        let feed = tokio::spawn(async move {
            for i in 0..4096u64 {
                let (tx, _rx) = tokio::sync::oneshot::channel();
                let req = crate::shard::AppendReq {
                    usage: Default::default(),
                    hash: [9u8; 16],
                    route: [0u8; 16],
                    enqueued_at: Instant::now(),
                    entries: vec![bytes::Bytes::from(vec![b'x'; 1024])],
                    routing_key: String::new(),
                    key_hash: crate::crypto::stream_hash(""),
                    producer_lineage: Vec::new(),
                    key_version: 1,
                    subkey: [0u8; 32],
                    ts_hint_ms: Some(i as i64),
                    seq: None,
                    bytes: 1024,
                    finish: crate::shard::AppendFinish::Open,
                    producer: None,
                    deferred_error: None,
                    sealed_reject_new: None,
                    touch: None,
                    seal_gen: None,
                    billing: None,
                    resp: tx,
                };
                let _ = feeder.try_enqueue(req);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let mut blocked = 0;
        while Instant::now() < deadline {
            blocked = engine.wedge_ms();
            if blocked > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        feed.abort();
        assert!(
            blocked > 5_000,
            "wedge_ms never crossed the shed threshold (last {blocked})"
        );
    }

    /// The stale-durability wedge mode: db.write keeps succeeding (default
    /// unflushed cap is huge) while WAL flushes stall — committed groups
    /// age in in_flight and oldest_inflight_ms must cross the threshold.
    /// This is the mode the 2026-07-22 cloud gate proved commit_blocked_ms
    /// alone cannot see.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wedge_detects_stale_durability() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        // Default (large) unflushed cap: writes are admitted, durability stalls.
        let db = Db::builder("d/shard", slow.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "d".into(),
            Arc::new(db),
            slow.clone(),
            ShardConfig::default(),
            absorb_tx,
            None,
            crate::shard::ShardMaintenance::default(),
        );
        for i in 0..8u64 {
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                usage: Default::default(),
                hash: [7u8; 16],
                route: [0u8; 16],
                enqueued_at: Instant::now(),
                entries: vec![bytes::Bytes::from(vec![b'y'; 512])],
                routing_key: String::new(),
                key_hash: crate::crypto::stream_hash(""),
                producer_lineage: Vec::new(),
                key_version: 1,
                subkey: [0u8; 32],
                ts_hint_ms: Some(i as i64),
                seq: None,
                bytes: 512,
                finish: crate::shard::AppendFinish::Open,
                producer: None,
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                seal_gen: None,
                billing: None,
                resp: tx,
            };
            let _ = engine.try_enqueue(req);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut wedge = 0;
        while Instant::now() < deadline {
            wedge = engine.wedge_ms();
            if wedge > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            wedge > 5_000,
            "oldest_inflight_ms never crossed the shed threshold (last {wedge})"
        );
        assert!(
            engine.oldest_inflight_ms() > 5_000,
            "the stale-durability component specifically must be the signal"
        );
    }
}

#[cfg(test)]
mod bounded_discovery_tests {
    use super::*;

    #[test]
    fn r09_hot_prefix_cannot_starve_other_due_streams() {
        let now = Instant::now();
        let cfg = AbsorberConfig::default();
        let pending: HashMap<_, _> = (0..6u8)
            .map(|id| {
                (
                    [id; 16],
                    PendingAbsorb {
                        bytes: u64::MAX - id as u64,
                        since: now,
                        failures: 0,
                        retry_after: None,
                    },
                )
            })
            .collect();
        let first: Vec<_> = due_streams(&pending, &cfg, now, None)
            .into_iter()
            .take(3)
            .map(|(hash, _)| hash)
            .collect();
        let second: Vec<_> = due_streams(&pending, &cfg, now, first.last().copied())
            .into_iter()
            .take(3)
            .map(|(hash, _)| hash)
            .collect();
        assert_eq!(first, vec![[0; 16], [1; 16], [2; 16]]);
        assert_eq!(second, vec![[3; 16], [4; 16], [5; 16]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r09_discovery_pages_progress_without_exceeding_pending_capacity() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("r09-history", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let mut batch = WriteBatch::new();
        for id in 0..260u64 {
            let mut hash = [0; 16];
            hash[..8].copy_from_slice(&id.to_be_bytes());
            let marker = crate::shard::dirty_value_for_tests(&crate::shard::StreamMaintenance {
                next: 1,
                unabsorbed_bytes: 64,
                ..Default::default()
            });
            let width = [16, 24, 32][id as usize % 3];
            batch.put(crate::shard::dirty_key(&hash), &marker[..width]);
            batch.put(
                crate::shard::tail_key(&hash),
                crate::shard::encode_tail_for_tests(&crate::shard::TailFields {
                    next: 1,
                    unabsorbed_bytes: 64,
                    route: [1; 16],
                    ..Default::default()
                }),
            );
        }
        db.write(batch).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let engine = ShardEngine::start(
            "r09-history".into(),
            db.clone(),
            store.clone(),
            crate::shard::ShardConfig::default(),
            tx,
            None,
            Default::default(),
        );
        let absorber = Absorber::new(
            store,
            engine.clone(),
            Arc::new(KeyCache::default()),
            AbsorberConfig::default(),
        );
        let mut pending = HashMap::new();
        assert_eq!(
            absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
            DISCOVERY_PAGE_STREAMS
        );
        assert_eq!(pending.len(), DISCOVERY_PAGE_STREAMS);
        assert!(absorber.discovery_after.lock().unwrap().is_some());
        assert_eq!(
            absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
            4
        );
        assert_eq!(pending.len(), 260);
        assert!(absorber.discovery_after.lock().unwrap().is_none());
        pending.clear();
        for id in 0..MAX_PENDING_STREAMS {
            let mut hash = [255; 16];
            hash[..8].copy_from_slice(&(id as u64).to_be_bytes());
            pending.insert(
                hash,
                PendingAbsorb {
                    bytes: 1,
                    since: Instant::now(),
                    failures: 0,
                    retry_after: None,
                },
            );
        }
        absorber.seed_from_dirty_index(&mut pending).await.unwrap();
        assert_eq!(pending.len(), MAX_PENDING_STREAMS);
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
mod record_validation_tests;

mod worker;

#[cfg(test)]
mod postings_validation_tests;
