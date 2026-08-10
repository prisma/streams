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

// ---- block transformer: AES-256-GCM with a random nonce per block ----

/// Operator pause for the whole absorber (fleet runbook).
pub fn absorb_pause_flag() -> &'static std::sync::atomic::AtomicBool {
    static F: std::sync::OnceLock<std::sync::atomic::AtomicBool> = std::sync::OnceLock::new();
    F.get_or_init(|| {
        std::sync::atomic::AtomicBool::new(
            std::env::var("ABSORB_PAUSE").ok().as_deref() == Some("1"),
        )
    })
}

pub fn absorb_paused() -> bool {
    absorb_pause_flag().load(std::sync::atomic::Ordering::Relaxed)
}

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
pub fn absorb_worst_frame_transient() -> usize {
    worst_frame_transient_for(crate::http::max_body_bytes())
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
pub static RESOLVED_GATHER_PACKING_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The startup clamp, as one function so the absorber config, the
/// startup log, and the debug surface cannot disagree about it.
pub fn resolved_gather_packing_bytes(configured: usize) -> usize {
    configured.min(absorb_budget().capacity() / ABSORB_BUILD_MULTIPLIER)
}

/// What ONE gather actually reserves.
///
/// R23-3: the reservation is `max(packing x multiplier, worst_frame)`
/// clamped to capacity — NOT the worst frame alone. Reporting
/// `capacity / worst_frame` as the concurrency (as the debug surface and
/// the first chaos report did) overstates it whenever the packing term
/// dominates, which is the common case: at bare defaults the packing
/// limit clamps to capacity/3, so `packing x 3` IS the whole capacity
/// and only one gather can ever run.
pub fn per_gather_reservation_bytes() -> usize {
    let cap = absorb_budget().capacity();
    let packing = RESOLVED_GATHER_PACKING_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    let by_packing = packing.saturating_mul(ABSORB_BUILD_MULTIPLIER);
    by_packing.max(absorb_worst_frame_transient()).clamp(1, cap)
}

/// Gathers that can genuinely run at once: the configured slot count,
/// bounded by how many whole reservations fit in the budget.
pub fn effective_gather_concurrency() -> usize {
    let cap = absorb_budget().capacity();
    let per = per_gather_reservation_bytes().max(1);
    absorb_budget().gather_slots().min((cap / per).max(1))
}

/// Injected history-flush slowdown, ms per gather flush (0 = off).
/// Set via POST /v1/debug/history-stall?ms= — the STALLED-HISTORY-
/// FLUSH acceptance leg's lever: it stalls the actual gather flush
/// path WITH the reservation held (it does not pause the SlateDB
/// compactor itself).
pub static HISTORY_FLUSH_STALL_MS: AtomicU64 = AtomicU64::new(0);

/// The budget floor as a pure function (tested directly): a configured
/// capacity below one worst-case frame build is raised to it.
pub fn floored_budget_capacity(configured: usize) -> usize {
    configured.max(absorb_worst_frame_transient())
}

/// The RESOLVED memory posture, captured once at startup from the same
/// values the budget summary logs — the acceptance campaign's
/// verify-before-load reads this via /v1/debug/absorb and requires
/// EXACT equality with deploy/profiles/compute-1g.env. A verifier that
/// checks four of twelve knobs proves nothing about the other eight.
pub static RESOLVED_MEMORY_CONFIG: std::sync::OnceLock<serde_json::Value> =
    std::sync::OnceLock::new();

pub fn absorb_budget() -> &'static AbsorbBudget {
    static B: std::sync::OnceLock<AbsorbBudget> = std::sync::OnceLock::new();
    B.get_or_init(|| {
        // The test binary runs MANY independent DST engines in one
        // process; a production-sized global budget would serialize
        // their gathers ACROSS TESTS and turn timing-sensitive
        // scenarios flaky. Budget semantics are pinned by dedicated
        // unit tests on local AbsorbBudget instances; the global gets
        // ample capacity under cfg(test) unless a test opts in via
        // env.
        #[cfg(test)]
        let default_bytes: usize = 4 * 1024 * 1024 * 1024;
        #[cfg(not(test))]
        let default_bytes: usize = 64 * 1024 * 1024;
        #[cfg(test)]
        let default_gathers: usize = 64;
        #[cfg(not(test))]
        let default_gathers: usize = 2;
        let configured: usize = std::env::var("ABSORB_GLOBAL_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_bytes);
        // The floor makes the envelope claim TRUE for oversized
        // frames: capacity always covers one worst-case build.
        let bytes = floored_budget_capacity(configured);
        if bytes > configured {
            tracing::warn!(
                "ABSORB_GLOBAL_BUDGET_BYTES raised {configured} -> {bytes}: the budget \
                 must cover one worst-case frame build ({} B frame x{})",
                crate::http::MAX_BODY_BYTES,
                ABSORB_BUILD_MULTIPLIER,
            );
        }
        let gathers: usize = std::env::var("ABSORB_GLOBAL_GATHERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_gathers)
            .max(1);
        AbsorbBudget::new(bytes, gathers)
    })
}

/// The shed expression (OOM review P2), factored for a deterministic
/// test: pressure = sampled RSS + absorber bytes ALREADY RESERVED —
/// the reservation is visible the instant it is granted, so admission
/// backs off BEFORE the allocation shows up in an RSS sample.
pub fn memory_pressure_mb(rss_mb: u64, reserved_bytes: u64) -> u64 {
    rss_mb.saturating_add(reserved_bytes / (1024 * 1024))
}

pub struct AbsorbReservation {
    bytes: usize,
    budget: &'static AbsorbBudget,
    // RAII permits (review: cancellation safety). If reserve() is
    // cancelled mid-acquire — engine shutdown aborting the absorber, a
    // timed-out test dropping the future, a future select! — the
    // already-held permit drops and returns to its semaphore on its
    // own. forget()/add_permits bookkeeping leaked a gather slot
    // permanently in exactly that window.
    _gather: tokio::sync::SemaphorePermit<'static>,
    _bytes: tokio::sync::SemaphorePermit<'static>,
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
    pub async fn reserve(&'static self, estimate: usize) -> AbsorbReservation {
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

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn gather_slots_free(&self) -> usize {
        self.gathers.available_permits()
    }
}

impl AbsorbReservation {
    /// Bytes actually GRANTED (post-clamp) — what the gauges report.
    pub fn granted(&self) -> usize {
        self.bytes
    }
}

impl Drop for AbsorbReservation {
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

/// Bytes currently reserved by in-flight gathers — the admission shed
/// adds this to sampled RSS (a gather can allocate tens of MiB between
/// RSS samples; the reservation is visible the instant it is granted).
pub fn absorb_reserved_bytes() -> u64 {
    absorb_budget()
        .reserved
        .load(std::sync::atomic::Ordering::Relaxed)
}

pub fn absorb_gathers_inflight() -> u64 {
    absorb_budget()
        .inflight
        .load(std::sync::atomic::Ordering::Relaxed)
}

// Gather observability (OOM review instrumentation): last-gather phase
// timings + reserved-vs-actual, and cumulative absorbed/ingested bytes
// for rate derivation between ops snapshots.
pub static ABSORB_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static INGEST_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_RESERVED: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_ACTUAL: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_READ_MS: AtomicU64 = AtomicU64::new(0);
/// R23-6 read-amplification instrumentation for the last gather.
pub static GATHER_LAST_GETS: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_FETCHED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Fetched bytes per useful frame byte, x1000.
pub static GATHER_LAST_READ_AMP_X1000: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_WRITE_MS: AtomicU64 = AtomicU64::new(0);
pub static GATHER_LAST_FLUSH_MS: AtomicU64 = AtomicU64::new(0);
pub static HISTORY_FLUSH_WAIT_MS_MAX: AtomicU64 = AtomicU64::new(0);

pub(crate) fn history_cache() -> Arc<slatedb::db_cache::foyer::FoyerCache> {
    static CACHE: std::sync::OnceLock<Arc<slatedb::db_cache::foyer::FoyerCache>> =
        std::sync::OnceLock::new();
    CACHE
        .get_or_init(|| {
            let bytes = std::env::var("HISTORY_CACHE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(32 * 1024 * 1024);
            Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: bytes,
                    ..Default::default()
                },
            ))
        })
        .clone()
}

/// Settings for the SHARED history v2 partition (docs/HISTORY-V2.md).
/// Differences from v1 per-stream DBs, each deliberate: NO compression
/// (values are stream-key-encrypted frames — already compressed before
/// encryption, and ciphertext does not compress) and NO block
/// transformer (the frames are the ciphertext; object storage never
/// sees plaintext either way).
pub(crate) fn history2_settings() -> Settings {
    Settings {
        compression_codec: None,
        ..history_settings()
    }
}

pub(crate) fn history_settings() -> Settings {
    // Bench-only escape hatch: HISTORY_COMPACTOR=off disables the embedded
    // compactor (and lifts the L0 caps so flushes never block on it). Used
    // with the s3lite --discard-substr mode, where history SST bodies are
    // dropped and must never be re-read. Production keeps the compactor.
    let compactor_off = std::env::var("HISTORY_COMPACTOR")
        .map(|v| v == "off")
        .unwrap_or(false);
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
    let gc_interval = {
        let secs: u64 = std::env::var("HISTORY_GC_INTERVAL_SECS")
            .ok()
            .or_else(|| std::env::var("HISTORY_GC_MAX_INTERVAL_SECS").ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);
        (secs > 0).then(|| Duration::from_secs(secs))
    };
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
            Settings::default().compactor_options
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
    /// Interim sparse-stream policy (cost review round 2 verdict): the
    /// AGE trigger only fires for streams with at least this many
    /// pending bytes. A one-record stream costs ~43 Class A requests to
    /// move into per-stream history and its cold reads get SLOWER
    /// (28 → 321 ms), so tiny streams stay in the shard log — already
    /// durable there — until they accumulate real volume, the byte
    /// threshold fires, or shared history (docs/HISTORY-V2.md) lands.
    /// Deferred streams are counted separately from lag
    /// (`deferred_sparse_*` in /v1/debug/usage) so an intentional cost
    /// policy never reads as unhealthy absorption.
    pub min_age_bytes: u64,
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
            min_age_bytes: 256 * 1024,
            gather_max_bytes: 32 * 1024 * 1024,
        }
    }
}

struct PendingAbsorb {
    bytes: u64,
    since: Instant,
    /// Consecutive non-fence absorb failures; drives exponential backoff so
    /// a persistent error retries at tick·2^n instead of every tick.
    failures: u32,
    /// Earliest next attempt (backoff); zero-delay until the first failure.
    retry_after: Option<Instant>,
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
fn absorb_error_is_fence(msg: &str) -> bool {
    msg.contains("detected newer DB client")
        || msg.contains("Fenced")
        || msg.contains("Closed error")
}

pub struct Absorber {
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
        Absorber {
            data_store,
            shard,
            keys,
            cfg,
            submitted: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn start(
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        mut rx: mpsc::Receiver<AbsorbSignal>,
    ) -> tokio::task::JoinHandle<()> {
        let absorber = Self::new(data_store, shard, keys, cfg);
        tokio::spawn(async move {
            let mut pending: HashMap<[u8; 16], PendingAbsorb> = HashMap::new();
            // Restart rediscovery (static audit P1, hardened round 4):
            // seed from the durable dirty-stream index so work left
            // outstanding by a previous owner converges WITHOUT the
            // customer ever touching those streams again. The scan runs
            // inside the tick loop so signals keep flowing while it
            // retries: a failed startup scan with no retry permanently
            // stranded pre-restart streams (no signal, no handle, no
            // pending entry — no rediscovery path at all). Once seeded, a
            // low-cadence rescan re-merges anything runtime handle
            // eviction or dropped signals let slip. The resident-handle
            // sweep below remains as belt-and-braces.
            let mut seeded = false;
            let mut seed_failures: u32 = 0;
            let mut seed_next_tick: u32 = 0;
            const RESCAN_EVERY: u32 = 120; // ~10 min at the 5 s tick
            // OOM review item 2: co-opened shards must not become due
            // and flush together — seed each absorber's phase from its
            // shard prefix, exactly like the WAL tick stagger. Sixteen
            // absorbers with identical phases synchronized their gather
            // peaks; staggered phases spread them across the tick.
            let phase = {
                let h = crate::crypto::stream_hash(&absorber.shard.prefix);
                let tick_ms = absorber.cfg.tick.as_millis().max(1) as u64;
                Duration::from_millis(u64::from_le_bytes(h[..8].try_into().unwrap()) % tick_ms)
            };
            let mut tick =
                tokio::time::interval_at(tokio::time::Instant::now() + phase, absorber.cfg.tick);
            let mut tick_n: u32 = 0;
            loop {
                // Lifecycle: this task holds the engine Arc, so the signal
                // channel can never close on its own — without this check a
                // fenced shard's absorber survives as a zombie, retrying
                // forever against a dead db (the absorption war's fuel).
                if absorber.shard.is_closed() {
                    let dropped: u64 = pending.values().map(|p| p.bytes).sum();
                    tracing::info!(
                        shard = %absorber.shard.prefix,
                        pending_bytes = dropped,
                        "absorber exiting: shard fenced/closed"
                    );
                    // The new owner absorbs this backlog; leaving the lag
                    // entries frozen here reads as phantom absorb-lag on
                    // the heartbeat forever (and would re-trigger the
                    // rebalancer's alarm view after the move).
                    for h in pending.keys() {
                        crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(*h));
                    }
                    crate::usage::clear_shard_lag(&absorber.shard.prefix);
                    // The per-shard pending-summary row too (review round
                    // 4): after a shard moves, the old owner's frozen row
                    // double-counts against the new owner's — the
                    // instance rollup reports phantom backlog, and
                    // wide-report treats that rollup as its drain proof.
                    crate::usage::clear_absorb_pending_summary(&absorber.shard.prefix);
                    return;
                }
                tokio::select! {
                    sig = rx.recv() => {
                        let Some(sig) = sig else { return };
                        let e = pending.entry(sig.hash).or_insert(PendingAbsorb {
                            bytes: 0,
                            since: Instant::now(),
                            failures: 0,
                            retry_after: None,
                        });
                        e.bytes += sig.appended_bytes;
                    }
                    _ = tick.tick() => {
                        let now = Instant::now();
                        tick_n = tick_n.wrapping_add(1);
                        // Durable-index discovery: retry with exponential
                        // backoff until the FIRST scan succeeds, then
                        // rescan at low cadence as a safety net.
                        if (!seeded && tick_n >= seed_next_tick)
                            || (seeded && tick_n.is_multiple_of(RESCAN_EVERY))
                        {
                            match absorber.seed_from_dirty_index(&mut pending).await {
                                Ok(n) => {
                                    if !seeded && n > 0 {
                                        tracing::info!(
                                            "absorber seeded {} dirty streams from the durable index ({})",
                                            n,
                                            absorber.shard.prefix
                                        );
                                    }
                                    seeded = true;
                                }
                                Err(e) => {
                                    if seeded {
                                        tracing::warn!("dirty-stream index rescan failed: {e}");
                                    } else {
                                        seed_failures = seed_failures.saturating_add(1);
                                        let shift = seed_failures.min(6);
                                        seed_next_tick = tick_n
                                            .saturating_add(2u32.saturating_pow(shift));
                                        tracing::warn!(
                                            failures = seed_failures,
                                            "dirty-stream index scan failed at absorber start (retrying): {e}"
                                        );
                                    }
                                }
                            }
                        }
                        // Re-discovery sweep: signals are the fast path;
                        // this closes their gaps (the bounded channel's
                        // try_send drops under a wide backlog, and a
                        // restarted instance has no signals for pre-crash
                        // data). Thin backlogs (a few records) enter as
                        // small-lane entries due by AGE — a re-discovered
                        // wide backlog must trickle through the capped
                        // lanes, not stampede them (the uncapped first
                        // version opened a history DB per stream faster
                        // than anything evicted: 2.3 GB RSS in seven
                        // minutes). Fat backlogs enter due-now and big.
                        if tick_n.is_multiple_of(absorber.cfg.sweep_every.max(1)) {
                            for (hash, backlog_records) in absorber.shard.absorb_backlog() {
                                pending.entry(hash).or_insert_with(|| PendingAbsorb {
                                    // Signals carry exact appended bytes;
                                    // the sweep only knows the record
                                    // count. Estimate ~1 KiB/record so
                                    // fat recovered backlogs become due
                                    // and thin ones defer with the rest.
                                    bytes: backlog_records.saturating_mul(1024),
                                    since: Instant::now(),
                                    failures: 0,
                                    retry_after: None,
                                });
                            }
                            // Prune the submitted high-water map (it
                            // otherwise grows with every stream ever
                            // absorbed): an entry is only load-bearing
                            // while a re-gather could still observe a
                            // stale durable boundary — i.e. while the
                            // stream is pending or its resident absorbed
                            // boundary trails the submitted mark. Frames
                            // are deterministic and boundary submits are
                            // guarded, so over-pruning merely costs an
                            // idempotent rewrite.
                            absorber.submitted.lock().unwrap().retain(|h, v| {
                                pending.contains_key(h)
                                    || absorber
                                        .shard
                                        .resident_absorbed(h)
                                        .is_some_and(|a| a < v.0)
                            });
                        }
                        // Publish absorption lag (scale-out signal) for
                        // ELIGIBLE streams only: a stream the sparse
                        // policy defers (under min_age_bytes) is a cost
                        // decision, not unhealthy absorption, and must
                        // not trip the rebalancer. Deferred streams are
                        // counted separately.
                        let mut eligible: u64 = 0;
                        let mut oldest_eligible: u64 = 0;
                        let mut deferred: u64 = 0;
                        let mut deferred_bytes: u64 = 0;
                        for (h, p) in pending.iter() {
                            let age = p.since.elapsed().as_secs();
                            if p.bytes >= absorber.cfg.threshold_bytes
                                || p.bytes >= absorber.cfg.min_age_bytes
                            {
                                eligible += 1;
                                oldest_eligible = oldest_eligible.max(age);
                                crate::usage::set_absorb_lag(crate::crypto::SegmentHash(*h), age);
                            } else {
                                deferred += 1;
                                deferred_bytes += p.bytes;
                                crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(*h));
                            }
                        }
                        crate::usage::set_absorb_pending_summary(
                            &absorber.shard.prefix,
                            eligible,
                            oldest_eligible,
                            deferred,
                            deferred_bytes,
                        );
                        // Per-shard lag: the rebalancer picks its victim
                        // from THIS, keyed by the shard we actually serve.
                        crate::usage::set_shard_lag(&absorber.shard.prefix, oldest_eligible);
                        // Test hook (SCALING.md D3): pause absorption so
                        // lag grows while the tick keeps publishing it.
                        // RUNTIME-togglable: pausing via env needs a
                        // restart, and a restart hands the instance's
                        // shards to its peers — the paused instance then
                        // has no absorber to lag (ladder p8 D3).
                        if absorb_paused() {
                            continue;
                        }
                        // Due = byte threshold, OR old enough AND fat
                        // enough for age absorption (the interim sparse
                        // policy: tiny streams stay in the shard log).
                        let mut due: Vec<([u8; 16], u64)> = pending
                            .iter()
                            .filter(|(_, p)| {
                                (p.bytes >= absorber.cfg.threshold_bytes
                                    || (p.bytes >= absorber.cfg.min_age_bytes
                                        && p.since.elapsed() >= absorber.cfg.threshold_age))
                                    && p.retry_after.map(|t| now >= t).unwrap_or(true)
                            })
                            .map(|(h, p)| (*h, p.bytes))
                            .collect();
                        // Fattest first: under backlog pressure the hot
                        // streams (large pending bytes) must not queue
                        // behind ten thousand one-record strays — their
                        // unabsorbed bytes are what grows the shard log.
                        due.sort_unstable_by(|a, b| b.1.cmp(&a.1));
                        // Three lanes. V2 (shared partition, one flush for
                        // the whole lane) takes every stream whose
                        // history lives — or will live — in the shared
                        // partition: the history_v2 flag, or a stream
                        // that has never absorbed. Legacy v1 streams keep
                        // the two per-stream lanes (docs/COST-WIDE1.md §1:
                        // the serial grind was the ceiling; the
                        // concurrent small lane its repair). ALL lanes
                        // are capped per tick: the tick must return to
                        // the select loop, and v1 eviction must run often
                        // enough that open_dbs stays near the LRU — an
                        // uncapped tick once grew 2.3 GB of open history
                        // DBs before its first eviction. Leftover due
                        // entries simply run next tick. Classification
                        // reads resident handle state (map lookup) and is
                        // itself capped.
                        const V2_LANE_PER_TICK: usize = 1024;
                        const CLASSIFY_PER_TICK: usize = 4096;
                        let mut v2_lane: Vec<[u8; 16]> = Vec::new();
                        for (hash, bytes) in due.into_iter().take(CLASSIFY_PER_TICK) {
                            if v2_lane.len() >= V2_LANE_PER_TICK {
                                break;
                            }
                            let Ok(handle) = absorber.shard.stream_handle(hash).await else {
                                continue;
                            };
                            // Lane eligibility reads the APPLIED tail, not
                            // the durable one (round-4 root cause): a
                            // signal can arrive before its append's batch
                            // DISPATCHES, so the durable snapshot briefly
                            // shows route==0 / absorbed==0 and the lane
                            // decision flaps — the zero-route guard sent
                            // fresh routed streams down v1, and one tick
                            // later a stale absorbed==0 re-admitted v2.
                            // `applied` is updated synchronously at commit,
                            // strictly before any signal for that batch
                            // exists, so it cannot race the classifier.
                            // (The committer-side layout seal remains the
                            // hard correctness backstop.)
                            let (absorbed, v2flag, route) = {
                                let st = handle.state.lock().unwrap();
                                (st.applied.absorbed, st.applied.history_v2, st.applied.route)
                            };
                            // Zero-route guard (static audit): a legacy
                            // stream with no name-level route must NOT
                            // enter v2 — its records would be keyed under
                            // route 0x00.. and a future route-range split
                            // would classify them into the wrong range.
                            // Such streams keep the v1 per-stream layout.
                            // The v1 per-stream layout was DELETED in the
                            // pre-launch clean switch: every stream in a
                            // fresh namespace carries a route from its
                            // first append. A zero-route tail with data is
                            // a bug, not a layout — count it, drop it, and
                            // never write the deleted format.
                            let v2_eligible = v2flag || (absorbed == 0 && route != [0u8; 16]);
                            #[cfg(test)]
                            if std::env::var("DST_DRAIN_TRACE").is_ok() {
                                eprintln!(
                                    "CLASSIFY {} absorbed={absorbed} v2flag={v2flag} route_set={} eligible={v2_eligible} bytes={bytes}",
                                    crate::crypto::hex(&hash[..4]),
                                    route != [0u8; 16],
                                );
                            }
                            let _ = bytes;
                            if v2_eligible {
                                v2_lane.push(hash);
                            } else {
                                ABSORB_ZERO_ROUTE_DROPPED
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::warn!(
                                    "zero-route tail {} has unabsorbed data; the v1 layout \
                                     no longer exists — dropping from the absorb queue",
                                    crate::crypto::hex(&hash[..4]),
                                );
                                pending.remove(&hash);
                            }
                        }
                        if !v2_lane.is_empty() && !absorber.shard.is_closed() {
                            // OOM review item 1: reserve the PROCESS-WIDE
                            // budget before a single frame is read. The
                            // estimate is this gather's packing cap times
                            // the build multiplier; waiting here is the
                            // intended backpressure when other shards'
                            // gathers hold the budget.
                            let est = absorber
                                .cfg
                                .gather_max_bytes
                                .saturating_mul(ABSORB_BUILD_MULTIPLIER)
                                .max(absorb_worst_frame_transient());
                            let _reservation = absorb_budget().reserve(est).await;
                            GATHER_LAST_RESERVED.store(
                                _reservation.granted() as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            if absorber.shard.is_closed() {
                                continue; // fenced while waiting for budget
                            }
                            match absorber.absorb_gather_v2(&v2_lane).await {
                                Ok(outcome) => {
                                    // Retire ONLY what the gather settled:
                                    // covered streams advanced; no_work had
                                    // nothing durable to absorb (residues
                                    // and new data re-arrive via
                                    // signals/sweep). Budget-deferred
                                    // streams KEEP their pending entry, lag
                                    // and age — they gather next tick
                                    // without needing a new signal or the
                                    // ~60 s handle sweep (review round 4:
                                    // removing them silently stranded
                                    // their backlog for up to a minute and
                                    // blinded the fleet lag view).
                                    let partial: std::collections::HashMap<[u8; 16], u64> =
                                        outcome.partial.iter().copied().collect();
                                    for (h, _, _) in &outcome.advanced {
                                        // A PARTIAL advance is progress, not
                                        // completion: keep it pending so the
                                        // next tick continues immediately.
                                        if partial.contains_key(h) {
                                            continue;
                                        }
                                        pending.remove(h);
                                        crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(*h));
                                    }
                                    for (h, remaining) in &partial {
                                        let est = remaining.saturating_mul(1024);
                                        pending
                                            .entry(*h)
                                            .and_modify(|p| {
                                                p.bytes = p.bytes.max(est);
                                                // Progress clears the failure
                                                // backoff; the age is left
                                                // alone so age-based due keeps
                                                // its original meaning.
                                                p.failures = 0;
                                                p.retry_after = None;
                                            })
                                            .or_insert(PendingAbsorb {
                                                bytes: est,
                                                since: Instant::now(),
                                                failures: 0,
                                                retry_after: None,
                                            });
                                    }
                                    for h in &outcome.no_work {
                                        pending.remove(h);
                                        crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(*h));
                                    }
                                }
                                Err(e) => {
                                    let msg = e.to_string();
                                    if absorb_error_is_fence(&msg) {
                                        tracing::warn!(
                                            "v2 gather fence-class ({} streams): {msg}",
                                            v2_lane.len()
                                        );
                                        // Engine is dying; the exit path
                                        // clears pending.
                                    } else {
                                        tracing::warn!(
                                            "v2 gather failed ({} streams): {msg}",
                                            v2_lane.len()
                                        );
                                        for h in &v2_lane {
                                            if let Some(p) = pending.get_mut(h) {
                                                p.failures = p.failures.saturating_add(1);
                                                let shift = p.failures.min(6);
                                                p.retry_after = Some(
                                                    now + absorber.cfg.tick * 2u32.pow(shift),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
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
        let dirty = self.shard.scan_dirty_streams().await?;
        let mut absorb_seeded = 0usize;
        for (h, absorbed, next) in dirty {
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
    pub(crate) async fn absorb_gather_v2(
        &self,
        streams: &[[u8; 16]],
    ) -> anyhow::Result<GatherOutcome> {
        const PER_STREAM_CAP: usize = 4 * 1024 * 1024;
        // Rough WriteBatch bookkeeping cost per entry, on top of key+value.
        const ENTRY_OVERHEAD: usize = 64;
        let part = self.shard.history_partition().await?;
        let t_read = Instant::now();
        // R23-6: snapshot object-store GETs so this gather's READ
        // AMPLIFICATION (bytes fetched per useful frame byte) is
        // measurable. The 21-42 s read phase is the real CHAOS-5
        // ceiling; amplification is what explains it.
        let get0 = crate::store_timing::GET_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let bytes0 = crate::store_timing::GET_BYTES.load(std::sync::atomic::Ordering::Relaxed);
        let mut wb = WriteBatch::new();
        let mut out = GatherOutcome::default();
        let mut batch_bytes: usize = 0;
        // (segment, chunk_from, chunk_to, per-key runs) for write-through
        // cache warming — installed only after the batch flush succeeds.
        type WarmChunk = (
            SegmentHash,
            u64,
            u64,
            Vec<([u8; 16], Vec<crate::postings::AbsRun>)>,
        );
        let mut warm_installs: Vec<WarmChunk> = Vec::new();
        for hash in streams {
            // Aggregate budget: the batch is held in memory until the one
            // flush below, so its size — not the lane's stream count — is
            // what a 1 GiB instance actually feels. Anything deferred here
            // stays in the pending set and gathers on a later tick.
            if batch_bytes >= self.cfg.gather_max_bytes {
                out.deferred_budget.push(*hash);
                continue;
            }
            let handle = self.shard.stream_handle(*hash).await?;
            let (from, upto, route) = {
                let st = handle.state.lock().unwrap();
                (
                    st.durable.absorbed,
                    st.durable.next,
                    RouteHash(st.durable.route),
                )
            };
            let inc = SegmentHash(*hash);
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
            let per_stream = PER_STREAM_CAP.min(self.cfg.gather_max_bytes);
            let chunk = read_frames_range(&self.shard, &handle, from, upto, per_stream).await?;
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
                if crate::crypto::decode_frame(raw).is_none() {
                    anyhow::bail!("undecodable frame during v2 gather");
                }
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
            batch_bytes += chunk_bytes;
            #[cfg(test)]
            if std::env::var("DST_DRAIN_TRACE").is_ok() {
                let offs: Vec<u64> = chunk
                    .frames
                    .iter()
                    .filter_map(|raw| crate::crypto::decode_frame(raw))
                    .map(|f| f.header.offset)
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
                let Some(frame) = crate::crypto::decode_frame(raw) else {
                    anyhow::bail!("undecodable frame during v2 gather");
                };
                let off = frame.header.offset;
                wb.put(hist2_record_key(route, inc, off), raw.clone());
                pages.note_frame(
                    crate::postings::rk_hash(&frame.header.routing_key),
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
            let mut chunk_runs: std::collections::HashMap<[u8; 16], Vec<crate::postings::AbsRun>> =
                std::collections::HashMap::new();
            for (kh, bucket, first, value) in emitted {
                match crate::postings::decode_page_abs(first, &value) {
                    Some(abs) => {
                        POSTINGS_RUNS_WRITTEN
                            .fetch_add(abs.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        crate::postings::append_page_runs(chunk_runs.entry(kh.0).or_default(), abs);
                    }
                    None => anyhow::bail!("postings page failed self-decode during gather"),
                }
                wb.put(
                    crate::postings::postings_key(route, inc, &kh, bucket, first),
                    value,
                );
            }
            POSTINGS_BYTES_WRITTEN.fetch_add(postings_bytes, std::sync::atomic::Ordering::Relaxed);
            CANONICAL_BYTES_WRITTEN.fetch_add(chunk_raw, std::sync::atomic::Ordering::Relaxed);
            warm_installs.push((inc, from, last + 1, chunk_runs.into_iter().collect()));
            out.advanced.push((*hash, last + 1, chunk_raw));
            // Truncated by the per-stream cap: more durable data sits
            // below `upto`. The caller must keep this stream pending.
            if last + 1 < upto {
                out.partial.push((*hash, upto - (last + 1)));
            }
        }
        if out.advanced.is_empty() {
            return Ok(out);
        }
        let ord = std::sync::atomic::Ordering::Relaxed;
        GATHER_LAST_READ_MS.store(t_read.elapsed().as_millis() as u64, ord);
        GATHER_LAST_ACTUAL.store(batch_bytes as u64, ord);
        let gets = crate::store_timing::GET_COUNT
            .load(ord)
            .saturating_sub(get0);
        let fetched = crate::store_timing::GET_BYTES
            .load(ord)
            .saturating_sub(bytes0);
        GATHER_LAST_GETS.store(gets, ord);
        GATHER_LAST_FETCHED_BYTES.store(fetched, ord);
        // x1000 so the ratio survives as an integer atomic.
        GATHER_LAST_READ_AMP_X1000.store(
            if batch_bytes > 0 {
                fetched.saturating_mul(1000) / batch_bytes as u64
            } else {
                0
            },
            ord,
        );
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
        ABSORB_BYTES_TOTAL.fetch_add(out.advanced.iter().map(|(_, _, b)| *b).sum::<u64>(), ord);
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
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
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
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
    let mut frames: Vec<Bytes> = Vec::new();
    let mut last: Option<u64> = None;
    let mut completed = true;
    let mut total = 0usize;
    let range = hist2_record_key(route, inc, from)..hist2_record_key(route, inc, upto);
    let mut iter = part.scan_with_options(range, &hist_scan_opts()).await?;
    while let Some(kv) = iter.next().await? {
        let off = u64::from_be_bytes(
            kv.key[kv.key.len() - 8..]
                .try_into()
                .expect("hist2 key tail"),
        );
        total += kv.value.len();
        frames.push(kv.value);
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
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
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
            let first = u64::from_be_bytes(
                kv.key[kv.key.len() - 8..]
                    .try_into()
                    .expect("postings key tail"),
            );
            match crate::postings::decode_page_abs(first, &kv.value) {
                Some(abs) => crate::postings::append_page_runs(&mut runs, abs),
                None => {
                    corrupt = true;
                    break;
                }
            }
        }
    }
    if corrupt {
        POSTINGS_CORRUPT.fetch_add(1, Relaxed);
        return read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await;
    }
    let clipped = clip_runs_to(&runs, from, upto);
    execute_postings_plan(part, route, inc, rk, clipped, upto, upto, max_bytes).await
}

fn clip_runs_to(
    runs: &[crate::postings::AbsRun],
    from: u64,
    upto: u64,
) -> Vec<crate::postings::AbsRun> {
    let mut clipped: Vec<crate::postings::AbsRun> = Vec::new();
    for r in runs {
        let start = r.start.max(from);
        let end = (r.start + r.count as u64).min(upto);
        if start >= end {
            continue;
        }
        // Byte fields stay whole-run estimates after clipping — the
        // planner treats them as estimates, and the byte budget below
        // enforces the real cap during execution.
        clipped.push(crate::postings::AbsRun {
            start,
            count: (end - start) as u32,
            matching_bytes: r.matching_bytes,
            gap_bytes_before: r.gap_bytes_before,
        });
    }
    clipped
}

/// Keyed read through the DECODED SLICE CACHE (spec §7): the engine's
/// cache resolves the runs (hit, single-flight cold load, or forward
/// extension), then the shared planner/executor below serves them.
/// `provable_to < upto` (a load window that could not reach the whole
/// range) yields an honest partial at the proven boundary.
#[allow(clippy::too_many_arguments)]
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
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
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
            let clipped = clip_runs_to(&runs, from, provable_to);
            execute_postings_plan(part, route, inc, rk, clipped, provable_to, upto, max_bytes).await
        }
    }
}

/// Shared span planner + executor (spec §8): plans against
/// [.., provable_to), executes each span as ONE canonical range scan
/// with exact-key verification, and reports completion relative to the
/// FULL requested `upto` (provable_to < upto is always a partial).
async fn execute_postings_plan(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    clipped: Vec<crate::postings::AbsRun>,
    provable_to: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    // 3. Plan bounded spans.
    let cfg = crate::postings::PlanCfg {
        max_scan_bytes: (max_bytes as u64).min(crate::postings::PlanCfg::default().max_scan_bytes),
        ..Default::default()
    };
    let plan = crate::postings::plan_spans(&clipped, provable_to, &cfg);
    let mut spans_used = 0u64;
    let mut frames: Vec<Bytes> = Vec::new();
    let mut last: Option<u64> = None;
    let mut total = 0usize;
    let mut truncated = false;
    // 4. Execute each span as one canonical range scan with exact-key
    // verification.
    // Spec §8.4: bounded-concurrency span execution (max 4 in flight),
    // results assembled in span order — cold multi-span reads pay
    // max(RTT), not sum(RTT). Serial execution measured 2x the covering
    // baseline's cold p50 on the two-span batch-1 shape.
    {
        use futures_util::StreamExt;
        let mut results = futures_util::stream::iter(plan.spans.iter().copied().map(|span| {
            let part = part.clone();
            let rk = rk.to_string();
            async move {
                let range = hist2_record_key(route, inc, span.start)
                    ..hist2_record_key(route, inc, span.end);
                // Read-ahead sized from the plan's own scan estimate: a
                // blanket 2 MiB per span floods the shared history block
                // cache (32 MiB default) — ~16 keyed reads evict every
                // index/filter/data block, so warm reads re-fetch the
                // world (measured: warm == cold, ~20 GETs per read on a
                // multi-SST partition). Spans are planner-bounded and
                // typically tiny; fetch what the span needs plus slack.
                let opts = slatedb::config::ScanOptions {
                    read_ahead_bytes: (span.scan_bytes.saturating_mul(3) / 2)
                        .clamp(64 * 1024, 2 * 1024 * 1024)
                        as usize,
                    max_fetch_tasks: 2,
                    cache_blocks: true,
                    ..Default::default()
                };
                let mut iter = part.scan_with_options(range, &opts).await?;
                let mut hits: Vec<(u64, Bytes)> = Vec::new();
                let mut span_bytes = 0usize;
                let mut span_trunc = false;
                while let Some(kv) = iter.next().await? {
                    READ_FRAMES_SCANNED.fetch_add(1, Relaxed);
                    let Some(f) = crate::crypto::decode_frame(&kv.value) else {
                        anyhow::bail!("undecodable v2 history frame");
                    };
                    if f.header.routing_key != rk {
                        continue;
                    }
                    READ_FRAMES_MATCHED.fetch_add(1, Relaxed);
                    // Stop MATERIALIZING once this span alone could fill
                    // the whole response — never buffer an unbounded run
                    // into memory (review blocker: long runs must page,
                    // and the FIRST record must always fit regardless of
                    // its size).
                    if !hits.is_empty() && span_bytes + kv.value.len() > max_bytes {
                        span_trunc = true;
                        break;
                    }
                    span_bytes += kv.value.len();
                    hits.push((f.header.offset, kv.value));
                }
                anyhow::Ok((span, hits, span_trunc))
            }
        }))
        .buffered(4);
        'spans: while let Some(res) = results.next().await {
            let (span, hits, span_trunc) = res?;
            spans_used += 1;
            for (off, raw) in hits {
                total += raw.len();
                frames.push(raw);
                last = Some(off);
                if total >= max_bytes {
                    truncated = true;
                    break 'spans;
                }
            }
            if span_trunc {
                // The span stopped mid-run: the cursor holds at the last
                // emitted record, NOT the span end — later results are
                // discarded and the caller re-polls from there.
                truncated = true;
                break 'spans;
            }
            // The span is fully consumed even if nothing matched (hash
            // collisions or clipping estimates): the cursor may advance.
            last = Some(last.map_or(span.end - 1, |l| l.max(span.end - 1)));
        }
    }
    READ_SPANS_MAX.fetch_max(spans_used, Relaxed);
    if truncated {
        return Ok((frames, last, false));
    }
    // 5. Cursor semantics: a complete plan consumed everything the
    // index PROVED — including any match-free tail — so the caller's
    // next page starts there. Completion is relative to the full
    // request: an index window short of `upto` is an honest partial.
    last = Some(last.map_or(plan.consumed_to.saturating_sub(1), |l| {
        l.max(plan.consumed_to.saturating_sub(1))
    }));
    Ok((frames, last, plan.complete && provable_to >= upto))
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
) -> anyhow::Result<(Vec<Bytes>, Option<u64>, bool)> {
    let mut frames: Vec<Bytes> = Vec::new();
    let mut last: Option<u64> = None;
    let mut completed = true;
    let mut total = 0usize;
    let range = hist2_record_key(route, inc, from)..hist2_record_key(route, inc, upto);
    let mut iter = part.scan_with_options(range, &hist_scan_opts()).await?;
    while let Some(kv) = iter.next().await? {
        let Some(f) = crate::crypto::decode_frame(&kv.value) else {
            anyhow::bail!("undecodable v2 history frame");
        };
        let off = f.header.offset;
        if f.header.routing_key != rk {
            // Consumed but not matching: the cursor may advance past it.
            last = Some(last.map_or(off, |l| l.max(off)));
            continue;
        }
        total += kv.value.len();
        frames.push(kv.value);
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
    fn fence_class_errors_are_recognized() {
        assert!(absorb_error_is_fence(
            "error: detected newer DB client at manifest 7"
        ));
        assert!(absorb_error_is_fence("Fenced"));
        assert!(absorb_error_is_fence("io wrapper: Closed error: db closed"));
        assert!(!absorb_error_is_fence("timeout waiting for PUT"));
        assert!(!absorb_error_is_fence("connection reset by peer"));
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
        let cap = absorb_budget().capacity();
        let per = per_gather_reservation_bytes();
        let reported = effective_gather_concurrency();

        assert!(per <= cap, "a reservation may never exceed capacity");
        assert!(reported >= 1, "at least one gather must always run");
        assert!(
            reported <= absorb_budget().gather_slots(),
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
            (crate::http::MAX_BODY_BYTES + FRAME_ENCODING_ALLOWANCE) * ABSORB_BUILD_MULTIPLIER
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
                assert_eq!(budget.reserved_bytes(), absorb_worst_frame_transient() as u64);
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
                    close: false,
                    producer: None,
                    deferred_error: None,
                    sealed_reject_new: None,
                    touch: None,
                    seal_gen: None,
                    seal_fence_to: None,
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
                close: false,
                producer: None,
                deferred_error: None,
                sealed_reject_new: None,
                touch: None,
                seal_gen: None,
                seal_fence_to: None,
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
