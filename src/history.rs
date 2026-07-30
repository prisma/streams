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

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::{CompressionCodec, Settings, WriteOptions};
use slatedb::{BlockTransformer, Db, DbReader, WriteBatch};
use tokio::sync::mpsc;

use crate::crypto::{
    RouteHash, SegmentHash, StreamKey, decode_frame, decrypt_frame, derive_subkey, hex,
};
use crate::shard::{AbsorbSignal, ShardEngine, now_ms, read_frames_range};

// ---- block transformer: AES-256-GCM with a random nonce per block ----

pub struct AesBlockTransformer {
    cipher: Aes256Gcm,
}

impl AesBlockTransformer {
    pub fn new(key: &StreamKey) -> AesBlockTransformer {
        AesBlockTransformer {
            cipher: Aes256Gcm::new((&key.0).into()),
        }
    }
}

#[async_trait::async_trait]
impl BlockTransformer for AesBlockTransformer {
    async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let mut nonce = [0u8; 12];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut nonce);
        let ct = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &data,
                    aad: b"",
                },
            )
            .map_err(|_| block_err("block encrypt failed"))?;
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(Bytes::from(out))
    }

    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        if data.len() < 12 {
            return Err(block_err("block too short"));
        }
        let (nonce, ct) = data.split_at(12);
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad: b"" })
            .map_err(|_| block_err("block decrypt failed (wrong stream key?)"))?;
        Ok(Bytes::from(pt))
    }
}

/// Test-only absorber pause (D3). Initialized from ABSORB_PAUSE, then
/// flipped at runtime via /v1/debug/absorb-pause so the ladder can pause
/// an instance WITHOUT restarting it (a restart moves its shards away).
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

fn block_err(msg: &str) -> slatedb::Error {
    slatedb::Error::data(msg.to_string())
}

// ---- history record codec ----

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

pub fn hist_record_key(offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"r!");
    k.extend_from_slice(&offset.to_be_bytes());
    k
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

pub fn hist_key_index_key(rk: &str, offset: u64) -> Vec<u8> {
    let rkb = rk.as_bytes();
    let mut k = Vec::with_capacity(12 + rkb.len());
    k.extend_from_slice(b"k!");
    k.extend_from_slice(&(rkb.len() as u16).to_be_bytes());
    k.extend_from_slice(rkb);
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

pub fn encode_hist_record(ts: i64, key_version: u32, rk: &str, payload: &[u8]) -> Vec<u8> {
    let rkb = rk.as_bytes();
    let mut v = Vec::with_capacity(15 + rkb.len() + payload.len());
    v.push(1);
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&key_version.to_le_bytes());
    v.extend_from_slice(&(rkb.len() as u16).to_le_bytes());
    v.extend_from_slice(rkb);
    v.extend_from_slice(payload);
    v
}

pub struct HistRecord {
    pub ts: i64,
    pub key_version: u32,
    pub routing_key: String,
    pub payload: Bytes,
}

pub fn decode_hist_record(v: &Bytes) -> Option<HistRecord> {
    if v.len() < 15 || v[0] != 1 {
        return None;
    }
    let ts = i64::from_le_bytes(v[1..9].try_into().ok()?);
    let key_version = u32::from_le_bytes(v[9..13].try_into().ok()?);
    let rk_len = u16::from_le_bytes(v[13..15].try_into().ok()?) as usize;
    let routing_key = String::from_utf8(v.get(15..15 + rk_len)?.to_vec()).ok()?;
    let payload = v.slice(15 + rk_len..);
    Some(HistRecord {
        ts,
        key_version,
        routing_key,
        payload,
    })
}

// ---- settings (D23 maintenance profile + F2 pattern) ----

/// Shared block cache for ALL history DBs (absorber writes + reads):
/// SlateDB's per-DB default is 512 MB, and the absorber opens a DB per
/// absorbed stream — unbounded aggregate cache on a 1 GB box.
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

fn history_settings() -> Settings {
    // Bench-only escape hatch: HISTORY_COMPACTOR=off disables the embedded
    // compactor (and lifts the L0 caps so flushes never block on it). Used
    // with the s3lite --discard-substr mode, where history SST bodies are
    // dropped and must never be re-read. Production keeps the compactor.
    let compactor_off = std::env::var("HISTORY_COMPACTOR")
        .map(|v| v == "off")
        .unwrap_or(false);
    // Quiet-backoff ceiling for every GC directory sweep: history DBs
    // (per-stream v1 AND the shared v2 partitions) are quiet most of the
    // time, and their fixed-cadence LISTs were 79% of v2's residual
    // request cost (docs/HISTORY-V2.md scorecard). Any sweep that finds
    // work snaps back to the base interval. HISTORY_GC_MAX_INTERVAL_SECS
    // (0 = fixed cadence).
    let gc_max = {
        let secs: u64 = std::env::var("HISTORY_GC_MAX_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);
        (secs > 0).then(|| Duration::from_secs(secs))
    };
    // Busy directories never reach the backoff ceiling, so they'd still
    // pay a LIST per sweep — reuse one listing as the candidate inventory
    // for this long instead (anchors are re-read fresh every sweep; only
    // NEW garbage waits, up to the TTL). HISTORY_GC_LIST_TTL_SECS
    // (0 = list every sweep).
    let gc_ttl = {
        let secs: u64 = std::env::var("HISTORY_GC_LIST_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600);
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
            max_interval: gc_max,
            list_cache_ttl: gc_ttl,
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
            if map.len() >= KEY_CACHE_MAX {
                if let Some(oldest) = map.iter().min_by_key(|(_, e)| e.at).map(|(h, _)| *h) {
                    map.remove(&oldest);
                }
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
    /// Test-only escape hatch: classify every stream as legacy v1 so the
    /// per-stream lanes, HistReaders coverage machinery and k!-index
    /// behavior keep their DST coverage — v1 remains a supported layout
    /// for streams that absorbed before v2 existed.
    pub force_v1: bool,
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
            force_v1: false,
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
}

/// Shared post-pass bookkeeping for both lanes: success retires the
/// pending entry, key-missing backs off slowly (the key arrives with the
/// next keyed request — hammering every tick just burns CPU across a
/// wide key-expired backlog), fence-class drops the claim, and other
/// errors take exponential backoff.
async fn apply_absorb_result(
    absorber: &Absorber,
    pending: &mut HashMap<[u8; 16], PendingAbsorb>,
    hash: [u8; 16],
    res: anyhow::Result<bool>,
    now: Instant,
) {
    match res {
        Ok(absorbed) => {
            if absorbed {
                pending.remove(&hash);
                crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(hash));
            } else if let Some(p) = pending.get_mut(&hash) {
                // Key missing (KeyCache TTL or never supplied): the
                // contract is absorb-on-next-touch — a keyed request
                // re-fills the cache and the sweep/signal re-drives the
                // pass. Until then, retry slowly instead of every tick.
                p.failures = 0;
                p.retry_after = Some(now + absorber.cfg.tick * 64);
            }
        }
        Err(e) => {
            let msg = e.to_string();
            if absorb_error_is_fence(&msg) {
                tracing::warn!(
                    "dropping absorb claim for {} (fence-class): {msg}",
                    hex(&hash)
                );
                pending.remove(&hash);
                absorber.close_db(&hash).await;
            } else if let Some(p) = pending.get_mut(&hash) {
                p.failures = p.failures.saturating_add(1);
                let shift = p.failures.min(6);
                p.retry_after = Some(now + absorber.cfg.tick * 2u32.pow(shift));
                // Log at failure 1, 2, 4, 8, ... only.
                if p.failures.is_power_of_two() {
                    tracing::warn!(
                        failures = p.failures,
                        "absorb failed for {}: {msg}",
                        hex(&hash)
                    );
                }
            }
        }
    }
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
    open_dbs: tokio::sync::Mutex<HashMap<[u8; 16], (Arc<Db>, Instant)>>,
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
const HISTORY_DB_LRU: usize = 16;
const HISTORY_DB_IDLE: Duration = Duration::from_secs(120);

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
            open_dbs: tokio::sync::Mutex::new(HashMap::new()),
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
            let mut tick = tokio::time::interval(absorber.cfg.tick);
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
                    let handles: Vec<[u8; 16]> =
                        absorber.open_dbs.lock().await.keys().copied().collect();
                    for h in handles {
                        absorber.close_db(&h).await;
                    }
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
                            || (seeded && tick_n % RESCAN_EVERY == 0)
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
                        if tick_n % absorber.cfg.sweep_every.max(1) == 0 {
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
                        const BIG_LANE_PER_TICK: usize = 16;
                        const SMALL_LANE_PER_TICK: usize = 256;
                        const V2_LANE_PER_TICK: usize = 1024;
                        const CLASSIFY_PER_TICK: usize = 4096;
                        let mut v2_lane: Vec<[u8; 16]> = Vec::new();
                        let mut small: Vec<([u8; 16], u64)> = Vec::new();
                        let mut big: Vec<([u8; 16], u64)> = Vec::new();
                        for (hash, bytes) in due.into_iter().take(CLASSIFY_PER_TICK) {
                            if v2_lane.len() >= V2_LANE_PER_TICK
                                && small.len() >= SMALL_LANE_PER_TICK
                                && big.len() >= BIG_LANE_PER_TICK
                            {
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
                            let v2_eligible = v2flag || (absorbed == 0 && route != [0u8; 16]);
                            #[cfg(test)]
                            if std::env::var("DST_DRAIN_TRACE").is_ok() {
                                eprintln!(
                                    "CLASSIFY {} absorbed={absorbed} v2flag={v2flag} route_set={} eligible={v2_eligible} bytes={bytes}",
                                    crate::crypto::hex(&hash[..4]),
                                    route != [0u8; 16],
                                );
                            }
                            if !absorber.cfg.force_v1 && v2_eligible {
                                if v2_lane.len() < V2_LANE_PER_TICK {
                                    v2_lane.push(hash);
                                }
                            } else if bytes <= absorber.cfg.small_pass_bytes {
                                if small.len() < SMALL_LANE_PER_TICK {
                                    small.push((hash, bytes));
                                }
                            } else if big.len() < BIG_LANE_PER_TICK {
                                big.push((hash, bytes));
                            }
                        }
                        if !v2_lane.is_empty() && !absorber.shard.is_closed() {
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
                                    for (h, _, _) in &outcome.advanced {
                                        pending.remove(h);
                                        crate::usage::clear_absorb_lag(crate::crypto::SegmentHash(*h));
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
                        for (hash, _) in big.into_iter().take(BIG_LANE_PER_TICK) {
                            if absorber.shard.is_closed() {
                                break; // exit path above runs on next loop
                            }
                            let res = absorber.absorb_one(&hash).await;
                            apply_absorb_result(&absorber, &mut pending, hash, res, now).await;
                        }
                        // Chunked: evict between chunks (never mid-chunk —
                        // eviction would close a Db a concurrent sibling
                        // pass is still writing), so open_dbs stays within
                        // chunk size of the LRU cap.
                        let chunk = (absorber.cfg.concurrency.max(1) * 2).max(2);
                        let mut small_iter =
                            small.into_iter().take(SMALL_LANE_PER_TICK).peekable();
                        while small_iter.peek().is_some() && !absorber.shard.is_closed() {
                            use futures_util::StreamExt;
                            let batch: Vec<_> = small_iter.by_ref().take(chunk).collect();
                            let a = &absorber;
                            let results: Vec<([u8; 16], anyhow::Result<bool>)> =
                                futures_util::stream::iter(batch.into_iter().map(|(hash, _)| {
                                    async move { (hash, a.absorb_one_small(&hash).await) }
                                }))
                                .buffer_unordered(absorber.cfg.concurrency.max(1))
                                .collect()
                                .await;
                            for (hash, res) in results {
                                apply_absorb_result(&absorber, &mut pending, hash, res, now)
                                    .await;
                            }
                            absorber.evict_idle_dbs().await;
                        }
                        absorber.evict_idle_dbs().await;
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
        let mut wb = WriteBatch::new();
        let mut out = GatherOutcome::default();
        let mut batch_bytes: usize = 0;
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
            for (kh, bucket, first, value) in emitted {
                if let Some(pg) = crate::postings::decode_page(&value) {
                    POSTINGS_RUNS_WRITTEN
                        .fetch_add(pg.runs.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                wb.put(
                    crate::postings::postings_key(route, inc, &kh, bucket, first),
                    value,
                );
            }
            POSTINGS_BYTES_WRITTEN.fetch_add(postings_bytes, std::sync::atomic::Ordering::Relaxed);
            CANONICAL_BYTES_WRITTEN.fetch_add(chunk_raw, std::sync::atomic::Ordering::Relaxed);
            out.advanced.push((*hash, last + 1, chunk_raw));
        }
        if out.advanced.is_empty() {
            return Ok(out);
        }
        part.write_with_options(
            wb,
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await?;
        part.flush().await?; // wal off => memtable -> L0, manifest published
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

    /// Serial full-budget pass (the validated hot path). Evicts idle DB
    /// handles inline, as it always has — safe because big-lane passes
    /// never overlap another pass.
    async fn absorb_one(&self, hash: &[u8; 16]) -> anyhow::Result<bool> {
        self.absorb_pass(hash, self.cfg.pass_bytes, true).await
    }

    /// Small-lane pass: bounded budget, and NO inline eviction — several
    /// of these run concurrently, and evicting here could close a Db a
    /// sibling pass is still writing. The tick evicts after the lane.
    async fn absorb_one_small(&self, hash: &[u8; 16]) -> anyhow::Result<bool> {
        self.absorb_pass(hash, self.cfg.small_pass_bytes, false)
            .await
    }

    /// Close a cached history handle (fence-class failure or eviction).
    async fn close_db(&self, hash: &[u8; 16]) {
        // The submit high-water mark travels with the claim: dropping the
        // claim (fence) must also drop the mark, so a hypothetical
        // re-acquire re-paces from published state.
        self.submitted.lock().unwrap().remove(hash);
        let entry = self.open_dbs.lock().await.remove(hash);
        if let Some((db, _)) = entry {
            let _ = db.close().await;
        }
    }

    /// LRU + idle eviction: bound resident history DBs to HISTORY_DB_LRU,
    /// and drop any handle unused for HISTORY_DB_IDLE.
    async fn evict_idle_dbs(&self) {
        let mut victims: Vec<(Arc<Db>, [u8; 16])> = Vec::new();
        {
            let mut cache = self.open_dbs.lock().await;
            let now = Instant::now();
            let idle: Vec<[u8; 16]> = cache
                .iter()
                .filter(|(_, (_, last))| now.duration_since(*last) >= HISTORY_DB_IDLE)
                .map(|(h, _)| *h)
                .collect();
            for h in idle {
                if let Some((db, _)) = cache.remove(&h) {
                    victims.push((db, h));
                }
            }
            while cache.len() > HISTORY_DB_LRU {
                let oldest = cache
                    .iter()
                    .min_by_key(|(_, (_, last))| *last)
                    .map(|(h, _)| *h);
                match oldest {
                    Some(h) => {
                        if let Some((db, _)) = cache.remove(&h) {
                            victims.push((db, h));
                        }
                    }
                    None => break,
                }
            }
        }
        for (db, _) in victims {
            let _ = db.close().await;
        }
    }

    /// Returns Ok(false) if the stream key isn't available.
    async fn absorb_pass(
        &self,
        hash: &[u8; 16],
        budget: u64,
        evict_inline: bool,
    ) -> anyhow::Result<bool> {
        let Some((key, epoch)) = self.keys.get(hash) else {
            return Ok(false);
        };
        let handle = self.shard.stream_handle(*hash).await?;
        let (from, upto) = {
            let st = handle.state.lock().unwrap();
            (st.durable.absorbed, st.durable.next)
        };
        #[cfg(test)]
        if std::env::var("DST_DRAIN_TRACE").is_ok() {
            eprintln!(
                "V1PASS {} from={from} upto={upto}",
                crate::crypto::hex(&hash[..4])
            );
        }
        // Skip what we already submitted: the published `absorbed` lags the
        // committer by durability + dispatch, and re-absorbing that window
        // is duplicate work (see the `submitted` field note).
        // Lane-scoped floor (see the field note): a v2 mark here may
        // describe an advance the layout seal dropped — its range lives
        // only in the shared partition a v1 stream never reads.
        let from = {
            let submitted = self.submitted.lock().unwrap();
            submitted
                .get(hash)
                .and_then(|(u, v2)| (!*v2).then_some(*u))
                .unwrap_or(0)
                .max(from)
        };
        if from >= upto {
            return Ok(true);
        }

        // Read + decrypt the un-absorbed durable range from the shard log.
        // Reads are issued as disjoint offset windows with bounded
        // concurrency (offsets below the durable frontier are dense, so
        // windows partition the log exactly); results are processed in
        // order so the absorbed boundary stays a contiguous prefix. The
        // old serial 8 MB chunk loop capped absorption ~10k rec/s.
        let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();
        let mut items: Vec<(u64, HistRecord)> = Vec::new();
        let mut pass_bytes = 0u64;
        // `items` holds the DECRYPTED pass. With v3 (compressed) frames the
        // raw byte budget alone is a memory landmine: 32 MB of frames can
        // decompress to ~1 GB of plaintext, which OOM-killed 1 GB instances
        // before the first history write (sinmax run 9, 2026-07-23). Bound
        // the pass by plaintext bytes with the same budget.
        let mut pt_bytes = 0u64;
        const WINDOW: u64 = 32_768;
        let mut window_reads = {
            use futures_util::StreamExt;
            let shard = self.shard.clone();
            let handle = handle.clone();
            futures_util::stream::iter((from..upto).step_by(WINDOW as usize).map(move |s| {
                let shard = shard.clone();
                let handle = handle.clone();
                let e = (s + WINDOW).min(upto);
                async move {
                    read_frames_range(&shard, &handle, s, e, 64 * 1024 * 1024)
                        .await
                        .map(|r| (e, r))
                }
            }))
            .buffered(4)
        };
        use futures_util::StreamExt;
        while let Some(res) = window_reads.next().await {
            let (win_end, chunk) = res?;
            if chunk.frames.is_empty() {
                break;
            }
            pass_bytes += chunk.frames.iter().map(|f| f.len() as u64).sum::<u64>();
            let last_offset = chunk.last_offset;
            // Decode+decrypt+decompress is CPU-bound (v3 frames expand up
            // to ~30x). Run 12 measured the cost of doing it on the async
            // runtime: tokio timer p99 848 ms vs 3.6 ms on a raw thread —
            // the whole ack path starved behind these loops. The blocking
            // pool gets OS preemption instead of cooperative starvation.
            let key_b = key.clone();
            let epoch_b = epoch;
            let hash_b = *hash;
            let frames = chunk.frames;
            let subkeys_in = std::mem::take(&mut subkeys);
            type SubkeyMap = HashMap<(String, u32), [u8; 32]>;
            let joined = tokio::task::spawn_blocking(
                move || -> Result<(Vec<(u64, HistRecord)>, SubkeyMap), String> {
                    let mut subkeys = subkeys_in;
                    let mut out = Vec::with_capacity(frames.len());
                    for raw in &frames {
                        let frame = decode_frame(raw)
                            .ok_or_else(|| "undecodable frame during absorb".to_string())?;
                        let sk = *subkeys
                            .entry((frame.header.routing_key.clone(), frame.header.key_version))
                            .or_insert_with(|| {
                                derive_subkey(
                                    &key_b,
                                    &epoch_b,
                                    &frame.header.routing_key,
                                    frame.header.key_version,
                                )
                            });
                        let pt = decrypt_frame(&sk, &hash_b, &frame, raw)
                            .map_err(|e| format!("absorb decrypt: {e}"))?;
                        out.push((
                            frame.header.offset,
                            HistRecord {
                                ts: frame.header.ts_ms,
                                key_version: frame.header.key_version,
                                routing_key: frame.header.routing_key,
                                payload: Bytes::from(pt),
                            },
                        ));
                    }
                    Ok((out, subkeys))
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("absorb decode join: {e}"))?;
            let (decoded, subkeys_back) = joined.map_err(|e| anyhow::anyhow!(e))?;
            subkeys = subkeys_back;
            for (offset, rec) in decoded {
                pt_bytes += rec.payload.len() as u64;
                items.push((offset, rec));
            }
            // A byte-truncated window breaks offset contiguity past its
            // last frame: stop here; the boundary advances to what we have.
            let complete = last_offset.map(|l| l + 1 >= win_end).unwrap_or(false);
            if !complete || pass_bytes >= budget || pt_bytes >= budget {
                break;
            }
        }
        drop(window_reads);
        if items.is_empty() {
            return Ok(true);
        }
        let absorbed_upto = items.last().map(|(o, _)| o + 1).unwrap_or(upto);

        // Bulk write through a cached handle (open once, reuse across
        // passes), explicit flush per pass so the boundary only advances
        // over durable data. See open_dbs field note for why not
        // open/close per pass. The OPEN happens outside the cache lock:
        // it is 1-2 s of manifest round-trips, and the concurrent small
        // lane exists precisely to overlap that latency across streams —
        // holding the map lock through it would re-serialize the lane.
        // Two passes for the same hash never overlap (one pending entry,
        // due at most once per tick), so a duplicate racing open is not
        // possible; different hashes racing is the point.
        let cached = {
            let mut cache = self.open_dbs.lock().await;
            if let Some((db, last_used)) = cache.get_mut(hash) {
                *last_used = Instant::now();
                Some(db.clone())
            } else {
                None
            }
        };
        let db = match cached {
            Some(db) => db,
            None => {
                let path = history_db_path(hash);
                let store = self.data_store.clone();
                let k = key.clone();
                let db = Arc::new(
                    crate::on_slatedb_rt(async move {
                        Db::builder(path.as_str(), store)
                            .with_settings(history_settings())
                            .with_db_cache(history_cache())
                            .with_block_transformer(Arc::new(AesBlockTransformer::new(&k)))
                            .build()
                            .await
                    })
                    .await?,
                );
                self.open_dbs
                    .lock()
                    .await
                    .insert(*hash, (db.clone(), Instant::now()));
                db
            }
        };
        let mut i = 0;
        while i < items.len() {
            let mut wb = WriteBatch::new();
            let end = (i + self.cfg.batch_puts / 2).min(items.len());
            for (offset, rec) in &items[i..end] {
                let value =
                    encode_hist_record(rec.ts, rec.key_version, &rec.routing_key, &rec.payload);
                // Unkeyed records get no k! index copy. The index is a
                // full payload duplicate, so for the common unkeyed
                // workload it doubled history bytes — twice the SSTs,
                // compaction traffic and cache pressure for an index that
                // only an empty-key filter could use (object-store cost
                // review, item 6). Empty-key filtered reads are served
                // from the primary r! range instead (read_history).
                if rec.routing_key.is_empty() {
                    wb.put(hist_record_key(*offset), value);
                } else {
                    wb.put(hist_record_key(*offset), value.clone());
                    wb.put(hist_key_index_key(&rec.routing_key, *offset), value);
                }
            }
            db.write_with_options(
                wb,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await?;
            i = end;
        }
        db.flush().await?; // wal off => memtable -> L0 (durable)
        if evict_inline {
            self.evict_idle_dbs().await;
        }

        // Advance the readers' boundary (+ budgeted trim) in the shard
        // log. `pass_bytes` is the raw frame bytes of exactly the items
        // absorbed — the committer's unabsorbed_bytes decrement.
        self.shard
            .submit_absorbed(*hash, absorbed_upto, pass_bytes)
            .await;
        {
            let mut submitted = self.submitted.lock().unwrap();
            let e = submitted.entry(*hash).or_insert((0, false));
            if e.1 {
                *e = (absorbed_upto, false);
            } else {
                e.0 = e.0.max(absorbed_upto);
            }
        }
        tracing::info!(
            "absorbed {} records into {} (upto {})",
            items.len(),
            history_db_path(hash),
            absorbed_upto
        );
        Ok(true)
    }
}

// ---- history reads ----

pub struct HistoryReadResult {
    pub records: Vec<(u64, HistRecord)>,
    pub last_offset: Option<u64>,
    /// True when the requested range was fully scanned (not byte-truncated):
    /// the caller may treat everything below `upto` as consumed.
    pub completed: bool,
}

/// Read [from, upto) from a stream's history DB (plaintext records).
/// `key_filter` uses the k! index (contiguous per routing key).

// ---- history DbReader cache -----------------------------------------
//
// Every history read used to build a fresh `DbReader`: several manifest
// GETs, a checkpoint WRITE against the history manifest (readers without
// an explicit checkpoint maintain their own), and the WAL replay probe —
// per request, on the user-visible read path. Those are exactly the
// small-metadata operations Tigris sometimes serves from a remote region
// (docs/SOAK-REGIONS.md, "metadata trickle"), so each cold read carried a
// small chance of a transcontinental round trip.
//
// The cache keeps one polling reader per (stream, key). Correctness at
// the absorbed boundary does NOT rely on the reader's poll: before
// serving a read that requires offsets up to `upto`, the cache proves
// coverage — `seen_upto >= upto`, or a one-row probe of
// `hist_record_key(upto - 1)` through the reader — and reopens a fresh
// reader when the probe misses. A fresh reader must see the range: the
// absorber flushes the history db (memtable → L0, manifest published)
// BEFORE the absorbed boundary advances, so any reader opened after the
// boundary moved observes a manifest that covers it. The probe works for
// key-filtered reads too, where offset-contiguity checks cannot.
//
// Readers are read-only: no fencing concerns. Entries are LRU-capped and
// idle-evicted like the absorber's writer handles.

/// Per-instance history-reader service. One per object store (production:
/// one, owned by AppState; DST: one per simulated node/store), which is
/// what keys readers correctly — the old process-global cache keyed only
/// (stream, key-disc), so two stores with the same stream hash in one
/// process would have shared a reader, and simulated nodes could not have
/// independent open/eviction behavior.
///
/// Concurrency contract (the 2026-07-27 review's P0): **all probe and
/// open work is per-key single-flight and cache-owned.** A caller never
/// performs the probe/open in its own future; it subscribes to the slot's
/// watch and a spawned worker does the work, inserts the result into the
/// map, and only then notifies. Consequences, each load-bearing:
///
///   - 64 concurrent cold readers = 1 open, 63 coalesced waiters
///     (was: 64 opens — the metadata storm the cache exists to prevent,
///     recreated at request concurrency);
///   - caller cancellation cannot detach the open: the worker is not the
///     caller's future, so the reader still lands in the cache with zero
///     surviving callers (the read-only cousin of the shard
///     detached-reopen storm, closed the same way as OpenGate);
///   - a transient probe ERROR is an error, not staleness: the healthy
///     reader stays cached and the caller sees the error — one flaky GET
///     must not amplify into eviction + checkpoint cleanup + fresh
///     manifest/checkpoint traffic (was: `Ok(Some)` vs everything-else);
///   - replaced and evicted readers are closed explicitly on the SlateDB
///     runtime, with closes counted, so checkpoint cleanup neither races
///     the request runtime nor silently leaks.
pub struct HistReaders {
    store: Arc<dyn ObjectStore>,
    map: std::sync::Mutex<HashMap<([u8; 16], u64), Slot>>,
    cap: usize,
    idle: Duration,
    /// Reader manifest-poll interval, ms. Per-instance so DST can pin it
    /// to an hour and make staleness deterministic without a process-wide
    /// static (which forced test serialization).
    poll_ms: u64,
    pub metrics: HistReaderMetrics,
}

#[derive(Default)]
pub struct HistReaderMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub probes: AtomicU64,
    pub probe_errors: AtomicU64,
    pub stale_reopens: AtomicU64,
    pub opens_started: AtomicU64,
    pub opens_completed: AtomicU64,
    pub opens_failed: AtomicU64,
    pub coalesced: AtomicU64,
    pub evictions: AtomicU64,
    pub closes_started: AtomicU64,
    pub closes_completed: AtomicU64,
    pub close_failures: AtomicU64,
}

use std::sync::atomic::AtomicU64;

type SlotResult = Result<(Arc<DbReader>, bool), String>;

enum Slot {
    Ready(CachedReader),
    /// A cache-owned worker is probing/refreshing/opening toward
    /// `target_upto`. Callers subscribe; the worker publishes exactly
    /// once, AFTER updating the map.
    Pending {
        rx: tokio::sync::watch::Receiver<Option<SlotResult>>,
        target_upto: u64,
    },
}

struct CachedReader {
    reader: Arc<DbReader>,
    last_used: Instant,
    /// Offsets `[0, seen_upto)` are proven visible through this reader.
    seen_upto: u64,
}

/// Non-cryptographic key discriminator: two requests for the same stream
/// with different keys must not share a reader (its block transformer is
/// key-bound).
fn key_disc(key: &StreamKey) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.0.iter() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_1b3);
    }
    h
}

impl HistReaders {
    pub fn new(store: Arc<dyn ObjectStore>, cap: usize, idle: Duration, poll_ms: u64) -> Arc<Self> {
        Arc::new(HistReaders {
            store,
            map: std::sync::Mutex::new(HashMap::new()),
            cap: cap.max(1),
            idle,
            poll_ms,
            metrics: HistReaderMetrics::default(),
        })
    }

    /// Production defaults; cap/idle env-tunable in main.
    pub fn with_defaults(store: Arc<dyn ObjectStore>) -> Arc<Self> {
        HistReaders::new(store, 8, Duration::from_secs(120), 5_000)
    }

    pub fn stats_json(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Relaxed;
        let m = &self.metrics;
        serde_json::json!({
            "hits": m.hits.load(Relaxed),
            "misses": m.misses.load(Relaxed),
            "probes": m.probes.load(Relaxed),
            "probe_errors": m.probe_errors.load(Relaxed),
            "stale_reopens": m.stale_reopens.load(Relaxed),
            "opens_started": m.opens_started.load(Relaxed),
            "opens_completed": m.opens_completed.load(Relaxed),
            "opens_failed": m.opens_failed.load(Relaxed),
            "coalesced": m.coalesced.load(Relaxed),
            "evictions": m.evictions.load(Relaxed),
            "closes_started": m.closes_started.load(Relaxed),
            "closes_completed": m.closes_completed.load(Relaxed),
            "close_failures": m.close_failures.load(Relaxed),
            "live_readers": self.map.lock().unwrap().len(),
        })
    }

    /// A reader proven to cover `[0, upto)`. Returns `(reader, covered)`;
    /// `covered = false` means even a freshly opened reader cannot see
    /// `upto - 1` yet, and the caller must report an honest partial read
    /// rather than skipping ahead.
    pub async fn acquire(
        self: &Arc<Self>,
        hash: &[u8; 16],
        key: &StreamKey,
        upto: u64,
    ) -> anyhow::Result<(Arc<DbReader>, bool)> {
        use std::sync::atomic::Ordering::Relaxed;
        let ck = (*hash, key_disc(key));
        // Bounded re-checks: each pass either returns, subscribes to a
        // worker, or starts one. Two workers back-to-back (a refresh
        // toward a lower upto finishing just as we need a higher one) is
        // the realistic worst case; five is unreachable without a bug.
        for _ in 0..5 {
            let mut wait_rx = {
                let mut map = self.map.lock().unwrap();
                match map.get_mut(&ck) {
                    Some(Slot::Ready(e)) if e.seen_upto >= upto => {
                        e.last_used = Instant::now();
                        self.metrics.hits.fetch_add(1, Relaxed);
                        return Ok((e.reader.clone(), true));
                    }
                    Some(Slot::Ready(e)) => {
                        // Cached but not proven for this boundary: hand
                        // the probe (and possible reopen) to a worker.
                        let probe = e.reader.clone();
                        let (tx, rx) = tokio::sync::watch::channel(None);
                        map.insert(
                            ck,
                            Slot::Pending {
                                rx: rx.clone(),
                                target_upto: upto,
                            },
                        );
                        self.spawn_worker(ck, *hash, key.clone(), upto, Some(probe), tx);
                        rx
                    }
                    Some(Slot::Pending { rx, target_upto }) => {
                        // Someone is already working. If their target
                        // covers ours, their result is ours; if not, we
                        // still wait (upto only moves at absorb cadence —
                        // re-check on the next pass).
                        let _ = target_upto;
                        self.metrics.coalesced.fetch_add(1, Relaxed);
                        rx.clone()
                    }
                    None => {
                        self.metrics.misses.fetch_add(1, Relaxed);
                        let (tx, rx) = tokio::sync::watch::channel(None);
                        map.insert(
                            ck,
                            Slot::Pending {
                                rx: rx.clone(),
                                target_upto: upto,
                            },
                        );
                        self.spawn_worker(ck, *hash, key.clone(), upto, None, tx);
                        rx
                    }
                }
            };
            // Await the worker's single publication. If the worker (or
            // its channel) vanished, loop and re-evaluate.
            if wait_rx.borrow().is_none() && wait_rx.changed().await.is_err() {
                continue;
            }
            let published = wait_rx.borrow().clone();
            match published {
                Some(Ok((reader, covered_target))) => {
                    // Covering [0, target) covers [0, upto) whenever
                    // target >= upto; the worker recorded seen_upto, so
                    // when in doubt one more pass re-checks the map.
                    let proven = {
                        let map = self.map.lock().unwrap();
                        matches!(map.get(&ck), Some(Slot::Ready(e)) if e.seen_upto >= upto)
                    };
                    if proven {
                        return Ok((reader, true));
                    }
                    if covered_target {
                        continue; // their target < ours: go probe for ours
                    }
                    return Ok((reader, false));
                }
                Some(Err(e)) => return Err(anyhow::anyhow!(e)),
                None => continue,
            }
        }
        anyhow::bail!("history reader acquire: no stable slot after 5 passes")
    }

    /// The cache-owned worker: probes the candidate (if any), reopens on
    /// proven staleness, publishes into the map, THEN notifies. Runs as
    /// its own task so caller cancellation cannot detach it.
    fn spawn_worker(
        self: &Arc<Self>,
        ck: ([u8; 16], u64),
        hash: [u8; 16],
        key: StreamKey,
        upto: u64,
        probe_candidate: Option<Arc<DbReader>>,
        tx: tokio::sync::watch::Sender<Option<SlotResult>>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let svc = self.clone();
        tokio::spawn(async move {
            let probe_key = hist_record_key(upto.saturating_sub(1));

            if let Some(reader) = probe_candidate {
                svc.metrics.probes.fetch_add(1, Relaxed);
                match reader.get(probe_key.clone()).await {
                    Ok(Some(_)) => {
                        let mut map = svc.map.lock().unwrap();
                        map.insert(
                            ck,
                            Slot::Ready(CachedReader {
                                reader: reader.clone(),
                                last_used: Instant::now(),
                                seen_upto: upto,
                            }),
                        );
                        drop(map);
                        svc.metrics.hits.fetch_add(1, Relaxed);
                        let _ = tx.send(Some(Ok((reader, true))));
                        return;
                    }
                    Ok(None) => {
                        // Genuinely stale: its manifest view predates the
                        // absorbed boundary. Replace it.
                        svc.metrics.stale_reopens.fetch_add(1, Relaxed);
                        svc.close_reader(reader);
                    }
                    Err(e) => {
                        // Transient storage trouble is NOT staleness: keep
                        // the healthy reader cached (unproven for this
                        // boundary — a later probe will retry) and hand
                        // the caller the error.
                        svc.metrics.probe_errors.fetch_add(1, Relaxed);
                        let mut map = svc.map.lock().unwrap();
                        map.insert(
                            ck,
                            Slot::Ready(CachedReader {
                                reader,
                                last_used: Instant::now(),
                                seen_upto: 0,
                            }),
                        );
                        drop(map);
                        let _ = tx.send(Some(Err(format!("history probe: {e}"))));
                        return;
                    }
                }
            }

            // Fresh open (cold miss, or stale replacement).
            svc.metrics.opens_started.fetch_add(1, Relaxed);
            let opened = open_history_reader(&svc.store, &hash, &key, svc.poll_ms).await;
            match opened {
                Ok(reader) => {
                    svc.metrics.opens_completed.fetch_add(1, Relaxed);
                    // A fresh reader must see the boundary (the absorber
                    // publishes history before advancing it). A probe
                    // ERROR here is conservative non-coverage: the read
                    // reports completed=false and the caller re-polls.
                    let covered = upto == 0 || matches!(reader.get(probe_key).await, Ok(Some(_)));
                    let mut evicted: Vec<Arc<DbReader>> = Vec::new();
                    {
                        let mut map = svc.map.lock().unwrap();
                        map.insert(
                            ck,
                            Slot::Ready(CachedReader {
                                reader: reader.clone(),
                                last_used: Instant::now(),
                                seen_upto: if covered { upto } else { 0 },
                            }),
                        );
                        // LRU + idle eviction. Pending slots are never
                        // victims: evicting a slot someone is awaiting
                        // would strand its waiters.
                        let now = Instant::now();
                        let mut victims: Vec<([u8; 16], u64)> = map
                            .iter()
                            .filter(|(k2, s)| {
                                **k2 != ck
                                    && matches!(s, Slot::Ready(e) if now - e.last_used > svc.idle)
                            })
                            .map(|(k2, _)| *k2)
                            .collect();
                        loop {
                            let ready_left = map
                                .iter()
                                .filter(|(k2, s)| {
                                    matches!(s, Slot::Ready(_)) && !victims.contains(k2)
                                })
                                .count();
                            if ready_left <= svc.cap {
                                break;
                            }
                            match map
                                .iter()
                                .filter(|(k2, s)| {
                                    **k2 != ck
                                        && matches!(s, Slot::Ready(_))
                                        && !victims.contains(k2)
                                })
                                .min_by_key(|(_, s)| match s {
                                    Slot::Ready(e) => e.last_used,
                                    _ => now,
                                })
                                .map(|(k2, _)| *k2)
                            {
                                Some(oldest) => victims.push(oldest),
                                None => break,
                            }
                        }
                        for v in victims {
                            if let Some(Slot::Ready(e)) = map.remove(&v) {
                                svc.metrics.evictions.fetch_add(1, Relaxed);
                                evicted.push(e.reader);
                            }
                        }
                    }
                    for r in evicted {
                        svc.close_reader(r);
                    }
                    let _ = tx.send(Some(Ok((reader, covered))));
                }
                Err(e) => {
                    svc.metrics.opens_failed.fetch_add(1, Relaxed);
                    let mut map = svc.map.lock().unwrap();
                    // Leave no Pending tombstone: the next caller retries.
                    if matches!(map.get(&ck), Some(Slot::Pending { .. })) {
                        map.remove(&ck);
                    }
                    drop(map);
                    let _ = tx.send(Some(Err(format!("history reader open: {e}"))));
                }
            }
        });
    }

    /// Close on the SlateDB runtime (checkpoint cleanup does store I/O and
    /// must not take quanta on the request runtime), counted so eviction
    /// lifecycle is provable.
    fn close_reader(self: &Arc<Self>, reader: Arc<DbReader>) {
        use std::sync::atomic::Ordering::Relaxed;
        self.metrics.closes_started.fetch_add(1, Relaxed);
        let svc = self.clone();
        tokio::spawn(async move {
            let r = crate::on_slatedb_rt(async move {
                match Arc::try_unwrap(reader) {
                    Ok(r) => r.close().await.map_err(|e| e.to_string()),
                    // Another holder still reads through it; its Drop
                    // handles cleanup when they finish.
                    Err(_still_shared) => Ok(()),
                }
            })
            .await;
            match r {
                Ok(()) => svc.metrics.closes_completed.fetch_add(1, Relaxed),
                Err(_) => svc.metrics.close_failures.fetch_add(1, Relaxed),
            };
        });
    }

    #[cfg(test)]
    pub fn len_for_tests(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

async fn open_history_reader(
    data_store: &Arc<dyn ObjectStore>,
    hash: &[u8; 16],
    key: &StreamKey,
    poll_ms: u64,
) -> anyhow::Result<Arc<DbReader>> {
    let path = history_db_path(hash);
    let store = data_store.clone();
    let k = key.clone();
    let reader = crate::on_slatedb_rt(async move {
        DbReader::builder(path.as_str(), store)
            .with_options(slatedb::config::DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(poll_ms),
                // must exceed 2x poll; also bounds how long an evicted
                // reader's checkpoint can pin history objects
                checkpoint_lifetime: Duration::from_millis((poll_ms * 4).max(60_000)),
                // The absorber flushes (memtable -> L0) before the
                // absorbed boundary advances, so everything a reader is
                // allowed to need is already in L0 — skipping WAL replay
                // removes the reader's WAL reads entirely.
                skip_wal_replay: true,
                ..Default::default()
            })
            .with_block_transformer(Arc::new(AesBlockTransformer::new(&k)))
            .build()
            .await
    })
    .await?;
    Ok(Arc::new(reader))
}

pub async fn read_history(
    readers: &Arc<HistReaders>,
    hash: &[u8; 16],
    key: &StreamKey,
    from: u64,
    upto: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<HistoryReadResult> {
    // Coverage-proven reader via the single-flight service. `covered =
    // false` means even a fresh reader cannot see `upto - 1` yet; the read
    // still runs, but reports completed = false so the caller re-polls
    // instead of skipping the missing tail (which would be a silent gap).
    let (reader, covered) = readers.acquire(hash, key, upto).await?;
    let mut out = HistoryReadResult {
        records: Vec::new(),
        last_offset: None,
        completed: covered,
    };
    let mut total = 0usize;
    match key_filter {
        None => {
            let mut iter = reader
                .scan_with_options(
                    hist_record_key(from)..hist_record_key(upto),
                    &hist_scan_opts(),
                )
                .await?;
            while let Some(kv) = iter.next().await? {
                let off = u64::from_be_bytes(kv.key[2..10].try_into().expect("hist key"));
                if let Some(rec) = decode_hist_record(&kv.value) {
                    total += rec.payload.len();
                    out.records.push((off, rec));
                    out.last_offset = Some(off);
                    if total >= max_bytes {
                        out.completed = false;
                        break;
                    }
                }
            }
        }
        // Empty-key filter: unkeyed records carry no k! index copy (the
        // absorber stopped writing the payload duplicate), so serve from
        // the primary r! range and filter. For an all-unkeyed stream this
        // scans exactly the bytes the index copy would have held; only a
        // mixed stream filtered by "" pays to skip its keyed records.
        Some("") => {
            let mut iter = reader
                .scan_with_options(
                    hist_record_key(from)..hist_record_key(upto),
                    &hist_scan_opts(),
                )
                .await?;
            while let Some(kv) = iter.next().await? {
                let off = u64::from_be_bytes(kv.key[2..10].try_into().expect("hist key"));
                if let Some(rec) = decode_hist_record(&kv.value) {
                    if !rec.routing_key.is_empty() {
                        continue;
                    }
                    total += rec.payload.len();
                    out.records.push((off, rec));
                    out.last_offset = Some(off);
                    if total >= max_bytes {
                        out.completed = false;
                        break;
                    }
                }
            }
        }
        Some(rk) => {
            let range = hist_key_index_key(rk, from)..hist_key_index_key(rk, upto);
            let mut iter = reader.scan_with_options(range, &hist_scan_opts()).await?;
            while let Some(kv) = iter.next().await? {
                let klen = kv.key.len();
                let off = u64::from_be_bytes(kv.key[klen - 8..].try_into().expect("k! key"));
                if let Some(rec) = decode_hist_record(&kv.value) {
                    total += rec.payload.len();
                    out.records.push((off, rec));
                    out.last_offset = Some(off);
                    if total >= max_bytes {
                        out.completed = false;
                        break;
                    }
                }
            }
        }
    }
    // The reader is cached and stays open; the service owns its lifecycle.
    Ok(out)
}

/// Read [from, upto) of a v2-layout stream from the shard's SHARED
/// partition, through the owner's open writer Db — no DbReader, no
/// checkpoint, no coverage probe (the boundary only advances after this
/// very Db's flush, so the writer's own view always covers it). Returns
/// RAW encrypted frames; the caller decodes them with the same
/// machinery as shard-tail frames. `key_filter`: `Some("")` scans the
/// record range and filters by the plaintext frame header (unkeyed
/// records carry no index entry); a non-empty filter scans the index.
/// Postings/read-planner telemetry (ROUTING-V3 §3/§5 gates): postings
/// vs canonical write bytes, spans per keyed response, scanned vs
/// matched amplification, and how often the pre-postings fallback ran.
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
                let mut iter = part.scan_with_options(range, &hist_scan_opts()).await?;
                let mut hits: Vec<(u64, Bytes)> = Vec::new();
                while let Some(kv) = iter.next().await? {
                    READ_FRAMES_SCANNED.fetch_add(1, Relaxed);
                    let Some(f) = crate::crypto::decode_frame(&kv.value) else {
                        anyhow::bail!("undecodable v2 history frame");
                    };
                    if f.header.routing_key != rk {
                        continue;
                    }
                    READ_FRAMES_MATCHED.fetch_add(1, Relaxed);
                    hits.push((f.header.offset, kv.value));
                }
                anyhow::Ok((span, hits))
            }
        }))
        .buffered(4);
        'spans: while let Some(res) = results.next().await {
            let (span, hits) = res?;
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

pub fn ts_now() -> i64 {
    now_ms()
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
                    touch: None,
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
                touch: None,
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
