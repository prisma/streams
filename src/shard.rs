//! Shard log engine: one SlateDB per shard, hash-first keyspace, committer +
//! durable-watermark acker (§3.4). Record values ARE the wire frames (§3.7):
//! encryption happens in the committer, after offset assignment, because the
//! nonce is the offset.
//!
//! Keyspace (hash-first so a hash range is one contiguous split range):
//!   <hash16> 't'                 tail state
//!   <hash16> 'r' <offset u64 BE> record frame

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::config::{DurabilityLevel, ScanOptions, WriteOptions};
use slatedb::{Db, WriteBatch};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::crypto::{FrameHeader, decode_frame, encrypt_frame};

pub fn tail_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(hash);
    k.push(b't');
    k
}

pub fn record_key(hash: &[u8; 16], offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(25);
    k.extend_from_slice(hash);
    k.push(b'r');
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

/// Tail value v3:
/// [ver u8=3][next u64][last_ts i64][logical u64][absorbed u64][trimmed u64][flags u8][seq_len u16][seq][route16?]
///
/// `flags` is a bitmask: bit0 = closed, bit1 = history v2 (the stream's
/// absorbed range lives in the shared per-shard partition, not a
/// per-stream DB). The optional trailing route16 (the shard-routing
/// hash) is a backward-compatible extension: v3 decoders read exactly
/// `seq_len` seq bytes and ignore trailing bytes. Downgrade caveat: a
/// pre-bitmask binary reads flags with `== 1`, so it would see a
/// closed+v2 stream (flags=3) as open — acceptable for forward-only
/// deployments, noted here because it is not zero.
fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let mut v = Vec::with_capacity(60 + seq.len());
    v.push(3);
    v.extend_from_slice(&t.next.to_le_bytes());
    v.extend_from_slice(&t.ts.to_le_bytes());
    v.extend_from_slice(&t.logical.to_le_bytes());
    v.extend_from_slice(&t.absorbed.to_le_bytes());
    v.extend_from_slice(&t.trimmed.to_le_bytes());
    let mut flags = 0u8;
    if t.closed {
        flags |= 1;
    }
    if t.history_v2 {
        flags |= 2;
    }
    v.push(flags);
    v.extend_from_slice(&(seq.len() as u16).to_le_bytes());
    v.extend_from_slice(seq);
    v.extend_from_slice(&t.route);
    v
}

fn decode_tail(v: &[u8]) -> Option<TailFields> {
    if v.len() < 44 || (v[0] != 2 && v[0] != 3) {
        return None;
    }
    let v3 = v[0] == 3;
    let next = u64::from_le_bytes(v[1..9].try_into().ok()?);
    let ts = i64::from_le_bytes(v[9..17].try_into().ok()?);
    let logical = u64::from_le_bytes(v[17..25].try_into().ok()?);
    let absorbed = u64::from_le_bytes(v[25..33].try_into().ok()?);
    let trimmed = u64::from_le_bytes(v[33..41].try_into().ok()?);
    let (flags, seq_at) = if v3 { (v[41], 42usize) } else { (0u8, 41usize) };
    let seq_len = u16::from_le_bytes(v[seq_at..seq_at + 2].try_into().ok()?) as usize;
    let seq = if seq_len == 0 {
        None
    } else {
        Some(String::from_utf8(v.get(seq_at + 2..seq_at + 2 + seq_len)?.to_vec()).ok()?)
    };
    let route_at = seq_at + 2 + seq_len;
    let route: [u8; 16] = v
        .get(route_at..route_at + 16)
        .and_then(|r| r.try_into().ok())
        .unwrap_or([0u8; 16]);
    Some(TailFields {
        next,
        ts,
        logical,
        absorbed,
        trimmed,
        seq,
        closed: flags & 1 != 0,
        history_v2: flags & 2 != 0,
        route,
    })
}

pub fn producer_key(hash: &[u8; 16], producer_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + producer_id.len());
    k.extend_from_slice(hash);
    k.push(b'q');
    k.extend_from_slice(producer_id.as_bytes());
    k
}

/// Durable dirty-stream index (static audit P1): a marker per stream
/// with `absorbed < next`, written in the SAME committer batch as the
/// tail it describes and deleted in the batch whose absorbed boundary
/// catches up. A fresh owner scans this prefix once at absorber start,
/// so unabsorbed tails are rediscovered after restart/handoff without
/// the customer ever touching the stream again. Lives under a reserved
/// sentinel "hash" of all-0xFF (unreachable for SHA-derived stream
/// hashes) with its own tag byte, so it can be range-scanned without
/// colliding with `<hash16><tag>` stream keys, and sorts at the end of
/// the keyspace.
const DIRTY_SENTINEL: [u8; 16] = [0xFF; 16];

pub fn dirty_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(&DIRTY_SENTINEL);
    k.push(b'D');
    k.extend_from_slice(hash);
    k
}

fn dirty_value(absorbed: u64, next: u64) -> [u8; 8 + 8] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&absorbed.to_le_bytes());
    v[8..].copy_from_slice(&next.to_le_bytes());
    v
}

#[derive(Clone, Debug, Default)]
pub struct TailFields {
    pub next: u64,
    pub ts: i64,
    pub logical: u64,
    pub absorbed: u64,
    pub trimmed: u64,
    pub seq: Option<String>,
    pub closed: bool,
    /// This stream's absorbed range lives in the shared per-shard
    /// history partition (v2). Set by the first AbsorbedBatch that
    /// covers the stream; absorbed > 0 with this bit UNSET means legacy
    /// per-stream history (v1) and the stream stays v1.
    pub history_v2: bool,
    /// Shard-routing hash (stream_hash(name)); zeros for streams last
    /// written by callers without a name identity or by older binaries.
    pub route: [u8; 16],
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub struct StreamState {
    pub durable: TailFields,
    pub applied: TailFields,
    /// Producer idempotence state: id -> (epoch, highest seq). Loaded from
    /// the durable `q` keys on first use, applied by the committer.
    /// producer id -> (epoch, seq, last_offset of that seq's commit).
    /// The offset makes a duplicate ack return the ORIGINAL committed
    /// offset instead of whatever the tail happens to be when the retry
    /// arrives — with interleaved appends those differ, and clients use
    /// the ack offset for read-your-write.
    pub producers: HashMap<String, (u64, u64, u64)>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
}

pub struct StreamHandle {
    pub hash: [u8; 16],
    pub state: Mutex<StreamState>,
    pub notify: Notify,
    /// Durable-tail ring: recently-durable frames, published by
    /// dispatch_durable BEFORE acks go out, so a reader woken by an ack
    /// (or by tail notify) finds the record here without a DB scan.
    /// Empty unless ShardConfig.tail_ring_bytes > 0.
    pub ring: Mutex<TailRing>,
}

/// One durably-committed group's frames for one stream: a contiguous
/// offset range [first, next) in publish order.
pub struct RingBatch {
    pub first: u64,
    pub next: u64,
    pub frames: Vec<(u64, Bytes)>,
    pub bytes: usize,
}

#[derive(Default)]
pub struct TailRing {
    /// Contiguous in coverage: back.next of batch k == front.first of
    /// batch k+1 for consecutive batches (all publishes come through the
    /// same committer in offset order; eviction only pops the front).
    pub batches: std::collections::VecDeque<RingBatch>,
    pub bytes: usize,
}

impl TailRing {
    fn floor(&self) -> Option<u64> {
        self.batches.front().map(|b| b.first)
    }
    fn ceil(&self) -> Option<u64> {
        self.batches.back().map(|b| b.next)
    }
}

/// state-protocol feed: key IDs derived at append time, delivered to the
/// stream's touch journal only after the batch is durable (H2 hook).
/// `next_offset` is filled in by the committer once offsets are assigned so
/// wait responses can carry the covered stream offset (delta reads).
pub struct TouchFeed {
    pub journal: Arc<crate::touch::TouchJournal>,
    pub key_ids: Vec<u32>,
    pub next_offset: u64,
}

#[derive(Debug, Clone)]
pub struct ProducerReq {
    pub id: String,
    pub epoch: u64,
    pub seq: u64,
}

/// Validation failures that must be deferred until after the producer
/// duplicate check (a retry must return 204 even if e.g. the content type
/// no longer matches).
#[derive(Debug, Clone)]
pub enum DeferredErr {
    CtMismatch,
    BadBody(String),
}

pub struct AppendReq {
    pub hash: [u8; 16],
    /// Shard-routing identity (`stream_hash(name)`), persisted into the
    /// tail so history v2 can key the shared partition route-first and a
    /// future shard split can clone by range. Zeros when the caller has
    /// no name-level identity (some DST paths); the v2 keyspace accepts
    /// a zero route, it just can't range-split those entries.
    pub route: [u8; 16],
    pub enqueued_at: std::time::Instant,
    /// Plaintext entries; encrypted in the committer with nonce = offset.
    pub entries: Vec<Bytes>,
    pub routing_key: String,
    pub key_version: u32,
    pub subkey: [u8; 32],
    pub ts_hint_ms: Option<i64>,
    pub seq: Option<String>,
    pub bytes: usize,
    pub close: bool,
    pub producer: Option<ProducerReq>,
    pub deferred_error: Option<DeferredErr>,
    pub touch: Option<TouchFeed>,
    /// Usage counters for the STREAM identity (http-side hash), so
    /// committer-side byte accounting lands on the same row as the
    /// request/record counters.
    pub usage: std::sync::Arc<crate::usage::Counters>,
    pub resp: oneshot::Sender<Result<AppendAck, AppendErr>>,
}

#[derive(Debug, Clone)]
pub struct AppendAck {
    pub last_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    /// Echoed on producer appends: (epoch, seq to report).
    pub producer: Option<(u64, u64)>,
    /// True for producer duplicates (204, body ignored).
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub enum AppendErr {
    SeqConflict {
        current: Option<String>,
    },
    Closed {
        next_offset: u64,
    },
    ProducerGap {
        expected: u64,
        received: u64,
    },
    ProducerStale {
        current_epoch: u64,
    },
    ProducerEpochSeq,
    CtMismatch,
    BadBody(String),
    Internal(String),
    /// The shard was fenced by a new owner mid-request: retryable, the
    /// router converges within the anti-flap holdoff.
    Moved,
}

pub enum CommitOp {
    Append(AppendReq),
    /// Queue-profile state transition (PROFILES.md §7): serialized with
    /// appends, durable at the watermark like everything else.
    Queue {
        hash: [u8; 16],
        op: crate::queue::QueueOp,
        resp: oneshot::Sender<Result<crate::queue::QueueOut, String>>,
    },
    /// Absorber confirmation: history tier now durably holds [.., upto).
    /// Advances the readers' boundary and trims previously-absorbed records
    /// (deferred one round so in-flight readers never lose their range).
    /// `v2` marks the range as living in the SHARED per-shard partition
    /// (docs/HISTORY-V2.md); the first advancing v2 op sets the stream's
    /// history_v2 flag, which gates the read path's history source. The
    /// v2 absorber flushes MANY streams once and then submits one of
    /// these per covered stream; they coalesce into the same committer
    /// batch, so the boundaries land in one tracker write-batch.
    Absorbed {
        hash: [u8; 16],
        upto: u64,
        v2: bool,
    },
}

/// Notification to the absorber that a stream accumulated shard-log bytes.
#[derive(Debug, Clone)]
pub struct AbsorbSignal {
    pub hash: [u8; 16],
    pub appended_bytes: u64,
}

#[derive(Clone)]
pub struct ShardConfig {
    pub queue_reqs: usize,
    pub max_batch_reqs: usize,
    pub max_batch_bytes: usize,
    pub max_trim_per_op: u64,
    /// Commit pacing: once a drained group has at least this many requests
    /// (i.e. the stream is BUSY), keep gathering until `gather_window` so
    /// one flush cycle ships one big WAL SST instead of many tiny ones.
    pub pace_min_reqs: usize,
    pub gather_window: std::time::Duration,
    /// Group-commit WAL flushing: instead of waiting for SlateDB's fixed
    /// flush tick, a pump task flushes the WAL the moment the previous
    /// flush completes if commits are waiting. Under load the cadence
    /// self-clocks to the WAL PUT RTT (the in-flight PUT is the batching
    /// window); `wal_flush_gap` only bounds the SST mint rate when the
    /// PUT RTT is shorter than the gap — the object-churn ceiling stays
    /// exactly where the old tick put it.
    pub wal_group_commit: bool,
    pub wal_flush_gap: std::time::Duration,
    /// Post-ACK gather window. After a busy flush completes, the pump
    /// itself releases the acknowledgements for everything that flush made
    /// durable (an explicit barrier — not a scheduler race with the
    /// acker), then waits this long before freezing the next WAL. The
    /// point: closed-loop producers' next requests, issued in reaction to
    /// those acks, arrive DURING the window and join the next WAL instead
    /// of missing its freeze by a millisecond and waiting a full extra
    /// PUT behind it. Zero disables the barrier and the window (acks
    /// release only via the acker; the next freeze races the ack herd —
    /// measured to cost c2 ≈ 2×c1 append p50). Never delays an idle
    /// shard's first write: the pump only gathers after a flush that
    /// dispatched work.
    pub wal_post_ack_gather: std::time::Duration,
    /// Skip the gather when the NEXT WAL is already busy: a window only
    /// pays off when the coming generation is small (it exists to let an
    /// ack-triggered herd join); at saturation it is a pure latency and
    /// throughput tax. Thresholds are checked after ack dispatch, against
    /// what is already committed-but-unflushed.
    pub wal_gather_skip_reqs: u32,
    pub wal_gather_skip_bytes: u64,
    /// Durable-tail ring budget in bytes for THIS engine (0 = off). When
    /// on, dispatch_durable publishes each group's freshly-durable frames
    /// into a per-stream in-memory ring before releasing acks, and live
    /// tail reads are served from that ring instead of a SlateDB scan.
    /// The canonical read path (scan -> history) remains the source of
    /// truth for anything the ring no longer covers: restart, eviction,
    /// lagging consumers.
    pub tail_ring_bytes: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            queue_reqs: 65_536,
            max_batch_reqs: 16_384,
            max_batch_bytes: 48 * 1024 * 1024,
            max_trim_per_op: 8_192,
            pace_min_reqs: 32,
            gather_window: std::time::Duration::from_millis(15),
            wal_group_commit: false,
            wal_flush_gap: std::time::Duration::from_millis(25),
            wal_post_ack_gather: std::time::Duration::ZERO,
            wal_gather_skip_reqs: 32,
            wal_gather_skip_bytes: 1024 * 1024,
            tail_ring_bytes: 0,
        }
    }
}

/// Per-commit-group pipeline decomposition: where a request's time goes
/// between arriving at the committer and being durably acked.
#[derive(Clone, Copy, Debug)]
pub struct GroupTiming {
    pub ts_ms: i64,
    /// Oldest request's wait in the committer queue before this group.
    pub queue_wait_us: u32,
    /// Serial committer work: producer checks, encryption, WriteBatch build.
    pub encode_us: u32,
    /// db.write (memtable apply; blocks under byte backpressure).
    pub write_us: u32,
    /// Wait for the durable watermark (WAL flush + PUT).
    pub durable_wait_us: u32,
    pub reqs: u32,
    pub records: u32,
    pub bytes: u64,
}

struct InFlightGroup {
    seq: u64,
    /// Commit-pipeline instrumentation: when db.write returned.
    written_at: std::time::Instant,
    queue_wait_us: u32,
    encode_us: u32,
    /// How long db.write itself took (µs).
    write_us: u32,
    reqs: u32,
    records_n: u32,
    bytes: u64,
    acks: Vec<(oneshot::Sender<Result<AppendAck, AppendErr>>, AppendAck)>,
    queue_acks: Vec<(
        oneshot::Sender<Result<crate::queue::QueueOut, String>>,
        crate::queue::QueueOut,
    )>,
    tails: Vec<(Arc<StreamHandle>, TailFields)>,
    /// Frames to publish into the durable-tail ring at dispatch time,
    /// BEFORE tail state moves and acks are sent.
    ring_pub: Vec<(Arc<StreamHandle>, Vec<(u64, Bytes)>)>,
    signals: Vec<AbsorbSignal>,
    touches: Vec<TouchFeed>,
}

pub struct ShardEngine {
    pub prefix: String,
    pub db: Arc<Db>,
    /// Object store the shard's DBs live on — held so the engine can
    /// lazily open its shared history v2 partition.
    data_store: Arc<dyn object_store::ObjectStore>,
    /// Shared history v2 partition (docs/HISTORY-V2.md): ONE writer Db
    /// per shard at `{prefix}/history2`, opened lazily by whoever needs
    /// it first (absorber gather lane or a v2 history read) and shared —
    /// two independent opens would fence each other. Closed with the
    /// engine; a new shard owner's open fences this one at the slatedb
    /// layer, same dynamics as the per-stream v1 DBs.
    history2: tokio::sync::OnceCell<Arc<Db>>,
    streams: Mutex<HashMap<[u8; 16], Arc<StreamHandle>>>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<Vec<InFlightGroup>>,
    /// Serializes durable-dispatch between the pump (post-flush barrier)
    /// and the acker (failsafe + fencing path). Group drains are already
    /// exclusive via the in_flight lock; this additionally keeps tail
    /// state updates applying in seq order across the two callers.
    dispatch_gate: Mutex<()>,
    /// Pump telemetry: flushes issued, requests acked at the pump's own
    /// barrier, gather windows taken. acked/flushes is the requests-per-
    /// WAL figure the flush-scheduling change is judged by.
    pub pump_flushes: AtomicU64,
    pub pump_barrier_acked: AtomicU64,
    pub pump_gathers: AtomicU64,
    /// Windows skipped because the next generation was already busy
    /// (adaptive gather), and requests observed to arrive DURING applied
    /// windows (the herd the window exists to catch).
    pub pump_gathers_skipped_busy: AtomicU64,
    pub pump_gathered_reqs: AtomicU64,
    /// Per-flush ledger: what each pump flush actually shipped.
    /// requests_per_wal = flushed_reqs / flushes, delta'd by the harness.
    pub pump_flushed_reqs: AtomicU64,
    pub pump_flushed_records: AtomicU64,
    pub pump_flushed_bytes: AtomicU64,
    /// Ack-to-next-enqueue: µs from an ack dispatch to the FIRST client
    /// request that follows it — direct evidence the ack-triggered herd
    /// arrives within the gather window (sum/count; armed at dispatch).
    pub ack_to_enqueue_sum_us: AtomicU64,
    pub ack_to_enqueue_count: AtomicU64,
    ack_armed_at_us: AtomicU64,
    /// Monotonic epoch for cheap µs stamps.
    epoch: std::time::Instant,
    /// Durable-tail ring accounting. `ring_budget` is the remaining global
    /// byte allowance (config minus resident bytes; goes negative
    /// transiently during a publish, restored by eviction). `ring_fifo`
    /// mirrors publish order engine-wide: one entry per published batch,
    /// so popping its front always evicts the globally oldest batch.
    ring_enabled: bool,
    ring_budget: std::sync::atomic::AtomicI64,
    ring_fifo: Mutex<std::collections::VecDeque<Arc<StreamHandle>>>,
    pub ring_published: AtomicU64,
    pub ring_hits: AtomicU64,
    pub ring_misses: AtomicU64,
    /// Miss causes. below_floor = reader lagging behind eviction;
    /// above_ceil = reader knows an end the ring has not been handed yet
    /// (mid-dispatch); one miss can set both. empty = stream has no ring
    /// (never published, or fully evicted). With the ring enabled,
    /// hits + misses = ring_read attempts.
    pub ring_miss_below_floor: AtomicU64,
    pub ring_miss_above_ceil: AtomicU64,
    pub ring_miss_empty: AtomicU64,
    pub ring_evicted: AtomicU64,
    /// Resident bytes high-water mark; current residency is
    /// (config budget - ring_budget), exposed alongside it.
    pub ring_peak_bytes: AtomicU64,
    ring_cfg_bytes: u64,
    /// Level-triggered close signal for background tasks (see start()).
    close_tx: tokio::sync::watch::Sender<bool>,
    /// Handles for every task this engine spawned, so termination is a
    /// provable fact (`await_terminated`) instead of an assumption.
    tasks: Mutex<Vec<(&'static str, tokio::task::JoinHandle<()>)>>,
    flush_wake: Notify,
    /// Group-commit pump wake: one permit means "commits landed since the
    /// pump last looked". Distinct from flush_wake, whose permit the acker
    /// loop consumes.
    pump_wake: Notify,
    absorb_tx: mpsc::Sender<AbsorbSignal>,
    /// Invoked when the shard db closes (fenced by a new owner / fatal):
    /// wired to TouchRegistry::close_shard so hanging /touch/wait clients
    /// get stale immediately instead of dangling until timeout.
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Set once when the shard db reports closed (fenced by a new owner or
    /// fatal). Everything still holding this engine — request handlers, the
    /// committer, the absorber — must fail fast / exit instead of retrying
    /// against a dead db (the "zombie engine" fuel of the absorption war).
    closed: std::sync::atomic::AtomicBool,
    /// Wall-clock ms when the current db.write began, 0 when idle. A
    /// nonzero value that stays old means the commit pipeline is BLOCKED
    /// (L0-full / unflushed-full while compaction lags) — admission must
    /// shed instead of letting appends hang into the front door's 30 s
    /// kill (the 2026-07-21 8-minute wedge).
    commit_write_started_ms: std::sync::atomic::AtomicI64,
    pub stats_appended: AtomicU64,
    /// Last commit-group timings for /v1/debug/timings.
    pub timings: Mutex<std::collections::VecDeque<GroupTiming>>,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl ShardEngine {
    pub fn start(
        prefix: String,
        db: Arc<Db>,
        data_store: Arc<dyn object_store::ObjectStore>,
        cfg: ShardConfig,
        absorb_tx: mpsc::Sender<AbsorbSignal>,
        on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Arc<ShardEngine> {
        let (tx, rx) = mpsc::channel(cfg.queue_reqs);
        // Level-triggered close signal for the background tasks. The
        // committer cannot rely on its channel closing: the engine itself
        // holds a sender, and the committer holds the engine — a retain
        // cycle that used to leave one committer task (and the whole
        // engine allocation) resident per shard move, forever.
        let (close_tx, _) = tokio::sync::watch::channel(false);
        let engine = Arc::new(ShardEngine {
            prefix,
            db,
            data_store,
            history2: tokio::sync::OnceCell::new(),
            streams: Mutex::new(HashMap::new()),
            tx,
            in_flight: Mutex::new(Vec::new()),
            dispatch_gate: Mutex::new(()),
            pump_flushes: AtomicU64::new(0),
            pump_barrier_acked: AtomicU64::new(0),
            pump_gathers: AtomicU64::new(0),
            pump_gathers_skipped_busy: AtomicU64::new(0),
            pump_gathered_reqs: AtomicU64::new(0),
            pump_flushed_reqs: AtomicU64::new(0),
            pump_flushed_records: AtomicU64::new(0),
            pump_flushed_bytes: AtomicU64::new(0),
            ack_to_enqueue_sum_us: AtomicU64::new(0),
            ack_to_enqueue_count: AtomicU64::new(0),
            ack_armed_at_us: AtomicU64::new(0),
            epoch: std::time::Instant::now(),
            ring_enabled: cfg.tail_ring_bytes > 0,
            ring_budget: std::sync::atomic::AtomicI64::new(cfg.tail_ring_bytes as i64),
            ring_fifo: Mutex::new(std::collections::VecDeque::new()),
            ring_published: AtomicU64::new(0),
            ring_hits: AtomicU64::new(0),
            ring_misses: AtomicU64::new(0),
            ring_miss_below_floor: AtomicU64::new(0),
            ring_miss_above_ceil: AtomicU64::new(0),
            ring_miss_empty: AtomicU64::new(0),
            ring_evicted: AtomicU64::new(0),
            ring_peak_bytes: AtomicU64::new(0),
            ring_cfg_bytes: cfg.tail_ring_bytes as u64,
            flush_wake: Notify::new(),
            pump_wake: Notify::new(),
            absorb_tx,
            on_close,
            closed: std::sync::atomic::AtomicBool::new(false),
            close_tx,
            tasks: Mutex::new(Vec::new()),
            commit_write_started_ms: std::sync::atomic::AtomicI64::new(0),
            stats_appended: AtomicU64::new(0),
            timings: Mutex::new(std::collections::VecDeque::new()),
        });
        // Group-commit flush pump: waits for a committed group, flushes the
        // WAL, and immediately flushes again if more groups arrived while
        // the PUT was in flight — the ack path stops paying the tick
        // alignment (avg tick/2) on top of the serial-PUT queue. The gap
        // check runs start-to-start, so when the PUT RTT exceeds the gap
        // (the normal Tigris case) it adds zero wait, and when the store is
        // faster than the gap it enforces the same max SST mint rate as the
        // old tick. SlateDB's own flush_interval stays on as a long
        // failsafe (shard_settings stretches it when the pump is enabled).
        let mut task_handles: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();
        if cfg.wal_group_commit {
            let pump = engine.clone();
            let gap = cfg.wal_flush_gap;
            let gather = cfg.wal_post_ack_gather;
            let skip_reqs = cfg.wal_gather_skip_reqs;
            let skip_bytes = cfg.wal_gather_skip_bytes;
            tracing::info!(
                shard = %pump.prefix,
                gap_ms = gap.as_millis() as u64,
                gather_ms = gather.as_millis() as u64,
                "WAL group-commit pump on"
            );
            let h = tokio::spawn(async move {
                use slatedb::config::{FlushOptions, FlushType};
                let mut status_rx = pump.db.subscribe();
                let mut last_start: Option<std::time::Instant> = None;
                loop {
                    pump.pump_wake.notified().await;
                    if pump.is_closed() {
                        return;
                    }
                    // Only flush when a commit is actually awaiting
                    // durability. Without this, an ack-triggered client
                    // herd arrives just after a speculative empty flush
                    // froze the buffer and waits a full extra PUT behind
                    // it (closed-loop A/B measured 52 ms vs 29 ms
                    // durable_wait on identical load).
                    if pump.in_flight.lock().unwrap().is_empty() {
                        continue;
                    }
                    if let Some(t0) = last_start {
                        let since = t0.elapsed();
                        if since < gap {
                            tokio::time::sleep(gap - since).await;
                            if pump.is_closed() {
                                return;
                            }
                        }
                    }
                    // Herd-settle: a synced herd's requests arrive within
                    // microseconds of each other, and the wake->freeze
                    // path is tight enough to split them into two WALs.
                    // 1 ms is far above their spread and far below the
                    // PUT RTT, so a solo producer pays ~1 ms and a herd
                    // stays one WAL.
                    if !gather.is_zero() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        if pump.is_closed() {
                            return;
                        }
                    }
                    last_start = Some(std::time::Instant::now());
                    // The seqs this flush is about to make durable: every
                    // group written before the freeze. Captured BEFORE the
                    // flush call, because the barrier below must wait for
                    // the watermark to cover them — `flush()` resolves
                    // before the status watch publishes the new durable
                    // seq (measured: at c2 the post-flush borrow saw a
                    // stale watermark on 1612 of 1626 flushes, silently
                    // reducing the barrier to the old acker race).
                    // Per-flush ledger + barrier target, captured together
                    // BEFORE the flush (capturing after could include — and
                    // wait on — the NEXT generation: deadlock shape).
                    let (target_seq, fl_reqs, fl_records, fl_bytes) = {
                        let q = pump.in_flight.lock().unwrap();
                        (
                            q.last().map(|g| g.seq),
                            q.iter().map(|g| g.reqs as u64).sum::<u64>(),
                            q.iter().map(|g| g.records_n as u64).sum::<u64>(),
                            q.iter().map(|g| g.bytes).sum::<u64>(),
                        )
                    };
                    match pump
                        .db
                        .flush_with_options(FlushOptions {
                            flush_type: FlushType::Wal,
                        })
                        .await
                    {
                        Err(e) => {
                            if pump.is_closed() {
                                return;
                            }
                            tracing::warn!(shard = %pump.prefix, "group-commit WAL flush failed: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        Ok(()) => {
                            pump.pump_flushes.fetch_add(1, Ordering::Relaxed);
                            pump.pump_flushed_reqs.fetch_add(fl_reqs, Ordering::Relaxed);
                            pump.pump_flushed_records
                                .fetch_add(fl_records, Ordering::Relaxed);
                            pump.pump_flushed_bytes.fetch_add(fl_bytes, Ordering::Relaxed);
                            if gather.is_zero() {
                                continue;
                            }
                            let Some(target) = target_seq else {
                                continue;
                            };
                            // Explicit barrier: wait for the durable
                            // watermark to actually cover what we froze,
                            // then release those acks HERE, synchronously.
                            // When dispatch_durable returns, every response
                            // this flush unblocked is on its way to a
                            // socket. The 250 ms ceiling is a failsafe
                            // (fencing, store stall): the acker still owns
                            // dispatch if we bail.
                            // The watch Ref is !Send, so copy out of it
                            // inside this block — nothing Ref-typed may
                            // survive to the gather sleep below.
                            // Ok(seq) = dispatch; Err(true) = watch gone
                            // (db closed, exit); Err(false) = skip
                            // (fenced, or failsafe timeout: the acker
                            // still owns dispatch).
                            let seen: Result<u64, bool> = {
                                match tokio::time::timeout(
                                    std::time::Duration::from_millis(250),
                                    status_rx.wait_for(|s| {
                                        s.durable_seq >= target || s.close_reason.is_some()
                                    }),
                                )
                                .await
                                {
                                    Ok(Ok(sref)) if sref.close_reason.is_some() => Err(false),
                                    Ok(Ok(sref)) => Ok(sref.durable_seq),
                                    Ok(Err(_)) => Err(true),
                                    Err(_) => Err(false),
                                }
                            };
                            let durable_seq = match seen {
                                Ok(seq) => seq,
                                Err(true) => return,
                                Err(false) => continue,
                            };
                            // Drain whatever the acker has not already
                            // taken. Who wins that race is irrelevant to
                            // the barrier: the acker fires on the same
                            // watch change, and this call blocks on the
                            // dispatch_gate until any concurrent acker
                            // dispatch has finished sending. Either way,
                            // when this returns, every ack this flush
                            // unblocked is on the wire.
                            let acked = pump.dispatch_durable(durable_seq);
                            pump.pump_barrier_acked
                                .fetch_add(acked as u64, Ordering::Relaxed);
                            // Arm the ack->next-enqueue probe: the next
                            // try_enqueue stamps the herd's reaction time.
                            pump.ack_armed_at_us.store(
                                pump.epoch.elapsed().as_micros().max(1) as u64,
                                Ordering::Relaxed,
                            );
                            // Gather ONLY when this completion proves
                            // concurrency: someone is already waiting in
                            // in_flight (they arrived mid-PUT — the herd
                            // has drifted across two WAL generations).
                            // The window lets the just-acked clients'
                            // follow-ups land in the same WAL as the
                            // waiter, re-syncing the herd. When nobody is
                            // waiting the shard is solo or the herd is in
                            // sync — either way a window would tax c1 by
                            // its full length for nothing (measured:
                            // +7 ms on c1 p50). NOT keyed on `acked > 0`:
                            // the acker fires on the same watch change
                            // and wins the dispatch race on >99 % of
                            // flushes; keying on it skipped the window on
                            // 1636 of 1651 c2 flushes.
                            // Adaptive: gather only when (a) the herd has
                            // drifted (someone already waits) AND (b) the
                            // next generation is still SMALL — a window in
                            // front of an already-big WAL is a pure tax
                            // (review #2's throughput concern at CDG's top
                            // tiers).
                            let (drifted, pend_reqs, pend_bytes) = {
                                let q = pump.in_flight.lock().unwrap();
                                (
                                    !q.is_empty(),
                                    q.iter().map(|g| g.reqs).sum::<u32>(),
                                    q.iter().map(|g| g.bytes).sum::<u64>(),
                                )
                            };
                            if drifted
                                && (pend_reqs >= skip_reqs || pend_bytes >= skip_bytes)
                            {
                                pump.pump_gathers_skipped_busy
                                    .fetch_add(1, Ordering::Relaxed);
                            } else if drifted {
                                pump.pump_gathers.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(gather).await;
                                if pump.is_closed() {
                                    return;
                                }
                                // What the window caught: requests present
                                // now that were not pending when it opened.
                                let after: u32 = pump
                                    .in_flight
                                    .lock()
                                    .unwrap()
                                    .iter()
                                    .map(|g| g.reqs)
                                    .sum();
                                pump.pump_gathered_reqs.fetch_add(
                                    after.saturating_sub(pend_reqs) as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            });
            task_handles.push(("pump", h));
        }
        let committer = engine.clone();
        task_handles.push((
            "committer",
            tokio::spawn(async move { committer.committer_loop(rx, cfg).await }),
        ));
        let acker = engine.clone();
        task_handles.push((
            "acker",
            tokio::spawn(async move { acker.acker_loop().await }),
        ));
        // F1 recovery bound: `max_wal_flushes_before_l0_flush` has a 4096
        // upstream floor, so we cap the WAL replay window ourselves with a
        // periodic explicit memtable->L0 flush whenever data accumulated.
        let ticker = engine.clone();
        let mut ticker_closed = engine.close_tx.subscribe();
        task_handles.push((
            "flush-ticker",
            tokio::spawn(async move {
                use slatedb::config::{FlushOptions, FlushType};
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                let mut last_appended = 0u64;
                loop {
                    tokio::select! {
                        _ = ticker_closed.changed() => {
                            if *ticker_closed.borrow() {
                                return;
                            }
                        }
                        _ = interval.tick() => {}
                    }
                    if ticker.is_closed() {
                        return;
                    }
                    let appended = ticker.stats_appended.load(Ordering::Relaxed);
                    if appended != last_appended {
                        last_appended = appended;
                        if let Err(e) = ticker
                            .db
                            .flush_with_options(FlushOptions {
                                flush_type: FlushType::MemTable,
                            })
                            .await
                        {
                            tracing::warn!(shard = %ticker.prefix, "memtable flush tick failed: {e}");
                        }
                    }
                }
            }),
        ));
        *engine.tasks.lock().unwrap() = task_handles;
        engine
    }

    /// Await every background task this engine spawned, up to `timeout`.
    ///
    /// `JoinHandle::is_finished()` is not evidence of clean termination —
    /// it is also true after a panic. This joins each task and names the
    /// stragglers, so "the fenced owner's tasks exited" is a provable
    /// statement (the committer used to be unprovable: it held the engine,
    /// the engine held its sender, so the channel could never close).
    pub async fn await_terminated(&self, timeout: std::time::Duration) -> Result<(), String> {
        let handles: Vec<(&'static str, tokio::task::JoinHandle<()>)> =
            self.tasks.lock().unwrap().drain(..).collect();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut failed: Vec<String> = Vec::new();
        for (name, h) in handles {
            match tokio::time::timeout_at(deadline, h).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => failed.push(format!("{name}: panicked ({e})")),
                Err(_) => failed.push(format!("{name}: still running at timeout")),
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    }

    pub fn try_enqueue(&self, req: AppendReq) -> Result<(), AppendReq> {
        // Ack->next-enqueue probe (armed by the pump at ack dispatch): the
        // first request after an ack wave stamps how fast the closed-loop
        // herd reacts — the number the gather window is sized against.
        let armed = self.ack_armed_at_us.swap(0, Ordering::Relaxed);
        if armed != 0 {
            let now = self.epoch.elapsed().as_micros() as u64;
            self.ack_to_enqueue_sum_us
                .fetch_add(now.saturating_sub(armed), Ordering::Relaxed);
            self.ack_to_enqueue_count.fetch_add(1, Ordering::Relaxed);
        }
        self.tx
            .try_send(CommitOp::Append(req))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(CommitOp::Append(r)) => r,
                mpsc::error::TrySendError::Closed(CommitOp::Append(r)) => r,
                _ => unreachable!(),
            })
    }

    /// True once the shard db reported closed (fenced by a new owner or a
    /// fatal storage error). Holders must stop using this engine.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Proactive close (rebalancer moved this shard away): mark closed,
    /// wake the pump, fail everything in flight NOW. Without this,
    /// requests already queued here hang until the new owner's fence
    /// propagates — clients sat out their full timeout (ladder D3:
    /// exactly one in-flight batch per worker lost at the move moment).
    pub fn begin_close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return; // already closing
        }
        let _ = self.close_tx.send(true);
        self.pump_wake.notify_one();
        let stranded: Vec<InFlightGroup> = self.in_flight.lock().unwrap().drain(..).collect();
        for group in stranded {
            for (resp, _) in group.acks {
                let _ = resp.send(Err(AppendErr::Moved));
            }
            for (resp, _) in group.queue_acks {
                let _ = resp.send(Err("shard fenced/moved; retry".into()));
            }
        }
        if let Some(cb) = &self.on_close {
            cb();
        }
        // Actually close the slatedb Db. Without this the moved-away
        // shard keeps a ZOMBIE db: its compactor/GC/flusher run until the
        // new owner fences it on first routed request — which lazy opening
        // can delay indefinitely (ladder p3: 92 minutes of unowned zombie
        // on shard 000, GC racing the eventual open).
        let db = self.db.clone();
        let history2 = self.history2.get().cloned();
        let prefix = self.prefix.clone();
        tokio::spawn(async move {
            if let Err(e) = db.close().await {
                tracing::warn!(shard = %prefix, "db close after move-away: {e}");
            }
            // Same zombie logic for the shared history partition: the new
            // owner's open fences it, but close it deliberately.
            if let Some(h2) = history2 {
                if let Err(e) = h2.close().await {
                    tracing::warn!(shard = %prefix, "history2 close after move-away: {e}");
                }
            }
        });
    }

    /// How long the current commit db.write has been blocked (0 = idle).
    /// A sustained value means SlateDB backpressure (L0-full/unflushed-full
    /// with lagging compaction): admission should shed 429 instead of
    /// queueing appends into a hang.
    pub fn commit_blocked_ms(&self) -> i64 {
        let started = self.commit_write_started_ms.load(Ordering::SeqCst);
        if started == 0 {
            0
        } else {
            (now_ms() - started).max(0)
        }
    }

    #[cfg(test)]
    pub fn set_commit_write_started_ms(&self, v: i64) {
        self.commit_write_started_ms.store(v, Ordering::SeqCst);
    }

    /// Age of the oldest committed-but-not-durable group (0 = none). THE
    /// wedge signal for the common stall mode: db.write keeps succeeding
    /// (memtable has room) while the WAL-flush pipeline is stalled behind
    /// L0-full, so groups pile up here waiting for the durable watermark.
    /// The 2026-07-22 final gate run proved commit_blocked_ms alone misses
    /// this mode entirely (wedge_shed=0 through a 10-minute wedge).
    pub fn oldest_inflight_ms(&self) -> i64 {
        self.in_flight
            .lock()
            .unwrap()
            .first()
            .map(|g| g.written_at.elapsed().as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }

    /// Combined wedge signal: blocked commit write OR stale durability.
    pub fn wedge_ms(&self) -> i64 {
        self.commit_blocked_ms().max(self.oldest_inflight_ms())
    }

    pub async fn submit_absorbed(&self, hash: [u8; 16], upto: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed { hash, upto, v2: false })
            .await;
    }

    /// v2 boundary advance: the range is in the shared partition.
    pub async fn submit_absorbed_v2(&self, hash: [u8; 16], upto: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed { hash, upto, v2: true })
            .await;
    }

    /// The shard's shared history v2 partition, opened once and shared
    /// between the absorber's gather lane and v2 history reads. Values
    /// are raw stream-key-encrypted frames, so the partition needs no
    /// block transformer and no compression (frames compress before
    /// encryption; re-compressing ciphertext is pure waste).
    pub async fn history_partition(&self) -> Result<Arc<Db>, slatedb::Error> {
        if self.is_closed() {
            return Err(slatedb::Error::data("engine closed".to_string()));
        }
        self.history2
            .get_or_try_init(|| async {
                let path = format!("{}/history2", self.prefix);
                let store = self.data_store.clone();
                let db = crate::on_slatedb_rt(async move {
                    Db::builder(path.as_str(), store)
                        .with_settings(crate::history::history2_settings())
                        .with_db_cache(crate::history::history_cache())
                        .build()
                        .await
                })
                .await?;
                Ok(Arc::new(db))
            })
            .await
            .cloned()
    }

    /// Streams this engine holds open whose durable log extends past their
    /// absorbed boundary — the absorber's re-discovery sweep. Signals are
    /// the fast path; this closes their gaps (a bounded `try_send` channel
    /// drops under a wide backlog, and a restarted instance has no signals
    /// for data absorbed-before-crash): any stream a signal missed is
    /// re-found here as long as its handle is resident. Handles are
    /// snapshotted under the lock and inspected outside it.
    pub fn absorb_backlog(&self) -> Vec<([u8; 16], u64)> {
        let handles: Vec<([u8; 16], Arc<StreamHandle>)> = self
            .streams
            .lock()
            .unwrap()
            .iter()
            .map(|(h, e)| (*h, e.clone()))
            .collect();
        handles
            .into_iter()
            .filter_map(|(h, e)| {
                let st = e.state.lock().unwrap();
                let backlog = st.durable.next.saturating_sub(st.durable.absorbed);
                (backlog > 0).then_some((h, backlog))
            })
            .collect()
    }

    /// Enumerate the durable dirty-stream index: every stream whose last
    /// committed batch left `absorbed < next`, with those two boundaries
    /// as of that batch. This is how a fresh owner rediscovers unabsorbed
    /// tails after restart/handoff WITHOUT materializing stream handles
    /// (the resident-handle sweep in `absorb_backlog` only sees streams
    /// something already touched) and without customer keys.
    pub async fn scan_dirty_streams(&self) -> anyhow::Result<Vec<([u8; 16], u64, u64)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&DIRTY_SENTINEL);
        pfx.push(b'D');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != 33 || kv.value.len() < 16 {
                continue;
            }
            let mut h = [0u8; 16];
            h.copy_from_slice(&kv.key[17..33]);
            let absorbed = u64::from_le_bytes(kv.value[..8].try_into().unwrap());
            let next = u64::from_le_bytes(kv.value[8..16].try_into().unwrap());
            out.push((h, absorbed, next));
        }
        Ok(out)
    }

    /// The absorbed boundary as recorded by the REMOTELY-DURABLE tracker —
    /// the strongest boundary any `DurabilityLevel::Remote` scan of the
    /// shard log can have observed trims for. The published handle state is
    /// NOT enough for that purpose: trim deletes become scan-visible when
    /// their batch is durable, while `handle.state.durable` advances only
    /// at dispatch, which can lag durability arbitrarily under load
    /// (2026-07-27 boundary-race DST failure). Readers revalidating a tail
    /// scan against concurrent absorption must consult this.
    pub async fn durable_absorbed(&self, hash: &[u8; 16]) -> Result<u64, slatedb::Error> {
        let v = self
            .db
            .get_with_options(
                tail_key(hash),
                &slatedb::config::ReadOptions {
                    durability_filter: DurabilityLevel::Remote,
                    ..Default::default()
                },
            )
            .await?;
        Ok(v.and_then(|b| decode_tail(&b)).map_or(0, |t| t.absorbed))
    }

    pub async fn submit_queue(
        &self,
        hash: [u8; 16],
        op: crate::queue::QueueOp,
    ) -> Result<crate::queue::QueueOut, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CommitOp::Queue { hash, op, resp: tx })
            .await
            .map_err(|_| "committer gone".to_string())?;
        rx.await
            .map_err(|_| "committer dropped request".to_string())?
    }

    pub async fn stream_handle(&self, hash: [u8; 16]) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(h) = self.streams.lock().unwrap().get(&hash) {
            return Ok(h.clone());
        }
        let tail = match self.db.get(tail_key(&hash)).await? {
            Some(raw) => decode_tail(&raw).unwrap_or_default(),
            None => TailFields::default(),
        };
        let handle = Arc::new(StreamHandle {
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                producers: HashMap::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            ring: Mutex::new(TailRing::default()),
        });
        let mut map = self.streams.lock().unwrap();
        Ok(map.entry(hash).or_insert(handle).clone())
    }

    async fn committer_loop(self: Arc<Self>, mut rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {
        // The close signal is the ONLY way out: this task holds the engine
        // and the engine holds a sender, so `rx` can never report closed.
        let mut closed_rx = self.close_tx.subscribe();
        loop {
            let first = tokio::select! {
                _ = closed_rx.changed() => {
                    if !*closed_rx.borrow() {
                        continue;
                    }
                    // Fail everything still queued — their clients would
                    // otherwise hang into their own timeouts — then exit.
                    while let Ok(op) = rx.try_recv() {
                        match op {
                            CommitOp::Append(r) => {
                                let _ = r.resp.send(Err(AppendErr::Moved));
                            }
                            CommitOp::Queue { resp, .. } => {
                                let _ = resp.send(Err("shard fenced/moved; retry".into()));
                            }
                            CommitOp::Absorbed { .. } => {}
                        }
                    }
                    return;
                }
                got = rx.recv() => {
                    let Some(op) = got else { return };
                    op
                }
            };
            if self.is_closed() {
                // Set-before-subscribe race: honor the flag, fail the op we
                // just took plus the rest of the queue, and exit.
                match first {
                    CommitOp::Append(r) => {
                        let _ = r.resp.send(Err(AppendErr::Moved));
                    }
                    CommitOp::Queue { resp, .. } => {
                        let _ = resp.send(Err("shard fenced/moved; retry".into()));
                    }
                    CommitOp::Absorbed { .. } => {}
                }
                while let Ok(op) = rx.try_recv() {
                    match op {
                        CommitOp::Append(r) => {
                            let _ = r.resp.send(Err(AppendErr::Moved));
                        }
                        CommitOp::Queue { resp, .. } => {
                            let _ = resp.send(Err("shard fenced/moved; retry".into()));
                        }
                        CommitOp::Absorbed { .. } => {}
                    }
                }
                return;
            }
            let mut ops = vec![first];
            let mut bytes = match &ops[0] {
                CommitOp::Append(r) => r.bytes,
                _ => 0,
            };
            while ops.len() < cfg.max_batch_reqs && bytes < cfg.max_batch_bytes {
                match rx.try_recv() {
                    Ok(op) => {
                        if let CommitOp::Append(r) = &op {
                            bytes += r.bytes;
                        }
                        ops.push(op);
                    }
                    Err(_) => break,
                }
            }
            // PACING (throughput-critical): each commit group becomes ONE
            // write batch and therefore ~one WAL SST; the flusher PUTs WAL
            // SSTs serially at ~1/objstore-RTT. Committing eagerly makes
            // throughput = small-group × PUT-rate (measured 4 MB/s at 25 ms
            // RTT). Under load, gather up to `gather_window` so each flush
            // cycle ships one BIG group instead of many tiny ones — bursts
            // measured 90 MB/s through this exact path. Quiet streams skip
            // the wait entirely (latency unchanged at low rate).
            if ops.len() >= cfg.pace_min_reqs
                && ops.len() < cfg.max_batch_reqs
                && bytes < cfg.max_batch_bytes
            {
                let deadline = tokio::time::Instant::now() + cfg.gather_window;
                loop {
                    if ops.len() >= cfg.max_batch_reqs || bytes >= cfg.max_batch_bytes {
                        break;
                    }
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(op)) => {
                            if let CommitOp::Append(r) = &op {
                                bytes += r.bytes;
                            }
                            ops.push(op);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            self.commit_group(ops, &cfg).await;
        }
    }

    async fn commit_group(&self, ops: Vec<CommitOp>, cfg: &ShardConfig) {
        if self.is_closed() {
            // Fenced mid-flight: fail fast instead of writing to a dead db.
            for op in ops {
                match op {
                    CommitOp::Append(r) => {
                        let _ = r.resp.send(Err(AppendErr::Moved));
                    }
                    CommitOp::Queue { resp, .. } => {
                        let _ = resp.send(Err("shard fenced/moved; retry".into()));
                    }
                    CommitOp::Absorbed { .. } => {}
                }
            }
            return;
        }
        let group_t0 = std::time::Instant::now();
        let mut oldest_enqueue: Option<std::time::Instant> = None;
        for op in &ops {
            if let CommitOp::Append(r) = op {
                if oldest_enqueue.map(|o| r.enqueued_at < o).unwrap_or(true) {
                    oldest_enqueue = Some(r.enqueued_at);
                }
            }
        }
        let queue_wait_us = oldest_enqueue
            .map(|o| group_t0.duration_since(o).as_micros().min(u32::MAX as u128) as u32)
            .unwrap_or(0);
        let group_reqs = ops.len() as u32;
        struct Local {
            handle: Arc<StreamHandle>,
            fields: TailFields,
            base: TailFields,
            producers: HashMap<String, (u64, u64, u64)>,
            appended_bytes: u64,
            /// Frames written by this group, retained for the durable-tail
            /// ring (empty when the ring is off). Offsets are contiguous
            /// per stream: every append path assigns at fields.next.
            ring_recs: Vec<(u64, Bytes)>,
        }

        let mut wb = WriteBatch::new();
        let mut pending: Vec<(oneshot::Sender<Result<AppendAck, AppendErr>>, AppendAck)> =
            Vec::new();
        let mut locals: HashMap<[u8; 16], Local> = HashMap::new();
        let mut records = 0u64;
        let mut touches: Vec<TouchFeed> = Vec::new();
        let mut queue_pending: Vec<(
            oneshot::Sender<Result<crate::queue::QueueOut, String>>,
            crate::queue::QueueOut,
        )> = Vec::new();
        let mut extra_writes = false;

        for op in ops {
            let hash = match &op {
                CommitOp::Append(r) => r.hash,
                CommitOp::Absorbed { hash, .. } => *hash,
                CommitOp::Queue { hash, .. } => *hash,
            };
            if !locals.contains_key(&hash) {
                match self.stream_handle(hash).await {
                    Ok(handle) => {
                        let applied = handle.state.lock().unwrap().applied.clone();
                        locals.insert(
                            hash,
                            Local {
                                handle,
                                fields: applied.clone(),
                                base: applied,
                                producers: HashMap::new(),
                                appended_bytes: 0,
                                ring_recs: Vec::new(),
                            },
                        );
                    }
                    Err(e) => {
                        if let CommitOp::Append(r) = op {
                            let _ = r.resp.send(Err(AppendErr::Internal(e.to_string())));
                        }
                        continue;
                    }
                }
            }
            let local = locals.get_mut(&hash).expect("local");

            match op {
                CommitOp::Append(req) => {
                    // Producer state: ensure loaded (durable `q` key) into
                    // the batch-local staging map.
                    if let Some(pr) = &req.producer {
                        if !local.producers.contains_key(&pr.id) {
                            let shared = {
                                let st = local.handle.state.lock().unwrap();
                                st.producers.get(&pr.id).copied()
                            };
                            let loaded = match shared {
                                Some(v) => Some(v),
                                None => match self.db.get(producer_key(&hash, &pr.id)).await {
                                    // 24-byte current format carries the
                                    // commit offset; 16-byte legacy rows
                                    // fall back to "offset unknown" (0),
                                    // where the duplicate ack degrades to
                                    // the old tail-based answer.
                                    Ok(Some(v)) if v.len() >= 24 => Some((
                                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                                        u64::from_le_bytes(v[16..24].try_into().unwrap()),
                                    )),
                                    Ok(Some(v)) if v.len() >= 16 => Some((
                                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                                        0,
                                    )),
                                    Ok(_) => None,
                                    Err(e) => {
                                        let _ =
                                            req.resp.send(Err(AppendErr::Internal(e.to_string())));
                                        continue;
                                    }
                                },
                            };
                            if let Some(v) = loaded {
                                local.producers.insert(pr.id.clone(), v);
                            }
                        }
                    }
                    // Contract check order: stale epoch (403) -> duplicate
                    // (204, before everything below) -> epoch/seq rules ->
                    // gap (409) -> closed (409) -> deferred ct/body errors ->
                    // Stream-Seq -> append.
                    let mut prod_echo: Option<(u64, u64)> = None;
                    if let Some(pr) = &req.producer {
                        match local.producers.get(&pr.id).copied() {
                            Some((ce, cs, coff)) => {
                                if pr.epoch < ce {
                                    let _ = req
                                        .resp
                                        .send(Err(AppendErr::ProducerStale { current_epoch: ce }));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq <= cs {
                                    // Duplicate: answer with the ORIGINAL
                                    // committed offset when the stored
                                    // producer row carries it (24-byte
                                    // format); a legacy 16-byte row (coff
                                    // == 0 with a non-empty log) degrades
                                    // to the tail-based answer.
                                    let last = if pr.seq == cs && coff != 0 {
                                        coff
                                    } else {
                                        local.fields.next.wrapping_sub(1)
                                    };
                                    let _ = req.resp.send(Ok(AppendAck {
                                        last_offset: last,
                                        next_offset: local.fields.next,
                                        closed: local.fields.closed,
                                        producer: Some((ce, cs)),
                                        duplicate: true,
                                    }));
                                    continue;
                                }
                                if pr.epoch > ce && pr.seq != 0 {
                                    let _ = req.resp.send(Err(AppendErr::ProducerEpochSeq));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq > cs + 1 {
                                    let _ = req.resp.send(Err(AppendErr::ProducerGap {
                                        expected: cs + 1,
                                        received: pr.seq,
                                    }));
                                    continue;
                                }
                            }
                            None => {
                                if pr.seq != 0 {
                                    let _ = req.resp.send(Err(AppendErr::ProducerGap {
                                        expected: 0,
                                        received: pr.seq,
                                    }));
                                    continue;
                                }
                            }
                        }
                        prod_echo = Some((pr.epoch, pr.seq));
                    }
                    if local.fields.closed {
                        if req.close && req.entries.is_empty() && req.producer.is_none() {
                            // Idempotent close-only.
                            let _ = req.resp.send(Ok(AppendAck {
                                last_offset: local.fields.next.wrapping_sub(1),
                                next_offset: local.fields.next,
                                closed: true,
                                producer: None,
                                duplicate: false,
                            }));
                        } else {
                            let _ = req.resp.send(Err(AppendErr::Closed {
                                next_offset: local.fields.next,
                            }));
                        }
                        continue;
                    }
                    if let Some(d) = &req.deferred_error {
                        let _ = req.resp.send(Err(match d {
                            DeferredErr::CtMismatch => AppendErr::CtMismatch,
                            DeferredErr::BadBody(m) => AppendErr::BadBody(m.clone()),
                        }));
                        continue;
                    }
                    if let Some(seq) = &req.seq {
                        if let Some(cur) = &local.fields.seq {
                            if seq <= cur {
                                let _ = req.resp.send(Err(AppendErr::SeqConflict {
                                    current: Some(cur.clone()),
                                }));
                                continue;
                            }
                        }
                    }
                    // Accept: stage producer + close + records. Entries are
                    // staged below starting at the CURRENT `next`, so this
                    // append's last offset is predictable here — persist it
                    // with the producer row so a later duplicate retry can
                    // be answered with the original offset.
                    if let Some(pr) = &req.producer {
                        let commit_last = if req.entries.is_empty() {
                            local.fields.next.wrapping_sub(1)
                        } else {
                            local.fields.next + req.entries.len() as u64 - 1
                        };
                        local
                            .producers
                            .insert(pr.id.clone(), (pr.epoch, pr.seq, commit_last));
                        let mut v = Vec::with_capacity(24);
                        v.extend_from_slice(&pr.epoch.to_le_bytes());
                        v.extend_from_slice(&pr.seq.to_le_bytes());
                        v.extend_from_slice(&commit_last.to_le_bytes());
                        wb.put(producer_key(&hash, &pr.id), v);
                    }
                    if req.close {
                        local.fields.closed = true;
                    }
                    if req.entries.is_empty() {
                        pending.push((
                            req.resp,
                            AppendAck {
                                last_offset: local.fields.next.wrapping_sub(1),
                                next_offset: local.fields.next,
                                closed: local.fields.closed,
                                producer: prod_echo,
                                duplicate: false,
                            },
                        ));
                        continue;
                    }
                    let ts = req.ts_hint_ms.unwrap_or_else(now_ms).max(local.fields.ts);
                    let start = local.fields.next;
                    // One key schedule per request, reused across the batch
                    // (was: cipher init + routing-key clone PER RECORD).
                    let cipher = crate::crypto::FrameCipher::new(&req.subkey);
                    let usage = req.usage.clone();
                    let (mut pt_sum, mut frame_sum) = (0u64, 0u64);
                    for (i, payload) in req.entries.iter().enumerate() {
                        let offset = start + i as u64;
                        let frame = cipher.encrypt(
                            &hash,
                            offset,
                            ts,
                            req.key_version,
                            &req.routing_key,
                            payload,
                        );
                        pt_sum += payload.len() as u64;
                        frame_sum += frame.len() as u64;
                        let frame = Bytes::from(frame);
                        if self.ring_enabled {
                            local.ring_recs.push((offset, frame.clone()));
                        }
                        wb.put(record_key(&hash, offset), frame);
                        local.fields.logical += payload.len() as u64;
                        local.appended_bytes += payload.len() as u64;
                    }
                    usage
                        .plaintext_bytes
                        .fetch_add(pt_sum, std::sync::atomic::Ordering::Relaxed);
                    usage
                        .frame_bytes
                        .fetch_add(frame_sum, std::sync::atomic::Ordering::Relaxed);
                    records += req.entries.len() as u64;
                    local.fields.next = start + req.entries.len() as u64;
                    local.fields.ts = ts;
                    // Route is set-once, and FROZEN once v2 absorption has
                    // begun: the shared-partition keys embed it, so a
                    // zero-route stream that absorbed under zero must not
                    // acquire a route later — its writes and reads would
                    // disagree on the key prefix.
                    if local.fields.route == [0u8; 16]
                        && req.route != [0u8; 16]
                        && !local.fields.history_v2
                    {
                        local.fields.route = req.route;
                    }
                    if req.seq.is_some() {
                        local.fields.seq = req.seq.clone();
                    }
                    if let Some(mut t) = req.touch {
                        t.next_offset = local.fields.next;
                        touches.push(t);
                    }
                    pending.push((
                        req.resp,
                        AppendAck {
                            last_offset: local.fields.next - 1,
                            next_offset: local.fields.next,
                            closed: local.fields.closed,
                            producer: prod_echo,
                            duplicate: false,
                        },
                    ));
                }
                CommitOp::Absorbed { upto, v2, .. } => {
                    let prev_absorbed = local.fields.absorbed;
                    // Only an op that ADVANCES the boundary may move the
                    // trim. The absorber re-submits an already-covered
                    // `upto` when it starts a pass before the previous
                    // advance has been dispatched to handle state; letting
                    // that duplicate trim toward `prev_absorbed` (== the
                    // live boundary) collapses the one-pass lag that
                    // in-flight readers holding a stale absorbed snapshot
                    // depend on (2026-07-27 boundary-race DST failure).
                    if upto > prev_absorbed {
                        local.fields.absorbed = upto.min(local.fields.next);
                        if v2 {
                            local.fields.history_v2 = true;
                        }
                        // Deferred trim: delete only up to the *previous*
                        // absorbed boundary, bounded per op.
                        let trim_to =
                            prev_absorbed.min(local.fields.trimmed + cfg.max_trim_per_op);
                        for off in local.fields.trimmed..trim_to {
                            wb.delete(record_key(&hash, off));
                        }
                        local.fields.trimmed = trim_to;
                    }
                }
                CommitOp::Queue { op, resp, .. } => {
                    use crate::queue::*;
                    // Lazy load of durable consumer state.
                    let loaded = { local.handle.state.lock().unwrap().queue.loaded };
                    let mut load_err: Option<String> = None;
                    if !loaded {
                        let mut fresh = QueueState {
                            consumers: HashMap::new(),
                            loaded: true,
                        };
                        'tags: for tag in [b'c', b'l', b'x'] {
                            let mut pfx = Vec::with_capacity(17);
                            pfx.extend_from_slice(&hash);
                            pfx.push(tag);
                            let mut iter = match self.db.scan_prefix(&pfx[..], ..).await {
                                Ok(i) => i,
                                Err(e) => {
                                    load_err = Some(e.to_string());
                                    break 'tags;
                                }
                            };
                            loop {
                                match iter.next().await {
                                    Ok(Some(kv)) => {
                                        let rest = &kv.key[17..];
                                        match tag {
                                            b'c' => {
                                                let consumer =
                                                    String::from_utf8_lossy(rest).into_owned();
                                                let cur = u64::from_le_bytes(
                                                    kv.value[..8].try_into().unwrap_or([0; 8]),
                                                );
                                                fresh
                                                    .consumers
                                                    .entry(consumer)
                                                    .or_default()
                                                    .cursor = cur;
                                            }
                                            _ => {
                                                let Some(sep) = rest.iter().position(|b| *b == 0)
                                                else {
                                                    continue;
                                                };
                                                let consumer =
                                                    String::from_utf8_lossy(&rest[..sep])
                                                        .into_owned();
                                                let off = u64::from_be_bytes(
                                                    rest[sep + 1..sep + 9]
                                                        .try_into()
                                                        .unwrap_or([0; 8]),
                                                );
                                                let cs =
                                                    fresh.consumers.entry(consumer).or_default();
                                                if tag == b'l' {
                                                    if let Some(l) = decode_lease(&kv.value) {
                                                        cs.leases.insert(off, l);
                                                    }
                                                } else {
                                                    cs.acked.insert(off);
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!("queue state scan: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                        local.handle.state.lock().unwrap().queue = fresh;
                    }
                    if let Some(m) = load_err {
                        let _ = resp.send(Err(m));
                        continue;
                    }
                    let now = now_ms();
                    let mut dlq_refs: Vec<(u64, u32)> = Vec::new(); // (orig off, attempts)
                    let (out, dlq_subkey) = {
                        let mut st = local.handle.state.lock().unwrap();
                        match op {
                            QueueOp::Receive {
                                consumer,
                                max,
                                visibility_ms,
                                max_deliveries,
                                dlq_subkey,
                            } => {
                                let cs = st.queue.consumers.entry(consumer.clone()).or_default();
                                let mut leased = Vec::new();
                                let mut off = cs.cursor;
                                let mut steps = 0usize;
                                while off < local.fields.next
                                    && leased.len() < max
                                    && steps < max * 8 + 4096
                                {
                                    steps += 1;
                                    if cs.acked.contains(&off) {
                                        off += 1;
                                        continue;
                                    }
                                    let prev = cs.leases.get(&off).copied();
                                    if let Some(l) = prev {
                                        if l.deadline_ms > now {
                                            off += 1;
                                            continue; // in flight
                                        }
                                        if l.delivery_count >= max_deliveries {
                                            // Poison: settle + DLQ reference.
                                            cs.leases.remove(&off);
                                            wb.delete(lease_key(&hash, &consumer, off));
                                            cs.acked.insert(off);
                                            wb.put(ack_key(&hash, &consumer, off), b"");
                                            dlq_refs.push((off, l.delivery_count));
                                            extra_writes = true;
                                            off += 1;
                                            continue;
                                        }
                                    }
                                    let lease = Lease {
                                        deadline_ms: now + visibility_ms as i64,
                                        delivery_count: prev.map(|l| l.delivery_count).unwrap_or(0)
                                            + 1,
                                        lease_gen: prev.map(|l| l.lease_gen).unwrap_or(0) + 1,
                                    };
                                    wb.put(lease_key(&hash, &consumer, off), encode_lease(&lease));
                                    extra_writes = true;
                                    cs.leases.insert(off, lease);
                                    leased.push((off, lease.lease_gen, lease.delivery_count));
                                    off += 1;
                                }
                                // Advance cursor over settled prefix.
                                while cs.acked.remove(&cs.cursor) {
                                    wb.delete(ack_key(&hash, &consumer, cs.cursor));
                                    cs.cursor += 1;
                                    extra_writes = true;
                                }
                                wb.put(
                                    cursor_key(&hash, &consumer),
                                    cs.cursor.to_le_bytes().to_vec(),
                                );
                                let backlog = (local.fields.next - cs.cursor)
                                    .saturating_sub(cs.acked.len() as u64);
                                (QueueOut::Received { leased, backlog }, dlq_subkey)
                            }
                            QueueOp::Settle {
                                consumer,
                                acks,
                                retries,
                                extends,
                                max_deliveries,
                                dlq_subkey,
                            } => {
                                let cs = st.queue.consumers.entry(consumer.clone()).or_default();
                                let (mut a, mut r, mut e2, mut dq) =
                                    (0usize, 0usize, 0usize, 0usize);
                                for (off, tok_gen) in acks {
                                    if cs.leases.get(&off).map(|l| l.lease_gen) == Some(tok_gen) {
                                        cs.leases.remove(&off);
                                        wb.delete(lease_key(&hash, &consumer, off));
                                        cs.acked.insert(off);
                                        wb.put(ack_key(&hash, &consumer, off), b"");
                                        extra_writes = true;
                                        a += 1;
                                    }
                                }
                                for (off, tok_gen, delay) in retries {
                                    if let Some(l) = cs.leases.get(&off).copied() {
                                        if l.lease_gen != tok_gen {
                                            continue;
                                        }
                                        if l.delivery_count >= max_deliveries {
                                            cs.leases.remove(&off);
                                            wb.delete(lease_key(&hash, &consumer, off));
                                            cs.acked.insert(off);
                                            wb.put(ack_key(&hash, &consumer, off), b"");
                                            dlq_refs.push((off, l.delivery_count));
                                            dq += 1;
                                        } else {
                                            let nl = Lease {
                                                deadline_ms: now + delay as i64,
                                                ..l
                                            };
                                            cs.leases.insert(off, nl);
                                            wb.put(
                                                lease_key(&hash, &consumer, off),
                                                encode_lease(&nl),
                                            );
                                            r += 1;
                                        }
                                        extra_writes = true;
                                    }
                                }
                                for (off, tok_gen, vis) in extends {
                                    if let Some(l) = cs.leases.get(&off).copied() {
                                        if l.lease_gen == tok_gen {
                                            let nl = Lease {
                                                deadline_ms: now + vis as i64,
                                                ..l
                                            };
                                            cs.leases.insert(off, nl);
                                            wb.put(
                                                lease_key(&hash, &consumer, off),
                                                encode_lease(&nl),
                                            );
                                            extra_writes = true;
                                            e2 += 1;
                                        }
                                    }
                                }
                                while cs.acked.remove(&cs.cursor) {
                                    wb.delete(ack_key(&hash, &consumer, cs.cursor));
                                    cs.cursor += 1;
                                    extra_writes = true;
                                }
                                wb.put(
                                    cursor_key(&hash, &consumer),
                                    cs.cursor.to_le_bytes().to_vec(),
                                );
                                let backlog = (local.fields.next - cs.cursor)
                                    .saturating_sub(cs.acked.len() as u64);
                                (
                                    QueueOut::Settled {
                                        acked: a,
                                        retried: r,
                                        extended: e2,
                                        dlq: dq,
                                        backlog,
                                    },
                                    dlq_subkey,
                                )
                            }
                        }
                    };
                    // Append DLQ reference records under routing key "$dlq".
                    for (orig, attempts) in dlq_refs {
                        let payload = format!("{{\"offset\":{orig},\"attempts\":{attempts}}}");
                        let offset = local.fields.next;
                        let frame = encrypt_frame(
                            &dlq_subkey,
                            &hash,
                            &FrameHeader {
                                offset,
                                ts_ms: now,
                                key_version: 0,
                                routing_key: "$dlq".to_string(),
                            },
                            payload.as_bytes(),
                        );
                        let frame = Bytes::from(frame);
                        if self.ring_enabled {
                            local.ring_recs.push((offset, frame.clone()));
                        }
                        wb.put(record_key(&hash, offset), frame);
                        local.fields.next += 1;
                        local.fields.logical += payload.len() as u64;
                        records += 1;
                    }
                    queue_pending.push((resp, out));
                }
            }
        }

        let mut tails = Vec::with_capacity(locals.len());
        let mut ring_pub: Vec<(Arc<StreamHandle>, Vec<(u64, Bytes)>)> = Vec::new();
        let mut signals = Vec::new();
        let mut changed = false;
        for (hash, local) in &locals {
            if !local.ring_recs.is_empty() {
                ring_pub.push((local.handle.clone(), local.ring_recs.clone()));
            }
            let f = &local.fields;
            let b = &local.base;
            if f.next != b.next
                || f.absorbed != b.absorbed
                || f.trimmed != b.trimmed
                || f.seq != b.seq
                || f.closed != b.closed
            {
                wb.put(tail_key(hash), encode_tail(f));
                // Dirty-stream index: marker present iff absorbed < next,
                // maintained atomically with the tail it describes.
                let was_dirty = b.absorbed < b.next;
                let is_dirty = f.absorbed < f.next;
                if is_dirty {
                    wb.put(dirty_key(hash), dirty_value(f.absorbed, f.next));
                } else if was_dirty {
                    wb.delete(dirty_key(hash));
                }
                changed = true;
            }
            tails.push((local.handle.clone(), f.clone()));
            if local.appended_bytes > 0 {
                signals.push(AbsorbSignal {
                    hash: *hash,
                    appended_bytes: local.appended_bytes,
                });
            }
        }
        if pending.is_empty() && !changed && queue_pending.is_empty() {
            return;
        }
        if !changed && records == 0 && touches.is_empty() && !extra_writes {
            // Nothing to persist (e.g. zero-entry append): nothing can move
            // the durable watermark either, so ACK directly.
            for (resp, ack) in pending {
                let _ = resp.send(Ok(ack));
            }
            for (resp, out) in queue_pending {
                let _ = resp.send(Ok(out));
            }
            return;
        }

        let encode_us = group_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
        let group_bytes: u64 = locals.iter().map(|(_, l)| l.appended_bytes).sum();
        let write_t0 = std::time::Instant::now();
        // Publish the write start so admission can observe a blocked commit
        // pipeline (L0-full / unflushed-full backpressure blocks this await;
        // 2026-07-21: an 8-minute block stranded every in-flight append into
        // the platform front door's 30 s kill). Cleared on completion.
        self.commit_write_started_ms
            .store(now_ms(), Ordering::SeqCst);
        let res = self
            .db
            .write_with_options(
                wb,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await;
        self.commit_write_started_ms.store(0, Ordering::SeqCst);
        let write_us = write_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;

        match res {
            Ok(handle) => {
                for (_, local) in &locals {
                    let mut st = local.handle.state.lock().unwrap();
                    st.applied = local.fields.clone();
                    for (id, v) in &local.producers {
                        st.producers.insert(id.clone(), *v);
                    }
                }
                self.stats_appended.fetch_add(records, Ordering::Relaxed);
                self.in_flight.lock().unwrap().push(InFlightGroup {
                    seq: handle.seqnum(),
                    written_at: std::time::Instant::now(),
                    queue_wait_us,
                    encode_us,
                    write_us,
                    reqs: group_reqs,
                    records_n: records as u32,
                    bytes: group_bytes,
                    acks: pending,
                    queue_acks: queue_pending,
                    tails,
                    ring_pub,
                    signals,
                    touches,
                });
                self.flush_wake.notify_one();
                self.pump_wake.notify_one();
            }
            Err(e) => {
                let msg = e.to_string();
                for (resp, _) in pending {
                    let _ = resp.send(Err(AppendErr::Internal(msg.clone())));
                }
                for (resp, _) in queue_pending {
                    let _ = resp.send(Err(msg.clone()));
                }
            }
        }
    }

    /// Publish one group's frames for one stream into its ring, then
    /// evict globally-oldest batches until the engine-wide budget is
    /// non-negative. FIFO mirrors publish order across streams, so its
    /// front IS the globally oldest batch.
    fn ring_publish(&self, handle: &Arc<StreamHandle>, recs: &[(u64, Bytes)]) {
        let bytes: usize = recs.iter().map(|(_, f)| f.len()).sum();
        let (first, next) = (recs[0].0, recs[recs.len() - 1].0 + 1);
        {
            let mut ring = handle.ring.lock().unwrap();
            // A shard handoff replays through a fresh engine, so within
            // one engine offsets only grow. If a gap somehow appears
            // (defensive: absorber trim races ahead), reset rather than
            // serve a hole.
            if ring.ceil().is_some_and(|c| c != first) {
                let dropped = ring.bytes;
                ring.batches.clear();
                ring.bytes = 0;
                self.ring_budget
                    .fetch_add(dropped as i64, Ordering::Relaxed);
                let mut fifo = self.ring_fifo.lock().unwrap();
                fifo.retain(|h| !Arc::ptr_eq(h, handle));
            }
            ring.batches.push_back(RingBatch {
                first,
                next,
                frames: recs.to_vec(),
                bytes,
            });
            ring.bytes += bytes;
        }
        self.ring_fifo.lock().unwrap().push_back(handle.clone());
        self.ring_published.fetch_add(1, Ordering::Relaxed);
        let after = self.ring_budget.fetch_sub(bytes as i64, Ordering::Relaxed) - bytes as i64;
        let resident = (self.ring_cfg_bytes as i64 - after).max(0) as u64;
        self.ring_peak_bytes.fetch_max(resident, Ordering::Relaxed);
        while self.ring_budget.load(Ordering::Relaxed) < 0 {
            let Some(victim) = self.ring_fifo.lock().unwrap().pop_front() else {
                break;
            };
            let mut ring = victim.ring.lock().unwrap();
            if let Some(b) = ring.batches.pop_front() {
                ring.bytes -= b.bytes;
                self.ring_budget.fetch_add(b.bytes as i64, Ordering::Relaxed);
                self.ring_evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn ring_resident_bytes(&self) -> u64 {
        (self.ring_cfg_bytes as i64 - self.ring_budget.load(Ordering::Relaxed)).max(0) as u64
    }

    /// Serve [scan_from, scan_to) from the stream's ring if the ring
    /// covers scan_from. Returns None when it does not (caller falls back
    /// to the canonical scan). Mirrors the DB path's contract exactly:
    /// stop at max_bytes, end = scan_to, last_offset = progress.
    pub fn ring_read(
        &self,
        handle: &StreamHandle,
        scan_from: u64,
        scan_to: u64,
        max_bytes: usize,
    ) -> Option<FrameReadResult> {
        if !self.ring_enabled || scan_from >= scan_to {
            return None;
        }
        let ring = handle.ring.lock().unwrap();
        let (Some(floor), Some(ceil)) = (ring.floor(), ring.ceil()) else {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            self.ring_miss_empty.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        // The ring can serve only what it contiguously holds. scan_to
        // beyond the ceiling means the caller knows about data the ring
        // has not been handed yet (possible mid-dispatch): DB path.
        if scan_from < floor || scan_to > ceil {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            if scan_from < floor {
                self.ring_miss_below_floor.fetch_add(1, Ordering::Relaxed);
            }
            if scan_to > ceil {
                self.ring_miss_above_ceil.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        let mut out = FrameReadResult {
            frames: Vec::new(),
            last_offset: None,
            end: scan_to,
        };
        let mut total = 0usize;
        for b in ring.batches.iter() {
            if b.next <= scan_from {
                continue;
            }
            if b.first >= scan_to {
                break;
            }
            for (off, f) in &b.frames {
                if *off < scan_from {
                    continue;
                }
                if *off >= scan_to {
                    break;
                }
                total += f.len();
                out.frames.push(f.clone());
                out.last_offset = Some(*off);
                if total >= max_bytes {
                    self.ring_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(out);
                }
            }
        }
        self.ring_hits.fetch_add(1, Ordering::Relaxed);
        Some(out)
    }

    /// Test hook: hold the dispatch gate. While held, NEITHER the acker
    /// nor the pump can dispatch acks — the deterministic stand-in for
    /// "the acker is paused after durability, before response dispatch".
    #[cfg(test)]
    pub fn test_hold_dispatch(&self) -> std::sync::MutexGuard<'_, ()> {
        self.dispatch_gate.lock().unwrap()
    }

    /// Release everything the durable watermark now covers: record
    /// timings, publish tail state, send producer/queue acks, feed the
    /// absorber and touch journals. Entirely synchronous, so the caller
    /// can rely on "when this returns, the acks are on their way" — the
    /// property the pump's gather window is built on. Returns requests
    /// dispatched. Called from the acker (watch-driven failsafe + the
    /// only path when the pump is off) and from the pump (explicit
    /// barrier right after its flush returns).
    fn dispatch_durable(&self, durable_seq: u64) -> u32 {
        let _order = self.dispatch_gate.lock().unwrap();
        let ready: Vec<InFlightGroup> = {
            let mut q = self.in_flight.lock().unwrap();
            let split = q.partition_point(|g| g.seq <= durable_seq);
            q.drain(..split).collect()
        };
        let mut dispatched = 0u32;
        for group in ready {
            dispatched += group.reqs;
            // Publish to the durable-tail ring FIRST: the ring ceiling
            // must already cover an offset by the time tail state (and
            // then an ack) makes that offset visible, or a reader woken
            // by the ack would miss the fast path — or worse, serve a
            // truncated range.
            for (handle, recs) in &group.ring_pub {
                self.ring_publish(handle, recs);
            }
            {
                let wait_us =
                    group.written_at.elapsed().as_micros().min(u32::MAX as u128) as u32;
                let mut t = self.timings.lock().unwrap();
                t.push_back(GroupTiming {
                    ts_ms: now_ms(),
                    queue_wait_us: group.queue_wait_us,
                    encode_us: group.encode_us,
                    write_us: group.write_us,
                    durable_wait_us: wait_us,
                    reqs: group.reqs,
                    records: group.records_n,
                    bytes: group.bytes,
                });
                if t.len() > 128 {
                    t.pop_front();
                }
            }
            for (handle, fields) in &group.tails {
                handle.state.lock().unwrap().durable = fields.clone();
                handle.notify.notify_waiters();
            }
            for (resp, ack) in group.acks {
                let _ = resp.send(Ok(ack));
            }
            for (resp, out) in group.queue_acks {
                let _ = resp.send(Ok(out));
            }
            for s in group.signals {
                let _ = self.absorb_tx.try_send(s);
            }
            // H2: feed touch journals only after the data is durable and
            // reader-visible, so an invalidation always finds fresh data.
            for t in group.touches {
                t.journal.ingest(&t.key_ids, t.next_offset);
            }
        }
        dispatched
    }

    async fn acker_loop(self: Arc<Self>) {
        let mut status_rx = self.db.subscribe();
        loop {
            let durable_seq = {
                let status = status_rx.borrow_and_update();
                if let Some(reason) = &status.close_reason {
                    // Fenced or closed: this shard moved. The process serves
                    // other shards. Fail every queued group NOW — waiting for
                    // Arc drops leaves clients hanging into the front door's
                    // 30 s kill (the absorber holds this engine, so the Arc
                    // may never drop). Touch waiters wake with stale.
                    tracing::error!(shard = %self.prefix, "shard db closed: {reason:?}");
                    self.closed.store(true, Ordering::SeqCst);
                    let _ = self.close_tx.send(true);
                    // Wake the group-commit pump so it observes closed and
                    // exits instead of parking on its Notify forever.
                    self.pump_wake.notify_one();
                    let stranded: Vec<InFlightGroup> =
                        self.in_flight.lock().unwrap().drain(..).collect();
                    for group in stranded {
                        for (resp, _) in group.acks {
                            let _ = resp.send(Err(AppendErr::Moved));
                        }
                        for (resp, _) in group.queue_acks {
                            let _ = resp.send(Err("shard fenced/moved; retry".into()));
                        }
                    }
                    if let Some(cb) = &self.on_close {
                        cb();
                    }
                    return;
                }
                status.durable_seq
            };
            self.dispatch_durable(durable_seq);
            tokio::select! {
                changed = status_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = self.flush_wake.notified() => {}
            }
        }
    }
}

/// Frames with offset in [scan_from, durable_next), optionally filtered by
/// routing key (frame metadata; no decryption needed).
pub struct FrameReadResult {
    pub frames: Vec<Bytes>,
    pub last_offset: Option<u64>,
    pub end: u64,
}

/// Range-bounded frame read: scans `[scan_from, scan_to)` regardless of the
/// durable frontier. Offsets below the frontier are dense, so disjoint
/// ranges partition the log exactly — the absorber issues several of these
/// concurrently to hide per-chunk object-store latency (a serial 8 MB chunk
/// loop absorbed ~10k rec/s against a 150k rec/s ingest; bench 2026-07-14).
pub async fn read_frames_range(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    scan_to: u64,
    max_bytes: usize,
) -> Result<FrameReadResult, slatedb::Error> {
    let hash = handle.hash;
    let mut out = FrameReadResult {
        frames: Vec::new(),
        last_offset: None,
        end: scan_to,
    };
    if scan_from >= scan_to {
        return Ok(out);
    }
    // Durable-tail fast path: live readers chase offsets the ring still
    // holds; the scan below is the canonical fallback (restart, eviction,
    // lagging consumers, ring off).
    if let Some(hit) = engine.ring_read(handle, scan_from, scan_to, max_bytes) {
        return Ok(hit);
    }
    let range = record_key(&hash, scan_from)..record_key(&hash, scan_to);
    let mut iter = engine
        .db
        .scan_with_options(
            range,
            &ScanOptions {
                durability_filter: DurabilityLevel::Remote,
                read_ahead_bytes: 2 * 1024 * 1024,
                max_fetch_tasks: 4,
                ..Default::default()
            },
        )
        .await?;
    let mut total = 0usize;
    while let Some(kv) = iter.next().await? {
        let off = u64::from_be_bytes(kv.key[17..25].try_into().expect("record key"));
        total += kv.value.len();
        out.frames.push(kv.value);
        out.last_offset = Some(off);
        if total >= max_bytes {
            break;
        }
    }
    Ok(out)
}

pub async fn read_frames(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> Result<FrameReadResult, slatedb::Error> {
    let (hash, end) = {
        let st = handle.state.lock().unwrap();
        (handle.hash, st.durable.next)
    };
    let mut out = FrameReadResult {
        frames: Vec::new(),
        last_offset: None,
        end,
    };
    if scan_from >= end {
        return Ok(out);
    }
    // Durable-tail fast path (see read_frames_range). Only for unfiltered
    // reads: a key_filter changes which frames belong in the result, and
    // the DB path applies it during the scan.
    if key_filter.is_none() {
        if let Some(hit) = engine.ring_read(handle, scan_from, end, max_bytes) {
            return Ok(hit);
        }
    }
    let range = record_key(&hash, scan_from)..record_key(&hash, end);
    let mut iter = engine
        .db
        .scan_with_options(
            range,
            &ScanOptions {
                durability_filter: DurabilityLevel::Remote,
                read_ahead_bytes: 2 * 1024 * 1024,
                max_fetch_tasks: 4,
                ..Default::default()
            },
        )
        .await?;
    let mut total = 0usize;
    while let Some(kv) = iter.next().await? {
        let off = u64::from_be_bytes(kv.key[17..25].try_into().expect("record key"));
        if let Some(kf) = key_filter {
            match decode_frame(&kv.value) {
                Some(f) if f.header.routing_key == kf => {}
                _ => {
                    out.last_offset = Some(off);
                    continue;
                }
            }
        }
        total += kv.value.len();
        out.frames.push(kv.value);
        out.last_offset = Some(off);
        if total >= max_bytes {
            break;
        }
    }
    Ok(out)
}
