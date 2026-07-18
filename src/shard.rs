//! Shard log engine: one SlateDB per shard, hash-first keyspace, committer +
//! durable-watermark acker (§3.4). Record values ARE the wire frames (§3.7):
//! encryption happens in the committer, after offset assignment, because the
//! nonce is the offset.
//!
//! Keyspace (hash-first so a hash range is one contiguous split range):
//!   <hash16> 't'                 tail state
//!   <hash16> 'r' <offset u64 BE> record frame

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::ObjectStoreExt;
use slatedb::config::{DurabilityLevel, FlushOptions, FlushType, ScanOptions, WriteOptions};
use slatedb::{CloseReason, Db, WriteBatch};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::crypto::{FrameHeader, decode_frame, encrypt_frame};
use crate::registry::StorageHash;

const RESIDENT_PRODUCER_CAPACITY: usize = 1_024;

pub fn tail_key(hash: &StorageHash) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(hash);
    k.push(b't');
    k
}

pub fn record_key(hash: &StorageHash, offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(41);
    k.extend_from_slice(hash);
    k.push(b'r');
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

/// Durable plaintext-byte debt for the history absorber. `a` sorts before
/// every other per-stream kind, allowing recovery to inspect one key per
/// storage hash and seek past arbitrarily large record ranges.
pub fn absorb_pending_key(hash: &StorageHash) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(hash);
    k.push(b'a');
    k
}

fn decode_absorb_pending(value: &[u8]) -> Option<u64> {
    let encoded: [u8; 8] = value.try_into().ok()?;
    Some(u64::from_le_bytes(encoded))
}

fn storage_hash_successor(mut hash: StorageHash) -> Option<StorageHash> {
    for byte in hash.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(hash);
        }
        *byte = 0;
    }
    None
}

/// Tail value v3:
/// [ver u8=3][next u64][last_ts i64][logical u64][absorbed u64][trimmed u64][flags u8][seq_len u16][seq]
fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let mut v = Vec::with_capacity(44 + seq.len());
    v.push(3);
    v.extend_from_slice(&t.next.to_le_bytes());
    v.extend_from_slice(&t.ts.to_le_bytes());
    v.extend_from_slice(&t.logical.to_le_bytes());
    v.extend_from_slice(&t.absorbed.to_le_bytes());
    v.extend_from_slice(&t.trimmed.to_le_bytes());
    v.push(if t.closed { 1 } else { 0 });
    v.extend_from_slice(&(seq.len() as u16).to_le_bytes());
    v.extend_from_slice(seq);
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
    let (closed, seq_at) = if v3 {
        (v[41] == 1, 42usize)
    } else {
        (false, 41usize)
    };
    let seq_len = u16::from_le_bytes(v[seq_at..seq_at + 2].try_into().ok()?) as usize;
    let seq = if seq_len == 0 {
        None
    } else {
        Some(String::from_utf8(v.get(seq_at + 2..seq_at + 2 + seq_len)?.to_vec()).ok()?)
    };
    Some(TailFields {
        next,
        ts,
        logical,
        absorbed,
        trimmed,
        seq,
        closed,
    })
}

pub fn producer_key(hash: &StorageHash, producer_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(33 + producer_id.len());
    k.extend_from_slice(hash);
    k.push(b'q');
    k.extend_from_slice(producer_id.as_bytes());
    k
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TailFields {
    pub next: u64,
    pub ts: i64,
    pub logical: u64,
    pub absorbed: u64,
    pub trimmed: u64,
    pub seq: Option<String>,
    pub closed: bool,
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub struct StreamState {
    pub durable: TailFields,
    pub applied: TailFields,
    /// Plaintext bytes whose record range is not yet durably represented by
    /// the history tier. Persisted separately under `absorb_pending_key`.
    pub durable_absorb_pending_bytes: u64,
    pub applied_absorb_pending_bytes: u64,
    /// Producer idempotence state: id -> (epoch, highest seq). Loaded from
    /// the durable `q` keys on first use, applied by the committer.
    pub producers: HashMap<String, (u64, u64)>,
    producer_order: VecDeque<String>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
}

impl StreamState {
    fn cache_producer(&mut self, id: String, value: (u64, u64)) {
        if !self.producers.contains_key(&id) {
            self.producer_order.push_back(id.clone());
        }
        self.producers.insert(id, value);
        while self.producers.len() > RESIDENT_PRODUCER_CAPACITY {
            let Some(oldest) = self.producer_order.pop_front() else {
                break;
            };
            self.producers.remove(&oldest);
        }
    }
}

pub struct StreamHandle {
    pub hash: StorageHash,
    pub state: Mutex<StreamState>,
    pub notify: Notify,
    visibility: Mutex<TailVisibility>,
}

struct TailVisibility {
    next: u64,
    committed_at: Option<std::time::Instant>,
}

impl StreamHandle {
    fn mark_visible(&self, next: u64) {
        let mut visibility = self.visibility.lock().unwrap();
        if next > visibility.next {
            visibility.next = next;
            visibility.committed_at = Some(std::time::Instant::now());
        }
    }

    pub fn tail_freshness(&self, delivered_next: u64) -> Option<std::time::Duration> {
        let visibility = self.visibility.lock().unwrap();
        (delivered_next >= visibility.next)
            .then(|| visibility.committed_at.map(|at| at.elapsed()))
            .flatten()
    }
}

struct StreamCache {
    map: HashMap<StorageHash, Arc<StreamHandle>>,
    order: VecDeque<StorageHash>,
    capacity: usize,
}

impl StreamCache {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn get(&self, hash: &StorageHash) -> Option<Arc<StreamHandle>> {
        self.map.get(hash).cloned()
    }

    fn insert(
        &mut self,
        hash: StorageHash,
        handle: Arc<StreamHandle>,
    ) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(existing) = self.map.get(&hash) {
            return Ok(existing.clone());
        }

        let candidates = self.order.len();
        for _ in 0..candidates {
            if self.map.len() < self.capacity {
                break;
            }
            let Some(candidate) = self.order.pop_front() else {
                break;
            };
            let evictable = self
                .map
                .get(&candidate)
                .map(|entry| {
                    Arc::strong_count(entry) == 1 && {
                        let state = entry.state.lock().unwrap();
                        state.applied == state.durable
                    }
                })
                .unwrap_or(true);
            if evictable {
                self.map.remove(&candidate);
            } else {
                self.order.push_back(candidate);
            }
        }
        if self.map.len() >= self.capacity {
            return Err(slatedb::Error::unavailable(
                "active stream handle capacity exhausted".to_string(),
            ));
        }
        self.map.insert(hash, handle.clone());
        self.order.push_back(hash);
        Ok(handle)
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
    pub customer_id: String,
    pub hash: StorageHash,
    /// Relative service share among this customer's streams (1..=100).
    pub fair_weight: u16,
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
    pub resp: oneshot::Sender<Result<AppendAck, AppendErr>>,
}

#[derive(Debug, Clone)]
pub struct AppendAck {
    pub next_offset: u64,
    pub closed: bool,
    /// Echoed on producer appends: (epoch, seq to report).
    pub producer: Option<(u64, u64)>,
    /// True for producer duplicates (204, body ignored).
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub enum AppendErr {
    SeqConflict { current: Option<String> },
    Closed { next_offset: u64 },
    ProducerGap { expected: u64, received: u64 },
    ProducerStale { current_epoch: u64 },
    ProducerEpochSeq,
    ShardMoved,
    Overloaded,
    CtMismatch,
    BadBody(String),
    Internal(String),
}

pub enum CommitOp {
    Append(AppendReq),
    /// Queue-profile state transition (PROFILES.md §7): serialized with
    /// appends, durable at the watermark like everything else.
    Queue {
        customer_id: String,
        hash: StorageHash,
        fair_weight: u16,
        op: crate::queue::QueueOp,
        resp: oneshot::Sender<Result<crate::queue::QueueOut, String>>,
    },
    /// Absorber confirmation: history tier now durably holds [.., upto).
    /// Advances the readers' boundary and trims previously-absorbed records
    /// (deferred one round so in-flight readers never lose their range).
    Absorbed {
        hash: StorageHash,
        upto: u64,
        absorbed_bytes: u64,
        resp: oneshot::Sender<Result<(), String>>,
    },
    /// Ordered after every operation admitted before a split quiescence gate.
    /// It never enters a WriteBatch; the committer resolves it only after its
    /// fair backlog and every prior remote-durability ACK group are empty.
    Barrier {
        resp: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum FairKey {
    Tenant(String),
    Internal,
}

/// Persistent bounded look-ahead over the already-bounded ingress channel.
/// One operation per active tenant is selected per round, preserving each
/// tenant's FIFO order. Byte admission is still enforced at batch assembly;
/// a request larger than the remaining budget waits for the next group.
#[derive(Default)]
struct FairCommitQueue {
    queues: HashMap<FairKey, TenantFairQueue>,
    active: VecDeque<FairKey>,
    len: usize,
}

struct StreamFairQueue {
    ops: VecDeque<CommitOp>,
    weight: u16,
    credits: u16,
}

#[derive(Default)]
struct TenantFairQueue {
    streams: HashMap<StorageHash, StreamFairQueue>,
    active: VecDeque<StorageHash>,
}

impl FairCommitQueue {
    fn key(op: &CommitOp) -> FairKey {
        match op {
            CommitOp::Append(request) => FairKey::Tenant(request.customer_id.clone()),
            CommitOp::Queue { customer_id, .. } => FairKey::Tenant(customer_id.clone()),
            CommitOp::Absorbed { .. } | CommitOp::Barrier { .. } => FairKey::Internal,
        }
    }

    fn stream_key(op: &CommitOp) -> StorageHash {
        match op {
            CommitOp::Append(request) => request.hash,
            CommitOp::Queue { hash, .. } | CommitOp::Absorbed { hash, .. } => *hash,
            CommitOp::Barrier { .. } => [0; 32],
        }
    }

    fn weight(op: &CommitOp) -> u16 {
        match op {
            CommitOp::Append(request) => request.fair_weight.clamp(1, 100),
            CommitOp::Queue { fair_weight, .. } => (*fair_weight).clamp(1, 100),
            CommitOp::Absorbed { .. } | CommitOp::Barrier { .. } => 1,
        }
    }

    fn bytes(op: &CommitOp) -> usize {
        match op {
            CommitOp::Append(request) => request.bytes,
            _ => 0,
        }
    }

    fn push(&mut self, op: CommitOp) {
        let key = Self::key(&op);
        let stream_key = Self::stream_key(&op);
        let weight = Self::weight(&op);
        let tenant_is_new = !self.queues.contains_key(&key);
        let tenant = self.queues.entry(key.clone()).or_default();
        let stream_is_new = !tenant.streams.contains_key(&stream_key);
        let stream = tenant
            .streams
            .entry(stream_key)
            .or_insert_with(|| StreamFairQueue {
                ops: VecDeque::new(),
                weight,
                credits: 0,
            });
        // Descriptor configuration is immutable for one incarnation. Using
        // max is fail-safe for a queue operation racing the first append of a
        // weighted queue-profile stream.
        stream.weight = stream.weight.max(weight);
        stream.ops.push_back(op);
        if stream_is_new {
            tenant.active.push_back(stream_key);
        }
        if tenant_is_new {
            self.active.push_back(key);
        }
        self.len += 1;
    }

    fn pop_batch(&mut self, max_reqs: usize, max_bytes: usize) -> Vec<CommitOp> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut skipped = 0usize;
        while out.len() < max_reqs && self.len > 0 {
            let Some(key) = self.active.pop_front() else {
                break;
            };
            let (stream_key, op_bytes) = {
                let tenant = self.queues.get_mut(&key).expect("active tenant queue");
                let stream_key = tenant.active.pop_front().expect("active stream queue");
                let stream = tenant
                    .streams
                    .get_mut(&stream_key)
                    .expect("active stream state");
                if stream.credits == 0 {
                    stream.credits = stream.weight;
                }
                let op_bytes = stream.ops.front().map(Self::bytes).unwrap_or(0);
                (stream_key, op_bytes)
            };
            if !out.is_empty() && bytes.saturating_add(op_bytes) > max_bytes {
                let tenant = self.queues.get_mut(&key).expect("active tenant queue");
                let stream = tenant
                    .streams
                    .get_mut(&stream_key)
                    .expect("active stream state");
                // A large head must not burn all weighted credits while it is
                // waiting for the next byte budget.
                stream.credits = 0;
                tenant.active.push_back(stream_key);
                self.active.push_back(key);
                skipped += 1;
                if skipped >= self.len {
                    break;
                }
                continue;
            }
            skipped = 0;
            let (op, tenant_empty) = {
                let tenant = self.queues.get_mut(&key).expect("active tenant queue");
                let stream = tenant
                    .streams
                    .get_mut(&stream_key)
                    .expect("active stream state");
                let op = stream.ops.pop_front().expect("non-empty fair stream");
                stream.credits = stream.credits.saturating_sub(1);
                if stream.ops.is_empty() {
                    tenant.streams.remove(&stream_key);
                } else if stream.credits > 0 {
                    tenant.active.push_front(stream_key);
                } else {
                    tenant.active.push_back(stream_key);
                }
                (op, tenant.active.is_empty())
            };
            if tenant_empty {
                self.queues.remove(&key);
            } else {
                self.active.push_back(key);
            }
            self.len -= 1;
            bytes = bytes.saturating_add(op_bytes);
            out.push(op);
        }
        out
    }

    fn fail_all(&mut self) {
        for tenant in self.queues.values_mut() {
            for stream in tenant.streams.values_mut() {
                while let Some(op) = stream.ops.pop_front() {
                    fail_commit_op(op);
                }
            }
        }
        self.queues.clear();
        self.active.clear();
        self.len = 0;
    }
}

/// Notification to the absorber that a stream accumulated shard-log bytes.
#[derive(Debug, Clone)]
pub struct AbsorbSignal {
    pub hash: StorageHash,
    pub appended_bytes: u64,
}

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
    /// Hard cap on resident stream state per shard process. Idle, fully
    /// durable handles are evicted; if every handle is active, callers get
    /// an explicit retryable overload instead of an unbounded allocation.
    pub max_stream_handles: usize,
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
            max_stream_handles: 20_000,
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
    absorbed_acks: Vec<oneshot::Sender<Result<(), String>>>,
    tails: Vec<(Arc<StreamHandle>, TailFields, u64)>,
    signals: Vec<AbsorbSignal>,
    touches: Vec<TouchFeed>,
}

pub struct ShardEngine {
    pub prefix: String,
    pub db: Arc<Db>,
    streams: Mutex<StreamCache>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<Vec<InFlightGroup>>,
    flush_wake: Notify,
    in_flight_empty: Notify,
    shutdown: Notify,
    closed: AtomicBool,
    accepting: AtomicBool,
    fence_recorded: AtomicBool,
    admission_gate: Mutex<()>,
    quiesce_gate: tokio::sync::Mutex<()>,
    writer_epoch: u64,
    last_owner_proof: Mutex<std::time::Instant>,
    owner_proof_lock: tokio::sync::Mutex<()>,
    absorb_tx: mpsc::Sender<AbsorbSignal>,
    telemetry: Arc<crate::telemetry::Telemetry>,
    /// Strong reconfiguration fence checked after remote durability but before ACKs.
    /// Without this, a stale ring owner can acknowledge a parent write after
    /// another process cloned it into children.
    reconfiguration_fence_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// Invoked when the shard db closes (fenced by a new owner / fatal):
    /// wired to TouchRegistry::close_shard so hanging /touch/wait clients
    /// get stale immediately instead of dangling until timeout.
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    pub stats_appended: AtomicU64,
    /// Plaintext payload bytes committed since this engine opened. Used as a
    /// monotonic input to the sustained hot-shard split trigger.
    pub stats_appended_bytes: AtomicU64,
    /// Last commit-group timings for /v1/debug/timings.
    pub timings: Mutex<std::collections::VecDeque<GroupTiming>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueError {
    Full,
    ShardMoved,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn union_flush_sentinel(prefix: &str) -> Result<Vec<u8>, String> {
    if prefix.is_empty()
        || prefix.len() > 128
        || !prefix.bytes().all(|bit| bit == b'0' || bit == b'1')
    {
        return Err("union source must have a non-empty binary shard prefix".to_string());
    }
    let mut value = 0u128;
    for bit in prefix.bytes() {
        value = (value << 1) | u128::from(bit == b'1');
    }
    let lower = value << (128 - prefix.len());
    let mut key = Vec::with_capacity(17);
    key.extend_from_slice(&lower.to_be_bytes());
    // Every service key is at least routing-hash16 + incarnation16 + kind1.
    // A 17-byte key is therefore permanently outside the application grammar.
    key.push(0xff);
    Ok(key)
}

impl ShardEngine {
    /// Enumerate durable history debt before the shard starts accepting new
    /// writes. The seek skips every remaining key for a storage hash, so the
    /// cost is proportional to streams rather than retained records.
    ///
    /// Older shard logs have no `a` marker. Their cumulative logical byte
    /// count is a conservative recovery estimate; the first successful pass
    /// reconciles it to the exact absorbed frontier.
    pub async fn recover_pending_absorptions(db: &Db) -> Result<Vec<AbsorbSignal>, slatedb::Error> {
        const MAX_RECOVERED_STREAMS: usize = 100_000;

        let mut iter = db.scan(..).await?;
        let mut recovered = Vec::new();
        while let Some(kv) = iter.next().await? {
            // Projection flush sentinels and future shard metadata are not
            // application keys. Every current stream key starts with the
            // complete 32-byte storage hash.
            if kv.key.len() < 32 {
                continue;
            }
            let hash: StorageHash = kv.key[..32]
                .try_into()
                .expect("length checked storage hash");
            let marker = if kv.key.len() == 33 && kv.key[32] == b'a' {
                Some(decode_absorb_pending(&kv.value).ok_or_else(|| {
                    slatedb::Error::data("corrupt absorber pending-byte marker".to_string())
                })?)
            } else {
                None
            };
            let tail = match db.get(tail_key(&hash)).await? {
                Some(raw) => Some(decode_tail(&raw).ok_or_else(|| {
                    slatedb::Error::data("corrupt stream tail during absorber recovery".to_string())
                })?),
                None => None,
            };
            let pending_bytes = match (marker, tail) {
                (Some(bytes), Some(tail)) if bytes > 0 || tail.absorbed < tail.next => bytes.max(1),
                // A marker left behind after the frontier advanced must be
                // scheduled too: the idempotent absorbed op deletes it.
                (Some(bytes), None) if bytes > 0 => bytes,
                (None, Some(tail)) if tail.absorbed < tail.next => tail.logical.max(1),
                _ => 0,
            };
            if pending_bytes > 0 {
                if recovered.len() == MAX_RECOVERED_STREAMS {
                    return Err(slatedb::Error::data(format!(
                        "absorber recovery exceeds bounded scheduler capacity ({MAX_RECOVERED_STREAMS} streams)"
                    )));
                }
                recovered.push(AbsorbSignal {
                    hash,
                    appended_bytes: pending_bytes,
                });
            }
            let Some(next_hash) = storage_hash_successor(hash) else {
                break;
            };
            iter.seek(next_hash).await?;
        }
        Ok(recovered)
    }

    /// Manifest writer incarnation for telemetry/control-plane sampling.
    /// A reopen changes this value, so rate observers can distinguish a
    /// counter reset from an actually idle shard.
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    pub fn start(
        prefix: String,
        db: Arc<Db>,
        cfg: ShardConfig,
        absorb_tx: mpsc::Sender<AbsorbSignal>,
        telemetry: Arc<crate::telemetry::Telemetry>,
        on_close: Option<Arc<dyn Fn() + Send + Sync>>,
        reconfiguration_fence_store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Arc<ShardEngine> {
        let (tx, rx) = mpsc::channel(cfg.queue_reqs);
        let writer_epoch = db.status().current_manifest.writer_epoch();
        let engine = Arc::new(ShardEngine {
            prefix,
            db,
            streams: Mutex::new(StreamCache::new(cfg.max_stream_handles)),
            tx,
            in_flight: Mutex::new(Vec::new()),
            flush_wake: Notify::new(),
            in_flight_empty: Notify::new(),
            shutdown: Notify::new(),
            closed: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            fence_recorded: AtomicBool::new(false),
            admission_gate: Mutex::new(()),
            quiesce_gate: tokio::sync::Mutex::new(()),
            writer_epoch,
            last_owner_proof: Mutex::new(std::time::Instant::now()),
            owner_proof_lock: tokio::sync::Mutex::new(()),
            absorb_tx,
            telemetry,
            reconfiguration_fence_store,
            on_close,
            stats_appended: AtomicU64::new(0),
            stats_appended_bytes: AtomicU64::new(0),
            timings: Mutex::new(std::collections::VecDeque::new()),
        });
        let committer = engine.clone();
        tokio::spawn(async move { committer.committer_loop(rx, cfg).await });
        let acker = engine.clone();
        tokio::spawn(async move { acker.acker_loop().await });
        // F1 recovery bound: `max_wal_flushes_before_l0_flush` has a 4096
        // upstream floor, so we cap the WAL replay window ourselves with a
        // periodic explicit memtable->L0 flush whenever data accumulated.
        let ticker = engine.clone();
        tokio::spawn(async move {
            use slatedb::config::{FlushOptions, FlushType};
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut last_appended = 0u64;
            loop {
                interval.tick().await;
                if ticker.closed.load(Ordering::Acquire) {
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
        });
        engine
    }

    pub fn try_enqueue(&self, req: AppendReq) -> Result<(), EnqueueError> {
        let _gate = self.admission_gate.lock().unwrap();
        if self.closed.load(Ordering::Acquire) || !self.accepting.load(Ordering::Acquire) {
            return Err(EnqueueError::ShardMoved);
        }
        self.tx
            .try_send(CommitOp::Append(req))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(CommitOp::Append(_)) => EnqueueError::Full,
                mpsc::error::TrySendError::Closed(CommitOp::Append(_)) => EnqueueError::ShardMoved,
                _ => unreachable!(),
            })
    }

    pub fn queue_limit(&self) -> usize {
        self.tx.max_capacity()
    }

    /// L8 stale-owner read guard. A recent remotely durable commit proves
    /// this writer epoch still owns the manifest. Idle shards force an
    /// immediate remote manifest refresh at least every five seconds and
    /// reject reads if another writer has claimed a higher epoch.
    pub async fn prove_ownership(&self) -> Result<(), slatedb::Error> {
        const MAX_PROOF_AGE: std::time::Duration = std::time::Duration::from_secs(5);
        if self.closed.load(Ordering::Acquire) {
            return Err(slatedb::Error::closed(
                "shard is no longer owned".to_string(),
                slatedb::CloseReason::Fenced,
            ));
        }
        if self.last_owner_proof.lock().unwrap().elapsed() <= MAX_PROOF_AGE {
            return Ok(());
        }
        let _proof_guard = self.owner_proof_lock.lock().await;
        if self.last_owner_proof.lock().unwrap().elapsed() <= MAX_PROOF_AGE {
            return Ok(());
        }
        self.db.refresh_manifest().await?;
        let observed = self.db.status().current_manifest.writer_epoch();
        if observed != self.writer_epoch {
            self.record_fence_once(crate::telemetry::FenceKind::Writer);
            self.mark_moved();
            return Err(slatedb::Error::closed(
                format!(
                    "writer epoch changed from {} to {observed}",
                    self.writer_epoch
                ),
                slatedb::CloseReason::Fenced,
            ));
        }
        *self.last_owner_proof.lock().unwrap() = std::time::Instant::now();
        Ok(())
    }

    fn record_fence_once(&self, kind: crate::telemetry::FenceKind) {
        if !self.fence_recorded.swap(true, Ordering::AcqRel) {
            self.telemetry.record_fence(kind);
        }
    }

    fn mark_moved(&self) {
        let _gate = self.admission_gate.lock().unwrap();
        self.accepting.store(false, Ordering::Release);
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shutdown.notify_one();
        self.flush_wake.notify_waiters();
        let groups = std::mem::take(&mut *self.in_flight.lock().unwrap());
        for group in groups {
            for (resp, _) in group.acks {
                let _ = resp.send(Err(AppendErr::ShardMoved));
            }
            for (resp, _) in group.queue_acks {
                let _ = resp.send(Err("shard moved".to_string()));
            }
            for resp in group.absorbed_acks {
                let _ = resp.send(Err("shard moved".to_string()));
            }
        }
        self.in_flight_empty.notify_waiters();
        if let Some(callback) = &self.on_close {
            callback();
        }
    }

    /// Retire a cached parent/child after a last-known-good topology update.
    /// This uses the same fail-fast path as writer fencing: no request may
    /// continue through a DB whose projection is no longer in the live trie.
    pub fn retire(&self) {
        self.mark_moved();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn reconfiguration_fence_exists(&self) -> Result<bool, String> {
        let Some(store) = &self.reconfiguration_fence_store else {
            return Ok(false);
        };
        let path = crate::reconfiguration::fence_path(&self.prefix);
        match store.get(&path).await {
            Ok(result) => {
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                match crate::reconfiguration::decode_fence(&raw)? {
                    crate::reconfiguration::FenceDocument::Released(_)
                    | crate::reconfiguration::FenceDocument::ReleasedSplit(_) => Ok(false),
                    crate::reconfiguration::FenceDocument::Split
                    | crate::reconfiguration::FenceDocument::Merge(_) => Ok(true),
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    pub async fn submit_absorbed(&self, hash: StorageHash, upto: u64, absorbed_bytes: u64) -> bool {
        let (resp, ack) = oneshot::channel();
        let gate = self.admission_gate.lock().unwrap();
        if self.closed.load(Ordering::Acquire) || !self.accepting.load(Ordering::Acquire) {
            return false;
        }
        let queued = self
            .tx
            .try_send(CommitOp::Absorbed {
                hash,
                upto,
                absorbed_bytes,
                resp,
            })
            .is_ok();
        drop(gate);
        if !queued {
            return false;
        }
        matches!(
            tokio::time::timeout(std::time::Duration::from_secs(30), ack).await,
            Ok(Ok(Ok(())))
        )
    }

    pub async fn submit_queue(
        &self,
        customer_id: String,
        hash: StorageHash,
        fair_weight: u16,
        op: crate::queue::QueueOp,
    ) -> Result<crate::queue::QueueOut, String> {
        let gate = self.admission_gate.lock().unwrap();
        if self.closed.load(Ordering::Acquire) || !self.accepting.load(Ordering::Acquire) {
            return Err("shard moved".to_string());
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(CommitOp::Queue {
                customer_id,
                hash,
                fair_weight,
                op,
                resp: tx,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => "committer overloaded".to_string(),
                mpsc::error::TrySendError::Closed(_) => "committer gone".to_string(),
            })?;
        drop(gate);
        rx.await
            .map_err(|_| "committer dropped request".to_string())?
    }

    /// Stop admitting operations, order a barrier after every operation that
    /// won the admission mutex, wait for their remote durable ACK groups, and
    /// flush/close the parent DB. After success the caller may safely create
    /// projection clones. This operation is one-way by design.
    pub async fn quiesce_for_split(&self) -> Result<(), String> {
        self.quiesce_for_reconfiguration(false).await
    }

    /// Merge sources must have no replayable data WAL. A projected child can
    /// contain only out-of-range rows in an inherited WAL, leaving its
    /// memtable empty and preventing SlateDB from advancing the replay
    /// watermark. A reserved, invalid-for-the-service tombstone makes that
    /// progress explicit without adding a readable application key.
    pub async fn quiesce_for_union(&self) -> Result<(), String> {
        self.quiesce_for_reconfiguration(true).await
    }

    async fn quiesce_for_reconfiguration(&self, union_source: bool) -> Result<(), String> {
        let _quiesce = self.quiesce_gate.lock().await;
        {
            let _gate = self.admission_gate.lock().unwrap();
            if self.closed.load(Ordering::Acquire) {
                return Err("shard already closed".to_string());
            }
            if !self.accepting.swap(false, Ordering::AcqRel) {
                return Err("shard already quiescing".to_string());
            }
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CommitOp::Barrier { resp: tx })
            .await
            .map_err(|_| "committer closed before split barrier".to_string())?;
        tokio::time::timeout(std::time::Duration::from_secs(30), rx)
            .await
            .map_err(|_| "split durability barrier timed out".to_string())?
            .map_err(|_| "split durability barrier responder dropped".to_string())??;
        if union_source {
            let key = union_flush_sentinel(&self.prefix)?;
            self.db
                .delete_with_options(&key, &WriteOptions::default())
                .await
                .map_err(|error| format!("write union flush sentinel: {error}"))?;
        }
        // Flush WAL first, then freeze/flush the memtable. SlateDB's memtable
        // flush snapshots `recent_flushed_wal_id` before its internally
        // requested WAL flush completes; doing these as two ordered calls is
        // what advances replay_after_wal_id through the final data WAL.
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .map_err(|error| format!("flush quiesced shard WAL: {error}"))?;
        // A projected clone can inherit a replayable data WAL even if this
        // child never receives a post-split write. Multi-source SlateDB union
        // deliberately rejects data WALs, so every reconfiguration barrier
        // materializes replayed + newly committed state into L0.
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(|error| format!("flush quiesced shard to L0: {error}"))?;
        self.db
            .close()
            .await
            .map_err(|error| format!("close quiesced shard: {error}"))?;
        self.mark_moved();
        Ok(())
    }

    pub async fn stream_handle(
        &self,
        hash: StorageHash,
    ) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(handle) = self.streams.lock().unwrap().get(&hash) {
            return Ok(handle);
        }
        let tail = match self.db.get(tail_key(&hash)).await? {
            Some(raw) => decode_tail(&raw).unwrap_or_default(),
            None => TailFields::default(),
        };
        let absorb_pending_bytes = match self.db.get(absorb_pending_key(&hash)).await? {
            Some(raw) => decode_absorb_pending(&raw).ok_or_else(|| {
                slatedb::Error::data("corrupt absorber pending-byte marker".to_string())
            })?,
            None => 0,
        };
        let visible_next = tail.next;
        let handle = Arc::new(StreamHandle {
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                durable_absorb_pending_bytes: absorb_pending_bytes,
                applied_absorb_pending_bytes: absorb_pending_bytes,
                producers: HashMap::new(),
                producer_order: VecDeque::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            visibility: Mutex::new(TailVisibility {
                next: visible_next,
                committed_at: None,
            }),
        });
        self.streams.lock().unwrap().insert(hash, handle)
    }

    async fn committer_loop(self: Arc<Self>, mut rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {
        let mut fair = FairCommitQueue::default();
        let mut barrier: Option<oneshot::Sender<Result<(), String>>> = None;
        loop {
            if self.closed.load(Ordering::Acquire) {
                fair.fail_all();
                fail_queued_ops(&mut rx);
                return;
            }
            if fair.len == 0 {
                if let Some(resp) = barrier.take() {
                    loop {
                        let notified = self.in_flight_empty.notified();
                        if self.in_flight.lock().unwrap().is_empty() {
                            break;
                        }
                        notified.await;
                    }
                    let result = self
                        .db
                        .flush()
                        .await
                        .map_err(|error| format!("flush split barrier: {error}"));
                    let _ = resp.send(result);
                    return;
                }
                let first = tokio::select! {
                    biased;
                    _ = self.shutdown.notified() => {
                        fail_queued_ops(&mut rx);
                        return;
                    }
                    op = rx.recv() => op,
                };
                let Some(first) = first else { return };
                match first {
                    CommitOp::Barrier { resp } => {
                        barrier = Some(resp);
                        continue;
                    }
                    op => fair.push(op),
                }
            }
            // Look ahead beyond the byte limit so a queue of large requests
            // from one tenant cannot hide a small request from another.
            while fair.len < cfg.max_batch_reqs && barrier.is_none() {
                match rx.try_recv() {
                    Ok(CommitOp::Barrier { resp }) => {
                        barrier = Some(resp);
                        break;
                    }
                    Ok(op) => fair.push(op),
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
            if barrier.is_none() && fair.len >= cfg.pace_min_reqs && fair.len < cfg.max_batch_reqs {
                let deadline = tokio::time::Instant::now() + cfg.gather_window;
                loop {
                    if fair.len >= cfg.max_batch_reqs {
                        break;
                    }
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(CommitOp::Barrier { resp })) => {
                            barrier = Some(resp);
                            break;
                        }
                        Ok(Some(op)) => {
                            fair.push(op);
                            if self.closed.load(Ordering::Acquire) {
                                fair.fail_all();
                                fail_queued_ops(&mut rx);
                                return;
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            let ops = fair.pop_batch(cfg.max_batch_reqs, cfg.max_batch_bytes);
            if self.closed.load(Ordering::Acquire) {
                fair.fail_all();
                for op in ops {
                    fail_commit_op(op);
                }
                fail_queued_ops(&mut rx);
                return;
            }
            self.commit_group(ops, &cfg).await;
        }
    }

    async fn commit_group(&self, ops: Vec<CommitOp>, cfg: &ShardConfig) {
        let group_t0 = std::time::Instant::now();
        let mut oldest_enqueue: Option<std::time::Instant> = None;
        for op in &ops {
            if let CommitOp::Append(r) = op
                && oldest_enqueue.map(|o| r.enqueued_at < o).unwrap_or(true)
            {
                oldest_enqueue = Some(r.enqueued_at);
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
            absorb_pending_bytes: u64,
            base_absorb_pending_bytes: u64,
            producers: HashMap<String, (u64, u64)>,
            queue: Option<crate::queue::QueueState>,
            appended_bytes: u64,
        }

        let mut wb = WriteBatch::new();
        let mut pending: Vec<(oneshot::Sender<Result<AppendAck, AppendErr>>, AppendAck)> =
            Vec::new();
        let mut locals: HashMap<StorageHash, Local> = HashMap::new();
        let mut records = 0u64;
        let mut touches: Vec<TouchFeed> = Vec::new();
        let mut queue_pending: Vec<(
            oneshot::Sender<Result<crate::queue::QueueOut, String>>,
            crate::queue::QueueOut,
        )> = Vec::new();
        let mut absorbed_pending: Vec<oneshot::Sender<Result<(), String>>> = Vec::new();
        let mut extra_writes = false;

        for op in ops {
            let hash = match &op {
                CommitOp::Append(r) => r.hash,
                CommitOp::Absorbed { hash, .. } => *hash,
                CommitOp::Queue { hash, .. } => *hash,
                CommitOp::Barrier { .. } => unreachable!("barrier enters commit group"),
            };
            // The handle load is async, so holding a HashMap Entry across it
            // would make the control flow and future lifetime worse.
            #[allow(clippy::map_entry)]
            if !locals.contains_key(&hash) {
                match self.stream_handle(hash).await {
                    Ok(handle) => {
                        let (applied, absorb_pending_bytes) = {
                            let state = handle.state.lock().unwrap();
                            (state.applied.clone(), state.applied_absorb_pending_bytes)
                        };
                        locals.insert(
                            hash,
                            Local {
                                handle,
                                fields: applied.clone(),
                                base: applied,
                                absorb_pending_bytes,
                                base_absorb_pending_bytes: absorb_pending_bytes,
                                producers: HashMap::new(),
                                queue: None,
                                appended_bytes: 0,
                            },
                        );
                    }
                    Err(e) => {
                        match op {
                            CommitOp::Append(r) => {
                                let error = if e.kind() == slatedb::ErrorKind::Unavailable {
                                    AppendErr::Overloaded
                                } else {
                                    AppendErr::Internal(e.to_string())
                                };
                                let _ = r.resp.send(Err(error));
                            }
                            CommitOp::Absorbed { resp, .. } => {
                                let _ = resp.send(Err(e.to_string()));
                            }
                            CommitOp::Queue { resp, .. } => {
                                let _ = resp.send(Err(e.to_string()));
                            }
                            CommitOp::Barrier { .. } => unreachable!(),
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
                    if let Some(pr) = &req.producer
                        && !local.producers.contains_key(&pr.id)
                    {
                        let shared = {
                            let st = local.handle.state.lock().unwrap();
                            st.producers.get(&pr.id).copied()
                        };
                        let loaded = match shared {
                            Some(v) => Some(v),
                            None => match self.db.get(producer_key(&hash, &pr.id)).await {
                                Ok(Some(v)) if v.len() >= 16 => Some((
                                    u64::from_le_bytes(v[0..8].try_into().unwrap()),
                                    u64::from_le_bytes(v[8..16].try_into().unwrap()),
                                )),
                                Ok(_) => None,
                                Err(e) => {
                                    let _ = req.resp.send(Err(AppendErr::Internal(e.to_string())));
                                    continue;
                                }
                            },
                        };
                        if let Some(v) = loaded {
                            local.producers.insert(pr.id.clone(), v);
                        }
                    }
                    // Contract check order: stale epoch (403) -> duplicate
                    // (204, before everything below) -> epoch/seq rules ->
                    // gap (409) -> closed (409) -> deferred ct/body errors ->
                    // Stream-Seq -> append.
                    let mut prod_echo: Option<(u64, u64)> = None;
                    if let Some(pr) = &req.producer {
                        match local.producers.get(&pr.id).copied() {
                            Some((ce, cs)) => {
                                if pr.epoch < ce {
                                    let _ = req
                                        .resp
                                        .send(Err(AppendErr::ProducerStale { current_epoch: ce }));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq <= cs {
                                    let _ = req.resp.send(Ok(AppendAck {
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
                    if let Some(seq) = &req.seq
                        && let Some(cur) = &local.fields.seq
                        && seq <= cur
                    {
                        let _ = req.resp.send(Err(AppendErr::SeqConflict {
                            current: Some(cur.clone()),
                        }));
                        continue;
                    }
                    // Accept: stage producer + close + records.
                    if let Some(pr) = &req.producer {
                        local.producers.insert(pr.id.clone(), (pr.epoch, pr.seq));
                        let mut v = Vec::with_capacity(16);
                        v.extend_from_slice(&pr.epoch.to_le_bytes());
                        v.extend_from_slice(&pr.seq.to_le_bytes());
                        wb.put(producer_key(&hash, &pr.id), v);
                    }
                    if req.close {
                        local.fields.closed = true;
                    }
                    if req.entries.is_empty() {
                        pending.push((
                            req.resp,
                            AppendAck {
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
                        wb.put(record_key(&hash, offset), frame);
                        local.fields.logical += payload.len() as u64;
                        local.appended_bytes += payload.len() as u64;
                        local.absorb_pending_bytes = local
                            .absorb_pending_bytes
                            .saturating_add(payload.len() as u64);
                    }
                    records += req.entries.len() as u64;
                    local.fields.next = start + req.entries.len() as u64;
                    local.fields.ts = ts;
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
                            next_offset: local.fields.next,
                            closed: local.fields.closed,
                            producer: prod_echo,
                            duplicate: false,
                        },
                    ));
                }
                CommitOp::Absorbed {
                    upto,
                    absorbed_bytes,
                    resp,
                    ..
                } => {
                    let prev_absorbed = local.fields.absorbed;
                    if upto > prev_absorbed {
                        local.fields.absorbed = upto.min(local.fields.next);
                        local.absorb_pending_bytes =
                            local.absorb_pending_bytes.saturating_sub(absorbed_bytes);
                    } else if local.fields.absorbed >= local.fields.next {
                        // Reconcile a marker left by an older binary or an
                        // ACK whose response was lost after remote durability.
                        local.absorb_pending_bytes = 0;
                    }
                    // Deferred trim: delete only up to the *previous* absorbed
                    // boundary, bounded per op.
                    let trim_to = prev_absorbed.min(local.fields.trimmed + cfg.max_trim_per_op);
                    for off in local.fields.trimmed..trim_to {
                        wb.delete(record_key(&hash, off));
                    }
                    local.fields.trimmed = trim_to;
                    absorbed_pending.push(resp);
                }
                CommitOp::Queue { op, resp, .. } => {
                    use crate::queue::*;
                    // Load only the addressed consumer. Loading every consumer
                    // in a stream lets one tenant force an unbounded resident
                    // map and makes cold access proportional to total history.
                    if local.queue.is_none() {
                        local.queue = Some(local.handle.state.lock().unwrap().queue.clone());
                    }
                    let consumer = match &op {
                        QueueOp::Receive { consumer, .. } | QueueOp::Settle { consumer, .. } => {
                            consumer.clone()
                        }
                    };
                    let mut load_err: Option<String> = None;
                    if !local.queue.as_ref().unwrap().loaded.contains(&consumer) {
                        let mut fresh = ConsumerState::default();
                        match self.db.get(cursor_key(&hash, &consumer)).await {
                            Ok(Some(value)) if value.len() == 8 => {
                                fresh.cursor = u64::from_le_bytes(value[..8].try_into().unwrap());
                            }
                            Ok(Some(_)) => {
                                load_err = Some("corrupt queue cursor".to_string());
                            }
                            Ok(None) => {}
                            Err(error) => load_err = Some(error.to_string()),
                        }
                        'tags: for tag in [b'l', b'x'] {
                            if load_err.is_some() {
                                break;
                            }
                            let mut prefix = Vec::with_capacity(18 + consumer.len());
                            prefix.extend_from_slice(&hash);
                            prefix.push(tag);
                            prefix.extend_from_slice(consumer.as_bytes());
                            prefix.push(0);
                            let mut iter = match self.db.scan_prefix(&prefix[..], ..).await {
                                Ok(i) => i,
                                Err(e) => {
                                    load_err = Some(e.to_string());
                                    break 'tags;
                                }
                            };
                            loop {
                                match iter.next().await {
                                    Ok(Some(kv)) => {
                                        let Some(offset_bytes) =
                                            kv.key.get(prefix.len()..prefix.len() + 8)
                                        else {
                                            load_err = Some("corrupt queue state key".to_string());
                                            break 'tags;
                                        };
                                        let off =
                                            u64::from_be_bytes(offset_bytes.try_into().unwrap());
                                        if tag == b'l' {
                                            let Some(lease) = decode_lease(&kv.value) else {
                                                load_err = Some("corrupt queue lease".to_string());
                                                break 'tags;
                                            };
                                            fresh.leases.insert(off, lease);
                                        } else {
                                            fresh.acked.insert(off);
                                        }
                                        if fresh.leases.len() + fresh.acked.len()
                                            > MAX_CONSUMER_OUTSTANDING
                                        {
                                            load_err = Some(
                                                "queue consumer outstanding-state limit exceeded"
                                                    .to_string(),
                                            );
                                            break 'tags;
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        load_err = Some(e.to_string());
                                        break 'tags;
                                    }
                                }
                            }
                        }
                        if load_err.is_none() {
                            local
                                .queue
                                .as_mut()
                                .unwrap()
                                .insert_loaded(consumer.clone(), fresh);
                        }
                    }
                    if let Some(m) = load_err {
                        let _ = resp.send(Err(m));
                        continue;
                    }
                    let now = now_ms();
                    let mut dlq_refs: Vec<(u64, u32)> = Vec::new(); // (orig off, attempts)
                    let (out, dlq_subkey) = {
                        let queue = local.queue.as_mut().unwrap();
                        match op {
                            QueueOp::Receive {
                                consumer,
                                max,
                                visibility_ms,
                                max_deliveries,
                                dlq_subkey,
                            } => {
                                let cs =
                                    queue.consumers.get_mut(&consumer).expect("loaded consumer");
                                let mut leased = Vec::new();
                                let target = max.min(
                                    MAX_CONSUMER_OUTSTANDING
                                        .saturating_sub(cs.leases.len() + cs.acked.len()),
                                );
                                let mut off = cs.cursor;
                                let mut steps = 0usize;
                                while off < local.fields.next
                                    && leased.len() < target
                                    && steps < target * 8 + 4096
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
                                wb.put(cursor_key(&hash, &consumer), cs.cursor.to_le_bytes());
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
                                let cs =
                                    queue.consumers.get_mut(&consumer).expect("loaded consumer");
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
                                    if let Some(l) = cs.leases.get(&off).copied()
                                        && l.lease_gen == tok_gen
                                    {
                                        let nl = Lease {
                                            deadline_ms: now + vis as i64,
                                            ..l
                                        };
                                        cs.leases.insert(off, nl);
                                        wb.put(lease_key(&hash, &consumer, off), encode_lease(&nl));
                                        extra_writes = true;
                                        e2 += 1;
                                    }
                                }
                                while cs.acked.remove(&cs.cursor) {
                                    wb.delete(ack_key(&hash, &consumer, cs.cursor));
                                    cs.cursor += 1;
                                    extra_writes = true;
                                }
                                wb.put(cursor_key(&hash, &consumer), cs.cursor.to_le_bytes());
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
                        let payload_bytes = payload.len() as u64;
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
                        wb.put(record_key(&hash, offset), frame);
                        local.fields.next += 1;
                        local.fields.logical += payload_bytes;
                        local.appended_bytes = local.appended_bytes.saturating_add(payload_bytes);
                        local.absorb_pending_bytes =
                            local.absorb_pending_bytes.saturating_add(payload_bytes);
                        records += 1;
                    }
                    queue_pending.push((resp, out));
                }
                CommitOp::Barrier { .. } => unreachable!("barrier enters commit group"),
            }
        }

        let mut tails = Vec::with_capacity(locals.len());
        let mut signals = Vec::new();
        let mut changed = false;
        for (hash, local) in &locals {
            let f = &local.fields;
            let b = &local.base;
            if f.next != b.next
                || f.absorbed != b.absorbed
                || f.trimmed != b.trimmed
                || f.seq != b.seq
                || f.closed != b.closed
            {
                wb.put(tail_key(hash), encode_tail(f));
                changed = true;
            }
            if local.absorb_pending_bytes != local.base_absorb_pending_bytes {
                if local.absorb_pending_bytes == 0 {
                    wb.delete(absorb_pending_key(hash));
                } else {
                    wb.put(
                        absorb_pending_key(hash),
                        local.absorb_pending_bytes.to_le_bytes(),
                    );
                }
                changed = true;
            }
            tails.push((local.handle.clone(), f.clone(), local.absorb_pending_bytes));
            if local.appended_bytes > 0 {
                signals.push(AbsorbSignal {
                    hash: *hash,
                    appended_bytes: local.appended_bytes,
                });
            }
        }
        if pending.is_empty() && !changed && queue_pending.is_empty() && absorbed_pending.is_empty()
        {
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
            for resp in absorbed_pending {
                let _ = resp.send(Ok(()));
            }
            return;
        }

        let encode_us = group_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
        let group_bytes: u64 = locals.values().map(|l| l.appended_bytes).sum();
        let write_t0 = std::time::Instant::now();
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
        let write_us = write_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;

        match res {
            Ok(handle) => {
                if self.closed.load(Ordering::Acquire) {
                    for (resp, _) in pending {
                        let _ = resp.send(Err(AppendErr::ShardMoved));
                    }
                    for (resp, _) in queue_pending {
                        let _ = resp.send(Err("shard moved".to_string()));
                    }
                    for resp in absorbed_pending {
                        let _ = resp.send(Err("shard moved".to_string()));
                    }
                    return;
                }
                for local in locals.values() {
                    let mut st = local.handle.state.lock().unwrap();
                    st.applied = local.fields.clone();
                    st.applied_absorb_pending_bytes = local.absorb_pending_bytes;
                    for (id, v) in &local.producers {
                        st.cache_producer(id.clone(), *v);
                    }
                    if let Some(queue) = &local.queue {
                        let mut queue = queue.clone();
                        queue.trim_resident();
                        st.queue = queue;
                    }
                }
                self.stats_appended.fetch_add(records, Ordering::Relaxed);
                self.stats_appended_bytes
                    .fetch_add(group_bytes, Ordering::Relaxed);
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
                    absorbed_acks: absorbed_pending,
                    tails,
                    signals,
                    touches,
                });
                self.flush_wake.notify_one();
            }
            Err(e) => {
                let msg = e.to_string();
                for (resp, _) in pending {
                    let _ = resp.send(Err(AppendErr::Internal(msg.clone())));
                }
                for (resp, _) in queue_pending {
                    let _ = resp.send(Err(msg.clone()));
                }
                for resp in absorbed_pending {
                    let _ = resp.send(Err(msg.clone()));
                }
            }
        }
    }

    async fn acker_loop(self: Arc<Self>) {
        let mut status_rx = self.db.subscribe();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            let durable_seq = {
                let status = status_rx.borrow_and_update();
                if let Some(reason) = &status.close_reason {
                    // Fenced or closed: this shard moved. The process serves
                    // other shards; groups here fail via dropped responders,
                    // and touch waiters are woken with stale right now.
                    match reason {
                        CloseReason::Clean => {
                            tracing::info!(shard = %self.prefix, "shard db closed cleanly")
                        }
                        CloseReason::Fenced => {
                            self.record_fence_once(crate::telemetry::FenceKind::Writer);
                            tracing::warn!(shard = %self.prefix, "shard writer was fenced")
                        }
                        _ => tracing::error!(shard = %self.prefix, "shard db closed: {reason:?}"),
                    }
                    self.mark_moved();
                    return;
                }
                status.durable_seq
            };
            let ready: Vec<InFlightGroup> = {
                let mut q = self.in_flight.lock().unwrap();
                let split = q.partition_point(|g| g.seq <= durable_seq);
                q.drain(..split).collect()
            };
            if !ready.is_empty() {
                let fenced = match self.reconfiguration_fence_exists().await {
                    Ok(fenced) => fenced,
                    Err(error) => {
                        tracing::error!(shard = %self.prefix, "reconfiguration fence check failed closed: {error}");
                        true
                    }
                };
                if fenced {
                    self.record_fence_once(crate::telemetry::FenceKind::Reconfiguration);
                    tracing::warn!(
                        shard = %self.prefix,
                        groups = ready.len(),
                        "withholding durable acknowledgements behind reconfiguration fence"
                    );
                    for group in ready {
                        for (resp, _) in group.acks {
                            let _ = resp.send(Err(AppendErr::ShardMoved));
                        }
                        for (resp, _) in group.queue_acks {
                            let _ = resp.send(Err("shard moved".to_string()));
                        }
                        for resp in group.absorbed_acks {
                            let _ = resp.send(Err("shard moved".to_string()));
                        }
                    }
                    self.mark_moved();
                    return;
                }
            }
            for group in ready {
                *self.last_owner_proof.lock().unwrap() = std::time::Instant::now();
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
                for (handle, fields, absorb_pending_bytes) in &group.tails {
                    let mut state = handle.state.lock().unwrap();
                    state.durable = fields.clone();
                    state.durable_absorb_pending_bytes = *absorb_pending_bytes;
                    drop(state);
                    handle.mark_visible(fields.next);
                    handle.notify.notify_waiters();
                }
                // Feed invalidation journals before releasing request ACKs.
                // Once an ACK is observable, a dependent state-protocol
                // request must be able to find this durable append.
                for t in group.touches {
                    t.journal.ingest(&t.key_ids, t.next_offset);
                }
                for (resp, ack) in group.acks {
                    let _ = resp.send(Ok(ack));
                }
                for (resp, out) in group.queue_acks {
                    let _ = resp.send(Ok(out));
                }
                for resp in group.absorbed_acks {
                    let _ = resp.send(Ok(()));
                }
                for s in group.signals {
                    // History maintenance is bounded and fail-closed. Do not
                    // await capacity here: the absorber can itself be waiting
                    // for this durability acknowledger, so waiting would form
                    // a saturation deadlock. A full or closed actor makes the
                    // shard/process unready instead of silently losing work.
                    self.telemetry.add_absorber_pending_bytes(s.appended_bytes);
                    if let Err(error) = self.absorb_tx.try_send(s) {
                        let (reason, signal) = match error {
                            mpsc::error::TrySendError::Full(signal) => ("full", signal),
                            mpsc::error::TrySendError::Closed(signal) => ("closed", signal),
                        };
                        self.telemetry
                            .remove_absorber_pending_bytes(signal.appended_bytes);
                        self.telemetry.mark_absorber_unhealthy();
                        tracing::error!(
                            shard = %self.prefix,
                            reason,
                            "history absorber unavailable; failing shard closed"
                        );
                        self.mark_moved();
                        return;
                    }
                }
            }
            if self.in_flight.lock().unwrap().is_empty() {
                self.in_flight_empty.notify_waiters();
            }
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

fn fail_commit_op(op: CommitOp) {
    match op {
        CommitOp::Append(req) => {
            let _ = req.resp.send(Err(AppendErr::ShardMoved));
        }
        CommitOp::Queue { resp, .. } => {
            let _ = resp.send(Err("shard moved".to_string()));
        }
        CommitOp::Absorbed { resp, .. } => {
            let _ = resp.send(Err("shard moved".to_string()));
        }
        CommitOp::Barrier { resp } => {
            let _ = resp.send(Err("shard moved".to_string()));
        }
    }
}

fn fail_queued_ops(rx: &mut mpsc::Receiver<CommitOp>) {
    rx.close();
    while let Ok(op) = rx.try_recv() {
        fail_commit_op(op);
    }
}

/// Frames with offset in [scan_from, durable_next), optionally filtered by
/// routing key (frame metadata; no decryption needed).
pub struct FrameReadResult {
    pub frames: Vec<Bytes>,
    pub last_offset: Option<u64>,
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
    };
    if scan_from >= scan_to {
        return Ok(out);
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
        let offset_at = hash.len() + 1;
        let off = u64::from_be_bytes(
            kv.key[offset_at..offset_at + 8]
                .try_into()
                .expect("record key"),
        );
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
    };
    if scan_from >= end {
        return Ok(out);
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
        let offset_at = hash.len() + 1;
        let off = u64::from_be_bytes(
            kv.key[offset_at..offset_at + 8]
                .try_into()
                .expect("record key"),
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_flush_sentinel_is_in_range_but_outside_the_service_key_grammar() {
        let zero = union_flush_sentinel("0").unwrap();
        let one = union_flush_sentinel("1").unwrap();
        assert_eq!(zero.len(), 17);
        assert_eq!(one.len(), 17);
        assert_eq!(zero[0] & 0x80, 0);
        assert_eq!(one[0] & 0x80, 0x80);
        assert!(union_flush_sentinel("").is_err());
    }

    fn handle(hash: StorageHash) -> Arc<StreamHandle> {
        Arc::new(StreamHandle {
            hash,
            state: Mutex::new(StreamState {
                durable: TailFields::default(),
                applied: TailFields::default(),
                durable_absorb_pending_bytes: 0,
                applied_absorb_pending_bytes: 0,
                producers: HashMap::new(),
                producer_order: VecDeque::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            visibility: Mutex::new(TailVisibility {
                next: 0,
                committed_at: None,
            }),
        })
    }

    fn append_op_on(customer_id: &str, stream: u8, bytes: usize, fair_weight: u16) -> CommitOp {
        let (resp, _rx) = oneshot::channel();
        CommitOp::Append(AppendReq {
            customer_id: customer_id.to_string(),
            hash: [stream; 32],
            fair_weight,
            enqueued_at: std::time::Instant::now(),
            entries: vec![Bytes::from(vec![0; bytes])],
            routing_key: String::new(),
            key_version: 0,
            subkey: [0; 32],
            ts_hint_ms: None,
            seq: None,
            bytes,
            close: false,
            producer: None,
            deferred_error: None,
            touch: None,
            resp,
        })
    }

    fn append_op(customer_id: &str, bytes: usize) -> CommitOp {
        append_op_on(customer_id, bytes as u8, bytes, 1)
    }

    fn customer(op: &CommitOp) -> &str {
        match op {
            CommitOp::Append(request) => &request.customer_id,
            _ => "internal",
        }
    }

    fn stream(op: &CommitOp) -> u8 {
        match op {
            CommitOp::Append(request) => request.hash[0],
            _ => 0,
        }
    }

    #[test]
    fn committer_round_robins_tenants_and_looks_past_large_requests() {
        let mut fair = FairCommitQueue::default();
        fair.push(append_op("a", 10));
        fair.push(append_op("a", 10));
        fair.push(append_op("b", 1));

        let first = fair.pop_batch(10, 10);
        assert_eq!(first.iter().map(customer).collect::<Vec<_>>(), vec!["a"]);
        let second = fair.pop_batch(10, 10);
        assert_eq!(second.iter().map(customer).collect::<Vec<_>>(), vec!["b"]);
        let third = fair.pop_batch(10, 10);
        assert_eq!(third.iter().map(customer).collect::<Vec<_>>(), vec!["a"]);

        let mut fair = FairCommitQueue::default();
        for _ in 0..3 {
            fair.push(append_op("a", 1));
        }
        fair.push(append_op("b", 1));
        let batch = fair.pop_batch(4, 100);
        assert_eq!(
            batch.iter().map(customer).collect::<Vec<_>>(),
            vec!["a", "b", "a", "a"]
        );

        // The outer tenant remains one turn per round, while streams inside a
        // tenant use their bounded provisioned weights.
        let mut fair = FairCommitQueue::default();
        for _ in 0..3 {
            fair.push(append_op_on("a", 1, 1, 2));
            fair.push(append_op_on("a", 2, 1, 1));
        }
        let batch = fair.pop_batch(6, 100);
        assert_eq!(
            batch.iter().map(stream).collect::<Vec<_>>(),
            vec![1, 1, 2, 1, 2, 2]
        );
    }

    #[test]
    fn stream_cache_is_bounded_and_never_evicts_active_state() {
        let mut cache = StreamCache::new(1);
        let first = cache.insert([1; 32], handle([1; 32])).unwrap();
        let held_by_reader = first.clone();

        let error = match cache.insert([2; 32], handle([2; 32])) {
            Ok(_) => panic!("active handle must not be evicted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), slatedb::ErrorKind::Unavailable);
        assert_eq!(cache.map.len(), 1);
        assert!(cache.map.contains_key(&[1; 32]));

        drop(held_by_reader);
        drop(first);
        cache.insert([2; 32], handle([2; 32])).unwrap();
        assert_eq!(cache.map.len(), 1);
        assert!(cache.map.contains_key(&[2; 32]));
    }

    #[test]
    fn stream_cache_keeps_applied_but_not_durable_state() {
        let mut cache = StreamCache::new(1);
        let first = handle([1; 32]);
        first.state.lock().unwrap().applied.next = 1;
        cache.insert([1; 32], first).unwrap();

        let error = match cache.insert([2; 32], handle([2; 32])) {
            Ok(_) => panic!("non-durable handle must not be evicted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), slatedb::ErrorKind::Unavailable);
        assert!(cache.map.contains_key(&[1; 32]));
    }

    #[test]
    fn resident_producer_state_is_bounded() {
        let handle = handle([1; 32]);
        let mut state = handle.state.lock().unwrap();
        for id in 0..=RESIDENT_PRODUCER_CAPACITY {
            state.cache_producer(id.to_string(), (0, id as u64));
        }

        assert_eq!(state.producers.len(), RESIDENT_PRODUCER_CAPACITY);
        assert!(!state.producers.contains_key("0"));
        assert_eq!(
            state.producers.get(&RESIDENT_PRODUCER_CAPACITY.to_string()),
            Some(&(0, RESIDENT_PRODUCER_CAPACITY as u64))
        );
    }

    #[tokio::test]
    async fn idle_owner_refresh_rejects_reads_after_fencing() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db1 = Arc::new(
            Db::builder("owner-proof", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (absorb_tx, _absorb_rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            String::new(),
            db1,
            ShardConfig::default(),
            absorb_tx,
            Arc::new(crate::telemetry::Telemetry::default()),
            None,
            None,
        );
        let db2 = Db::builder("owner-proof", store).build().await.unwrap();
        assert!(db2.status().current_manifest.writer_epoch() > engine.writer_epoch);
        *engine.last_owner_proof.lock().unwrap() =
            std::time::Instant::now() - std::time::Duration::from_secs(6);

        let error = engine.prove_ownership().await.unwrap_err();

        assert_eq!(
            error.kind(),
            slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
        );
        assert!(engine.closed.load(Ordering::Acquire));
        db2.close().await.unwrap();
    }

    #[tokio::test]
    async fn split_barrier_durably_drains_prior_writes_and_rejects_new_ones() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("split-barrier", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (absorb_tx, _absorb_rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            String::new(),
            db,
            ShardConfig::default(),
            absorb_tx,
            Arc::new(crate::telemetry::Telemetry::default()),
            None,
            None,
        );
        let hash = [4u8; 32];
        let (resp, ack) = oneshot::channel();
        assert!(
            engine
                .try_enqueue(AppendReq {
                    customer_id: "customer-a".into(),
                    hash,
                    fair_weight: 1,
                    enqueued_at: std::time::Instant::now(),
                    entries: vec![Bytes::from_static(b"before-barrier")],
                    routing_key: String::new(),
                    key_version: 0,
                    subkey: [7; 32],
                    ts_hint_ms: None,
                    seq: None,
                    bytes: 14,
                    close: false,
                    producer: None,
                    deferred_error: None,
                    touch: None,
                    resp,
                })
                .is_ok()
        );

        let quiescing = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.quiesce_for_split().await })
        };
        while engine.accepting.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let CommitOp::Append(after) = append_op("customer-b", 1) else {
            unreachable!()
        };
        assert_eq!(engine.try_enqueue(after), Err(EnqueueError::ShardMoved));
        assert_eq!(ack.await.unwrap().unwrap().next_offset, 1);
        quiescing.await.unwrap().unwrap();

        let reopened = Db::builder("split-barrier", store).build().await.unwrap();
        assert!(reopened.get(record_key(&hash, 0)).await.unwrap().is_some());
        assert_eq!(
            decode_tail(&reopened.get(tail_key(&hash)).await.unwrap().unwrap())
                .unwrap()
                .next,
            1
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn absorbed_frontier_is_acknowledged_only_after_remote_durability() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder("absorbed-ack", store).build().await.unwrap());
        let (absorb_tx, _absorb_rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            String::new(),
            db,
            ShardConfig::default(),
            absorb_tx,
            Arc::new(crate::telemetry::Telemetry::default()),
            None,
            None,
        );
        let hash = [8u8; 32];
        let (resp, ack) = oneshot::channel();
        let CommitOp::Append(req) = append_op("customer", 1) else {
            unreachable!()
        };
        assert!(engine.try_enqueue(AppendReq { hash, resp, ..req }).is_ok());
        assert_eq!(ack.await.unwrap().unwrap().next_offset, 1);
        let handle = engine.stream_handle(hash).await.unwrap();
        assert!(handle.tail_freshness(1).is_some());

        let recovered = ShardEngine::recover_pending_absorptions(&engine.db)
            .await
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].hash, hash);
        assert_eq!(recovered[0].appended_bytes, 1);

        assert!(engine.submit_absorbed(hash, 1, 1).await);
        assert_eq!(handle.state.lock().unwrap().durable.absorbed, 1);
        assert!(
            ShardEngine::recover_pending_absorptions(&engine.db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn absorber_debt_recovery_tracks_partial_progress_exactly() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("absorber-debt-partial", store)
                .build()
                .await
                .unwrap(),
        );
        let (absorb_tx, _absorb_rx) = mpsc::channel(8);
        let engine = ShardEngine::start(
            String::new(),
            db,
            ShardConfig::default(),
            absorb_tx,
            Arc::new(crate::telemetry::Telemetry::default()),
            None,
            None,
        );
        let hash = [7u8; 32];
        for bytes in [3, 5] {
            let (resp, ack) = oneshot::channel();
            let CommitOp::Append(req) = append_op("customer", bytes) else {
                unreachable!()
            };
            assert!(engine.try_enqueue(AppendReq { hash, resp, ..req }).is_ok());
            ack.await.unwrap().unwrap();
        }

        let recovered = ShardEngine::recover_pending_absorptions(&engine.db)
            .await
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].appended_bytes, 8);

        assert!(engine.submit_absorbed(hash, 1, 3).await);
        let recovered = ShardEngine::recover_pending_absorptions(&engine.db)
            .await
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].appended_bytes, 5);

        assert!(engine.submit_absorbed(hash, 2, 5).await);
        assert!(
            ShardEngine::recover_pending_absorptions(&engine.db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn absorber_recovery_migrates_unmarked_legacy_tail_conservatively() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Db::builder("absorber-debt-legacy", store)
            .build()
            .await
            .unwrap();
        let hash = [6u8; 32];
        db.put(
            tail_key(&hash),
            encode_tail(&TailFields {
                next: 2,
                logical: 11,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let recovered = ShardEngine::recover_pending_absorptions(&db).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].hash, hash);
        assert_eq!(recovered[0].appended_bytes, 11);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn absorber_recovery_fails_closed_on_corrupt_marker() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Db::builder("absorber-debt-corrupt", store)
            .build()
            .await
            .unwrap();
        db.put(absorb_pending_key(&[5u8; 32]), b"short")
            .await
            .unwrap();

        let error = ShardEngine::recover_pending_absorptions(&db)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("corrupt absorber pending-byte marker")
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn absorber_queue_saturation_fails_closed_without_deadlocking_acker() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("absorber-saturation", store)
                .build()
                .await
                .unwrap(),
        );
        let (absorb_tx, _absorb_rx) = mpsc::channel(1);
        let telemetry = Arc::new(crate::telemetry::Telemetry::default());
        let engine = ShardEngine::start(
            String::new(),
            db,
            ShardConfig::default(),
            absorb_tx,
            telemetry.clone(),
            None,
            None,
        );
        for (index, hash) in [[9u8; 32], [10u8; 32]].into_iter().enumerate() {
            let (resp, ack) = oneshot::channel();
            let CommitOp::Append(req) = append_op("customer", 1) else {
                unreachable!()
            };
            assert!(engine.try_enqueue(AppendReq { hash, resp, ..req }).is_ok());
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(2), ack)
                    .await
                    .expect("durability acknowledger deadlocked")
                    .unwrap()
                    .unwrap()
                    .next_offset,
                1,
                "append {index}"
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while telemetry.absorber_healthy() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("absorber saturation did not fail readiness");
        assert!(engine.is_closed());
    }

    #[tokio::test]
    async fn durable_group_is_not_acknowledged_after_split_intent() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("split-fence", store.clone())
                .build()
                .await
                .unwrap(),
        );
        store
            .put(
                &object_store::path::Path::from("split-intents/root.json"),
                object_store::PutPayload::from_static(b"fenced"),
            )
            .await
            .unwrap();
        let (absorb_tx, _absorb_rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            String::new(),
            db,
            ShardConfig::default(),
            absorb_tx,
            Arc::new(crate::telemetry::Telemetry::default()),
            None,
            Some(store),
        );
        let CommitOp::Append(req) = append_op("customer-a", 0) else {
            unreachable!()
        };
        let (resp, ack) = oneshot::channel();
        let req = AppendReq { resp, ..req };
        assert!(engine.try_enqueue(req).is_ok());

        assert!(matches!(ack.await.unwrap(), Err(AppendErr::ShardMoved)));
        assert!(engine.is_closed());
    }
}
