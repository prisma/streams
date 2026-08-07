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
/// [ver u8=3][next u64][last_ts i64][logical u64][absorbed u64][trimmed u64][flags u8][seq_len u16][seq][route16?][trim_safe_to u64?][unabsorbed_bytes u64?]
///
/// `flags` is a bitmask: bit0 = closed, bit1 = history v2 (the stream's
/// absorbed range lives in the shared per-shard partition, not a
/// per-stream DB). The optional trailing route16 (the shard-routing
/// hash), trim_safe_to and unabsorbed_bytes are backward-compatible
/// extensions: v3 decoders read exactly `seq_len` seq bytes and ignore
/// trailing bytes. `trim_safe_to` is the highest offset physical
/// trimming may reach (the absorbed boundary as of the PREVIOUS
/// advance — one advance of lag so in-flight readers holding a stale
/// absorbed snapshot never lose their range); `unabsorbed_bytes` is the
/// exact stored frame bytes in [absorbed, next), maintained by the
/// committer so restart rediscovery sizes pending work truthfully
/// instead of estimating (a single 32 MiB record used to estimate as
/// 1 KiB and never re-absorb under the default policy). Downgrade
/// caveat: a pre-bitmask binary reads flags with `== 1`, so it would
/// see a closed+v2 stream (flags=3) as open — acceptable for
/// forward-only deployments, noted here because it is not zero.
fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let mut v = Vec::with_capacity(76 + seq.len());
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
    v.extend_from_slice(&t.trim_safe_to.to_le_bytes());
    v.extend_from_slice(&t.unabsorbed_bytes.to_le_bytes());
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
    let le8 = |at: usize| -> u64 {
        v.get(at..at + 8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    };
    let trim_safe_to = le8(route_at + 16);
    let unabsorbed_bytes = le8(route_at + 24);
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
        trim_safe_to,
        unabsorbed_bytes,
    })
}

/// Per-routing-key Stream-Seq row (ROUTING-V3 §3.6): seq is scoped to
/// the KEY, not the segment — a segment carries many keys' lanes.
pub fn seq_key(hash: &[u8; 16], key_hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(hash);
    k.push(b's');
    k.extend_from_slice(key_hash);
    k
}

pub fn producer_key(hash: &[u8; 16], key_hash: &[u8; 16], producer_id: &str) -> Vec<u8> {
    // <segment identity> 'q' <routing-key hash> <producer id> — producer
    // sessions are scoped per ROUTING KEY (review finding 5): one
    // producer id keeps independent sequence lanes for different keys,
    // and the scope does not change across a split (each key's lane
    // follows its key through the predecessor chain).
    let mut k = Vec::with_capacity(33 + producer_id.len());
    k.extend_from_slice(hash);
    k.push(b'q');
    k.extend_from_slice(key_hash);
    k.extend_from_slice(producer_id.as_bytes());
    k
}

/// Durable dirty-stream index (static audit P1): a marker per stream
/// with outstanding maintenance — unabsorbed tail (`absorbed < next`)
/// or pending physical trim (`trimmed < trim_safe_to`) — written in the
/// SAME committer batch as the tail it describes and deleted in the
/// batch that catches both up. A fresh owner scans this prefix once at
/// absorber start, so outstanding work is rediscovered after
/// restart/handoff without the customer ever touching the stream again.
/// Lives under a sentinel "hash" of all-0xFF; a truncated-SHA stream
/// hash CAN equal that value (p = 2^-128, astronomically unlikely, not
/// impossible), but the distinct tag byte `D` — no stream row uses it —
/// is what actually guarantees these keys never collide with
/// `<hash16><tag>` stream rows. The sentinel's job is only to sort the
/// index at the end of the keyspace for one cheap range scan. NOTE for
/// physical range splitting (future): these markers sort OUTSIDE every
/// stream's route range, so a range split cannot carry them into the
/// child by key range — the index needs a route-local representation
/// (or its own tracker partition) before splits land; static handoff
/// (new owner opens the whole shard DB) is unaffected.
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
    /// Highest offset physical trimming may reach: the absorbed boundary
    /// as of the PREVIOUS advance (one advance of lag, so in-flight
    /// readers holding a stale absorbed snapshot never lose their
    /// range). Trim maintenance moves `trimmed` toward this under a
    /// GLOBAL per-commit delete budget — boundary publication and
    /// physical trimming are decoupled so a 1,024-stream second
    /// absorption wave can never build one multi-gigabyte delete batch.
    pub trim_safe_to: u64,
    /// Exact stored frame bytes in [absorbed, next), maintained by the
    /// committer (appends add frame lengths; absorb advances subtract
    /// the bytes the absorber actually copied). Restart rediscovery
    /// reads this instead of estimating records × 1 KiB, so the default
    /// absorption policy's byte thresholds see the truth.
    pub unabsorbed_bytes: u64,
}

/// Read visibility level. `Durable` (the pinned default everywhere)
/// serves only storage-durable records. `Applied` additionally serves
/// the live tail's applied-but-not-yet-durable suffix — the product
/// surface's opt-in low-latency subscribe mode. Applied is a READ-SIDE
/// clamp only: acks, consumers, watches, absorption and trim stay
/// durable-gated exactly as before.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Deliver {
    #[default]
    Durable,
    Applied,
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub struct StreamState {
    pub durable: TailFields,
    pub applied: TailFields,
    /// Producer idempotence state: id -> (epoch, highest seq). Loaded from
    /// the durable `q` keys on first use, applied by the committer.
    /// producer id -> (epoch, seq, last_offset of that seq's commit,
    /// request hash of that commit — zeros when none was recorded).
    /// The offset makes a duplicate ack return the ORIGINAL committed
    /// offset instead of whatever the tail happens to be when the retry
    /// arrives — with interleaved appends those differ, and clients use
    /// the ack offset for read-your-write. The hash backs the product
    /// surface's 409 producer_sequence_reused (same tuple, different
    /// request).
    pub producers: HashMap<([u8; 16], String), (u64, u64, u64, [u8; 16])>,
    /// Per-routing-key Stream-Seq lanes (ROUTING-V3 §3.6), loaded
    /// lazily from the durable `s` rows and applied by the committer.
    pub seqs: HashMap<[u8; 16], String>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
}

pub struct StreamHandle {
    pub hash: [u8; 16],
    pub state: Mutex<StreamState>,
    pub notify: Notify,
    /// Fired by the committer at write success (apply), before the
    /// durability barrier — the wake for `Deliver::Applied` waiters.
    /// Durable waiters keep `notify` (fired only at durable dispatch);
    /// the separate wake keeps the pinned durable read path free of
    /// spurious wakeups.
    pub applied_notify: Notify,
    /// Durable-tail ring: recently-durable frames, published by
    /// dispatch_durable BEFORE acks go out, so a reader woken by an ack
    /// (or by tail notify) finds the record here without a DB scan.
    /// Empty unless ShardConfig.tail_ring_bytes > 0.
    pub ring: Mutex<TailRing>,
    /// Millisecond timestamp of the last lookup — feeds idle eviction
    /// (resident handles previously lived forever; a wide shard held
    /// 100k of them, the largest per-stream memory term).
    pub last_touch_ms: std::sync::atomic::AtomicU64,
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

/// Producer identities the SERVER mints for records a client did not
/// coordinate (a seal's final append, a raw close carrying content).
/// They live in the same durable keyspace as public ones, so the wire
/// parser refuses this prefix — otherwise a caller could pre-create the
/// row and turn a later final append into a false duplicate.
pub const INTERNAL_PRODUCER_PREFIX: &str = "\u{0}prisma-internal\u{0}";

#[derive(Debug, Clone)]
pub struct ProducerReq {
    pub id: String,
    pub epoch: u64,
    pub seq: u64,
    /// Product-surface request hash (spec Stage 5 §7): 16 bytes over
    /// (operation kind, routing key, content type, body bytes, seal
    /// flag). None on the raw standards route — the pinned protocol's
    /// duplicate contract does not compare bodies.
    pub request_hash: Option<[u8; 16]>,
}

/// Validation failures that must be deferred until after the producer
/// duplicate check (a retry must return 204 even if e.g. the content type
/// no longer matches).
#[derive(Debug, Clone)]
pub enum DeferredErr {
    CtMismatch,
    BadBody(String),
}

/// Why a new producer sequence must not be accepted right now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SealedReject {
    Sealing,
    Sealed,
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
    /// stream_hash(routing_key), computed once at admission: the
    /// per-key Stream-Seq lane and postings/sketch identity.
    pub key_hash: [u8; 16],
    /// Predecessor segment identities for the routing key's lineage
    /// (nearest-first, empty for single-segment streams): the committer
    /// resolves producer state through this chain after a split
    /// (ROUTING-V3 §3.6) so a retry that committed on the sealed parent
    /// is suppressed by the child without consuming an offset.
    pub producer_lineage: Vec<[u8; 16]>,
    pub key_version: u32,
    pub subkey: [u8; 32],
    pub ts_hint_ms: Option<i64>,
    pub seq: Option<String>,
    pub bytes: usize,
    pub close: bool,
    /// Billing attribution (docs/OBSERVABILITY-BILLING.md §6): when
    /// present, the committer updates the durable SegmentBillingMeta
    /// row in the SAME WriteBatch as the records. None on internal
    /// writes that are not customer ingest (fences, absorber copies).
    pub billing: Option<std::sync::Arc<crate::billing::BillingRef>>,
    /// The seal-claim generation that authorizes this append (present
    /// on every claim-authorized write: final-bearing closes, plain
    /// closes that installed an Empty claim, run_seal's segment closes,
    /// split/merge segment closes). Checked against the segment's
    /// fence AFTER duplicate detection and BEFORE any record is staged
    /// or the close applied: a stale generation means the claim this
    /// append belonged to was taken over, and its write must not land.
    pub seal_gen: Option<u64>,
    /// Control message: raise the segment's seal fence to this
    /// generation and report whether the segment is closed. Processed
    /// in queue order, so by the time it answers, every append enqueued
    /// before it has been decided — the answer is a barrier, not a
    /// snapshot. Entries/close are ignored on fence messages.
    pub seal_fence_to: Option<u64>,
    pub producer: Option<ProducerReq>,
    pub deferred_error: Option<DeferredErr>,
    /// The COLLECTION is sealing or sealed while this physical segment
    /// may still be open. Producer requests are admitted anyway so a
    /// retry can be recognised as a duplicate and answered with its
    /// original result — but a genuinely NEW sequence must be refused,
    /// which only the committer can tell apart. Evaluated after
    /// duplicate detection; None for ordinary appends.
    pub sealed_reject_new: Option<SealedReject>,
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
    /// Product surface only: same (producer, epoch, seq) with a
    /// DIFFERENT request hash (spec Stage 5 §7).
    ProducerSeqReused,
    Closed {
        next_offset: u64,
    },
    /// The seal claim authorizing this append was taken over: its
    /// generation is below the segment's fence. The write did not
    /// happen. Retryable — a live owner re-enters the claim (renewing
    /// its generation) and retries; a dead one's client is told the
    /// seal was superseded.
    SealSuperseded,
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
    /// Usage-outbox acknowledgment (§6.3): `_usage` durably holds the
    /// snapshot at `version` — delete the dirty marker iff no NEWER
    /// version exists, and delete the exact listed closed-month rows.
    /// Serialized through the committer so an append racing the drain
    /// keeps its newer version dirty. Fire-and-forget by design: a lost
    /// ack re-emits an identical snapshot, which the rollup
    /// deduplicates by version.
    UsageAck {
        hash: [u8; 16],
        version: u64,
        month_final_keys: Vec<Vec<u8>>,
    },
    /// Hard-delete/expiry closure (§6.2): advance the storage clock to
    /// the PERSISTED logical close instant (round-22 item 7 — the
    /// tombstone's stamp or the configured expiry, never "whenever
    /// this op finally ran"), zero the owned-bytes gauge, bump the
    /// version and mark dirty — the terminal storage observation for
    /// the incarnation. `close_ms <= 0` falls back to billing-now.
    BillingClose {
        hash: [u8; 16],
        close_ms: i64,
    },
    /// Durable fork-retention flag (round-22 item 7): a soft-deleted
    /// source retained by live forks keeps accruing storage under the
    /// fork billing contract; the flag must survive restarts and
    /// ownership moves on the row itself, not only in emitted
    /// snapshots.
    BillingRetained {
        hash: [u8; 16],
        retained: bool,
    },
    /// Queue-profile state transition (PROFILES.md §7): serialized with
    /// appends, durable at the watermark like everything else.
    Queue {
        hash: [u8; 16],
        op: crate::queue::QueueOp,
        resp: oneshot::Sender<Result<crate::queue::QueueOut, String>>,
    },
    /// One gather's worth of absorber confirmations, carried as a SINGLE
    /// committer message so every covered boundary lands in the same
    /// write batch deterministically (the per-stream sends only
    /// coalesced opportunistically — the committer could run between
    /// them). Each entry is (hash, new upto, frame bytes the absorber
    /// copied for that stream — decremented from the tail's
    /// unabsorbed_bytes gauge). Expanded into per-stream `Absorbed` ops
    /// at commit_group entry.
    AbsorbedBatch {
        streams: Vec<([u8; 16], u64, u64)>,
        v2: bool,
    },
    /// Trim maintenance pulse (flush ticker, whenever the trim-debt set
    /// is non-empty): round-robins streams with `trimmed <
    /// trim_safe_to` and emits record deletes under the commit group's
    /// GLOBAL trim budget. This is where the bulk of physical trimming
    /// happens — the `Absorbed` arm only advances boundaries and takes
    /// whatever budget is left over.
    TrimTick,
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
        /// Stored frame bytes the absorber copied for this advance.
        bytes: u64,
        v2: bool,
    },
    /// TrimTick expansion product (commit_group entry): one stream's
    /// budgeted trim step. Never sent over the channel directly.
    TrimStep {
        hash: [u8; 16],
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
    /// Evict resident stream handles idle at least this long (and
    /// referenced by nothing but the map). Zero disables. The durable
    /// dirty-stream index keeps unabsorbed evictees discoverable, so
    /// eviction never strands a tail.
    pub handle_idle_evict: std::time::Duration,
    /// Capacity cap on resident stream handles (0 disables): when the
    /// map exceeds this, the ticker evicts oldest-touched unreferenced
    /// handles down to the cap WITHOUT waiting for the idle threshold —
    /// a cardinality burst can otherwise accumulate rate × idle-window
    /// handles before the first one ages out. Referenced handles
    /// (strong_count > 1) are never evicted, so the map can exceed the
    /// cap by the number of streams actively in use.
    pub handle_max_resident: usize,
    /// GLOBAL cap on record-trim deletes per commit group, shared by
    /// every `Absorbed` advance and `TrimStep` in the group. Without it
    /// one gather's AbsorbedBatch over 1,024 mature streams ×
    /// max_trim_per_op could expand into a multi-gigabyte WriteBatch
    /// (67M deletes at the wide posture's TRIM_PER_OP=65536).
    /// `max_trim_per_op` remains the per-stream bound within a group.
    pub trim_global_budget: u64,
    /// Decoded postings-slice cache budget (spec §7.1), bytes.
    pub postings_cache_bytes: usize,
    /// Process-shared postings cache (review finding 7: ONE budget for
    /// the whole process, not one per engine — 32 engines x 16 MiB was
    /// a nominal 512 MiB). main.rs passes the global; tests pass None
    /// for hermetic per-engine caches (counters stay isolated).
    pub shared_postings_cache: Option<Arc<crate::postings_cache::PostingsCache>>,
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
            handle_idle_evict: std::time::Duration::from_secs(600),
            handle_max_resident: 65_536,
            trim_global_budget: 65_536,
            postings_cache_bytes: crate::postings_cache::POSTINGS_CACHE_BYTES,
            shared_postings_cache: None,
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
    acks: Vec<(
        oneshot::Sender<Result<AppendAck, AppendErr>>,
        Result<AppendAck, AppendErr>,
    )>,
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
    /// Seal fences by segment identity — ENGINE-level, deliberately
    /// outside the evictable [`StreamHandle`], and deliberately WITHOUT
    /// any expiry: an AppendReq has no maximum queue residence (a
    /// timed-out HTTP handler drops only its receiver, and backpressure
    /// can hold the queue arbitrarily long), so no wall-clock bound on
    /// a fence is a proof about the request it exists to stop. One u64
    /// per ever-fenced segment, for the engine's lifetime, is the
    /// price of that proof; the map dies with the queue it protects. an AppendReq waiting in
    /// the committer channel holds only the stream hash, so a handle
    /// can be idle-evicted (or displaced by the resident cap) while a
    /// stale claim-authorized write is still queued, and a fence that
    /// lived in the handle would be reborn as zero when the committer
    /// reloaded it. This map dies with the engine and its queue —
    /// which is the exact lifetime the fence protects.
    seal_fences: Mutex<HashMap<[u8; 16], u64>>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<Vec<InFlightGroup>>,
    /// Serializes durable-dispatch between the pump (post-flush barrier)
    /// and the acker (failsafe + fencing path). Group drains are already
    /// exclusive via the in_flight lock; this additionally keeps tail
    /// state updates applying in seq order across the two callers.
    dispatch_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    // tokio (not std) DELIBERATELY: both gates are held across awaits
    // by tests, and their non-test acquirers run as tasks on the shared
    // runtime. A std lock() there blocks the WORKER THREAD; the #115
    // hunt caught that in the act — the blocked worker stranded the
    // timer driver, the gate-holding test's own sleep/watchdog timers
    // died, and the release never came (deadlock, all threads parked).
    // An async lock parks the TASK and the runtime keeps breathing.
    commit_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    appends_enqueued: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fail_group_for: Mutex<Option<std::collections::HashSet<[u8; 16]>>>,
    #[cfg(test)]
    fail_config_scan: std::sync::atomic::AtomicBool,
    /// Consumer-generation fences (round 16): (identity, consumer) ->
    /// first LIVE generation. Installed by the deletion saga's segment
    /// op BEFORE its cleanup stages, so a Receive/Settle for a dead
    /// generation that is still in this committer's queue can never
    /// re-stage rows the cleanup ran too early to see. Engine-resident
    /// like the seal fences: it only has to outlive the queue that
    /// could contain stale ops; durably, dead generations are already
    /// harmless because generations live in the row keys.
    consumer_fences: Mutex<HashMap<([u8; 16], String), u64>>,
    #[cfg(test)]
    fail_group_tripped: std::sync::atomic::AtomicUsize,
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
    /// Idle threshold for resident-handle eviction (ShardConfig copy).
    handle_idle_evict: std::time::Duration,
    /// Capacity cap for resident handles (ShardConfig copy; 0 = off).
    handle_max_resident: usize,
    /// Streams with `trimmed < trim_safe_to`: physical-trim work the
    /// budgeted TrimTick maintenance still owes. Maintained by the
    /// committer after each successful group; seeded from the durable
    /// dirty index at absorber start and from tail loads. BTreeSet so
    /// the round-robin cursor is a plain range scan.
    trim_debt: Mutex<std::collections::BTreeSet<[u8; 16]>>,
    /// Round-robin position for TrimTick expansion.
    trim_cursor: Mutex<[u8; 16]>,
    /// Trim telemetry: deletes emitted in the last commit group that
    /// trimmed anything, the max ever emitted in one group (the bound
    /// the mature-second-wave gate reads), and a cumulative total.
    pub trim_deletes_last: AtomicU64,
    pub trim_deletes_max_batch: AtomicU64,
    pub trim_deletes_total: AtomicU64,
    /// Advances rejected by the layout seal (cross-lane absorb after the
    /// stream's history layout was decided). Nonzero means the absorber
    /// raced its own lane classification — harmless with the seal, but
    /// worth seeing.
    pub absorb_lane_dropped: AtomicU64,
    /// Decoded postings-slice cache (spec §7): keyed historical reads
    /// pay the index once per active window.
    pub postings_cache: Arc<crate::postings_cache::PostingsCache>,
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

/// Test-only fault injection for the durable dirty-index scan, keyed by
/// shard prefix so concurrent tests cannot poison each other. The
/// object-store fault substrate cannot reach this path deterministically
/// (SlateDB retries store faults internally), and the absorber's
/// scan-retry loop is exactly the code under test.
#[cfg(test)]
fn dirty_scan_faults() -> &'static Mutex<HashMap<String, u32>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u32>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arrange for the next `n` dirty-index scans on `prefix` to fail.
#[cfg(test)]
pub(crate) fn inject_dirty_scan_faults(prefix: &str, n: u32) {
    dirty_scan_faults()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), n);
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
            seal_fences: Mutex::new(HashMap::new()),
            #[cfg(test)]
            commit_gate: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            appends_enqueued: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            fail_group_for: Mutex::new(None),
            #[cfg(test)]
            fail_config_scan: std::sync::atomic::AtomicBool::new(false),
            consumer_fences: Mutex::new(HashMap::new()),
            #[cfg(test)]
            fail_group_tripped: std::sync::atomic::AtomicUsize::new(0),
            tx,
            in_flight: Mutex::new(Vec::new()),
            dispatch_gate: tokio::sync::Mutex::new(()),
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
            handle_idle_evict: cfg.handle_idle_evict,
            handle_max_resident: cfg.handle_max_resident,
            trim_debt: Mutex::new(std::collections::BTreeSet::new()),
            trim_cursor: Mutex::new([0u8; 16]),
            trim_deletes_last: AtomicU64::new(0),
            trim_deletes_max_batch: AtomicU64::new(0),
            trim_deletes_total: AtomicU64::new(0),
            absorb_lane_dropped: AtomicU64::new(0),
            postings_cache: cfg.shared_postings_cache.clone().unwrap_or_else(|| {
                crate::postings_cache::PostingsCache::new(cfg.postings_cache_bytes)
            }),
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
                            pump.pump_flushed_bytes
                                .fetch_add(fl_bytes, Ordering::Relaxed);
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
                            let acked = pump.dispatch_durable(durable_seq).await;
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
                            if drifted && (pend_reqs >= skip_reqs || pend_bytes >= skip_bytes) {
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
                                let after: u32 =
                                    pump.in_flight.lock().unwrap().iter().map(|g| g.reqs).sum();
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
                    let idle = ticker.handle_idle_evict;
                    if !idle.is_zero() || ticker.handle_max_resident > 0 {
                        let evicted =
                            ticker.evict_idle_handles(idle, ticker.handle_max_resident);
                        if evicted > 0 {
                            tracing::debug!(
                                shard = %ticker.prefix,
                                "evicted {evicted} idle stream handles"
                            );
                        }
                    }
                    ticker
                        .postings_cache
                        .sweep_idle(crate::postings_cache::POSTINGS_CACHE_IDLE);
                    // Trim maintenance pulse: whenever streams owe
                    // physical trims, queue one budgeted TrimTick.
                    // try_send — a full committer queue means the next
                    // tick retries; trim work is never urgent enough to
                    // block behind.
                    if !ticker.trim_debt.lock().unwrap().is_empty() {
                        let _ = ticker.tx.try_send(CommitOp::TrimTick);
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
        #[cfg(test)]
        self.appends_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        crate::ops::emit(
            crate::ops::OpsEvent::new(
                "engine_closed",
                format!(
                    "engine/{}/closed/{}",
                    self.prefix,
                    crate::shard::now_ms() / 1000
                ),
            )
            .shard(&self.prefix),
        );
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

    pub async fn submit_absorbed(&self, hash: [u8; 16], upto: u64, bytes: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed {
                hash,
                upto,
                bytes,
                v2: false,
            })
            .await;
    }

    /// v2 boundary advance: the range is in the shared partition.
    pub async fn submit_absorbed_v2(&self, hash: [u8; 16], upto: u64, bytes: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed {
                hash,
                upto,
                bytes,
                v2: true,
            })
            .await;
    }

    /// One gather's boundary advances as a SINGLE committer message:
    /// every covered stream lands in the same write batch by
    /// construction (per-stream sends only coalesced opportunistically).
    /// Entries are (hash, new upto, frame bytes copied).
    pub async fn submit_absorbed_batch_v2(&self, streams: Vec<([u8; 16], u64, u64)>) {
        if streams.is_empty() {
            return;
        }
        let _ = self
            .tx
            .send(CommitOp::AbsorbedBatch { streams, v2: true })
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
        let part = self
            .history2
            .get_or_try_init(|| async {
                let path = crate::sharddir::history2_path(&self.prefix);
                let store = self.data_store.clone();
                let db = crate::on_slatedb_rt(async move {
                    Db::builder(path.as_str(), store)
                        .with_settings(crate::history::history2_settings())
                        .with_db_cache(crate::history::history_cache())
                        .build()
                        .await
                })
                .await?;
                Ok::<_, slatedb::Error>(Arc::new(db))
            })
            .await
            .cloned()?;
        // Close race (static audit): begin_close snapshots only the
        // INITIALIZED cell — an open in flight at that instant would
        // otherwise escape the deliberate close path. Re-check after
        // init and shut the fresh partition down ourselves.
        if self.is_closed() {
            let doomed = part.clone();
            tokio::spawn(async move {
                let _ = doomed.close().await;
            });
            return Err(slatedb::Error::data("engine closed".to_string()));
        }
        Ok(part)
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
    /// Producer-state lookup through the routing key's predecessor
    /// chain (ROUTING-V3 §3.6): own identity first, then each sealed
    /// predecessor. A hit on a predecessor means the producer's last
    /// commit landed before a split — the caller stages it locally so
    /// the duplicate check answers with the ORIGINAL offset and no new
    /// offset is consumed.
    /// Split-safe Stream-Seq (review blocker 4): a sequence lane lives
    /// per (segment, routing key), but a child segment starts empty —
    /// without consulting its sealed predecessors, a sequence the
    /// PARENT already accepted would be accepted again on the child.
    /// Nearest identity wins, exactly like the producer chain.
    async fn load_seq_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
    ) -> Result<Option<String>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
                return Ok(String::from_utf8(v.to_vec()).ok());
            }
        }
        Ok(None)
    }

    async fn load_producer_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
        pid: &str,
    ) -> Result<Option<(u64, u64, u64, [u8; 16])>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            match self.db.get(producer_key(identity, key_hash, pid)).await? {
                Some(v) if v.len() >= 40 => {
                    let mut h = [0u8; 16];
                    h.copy_from_slice(&v[24..40]);
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::from_le_bytes(v[16..24].try_into().unwrap()),
                        h,
                    )));
                }
                Some(v) if v.len() >= 24 => {
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::from_le_bytes(v[16..24].try_into().unwrap()),
                        [0u8; 16],
                    )));
                }
                Some(v) if v.len() >= 16 => {
                    // Legacy 16-byte row: the commit offset is UNKNOWN.
                    // u64::MAX marks that (offset 0 is a perfectly valid
                    // commit — the old 0-sentinel answered the wrong
                    // offset for a first-record duplicate).
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::MAX,
                        [0u8; 16],
                    )));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    pub async fn scan_dirty_streams(&self) -> anyhow::Result<Vec<([u8; 16], u64, u64)>> {
        #[cfg(test)]
        {
            let mut faults = dirty_scan_faults().lock().unwrap();
            if let Some(n) = faults.get_mut(&self.prefix) {
                if *n > 0 {
                    *n -= 1;
                    anyhow::bail!("injected dirty-scan fault (test hook)");
                }
            }
        }
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

    /// The durable tail for one stream WITHOUT materializing a handle —
    /// the startup marker scan reads these for the exact pending state
    /// (unabsorbed_bytes, trim debt) of each marked stream; loading
    /// handles for every cold dirty stream is exactly what memory
    /// pruning must avoid.
    pub async fn tail_fields(&self, hash: &[u8; 16]) -> anyhow::Result<Option<TailFields>> {
        Ok(self
            .db
            .get(tail_key(hash))
            .await?
            .and_then(|raw| decode_tail(&raw)))
    }

    /// Enroll a stream in TrimTick maintenance (startup marker scan; the
    /// committer maintains the set itself for live streams).
    pub fn note_trim_debt(&self, hash: [u8; 16]) {
        self.trim_debt.lock().unwrap().insert(hash);
    }

    /// Queue one budgeted trim-maintenance pulse NOW (tests drive drain
    /// cadence with this; the 5 s flush ticker is the production driver).
    pub fn pump_trim_tick(&self) {
        let _ = self.tx.try_send(CommitOp::TrimTick);
    }

    /// (streams owing trims, last group's deletes, max deletes in any
    /// one group, cumulative deletes) — the mature-second-wave gate
    /// reads max ≤ trim_global_budget from here.
    pub fn trim_stats(&self) -> (usize, u64, u64, u64) {
        (
            self.trim_debt.lock().unwrap().len(),
            self.trim_deletes_last.load(Ordering::Relaxed),
            self.trim_deletes_max_batch.load(Ordering::Relaxed),
            self.trim_deletes_total.load(Ordering::Relaxed),
        )
    }

    /// The absorbed boundary as recorded by the REMOTELY-DURABLE tracker —
    /// the strongest boundary any `DurabilityLevel::Remote` scan of the
    /// shard log can have observed trims for. The published handle state is
    /// NOT enough for that purpose: trim deletes become scan-visible when
    /// their batch is durable, while `handle.state.durable` advances only
    /// at dispatch, which can lag durability arbitrarily under load
    /// (2026-07-27 boundary-race DST failure). Readers revalidating a tail
    /// scan against concurrent absorption must consult this.
    /// Remotely-durable `(absorbed, history_v2)` from the stored tail
    /// row. Returned TOGETHER because they must be read consistently: a
    /// reader that adopts a remote boundary while keeping a stale
    /// in-memory layout flag would refuse a v2 history range as v1
    /// (observed in the first-absorption flush-to-dispatch window).
    pub async fn durable_absorbed(&self, hash: &[u8; 16]) -> Result<(u64, bool), slatedb::Error> {
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
        Ok(v.and_then(|b| decode_tail(&b))
            .map_or((0, false), |t| (t.absorbed, t.history_v2)))
    }

    /// Durable consumer-cursor hint for the pull pre-read window. A
    /// stale value only widens the window; the committer's
    /// stop-at-uncovered rule keeps leasing exact.
    /// Create the tail row for a FORK's storage identity: its own
    /// record space begins at the fork boundary — nothing below exists
    /// under this identity (inherited records are served from the
    /// ancestor chain). No-op when the tail already exists (idempotent
    /// PUT retries).
    pub async fn seed_fork_tail(
        &self,
        hash: [u8; 16],
        route: [u8; 16],
        at: u64,
    ) -> Result<(), slatedb::Error> {
        if self.db.get(tail_key(&hash)).await?.is_some() {
            return Ok(());
        }
        let t = TailFields {
            next: at,
            absorbed: at,
            trimmed: at,
            trim_safe_to: at,
            history_v2: true,
            route,
            ..Default::default()
        };
        self.db
            .put(&tail_key(&hash)[..], &encode_tail(&t)[..])
            .await
            .map(|_| ())
    }

    pub async fn queue_cursor(
        &self,
        hash: [u8; 16],
        consumer: &str,
        cgen: u64,
    ) -> Result<u64, slatedb::Error> {
        Ok(self
            .db
            .get(crate::queue::cursor_key(&hash, consumer, cgen))
            .await?
            .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
            .unwrap_or(0))
    }

    pub async fn submit_queue(
        &self,
        hash: [u8; 16],
        op: crate::queue::QueueOp,
    ) -> Result<crate::queue::QueueOut, String> {
        let (tx, rx) = oneshot::channel();
        #[cfg(test)]
        self.appends_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.tx
            .send(CommitOp::Queue { hash, op, resp: tx })
            .await
            .map_err(|_| "committer gone".to_string())?;
        rx.await
            .map_err(|_| "committer dropped request".to_string())?
    }

    pub async fn stream_handle(&self, hash: [u8; 16]) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(h) = self.streams.lock().unwrap().get(&hash) {
            h.last_touch_ms
                .store(now_ms() as u64, std::sync::atomic::Ordering::Relaxed);
            return Ok(h.clone());
        }
        let tail = match self.db.get(tail_key(&hash)).await? {
            Some(raw) => decode_tail(&raw).unwrap_or_default(),
            None => TailFields::default(),
        };
        // Trim-debt discovery on load: a stream evicted (or restarted)
        // mid-maintenance re-enters the TrimTick rotation the moment
        // anything touches it again. The absorber's startup marker scan
        // covers never-touched streams.
        if tail.trimmed < tail.trim_safe_to {
            self.trim_debt.lock().unwrap().insert(hash);
        }
        let handle = Arc::new(StreamHandle {
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                producers: HashMap::new(),
                seqs: HashMap::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            applied_notify: Notify::new(),
            ring: Mutex::new(TailRing::default()),
            last_touch_ms: std::sync::atomic::AtomicU64::new(now_ms() as u64),
        });
        let mut map = self.streams.lock().unwrap();
        Ok(map.entry(hash).or_insert(handle).clone())
    }

    /// Evict resident handles idle at least `idle` and referenced by
    /// nobody but the map (strong_count == 1 — in-flight readers,
    /// waiters, ring publication and committer batches all hold clones,
    /// so anything in use is untouchable by construction). Then, if the
    /// map still exceeds `max_resident` (0 = uncapped), evict the
    /// OLDEST-touched unreferenced handles down to the cap regardless of
    /// idle age — time-based eviction alone lets a cardinality burst
    /// accumulate rate × idle-window handles before the first ages out.
    /// Referenced handles are never evicted, so the map can exceed the
    /// cap by the number of streams actively in use. Returns how many
    /// were dropped. A later touch reloads durable state from the shard
    /// DB, and the dirty-stream index keeps unabsorbed evictees
    /// discoverable.
    pub fn evict_idle_handles(&self, idle: std::time::Duration, max_resident: usize) -> usize {
        let mut map = self.streams.lock().unwrap();
        let before = map.len();
        if !idle.is_zero() {
            let cutoff = (now_ms() as u64).saturating_sub(idle.as_millis() as u64);
            map.retain(|_, h| {
                std::sync::Arc::strong_count(h) > 1
                    || h.last_touch_ms.load(std::sync::atomic::Ordering::Relaxed) > cutoff
            });
        }
        if max_resident > 0 && map.len() > max_resident {
            let mut evictable: Vec<(u64, [u8; 16])> = map
                .iter()
                .filter(|(_, h)| std::sync::Arc::strong_count(h) == 1)
                .map(|(k, h)| {
                    (
                        h.last_touch_ms.load(std::sync::atomic::Ordering::Relaxed),
                        *k,
                    )
                })
                .collect();
            evictable.sort_unstable();
            let excess = map.len() - max_resident;
            for (_, k) in evictable.into_iter().take(excess) {
                map.remove(&k);
            }
        }
        before - map.len()
    }

    pub fn resident_streams(&self) -> usize {
        self.streams.lock().unwrap().len()
    }

    /// Peek a resident handle's absorbed boundary WITHOUT materializing
    /// one (materialization is exactly what memory pruning must avoid).
    pub fn resident_absorbed(&self, hash: &[u8; 16]) -> Option<u64> {
        let h = self.streams.lock().unwrap().get(hash).cloned()?;
        let st = h.state.lock().unwrap();
        Some(st.durable.absorbed)
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
                            CommitOp::Absorbed { .. }
                            | CommitOp::AbsorbedBatch { .. }
                            | CommitOp::TrimTick
                            | CommitOp::UsageAck { .. }
                            | CommitOp::BillingClose { .. }
                            | CommitOp::BillingRetained { .. }
                            | CommitOp::TrimStep { .. } => {}
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
                    CommitOp::Absorbed { .. }
                    | CommitOp::AbsorbedBatch { .. }
                    | CommitOp::TrimTick
                    | CommitOp::UsageAck { .. }
                    | CommitOp::BillingClose { .. }
                    | CommitOp::BillingRetained { .. }
                    | CommitOp::TrimStep { .. } => {}
                }
                while let Ok(op) = rx.try_recv() {
                    match op {
                        CommitOp::Append(r) => {
                            let _ = r.resp.send(Err(AppendErr::Moved));
                        }
                        CommitOp::Queue { resp, .. } => {
                            let _ = resp.send(Err("shard fenced/moved; retry".into()));
                        }
                        CommitOp::Absorbed { .. }
                        | CommitOp::AbsorbedBatch { .. }
                        | CommitOp::TrimTick
                        | CommitOp::UsageAck { .. }
                        | CommitOp::BillingClose { .. }
                        | CommitOp::BillingRetained { .. }
                        | CommitOp::TrimStep { .. } => {}
                    }
                }
                return;
            }
            // Test gate: while held, the committer parks HERE — after
            // taking the first op, before draining the rest — so a test
            // releases it with N ops queued and gets exactly one group
            // containing all of them. Deterministic group composition,
            // the primitive every same-group scenario needs.
            #[cfg(test)]
            {
                let _hold = self.commit_gate.lock().await;
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

    /// The ONE way a commit group's promises fail: every pending
    /// result — success or provisional refusal alike — is replaced by
    /// the group's failure. Used by the real write-error arm and by
    /// the DST group-failure hook, so the two can never diverge.
    fn send_group_failure(
        msg: &str,
        pending: Vec<(
            oneshot::Sender<Result<AppendAck, AppendErr>>,
            Result<AppendAck, AppendErr>,
        )>,
        queue_pending: Vec<(
            oneshot::Sender<Result<crate::queue::QueueOut, String>>,
            crate::queue::QueueOut,
        )>,
    ) {
        for (resp, _) in pending {
            let _ = resp.send(Err(AppendErr::Internal(msg.to_string())));
        }
        for (resp, _) in queue_pending {
            let _ = resp.send(Err(msg.to_string()));
        }
    }

    async fn commit_group(&self, ops: Vec<CommitOp>, cfg: &ShardConfig) {
        // Expand gather batches into per-stream ops INSIDE this group, so
        // one gather's boundary advances share one write batch by
        // construction. TrimTick expands into a bounded round-robin
        // window of trim-debt streams; the byte-scale bound is the
        // group-global `trim_budget` below, this cap only bounds handle
        // loads per group.
        const TRIM_STREAMS_PER_TICK: usize = 64;
        let mut expanded: Vec<CommitOp> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                CommitOp::AbsorbedBatch { streams, v2 } => {
                    expanded.extend(streams.into_iter().map(|(hash, upto, bytes)| {
                        CommitOp::Absorbed {
                            hash,
                            upto,
                            bytes,
                            v2,
                        }
                    }));
                }
                CommitOp::TrimTick => {
                    use std::ops::Bound;
                    let debt = self.trim_debt.lock().unwrap();
                    if debt.is_empty() {
                        continue;
                    }
                    let mut cur = self.trim_cursor.lock().unwrap();
                    let picked: Vec<[u8; 16]> = debt
                        .range((Bound::Excluded(*cur), Bound::Unbounded))
                        .chain(debt.range((Bound::Unbounded, Bound::Included(*cur))))
                        .take(TRIM_STREAMS_PER_TICK)
                        .copied()
                        .collect();
                    if let Some(last) = picked.last() {
                        *cur = *last;
                    }
                    expanded.extend(picked.into_iter().map(|hash| CommitOp::TrimStep { hash }));
                }
                other => expanded.push(other),
            }
        }
        let ops = expanded;
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
                    CommitOp::Absorbed { .. }
                    | CommitOp::AbsorbedBatch { .. }
                    | CommitOp::TrimTick
                    | CommitOp::UsageAck { .. }
                    | CommitOp::BillingClose { .. }
                    | CommitOp::BillingRetained { .. }
                    | CommitOp::TrimStep { .. } => {}
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
            producers: HashMap<([u8; 16], String), (u64, u64, u64, [u8; 16])>,
            seqs: HashMap<[u8; 16], String>,
            /// Batch-local QUEUE state (round 12): consumer cursors,
            /// leases and acks staged against this group's WriteBatch,
            /// published to the shared handle only after the write
            /// succeeds. Mutating the handle in place left phantom
            /// leases and cursor movement in memory when the write
            /// failed — exactly the applied-vs-durable split the
            /// producer and tail state already respect.
            queue: Option<crate::queue::QueueState>,
            /// Batch-local consumer-config overlay (round 14): config
            /// rows are DB-backed, not handle-cached, so this exists
            /// purely for SAME-GROUP consistency — a ConfigPut and a
            /// later ConfigGet/Delete in one group must see each other,
            /// and the DB (behind an unwritten WriteBatch) cannot show
            /// them. Some(cfg) = put this group; None = deleted this
            /// group; absent key = untouched (read the DB). Discarded
            /// after the group either way: on success the rows are
            /// durable, on failure they never existed.
            queue_configs: std::collections::HashMap<String, crate::queue::ConsumerRecord>,
            appended_bytes: u64,
            /// Frames written by this group, retained for the durable-tail
            /// ring (empty when the ring is off). Offsets are contiguous
            /// per stream: every append path assigns at fields.next.
            ring_recs: Vec<(u64, Bytes)>,
            /// Durable billing state (§6.1), loaded from the DB on this
            /// group's first billed touch, mutated batch-locally, and
            /// staged into the same WriteBatch. A failed group discards
            /// it — the DB row is the only truth.
            billing: Option<crate::billing::SegmentBillingMetaV1>,
            billing_dirty: bool,
            /// Closed-month final snapshots produced by storage-clock
            /// rollover in this group, staged as sentinel-'V' rows.
            month_finals: Vec<crate::billing::SegmentSnapshot>,
        }

        let mut wb = WriteBatch::new();
        // Every response whose TRUTH depends on batch-local or applied
        // state — success or refusal alike (round 11). A conflict
        // derived from a producer row another request staged in this
        // very group is not a fact until that group commits; answering
        // it early and then losing the write hands the client a
        // definitive verdict about state that never existed.
        let mut pending: Vec<(
            oneshot::Sender<Result<AppendAck, AppendErr>>,
            Result<AppendAck, AppendErr>,
        )> = Vec::new();
        // Fence responses: durability-barriered separately, because a
        // fence-only group persists nothing of its own — its barrier is
        // whatever is already in flight.
        let mut fence_acks: Vec<(
            oneshot::Sender<Result<AppendAck, AppendErr>>,
            Result<AppendAck, AppendErr>,
        )> = Vec::new();
        // Client appends seen by THIS group (DST): the group-write
        // failpoint fires only for groups carrying a client write for
        // the armed identity — maintenance ops (absorber, trim) touch
        // the same stream's locals and must not consume the arm.
        #[cfg(test)]
        let mut client_append_hashes: std::collections::HashSet<[u8; 16]> =
            std::collections::HashSet::new();
        let mut locals: HashMap<[u8; 16], Local> = HashMap::new();
        let mut records = 0u64;
        let mut touches: Vec<TouchFeed> = Vec::new();
        let mut queue_pending: Vec<(
            oneshot::Sender<Result<crate::queue::QueueOut, String>>,
            crate::queue::QueueOut,
        )> = Vec::new();
        let mut extra_writes = false;
        // GLOBAL trim budget for this commit group: every record-delete a
        // boundary advance or TrimStep emits draws from this one pool, so
        // the group's WriteBatch delete count is bounded no matter how
        // many streams one gather covered (the unbounded-trim P0).
        let mut trim_budget: u64 = cfg.trim_global_budget;

        for op in ops {
            let hash = match &op {
                CommitOp::Append(r) => r.hash,
                CommitOp::Absorbed { hash, .. } => *hash,
                CommitOp::Queue { hash, .. } => *hash,
                CommitOp::TrimStep { hash } => *hash,
                CommitOp::UsageAck { hash, .. } => *hash,
                CommitOp::BillingClose { hash, .. } => *hash,
                CommitOp::BillingRetained { hash, .. } => *hash,
                // Expanded at commit_group entry; unreachable here.
                CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => continue,
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
                                seqs: HashMap::new(),
                                queue: None,
                                queue_configs: HashMap::new(),
                                appended_bytes: 0,
                                ring_recs: Vec::new(),
                                billing: None,
                                billing_dirty: false,
                                month_finals: Vec::new(),
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
                // Expanded at commit_group entry; unreachable here.
                CommitOp::AbsorbedBatch { .. } | CommitOp::TrimTick => {}
                CommitOp::UsageAck {
                    version,
                    month_final_keys,
                    ..
                } => {
                    // Newest-version check: batch-local state first (an
                    // append EARLIER in this very group already bumped
                    // it), else the durable row. A NEWER version stays
                    // dirty — the drain that acked version N must not
                    // erase evidence of N+1.
                    let cur = match &local.billing {
                        Some(bm) => bm.usage_version,
                        None => match self
                            .db
                            .get(&crate::billing::billing_meta_key(&hash)[..])
                            .await
                        {
                            Ok(Some(v)) => {
                                serde_json::from_slice::<crate::billing::SegmentBillingMetaV1>(&v)
                                    .map(|m| m.usage_version)
                                    .unwrap_or(0)
                            }
                            _ => 0,
                        },
                    };
                    if cur <= version {
                        wb.delete(crate::billing::usage_dirty_key(&hash));
                        extra_writes = true;
                    }
                    for k in month_final_keys {
                        wb.delete(k);
                        extra_writes = true;
                    }
                }
                CommitOp::BillingClose { close_ms, .. } => {
                    if local.billing.is_none() {
                        let loaded = match self
                            .db
                            .get(&crate::billing::billing_meta_key(&hash)[..])
                            .await
                        {
                            Ok(Some(v)) => serde_json::from_slice(&v).unwrap_or_default(),
                            _ => crate::billing::SegmentBillingMetaV1::default(),
                        };
                        local.billing = Some(loaded);
                    }
                    {
                        let bm = local.billing.as_mut().unwrap();
                        if bm.stream_id.is_empty() {
                            // Nothing was ever billed here; no row to close.
                            local.billing = None;
                        } else {
                            // Round-22 item 7: account to the PERSISTED
                            // logical close instant; the monotone guard
                            // in advance_storage_clock makes any late
                            // retry a no-op advance + idempotent zero.
                            let at = if close_ms > 0 {
                                close_ms
                            } else {
                                crate::billing::billing_now_ms()
                            };
                            let finals = &mut local.month_finals;
                            bm.advance_storage_clock(at, |closed| {
                                finals.push(closed.to_snapshot(true));
                            });
                            bm.owned_frame_bytes_current = 0;
                            bm.usage_version += 1;
                            local.billing_dirty = true;
                        }
                    }
                }
                CommitOp::BillingRetained { retained, .. } => {
                    if local.billing.is_none() {
                        let loaded = match self
                            .db
                            .get(&crate::billing::billing_meta_key(&hash)[..])
                            .await
                        {
                            Ok(Some(v)) => serde_json::from_slice(&v).unwrap_or_default(),
                            _ => crate::billing::SegmentBillingMetaV1::default(),
                        };
                        local.billing = Some(loaded);
                    }
                    {
                        let bm = local.billing.as_mut().unwrap();
                        if bm.stream_id.is_empty() {
                            // Nothing was ever billed here; no row to flag.
                            local.billing = None;
                        } else if bm.retained_by_forks != retained {
                            bm.retained_by_forks = retained;
                            bm.usage_version += 1;
                            local.billing_dirty = true;
                        }
                    }
                }
                CommitOp::Append(req) => {
                    #[cfg(test)]
                    client_append_hashes.insert(hash);
                    // Fence message: a takeover raising the segment's
                    // minimum claim generation. The RAISE is immediate
                    // (later ops in this very group already see it),
                    // but the RESPONSE is durability-barriered: it
                    // rides the group ack pipeline, so it reaches the
                    // taking-over operation only after the durable
                    // watermark covers every write decided before it.
                    // Answering from staged state let a takeover read
                    // closed=true off a WriteBatch that had not been
                    // written — publish Sealed — and then watch the
                    // write fail; the closed-report must be a fact
                    // about DURABLE state or it is not a fact at all.
                    if let Some(g) = req.seal_fence_to {
                        {
                            let mut f = self.seal_fences.lock().unwrap();
                            let e = f.entry(hash).or_insert(0);
                            *e = (*e).max(g);
                        }
                        fence_acks.push((
                            req.resp,
                            Ok(AppendAck {
                                last_offset: local.fields.next.wrapping_sub(1),
                                next_offset: local.fields.next,
                                closed: local.fields.closed,
                                producer: None,
                                duplicate: false,
                            }),
                        ));
                        continue;
                    }
                    // Producer state: ensure loaded (durable `q` key) into
                    // the batch-local staging map.
                    if let Some(pr) = &req.producer {
                        let plane = (req.key_hash, pr.id.clone());
                        if !local.producers.contains_key(&plane) {
                            let shared = {
                                let st = local.handle.state.lock().unwrap();
                                st.producers.get(&plane).copied()
                            };
                            let loaded = match shared {
                                Some(v) => Some(v),
                                // Own identity first, then the routing
                                // key's sealed predecessors (split-safe
                                // idempotence, ROUTING-V3 §3.6). Format
                                // parsing lives in the chain loader.
                                None => match self
                                    .load_producer_chain(
                                        &hash,
                                        &req.producer_lineage,
                                        &req.key_hash,
                                        &pr.id,
                                    )
                                    .await
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        let _ =
                                            req.resp.send(Err(AppendErr::Internal(e.to_string())));
                                        continue;
                                    }
                                },
                            };
                            if let Some(v) = loaded {
                                local.producers.insert(plane, v);
                            }
                        }
                    }
                    // Contract check order: stale epoch (403) -> duplicate
                    // (204, before everything below) -> epoch/seq rules ->
                    // gap (409) -> closed (409) -> deferred ct/body errors ->
                    // Stream-Seq -> append.
                    let mut prod_echo: Option<(u64, u64)> = None;
                    if let Some(pr) = &req.producer {
                        match local.producers.get(&(req.key_hash, pr.id.clone())).copied() {
                            Some((ce, cs, coff, chash)) => {
                                if pr.epoch < ce {
                                    pending.push((
                                        req.resp,
                                        Err(AppendErr::ProducerStale { current_epoch: ce }),
                                    ));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq <= cs {
                                    // Product-surface reuse check (spec
                                    // Stage 5 §7): the SAME tuple with a
                                    // DIFFERENT request is a caller bug,
                                    // not a duplicate. Only enforceable
                                    // for the latest sequence (older
                                    // hashes are not retained), and only
                                    // when both sides recorded a hash —
                                    // the raw protocol's duplicate
                                    // contract never compares bodies.
                                    if pr.seq == cs
                                        && chash != [0u8; 16]
                                        && req
                                            .producer
                                            .as_ref()
                                            .and_then(|p| p.request_hash)
                                            .is_some_and(|h| h != chash)
                                    {
                                        pending.push((req.resp, Err(AppendErr::ProducerSeqReused)));
                                        continue;
                                    }
                                    // Duplicate: answer with the ORIGINAL
                                    // committed offset when the stored
                                    // producer row carries it (24-byte
                                    // format; offset 0 is valid). Only a
                                    // legacy 16-byte row (coff == MAX)
                                    // degrades to the tail-based answer.
                                    let last = if pr.seq == cs && coff != u64::MAX {
                                        coff
                                    } else {
                                        local.fields.next.wrapping_sub(1)
                                    };
                                    // DURABILITY-BARRIERED (round 10):
                                    // this success is a statement that
                                    // the original write is durable —
                                    // but the row it was read from may
                                    // be batch-local staging (same
                                    // group) or applied-not-yet-durable
                                    // state. The ack rides the group
                                    // pipeline like every other
                                    // success: answered after the
                                    // barrier that actually covers it,
                                    // failed with the group if its
                                    // write fails.
                                    pending.push((
                                        req.resp,
                                        Ok(AppendAck {
                                            last_offset: last,
                                            next_offset: local.fields.next,
                                            closed: local.fields.closed,
                                            producer: Some((ce, cs)),
                                            duplicate: true,
                                        }),
                                    ));
                                    continue;
                                }
                                if pr.epoch > ce && pr.seq != 0 {
                                    pending.push((req.resp, Err(AppendErr::ProducerEpochSeq)));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq > cs + 1 {
                                    pending.push((
                                        req.resp,
                                        Err(AppendErr::ProducerGap {
                                            expected: cs + 1,
                                            received: pr.seq,
                                        }),
                                    ));
                                    continue;
                                }
                            }
                            None => {
                                if pr.seq != 0 {
                                    pending.push((
                                        req.resp,
                                        Err(AppendErr::ProducerGap {
                                            expected: 0,
                                            received: pr.seq,
                                        }),
                                    ));
                                    continue;
                                }
                            }
                        }
                        // Past every duplicate/gap answer above, this is a
                        // NEW sequence. If the collection is sealing or
                        // sealed, it must not land: the descriptor-level
                        // gate cannot make this call, because it cannot
                        // tell a retry from a new record.
                        if req.sealed_reject_new.is_some() {
                            pending.push((
                                req.resp,
                                Err(AppendErr::Closed {
                                    next_offset: local.fields.next,
                                }),
                            ));
                            continue;
                        }
                        prod_echo = Some((pr.epoch, pr.seq));
                    }
                    if local.fields.closed {
                        if req.close && req.entries.is_empty() && req.producer.is_none() {
                            // Idempotent close-only — but "the segment
                            // is closed" may have been established by
                            // an earlier op in THIS group or by an
                            // applied-not-yet-durable one. Barriered
                            // like every success whose truth depends
                            // on non-durable state (round 10).
                            pending.push((
                                req.resp,
                                Ok(AppendAck {
                                    last_offset: local.fields.next.wrapping_sub(1),
                                    next_offset: local.fields.next,
                                    closed: true,
                                    producer: None,
                                    duplicate: false,
                                }),
                            ));
                        } else {
                            pending.push((
                                req.resp,
                                Err(AppendErr::Closed {
                                    next_offset: local.fields.next,
                                }),
                            ));
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
                        // ROUTING-V3 §3.6: Stream-Seq is scoped to the
                        // ROUTING KEY. Lazy-load the key's lane (shared
                        // state, then the durable `s` row of THIS
                        // segment, then the sealed predecessor chain —
                        // a child must not re-accept a sequence its
                        // parent already took); the empty key is an
                        // ordinary lane, so single-key streams behave
                        // exactly as before.
                        if !local.seqs.contains_key(&req.key_hash) {
                            let shared = {
                                let st = local.handle.state.lock().unwrap();
                                st.seqs.get(&req.key_hash).cloned()
                            };
                            let loaded = match shared {
                                Some(v) => Some(v),
                                None => match self
                                    .load_seq_chain(&hash, &req.producer_lineage, &req.key_hash)
                                    .await
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        let _ =
                                            req.resp.send(Err(AppendErr::Internal(e.to_string())));
                                        continue;
                                    }
                                },
                            };
                            if let Some(v) = loaded {
                                local.seqs.insert(req.key_hash, v);
                            }
                        }
                        if let Some(cur) = local.seqs.get(&req.key_hash) {
                            if seq <= cur {
                                pending.push((
                                    req.resp,
                                    Err(AppendErr::SeqConflict {
                                        current: Some(cur.clone()),
                                    }),
                                ));
                                continue;
                            }
                        }
                    }
                    // Seal fence (round 8): a claim-authorized write
                    // below the segment's fence belongs to a taken-over
                    // claim, and a fenced segment refuses claimless
                    // closes outright. Checked AFTER every duplicate
                    // answer above — a retry of a write that already
                    // committed is answered with its original result no
                    // matter what happened to the claim since — and
                    // BEFORE anything is staged, so a superseded
                    // operation can neither write its record nor close
                    // the segment.
                    if req.seal_gen.is_some() || req.close {
                        let fence = self
                            .seal_fences
                            .lock()
                            .unwrap()
                            .get(&hash)
                            .copied()
                            .unwrap_or(0);
                        let stale = match (req.seal_gen, req.close) {
                            (Some(g), _) => g < fence,
                            (None, true) => fence > 0,
                            (None, false) => false,
                        };
                        if stale {
                            let _ = req.resp.send(Err(AppendErr::SealSuperseded));
                            continue;
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
                        let rhash = pr.request_hash.unwrap_or([0u8; 16]);
                        local.producers.insert(
                            (req.key_hash, pr.id.clone()),
                            (pr.epoch, pr.seq, commit_last, rhash),
                        );
                        let mut v = Vec::with_capacity(40);
                        v.extend_from_slice(&pr.epoch.to_le_bytes());
                        v.extend_from_slice(&pr.seq.to_le_bytes());
                        v.extend_from_slice(&commit_last.to_le_bytes());
                        v.extend_from_slice(&rhash);
                        wb.put(producer_key(&hash, &req.key_hash, &pr.id), v);
                    }
                    if req.close {
                        local.fields.closed = true;
                    }
                    if req.entries.is_empty() {
                        pending.push((
                            req.resp,
                            Ok(AppendAck {
                                last_offset: local.fields.next.wrapping_sub(1),
                                next_offset: local.fields.next,
                                closed: local.fields.closed,
                                producer: prod_echo,
                                duplicate: false,
                            }),
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
                        local.fields.unabsorbed_bytes += frame.len() as u64;
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
                    // DURABLE billing state (§6.1), same WriteBatch as
                    // the records above. Duplicates never reach here
                    // (the producer-dedupe arm answered earlier), so a
                    // duplicate adds exactly zero by construction.
                    if let Some(bref) = &req.billing {
                        if local.billing.is_none() {
                            let loaded = match self
                                .db
                                .get(&crate::billing::billing_meta_key(&hash)[..])
                                .await
                            {
                                Ok(Some(v)) => serde_json::from_slice(&v).unwrap_or_default(),
                                _ => crate::billing::SegmentBillingMetaV1::default(),
                            };
                            local.billing = Some(loaded);
                        }
                        let bm = local.billing.as_mut().unwrap();
                        if bm.stream_id.is_empty() {
                            bm.v = 1;
                            bm.account_id = bref.identity.account_id.clone();
                            bm.project_id = bref.identity.project_id.clone();
                            bm.stream_id = bref.identity.stream_id.clone();
                            bm.stream_name = bref.identity.stream_name.clone();
                            bm.segment_id = bref.segment_id;
                        }
                        let finals = &mut local.month_finals;
                        // TRUSTED time only (round-21): `ts` above is
                        // customer record metadata; billing integrates
                        // on the server's clock.
                        let bts = crate::billing::billing_now_ms();
                        bm.advance_storage_clock(bts, |closed| {
                            finals.push(closed.to_snapshot(true));
                        });
                        bm.ingest_payload_bytes_total += pt_sum;
                        bm.ingest_records_total += req.entries.len() as u64;
                        bm.month_ingest_payload_bytes += pt_sum;
                        bm.month_ingest_records += req.entries.len() as u64;
                        bm.owned_frame_bytes_current += frame_sum;
                        bm.usage_version += 1;
                        local.billing_dirty = true;
                    }
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
                    if let Some(seq) = &req.seq {
                        local.fields.seq = Some(seq.clone());
                        local.seqs.insert(req.key_hash, seq.clone());
                        wb.put(seq_key(&hash, &req.key_hash), seq.clone().into_bytes());
                    }
                    if let Some(mut t) = req.touch {
                        t.next_offset = local.fields.next;
                        touches.push(t);
                    }
                    pending.push((
                        req.resp,
                        Ok(AppendAck {
                            last_offset: local.fields.next - 1,
                            next_offset: local.fields.next,
                            closed: local.fields.closed,
                            producer: prod_echo,
                            duplicate: false,
                        }),
                    ));
                }
                CommitOp::Absorbed {
                    upto, bytes, v2, ..
                } => {
                    let prev_absorbed = local.fields.absorbed;
                    #[cfg(test)]
                    if std::env::var("DST_DRAIN_TRACE").is_ok() {
                        eprintln!(
                            "ADVANCE {} prev={prev_absorbed} upto={upto} v2={v2} next={} trimmed={} flag={}",
                            crate::crypto::hex(&hash[..4]),
                            local.fields.next,
                            local.fields.trimmed,
                            local.fields.history_v2
                        );
                    }
                    // LAYOUT SEAL (round 4 root-cause): the first advance
                    // decides the stream's history layout FOREVER — a v2
                    // advance is only legal on a fresh boundary or an
                    // already-v2 stream, a v1 advance only while the v2
                    // flag is unset. The absorber classifies lanes from a
                    // RACY snapshot (a signal can arrive before its
                    // append's tail dispatches, so the zero-route guard
                    // briefly reads route==0 and picks v1; one tick later
                    // a stale absorbed==0 re-admits v2) — without this
                    // seal the two lanes interleave and leave a
                    // flagged-v2 stream with ranges that exist ONLY in
                    // the per-stream v1 DB: acked records the v2 read
                    // path can never see (8% I1 failures under the DST
                    // loop). A dropped advance loses no data: its range
                    // stays in the shard log below a boundary that never
                    // moved, and the SEALED lane re-absorbs it (the
                    // absorber's submitted floors are lane-scoped so the
                    // dropped lane's mark cannot make the survivor skip
                    // the range).
                    let lane_ok = if v2 {
                        prev_absorbed == 0 || local.fields.history_v2
                    } else {
                        !local.fields.history_v2
                    };
                    if !lane_ok && upto > prev_absorbed {
                        self.absorb_lane_dropped.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            shard = %self.prefix,
                            v2,
                            upto,
                            prev = prev_absorbed,
                            "dropped cross-layout absorb advance (layout sealed)"
                        );
                    }
                    // Only an op that ADVANCES the boundary may move the
                    // trim target. The absorber re-submits an already-
                    // covered `upto` when it starts a pass before the
                    // previous advance has been dispatched to handle
                    // state; letting that duplicate raise the target to
                    // `prev_absorbed` (== the live boundary) collapses
                    // the one-pass lag that in-flight readers holding a
                    // stale absorbed snapshot depend on (2026-07-27
                    // boundary-race DST failure).
                    if lane_ok && upto > prev_absorbed {
                        local.fields.absorbed = upto.min(local.fields.next);
                        local.fields.unabsorbed_bytes =
                            local.fields.unabsorbed_bytes.saturating_sub(bytes);
                        if v2 {
                            local.fields.history_v2 = true;
                        }
                        // Boundary publication and physical trimming are
                        // decoupled (unbounded-trim P0): the advance only
                        // RECORDS the new safe target — the previous
                        // absorbed boundary, one advance of reader lag —
                        // and trims inline only what the group's global
                        // budget still allows. The remainder becomes
                        // trim debt, drained by TrimTick maintenance a
                        // budgeted slice per commit.
                        local.fields.trim_safe_to = local.fields.trim_safe_to.max(prev_absorbed);
                        let allowed = trim_budget.min(cfg.max_trim_per_op);
                        let trim_to = local
                            .fields
                            .trim_safe_to
                            .min(local.fields.trimmed + allowed);
                        for off in local.fields.trimmed..trim_to {
                            wb.delete(record_key(&hash, off));
                        }
                        trim_budget -= trim_to.saturating_sub(local.fields.trimmed);
                        local.fields.trimmed = local.fields.trimmed.max(trim_to);
                    }
                }
                CommitOp::TrimStep { .. } => {
                    // Budgeted maintenance slice toward the persisted safe
                    // target. `min(absorbed)` is defensive only —
                    // trim_safe_to is always a previous absorbed boundary.
                    let target = local.fields.trim_safe_to.min(local.fields.absorbed);
                    let allowed = trim_budget.min(cfg.max_trim_per_op);
                    let trim_to = target.min(local.fields.trimmed + allowed);
                    for off in local.fields.trimmed..trim_to {
                        wb.delete(record_key(&hash, off));
                    }
                    trim_budget -= trim_to.saturating_sub(local.fields.trimmed);
                    local.fields.trimmed = local.fields.trimmed.max(trim_to);
                }
                CommitOp::Queue { op, resp, .. } => {
                    #[cfg(test)]
                    client_append_hashes.insert(hash);
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
                                        // Key layout (round 16, one
                                        // format): name 0x00 gen[8]
                                        // for cursors; name 0x00
                                        // gen[8] off[8] for leases and
                                        // ack markers. Rows of a LOWER
                                        // generation than the state
                                        // already loaded are a dead
                                        // generation's residue —
                                        // ignored, never merged; a
                                        // HIGHER generation replaces
                                        // the state wholesale.
                                        let rest = &kv.key[17..];
                                        let Some(sep) = rest.iter().position(|b| *b == 0) else {
                                            continue;
                                        };
                                        let consumer =
                                            String::from_utf8_lossy(&rest[..sep]).into_owned();
                                        let tail = &rest[sep + 1..];
                                        if tail.len() < 8 {
                                            continue;
                                        }
                                        let cgen = u64::from_be_bytes(
                                            tail[..8].try_into().unwrap_or([0; 8]),
                                        );
                                        let cs = fresh.consumers.entry(consumer).or_default();
                                        if cs.cgen > cgen {
                                            continue;
                                        }
                                        if cs.cgen < cgen {
                                            *cs = ConsumerState {
                                                cgen,
                                                ..Default::default()
                                            };
                                        }
                                        match tag {
                                            b'c' => {
                                                cs.cursor = u64::from_le_bytes(
                                                    kv.value[..8].try_into().unwrap_or([0; 8]),
                                                );
                                            }
                                            _ => {
                                                if tail.len() < 16 {
                                                    continue;
                                                }
                                                let off = u64::from_be_bytes(
                                                    tail[8..16].try_into().unwrap_or([0; 8]),
                                                );
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
                                        // A truncated scan is a FAILED
                                        // load, not a smaller queue: a
                                        // missing lease or ack row would
                                        // redeliver or double-deliver.
                                        // Do not install partial state,
                                        // do not mark loaded — the next
                                        // request retries the load.
                                        load_err = Some(e.to_string());
                                        break 'tags;
                                    }
                                }
                            }
                        }
                        // Install ONLY a complete load. On failure the
                        // handle stays loaded=false and the request
                        // below fails, so the next one reloads.
                        if load_err.is_none() {
                            local.handle.state.lock().unwrap().queue = fresh;
                        }
                    }
                    if let Some(m) = load_err {
                        let _ = resp.send(Err(m));
                        continue;
                    }
                    // Config ops (spec Stage 2 §2.2): single-row,
                    // committer-serialized so the idempotent-create
                    // compare cannot race itself. Handled before the
                    // state lock because they read the row directly.
                    let op = match op {
                        QueueOp::ConfigPut { consumer, cfg } => {
                            // Existing RECORD: the same-group OVERLAY
                            // first (a ConfigPut/Lifecycle earlier in
                            // this group is invisible to the DB behind
                            // the unwritten batch), else the durable
                            // row. Records carry {generation, state,
                            // config}; recreation over a Deleted
                            // tombstone allocates generation+1, which
                            // is what makes any dead generation's
                            // residue inert (round 16).
                            let existing: Option<ConsumerRecord> =
                                match local.queue_configs.get(&consumer) {
                                    Some(staged) => Some(staged.clone()),
                                    None => {
                                        match self.db.get(&config_key(&hash, &consumer)[..]).await {
                                            Ok(v) => v.and_then(|v| {
                                                serde_json::from_slice::<ConsumerRecord>(&v).ok()
                                            }),
                                            Err(e) => {
                                                let _ = resp.send(Err(e.to_string()));
                                                continue;
                                            }
                                        }
                                    }
                                };
                            let out = match existing {
                                Some(rec)
                                    if rec.state == ConsumerLifecycle::Active
                                        && rec.config == cfg =>
                                {
                                    QueueOut::Config {
                                        rec: Some(rec),
                                        created: false,
                                        conflict: false,
                                    }
                                }
                                Some(rec) if rec.state == ConsumerLifecycle::Active => {
                                    QueueOut::Config {
                                        rec: Some(rec),
                                        created: false,
                                        conflict: true,
                                    }
                                }
                                Some(rec) if rec.state == ConsumerLifecycle::Deleting => {
                                    // A deletion in flight owns the name
                                    // until it settles; recreating now
                                    // would race the saga's fan-out.
                                    QueueOut::Config {
                                        rec: Some(rec),
                                        created: false,
                                        conflict: true,
                                    }
                                }
                                other => {
                                    // Absent, or a Deleted tombstone:
                                    // create at a strictly higher
                                    // generation than any that ever
                                    // lived under this name.
                                    let cgen = other.map(|r| r.generation + 1).unwrap_or(1);
                                    let rec = ConsumerRecord {
                                        generation: cgen,
                                        state: ConsumerLifecycle::Active,
                                        config: cfg,
                                    };
                                    let enc = serde_json::to_vec(&rec).unwrap_or_default();
                                    wb.put(config_key(&hash, &consumer), enc);
                                    extra_writes = true;
                                    local.queue_configs.insert(consumer.clone(), rec.clone());
                                    QueueOut::Config {
                                        rec: Some(rec),
                                        created: true,
                                        conflict: false,
                                    }
                                }
                            };
                            queue_pending.push((resp, out));
                            continue;
                        }
                        QueueOp::ConfigGet { consumer } => {
                            let rec = match local.queue_configs.get(&consumer) {
                                Some(staged) => Some(staged.clone()),
                                None => {
                                    match self.db.get(&config_key(&hash, &consumer)[..]).await {
                                        Ok(v) => v.and_then(|v| serde_json::from_slice(&v).ok()),
                                        Err(e) => {
                                            let _ = resp.send(Err(e.to_string()));
                                            continue;
                                        }
                                    }
                                }
                            };
                            queue_pending.push((
                                resp,
                                QueueOut::Config {
                                    rec,
                                    created: false,
                                    conflict: false,
                                },
                            ));
                            continue;
                        }
                        QueueOp::ConfigLifecycle {
                            consumer,
                            expect_gen,
                            deleting,
                        } => {
                            let existing: Option<ConsumerRecord> =
                                match local.queue_configs.get(&consumer) {
                                    Some(staged) => Some(staged.clone()),
                                    None => {
                                        match self.db.get(&config_key(&hash, &consumer)[..]).await {
                                            Ok(v) => v.and_then(|v| {
                                                serde_json::from_slice::<ConsumerRecord>(&v).ok()
                                            }),
                                            Err(e) => {
                                                let _ = resp.send(Err(e.to_string()));
                                                continue;
                                            }
                                        }
                                    }
                                };
                            let Some(rec) = existing else {
                                let _ = resp.send(Err(
                                    "consumer_not_found: no record for lifecycle change".into(),
                                ));
                                continue;
                            };
                            if rec.generation != expect_gen {
                                let _ = resp.send(Err(format!(
                                    "consumer_generation_conflict: record gen {} != expected {}",
                                    rec.generation, expect_gen
                                )));
                                continue;
                            }
                            let target = if deleting {
                                ConsumerLifecycle::Deleting
                            } else {
                                ConsumerLifecycle::Deleted
                            };
                            let legal = matches!(
                                (rec.state, target),
                                (ConsumerLifecycle::Active, ConsumerLifecycle::Deleting)
                                    | (ConsumerLifecycle::Deleting, ConsumerLifecycle::Deleting)
                                    | (ConsumerLifecycle::Deleting, ConsumerLifecycle::Deleted)
                                    | (ConsumerLifecycle::Deleted, ConsumerLifecycle::Deleted)
                            );
                            if !legal {
                                let _ = resp.send(Err(format!(
                                    "consumer_lifecycle_conflict: {:?} -> {:?}",
                                    rec.state, target
                                )));
                                continue;
                            }
                            let mut next = rec.clone();
                            next.state = target;
                            if next != rec {
                                let enc = serde_json::to_vec(&next).unwrap_or_default();
                                wb.put(config_key(&hash, &consumer), enc);
                                extra_writes = true;
                            }
                            local.queue_configs.insert(consumer.clone(), next.clone());
                            queue_pending.push((
                                resp,
                                QueueOut::Config {
                                    rec: Some(next),
                                    created: false,
                                    conflict: false,
                                },
                            ));
                            continue;
                        }
                        QueueOp::ConfigDeleteStep {
                            consumer,
                            fence_below,
                            max_rows,
                            max_bytes,
                        } => {
                            // One BOUNDED segment-cleanup step for the
                            // deletion saga (rounds 16-17). Order
                            // matters:
                            //
                            // FENCE FIRST — from this instant, any
                            // Receive/Settle for a generation below
                            // `fence_below` refuses, including ops
                            // already sitting in this committer's
                            // queue behind us. The fence only ever
                            // ratchets up, and installing it before
                            // the fallible scans is the conservative
                            // direction: a failed cleanup leaves a
                            // fence for a deletion that IS in
                            // progress (the saga retries), never a
                            // window where a dead generation can
                            // write.
                            {
                                let mut f = self.consumer_fences.lock().unwrap();
                                let e = f.entry((hash, consumer.clone())).or_insert(0);
                                *e = (*e).max(fence_below);
                            }
                            // ...and DURABLY, in this same ordered
                            // commit. The in-memory map dies with the
                            // engine, so a shard that moves to another
                            // instance opened an EMPTY fence map and
                            // accepted a parked dead-generation
                            // Receive — re-creating lease rows after
                            // the parent deletion had already answered
                            // 204 (round-19 must-fix 3). The row is
                            // monotonic; a concurrent lower fence can
                            // never lower it because every writer is
                            // this shard's single committer.
                            {
                                let fk = crate::queue::fence_key(&hash, &consumer);
                                let cur = match self.db.get(&fk[..]).await {
                                    Ok(Some(v)) => {
                                        u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8]))
                                    }
                                    _ => 0,
                                };
                                if fence_below > cur {
                                    wb.put(&fk[..], &fence_below.to_le_bytes()[..]);
                                    extra_writes = true;
                                }
                            }
                            // PHASE 1 — enumerate, fallibly and
                            // BOUNDEDLY. Deletion stages nothing until
                            // the scans it ran succeeded (round 15 A),
                            // and it stages ONLY rows whose decoded
                            // generation is strictly below the fence:
                            // the prefix spans every generation of the
                            // name, but a name is not an identity — a
                            // recreated generation's rows are another
                            // incarnation's property (round-17 P0; the
                            // old code deleted them). Budgets bound
                            // both the staged batch (max_rows /
                            // max_bytes) and the scan itself, so a
                            // million-row residue can never build one
                            // unbounded Vec or WriteBatch — the step
                            // reports `more` and the saga steps again
                            // from the durably reduced row set.
                            let pfx_gen = |key: &[u8], pfx_len: usize| -> u64 {
                                key.get(pfx_len..pfx_len + 8)
                                    .and_then(|b| b.try_into().ok())
                                    .map(u64::from_be_bytes)
                                    // A row too short to carry a
                                    // generation cannot belong to a
                                    // LIVE one (live writers always
                                    // encode it): treat as gen 0,
                                    // i.e. dead residue.
                                    .unwrap_or(0)
                            };
                            let scan_cap = max_rows.saturating_mul(4).max(1024);
                            let mut dead: Vec<Vec<u8>> = Vec::new();
                            let mut dead_bytes = 0usize;
                            let mut scanned = 0usize;
                            let mut more = false;
                            let mut scan_err: Option<String> = None;
                            #[cfg(test)]
                            if self.take_config_scan_failure() {
                                scan_err = Some("injected config-scan failure".into());
                            }
                            if scan_err.is_none() {
                                'scans: for tag in [b'c', b'l', b'x'] {
                                    let pfx = state_prefix(&hash, tag, &consumer);
                                    match self.db.scan_prefix(&pfx[..], ..).await {
                                        Ok(mut iter) => loop {
                                            if dead.len() >= max_rows
                                                || dead_bytes >= max_bytes
                                                || scanned >= scan_cap
                                            {
                                                more = true;
                                                break 'scans;
                                            }
                                            match iter.next().await {
                                                Ok(Some(kv)) => {
                                                    scanned += 1;
                                                    if pfx_gen(&kv.key, pfx.len()) < fence_below {
                                                        dead_bytes += kv.key.len();
                                                        dead.push(kv.key.to_vec());
                                                    }
                                                }
                                                Ok(None) => break,
                                                Err(e) => {
                                                    scan_err = Some(e.to_string());
                                                    break 'scans;
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            scan_err = Some(e.to_string());
                                            break 'scans;
                                        }
                                    }
                                }
                            }
                            if let Some(e) = scan_err {
                                // Nothing staged, nothing published:
                                // this segment's rows are exactly as
                                // they were (the fence stays — it is
                                // conservative), and the saga reports
                                // the failure instead of 204.
                                let _ = resp.send(Err(format!(
                                    "consumer delete aborted: state scan failed: {e}"
                                )));
                                continue;
                            }
                            // PHASE 2 — rows an EARLIER op in this
                            // same group staged into the unwritten
                            // WriteBatch (round 15 B) — but ONLY when
                            // the in-memory state belongs to a fenced
                            // (dead) generation. A recreated
                            // generation's live state is untouchable
                            // (round-17 P0). Budget-truncation here is
                            // safe: unstaged rows become durable with
                            // this group and the NEXT step's scan
                            // finds them — they are inert behind the
                            // fence meanwhile.
                            if local.queue.is_none() {
                                local.queue =
                                    Some(local.handle.state.lock().unwrap().queue.clone());
                            }
                            let local_dead = matches!(
                                local.queue.as_ref().unwrap().consumers.get(&consumer),
                                Some(cs) if cs.cgen < fence_below
                            );
                            if local_dead {
                                let cs = local
                                    .queue
                                    .as_ref()
                                    .unwrap()
                                    .consumers
                                    .get(&consumer)
                                    .expect("checked above");
                                for off in cs.leases.keys() {
                                    if dead.len() >= max_rows || dead_bytes >= max_bytes {
                                        more = true;
                                        break;
                                    }
                                    let k = lease_key(&hash, &consumer, cs.cgen, *off);
                                    dead_bytes += k.len();
                                    dead.push(k);
                                }
                                for off in cs.acked.iter() {
                                    if dead.len() >= max_rows || dead_bytes >= max_bytes {
                                        more = true;
                                        break;
                                    }
                                    let k = ack_key(&hash, &consumer, cs.cgen, *off);
                                    dead_bytes += k.len();
                                    dead.push(k);
                                }
                                if dead.len() < max_rows && dead_bytes < max_bytes {
                                    let k = cursor_key(&hash, &consumer, cs.cgen);
                                    dead_bytes += k.len();
                                    dead.push(k);
                                } else {
                                    more = true;
                                }
                                // The dead generation leaves memory
                                // NOW even if its rows were budget-
                                // truncated: the fence makes it inert,
                                // and its durable rows remain
                                // discoverable by the next step.
                                local.queue.as_mut().unwrap().consumers.remove(&consumer);
                            }
                            // PHASE 3 — stage the bounded batch. (The
                            // parent config RECORD is not this op's
                            // business: the saga tombstones it via
                            // ConfigLifecycle only after every
                            // segment's cleanup reports complete.) A
                            // step that found NOTHING stages nothing —
                            // an empty WriteBatch is a store error,
                            // and `complete=true` over an already-
                            // clean segment is the saga's normal
                            // stabilization path.
                            let deleted_rows = dead.len() as u64;
                            if !dead.is_empty() {
                                for k in dead {
                                    wb.delete(k);
                                }
                                extra_writes = true;
                            }
                            queue_pending.push((
                                resp,
                                QueueOut::DeleteStep {
                                    complete: !more,
                                    deleted_rows,
                                },
                            ));
                            continue;
                        }
                        other => other,
                    };
                    let now = now_ms();
                    // Generation discipline for delivery-state ops
                    // (round 16). Two independent gates:
                    //   1. The engine-resident fence — a segment
                    //      cleanup earlier in this group (or any time
                    //      since this engine opened) killed
                    //      generations below it; ops for those
                    //      generations refuse, closing the in-flight
                    //      window the durable scans cannot see.
                    //   2. The group-local config overlay — a
                    //      lifecycle change staged earlier in this
                    //      group (Deleting/Deleted, or a different
                    //      generation) refuses the op before anything
                    //      is staged.
                    if let Some((cname, op_gen)) = match &op {
                        QueueOp::Receive { consumer, cgen, .. }
                        | QueueOp::Settle { consumer, cgen, .. } => Some((consumer.clone(), *cgen)),
                        _ => None,
                    } {
                        // The engine-resident fence answers instantly
                        // for anything this engine fenced itself. If it
                        // has no entry, the DURABLE row decides — this
                        // engine may be a NEW owner of a shard whose
                        // fence was installed elsewhere (round-19
                        // must-fix 3), and an empty map must never read
                        // as "no fence". Loaded once per
                        // (segment, consumer) and cached, so the hot
                        // path stays a map lookup.
                        let fenced = {
                            let cached = {
                                let f = self.consumer_fences.lock().unwrap();
                                f.get(&(hash, cname.clone())).copied()
                            };
                            match cached {
                                Some(min_live) => op_gen < min_live,
                                None => {
                                    let fk = crate::queue::fence_key(&hash, &cname);
                                    let durable = match self.db.get(&fk[..]).await {
                                        Ok(Some(v)) => {
                                            u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8]))
                                        }
                                        // A failed read must NOT be
                                        // read as "unfenced": refuse
                                        // the op and let the client
                                        // retry (fail closed).
                                        Ok(None) => 0,
                                        Err(e) => {
                                            let _ = resp.send(Err(format!(
                                                "consumer_fence_unverified: {e}"
                                            )));
                                            continue;
                                        }
                                    };
                                    self.consumer_fences
                                        .lock()
                                        .unwrap()
                                        .insert((hash, cname.clone()), durable);
                                    op_gen < durable
                                }
                            }
                        };
                        if fenced {
                            let _ = resp.send(Err(format!(
                                "consumer_generation_fenced: generation {op_gen} was deleted"
                            )));
                            continue;
                        }
                        if let Some(staged) = local.queue_configs.get(&cname) {
                            if staged.state != ConsumerLifecycle::Active
                                || staged.generation != op_gen
                            {
                                let _ = resp.send(Err(format!(
                                    "consumer_not_found: generation {op_gen} is not the \
                                     active record in this commit group"
                                )));
                                continue;
                            }
                        }
                    }
                    if local.queue.is_none() {
                        local.queue = Some(local.handle.state.lock().unwrap().queue.clone());
                    }
                    let out = {
                        let st_queue = local.queue.as_mut().unwrap();
                        match op {
                            QueueOp::Receive {
                                consumer,
                                cgen,
                                max,
                                visibility_ms,
                                max_deliveries,
                                keys,
                                covered_to,
                            } => {
                                let cs = st_queue.consumers.entry(consumer.clone()).or_default();
                                // Generation binding: fresh state binds
                                // to the op's generation; state from a
                                // NEWER generation makes this op stale
                                // (refuse); state from an OLDER one is
                                // pre-cleanup residue in memory only —
                                // replace it (its durable rows are the
                                // cleanup's job, and its generation is
                                // fenced anyway).
                                if cs.cgen == 0 {
                                    cs.cgen = cgen;
                                } else if cs.cgen > cgen {
                                    let _ = resp.send(Err(format!(
                                        "consumer_generation_fenced: generation {cgen} \
                                         superseded by {}",
                                        cs.cgen
                                    )));
                                    continue;
                                } else if cs.cgen < cgen {
                                    *cs = ConsumerState {
                                        cgen,
                                        ..Default::default()
                                    };
                                }
                                let mut leased = Vec::new();
                                let mut poisoned: Vec<(u64, u32, u32, [u8; 16])> = Vec::new();
                                // Per-key FIFO (spec Stage 2 §2.3): a key
                                // with an ACTIVE lease anywhere blocks its
                                // later records; a batch leases at most one
                                // record per key.
                                let mut blocked: std::collections::HashSet<[u8; 16]> = cs
                                    .leases
                                    .values()
                                    .filter(|l| l.deadline_ms > now)
                                    .map(|l| l.key_hash)
                                    .collect();
                                let mut off = cs.cursor;
                                let mut steps = 0usize;
                                while off < local.fields.next
                                    && leased.len() < max
                                    && steps < max * 8 + 4096
                                {
                                    steps += 1;
                                    if off >= covered_to {
                                        break;
                                    }
                                    // Never lease a record whose key is
                                    // unknown — it could jump a blocked
                                    // key's queue.
                                    let Some(kh) = keys.get(&off).copied() else {
                                        break;
                                    };
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
                                            // Report; the HTTP layer appends
                                            // to the DLQ stream durably
                                            // FIRST, then acks (spec §2.8).
                                            // The key stays blocked
                                            // meanwhile.
                                            poisoned.push((off, l.lease_gen, l.delivery_count, kh));
                                            blocked.insert(kh);
                                            off += 1;
                                            continue;
                                        }
                                    }
                                    if blocked.contains(&kh) {
                                        off += 1;
                                        continue;
                                    }
                                    let lease = Lease {
                                        deadline_ms: now + visibility_ms as i64,
                                        delivery_count: prev.map(|l| l.delivery_count).unwrap_or(0)
                                            + 1,
                                        lease_gen: prev.map(|l| l.lease_gen).unwrap_or(0) + 1,
                                        key_hash: kh,
                                    };
                                    wb.put(
                                        lease_key(&hash, &consumer, cgen, off),
                                        encode_lease(&lease),
                                    );
                                    extra_writes = true;
                                    cs.leases.insert(off, lease);
                                    blocked.insert(kh);
                                    leased.push((off, lease.lease_gen, lease.delivery_count, kh));
                                    off += 1;
                                }
                                // Advance cursor over settled prefix.
                                while cs.acked.remove(&cs.cursor) {
                                    wb.delete(ack_key(&hash, &consumer, cgen, cs.cursor));
                                    cs.cursor += 1;
                                    extra_writes = true;
                                }
                                wb.put(
                                    cursor_key(&hash, &consumer, cgen),
                                    cs.cursor.to_le_bytes().to_vec(),
                                );
                                let backlog = (local.fields.next - cs.cursor)
                                    .saturating_sub(cs.acked.len() as u64);
                                QueueOut::Received {
                                    leased,
                                    backlog,
                                    poisoned,
                                }
                            }
                            QueueOp::Settle {
                                consumer,
                                cgen,
                                acks,
                                retries,
                                extends,
                                max_deliveries,
                            } => {
                                let cs = st_queue.consumers.entry(consumer.clone()).or_default();
                                if cs.cgen == 0 {
                                    cs.cgen = cgen;
                                } else if cs.cgen > cgen {
                                    let _ = resp.send(Err(format!(
                                        "consumer_generation_fenced: generation {cgen} \
                                         superseded by {}",
                                        cs.cgen
                                    )));
                                    continue;
                                } else if cs.cgen < cgen {
                                    *cs = ConsumerState {
                                        cgen,
                                        ..Default::default()
                                    };
                                }
                                let (mut a, mut r, mut e2, mut dq, mut stale) =
                                    (0usize, 0usize, 0usize, 0usize, 0usize);
                                let mut poisoned: Vec<(u64, u32, u32, [u8; 16])> = Vec::new();
                                for (off, tok_gen) in acks {
                                    if cs.leases.get(&off).map(|l| l.lease_gen) == Some(tok_gen) {
                                        cs.leases.remove(&off);
                                        wb.delete(lease_key(&hash, &consumer, cgen, off));
                                        cs.acked.insert(off);
                                        wb.put(ack_key(&hash, &consumer, cgen, off), b"");
                                        extra_writes = true;
                                        a += 1;
                                    } else {
                                        // Stale tokens are counted, never
                                        // errors, and cannot touch a newer
                                        // lease generation (spec §2.5/§2.7).
                                        stale += 1;
                                    }
                                }
                                for (off, tok_gen, delay) in retries {
                                    if let Some(l) = cs.leases.get(&off).copied() {
                                        if l.lease_gen != tok_gen {
                                            stale += 1;
                                            continue;
                                        }
                                        if l.delivery_count >= max_deliveries {
                                            // Report only: the DLQ-stream
                                            // append precedes the source
                                            // settle (spec §2.8); the lease
                                            // stays so the later ack still
                                            // gen-matches.
                                            poisoned.push((
                                                off,
                                                l.lease_gen,
                                                l.delivery_count,
                                                l.key_hash,
                                            ));
                                            dq += 1;
                                            continue;
                                        } else {
                                            let nl = Lease {
                                                deadline_ms: now + delay as i64,
                                                ..l
                                            };
                                            cs.leases.insert(off, nl);
                                            wb.put(
                                                lease_key(&hash, &consumer, cgen, off),
                                                encode_lease(&nl),
                                            );
                                            r += 1;
                                        }
                                        extra_writes = true;
                                    } else {
                                        stale += 1;
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
                                                lease_key(&hash, &consumer, cgen, off),
                                                encode_lease(&nl),
                                            );
                                            extra_writes = true;
                                            e2 += 1;
                                        } else {
                                            stale += 1;
                                        }
                                    } else {
                                        stale += 1;
                                    }
                                }
                                while cs.acked.remove(&cs.cursor) {
                                    wb.delete(ack_key(&hash, &consumer, cgen, cs.cursor));
                                    cs.cursor += 1;
                                    extra_writes = true;
                                }
                                wb.put(
                                    cursor_key(&hash, &consumer, cgen),
                                    cs.cursor.to_le_bytes().to_vec(),
                                );
                                let backlog = (local.fields.next - cs.cursor)
                                    .saturating_sub(cs.acked.len() as u64);
                                QueueOut::Settled {
                                    acked: a,
                                    retried: r,
                                    extended: e2,
                                    dlq: dq,
                                    backlog,
                                    stale,
                                    poisoned,
                                }
                            }
                            QueueOp::ConfigPut { .. }
                            | QueueOp::ConfigGet { .. }
                            | QueueOp::ConfigLifecycle { .. }
                            | QueueOp::ConfigDeleteStep { .. } => {
                                unreachable!("config ops handled before the state lock")
                            }
                        }
                    };
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
                || f.trim_safe_to != b.trim_safe_to
                || f.unabsorbed_bytes != b.unabsorbed_bytes
                || f.seq != b.seq
                || f.closed != b.closed
            {
                wb.put(tail_key(hash), encode_tail(f));
                // Dirty-stream index: marker present iff the stream has
                // outstanding maintenance — an unabsorbed tail or pending
                // physical trim — maintained atomically with the tail it
                // describes. (The value carries the absorb view; a
                // trim-only marker reads absorbed == next, and the
                // scanner reads the tail for the full state anyway.)
                let was_marked = b.absorbed < b.next || b.trimmed < b.trim_safe_to;
                let is_marked = f.absorbed < f.next || f.trimmed < f.trim_safe_to;
                if is_marked {
                    wb.put(dirty_key(hash), dirty_value(f.absorbed, f.next));
                } else if was_marked {
                    wb.delete(dirty_key(hash));
                }
                changed = true;
            }
            // Billing rows (§6.1/§6.3), atomic with everything above:
            // the meta row, its dirty marker (value = the version the
            // drainer must acknowledge), and any month-final snapshots
            // the storage clock closed in this group.
            if local.billing_dirty {
                if let Some(bm) = &local.billing {
                    wb.put(
                        crate::billing::billing_meta_key(hash),
                        serde_json::to_vec(bm).unwrap_or_default(),
                    );
                    wb.put(
                        crate::billing::usage_dirty_key(hash),
                        &bm.usage_version.to_le_bytes()[..],
                    );
                    changed = true;
                }
                for snap in &local.month_finals {
                    if let Some((y, m)) = crate::billing::parse_month(&snap.month) {
                        wb.put(
                            crate::billing::usage_month_final_key(hash, y, m),
                            serde_json::to_vec(snap).unwrap_or_default(),
                        );
                    }
                }
            }
            tails.push((local.handle.clone(), f.clone()));
            if local.appended_bytes > 0 {
                signals.push(AbsorbSignal {
                    hash: *hash,
                    appended_bytes: local.appended_bytes,
                });
            }
        }
        // Fence responses join the barrier that actually covers them:
        // a group with writes carries them itself; a fence-only group
        // attaches them to the NEWEST in-flight group (everything the
        // fence observed was applied by groups at or before it); with
        // nothing in flight and nothing staged, the observed state is
        // already durable and the answer goes out now.
        if !fence_acks.is_empty() {
            let group_writes = changed || records > 0 || !touches.is_empty() || extra_writes;
            if group_writes {
                pending.append(&mut fence_acks);
            } else {
                let mut infl = self.in_flight.lock().unwrap();
                if let Some(last) = infl.last_mut() {
                    last.acks.append(&mut fence_acks);
                } else {
                    drop(infl);
                    for (resp, res) in fence_acks.drain(..) {
                        let _ = resp.send(res);
                    }
                }
            }
        }
        if pending.is_empty() && !changed && queue_pending.is_empty() && !extra_writes {
            // extra_writes: an ack-only or maintenance-only group has no
            // pending responses and no tail movement, but its direct
            // WriteBatch entries (usage-outbox acks, fence rows) are
            // real writes that must reach the store.
            return;
        }
        if !changed && records == 0 && touches.is_empty() && !extra_writes {
            // Nothing persisted by THIS group — but the truths in these
            // acks (duplicate answers, idempotent closes, zero-entry
            // tails) were read from batch-local or applied state that
            // an EARLIER group may not have made durable yet. They join
            // the newest in-flight barrier, exactly like a fence-only
            // group; only when nothing is in flight is the observed
            // state already durable and the answer immediate.
            let mut infl = self.in_flight.lock().unwrap();
            if let Some(last) = infl.last_mut() {
                last.acks.append(&mut pending);
                last.queue_acks.append(&mut queue_pending);
            } else {
                drop(infl);
                for (resp, res) in pending {
                    let _ = resp.send(res);
                }
                for (resp, out) in queue_pending {
                    let _ = resp.send(Ok(out));
                }
            }
            return;
        }

        let encode_us = group_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
        let group_bytes: u64 = locals.iter().map(|(_, l)| l.appended_bytes).sum();
        // Deterministic group-write failure (DST): everything this
        // group promised — acks, duplicates, idempotent closes,
        // state-dependent refusals, fence reports — fails together,
        // through the exact same lines a real write error takes.
        #[cfg(test)]
        {
            let tripped = {
                let mut armed = self.fail_group_for.lock().unwrap();
                match armed.as_mut() {
                    Some(set) => {
                        let hit: Vec<[u8; 16]> = set
                            .iter()
                            .filter(|h| client_append_hashes.contains(*h))
                            .copied()
                            .collect();
                        for h in &hit {
                            set.remove(h);
                        }
                        !hit.is_empty()
                    }
                    None => false,
                }
            };
            if tripped {
                self.fail_group_tripped
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Through the SAME sender the real write-error arm
                // uses: a future change to the production failure path
                // is exercised by every DST scenario, not silently
                // diverged from.
                Self::send_group_failure("failpoint: group write failed", pending, queue_pending);
                return;
            }
        }
        let write_t0 = std::time::Instant::now();
        // Publish the write start so admission can observe a blocked commit
        // pipeline (L0-full / unflushed-full backpressure blocks this await;
        // 2026-07-21: an 8-minute block stranded every in-flight append into
        // the platform front door's 30 s kill). Cleared on completion.
        self.commit_write_started_ms
            .store(now_ms(), Ordering::SeqCst);
        let res = self
            .db
            .write_with_options(wb, &WriteOptions::default())
            .await;
        self.commit_write_started_ms.store(0, Ordering::SeqCst);
        let write_us = write_t0.elapsed().as_micros().min(u32::MAX as u128) as u32;

        match res {
            Ok(handle) => {
                for (_, local) in &locals {
                    let mut st = local.handle.state.lock().unwrap();
                    st.applied = local.fields.clone();
                    for (plane, v) in &local.producers {
                        st.producers.insert(plane.clone(), *v);
                    }
                    for (kh, v) in &local.seqs {
                        st.seqs.insert(*kh, v.clone());
                    }
                    if let Some(q) = &local.queue {
                        st.queue = (*q).clone();
                    }
                }
                // Wake Applied-mode readers now that the state above is
                // published; durable waiters are woken only at dispatch.
                for (_, local) in &locals {
                    local.handle.applied_notify.notify_waiters();
                }
                // Trim-debt bookkeeping: streams whose safe target still
                // leads their trim cursor stay in (or enter) the
                // maintenance set; caught-up streams leave it. Advisory
                // state — a stale entry is a no-op TrimStep that then
                // self-removes.
                {
                    let mut debt = self.trim_debt.lock().unwrap();
                    for (hash, local) in &locals {
                        if local.fields.trimmed < local.fields.trim_safe_to {
                            debt.insert(*hash);
                        } else {
                            debt.remove(hash);
                        }
                    }
                }
                let trim_used = cfg.trim_global_budget.saturating_sub(trim_budget);
                if trim_used > 0 {
                    self.trim_deletes_last.store(trim_used, Ordering::Relaxed);
                    self.trim_deletes_max_batch
                        .fetch_max(trim_used, Ordering::Relaxed);
                    self.trim_deletes_total
                        .fetch_add(trim_used, Ordering::Relaxed);
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
                Self::send_group_failure(&e.to_string(), pending, queue_pending);
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
                self.ring_budget
                    .fetch_add(b.bytes as i64, Ordering::Relaxed);
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

    /// Test hook: fail the next commit group that contains an op for
    /// the given segment identity — the deterministic stand-in for a
    /// WriteBatch that reaches the store and dies. One-shot; the
    /// tripped counter is the entered-proof a test asserts instead of
    /// assuming its failpoint fired.
    /// Test hook: hold the COMMIT gate. While held, the committer
    /// takes at most one op and then parks before gathering; releasing
    /// the guard lets it drain everything queued meanwhile into ONE
    /// commit group. The companion to `fail_next_group_for` for
    /// deterministic same-group scenarios.
    #[cfg(test)]
    pub async fn test_hold_commit(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_gate.lock().await
    }

    /// Entered-proof for group-composition tests: client ops — appends,
    /// fences, and queue submissions — enqueued on this engine so far. A test polls the delta
    /// instead of sleeping and hoping its request made the queue.
    #[cfg(test)]
    pub fn appends_enqueued(&self) -> u64 {
        self.appends_enqueued
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test-only ground truth for deletion tests: the number of
    /// DURABLE state rows (cursor/lease/ack, every generation) this
    /// consumer still has under this identity. The generation model
    /// makes leaked residue invisible to behavioral asserts — a
    /// recreated consumer ignores dead generations by design — so
    /// burial is proven by counting rows, not by pulling.
    #[cfg(test)]
    /// Test-only residue factory: durable lease rows under an OLD
    /// consumer generation, modeling the multi-generation residue a
    /// crashed/raced deletion leaves behind (round-17 stress gate).
    #[cfg(test)]
    pub async fn seed_consumer_residue_rows(
        &self,
        hash: [u8; 16],
        consumer: &str,
        cgen: u64,
        n: u64,
    ) -> Result<(), String> {
        let mut wb = WriteBatch::new();
        for off in 0..n {
            wb.put(
                crate::queue::lease_key(&hash, consumer, cgen, off),
                crate::queue::encode_lease(&crate::queue::Lease {
                    deadline_ms: 0,
                    delivery_count: 1,
                    lease_gen: 1,
                    key_hash: [0u8; 16],
                }),
            );
        }
        self.db
            .write_with_options(wb, &slatedb::config::WriteOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub async fn count_consumer_state_rows(
        &self,
        hash: [u8; 16],
        consumer: &str,
    ) -> Result<usize, String> {
        let mut n = 0usize;
        for tag in [b'c', b'l', b'x'] {
            let pfx = crate::queue::state_prefix(&hash, tag, consumer);
            let mut iter = self
                .db
                .scan_prefix(&pfx[..], ..)
                .await
                .map_err(|e| e.to_string())?;
            loop {
                match iter.next().await {
                    Ok(Some(_)) => n += 1,
                    Ok(None) => break,
                    Err(e) => return Err(e.to_string()),
                }
            }
        }
        Ok(n)
    }

    /// One-shot: the next ConfigDelete's state-row scan reports a
    /// failure at the scan boundary — the deterministic stand-in for a
    /// store error mid-enumeration. The contract under test: a failed
    /// scan stages NOTHING and the consumer is untouched.
    #[cfg(test)]
    pub fn fail_next_config_scan(&self) {
        self.fail_config_scan
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_config_scan_failure(&self) -> bool {
        self.fail_config_scan
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub fn fail_next_group_for(&self, identity: [u8; 16]) {
        self.fail_group_for
            .lock()
            .unwrap()
            .get_or_insert_with(std::collections::HashSet::new)
            .insert(identity);
    }

    /// Fence-map observability (round 12): the map is deliberately
    /// unbounded (no wall-clock expiry can be proven safe against a
    /// queue with no residence bound), so its cardinality must be
    /// visible before it could ever become material.
    pub fn seal_fence_stats(&self) -> (usize, u64) {
        let f = self.seal_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Usage-dirty index scan (§6.3): every segment whose durable
    /// billing state has versions `_usage` has not acknowledged.
    /// (hash, unacked version). One prefix scan; the drainer's
    /// discovery path after restart or ownership move.
    pub async fn usage_dirty_scan(&self) -> anyhow::Result<Vec<([u8; 16], u64)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'U');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != 33 || kv.value.len() < 8 {
                continue;
            }
            let mut h = [0u8; 16];
            h.copy_from_slice(&kv.key[17..33]);
            let v = u64::from_le_bytes(kv.value[..8].try_into().unwrap());
            out.push((h, v));
        }
        Ok(out)
    }

    /// The durable billing row for one segment (None = never billed).
    pub async fn billing_meta(
        &self,
        hash: [u8; 16],
    ) -> Option<crate::billing::SegmentBillingMetaV1> {
        self.db
            .get(&crate::billing::billing_meta_key(&hash)[..])
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
    }

    /// Closed-month final snapshots awaiting ledger acknowledgment
    /// (sentinel-'V' rows): (exact key, snapshot).
    pub async fn usage_month_finals(
        &self,
    ) -> anyhow::Result<Vec<(Vec<u8>, crate::billing::SegmentSnapshot)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'V');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if let Ok(snap) = serde_json::from_slice(&kv.value) {
                out.push((kv.key.to_vec(), snap));
            }
        }
        Ok(out)
    }

    /// Acknowledge `_usage` durability for a segment's snapshot at
    /// `version` (+ exact month-final rows). Fire-and-forget through
    /// the committer — see CommitOp::UsageAck.
    pub fn submit_usage_ack(&self, hash: [u8; 16], version: u64, month_final_keys: Vec<Vec<u8>>) {
        let _ = self.tx.try_send(CommitOp::UsageAck {
            hash,
            version,
            month_final_keys,
        });
    }

    /// Terminal storage closure for a hard-deleted or expired segment
    /// (§6.2), accounted to the persisted logical close instant.
    /// AWAITED submission (round-22 item 7): the caller knows whether
    /// the closure entered the committer queue — a full queue is
    /// backpressure, never a silent drop; the registry-persisted debt
    /// plus the sweep reconciler retry anything that still fails.
    pub async fn submit_billing_close(&self, hash: [u8; 16], close_ms: i64) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingClose { hash, close_ms })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }

    /// Durably persist the fork-retention flag on the billing row
    /// (round-22 item 7); awaited like the closure.
    pub async fn submit_billing_retained(
        &self,
        hash: [u8; 16],
        retained: bool,
    ) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingRetained { hash, retained })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }

    /// Consumer-fence cardinality (round 17): one non-expiring entry
    /// per (segment identity, deleted consumer name) — correct for
    /// safety, so its growth is surfaced instead of hidden. Any future
    /// cleanup must be proved by committer-queue progress, exactly like
    /// the seal fences; never wall-clock expiry.
    pub fn consumer_fence_stats(&self) -> (usize, u64) {
        let f = self.consumer_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Test hook: empty the engine-resident fence map WITHOUT touching
    /// the durable rows — the deterministic stand-in for "this shard
    /// just moved to another instance, which opened a fresh engine".
    /// The durable fence must still refuse dead generations.
    #[cfg(test)]
    pub fn forget_consumer_fences_for_test(&self) {
        self.consumer_fences.lock().unwrap().clear();
    }

    /// Test view of the DURABLE fence row (None = no row).
    #[cfg(test)]
    pub async fn durable_consumer_fence(&self, hash: [u8; 16], consumer: &str) -> Option<u64> {
        let k = crate::queue::fence_key(&hash, consumer);
        self.db
            .get(&k[..])
            .await
            .ok()
            .flatten()
            .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
    }

    #[cfg(test)]
    pub fn group_failures_tripped(&self) -> usize {
        self.fail_group_tripped
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test hook: hold the dispatch gate. While held, NEITHER the acker
    /// nor the pump can dispatch acks — the deterministic stand-in for
    /// "the acker is paused after durability, before response dispatch".
    #[cfg(test)]
    pub async fn test_hold_dispatch(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.dispatch_gate.lock().await
    }

    /// Release everything the durable watermark now covers: record
    /// timings, publish tail state, send producer/queue acks, feed the
    /// absorber and touch journals. Entirely synchronous, so the caller
    /// can rely on "when this returns, the acks are on their way" — the
    /// property the pump's gather window is built on. Returns requests
    /// dispatched. Called from the acker (watch-driven failsafe + the
    /// only path when the pump is off) and from the pump (explicit
    /// barrier right after its flush returns).
    async fn dispatch_durable(&self, durable_seq: u64) -> u32 {
        let _order = self.dispatch_gate.lock().await;
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
                let wait_us = group.written_at.elapsed().as_micros().min(u32::MAX as u128) as u32;
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
            for (resp, res) in group.acks {
                let _ = resp.send(res);
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
            self.dispatch_durable(durable_seq).await;
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
    deliver: Deliver,
) -> Result<FrameReadResult, slatedb::Error> {
    let (hash, end) = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            Deliver::Durable => st.durable.next,
            // max() is defensive: `applied` loads equal to `durable`
            // and only the committer advances it, but a floor here
            // means Applied can never see LESS than a durable reader.
            Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        (handle.hash, end)
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
    // DURABLE reads: a key_filter changes which frames belong in the
    // result, and the ring holds only durable frames — an Applied read
    // chasing the just-applied suffix must scan (the suffix is
    // memtable-resident, so the scan costs no store round-trip).
    if key_filter.is_none() && deliver == Deliver::Durable {
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
                durability_filter: match deliver {
                    Deliver::Durable => DurabilityLevel::Remote,
                    Deliver::Applied => DurabilityLevel::Memory,
                },
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
