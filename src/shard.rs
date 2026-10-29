//! Shard log engine: one SlateDB per shard, hash-first keyspace, committer +
//! durable-watermark acker (§3.4). Records are stored as layout 5 pages: the
//! committer seals each request's records into pages after offset
//! assignment, because the authenticated header carries the first offset.
//!
//! Keyspace (hash-first so a hash range is one contiguous split range):
//!   `<hash16> 't'`                      tail state
//!   `<hash16> 'p' <last offset u64 BE>` record page

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};
use tokio::sync::{Notify, mpsc, oneshot};

pub(crate) mod record;
pub(crate) use record::FrameReadResult;
#[cfg(test)]
pub(crate) use record::read_frames;
mod billing_ops;
use billing_ops::BillingReply;
mod commit_handoff;
use commit_handoff::{Attachment, CommitHandoff};
pub(crate) use tail_ring::RingScan;
mod commit_plan;
mod lane_rows;
mod lifecycle;
mod maintenance_row;
mod resident;
mod storage_close;
mod tail_ring;
mod transaction;
pub(crate) use commit_plan::{AppendFinish, CloseReq, EnqueueError, SealFenceReq, UsageAckScope};
use commit_plan::{
    BillingAckDecision, ConsumerGeneration, DurableEffects, ProducerDecision, decide_billing_ack,
    decide_consumer_generation, decide_producer, seal_authorized,
};
use lane_rows::{decode_producer_row, decode_seq_row, encode_producer_row};
pub(crate) use lifecycle::EngineShutdown;
mod handle_load;
mod layout;
pub(crate) mod seal;
#[cfg(test)]
pub(crate) use layout::StaticBilling;
mod rows;
#[cfg(test)]
use rows::decode_tail;
pub(crate) use rows::{
    TailFields, decode_cursor, producer_key, queue_tail_key, seal_fence_key, seq_key, tail_key,
};
use rows::{encode_tail, stored_tail};

/// Exact frame-byte flow, in the maintenance unit (R25-B). These are
/// process-lifetime observability counters, NOT admission inputs — the
/// admission source of truth is each engine's ledger.
pub(crate) static INGEST_FRAME_BYTES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static ABSORBED_FRAME_BYTES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Engine-owned maintenance state for one physical shard (R25-A).
///
/// The unit is EXACT ENCODED FRAME BYTES in `[absorbed, next)` — the
/// same unit `TailFields.unabsorbed_bytes` carries. The R24 version of
/// this accounting added uncompressed PAYLOAD bytes on append while
/// retiring compressed FRAME bytes on absorption; on the soak's all-`x`
/// records with FRAME_COMPRESS=1 that manufactured a 9.4% "absorption
/// ratio" and 3.87 GB of fictional backlog that were, in large part,
/// the benchmark's compression ratio. One unit, both directions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardMaintenance {
    pub version: u64,
    /// Exact encoded frame bytes still present in the shard tier and
    /// not covered by the durable absorbed boundary.
    pub unabsorbed_frame_bytes: u64,
    /// When the backlog moved 0 -> nonzero. Diagnostic only; it may
    /// conservatively predate the current oldest record.
    pub backlog_started_ms: i64,
    /// Last successful durable retirement of backlog bytes. This is the
    /// safe stall signal: unlike an "oldest record" approximation it
    /// does not become permanently old while absorption keeps making
    /// progress under continuous traffic.
    pub last_progress_ms: i64,
}

/// This shard's maintenance ledger, rebuilt from its tails before the
/// engine starts serving (layout 6: the ledger lives in memory only): the
/// unsealed bytes every stream's durable tail holds. A tail that does not
/// decode is left out and logged (its stream fails alone when loaded); a
/// store error fails the open.
pub(crate) async fn load_or_rebuild_maintenance(db: &Db) -> anyhow::Result<ShardMaintenance> {
    let mut total = 0u64;
    let mut rows = db.scan_prefix(&[crate::keyspace::STREAM][..], ..).await?;
    while let Some(row) = rows.next().await? {
        if row.key.len() != 18 || row.key.last() != Some(&b't') {
            continue;
        }
        let unsealed = match stored_tail(&row.value) {
            Ok(tail) => tail.unabsorbed_bytes,
            Err(e) => {
                tracing::error!("maintenance rebuild skipped a tail it cannot decode: {e}");
                continue;
            }
        };
        total = total
            .checked_add(unsealed)
            .ok_or_else(|| anyhow::anyhow!("maintenance rebuild overflow"))?;
    }
    let now = now_ms();
    Ok(ShardMaintenance {
        version: 1,
        unabsorbed_frame_bytes: total,
        backlog_started_ms: if total > 0 { now } else { 0 },
        last_progress_ms: if total > 0 { now } else { 0 },
    })
}

/// Read visibility level. `Durable` (the pinned default everywhere)
/// serves only storage-durable records. `Applied` additionally serves
/// the live tail's applied-but-not-yet-durable suffix — the product
/// surface's opt-in low-latency subscribe mode. Applied is a READ-SIDE
/// clamp only: acks, consumers, watches, absorption and trim stay
/// durable-gated exactly as before.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum Deliver {
    #[default]
    Durable,
    Applied,
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub(crate) struct StreamState {
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
    pub producers: ProducerRows,
    /// Per-routing-key Stream-Seq lanes (ROUTING-V3 §3.6), loaded
    /// lazily from the durable `s` rows and applied by the committer.
    pub seqs: HashMap<[u8; 16], String>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
    /// Rows the committer keeps resident (`resident::HotRows`).
    pub hot: resident::HotRows,
    /// The stream's applied unsealed pages (layout 6): every page in
    /// `[applied.absorbed, applied.next)`, never evicted while any is held.
    pub pending: crate::spill::pending::PendingPages,
}

pub(crate) struct StreamHandle {
    /// Physical database opening that admitted this handle and its durable ring.
    owner: std::sync::Weak<Db>,
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
    /// Round-13: this stream incarnation's durable-write pressure
    /// attribution (per-project memory admission). Bound once per
    /// resident handle from the tenant-qualified append path; the
    /// committer attributes group deltas at publish-on-success; Drop
    /// (idle eviction, shard close, owner movement) releases this
    /// instance's attribution exactly.
    pub pressure: std::sync::OnceLock<std::sync::Arc<crate::quota::StreamPressureBinding>>,
    /// Whether this stream is a native queue, whose tail row lives in the
    /// queue segment (layout 6): found there at load, or set by the first
    /// native-queue operation staged on it.
    pub native_queue: std::sync::atomic::AtomicBool,
}

impl StreamHandle {
    /// The key of this stream's tail row.
    pub(crate) fn tail_row_key(&self) -> Vec<u8> {
        if self.native_queue.load(std::sync::atomic::Ordering::Relaxed) {
            queue_tail_key(&self.hash)
        } else {
            tail_key(&self.hash)
        }
    }
}

impl StreamHandle {
    /// Bind (idempotently) under the state lock: the seed is the
    /// APPLIED tail's exact unabsorbed_bytes at this instant, and the
    /// lock ordering against the committer's publish site guarantees
    /// the seed and the group-delta attribution never double- or
    /// under-count a group.
    #[expect(
        clippy::unwrap_used,
        reason = "StreamHandle::bind_pressure; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    pub(crate) fn bind_pressure(&self, adm: std::sync::Arc<crate::quota::ProjectAdmission>) {
        if self.pressure.get().is_some() {
            return;
        }
        let st = self.state.lock().unwrap();
        let _ = self.pressure.get_or_init(|| {
            std::sync::Arc::new(crate::quota::StreamPressureBinding::bind(
                adm,
                st.applied.unabsorbed_bytes,
            ))
        });
        drop(st);
    }
}

/// One durably-committed group's pages for one stream: a contiguous
/// offset range [first, next) in publish order, each page as (last offset,
/// stored bytes).
#[derive(Clone, Default)]
pub(crate) struct RingBatch {
    pub first: u64,
    pub next: u64,
    pub frames: Vec<(u64, Bytes)>,
    pub bytes: usize,
}

impl RingBatch {
    /// Add the page holding `[first, last]`, the next one of the batch.
    fn push_page(&mut self, first: u64, last: u64, page: Bytes) {
        if self.frames.is_empty() {
            self.first = first;
        }
        debug_assert_eq!(
            self.next.max(self.first),
            first,
            "ring pages are contiguous"
        );
        self.next = last + 1;
        self.bytes += page.len();
        self.frames.push((last, page));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct TailRing {
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
pub(crate) struct TouchFeed {
    pub journal: Arc<crate::touch::TouchJournal>,
    pub key_ids: Vec<u32>,
    pub next_offset: u64,
}

/// Producer identities the SERVER mints for records a client did not
/// coordinate (a seal's final append, a raw close carrying content).
/// They live in the same durable keyspace as public ones, so the wire
/// parser refuses this prefix — otherwise a caller could pre-create the
/// row and turn a later final append into a false duplicate.
pub(crate) const INTERNAL_PRODUCER_PREFIX: &str = "\u{0}prisma-internal\u{0}";

#[derive(Debug, Clone)]
pub(crate) struct ProducerReq {
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
pub(crate) enum DeferredErr {
    CtMismatch,
    BadBody(String),
}

/// Why a new producer sequence must not be accepted right now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SealedReject {
    Sealing,
    Sealed,
}

pub(crate) struct AppendReq {
    pub hash: [u8; 16],
    /// Shard-routing identity (`stream_hash(name)`), persisted into the
    /// tail so history v2 can key the shared partition route-first and a
    /// future shard split can clone by range. Zeros when the caller has
    /// no name-level identity (some DST paths); the v2 keyspace accepts
    /// a zero route, it just can't range-split those entries.
    pub route: [u8; 16],
    pub enqueued_at: std::time::Instant,
    /// Plaintext entries; encrypted in the committer with fresh stored nonces.
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
    pub finish: AppendFinish,
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
pub(crate) struct AppendAck {
    pub last_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    /// Echoed on producer appends: (epoch, seq to report).
    pub producer: Option<(u64, u64)>,
    /// True for producer duplicates (204, body ignored).
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum AppendErr {
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

/// Producer rows keyed by (key hash, producer id): epoch, sequence, offset and request hash.
pub(crate) type ProducerRows = HashMap<([u8; 16], String), (u64, u64, u64, [u8; 16])>;
/// The lowest live consumer generation per (stream, consumer).
type ConsumerFences = Mutex<HashMap<([u8; 16], String), u64>>;

mod commit_op;
pub(crate) use commit_op::CommitOp;
mod native_queue_submit;
#[cfg(test)]
mod native_queue_tests;
mod queue_maintenance;

#[derive(Clone)]
pub(crate) struct ShardConfig {
    pub queue_reqs: usize,
    pub max_batch_reqs: usize,
    pub max_batch_bytes: usize,
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
    /// The canonical read path (unsealed pages, then the segment objects)
    /// remains the source of truth for anything the ring no longer covers.
    pub tail_ring_bytes: usize,
    /// Evict resident stream handles idle at least this long (and
    /// referenced by nothing but the map). Zero disables. A handle holding
    /// unsealed pages is held by the sealer's set, so it is never idle.
    pub handle_idle_evict: std::time::Duration,
    /// Capacity cap on resident stream handles (0 disables): when the
    /// map exceeds this, the sealer's loop evicts oldest-touched unreferenced
    /// handles down to the cap WITHOUT waiting for the idle threshold —
    /// a cardinality burst can otherwise accumulate rate × idle-window
    /// handles before the first one ages out. Referenced handles
    /// (strong_count > 1) are never evicted, so the map can exceed the
    /// cap by the number of streams actively in use.
    pub handle_max_resident: usize,
    pub shared_ops: Option<Arc<crate::ops::OpsService>>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            queue_reqs: 65_536,
            max_batch_reqs: 16_384,
            max_batch_bytes: 48 * 1024 * 1024,
            pace_min_reqs: 32,
            gather_window: std::time::Duration::from_millis(15),
            wal_group_commit: false,
            wal_flush_gap: std::time::Duration::from_millis(25),
            wal_post_ack_gather: std::time::Duration::ZERO,
            handle_idle_evict: std::time::Duration::from_secs(600),
            handle_max_resident: 65_536,
            shared_ops: None,
            wal_gather_skip_reqs: 32,
            wal_gather_skip_bytes: 1024 * 1024,
            tail_ring_bytes: 0,
        }
    }
}

/// Per-commit-group pipeline decomposition: where a request's time goes
/// between arriving at the committer and being durably acked.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupTiming {
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
    effects: DurableEffects,
}

pub(crate) struct ShardEngine {
    pub prefix: String,
    /// SlateDB writer epoch this engine claimed at open: the history its applied reads serve.
    pub writer_epoch: u64,
    pub db: Arc<Db>,
    /// R29: whether the billing sweep may close this engine — the
    /// scheduler's custody and the external stamp that revokes it, owned
    /// by `crate::billing::SweepCustody` so no caller can reorder them.
    pub(crate) sweep_custody: crate::billing::SweepCustody,
    /// Layout 6's in-memory state (DESIGN-6A): the usage dirty set, the
    /// unsealed set and the sealer's state.
    layout: layout::LayoutState,
    /// Engine-owned maintenance ledger (R25-A, layout 6): the shard's
    /// unsealed bytes, rebuilt from the tails before the engine serves
    /// and updated only after the write that moved them succeeds. Owned
    /// by the engine, so restore-before-serve, ownership handoff and
    /// old-owner-task cleanup are scoped to the engine lifecycle.
    maintenance: std::sync::RwLock<ShardMaintenance>,
    /// Object store of the shard's DBs: the sealer's segment objects.
    data_store: Arc<dyn object_store::ObjectStore>,
    /// Per-shard backpressure latch (hysteresis lives with the shard).
    pub maintenance_shard_shed: std::sync::atomic::AtomicBool,
    streams: Mutex<HashMap<[u8; 16], Arc<StreamHandle>>>,
    /// Seal fences by segment identity: a cache of the durable
    /// [`seal_fence_key`] rows, ENGINE-level because a queued AppendReq
    /// holds only the stream hash (an evictable handle would forget it)
    /// and never expiring (a queue has no residence bound). A fresh engine
    /// reloads the row, so a request that passed its claim check before an
    /// engine replacement still meets the takeover's fence (TLA-002-F1).
    seal_fences: Mutex<HashMap<[u8; 16], u64>>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<CommitHandoff>,
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
    completion_pause: Mutex<Option<Arc<retirement_tests::CompletionPause>>>,
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
    consumer_fences: ConsumerFences,
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
    pub ops: Arc<crate::ops::OpsService>,
    /// Level-triggered close signal for background tasks (see start()).
    close_tx: tokio::sync::watch::Sender<bool>,
    /// Handles for every task this engine spawned, so termination is a
    /// provable fact (`await_terminated`) instead of an assumption.
    tasks: lifecycle::EngineTasks,
    flush_wake: Notify,
    /// Group-commit pump wake: one permit means "commits landed since the
    /// pump last looked". Distinct from flush_wake, whose permit the acker
    /// loop consumes.
    pump_wake: Notify,
    /// Invoked when the shard db closes (fenced by a new owner / fatal):
    /// wired to TouchRegistry::close_shard so hanging /touch/wait clients
    /// get stale immediately instead of dangling until timeout.
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Set once when the shard db reports closed (fenced by a new owner or
    /// fatal). Everything still holding this engine — request handlers, the
    /// committer, the sealer — must fail fast / exit instead of retrying
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

#[expect(
    clippy::cast_possible_truncation,
    reason = "now_ms; the Unix millisecond count stays below i64::MAX for hundreds of millions of years; a checked conversion would only restate that horizon"
)]
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl ShardEngine {
    #[expect(
        clippy::too_many_lines,
        reason = "ShardEngine::start; the committer, acker, pump and trim tickers are spawned from one place so their channels and handles are wired in one visible order; splitting it would hide which task owns each channel end. The engine value it builds also records the writer epoch the opened database claimed, one field line that spawns and wires nothing"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "ShardEngine::start; the engine takes its prefix, database, store, config, signal channel, park and maintenance row separately as the opener resolved them; a builder would exist for this single call site. The writer epoch is read from the database argument, so the argument list is unchanged"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::start; the pump and trim tickers unwrap only the in-flight queue and trim-debt locks, and a poisoned one may hold a half-recorded group or debt; recovering either could acknowledge a group that never committed or trim a stream that still owes data. The writer-epoch initializer calls two manifest accessors and adds no unwrap site"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::excessive_nesting,
        reason = "ShardEngine::start; the pump and ticker loops nest each flush, eviction and trim verdict inside the tick that produced it and stamp their gaps in whole milliseconds and microseconds that fit u64; flattening them or checking the stamps would separate the verdicts from the tick and restate the clock. The writer-epoch initializer adds neither a cast nor a nested block"
    )]
    pub(crate) fn start(
        prefix: String,
        db: Arc<Db>,
        data_store: Arc<dyn object_store::ObjectStore>,
        cfg: ShardConfig,
        on_close: Option<Arc<dyn Fn() + Send + Sync>>,
        initial_maintenance: ShardMaintenance,
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
            writer_epoch: db.manifest().writer_epoch(),
            db,
            sweep_custody: crate::billing::SweepCustody::default(),
            layout: layout::LayoutState::default(),
            maintenance: std::sync::RwLock::new(initial_maintenance),
            maintenance_shard_shed: std::sync::atomic::AtomicBool::new(false),
            data_store,
            streams: Mutex::new(HashMap::new()),
            seal_fences: Mutex::new(HashMap::new()),
            #[cfg(test)]
            commit_gate: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            completion_pause: Mutex::new(None),
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
            in_flight: Mutex::new(CommitHandoff::default()),
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
            ops: cfg
                .shared_ops
                .clone()
                .unwrap_or_else(|| Arc::new(crate::ops::OpsService::new())),
            flush_wake: Notify::new(),
            pump_wake: Notify::new(),
            on_close,
            closed: std::sync::atomic::AtomicBool::new(false),
            close_tx,
            tasks: lifecycle::EngineTasks::default(),
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
            engine.spawn_required("pump", async move {
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
                    if pump.in_flight.lock().unwrap().pending().is_empty() {
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
                        let q = q.pending();
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
                            let acked = pump.dispatch_checked(durable_seq).await;
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
                        let q = q.pending();
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
                                    pump.in_flight.lock().unwrap().pending().iter().map(|g| g.reqs).sum();
                                pump.pump_gathered_reqs.fetch_add(
                                    after.saturating_sub(pend_reqs) as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            });
        }
        let committer = engine.clone();
        engine.spawn_required("committer", async move {
            committer.committer_loop(rx, cfg).await
        });
        let acker = engine.clone();
        engine.spawn_required("acker", async move { acker.acker_loop().await });
        engine
    }

    pub(crate) fn spawn_required(
        self: &Arc<Self>,
        role: &'static str,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.tasks.required(self, role, future);
        // Native queue maintenance (§6.8) and the sealer (layout 6) run
        // beside every engine's committer, from its first moment, so a
        // queue nobody touches after a restart or move is still served by
        // time alone, and the replay bound holds without traffic.
        if role == "committer" {
            self.spawn_queue_maintenance();
        }
    }

    pub(crate) fn required_task_failure(&self) -> Option<&'static str> {
        self.tasks.failure()
    }
    pub(crate) fn shutdown_handle(&self) -> EngineShutdown {
        self.tasks.handle()
    }
    #[cfg(test)]
    pub(crate) fn termination_complete(&self) -> bool {
        self.shutdown_handle().terminated()
    }

    /// Level-triggered notification also covers subscription after close.
    pub(crate) async fn closed(&self) {
        let mut closed = self.close_tx.subscribe();
        while !self.is_closed() {
            if closed.changed().await.is_err() {
                return;
            }
        }
    }

    /// Observe the engine's one owned shutdown. Timeout/cancellation only
    /// stops this observer; workers and storage closure retain their owner.
    #[cfg(test)]
    pub(crate) async fn await_terminated(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.begin_close();
        self.shutdown_handle().wait(timeout).await
    }

    /// Worker reservations may be released while a native store close is
    /// still blocked. This narrower milestone never reports full termination.
    #[cfg(test)]
    pub(crate) async fn await_workers(&self, timeout: std::time::Duration) -> Result<(), String> {
        self.begin_close();
        self.tasks.workers(timeout).await
    }

    pub(crate) fn try_enqueue(&self, req: AppendReq) -> Result<(), EnqueueError> {
        // Control-only closes enter the actor as a different command. Data
        // appends retain final-record completion atomically with their data.
        let command = if req.finish == AppendFinish::Close
            && req.entries.is_empty()
            && req.producer.is_none()
            && req.seq.is_none()
            && req.deferred_error.is_none()
            && req.sealed_reject_new.is_none()
        {
            CommitOp::Close(CloseReq {
                hash: req.hash,
                generation: req.seal_gen,
                resp: req.resp,
            })
        } else {
            CommitOp::Append(req)
        };
        self.try_command(command)
    }

    pub(crate) fn try_close(&self, req: CloseReq) -> Result<(), EnqueueError> {
        self.try_command(CommitOp::Close(req))
    }

    pub(crate) fn try_seal_fence(&self, req: SealFenceReq) -> Result<(), EnqueueError> {
        self.try_command(CommitOp::SealFence(req))
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "ShardEngine::try_command; the microseconds since the engine epoch fit u64 for hundreds of thousands of years; a checked conversion would only restate the clock"
    )]
    fn try_command(&self, command: CommitOp) -> Result<(), EnqueueError> {
        if self.is_closed() {
            return Err(EnqueueError::Closed);
        }
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => EnqueueError::Full,
            mpsc::error::TrySendError::Closed(_) => EnqueueError::Closed,
        })?;
        #[cfg(test)]
        self.appends_enqueued.fetch_add(1, Ordering::SeqCst);
        let armed = self.ack_armed_at_us.swap(0, Ordering::Relaxed);
        if armed != 0 {
            let now = self.epoch.elapsed().as_micros() as u64;
            self.ack_to_enqueue_sum_us
                .fetch_add(now.saturating_sub(armed), Ordering::Relaxed);
            self.ack_to_enqueue_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// True once the shard db reported closed (fenced by a new owner or a
    /// fatal storage error). Holders must stop using this engine.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Proactive close (rebalancer moved this shard away): mark closed,
    /// wake the pump, fail everything in flight NOW. Without this,
    /// requests already queued here hang until the new owner's fence
    /// propagates — clients sat out their full timeout (ladder D3:
    /// exactly one in-flight batch per worker lost at the move moment).
    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::begin_close; a close signal with no remaining subscribers has nothing to wake; a handled send would only restate that the engine is closing"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::begin_close; the stranded groups were collected under the queue guard that proved them present; a fallible take would add a branch no close reaches"
    )]
    pub(crate) fn begin_close(&self) {
        // This is the terminal handoff, shared with transaction publication,
        // no-write attachment and durable dispatch. Recover poisoning so a
        // failed worker still fences admission and retains shutdown authority.
        let stranded = {
            let mut handoff = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let stranded = handoff.retire();
            self.closed.store(true, Ordering::SeqCst);
            stranded
        };
        let first = stranded.is_some();
        if first {
            let _ = self.close_tx.send(true);
            self.pump_wake.notify_one();
            self.tasks.begin_close(self.db.clone(), self.prefix.clone());
        }
        // Wake every parked live reader — on EVERY call, before the
        // double-close guard. A session parked on an idle tail has no
        // traffic to surface the fence to it: its next read re-checks
        // ownership (owned_here) and takes the typed WrongOwner
        // cutoff — but only if something wakes it AFTER the ownership
        // map moved. The slatedb fence can close the engine BEFORE
        // the loser's override mirror updates: that early wake finds
        // the OLD map and re-parks, and the fleet tick's later
        // begin_close used to early-return through this guard without
        // notifying — the parked session then held keep-alives
        // forever (round-11.4 fleet finding; the guarded-skip variant
        // reproduced only on slow CI runners).
        for h in self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            h.notify.notify_waiters();
            h.applied_notify.notify_waiters();
        }
        if !first {
            return; // already closing
        }
        for group in stranded.unwrap() {
            group.effects.reject(AppendErr::Moved);
        }
        if let Some(cb) = &self.on_close {
            cb();
        }
        self.ops.emit(
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
    }

    /// How long the current commit db.write has been blocked (0 = idle).
    /// A sustained value means SlateDB backpressure (L0-full/unflushed-full
    /// with lagging compaction): admission should shed 429 instead of
    /// queueing appends into a hang.
    pub(crate) fn commit_blocked_ms(&self) -> i64 {
        let started = self.commit_write_started_ms.load(Ordering::SeqCst);
        if started == 0 {
            0
        } else {
            (now_ms() - started).max(0)
        }
    }

    #[cfg(test)]
    pub(crate) fn set_commit_write_started_ms(&self, v: i64) {
        self.commit_write_started_ms.store(v, Ordering::SeqCst);
    }

    /// Age of the oldest committed-but-not-durable group (0 = none). THE
    /// wedge signal for the common stall mode: db.write keeps succeeding
    /// (memtable has room) while the WAL-flush pipeline is stalled behind
    /// L0-full, so groups pile up here waiting for the durable watermark.
    /// The 2026-07-22 final gate run proved commit_blocked_ms alone misses
    /// this mode entirely (wedge_shed=0 through a 10-minute wedge).
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::oldest_inflight_ms; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "ShardEngine::oldest_inflight_ms; the elapsed milliseconds are clamped to i64::MAX before the cast; a checked conversion would only restate the clamp"
    )]
    pub(crate) fn oldest_inflight_ms(&self) -> i64 {
        self.in_flight
            .lock()
            .unwrap()
            .pending()
            .first()
            .map(|g| g.written_at.elapsed().as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }

    /// Combined wedge signal: blocked commit write OR stale durability.
    pub(crate) fn wedge_ms(&self) -> i64 {
        self.commit_blocked_ms().max(self.oldest_inflight_ms())
    }

    /// Split-safe Stream-Seq (review blocker 4): a sequence lane lives
    /// per (segment, routing key), but a child segment starts empty —
    /// without consulting its sealed predecessors, a sequence the
    /// PARENT already accepted would be accepted again on the child.
    /// Nearest identity wins, exactly like the producer chain: the nearest
    /// row that exists decides, and one that does not decode is corruption.
    async fn load_seq_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
    ) -> Result<Option<String>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
                return decode_seq_row(&v).map(Some);
            }
        }
        Ok(None)
    }

    /// Producer-state lookup through the routing key's predecessor
    /// chain (ROUTING-V3 §3.6): own identity first, then each sealed
    /// predecessor. A hit on a predecessor means the producer's last
    /// commit landed before a split — the caller stages it locally so
    /// the duplicate check answers with the ORIGINAL offset and no new
    /// offset is consumed. A row that exists but does not decode is
    /// corruption: it never defers to an older predecessor's row.
    async fn load_producer_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
        pid: &str,
    ) -> Result<Option<(u64, u64, u64, [u8; 16])>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(producer_key(identity, key_hash, pid)).await? {
                return decode_producer_row(&v).map(Some);
            }
        }
        Ok(None)
    }

    /// Published maintenance state for admission decisions.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::maintenance_snapshot; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    pub(crate) fn maintenance_snapshot(&self) -> ShardMaintenance {
        *self.maintenance.read().unwrap()
    }

    /// Publish new maintenance state. Callers must only do this AFTER
    /// the write that moved the bytes has succeeded — that ordering is the
    /// entire fix for phantom backlog.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::publish_maintenance; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    pub(crate) fn publish_maintenance(&self, m: ShardMaintenance) {
        *self.maintenance.write().unwrap() = m;
    }

    /// Durable consumer-cursor hint for the pull pre-read window. A
    /// stale value only widens the window; the committer's
    /// stop-at-uncovered rule keeps leasing exact.
    /// Create the tail row for a FORK's storage identity: its own
    /// record space begins at the fork boundary — nothing below exists
    /// under this identity (inherited records are served from the
    /// ancestor chain). No-op when the tail already exists (idempotent
    /// PUT retries).
    pub(crate) async fn seed_fork_tail(
        &self,
        hash: [u8; 16],
        route: [u8; 16],
        at: u64,
    ) -> Result<(), slatedb::Error> {
        if let Some(raw) = self.db.get(tail_key(&hash)).await? {
            stored_tail(&raw)?;
            return Ok(());
        }
        let t = TailFields {
            next: at,
            absorbed: at,
            route,
            ..Default::default()
        };
        self.db
            .put(&tail_key(&hash)[..], &encode_tail(&t)[..])
            .await
            .map(|_| ())
    }

    pub(crate) async fn queue_cursor(
        &self,
        hash: [u8; 16],
        consumer: &str,
        cgen: u64,
    ) -> Result<u64, slatedb::Error> {
        Ok(self
            .db
            .get(crate::queue::cursor_key(&hash, consumer, cgen))
            .await?
            .map(|v| decode_cursor(&v))
            .transpose()?
            .unwrap_or(0))
    }

    pub(crate) async fn submit_queue(
        &self,
        hash: [u8; 16],
        op: crate::queue::QueueOp,
    ) -> Result<crate::queue::QueueOut, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CommitOp::Queue { hash, op, resp: tx })
            .await
            .map_err(|_| "committer gone".to_string())?;
        // Entered-proof AFTER the send: the counter's contract is
        // "made the queue". Incrementing before the ASYNC send let a
        // test's ordering guard pass while this task was still
        // suspended short of the channel — a later submission could
        // overtake it (round-9 CI: a group-local delete overtook the
        // settle whose counter tick had already been observed).
        #[cfg(test)]
        self.appends_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        rx.await
            .map_err(|_| "committer dropped request".to_string())?
    }

    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::stream_handle; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "ShardEngine::stream_handle; now_ms is a non-negative Unix millisecond stamp; a checked conversion would only restate the clock"
    )]
    pub(crate) async fn stream_handle(
        &self,
        hash: [u8; 16],
    ) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(h) = self.streams.lock().unwrap().get(&hash) {
            h.last_touch_ms
                .store(now_ms() as u64, std::sync::atomic::Ordering::Relaxed);
            return Ok(h.clone());
        }
        let handle = self.new_handle(hash, self.load_tail(&hash).await?);
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
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::evict_idle_handles; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "ShardEngine::evict_idle_handles; the cutoff is a non-negative Unix millisecond stamp less an idle window that fits u64; checked conversions would only restate the clock"
    )]
    pub(crate) fn evict_idle_handles(
        &self,
        idle: std::time::Duration,
        max_resident: usize,
    ) -> usize {
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

    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::committer_loop; a caller that gave up before its reply has no receiver; a handled send would only restate that the request was abandoned"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "ShardEngine::committer_loop; the committer nests the batch gather and the pacing wait inside the group loop; flattening them would separate the gather from the group it fills"
    )]
    async fn committer_loop(self: Arc<Self>, mut rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {
        // The close signal is the ONLY way out: this task holds the engine
        // and the engine holds a sender, so `rx` can never report closed.
        loop {
            let first = tokio::select! {
                _ = self.closed() => {
                    // Fail everything still queued — their clients would
                    // otherwise hang into their own timeouts — then exit.
                    while let Ok(op) = rx.try_recv() {
                        match op {
                            CommitOp::Append(r) => {
                                let _ = r.resp.send(Err(AppendErr::Moved));
                            }
                    CommitOp::Close(CloseReq { resp, .. }) | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                        let _ = resp.send(Err(AppendErr::Moved));
                    }
                            CommitOp::Queue { resp, .. } => {
                                let _ = resp.send(Err("shard fenced/moved; retry".into()));
                            }
                            CommitOp::NativeQueue { resp, .. } => {
                                let _ = resp.send(Err(crate::native_queue::NqRefusal::Internal("shard fenced/moved; retry".into())));
                            }
                            // A billing op dropped here answers its own
                            // refusal (`BillingReply`), as in `reject_op`.
                            CommitOp::Seal(_)
                            | CommitOp::UsageAck { .. }
                            | CommitOp::BillingClose { .. }
                            | CommitOp::BillingRetained { .. } => {}
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
                transaction::CommitTransaction::reject_op(first, AppendErr::Moved);
                while let Ok(op) = rx.try_recv() {
                    transaction::CommitTransaction::reject_op(op, AppendErr::Moved);
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
            self.commit_group(ops).await;
        }
    }

    async fn commit_group(&self, ops: Vec<CommitOp>) {
        transaction::CommitTransaction::run(self, ops).await;
    }

    /// Test hook: hold the COMMIT gate. While held, the committer
    /// takes at most one op and then parks before gathering; releasing
    /// the guard lets it drain everything queued meanwhile into ONE
    /// commit group. The companion to `fail_next_group_for` for
    /// deterministic same-group scenarios.
    #[cfg(test)]
    pub(crate) async fn test_hold_commit(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_gate.lock().await
    }

    /// Entered-proof for group-composition tests: client ops — appends,
    /// fences, and queue submissions — enqueued on this engine so far. A test polls the delta
    /// instead of sleeping and hoping its request made the queue.
    #[cfg(test)]
    pub(crate) fn appends_enqueued(&self) -> u64 {
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
    pub(crate) async fn seed_consumer_residue_rows(
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

    #[cfg(test)]
    pub(crate) async fn count_consumer_state_rows(
        &self,
        hash: [u8; 16],
        consumer: &str,
    ) -> Result<usize, String> {
        let mut n = 0usize;
        for tag in *b"clx" {
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
    pub(crate) fn fail_next_config_scan(&self) {
        self.fail_config_scan
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_config_scan_failure(&self) -> bool {
        self.fail_config_scan
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Arm a one-shot failure for the next commit group that stages a
    /// seal (R25-D, layout 6): proves the seal's retirement of unsealed
    /// bytes is atomic with its boundary move.
    #[cfg(test)]
    pub(crate) fn fail_next_seal_group(&self) {
        self.layout
            .seal
            .hooks
            .fail_next_group
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_group_for(&self, identity: [u8; 16]) {
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
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::seal_fence_stats; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    pub(crate) fn seal_fence_stats(&self) -> (usize, u64) {
        let f = self.seal_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Acknowledge `_usage` durability for a segment's snapshot at
    /// `version` (+ exact month-final rows). Fire-and-forget through
    /// the committer — see CommitOp::UsageAck.
    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::submit_usage_ack; a command the committer queue cannot take is re-driven by the next absorb, usage or trim pass; a handled send would only restate that the queue is full or closed"
    )]
    pub(crate) fn submit_usage_ack(
        &self,
        hash: [u8; 16],
        version: u64,
        month_final_keys: Vec<Vec<u8>>,
    ) {
        let _ = self.tx.try_send(CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::ThroughVersion(version),
            month_final_keys,
        });
    }

    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::submit_usage_final_ack; a command the committer queue cannot take is re-driven by the next absorb, usage or trim pass; a handled send would only restate that the queue is full or closed"
    )]
    pub(crate) fn submit_usage_final_ack(&self, hash: [u8; 16], month_final_keys: Vec<Vec<u8>>) {
        let _ = self.tx.try_send(CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::FinalRowsOnly,
            month_final_keys,
        });
    }

    /// Consumer-fence cardinality (round 17): one non-expiring entry
    /// per (segment identity, deleted consumer name) — correct for
    /// safety, so its growth is surfaced instead of hidden. Any future
    /// cleanup must be proved by committer-queue progress, exactly like
    /// the seal fences; never wall-clock expiry.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::consumer_fence_stats; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    pub(crate) fn consumer_fence_stats(&self) -> (usize, u64) {
        let f = self.consumer_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Test hook: empty the engine-resident fence map WITHOUT touching
    /// the durable rows — the deterministic stand-in for "this shard
    /// just moved to another instance, which opened a fresh engine".
    /// The durable fence must still refuse dead generations.
    #[cfg(test)]
    pub(crate) fn forget_consumer_fences_for_test(&self) {
        self.consumer_fences.lock().unwrap().clear();
    }

    /// Test view of the DURABLE fence row (None = no row).
    #[cfg(test)]
    pub(crate) async fn durable_consumer_fence(
        &self,
        hash: [u8; 16],
        consumer: &str,
    ) -> Option<u64> {
        let k = crate::queue::fence_key(&hash, consumer);
        self.db
            .get(&k[..])
            .await
            .ok()
            .flatten()
            .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
    }

    #[cfg(test)]
    pub(crate) fn group_failures_tripped(&self) -> usize {
        self.fail_group_tripped
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test hook: hold the dispatch gate. While held, NEITHER the acker
    /// nor the pump can dispatch acks — the deterministic stand-in for
    /// "the acker is paused after durability, before response dispatch".
    #[cfg(test)]
    pub(crate) async fn test_hold_dispatch(&self) -> tokio::sync::MutexGuard<'_, ()> {
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
    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::dispatch_durable; a caller that gave up has no receiver and an absorb signal the pump cannot take is re-driven by its next tick; handled sends would only restate abandonment"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::dispatch_durable; a poisoned shard state may hold a partially applied tail, ring, fence, debt or maintenance update; recovering it could publish an offset or boundary that was never committed"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::excessive_nesting,
        reason = "ShardEngine::dispatch_durable; the durable wait is clamped to u32::MAX and the timing ring is trimmed inside the publication it records; checking the clamp or flattening the trim would restate the bound and separate the trim from the record"
    )]
    async fn dispatch_durable(&self, durable_seq: u64) -> u32 {
        let _order = self.dispatch_gate.lock().await;
        // Claim only proven remote-durable groups while this owner is live.
        // Retirement after the claim does not revoke that already-durable
        // completion; effects run outside the synchronous handoff mutex.
        let ready = self.in_flight.lock().unwrap().take_durable(durable_seq);
        #[cfg(test)]
        if !ready.is_empty() {
            self.completion_checkpoint(retirement_tests::CompletionPhase::Durable)
                .await;
        }
        let mut dispatched = 0u32;
        for group in ready {
            dispatched += group.reqs;
            // Publish to the durable-tail ring FIRST: the ring ceiling
            // must already cover an offset by the time tail state (and
            // then an ack) makes that offset visible, or a reader woken
            // by the ack would miss the fast path — or worse, serve a
            // truncated range.
            for (handle, recs) in &group.effects.ring_pub {
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
            for (handle, fields, wake) in &group.effects.tails {
                handle.publish_durable(fields, *wake);
            }
            for (usage, plaintext, frames) in group.effects.usage {
                usage
                    .plaintext_bytes
                    .fetch_add(plaintext, Ordering::Relaxed);
                usage.frame_bytes.fetch_add(frames, Ordering::Relaxed);
            }
            for (resp, res) in group.effects.acks {
                let _ = resp.send(res);
            }
            for (resp, out) in group.effects.queue_acks {
                let _ = resp.send(out);
            }
            for (resp, out) in group.effects.native_acks {
                let _ = resp.send(out);
            }
            // H2: feed touch journals only after the data is durable and
            // reader-visible, so an invalidation always finds fresh data.
            for t in group.effects.touches {
                t.journal.ingest(&t.key_ids, t.next_offset);
            }
        }
        dispatched
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "ShardEngine::acker_loop; the acker nests the close-reason verdict inside the status read of each wake; flattening it would separate the verdict from the status it reads"
    )]
    async fn acker_loop(self: Arc<Self>) {
        let mut status_rx = self.db.subscribe();
        loop {
            if self.is_closed() {
                return;
            }
            let durable_seq = {
                let status = status_rx.borrow_and_update();
                if let Some(reason) = &status.close_reason {
                    tracing::error!(shard = %self.prefix, "shard db closed: {reason:?}");
                    self.begin_close();
                    return;
                }
                status.durable_seq
            };
            self.dispatch_checked(durable_seq).await;
            tokio::select! {
                _ = self.closed() => return,
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

#[cfg(test)]
mod maintenance_tests;

#[cfg(test)]
mod storage_decode_tests;

#[cfg(test)]
fn billing_read_faults() -> &'static Mutex<HashMap<String, ()>> {
    static FAULTS: std::sync::OnceLock<Mutex<HashMap<String, ()>>> = std::sync::OnceLock::new();
    FAULTS.get_or_init(Default::default)
}

#[cfg(test)]
mod billing_read_tests;

#[cfg(test)]
mod commit_command_tests;

#[cfg(test)]
mod bounded_outbox_tests;

#[cfg(test)]
mod queue_publication_tests;

#[cfg(test)]
#[path = "shard/durability_frontier_tests.rs"]
mod durability_frontier_tests;

#[cfg(test)]
mod queue_codec_tests;

#[cfg(test)]
mod record_scan_tests;

#[cfg(test)]
mod read_budget_tests;
#[cfg(test)]
mod tail_ring_tests;

#[cfg(test)]
mod task_lifecycle_tests;

#[cfg(test)]
impl ShardEngine {
    pub(crate) fn test_abort_task(&self, role: &str) -> tokio::task::AbortHandle {
        self.tasks.abort(role)
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
use test_support::seal_open_faults;
#[cfg(test)]
mod transaction_tests;
#[cfg(test)]
pub(crate) use test_support::{
    decode_tail_for_tests, encode_tail_for_tests, inject_seal_open_faults, seed_billing,
    stored_billing, usage_dirty_version,
};

#[cfg(test)]
mod page_log_tests;
#[cfg(test)]
mod resident_tests;
#[cfg(test)]
mod retirement_tests;
