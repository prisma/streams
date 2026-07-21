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

pub fn producer_key(hash: &[u8; 16], producer_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + producer_id.len());
    k.extend_from_slice(hash);
    k.push(b'q');
    k.extend_from_slice(producer_id.as_bytes());
    k
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
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub struct StreamState {
    pub durable: TailFields,
    pub applied: TailFields,
    /// Producer idempotence state: id -> (epoch, highest seq). Loaded from
    /// the durable `q` keys on first use, applied by the committer.
    pub producers: HashMap<String, (u64, u64)>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
}

pub struct StreamHandle {
    pub hash: [u8; 16],
    pub state: Mutex<StreamState>,
    pub notify: Notify,
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
    Absorbed {
        hash: [u8; 16],
        upto: u64,
    },
}

/// Notification to the absorber that a stream accumulated shard-log bytes.
#[derive(Debug, Clone)]
pub struct AbsorbSignal {
    pub hash: [u8; 16],
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
    signals: Vec<AbsorbSignal>,
    touches: Vec<TouchFeed>,
}

pub struct ShardEngine {
    pub prefix: String,
    pub db: Arc<Db>,
    streams: Mutex<HashMap<[u8; 16], Arc<StreamHandle>>>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<Vec<InFlightGroup>>,
    flush_wake: Notify,
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
        cfg: ShardConfig,
        absorb_tx: mpsc::Sender<AbsorbSignal>,
        on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Arc<ShardEngine> {
        let (tx, rx) = mpsc::channel(cfg.queue_reqs);
        let engine = Arc::new(ShardEngine {
            prefix,
            db,
            streams: Mutex::new(HashMap::new()),
            tx,
            in_flight: Mutex::new(Vec::new()),
            flush_wake: Notify::new(),
            absorb_tx,
            on_close,
            closed: std::sync::atomic::AtomicBool::new(false),
            commit_write_started_ms: std::sync::atomic::AtomicI64::new(0),
            stats_appended: AtomicU64::new(0),
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

    pub fn try_enqueue(&self, req: AppendReq) -> Result<(), AppendReq> {
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

    pub async fn submit_absorbed(&self, hash: [u8; 16], upto: u64) {
        let _ = self.tx.send(CommitOp::Absorbed { hash, upto }).await;
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
        });
        let mut map = self.streams.lock().unwrap();
        Ok(map.entry(hash).or_insert(handle).clone())
    }

    async fn committer_loop(self: Arc<Self>, mut rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {
        loop {
            let Some(first) = rx.recv().await else { return };
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
            producers: HashMap<String, (u64, u64)>,
            appended_bytes: u64,
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
                                    Ok(Some(v)) if v.len() >= 16 => Some((
                                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
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
                            Some((ce, cs)) => {
                                if pr.epoch < ce {
                                    let _ = req
                                        .resp
                                        .send(Err(AppendErr::ProducerStale { current_epoch: ce }));
                                    continue;
                                }
                                if pr.epoch == ce && pr.seq <= cs {
                                    let _ = req.resp.send(Ok(AppendAck {
                                        last_offset: local.fields.next.wrapping_sub(1),
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
                            last_offset: local.fields.next - 1,
                            next_offset: local.fields.next,
                            closed: local.fields.closed,
                            producer: prod_echo,
                            duplicate: false,
                        },
                    ));
                }
                CommitOp::Absorbed { upto, .. } => {
                    let prev_absorbed = local.fields.absorbed;
                    if upto > prev_absorbed {
                        local.fields.absorbed = upto.min(local.fields.next);
                    }
                    // Deferred trim: delete only up to the *previous* absorbed
                    // boundary, bounded per op.
                    let trim_to = prev_absorbed.min(local.fields.trimmed + cfg.max_trim_per_op);
                    for off in local.fields.trimmed..trim_to {
                        wb.delete(record_key(&hash, off));
                    }
                    local.fields.trimmed = trim_to;
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
            }
        }
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
            let ready: Vec<InFlightGroup> = {
                let mut q = self.in_flight.lock().unwrap();
                let split = q.partition_point(|g| g.seq <= durable_seq);
                q.drain(..split).collect()
            };
            for group in ready {
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
