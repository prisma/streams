//! Test-only codec, fault-injection and counting helpers for the shard
//! engine.
#![cfg(test)]
use super::*;

pub(crate) fn decode_tail_for_tests(v: &[u8]) -> Option<TailFields> {
    decode_tail(v)
}

/// Test-only: the production tail encoder, exposed so golden tests can
/// pin the exact v4 byte layout without going through a shard engine.
pub(crate) fn encode_tail_for_tests(t: &TailFields) -> Vec<u8> {
    encode_tail(t)
}

/// Test-only fault injection for the sealer's open, keyed by shard prefix
/// so concurrent tests cannot poison each other. The object-store fault
/// substrate cannot reach the open's scans deterministically (SlateDB
/// retries store faults internally), and the sealer's open-retry loop is
/// exactly the code under test.
pub(super) fn seal_open_faults() -> &'static Mutex<HashMap<String, u32>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u32>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arrange for the next `n` sealer opens on `prefix` to fail.
pub(crate) fn inject_seal_open_faults(prefix: &str, n: u32) {
    seal_open_faults()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), n);
}

/// Test-only: the billing ops each segment's replies refused unapplied
/// (`BillingReply`'s drop), the same count `BILLING_OPS_REFUSED` keeps for
/// the whole process, where every concurrent test whose retiring engine
/// drops a billing op adds to it. Keyed by segment, so a test that owns its
/// segment reads an exact count no concurrent test can change.
fn billing_ops_refused() -> &'static Mutex<HashMap<[u8; 16], u64>> {
    static M: std::sync::OnceLock<Mutex<HashMap<[u8; 16], u64>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One billing op on `segment` refused unapplied.
pub(super) fn count_billing_op_refused(segment: [u8; 16]) {
    *billing_ops_refused()
        .lock()
        .unwrap()
        .entry(segment)
        .or_default() += 1;
}

/// The billing ops on `segment` refused unapplied so far.
pub(super) fn billing_ops_refused_on(segment: &[u8; 16]) -> u64 {
    billing_ops_refused()
        .lock()
        .unwrap()
        .get(segment)
        .copied()
        .unwrap_or_default()
}

/// Test-only: billing-row reads a shard holds open until a test releases
/// them, counting the most in flight at once. Keyed by shard prefix, so
/// concurrent tests cannot hold each other's reads.
pub(super) struct HeldReads {
    in_flight: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
    total: std::sync::atomic::AtomicUsize,
    gate: tokio::sync::Semaphore,
}

impl Default for HeldReads {
    fn default() -> Self {
        Self {
            in_flight: Default::default(),
            most: Default::default(),
            total: Default::default(),
            gate: tokio::sync::Semaphore::new(0),
        }
    }
}

impl HeldReads {
    pub(super) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub(super) fn most_in_flight(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }

    /// Every read that reached the hold, held or let through.
    pub(super) fn total(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }

    /// Let every held read, and every later one, through.
    pub(super) fn release(&self) {
        self.gate
            .add_permits(tokio::sync::Semaphore::MAX_PERMITS >> 1);
    }
}

fn held_reads() -> &'static Mutex<HashMap<String, Arc<HeldReads>>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, Arc<HeldReads>>>> =
        std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Hold `prefix`'s billing-row reads from now on.
pub(super) fn hold_billing_reads(prefix: &str) -> Arc<HeldReads> {
    let held = Arc::new(HeldReads::default());
    held_reads()
        .lock()
        .unwrap()
        .insert(prefix.to_owned(), held.clone());
    held
}

/// One billing-row read of `prefix`: held while its shard's reads are.
pub(super) async fn held_billing_read(prefix: &str) {
    let Some(held) = held_reads().lock().unwrap().get(prefix).cloned() else {
        return;
    };
    held.total.fetch_add(1, Ordering::SeqCst);
    let now = held.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    held.most.fetch_max(now, Ordering::SeqCst);
    drop(held.gate.acquire().await.unwrap());
    held.in_flight.fetch_sub(1, Ordering::SeqCst);
}

/// Hold `prefix`'s queue-state loads from now on.
pub(super) fn hold_queue_reads(prefix: &str) -> Arc<HeldReads> {
    hold_billing_reads(&format!("{prefix}#queue"))
}

/// One queue-state load of `prefix`: held while its shard's are.
pub(super) async fn held_queue_read(prefix: &str) {
    held_billing_read(&format!("{prefix}#queue")).await;
}

/// Hold `prefix`'s consumer-record reads from now on.
pub(super) fn hold_config_reads(prefix: &str) -> Arc<HeldReads> {
    hold_billing_reads(&format!("{prefix}#config"))
}

/// One consumer-record read of `prefix`: held while its shard's are.
pub(super) async fn held_config_read(prefix: &str) {
    held_billing_read(&format!("{prefix}#config")).await;
}

/// Test-only: stage `meta` as a billed segment stores it (layout 6, f3):
/// its static row, and its usage counters on a tail with `fields`. A test
/// that seeded the JSON row and its dirty marker before seeds this.
pub(crate) fn seed_billing(
    batch: &mut WriteBatch,
    hash: &[u8; 16],
    meta: &crate::billing::SegmentBillingMetaV1,
    fields: &TailFields,
) {
    let usage = crate::billing::stored_meta::UsageCounters::of(meta);
    batch.put(
        crate::billing::billing_meta_key(hash),
        crate::billing::stored_meta::encode_static(meta),
    );
    batch.put(
        tail_key(hash),
        encode_tail(&TailFields {
            usage,
            ..fields.clone()
        }),
    );
}

/// Test-only: the version the engine's usage dirty set holds for `hash`.
pub(crate) async fn usage_dirty_version(engine: &ShardEngine, hash: [u8; 16]) -> Option<u64> {
    let dirty = engine.usage_dirty_scan().await.unwrap();
    dirty.into_iter().find(|(h, _)| *h == hash).map(|(_, v)| v)
}

/// Test-only: `hash`'s billing meta as `db` stores it (layout 6, f3): its
/// static row with the usage counters its tail carries.
pub(crate) async fn stored_billing(
    db: &Db,
    hash: &[u8; 16],
) -> Option<crate::billing::SegmentBillingMetaV1> {
    let raw = db
        .get(crate::billing::billing_meta_key(hash))
        .await
        .unwrap()?;
    let mut meta = crate::billing::stored_meta::decode_static(&raw).unwrap();
    let tail = db.get(tail_key(hash)).await.unwrap().unwrap();
    if let Some(usage) = stored_tail(&tail).unwrap().usage {
        usage.apply(&mut meta);
    }
    Some(meta)
}

/// Test-only: per shard log (by the address of its `Db`), how often a
/// group staged a static billing row and how often the engine read one from
/// the store (layout 6, f3).
type RowIo = Mutex<HashMap<usize, (u64, u64)>>;
fn billing_row_io() -> &'static RowIo {
    static M: std::sync::OnceLock<RowIo> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A group of the shard log at `db` staged a static row.
pub(super) fn count_static_row_staged(db: *const Db) {
    billing_row_io()
        .lock()
        .unwrap()
        .entry(db.addr())
        .or_default()
        .0 += 1;
}

/// The engine of the shard log at `db` read a static row from the store.
pub(super) fn count_static_row_read(db: *const Db) {
    billing_row_io()
        .lock()
        .unwrap()
        .entry(db.addr())
        .or_default()
        .1 += 1;
}

/// (static rows staged, static rows read from the store) for `db`.
pub(crate) fn billing_row_io_on(db: &Arc<Db>) -> (u64, u64) {
    billing_row_io()
        .lock()
        .unwrap()
        .get(&Arc::as_ptr(db).addr())
        .copied()
        .unwrap_or_default()
}
