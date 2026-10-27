//! Test-only codec, fault-injection and counting helpers for the shard
//! engine.
#![cfg(test)]
use super::*;

/// Test-only: encode a tail then STRIP the trailing exact-gauge field,
/// producing the pre-gauge layout older builds wrote. DST uses this to
/// prove the R26-4 open-time repair; production code never writes it.
pub(crate) fn encode_tail_without_gauge_for_tests(t: &TailFields) -> Vec<u8> {
    let mut v = encode_tail(t);
    v.truncate(v.len() - 8);
    v
}

pub(crate) fn decode_tail_for_tests(v: &[u8]) -> Option<TailFields> {
    decode_tail(v)
}

/// Test-only: the production tail encoder, exposed so golden tests can
/// pin the exact v3 byte layout without going through a shard engine.
/// Test-only: the production dirty-row encoder, exposed so golden tests
/// can pin the exact 32-byte LE layout.
pub(crate) fn encode_tail_for_tests(t: &TailFields) -> Vec<u8> {
    encode_tail(t)
}

pub(crate) fn dirty_value_for_tests(m: &StreamMaintenance) -> [u8; 32] {
    dirty_value(m)
}

/// Test-only fault injection for the durable dirty-index scan, keyed by
/// shard prefix so concurrent tests cannot poison each other. The
/// object-store fault substrate cannot reach this path deterministically
/// (SlateDB retries store faults internally), and the absorber's
/// scan-retry loop is exactly the code under test.
pub(super) fn dirty_scan_faults() -> &'static Mutex<HashMap<String, u32>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u32>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arrange for the next `n` dirty-index scans on `prefix` to fail.
pub(crate) fn inject_dirty_scan_faults(prefix: &str, n: u32) {
    dirty_scan_faults()
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
