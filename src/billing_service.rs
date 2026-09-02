//! Billing service (WP-02 / PR 6-E): the instance's usage-ledger key,
//! the read-usage accumulator, the read spool and usage rollup slots
//! (opened lazily by the telemetry loops, exactly once), and the sweep
//! scheduler's bookkeeping — extracted from `http::AppState`. One per
//! runtime: parallel rigs never sum their residents into one gauge.

use std::sync::{Arc, OnceLock};

use crate::billing::{ReadBatch, ReadSpool, ReadUsageAccumulator, SweepSched};

#[derive(Clone)]
pub struct BillingService {
    inner: Arc<Inner>,
}

struct Inner {
    /// The usage ledger stream key; None = billing off (no ledger, no
    /// spool, no rollup, no ops/audit ledger appends).
    usage_key: Option<String>,
    reads: Arc<ReadUsageAccumulator>,
    read_spool: OnceLock<Arc<ReadSpool>>,
    sweep: SweepSched,
}

/// What the operator surfaces show about the durable read spool.
pub struct ReadSpoolStats {
    pub quarantined: u64,
    pub pending_rows: u64,
    pub pending_bytes: u64,
    /// (l0 ssts, l0 bytes, runs, mid-run bytes)
    pub l0: (u64, u64, u64, u64),
}

impl BillingService {
    pub fn new(usage_key: Option<String>, reads: Arc<ReadUsageAccumulator>) -> Self {
        Self {
            inner: Arc::new(Inner {
                usage_key,
                reads,
                read_spool: OnceLock::new(),
                sweep: SweepSched::default(),
            }),
        }
    }

    /// The usage ledger's stream key — `None` means billing is off.
    pub fn usage_key(&self) -> Option<String> {
        self.inner.usage_key.clone()
    }

    // -- read metering ------------------------------------------------

    /// Meter one read against a billing identity.
    pub fn meter_read(
        &self,
        id: &crate::billing::BillingIdentity,
        delta: crate::billing::RowDelta,
    ) {
        self.inner.reads.meter(id, delta);
    }

    /// Meter one delivered live chunk (§4.2: what actually left the body).
    pub fn meter_read_chunk(
        &self,
        id: &crate::billing::BillingIdentity,
        payload_bytes: u64,
        records: u64,
    ) {
        crate::billing::meter_read_chunk(&self.inner.reads, id, payload_bytes, records);
    }

    /// Seal the open read window once it is older than `max_age_ms`.
    pub fn seal_aged_reads(&self, max_age_ms: i64) {
        self.inner.reads.seal_if_aged(max_age_ms);
    }

    /// Take up to `max` sealed read batches for the ledger.
    pub fn drain_sealed_reads(&self, max: usize) -> Vec<ReadBatch> {
        self.inner.reads.drain_sealed(max)
    }

    /// Return batches the ledger did not accept.
    pub fn requeue_reads(&self, batches: Vec<ReadBatch>) {
        self.inner.reads.requeue(batches);
    }

    /// (rows, estimated bytes, sealed batches) not yet in the ledger.
    pub fn unflushed_reads(&self) -> (usize, usize, usize) {
        self.inner.reads.unflushed()
    }

    /// How often sealing was deferred under memory pressure.
    pub fn read_seal_deferrals(&self) -> u64 {
        self.inner
            .reads
            .seal_deferrals
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Tests only: the raw accumulator, for scenarios that drive the
    /// metering window directly (seal-now, snapshot, drain).
    #[cfg(test)]
    pub fn reads(&self) -> &Arc<ReadUsageAccumulator> {
        &self.inner.reads
    }

    /// Tests only: the raw spool.
    #[cfg(test)]
    pub fn read_spool(&self) -> Option<&Arc<ReadSpool>> {
        self.inner.read_spool.get()
    }

    // -- the durable read spool ---------------------------------------

    /// Install the spool. `Err` means one was already installed.
    pub fn install_read_spool(&self, spool: Arc<ReadSpool>) -> Result<(), Arc<ReadSpool>> {
        self.inner.read_spool.set(spool)
    }

    /// Whether the durable spool is open (required mode demands it).
    pub fn read_spool_open(&self) -> bool {
        self.inner.read_spool.get().is_some()
    }

    /// Persist sealed read batches into the spool before the ledger.
    pub async fn spool_sealed_reads(&self, max: usize) -> Result<(), String> {
        match self.inner.read_spool.get() {
            Some(spool) => crate::billing::spool_sealed(&self.inner.reads, spool, max).await,
            None => Ok(()),
        }
    }

    /// The next spooled batches to publish, with their spool keys.
    pub async fn pending_spooled(&self, max: usize) -> Result<Vec<(Vec<u8>, ReadBatch)>, String> {
        match self.inner.read_spool.get() {
            Some(spool) => spool.pending(max).await.map_err(|e| e.to_string()),
            None => Ok(Vec::new()),
        }
    }

    /// Release spooled batches — ONLY after the ledger acknowledged.
    pub async fn remove_spooled(&self, keys: &[Vec<u8>]) -> Result<(), String> {
        match self.inner.read_spool.get() {
            Some(spool) => spool.remove(keys).await.map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    /// The operator view of the spool, if it is open.
    pub fn read_spool_stats(&self) -> Option<ReadSpoolStats> {
        let spool = self.inner.read_spool.get()?;
        let (pending_rows, pending_bytes) = spool.resident();
        Some(ReadSpoolStats {
            quarantined: spool.quarantined_count(),
            pending_rows,
            pending_bytes,
            l0: spool.l0_stats(),
        })
    }

    /// (open, quarantined, depth) for the readiness/telemetry surface.
    pub async fn read_spool_health(&self) -> (bool, u64, u64) {
        match self.inner.read_spool.get() {
            Some(sp) => (true, sp.quarantined_count(), sp.depth().await as u64),
            None => (false, 0, 0),
        }
    }

    // -- the sweep protocol (R30 custody) ------------------------------

    /// Record that the sweep scheduler holds `prefix` under custody
    /// value `seq`, and refresh the peak gauge.
    pub fn claim_sweep_custody(&self, prefix: &str, seq: u64, held_now: usize) {
        self.inner
            .sweep
            .opened
            .lock()
            .unwrap()
            .insert(prefix.to_string(), seq);
        self.inner
            .sweep
            .peak
            .fetch_max(held_now, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drop `prefix` from custody and forget its quantum accounting.
    pub fn release_sweep_custody(&self, prefix: &str) {
        self.inner.sweep.opened.lock().unwrap().remove(prefix);
        self.inner.sweep.cycles.lock().unwrap().remove(prefix);
    }

    /// Count one residency cycle for `prefix`; returns the new count.
    pub fn note_sweep_cycle(&self, prefix: &str) -> usize {
        let mut c = self.inner.sweep.cycles.lock().unwrap();
        let e = c.entry(prefix.to_string()).or_insert(0);
        *e += 1;
        *e
    }

    /// How many engines exist only because debt discovery opened them.
    pub fn sweep_resident_engines(&self) -> usize {
        self.inner.sweep.opened.lock().unwrap().len()
    }

    /// The custody value the scheduler installed for `prefix`, if it
    /// holds it.
    pub fn sweep_custody_seq(&self, prefix: &str) -> Option<u64> {
        self.inner.sweep.opened.lock().unwrap().get(prefix).copied()
    }

    /// Peak concurrently scheduler-held engines (DST bound gate).
    pub fn sweep_peak(&self) -> usize {
        self.inner
            .sweep
            .peak
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Tests only: reset the peak gauge between scenarios.
    #[cfg(test)]
    pub fn reset_sweep_peak(&self) {
        self.inner
            .sweep
            .peak
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Advance the sweep's rotation cycle; returns the previous value.
    pub fn next_sweep_cycle(&self) -> usize {
        self.inner
            .sweep
            .cycle
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// The prefixes the scheduler currently holds under custody.
    pub fn sweep_custody_prefixes(&self) -> Vec<String> {
        self.inner
            .sweep
            .opened
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// The tombstone walk's resume point.
    pub fn sweep_walk_cursor(&self) -> Option<String> {
        self.inner.sweep.walk_cursor.lock().unwrap().clone()
    }

    /// Set (or clear, on a full circle) the walk's resume point.
    pub fn set_sweep_walk_cursor(&self, after: Option<String>) {
        *self.inner.sweep.walk_cursor.lock().unwrap() = after;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reads() -> Arc<ReadUsageAccumulator> {
        Arc::new(ReadUsageAccumulator::new(crate::billing::MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b".into(),
        }))
    }

    /// Off = no ledger key; the slots start empty and are per service.
    #[test]
    fn billing_off_has_no_key_and_empty_slots() {
        let a = BillingService::new(None, reads());
        let b = BillingService::new(Some("k".into()), reads());
        assert_eq!(a.usage_key(), None);
        assert_eq!(b.usage_key().as_deref(), Some("k"));
        assert!(!a.read_spool_open() && !b.read_spool_open());
        assert_eq!(a.unflushed_reads(), (0, 0, 0));
        assert_eq!(a.sweep_resident_engines(), 0);
        assert_eq!(a.sweep_walk_cursor(), None);
        a.set_sweep_walk_cursor(Some("p1".into()));
        assert_eq!(a.sweep_walk_cursor().as_deref(), Some("p1"));
        assert_eq!(b.sweep_walk_cursor(), None, "services are independent");
        a.claim_sweep_custody("00", 7, 1);
        assert_eq!(a.sweep_resident_engines(), 1);
        assert_eq!(a.note_sweep_cycle("00"), 1);
        assert_eq!(a.note_sweep_cycle("00"), 2);
        a.release_sweep_custody("00");
        assert_eq!(a.sweep_resident_engines(), 0);
        assert_eq!(
            a.note_sweep_cycle("00"),
            1,
            "quantum accounting reset with custody"
        );
    }
}
