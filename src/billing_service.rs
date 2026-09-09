//! Billing service (WP-02 / PR 6-E): the instance's usage-ledger key,
//! the read-usage accumulator, the read spool and usage rollup slots
//! (opened lazily by the telemetry loops, exactly once), and the sweep
//! scheduler's bookkeeping — extracted from `http::AppState`. One per
//! runtime: parallel rigs never sum their residents into one gauge.

use std::sync::{Arc, OnceLock};

use crate::billing::{ReadBatch, ReadSpool, ReadUsageAccumulator, SweepSched};

#[derive(Clone)]
pub(crate) struct BillingService {
    inner: Arc<Inner>,
}

struct Inner {
    /// The usage ledger stream key; None = billing off (no ledger, no
    /// spool, no rollup, no ops/audit ledger appends).
    usage_key: Option<String>,
    reads: Arc<ReadUsageAccumulator>,
    read_spool: OnceLock<Arc<ReadSpool>>,
    sweep: SweepSched,
    drain_progress: std::sync::Mutex<DrainProgress>,
}

#[derive(Default)]
struct DrainProgress {
    engine_after: Option<String>,
    // mt-lint: allow(name-keyed-map): keys are shard engine path prefixes, tracking bounded outbox scan cursors, never customer stream names.
    rows_after: std::collections::HashMap<String, [u8; 16]>,
}

/// Volatile batches remain owned until the spool or optional-mode ledger
/// accepts them durably. Cancellation requeues exactly these batches.
pub(crate) struct ReadDrain<'a> {
    accumulator: &'a ReadUsageAccumulator,
    pub batches: Vec<ReadBatch>,
}

impl<'a> ReadDrain<'a> {
    pub(crate) fn new(accumulator: &'a ReadUsageAccumulator, max: usize) -> Self {
        Self {
            accumulator,
            batches: accumulator.drain_sealed(max),
        }
    }

    pub(crate) fn accepted(&mut self) {
        self.batches.clear();
    }
}

impl Drop for ReadDrain<'_> {
    fn drop(&mut self) {
        self.accumulator.requeue(std::mem::take(&mut self.batches));
    }
}

/// What the operator surfaces show about the durable read spool.
pub(crate) struct ReadSpoolStats {
    pub quarantined: u64,
    pub pending_rows: u64,
    pub pending_bytes: u64,
    /// (l0 ssts, l0 bytes, runs, mid-run bytes)
    pub l0: (u64, u64, u64, u64),
}

impl BillingService {
    pub(crate) fn new(usage_key: Option<String>, reads: Arc<ReadUsageAccumulator>) -> Self {
        Self {
            inner: Arc::new(Inner {
                usage_key,
                reads,
                read_spool: OnceLock::new(),
                sweep: SweepSched::default(),
                drain_progress: Default::default(),
            }),
        }
    }

    /// The usage ledger's stream key — `None` means billing is off.
    pub(crate) fn usage_key(&self) -> Option<String> {
        self.inner.usage_key.clone()
    }

    // -- read metering ------------------------------------------------

    /// Meter one read against a billing identity.
    pub(crate) fn meter_read(
        &self,
        id: &crate::billing::BillingIdentity,
        delta: crate::billing::RowDelta,
    ) {
        self.inner.reads.meter(id, delta);
    }

    /// Meter one delivered live chunk (§4.2: what actually left the body).
    pub(crate) fn meter_read_chunk(
        &self,
        id: &crate::billing::BillingIdentity,
        payload_bytes: u64,
        records: u64,
    ) {
        crate::billing::meter_read_chunk(&self.inner.reads, id, payload_bytes, records);
    }

    /// Seal the open read window once it is older than `max_age_ms`.
    pub(crate) fn seal_aged_reads(&self, max_age_ms: i64) {
        self.inner.reads.seal_if_aged(max_age_ms);
    }

    pub(crate) fn read_drain(&self, max: usize) -> ReadDrain<'_> {
        ReadDrain::new(&self.inner.reads, max)
    }

    /// Fair bounded engine visits; cursors are advisory and durable outbox
    /// rows retain the work on cancellation, failure or owner replacement.
    pub(crate) fn drain_engines(
        &self,
        mut engines: Vec<Arc<crate::shard::ShardEngine>>,
        max: usize,
    ) -> Vec<Arc<crate::shard::ShardEngine>> {
        engines.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        let mut progress = self.inner.drain_progress.lock().unwrap();
        progress
            .rows_after
            .retain(|prefix, _| engines.iter().any(|e| &e.prefix == prefix));
        if let Some(after) = &progress.engine_after {
            let split = engines.partition_point(|engine| &engine.prefix <= after);
            engines.rotate_left(split);
        }
        engines.truncate(max);
        engines
    }

    pub(crate) fn begin_drain_engine(&self, prefix: &str) {
        self.inner.drain_progress.lock().unwrap().engine_after = Some(prefix.to_string());
    }

    pub(crate) fn drain_row_cursor(&self, prefix: &str) -> Option<[u8; 16]> {
        self.inner
            .drain_progress
            .lock()
            .unwrap()
            .rows_after
            .get(prefix)
            .copied()
    }

    pub(crate) fn set_drain_row_cursor(&self, prefix: &str, after: Option<[u8; 16]>) {
        let mut progress = self.inner.drain_progress.lock().unwrap();
        if let Some(after) = after {
            progress.rows_after.insert(prefix.to_string(), after);
        } else {
            progress.rows_after.remove(prefix);
        }
    }

    /// Tests only: inspect/remove sealed batches without a guarded handoff.
    #[cfg(test)]
    pub(crate) fn drain_sealed_reads(&self, max: usize) -> Vec<ReadBatch> {
        self.inner.reads.drain_sealed(max)
    }

    /// Tests only: install a batch at the accumulator head.
    #[cfg(test)]
    pub(crate) fn requeue_reads(&self, batches: Vec<ReadBatch>) {
        self.inner.reads.requeue(batches);
    }

    /// (rows, estimated bytes, sealed batches) not yet in the ledger.
    pub(crate) fn unflushed_reads(&self) -> (usize, usize, usize) {
        self.inner.reads.unflushed()
    }

    /// How often sealing was deferred under memory pressure.
    pub(crate) fn read_seal_deferrals(&self) -> u64 {
        self.inner
            .reads
            .seal_deferrals
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Tests only: the raw accumulator, for scenarios that drive the
    /// metering window directly (seal-now, snapshot, drain).
    #[cfg(test)]
    pub(crate) fn reads(&self) -> &Arc<ReadUsageAccumulator> {
        &self.inner.reads
    }

    /// Tests only: the raw spool.
    #[cfg(test)]
    pub(crate) fn read_spool(&self) -> Option<&Arc<ReadSpool>> {
        self.inner.read_spool.get()
    }

    // -- the durable read spool ---------------------------------------

    /// Install the spool. `Err` means one was already installed.
    pub(crate) fn install_read_spool(&self, spool: Arc<ReadSpool>) -> Result<(), Arc<ReadSpool>> {
        self.inner.read_spool.set(spool)
    }

    /// Whether the durable spool is open (required mode demands it).
    pub(crate) fn read_spool_open(&self) -> bool {
        self.inner.read_spool.get().is_some()
    }

    /// Persist sealed read batches into the spool before the ledger.
    pub(crate) async fn spool_sealed_reads(&self, max: usize) -> Result<(), String> {
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
    pub(crate) async fn remove_spooled(&self, keys: &[Vec<u8>]) -> Result<(), String> {
        match self.inner.read_spool.get() {
            Some(spool) => spool.remove(keys).await.map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    /// The operator view of the spool, if it is open.
    pub(crate) fn read_spool_stats(&self) -> Option<ReadSpoolStats> {
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
    pub(crate) async fn read_spool_health(&self) -> (bool, u64, u64) {
        match self.inner.read_spool.get() {
            Some(sp) => (true, sp.quarantined_count(), sp.depth().await as u64),
            None => (false, 0, 0),
        }
    }

    // -- the sweep protocol (R30 custody) ------------------------------

    /// Record that the sweep scheduler holds `prefix` under custody
    /// value `seq`, and refresh the peak gauge.
    pub(crate) fn claim_sweep_custody(&self, prefix: &str, seq: u64, held_now: usize) {
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
    pub(crate) fn release_sweep_custody(&self, prefix: &str) {
        self.inner.sweep.opened.lock().unwrap().remove(prefix);
        self.inner.sweep.cycles.lock().unwrap().remove(prefix);
    }

    /// Count one residency cycle for `prefix`; returns the new count.
    pub(crate) fn note_sweep_cycle(&self, prefix: &str) -> usize {
        let mut c = self.inner.sweep.cycles.lock().unwrap();
        let e = c.entry(prefix.to_string()).or_insert(0);
        *e += 1;
        *e
    }

    /// How many engines exist only because debt discovery opened them.
    pub(crate) fn sweep_resident_engines(&self) -> usize {
        self.inner.sweep.opened.lock().unwrap().len()
    }

    /// The custody value the scheduler installed for `prefix`, if it
    /// holds it.
    pub(crate) fn sweep_custody_seq(&self, prefix: &str) -> Option<u64> {
        self.inner.sweep.opened.lock().unwrap().get(prefix).copied()
    }

    /// Peak concurrently scheduler-held engines (DST bound gate).
    pub(crate) fn sweep_peak(&self) -> usize {
        self.inner
            .sweep
            .peak
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Tests only: reset the peak gauge between scenarios.
    #[cfg(test)]
    pub(crate) fn reset_sweep_peak(&self) {
        self.inner
            .sweep
            .peak
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Advance the sweep's rotation cycle; returns the previous value.
    pub(crate) fn next_sweep_cycle(&self) -> usize {
        self.inner
            .sweep
            .cycle
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// The prefixes the scheduler currently holds under custody.
    pub(crate) fn sweep_custody_prefixes(&self) -> Vec<String> {
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
    pub(crate) fn sweep_walk_cursor(&self) -> Option<String> {
        self.inner.sweep.walk_cursor.lock().unwrap().clone()
    }

    /// Set (or clear, on a full circle) the walk's resume point.
    pub(crate) fn set_sweep_walk_cursor(&self, after: Option<String>) {
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

#[cfg(test)]
mod drain_ownership_tests {
    use super::*;
    use crate::billing::MeterSource;

    #[tokio::test]
    async fn r09_cancelled_optional_read_drain_requeues_owned_batches() {
        let source = MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b".into(),
        };
        let service = BillingService::new(
            Some("key".into()),
            Arc::new(ReadUsageAccumulator::new(source.clone())),
        );
        service.requeue_reads(vec![ReadBatch {
            source,
            seq: 7,
            from_ms: 10,
            to_ms: 20,
            rows: vec![],
        }]);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let task_service = service.clone();
        let task = tokio::spawn(async move {
            let drain = task_service.read_drain(1);
            assert_eq!(drain.batches[0].seq, 7);
            entered_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(drain);
        });
        entered_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        let mut recovered = service.read_drain(1);
        assert_eq!(recovered.batches.len(), 1);
        assert_eq!(recovered.batches[0].seq, 7);
        recovered.accepted();
        drop(recovered);
        assert!(service.drain_sealed_reads(1).is_empty());
    }
}

#[cfg(test)]
mod drain_fairness_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r09_budget_exhaustion_on_first_engine_still_rotates_every_engine() {
        let service = BillingService::new(
            None,
            Arc::new(ReadUsageAccumulator::new(crate::billing::MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "b".into(),
            })),
        );
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let mut engines = Vec::new();
        for index in 0..5 {
            let prefix = format!("r09-fair/{index}");
            let db = Arc::new(
                slatedb::Db::builder(prefix.as_str(), store.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            engines.push(crate::shard::ShardEngine::start(
                prefix,
                db,
                store.clone(),
                crate::shard::ShardConfig::default(),
                tx,
                None,
                Default::default(),
            ));
        }
        let mut seen = Vec::new();
        for _ in 0..10 {
            let page = service.drain_engines(engines.clone(), 4);
            assert_eq!(page.len(), 4);
            // Model a byte budget consumed by this first engine. The
            // unvisited reserved engines must not advance the cursor.
            service.begin_drain_engine(&page[0].prefix);
            seen.push(page[0].prefix.clone());
        }
        for engine in &engines {
            assert_eq!(
                seen.iter()
                    .filter(|prefix| *prefix == &engine.prefix)
                    .count(),
                2
            );
        }
        for engine in engines {
            engine.begin_close();
            let _ = engine.db.close().await;
        }
    }
}
