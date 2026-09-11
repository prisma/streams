//! Read delivery observations and their ordered sealed batches share one owner.
use super::{BillingIdentity, MeterSource, ReadBatch, ReadRow, is_reserved_stream};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, atomic::AtomicU64};

/// Flush thresholds (§7.2). The active map seals into a batch on any of
/// these; a sealed batch is what the drainer appends to `_usage`.
pub(crate) const READ_FLUSH_INTERVAL_MS: i64 = 10_000;
const READ_FLUSH_MAX_ENTRIES: usize = 10_000;
const READ_FLUSH_MAX_EST_BYTES: usize = 1 << 20;
/// Sealed batches waiting for the ledger. When the ledger is down long
/// enough to fill this, sealing PAUSES and deltas keep merging into the
/// active map — attribution is never discarded (§14.1), memory stays
/// bounded by stream cardinality, and the lag is visible.
pub(crate) const READ_SEALED_MAX_BATCHES: usize = 64;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RowDelta {
    pub read_payload_bytes: u64,
    pub read_records: u64,
    pub read_operations: u64,
    pub queue_operations: u64,
    pub append_requests: u64,
}

struct ActiveMap {
    rows: HashMap<BillingIdentity, RowDelta>,
    opened_ms: i64,
    /// Rough encoded-size estimate (identity strings + numbers), used
    /// only against READ_FLUSH_MAX_EST_BYTES.
    est_bytes: usize,
}

struct ReadUsageState {
    active: ActiveMap,
    sealed: VecDeque<ReadBatch>,
    seq: u64,
}

pub(crate) struct ReadUsageAccumulator {
    state: Mutex<ReadUsageState>,
    source: MeterSource,
    /// Batches that could not seal because the sealed queue was full —
    /// a telemetry-lag signal, not data loss (rows kept merging).
    pub(crate) seal_deferrals: AtomicU64,
}

impl ReadUsageAccumulator {
    pub(crate) fn new(source: MeterSource) -> Self {
        ReadUsageAccumulator {
            state: Mutex::new(ReadUsageState {
                active: ActiveMap {
                    rows: HashMap::new(),
                    opened_ms: 0,
                    est_bytes: 0,
                },
                sealed: VecDeque::new(),
                seq: 0,
            }),
            source,
            seal_deferrals: AtomicU64::new(0),
        }
    }

    /// Add one observation. Reserved system streams are never metered
    /// (self-metering exclusion, §8.4).
    #[expect(
        clippy::unwrap_used,
        reason = "ReadUsageAccumulator meter; a poisoned transfer may have only partly charged a delivery; silently accepting another observation would hide incomplete billing"
    )]
    pub(crate) fn meter(&self, id: &BillingIdentity, d: RowDelta) {
        if is_reserved_stream(&id.stream_name) {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let a = &mut state.active;
        if a.rows.is_empty() {
            a.opened_ms = crate::shard::now_ms();
        }
        match a.rows.get_mut(id) {
            Some(row) => {
                row.read_payload_bytes += d.read_payload_bytes;
                row.read_records += d.read_records;
                row.read_operations += d.read_operations;
                row.queue_operations += d.queue_operations;
                row.append_requests += d.append_requests;
                a.est_bytes += 8;
            }
            None => {
                a.est_bytes += 120
                    + id.account_id.len()
                    + id.project_id.len()
                    + id.stream_id.len()
                    + id.stream_name.len();
                a.rows.insert(id.clone(), d);
            }
        }
        if a.rows.len() >= READ_FLUSH_MAX_ENTRIES || a.est_bytes >= READ_FLUSH_MAX_EST_BYTES {
            self.seal_locked(&mut state);
        }
    }

    fn seal_locked(&self, state: &mut ReadUsageState) {
        let ReadUsageState {
            active: a,
            sealed,
            seq,
        } = state;
        if a.rows.is_empty() {
            return;
        }
        if sealed.len() >= READ_SEALED_MAX_BATCHES {
            // Ledger outage: keep merging instead of sealing. Rotation
            // resumes as soon as the drainer catches up.
            self.seal_deferrals
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        let rows = std::mem::take(&mut a.rows)
            .into_iter()
            .map(|(identity, d)| ReadRow {
                identity,
                read_payload_bytes: d.read_payload_bytes,
                read_records: d.read_records,
                read_operations: d.read_operations,
                queue_operations: d.queue_operations,
                append_requests: d.append_requests,
            })
            .collect();
        let now = crate::shard::now_ms();
        sealed.push_back(ReadBatch {
            source: self.source.clone(),
            seq: *seq,
            from_ms: a.opened_ms,
            to_ms: now,
            rows,
        });
        // Preserve the previous atomic sequence's wrapping arithmetic.
        *seq = seq.wrapping_add(1);
        a.est_bytes = 0;
        a.opened_ms = now;
    }

    /// Timer/shutdown entry: seal if the active interval is at least
    /// `max_age_ms` old (0 = unconditionally).
    #[expect(
        clippy::unwrap_used,
        reason = "ReadUsageAccumulator seal_if_aged; a poisoned transfer may have moved only some active rows; reconstructing a batch could double count or discard usage"
    )]
    pub(crate) fn seal_if_aged(&self, max_age_ms: i64) {
        let mut state = self.state.lock().unwrap();
        let a = &mut state.active;
        if a.rows.is_empty() {
            return;
        }
        if max_age_ms == 0 || crate::shard::now_ms() - a.opened_ms >= max_age_ms {
            self.seal_locked(&mut state);
        }
    }

    /// Hand up to `max` sealed batches to the drainer. The drainer
    /// requeues on emission failure — a batch leaves this process only
    /// after `_usage` acknowledged it.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadUsageAccumulator drain_sealed; a poisoned queue may have an incomplete drain; continuing could lose the only retained copy of a billed observation"
    )]
    pub(crate) fn drain_sealed(&self, max: usize) -> Vec<ReadBatch> {
        let mut state = self.state.lock().unwrap();
        let sealed = &mut state.sealed;
        let n = sealed.len().min(max);
        sealed.drain(..n).collect()
    }

    /// Failed emission: put the batches back at the FRONT, original
    /// order, so sequence numbers stay as monotone as delivery allows.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadUsageAccumulator requeue; a poisoned queue may already contain part of this return; silent recovery could duplicate or lose billing batches"
    )]
    pub(crate) fn requeue(&self, batches: Vec<ReadBatch>) {
        let mut state = self.state.lock().unwrap();
        let sealed = &mut state.sealed;
        for b in batches.into_iter().rev() {
            sealed.push_front(b);
        }
    }

    /// (active rows, active est bytes, sealed batches) — the §14.2 lag
    /// gauges, and the "maximum possible loss" numerator.
    #[expect(
        clippy::unwrap_used,
        reason = "ReadUsageAccumulator unflushed; a poisoned transfer has no trustworthy active or sealed total; returning invented gauges would conceal billing loss"
    )]
    pub(crate) fn unflushed(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (
            state.active.rows.len(),
            state.active.est_bytes,
            state.sealed.len(),
        )
    }

    /// Tests inspect attribution before sealing; production exposes lag gauges.
    #[cfg(test)]
    pub(crate) fn snapshot_active(&self) -> Vec<(BillingIdentity, RowDelta)> {
        self.state
            .lock()
            .unwrap()
            .active
            .rows
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests;
