//! Billing service (WP-02 / PR 6-E): the instance's usage-ledger key,
//! the read-usage accumulator, the read spool and usage rollup slots
//! (opened lazily by the telemetry loops, exactly once), and the sweep
//! scheduler's bookkeeping — extracted from `http::AppState`. One per
//! runtime: parallel rigs never sum their residents into one gauge.

use std::sync::{Arc, OnceLock};

use crate::billing::{ReadSpool, ReadUsageAccumulator, SweepSched};
use crate::rollup::UsageRollup;

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
    rollup: OnceLock<Arc<UsageRollup>>,
    sweep: SweepSched,
}

impl BillingService {
    pub fn new(usage_key: Option<String>, reads: Arc<ReadUsageAccumulator>) -> Self {
        Self {
            inner: Arc::new(Inner {
                usage_key,
                reads,
                read_spool: OnceLock::new(),
                rollup: OnceLock::new(),
                sweep: SweepSched::default(),
            }),
        }
    }

    pub fn usage_key(&self) -> Option<String> {
        self.inner.usage_key.clone()
    }

    pub fn reads(&self) -> &Arc<ReadUsageAccumulator> {
        &self.inner.reads
    }

    pub fn read_spool(&self) -> Option<&Arc<ReadSpool>> {
        self.inner.read_spool.get()
    }

    /// Install the read spool exactly once; a second install returns the
    /// rejected value (the telemetry loop opens it, a rig may pre-open).
    pub fn install_read_spool(&self, spool: Arc<ReadSpool>) -> Result<(), Arc<ReadSpool>> {
        self.inner.read_spool.set(spool)
    }

    pub fn rollup(&self) -> Option<&Arc<UsageRollup>> {
        self.inner.rollup.get()
    }

    pub fn install_rollup(&self, rollup: Arc<UsageRollup>) -> Result<(), Arc<UsageRollup>> {
        self.inner.rollup.set(rollup)
    }

    /// The sweep scheduler's bookkeeping (custody marks, quantum cycles,
    /// the walk cursor) — the billing sweep's own state.
    pub fn sweep(&self) -> &SweepSched {
        &self.inner.sweep
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
        assert!(a.read_spool().is_none() && a.rollup().is_none());
        assert!(!Arc::ptr_eq(a.reads(), b.reads()));
        assert_eq!(a.sweep().held_for_test(), 0);
    }
}
