//! Whether the billing sweep may close an engine it opened (R29).
//!
//! The sweep installs custody on an engine it opened for billing; a
//! customer's resolution of the same engine revokes it; the sweep closes
//! the engine only while its own custody is still in place. Both words are
//! private to this type, so no caller can read or write one without the
//! other or reorder the handshake between them.

use std::sync::atomic::Ordering;

/// The atomic word the handshake runs over. Production runs `SweepCustody`
/// over std atomics; the Loom model runs the same methods over Loom's
/// atomics, which exist only inside a model.
pub(crate) trait CustodyWord {
    fn load(&self, order: Ordering) -> u64;
    fn store(&self, value: u64, order: Ordering);
    fn swap(&self, value: u64, order: Ordering) -> u64;
    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64>;
}

impl CustodyWord for std::sync::atomic::AtomicU64 {
    fn load(&self, order: Ordering) -> u64 {
        std::sync::atomic::AtomicU64::load(self, order)
    }
    fn store(&self, value: u64, order: Ordering) {
        std::sync::atomic::AtomicU64::store(self, value, order);
    }
    fn swap(&self, value: u64, order: Ordering) -> u64 {
        std::sync::atomic::AtomicU64::swap(self, value, order)
    }
    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        std::sync::atomic::AtomicU64::compare_exchange(self, current, new, success, failure)
    }
}

/// The sweep's custody of one engine and the external history that
/// revokes it. The invariants:
///
///   * custody installs ONLY onto an engine with zero external
///     history — a customer who resolved the engine before the sweep
///     probed it (including one who coalesced into the sweep's own
///     in-flight open) makes the install DECLINE, closing the
///     pre-mark window the R28 baseline model left open;
///   * an external resolution atomically revokes custody
///     (`stamp_external`);
///   * internal paths (tombstone walk, scaler) never stamp, so
///     maintenance cannot leak an engine out of the rotation;
///   * a close succeeds only via compare_exchange on the installer's
///     exact custody value — custody still present implies no
///     external stamp since install.
#[derive(Default)]
pub(crate) struct SweepCustody<W = std::sync::atomic::AtomicU64> {
    /// Nonzero once a customer has resolved this engine.
    last_external_seq: W,
    /// 0 = not held; otherwise the installing sweep's value.
    custody: W,
}

impl<W: CustodyWord> SweepCustody<W> {
    /// A customer resolved the engine: record it and revoke any custody.
    pub(crate) fn stamp_external(&self, seq: u64) {
        self.last_external_seq.store(seq, Ordering::Relaxed);
        self.custody.swap(0, Ordering::Relaxed);
    }

    /// Install custody under `seq`; false means the engine has external
    /// history, or gained it during the install.
    pub(crate) fn install(&self, seq: u64) -> bool {
        if self.last_external_seq.load(Ordering::Relaxed) != 0 {
            return false;
        }
        self.custody.store(seq, Ordering::Relaxed);
        // Re-check: a stamp that landed between the first read and the
        // store has either already revoked (swap saw our value) or carries
        // a newer last_external_seq; both mean decline.
        if self.last_external_seq.load(Ordering::Relaxed) != 0 {
            self.revoke_if(seq);
            return false;
        }
        true
    }

    /// Release custody only if it is still the installer's `seq`: the
    /// close and the decline path share this one check.
    pub(crate) fn revoke_if(&self, seq: u64) -> bool {
        self.custody
            .compare_exchange(seq, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Whether custody is still `seq` (the audit's hint; the close
    /// re-decides with `revoke_if`).
    pub(crate) fn holds(&self, seq: u64) -> bool {
        self.custody.load(Ordering::Relaxed) == seq
    }

    /// Whether any sweep holds custody (the budget's hint).
    pub(crate) fn held(&self) -> bool {
        self.custody.load(Ordering::Relaxed) != 0
    }

    #[cfg(test)]
    pub(crate) fn value(&self) -> u64 {
        self.custody.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn externally_resolved(&self) -> bool {
        self.last_external_seq.load(Ordering::Relaxed) != 0
    }
}

#[cfg(test)]
mod tests;
