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
///     (`stamp_external`) — from the gate's Ready path outside the
///     serving map's guard, so the handshake, not a lock, orders it;
///   * internal paths (tombstone walk, scaler) never stamp, so
///     maintenance cannot leak an engine out of the rotation;
///   * a close succeeds only via compare_exchange on the installer's
///     exact custody value — custody still present implies no
///     external stamp since install.
///
/// A stamp writes its history then revokes; an install publishes custody
/// then re-reads the history: the store-buffering shape, where two
/// relaxed sides can each miss the other and leave custody installed over
/// external history (even on x86). Every write to `custody` is therefore
/// an RMW (the stamp's swap, the install's swap, the revoke CAS), all
/// SeqCst. Whichever of the stamp's and the install's swaps comes second
/// in custody's order settles it: the stamp's second revokes; the
/// install's second reads the stamp's swap (every later write is an RMW,
/// so its release sequence is never broken), so the stamp's history write
/// happens before the install's re-check, which reads it and declines.
/// So once both have returned, custody is 0. The hints (`holds`, `held`)
/// stay Relaxed: every decision they feed is re-made by `revoke_if`. The
/// cost is one extra locked exchange per external resolution.
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
        self.last_external_seq.store(seq, Ordering::SeqCst);
        self.custody.swap(0, Ordering::SeqCst);
    }

    /// Install custody under `seq`; false means the engine has external
    /// history, or gained it during the install.
    pub(crate) fn install(&self, seq: u64) -> bool {
        if self.last_external_seq.load(Ordering::SeqCst) != 0 {
            return false;
        }
        self.custody.swap(seq, Ordering::SeqCst);
        // Re-check: a stamp landed during the install; release custody
        // unless the stamp's swap already did.
        if self.last_external_seq.load(Ordering::SeqCst) != 0 {
            self.revoke_if(seq);
            return false;
        }
        true
    }

    /// Release custody only if it is still the installer's `seq`: the
    /// close and the decline path share this one check.
    pub(crate) fn revoke_if(&self, seq: u64) -> bool {
        self.custody
            .compare_exchange(seq, 0, Ordering::SeqCst, Ordering::Relaxed)
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
