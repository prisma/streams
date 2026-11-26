//! The gate's two stops (§22 step 6, C10; GATE-DECISION-s22-join-2026-11-20 Addendum 41 B item 4).
//!
//! A stop or a drain's seal ends the gate's opens for good. A leaver's seal must be reversible:
//! the count can come back before its release. So the gate keeps the two apart, and the readers
//! that refuse opens (`get_or_open`, `wait_retired`, `publish_open`) keep their exact text,
//! `stopping.load(Ordering::SeqCst)`, now answered for either stop. `store` writes the stop
//! alone, so `stop()` and `seal_for_drain` are unchanged too. The fence judge (`judge_fence`)
//! reads either stop (`closed_to_opens`, Addendum 60 §1): whoever set one and gave shards up has
//! counted them, and a leaver's ordinary seal is set only once it holds nothing.

use std::sync::atomic::{AtomicBool, Ordering};

/// The gate's stops: the one-way stop (a stop, a drain's seal, its evacuation) and the
/// leaver's reversible seal.
#[derive(Debug, Default)]
pub(super) struct Stops {
    stop: AtomicBool,
    leave: AtomicBool,
}

impl Stops {
    /// Whether the gate starts no more opens, for either stop: the one-way stop or a leaver's
    /// seal. Two loads at `order`, not one atomic read of both.
    pub(super) fn load(&self, order: Ordering) -> bool {
        self.stop.load(order) || self.leave.load(order)
    }

    /// Sets or clears the one-way stop; a leaver's seal is not touched.
    pub(super) fn store(&self, stopped: bool, order: Ordering) {
        self.stop.store(stopped, order);
    }

    /// Sets or clears a leaver's seal; the one-way stop is not touched.
    fn seal_leave(&self, sealed: bool) {
        self.leave.store(sealed, Ordering::SeqCst);
    }

    /// Whether a leaver's seal is set.
    fn leave_sealed(&self) -> bool {
        self.leave.load(Ordering::SeqCst)
    }
}

impl super::OpenGate {
    /// A leaver's seal (C10): stop starting opens if this instance holds nothing, judged as the
    /// drain's seal is (`seal_for_drain`), but reversibly. Whether it is sealed.
    pub(crate) fn seal_for_leave(&self) -> bool {
        self.hold_nothing_then(|stops| stops.seal_leave(true))
    }

    /// The leave's fallback past its phase (Addendum 41 B, draft §4): refuse opens whatever this
    /// instance still holds, under the gate's state as `stop` does; unlike a stop, reversible.
    pub(crate) fn stop_for_leave(&self) {
        let Ok(_state) = self.inner.st.lock() else {
            return;
        };
        self.inner.stopping.seal_leave(true);
    }

    /// Lift a leaver's seal once this instance holds nothing (Addendum 60 §1): while an engine the
    /// fallback counted is resident the seal stays, so a count that returns cannot reopen the
    /// window a peer's fence of it would be judged in. Whether no leaver's seal is left.
    pub(crate) fn unseal_leave(&self) -> bool {
        !self.leave_sealed() || self.hold_nothing_then(|stops| stops.seal_leave(false))
    }

    /// Whether a leaver's seal is set.
    pub(crate) fn leave_sealed(&self) -> bool {
        self.inner.stopping.leave_sealed()
    }

    /// Whether the gate starts no opens, for either stop: the fence judge's test (`judge_fence`,
    /// Addendum 60 §1), since whoever set a stop and gave shards up has counted them.
    pub(crate) fn closed_to_opens(&self) -> bool {
        self.inner.stopping.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::Stops;

    #[test]
    fn the_gate_refuses_opens_for_either_stop_and_only_the_one_way_stop_is_sealed() {
        let stops = Stops::default();
        assert_eq!(
            (
                stops.load(Ordering::SeqCst),
                stops.stop.load(Ordering::SeqCst)
            ),
            (false, false)
        );

        stops.leave.store(true, Ordering::SeqCst);
        assert_eq!(
            (
                stops.load(Ordering::SeqCst),
                stops.stop.load(Ordering::SeqCst)
            ),
            (true, false),
            "a leaver's seal refuses opens and is not the drain's stop"
        );

        stops.store(true, Ordering::SeqCst);
        stops.leave.store(false, Ordering::SeqCst);
        assert_eq!(
            (
                stops.load(Ordering::SeqCst),
                stops.stop.load(Ordering::SeqCst)
            ),
            (true, true),
            "clearing a leaver's seal leaves the one-way stop"
        );

        stops.store(false, Ordering::SeqCst);
        stops.leave.store(true, Ordering::SeqCst);
        stops.store(true, Ordering::SeqCst);
        stops.store(false, Ordering::SeqCst);
        assert_eq!(
            (
                stops.load(Ordering::SeqCst),
                stops.leave.load(Ordering::SeqCst)
            ),
            (true, true),
            "the stop's store does not touch a leaver's seal"
        );
    }
}
