//! The gate's two stops (§22 step 6, C10; GATE-DECISION-s22-join-2026-11-20 Addendum 41 B item 4).
//!
//! A stop or a drain's seal ends the gate's opens for good. A leaver's seal must be reversible:
//! the count can come back before its release. So the gate keeps the two apart, and the readers
//! that refuse opens (`get_or_open`, `wait_retired`, `publish_open`) keep their exact text,
//! `stopping.load(Ordering::SeqCst)`, now answered for either stop. `store` writes the stop
//! alone, so `stop()` and `seal_for_drain` are unchanged too, and `sealed()`, which `judge_fence`
//! reads, stays the drain's stop: a leaver's seal holds nothing, so it has no writer to misjudge.

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

    /// Whether the one-way stop is set: a stop, a drain's seal or its evacuation, never a
    /// leaver's seal.
    pub(super) fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
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
            (stops.load(Ordering::SeqCst), stops.stopped()),
            (false, false)
        );

        stops.leave.store(true, Ordering::SeqCst);
        assert_eq!(
            (stops.load(Ordering::SeqCst), stops.stopped()),
            (true, false),
            "a leaver's seal refuses opens and is not the drain's stop"
        );

        stops.store(true, Ordering::SeqCst);
        stops.leave.store(false, Ordering::SeqCst);
        assert_eq!(
            (stops.load(Ordering::SeqCst), stops.stopped()),
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
