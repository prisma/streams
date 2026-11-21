//! Requests waiting on a planned move or on a shard's open (§22 rev 2, K1-K2c;
//! GATE-DECISION-s22-addendum-rev2-2026-11-20). A request that resolves a shard a planned move
//! holds, or one this instance has to open (whatever started that open), waits there for as long
//! as the move or the build takes, and before this count it stayed active for all of it: the
//! cell's writes to the leaf piled up on the write gate while the engine built, and the next
//! writes, to any leaf, were shed (193 and 903 at the scouting leg's scale-outs).
//!
//! Such a request takes one place here, held until it is answered (the served request's scope,
//! `park`), not only while it waits, so a release that wakes every waiter at once sheds nothing.
//! Places are capped at the in-flight cap; past it a waiter is active as before. A place takes
//! the request out of the write gate only: it stays in the survival bound and in the heartbeat's
//! in-flight count (demand the planner should see), and it shares no pool with parked waits.

use std::sync::atomic::Ordering;

use super::AdmissionController;

/// RAII place of one request waiting on a planned move or on a shard's open.
pub(crate) struct WaitingPlace {
    ctl: AdmissionController,
}

impl Drop for WaitingPlace {
    fn drop(&mut self) {
        self.ctl.inner.waiting.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AdmissionController {
    /// A place for one more request waiting on a move or an open: `None` when the cap's worth
    /// already wait, and so always with no in-flight cap (no write gate to leave: the count before
    /// the add is never below 0).
    pub(crate) fn enter_waiting(&self) -> Option<WaitingPlace> {
        let waiting = &self.inner.waiting;
        if waiting.fetch_add(1, Ordering::Relaxed) >= self.max_inflight() {
            waiting.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(WaitingPlace { ctl: self.clone() })
    }
}

#[cfg(test)]
mod tests {
    use super::AdmissionController;

    fn controller(max_inflight: i64) -> AdmissionController {
        AdmissionController::new(crate::admission::AdmissionKnobs {
            max_inflight,
            per_stream_cap: 0,
            rss_shed_mb: 0,
            project_memory_pressure_bytes: 0,
            project_memory_release_pct: 0,
            subscriptions: crate::admission::SubscriptionCapacity {
                effective: 0,
                configured: 0,
            },
            record_ceiling_bytes: 0,
        })
    }

    /// The places held now (the cap's own count, `enter_waiting`).
    fn places_held(c: &AdmissionController) -> i64 {
        c.inner.waiting.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A request the write gate's count holds: its ticket and its slot (`write_slots`).
    fn counted(
        c: &AdmissionController,
    ) -> (
        super::super::InflightTicket,
        super::super::write_slots::Slot,
    ) {
        (
            c.enter(true),
            super::super::write_slots::Slot::new(c, false),
        )
    }

    /// At a cap of 4, four waiters take places and a fifth does not; with the cap's worth waiting,
    /// the write gate admits at 8 in flight (4 active) and refuses at 9 (5 active), and once the
    /// waiters are answered the place is taken again. With no cap there is no place (no gate to
    /// leave), and asking leaves the count at zero. The gate judges the requests its count holds
    /// (`write_slots`, edge change #272); a waiter's place and slot are answered together.
    #[test]
    fn waiters_leave_the_write_gate_up_to_the_cap() {
        let c = controller(4);
        let tickets: Vec<_> = (0..4).map(|_| counted(&c)).collect();
        let mut waiters: Vec<_> = (0..4).map(|_| counted(&c)).collect();
        let places: Vec<_> = (0..5).filter_map(|_| c.enter_waiting()).collect();
        waiters.iter().for_each(|(_, slot)| slot.waits());
        assert_eq!((places.len(), places_held(&c)), (4, 4));
        assert_eq!(c.admit_write_inflight(), Ok(()), "8 in flight, 4 waiting");
        let ninth = counted(&c);
        assert!(c.admit_write_inflight().is_err(), "9 in flight, 4 waiting");
        drop(ninth);
        waiters.clear();
        drop(places);
        waiters.extend((0..4).map(|_| counted(&c)));
        assert_eq!(places_held(&c), 0);
        assert!(
            c.admit_write_inflight().is_err(),
            "8 in flight, none waiting"
        );
        assert!(
            c.enter_waiting().is_some(),
            "a dropped place is taken again"
        );
        drop((tickets, waiters));
        let uncapped = controller(0);
        let none: Vec<_> = (0..3).filter_map(|_| uncapped.enter_waiting()).collect();
        assert_eq!(
            (none.len(), places_held(&uncapped)),
            (0, 0),
            "no cap, no place, and the count left at zero"
        );
    }

    /// A waiter stays in the survival bound and in the heartbeat's in-flight count: with nine in
    /// flight at a cap of 2, two of them waiting, the bound still refuses (9 over 4 x 2) and the
    /// heartbeat reads nine.
    #[test]
    fn waiters_stay_in_the_survival_bound_and_the_heartbeat() {
        let c = controller(2);
        let _tickets: Vec<_> = (0..9).map(|_| c.enter(true)).collect();
        let _places: Vec<_> = (0..2).filter_map(|_| c.enter_waiting()).collect();
        assert_eq!(places_held(&c), 2);
        assert!(c.survival_refused(9, "/v1/stream/a"));
        assert_eq!(c.swap_active_peak().0, 9);
    }
}
