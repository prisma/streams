//! The write gate's count (Addendum 27 of the forward-wedge ruling, 2026-11-20; edge change #272).
//!
//! The write gate judged every request counted in flight at its arrival (`active - waiting`), the
//! writes that had not yet met the gate and the requests already shed but not yet answered
//! included. A burst that arrived at once was counted whole before most of its checks ran, so
//! nearly every check past the line shed: 458 against an excess of 188, and 0 or 459 for the same
//! pile by the order the checks fell in.
//!
//! The judged count is today's, less those two artefacts:
//! - a request that will meet the gate on its ordinary path (`product::operation::meets_write_gate`, by
//!   method and route at its arrival) enters at 0 and adds itself when it meets the gate, in one step with the check:
//!   past its line it takes nothing back out and is refused, so N writes checked at once beside R
//!   other requests shed exactly `N + R - line`;
//! - a shed request never adds itself;
//! - one that leaves by another door adds itself there: a write forwarded to its stream's owner
//!   when the forward begins (`park::forwarding`).
//!
//! Every other request counts from its arrival as before. Parked waits and the places of requests
//! waiting on a move or an open stay out of the count as before (`active - waiting`), for a
//! request the count holds. A request leaves the count when it is answered. The pre-auth survival
//! bound, the heartbeat's in-flight count and the RSS gates read `inflight` as before.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use super::AdmissionController;

/// The judged count's two parts on one controller.
#[derive(Debug, Default)]
pub(crate) struct Judged {
    /// Requests the count holds (`Slot::counted`), until each is answered.
    counted: AtomicI64,
    /// Of those, the ones holding a place among the requests waiting on a move or an open.
    waiting: AtomicI64,
    /// Served requests the gate met: entered at 0, and counted from their arrival (the classifier's
    /// pins: a route that meets the gate enters at 0).
    #[cfg(test)]
    pub(crate) met: [std::sync::atomic::AtomicU64; 2],
    /// Served requests entered at 0 answered without meeting the gate or another door (the
    /// classifier's pins: a route that enters at 0 meets the gate).
    #[cfg(test)]
    pub(crate) unmet: std::sync::atomic::AtomicU64,
    /// Gate meetings by the verdict fair shedding judged their request with: blamed, spared,
    /// even, none (the burst DST's verdicts, as the gate saw them).
    #[cfg(test)]
    pub(crate) verdicts: [std::sync::atomic::AtomicU64; 4],
}

/// One served request's place in the judged count (`park::Request`), returned when it is
/// answered.
pub(crate) struct Slot {
    ctl: AdmissionController,
    /// The count holds it.
    counted: AtomicBool,
    /// It holds a place among the requests waiting on a move or an open.
    waiting: AtomicBool,
    /// It entered at 0 (`product::operation::meets_write_gate`).
    #[cfg(test)]
    gated: bool,
    /// It met the gate or another door.
    #[cfg(test)]
    met: AtomicBool,
}

impl Slot {
    /// A served request's slot: counted from its arrival unless it will meet the gate.
    pub(super) fn new(ctl: &AdmissionController, gated: bool) -> Self {
        let slot = Self {
            ctl: ctl.clone(),
            counted: AtomicBool::new(false),
            waiting: AtomicBool::new(false),
            #[cfg(test)]
            gated,
            #[cfg(test)]
            met: AtomicBool::new(false),
        };
        if !gated {
            slot.count();
        }
        slot
    }

    /// The count holds the request from now (once).
    fn count(&self) {
        if self.counted.swap(true, Ordering::Relaxed) {
            return;
        }
        let judged = &self.ctl.inner.judged;
        judged.counted.fetch_add(1, Ordering::Relaxed);
        if self.waiting.load(Ordering::Relaxed) {
            judged.waiting.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The request meets the gate at `line`: one it does not hold yet joins the count in one step
    /// with the check, or is refused and takes nothing; one it holds is judged where it stands.
    pub(super) fn meet_gate(&self, line: i64) -> bool {
        #[cfg(test)]
        self.note_met();
        if self.counted.load(Ordering::Relaxed) {
            return self.ctl.judged() <= line;
        }
        let judged = &self.ctl.inner.judged;
        // A request holding a waiting place is out of the judged count once counted, as today's
        // `active - waiting` counted it in and out: it adds nothing to its own check.
        let own = i64::from(!self.waiting.load(Ordering::Relaxed));
        let mut now = judged.counted.load(Ordering::Relaxed);
        loop {
            if now + own - self.ctl.out_of_count() > line {
                return false;
            }
            match judged.counted.compare_exchange_weak(
                now,
                now + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(seen) => now = seen,
            }
        }
        self.counted.store(true, Ordering::Relaxed);
        if self.waiting.load(Ordering::Relaxed) {
            judged.waiting.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    /// The request leaves by another door that holds it here (a forward to its owner): the count
    /// holds it from now.
    pub(super) fn leaves_by_another_door(&self) {
        #[cfg(test)]
        self.met.store(true, Ordering::Relaxed);
        self.count();
    }

    /// The classifier's pins: which kind of served request the gate met, once.
    #[cfg(test)]
    fn note_met(&self) {
        if !self.met.swap(true, Ordering::Relaxed) {
            self.ctl.inner.judged.met[usize::from(!self.gated)].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// What the request adds to its own check: one, unless the count holds it already or it holds
    /// a waiting place.
    pub(super) fn uncounted(&self) -> bool {
        !self.counted.load(Ordering::Relaxed) && !self.waiting.load(Ordering::Relaxed)
    }

    /// The request took a place among those waiting on a move or an open (once).
    pub(super) fn waits(&self) {
        if !self.waiting.swap(true, Ordering::Relaxed) && self.counted.load(Ordering::Relaxed) {
            self.ctl
                .inner
                .judged
                .waiting
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        #[cfg(test)]
        if self.gated && !*self.met.get_mut() {
            self.ctl.inner.judged.unmet.fetch_add(1, Ordering::Relaxed);
        }
        if !*self.counted.get_mut() {
            return;
        }
        let judged = &self.ctl.inner.judged;
        judged.counted.fetch_sub(1, Ordering::Relaxed);
        if *self.waiting.get_mut() {
            judged.waiting.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl AdmissionController {
    /// The count the write gate judges: the requests it holds, less the parked and those holding a
    /// place among the requests waiting on a move or an open.
    pub(crate) fn judged(&self) -> i64 {
        self.inner.judged.counted.load(Ordering::Relaxed) - self.out_of_count()
    }

    /// What the count holds but does not judge: the parked, and those holding a place among the
    /// requests waiting on a move or an open.
    fn out_of_count(&self) -> i64 {
        self.inner.parked.load(Ordering::Relaxed)
            + self.inner.judged.waiting.load(Ordering::Relaxed)
    }

    /// The write gate (`admit_write_inflight`): a request fair shedding spares passes up to the
    /// hard line, any other up to the cap; with no cap, every request passes. Counts the shed.
    pub(super) fn meet_write_gate(&self) -> Result<(), super::WriteRefusal> {
        let cap = self.max_inflight();
        if cap <= 0 {
            return Ok(());
        }
        let verdict = super::park::verdict();
        #[cfg(test)]
        self.note_verdict(verdict);
        let spared = verdict == Some(super::fair_shed::Verdict::Spared);
        let line = if spared {
            i64::try_from(super::fair_shed::inflight_hard_line(cap.unsigned_abs())).unwrap_or(cap)
        } else {
            cap
        };
        let passed = super::park::meet_gate(line).unwrap_or_else(|| self.judged() <= line);
        if passed {
            return Ok(());
        }
        self.inner.shed_total.fetch_add(1, Ordering::Relaxed);
        self.inner.shed_inflight.fetch_add(1, Ordering::Relaxed);
        Err(super::WriteRefusal::Overloaded)
    }

    /// Rigs simulate load without holding tickets (the inflight admission tests push the count past
    /// the cap directly): `n` requests in flight, counted from their arrival as any request the
    /// gate does not classify.
    #[cfg(test)]
    pub(crate) fn add_inflight_for_test(&self, n: i64) {
        self.inner.inflight.fetch_add(n, Ordering::Relaxed);
        self.inner.judged.counted.fetch_add(n, Ordering::Relaxed);
    }

    /// The gate's meetings by verdict (`Judged::verdicts`): blamed, spared, even, none.
    #[cfg(test)]
    pub(crate) fn gate_verdicts(&self) -> [u64; 4] {
        [0, 1, 2, 3].map(|i| self.inner.judged.verdicts[i].load(Ordering::Relaxed))
    }

    #[cfg(test)]
    fn note_verdict(&self, verdict: Option<super::fair_shed::Verdict>) {
        use super::fair_shed::Verdict;
        let at = match verdict {
            Some(Verdict::Blamed) => 0,
            Some(Verdict::Spared) => 1,
            Some(Verdict::Even) => 2,
            None => 3,
        };
        self.inner.judged.verdicts[at].fetch_add(1, Ordering::Relaxed);
    }

    /// The classifier's pins: the served requests the gate met, entered at 0 and counted from
    /// their arrival, and those entered at 0 answered without meeting it or another door.
    #[cfg(test)]
    pub(crate) fn gate_pins(&self) -> ([u64; 2], u64) {
        let judged = &self.inner.judged;
        let met = [0, 1].map(|i| judged.met[i].load(Ordering::Relaxed));
        (met, judged.unmet.load(Ordering::Relaxed))
    }

    /// The judged count and the cap, when a cap is set and the count is over it (`shed_blamed`):
    /// the served request included, as the gate's own check includes it, though the count holds
    /// it only from its gate.
    pub(super) fn judged_over_cap(&self) -> Option<(u64, u64)> {
        let cap = u64::try_from(self.max_inflight()).unwrap_or(0);
        let own = super::park::own_uncounted().map_or(0, i64::from);
        let judged = u64::try_from(self.judged() + own).unwrap_or(0);
        (cap > 0 && judged > cap).then_some((judged, cap))
    }
}

#[cfg(test)]
mod tests;
