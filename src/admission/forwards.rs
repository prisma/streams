//! The forwards a non-owner holds while their owners answer (edge change
//! #235, `http::forward`). A forward is an active request at the non-owner
//! for as long as it waits, so an owner that never answers would hold every
//! request forwarded to it, up to the survival bound, and the non-owner's
//! own streams would be shed beside them. Forwards waiting at once are
//! capped at a quarter of the in-flight cap, so at most that share of the
//! non-owner's active requests waits on other instances; past it a forward
//! is not sent and the client gets the non-owner's `409` to replay.

use std::sync::atomic::Ordering;

use super::AdmissionController;

/// The forwards that may wait at once when no in-flight cap is set: the
/// share of the default cap (512).
const UNCAPPED: i64 = 128;

/// RAII place of one forward waiting for its owner's answer.
pub(crate) struct ForwardTicket {
    ctl: AdmissionController,
}

impl Drop for ForwardTicket {
    fn drop(&mut self) {
        self.ctl.inner.forwards.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AdmissionController {
    /// A place for one more forward waiting for its owner's answer: `None`
    /// when a quarter of the in-flight cap (at least one; 128 with no cap)
    /// already wait.
    pub(crate) fn enter_forward(&self) -> Option<ForwardTicket> {
        let cap = match self.max_inflight() {
            cap if cap > 0 => (cap / 4).max(1),
            _ => UNCAPPED,
        };
        let waiting = &self.inner.forwards;
        if waiting.fetch_add(1, Ordering::Relaxed) >= cap {
            waiting.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(ForwardTicket { ctl: self.clone() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// How many forwards one controller lets wait at once.
    fn places(ctl: &AdmissionController) -> Vec<ForwardTicket> {
        std::iter::from_fn(|| ctl.enter_forward())
            .take(1_000)
            .collect()
    }

    /// A quarter of the in-flight cap, at least one, and 128 with no cap;
    /// a place is free again once its forward has its answer.
    #[test]
    fn forwards_wait_within_a_quarter_of_the_in_flight_cap() {
        let counts = [512, 8, 7, 3, 1, 0].map(|cap| places(&controller(cap)).len());
        assert_eq!(counts, [128, 2, 1, 1, 1, 128]);
        let ctl = controller(8);
        let mut held = places(&ctl);
        assert!(ctl.enter_forward().is_none());
        held.pop();
        assert!(ctl.enter_forward().is_some(), "a returned place is free");
        assert_eq!(ctl.inner.forwards.load(Ordering::Relaxed), 1);
    }
}
