//! The stream-count seed, once per project (the landing-4 review's L2). A
//! project with a `max_streams` quota counts its streams on its first create
//! since this instance booted, a count charged to no project once it
//! completes (edge change #143, the owner's round-6 call 2). Concurrent
//! first creates share that count: the first counts under the project's
//! seed lock and seeds it, and the others wait for it and find the count
//! seeded, so the cell counts once per project per boot, not once per create
//! that arrives before the count is seeded. A count that fails (charged to
//! its create) or is dropped seeds nothing, and the next waiting create
//! counts.
//!
//! The first count lists the catalog's keys alone, tombstones included: an
//! upper bound, read without a descriptor (S2 of the forward-wedge ruling of
//! 2026-11-19; a walk of every descriptor outlasted every caller at 33,000
//! streams). A bound under the cap seeds the count. The exact walk runs only
//! where the bound cannot prove room: at the seed when the bound is at or
//! over the cap, and before a project seeded by its bound would be refused
//! `stream_limit`. That recount runs with this instance's reservations
//! quiesced (each holds the project's gate until it decides), so a project
//! is never refused on its bound and never admits what its exact count
//! refuses.

use super::{ProjectAdmission, QuotaRefusal, QuotaRegistry, StreamReservation};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

/// How a project's stream count was seeded, and the lock its count takes.
#[derive(Default)]
pub(super) struct StreamSeed {
    /// Held while one create counts the catalog for the seed or a recount.
    walk: Mutex<()>,
    /// The count was seeded by the catalog's key count, an upper bound.
    bounded: AtomicBool,
    /// Each reservation of a bound-seeded project holds this to share until
    /// it decides; the exact recount holds it alone.
    gate: Arc<RwLock<()>>,
}

/// A stream slot reserved under a seeded count (`StreamReservation`), and
/// the gate a bound-seeded project's reservation holds until it decides.
pub(crate) struct SeededReservation {
    reservation: StreamReservation,
    _gate: Option<OwnedRwLockReadGuard<()>>,
}

impl SeededReservation {
    /// Keep the slot: the caller created a new stream.
    pub(crate) fn commit(self) {
        self.reservation.commit();
    }
}

impl QuotaRegistry {
    /// Reserve one stream slot under `max_streams`, the count seeded first
    /// when this project has none since boot: from `bound`, the catalog's
    /// key count, when it is under the cap, else from `exact`, the walk.
    /// A refusal of a project seeded by its bound walks `exact` and reserves
    /// again on that count. `bound` and `exact` are each polled at most
    /// once, and not at all when the count is seeded already. The outer
    /// error is a count's; the inner, the reservation's refusal.
    pub(crate) async fn reserve_seeded_stream<E>(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bound: impl Future<Output = Result<u64, E>>,
        exact: impl Future<Output = Result<u64, E>>,
    ) -> Result<Result<Option<SeededReservation>, QuotaRefusal>, E> {
        let Some(admission) = self.tracked(project) else {
            return Ok(Ok(None));
        };
        let mut exact = Box::pin(exact);
        if self.needs_stream_seed(project) {
            admission
                .seed_from_bound(quotas.max_streams, bound, exact.as_mut())
                .await?;
        }
        let seed = &admission.seed;
        match self.reserve_gated(project, quotas, &admission).await {
            Err(QuotaRefusal::StreamLimit) if seed.bounded.load(Ordering::Acquire) => {}
            reserved => return Ok(reserved),
        }
        {
            let _walking = seed.walk.lock().await;
            if seed.bounded.load(Ordering::Acquire) {
                let _quiet = seed.gate.write().await;
                let count = exact.await?;
                admission.recount_streams(count);
                seed.bounded.store(false, Ordering::Release);
            }
        }
        Ok(self.reserve_gated(project, quotas, &admission).await)
    }

    /// One reservation on the seeded count; a bound-seeded project's holds
    /// its gate to share until it decides.
    async fn reserve_gated(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        admission: &ProjectAdmission,
    ) -> Result<Option<SeededReservation>, QuotaRefusal> {
        let seed = &admission.seed;
        let gate = if seed.bounded.load(Ordering::Acquire) {
            Some(seed.gate.clone().read_owned().await)
        } else {
            None
        };
        let reserved = self.reserve_stream(project, quotas, None)?;
        Ok(reserved.map(|reservation| SeededReservation {
            reservation,
            _gate: gate,
        }))
    }
}

impl ProjectAdmission {
    /// Seed the count under the project's seed lock unless another create
    /// seeded it while this one waited: from `bound` when it is under `cap`,
    /// else from `exact`, which a count seeded from `bound` never polls
    /// again. A poisoned count is not seeded.
    async fn seed_from_bound<E>(
        &self,
        cap: u64,
        bound: impl Future<Output = Result<u64, E>>,
        exact: std::pin::Pin<&mut impl Future<Output = Result<u64, E>>>,
    ) -> Result<(), E> {
        let _walking = self.seed.walk.lock().await;
        if !self.needs_seed() {
            return Ok(());
        }
        let count = bound.await?;
        if count < cap {
            self.seed.bounded.store(true, Ordering::Release);
            self.seed_streams(count);
            return Ok(());
        }
        let count = exact.await?;
        self.seed_streams(count);
        Ok(())
    }

    /// The count is unseeded and not poisoned.
    fn needs_seed(&self) -> bool {
        self.streams.lock().is_ok_and(|st| !st.seeded)
    }

    /// Seed the count unless a reservation seeded it first. A poisoned
    /// count is left alone: `reserve_stream` refuses it as before.
    fn seed_streams(&self, count: u64) {
        if let Ok(mut st) = self.streams.lock()
            && !st.seeded
        {
            st.seeded = true;
            st.count = count;
        }
    }

    /// Replace a bound-seeded count with the exact one, walked while no
    /// reservation of this project was undecided. A poisoned count is left
    /// alone, as `seed_streams` leaves it.
    fn recount_streams(&self, count: u64) {
        if let Ok(mut st) = self.streams.lock() {
            st.count = count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{QuotaRefusal, QuotaRegistry};
    use super::SeededReservation;
    use crate::project_policy::ProjectQuotas;
    use crate::tenant::ProjectId;

    type Count = Result<u64, &'static str>;
    type Reserved = Result<Result<Option<SeededReservation>, QuotaRefusal>, &'static str>;

    fn registry(max_streams: u64) -> (QuotaRegistry, ProjectId, ProjectQuotas) {
        let r = QuotaRegistry::default();
        let project = ProjectId::new("seeded").unwrap();
        let quotas = ProjectQuotas {
            max_streams,
            ..ProjectQuotas::default()
        };
        assert!(
            r.admit(&project, &quotas, 1_000).is_ok(),
            "the project is tracked"
        );
        (r, project, quotas)
    }

    /// One create's reservation, with counts that answer `bound` and `exact`.
    async fn reserve(
        r: &QuotaRegistry,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bound: Count,
        exact: Count,
    ) -> Reserved {
        let (bound, exact) = (std::future::ready(bound), std::future::ready(exact));
        r.reserve_seeded_stream(project, quotas, bound, exact).await
    }

    /// A failed count seeds nothing, the next count seeds what it returns, a
    /// later create finds it seeded and polls neither count, and the
    /// reservations count from the seed: at `max_streams` 6 an exact five
    /// (its bound, 7, at the cap) takes one more and refuses the next.
    #[tokio::test(crate = "crate::test_rt")]
    async fn one_completed_count_seeds_the_count_for_every_later_create() {
        let (r, project, quotas) = registry(6);
        let failed = reserve(&r, &project, &quotas, Err("list"), Ok(0)).await;
        assert!(matches!(failed, Err("list")), "a failed count");
        assert!(
            r.needs_stream_seed(&project),
            "a failed count seeds nothing"
        );
        match reserve(&r, &project, &quotas, Ok(7), Ok(5)).await {
            Ok(Ok(Some(sixth))) => sixth.commit(),
            _ => panic!("the sixth stream of an exact five"),
        }
        let seventh = reserve(&r, &project, &quotas, Err("listed"), Err("walked")).await;
        assert!(
            matches!(seventh, Ok(Err(QuotaRefusal::StreamLimit))),
            "the seventh"
        );
    }

    /// A bound under the cap seeds the count, and a refusal on it walks the
    /// exact count first: at `max_streams` 7 a bound of 6 admits a seventh
    /// by its own count; the next is refused on the bound, so the exact
    /// count, 5, is walked, which admits two and refuses the third without
    /// walking again.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_refusal_on_the_bound_recounts_the_exact_count_once() {
        let (r, project, quotas) = registry(7);
        let admitted = [
            (Ok(6), Err("not walked")),
            (Err("not listed"), Ok(5)),
            (Err("not listed"), Err("walked again")),
        ];
        for (n, (bound, exact)) in admitted.into_iter().enumerate() {
            match reserve(&r, &project, &quotas, bound, exact).await {
                Ok(Ok(Some(slot))) => slot.commit(),
                _ => panic!("create {n} is admitted"),
            }
        }
        let refused = reserve(&r, &project, &quotas, Err("listed"), Err("walked")).await;
        assert!(
            matches!(refused, Ok(Err(QuotaRefusal::StreamLimit))),
            "the third"
        );
    }
}
