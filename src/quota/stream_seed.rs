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
//! streams), which seeds the count. The exact walk runs only where the bound
//! cannot prove room: before a project seeded by its bound would be refused
//! `stream_limit`, which is its first create when the bound is at or over
//! the cap. That recount runs with this instance's reservations
//! quiesced (each holds the project's gate until it decides), so a project
//! is never refused on its bound and never admits what its exact count
//! refuses.
//!
//! A count belongs to the project's admission entry, not to the create that
//! starts it (S1 of the same ruling). The create drives its pass in its own
//! request, so a pass that fails is charged to it; a create that leaves
//! before its pass ends hands the rest, with the pass's locks, to the
//! runtime's request work (`SeedCounts::orphan`), where it runs to its end
//! charged to no one. A pass the runner refuses seeds nothing and releases
//! its locks, as a dropped walk did before.

use super::{ProjectAdmission, QuotaRefusal, QuotaRegistry, StreamReservation};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// How a project's stream count was seeded, and the locks its counts take.
#[derive(Default)]
pub(super) struct StreamSeed {
    /// Held by one pass at a time: the seed's count or a recount.
    walk: Arc<Mutex<()>>,
    /// The count was seeded by the catalog's key count, an upper bound.
    bounded: AtomicBool,
    /// Each reservation of a bound-seeded project holds this to share until
    /// it decides; the exact recount holds it alone.
    gate: Arc<RwLock<()>>,
    /// Passes handed off by a create that left before they ended.
    orphaned: AtomicU64,
    /// Of those, the ones the runner refused, which seeded nothing.
    refused: AtomicU64,
}

/// What a create gives the seed: the counts, as futures of their own, and
/// the runner a pass it leaves unfinished goes to.
pub(crate) trait SeedCounts<E>: Send + Sync + 'static {
    /// The catalog's key count, an upper bound.
    fn bound(&self) -> BoxFuture<'static, Result<u64, E>>;
    /// The exact count, a walk of the catalog.
    fn exact(&self) -> BoxFuture<'static, Result<u64, E>>;
    /// Run `rest`, the project's `pass`-th unfinished pass, to its end;
    /// false when the runner refused it.
    fn orphan(&self, pass: u64, rest: BoxFuture<'static, ()>) -> bool;
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
    /// when this project has none since boot, from the catalog's key count.
    /// A refusal of a project seeded by that bound recounts exactly and
    /// reserves again on that count. The outer error is a count's; the
    /// inner, the reservation's refusal.
    pub(crate) async fn reserve_seeded_stream<E: Send + 'static>(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        counts: Arc<dyn SeedCounts<E>>,
    ) -> Result<Result<Option<SeededReservation>, QuotaRefusal>, E> {
        let Some(admission) = self.tracked(project) else {
            return Ok(Ok(None));
        };
        if self.needs_stream_seed(project) {
            let walking = admission.seed.walk.clone().lock_owned().await;
            if admission.needs_seed() {
                let pass = admission.clone().seed_pass(counts.clone(), walking);
                Driven::new(&admission, &counts, pass).await?;
            }
        }
        let seed = &admission.seed;
        match self.reserve_gated(project, quotas, &admission).await {
            Err(QuotaRefusal::StreamLimit) => {}
            reserved => return Ok(reserved),
        }
        let walking = seed.walk.clone().lock_owned().await;
        if seed.bounded.load(Ordering::Acquire) {
            let quiet = seed.gate.clone().write_owned().await;
            let pass = admission
                .clone()
                .recount_pass(counts.clone(), walking, quiet);
            Driven::new(&admission, &counts, pass).await?;
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

    /// The passes a create handed off for `project`, and of those the ones
    /// the runner refused.
    #[cfg(test)]
    pub(crate) fn orphaned_seed_passes(&self, project: &ProjectId) -> (u64, u64) {
        self.tracked(project).map_or((0, 0), |a| {
            let seed = &a.seed;
            (
                seed.orphaned.load(Ordering::Relaxed),
                seed.refused.load(Ordering::Relaxed),
            )
        })
    }
}

impl ProjectAdmission {
    /// The seed's pass, under its walk lock: the count seeded from the
    /// catalog's key count, an upper bound. A bound at or over the cap proves
    /// no room, so the first reservation it refuses walks the exact count
    /// (`recount_pass`) before it answers.
    async fn seed_pass<E: Send + 'static>(
        self: Arc<Self>,
        counts: Arc<dyn SeedCounts<E>>,
        _walking: OwnedMutexGuard<()>,
    ) -> Result<(), E> {
        let count = counts.bound().await?;
        self.seed.bounded.store(true, Ordering::Release);
        self.seed_streams(count);
        Ok(())
    }

    /// A bound-seeded count's exact recount, under its walk lock and with
    /// its reservations quiesced (`quiet`, the gate held alone).
    async fn recount_pass<E: Send + 'static>(
        self: Arc<Self>,
        counts: Arc<dyn SeedCounts<E>>,
        _walking: OwnedMutexGuard<()>,
        _quiet: OwnedRwLockWriteGuard<()>,
    ) -> Result<(), E> {
        let count = counts.exact().await?;
        self.recount_streams(count);
        self.seed.bounded.store(false, Ordering::Release);
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

/// A pass its create drives in its own request. Dropped before the pass
/// ends, it hands the rest to the counts' runner.
struct Driven<E: Send + 'static> {
    pass: Option<BoxFuture<'static, Result<(), E>>>,
    admission: Arc<ProjectAdmission>,
    counts: Arc<dyn SeedCounts<E>>,
}

impl<E: Send + 'static> Driven<E> {
    fn new(
        admission: &Arc<ProjectAdmission>,
        counts: &Arc<dyn SeedCounts<E>>,
        pass: impl Future<Output = Result<(), E>> + Send + 'static,
    ) -> Self {
        Self {
            pass: Some(pass.boxed()),
            admission: admission.clone(),
            counts: counts.clone(),
        }
    }
}

impl<E: Send + 'static> Future for Driven<E> {
    type Output = Result<(), E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(pass) = self.pass.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let ended = pass.as_mut().poll(cx);
        if ended.is_ready() {
            self.pass = None;
        }
        ended
    }
}

impl<E: Send + 'static> Drop for Driven<E> {
    fn drop(&mut self) {
        let Some(pass) = self.pass.take() else {
            return;
        };
        let seed = &self.admission.seed;
        let number = seed.orphaned.fetch_add(1, Ordering::Relaxed);
        let rest = async move {
            if pass.await.is_err() {
                tracing::debug!(
                    "a stream-count seed pass its create left failed; it seeds nothing"
                );
            }
        };
        if !self.counts.orphan(number, rest.boxed()) {
            seed.refused.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{QuotaRefusal, QuotaRegistry};
    use super::SeedCounts;
    use crate::project_policy::ProjectQuotas;
    use crate::tenant::ProjectId;
    use futures_util::FutureExt;
    use futures_util::future::BoxFuture;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    type Count = Result<u64, &'static str>;

    /// Counts that answer what they were given, and a runner that runs
    /// nothing.
    struct Fixed(Count, Count);

    impl SeedCounts<&'static str> for Fixed {
        fn bound(&self) -> BoxFuture<'static, Count> {
            std::future::ready(self.0).boxed()
        }
        fn exact(&self) -> BoxFuture<'static, Count> {
            std::future::ready(self.1).boxed()
        }
        fn orphan(&self, _: u64, _: BoxFuture<'static, ()>) -> bool {
            false
        }
    }

    fn fixed(bound: Count, exact: Count) -> Arc<dyn SeedCounts<&'static str>> {
        Arc::new(Fixed(bound, exact))
    }

    /// A key count that answers once its sender sends, and a runner that
    /// keeps what it is handed (when `takes`) without running it.
    struct Held {
        bound: Mutex<Option<oneshot::Receiver<u64>>>,
        takes: bool,
        handed: Mutex<Vec<(u64, BoxFuture<'static, ()>)>>,
    }

    impl SeedCounts<&'static str> for Held {
        fn bound(&self) -> BoxFuture<'static, Count> {
            let sent = self.bound.lock().unwrap().take();
            async move { sent.ok_or("counted twice")?.await.map_err(|_| "unsent") }.boxed()
        }
        fn exact(&self) -> BoxFuture<'static, Count> {
            std::future::ready(Err("walked")).boxed()
        }
        fn orphan(&self, pass: u64, rest: BoxFuture<'static, ()>) -> bool {
            self.handed.lock().unwrap().push((pass, rest));
            self.takes
        }
    }

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

    /// A failed count seeds nothing, the next count seeds what it returns, a
    /// later create finds it seeded and runs neither count, and the
    /// reservations count from the seed: at `max_streams` 6 an exact five
    /// (its bound, 7, at the cap) takes one more and refuses the next.
    #[tokio::test(crate = "crate::test_rt")]
    async fn one_completed_count_seeds_the_count_for_every_later_create() {
        let (r, project, quotas) = registry(6);
        let failed = r
            .reserve_seeded_stream(&project, &quotas, fixed(Err("list"), Ok(0)))
            .await;
        assert!(matches!(failed, Err("list")), "a failed count");
        assert!(
            r.needs_stream_seed(&project),
            "a failed count seeds nothing"
        );
        let sixth = r
            .reserve_seeded_stream(&project, &quotas, fixed(Ok(7), Ok(5)))
            .await;
        match sixth {
            Ok(Ok(Some(sixth))) => sixth.commit(),
            _ => panic!("the sixth stream of an exact five"),
        }
        let again = fixed(Err("listed again"), Err("walked again"));
        let seventh = r.reserve_seeded_stream(&project, &quotas, again).await;
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
            let counts = fixed(bound, exact);
            match r.reserve_seeded_stream(&project, &quotas, counts).await {
                Ok(Ok(Some(slot))) => slot.commit(),
                _ => panic!("create {n} is admitted"),
            }
        }
        let again = fixed(Err("not listed"), Err("walked again"));
        let refused = r.reserve_seeded_stream(&project, &quotas, again).await;
        assert!(
            matches!(refused, Ok(Err(QuotaRefusal::StreamLimit))),
            "the third"
        );
    }

    /// A create that leaves while its key count is parked hands the rest of
    /// its pass, once, to the runner; the rest seeds the count with no create
    /// waiting, and the next create counts nothing (S1 of the forward-wedge
    /// ruling of 2026-11-19).
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_pass_its_create_left_seeds_the_count_from_the_runner() {
        let (r, project, quotas) = registry(7);
        let (send, parked) = oneshot::channel();
        let held = Arc::new(Held {
            bound: Mutex::new(Some(parked)),
            takes: true,
            handed: Mutex::new(Vec::new()),
        });
        let create = r.reserve_seeded_stream(&project, &quotas, held.clone());
        let left = tokio::time::timeout(std::time::Duration::from_millis(50), create).await;
        assert!(left.is_err(), "the create left while its count was parked");
        let mut handed = std::mem::take(&mut *held.handed.lock().unwrap());
        assert_eq!(
            (handed.len(), r.needs_stream_seed(&project)),
            (1, true),
            "the pass handed off, the count not yet seeded"
        );
        let (pass, rest) = handed.remove(0);
        send.send(3).unwrap();
        rest.await;
        assert_eq!(
            (
                pass,
                r.needs_stream_seed(&project),
                r.orphaned_seed_passes(&project)
            ),
            (0, false, (1, 0)),
            "the handed-off pass seeded the count"
        );
        let next = fixed(Err("listed again"), Err("walked again"));
        let next = r.reserve_seeded_stream(&project, &quotas, next).await;
        assert!(matches!(next, Ok(Ok(Some(_)))), "the next create on 3 of 7");
    }

    /// A pass the runner refuses seeds nothing and releases the seed's lock,
    /// so the next create counts again.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_pass_the_runner_refuses_seeds_nothing_and_frees_the_count() {
        let (r, project, quotas) = registry(7);
        let (_send, parked) = oneshot::channel();
        let held = Arc::new(Held {
            bound: Mutex::new(Some(parked)),
            takes: false,
            handed: Mutex::new(Vec::new()),
        });
        let create = r.reserve_seeded_stream(&project, &quotas, held.clone());
        let left = tokio::time::timeout(std::time::Duration::from_millis(50), create).await;
        assert!(left.is_err(), "the create left while its count was parked");
        drop(std::mem::take(&mut *held.handed.lock().unwrap()));
        let next = r
            .reserve_seeded_stream(&project, &quotas, fixed(Ok(2), Err("not walked")))
            .await;
        assert_eq!(
            (
                matches!(next, Ok(Ok(Some(_)))),
                r.orphaned_seed_passes(&project)
            ),
            (true, (1, 1)),
            "the next create seeds from its own count; one pass refused"
        );
    }
}
