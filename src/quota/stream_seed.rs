//! The stream-count seed walk, once per project (the landing-4 review's L2).
//! A project with a `max_streams` quota counts its streams from its catalog
//! on its first create since this instance booted, a walk charged to no
//! project once it completes (edge change #143, the owner's round-6 call
//! 2). Concurrent first creates share that walk: the first walks under the
//! project's seed lock and seeds the count, and the others wait for it and
//! find the count seeded, so the cell walks once per project per boot, not
//! once per create that arrives before the count is seeded. A walk that
//! fails (charged to its create) or is dropped seeds nothing, and the next
//! waiting create walks.

use super::{ProjectAdmission, QuotaRegistry};
use crate::tenant::ProjectId;

impl QuotaRegistry {
    /// The count `walk` returns when it ran (the caller passes it on to
    /// `reserve_stream`), or `None` when the project is untracked or its
    /// count is seeded already, before or while this create waited for
    /// another's walk; `walk` is then never polled.
    pub(crate) async fn seed_streams_once<E>(
        &self,
        project: &ProjectId,
        walk: impl Future<Output = Result<u64, E>>,
    ) -> Result<Option<u64>, E> {
        if !self.needs_stream_seed(project) {
            return Ok(None);
        }
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        let _walking = admission.seed_walk.lock().await;
        if !self.needs_stream_seed(project) {
            return Ok(None);
        }
        let count = walk.await?;
        admission.seed_streams(count);
        Ok(Some(count))
    }
}

impl ProjectAdmission {
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
}

#[cfg(test)]
mod tests {
    use super::super::{QuotaRefusal, QuotaRegistry};
    use crate::project_policy::ProjectQuotas;
    use crate::tenant::ProjectId;

    /// A failed walk seeds nothing, the next walk seeds the count it
    /// returns, a later create finds it seeded and never polls its walk,
    /// and the reservations count from the seed: at `max_streams` 6 a
    /// seeded five takes one more and refuses the next.
    #[tokio::test]
    async fn one_completed_walk_seeds_the_count_for_every_later_create() {
        let r = QuotaRegistry::default();
        let project = ProjectId::new("seeded").unwrap();
        let quotas = ProjectQuotas {
            max_streams: 6,
            ..ProjectQuotas::default()
        };
        let admitted = r.admit(&project, &quotas, 1_000);
        assert!(admitted.is_ok(), "the project is tracked");
        let failed = r.seed_streams_once(&project, async { Err("page") }).await;
        assert_eq!(failed, Err("page"), "a failed walk");
        assert!(r.needs_stream_seed(&project), "a failed walk seeds nothing");
        let seeded = r
            .seed_streams_once(&project, async { Ok::<_, &str>(5) })
            .await;
        assert_eq!(seeded, Ok(Some(5)), "the walk that completes");
        assert!(!r.needs_stream_seed(&project), "its count is seeded");
        let later = r
            .seed_streams_once(&project, async { Err("walked again") })
            .await;
        assert_eq!(later, Ok(None), "a later create walks nothing");
        match r.reserve_stream(&project, &quotas, None) {
            Ok(Some(sixth)) => sixth.commit(),
            other => panic!("the sixth stream of a seeded five: {:?}", other.is_ok()),
        }
        assert!(matches!(
            r.reserve_stream(&project, &quotas, None),
            Err(QuotaRefusal::StreamLimit)
        ));
    }
}
