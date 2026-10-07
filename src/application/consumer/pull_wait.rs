//! A consumer pull's wait between walks of its lineage (shared cells, the
//! cost review's 2.4). A pull with `waitMs` that found nothing to lease
//! waits until something can have made a message deliverable, not for a
//! fixed 50 ms between walks:
//!
//! - a record became durable in a lineage segment: the segment handle's
//!   commit notification;
//! - the consumer's own queue state moved (a settle, a nack, a lease taken
//!   by another pull of the consumer): published by a commit, which also
//!   notifies;
//! - one of the consumer's leases expires: its deadline.
//!
//! Every commit that touches the stream notifies its handle, other
//! consumers' receives included, so a wake walks again only when a
//! segment's durable tail or this consumer's state moved, or its engine
//! closed; otherwise the pull waits again. A walk that leases nothing
//! leaves the consumer's state as it was, so idle pulls never wake one
//! another.
//!
//! The first wait of each pull always walks once more: its view is taken
//! after the first walk, so a record committed during that walk could
//! otherwise go unseen. From then on each wait compares the view the
//! previous wait recorded as it ended, before the walk that followed. A pull
//! whose lineage holds a segment this instance does not serve, or whose
//! engine closed, has no local notification to wait on and walks every
//! 50 ms, as every pull did.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::{ConsumerSegment, ConsumerService};
use crate::shard::{ShardEngine, StreamHandle};

/// The walk interval of a pull that cannot wait on local notifications, as
/// every waiting pull walked before.
const FOREIGN_POLL: Duration = Duration::from_millis(50);

/// One pull's waits: the view of its lineage each wait last saw.
#[derive(Default)]
pub(crate) struct PullPark {
    seen: Mutex<Option<Vec<View>>>,
}

/// What a segment shows a waiting pull: its durable tail and a digest of
/// the consumer's queue state there (generation, cursor, settled marks and
/// every lease with its deadline and generation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct View {
    durable: u64,
    consumer: u64,
}

/// A lineage segment this instance serves, held for the length of a wait.
struct Watched {
    engine: Arc<ShardEngine>,
    handle: Arc<StreamHandle>,
}

impl PullPark {
    /// Wait until `consumer` may have a deliverable message in
    /// `lineage` or `deadline` passes. True: walk the lineage again;
    /// false: the deadline had already passed and the pull answers empty.
    pub(crate) async fn wait(
        &self,
        service: &Arc<ConsumerService>,
        lineage: &[ConsumerSegment],
        consumer: &str,
        deadline: Instant,
    ) -> bool {
        if Instant::now() >= deadline {
            return false;
        }
        let Some(watched) = watch(service, lineage).await else {
            return poll(deadline).await;
        };
        loop {
            // Registered before the view is read, so no commit after the
            // read goes unnoticed.
            let notified: Vec<_> = watched
                .iter()
                .map(|w| Box::pin(w.handle.notify.notified()))
                .collect();
            let Some((views, lease)) = observe(&watched, consumer) else {
                return poll(deadline).await;
            };
            if self.moved(views) {
                return true;
            }
            let wake = lease.map_or(deadline, |at| at.min(deadline));
            let woken = futures_util::future::select_all(notified);
            if tokio::time::timeout_at(wake, woken).await.is_err() {
                return true;
            }
        }
    }

    /// Record `views` as seen: whether they differ from the views the
    /// previous wait saw (always, on a pull's first wait).
    fn moved(&self, views: Vec<View>) -> bool {
        let Ok(mut seen) = self.seen.lock() else {
            return true;
        };
        let moved = seen.as_ref() != Some(&views);
        *seen = Some(views);
        moved
    }
}

/// Walk again after one poll interval (or at the deadline): the wait of a
/// pull with no local notification to wait on, or whose engine closed.
async fn poll(deadline: Instant) -> bool {
    tokio::time::sleep_until((Instant::now() + FOREIGN_POLL).min(deadline)).await;
    true
}

/// The lineage's segments, when this instance serves every one of them.
async fn watch(
    service: &Arc<ConsumerService>,
    lineage: &[ConsumerSegment],
) -> Option<Vec<Watched>> {
    let mut watched = Vec::with_capacity(lineage.len());
    for &(_, identity, route, _) in lineage {
        let engine = service.engine_for(&route).await.ok()?;
        let handle = engine.stream_handle(identity).await.ok()?;
        watched.push(Watched { engine, handle });
    }
    (!watched.is_empty()).then_some(watched)
}

/// Each segment's view and the earliest future deadline among the
/// consumer's leases. `None` when an engine closed or a state is poisoned:
/// the walk must run again to surface it.
fn observe(watched: &[Watched], consumer: &str) -> Option<(Vec<View>, Option<Instant>)> {
    let now_ms = crate::shard::now_ms();
    let mut views = Vec::with_capacity(watched.len());
    let mut next_lease: Option<i64> = None;
    for w in watched {
        if w.engine.is_closed() {
            return None;
        }
        let state = w.handle.state.lock().ok()?;
        let (digest, lease) = state
            .queue
            .consumers
            .get(consumer)
            .map_or((0, None), |cs| consumer_digest(cs, now_ms));
        next_lease = next_lease.into_iter().chain(lease).min();
        views.push(View {
            durable: state.durable.next,
            consumer: digest,
        });
    }
    let lease = next_lease
        .map(|at| Instant::now() + Duration::from_millis(u64::try_from(at - now_ms).unwrap_or(0)));
    Some((views, lease))
}

/// A digest of the consumer's queue state in one segment, and the earliest
/// deadline among its leases still in flight at `now_ms`.
fn consumer_digest(cs: &crate::queue::ConsumerState, now_ms: i64) -> (u64, Option<i64>) {
    let mut digest = DefaultHasher::new();
    (cs.cgen, cs.cursor, cs.acked.len()).hash(&mut digest);
    let mut next = None;
    for (offset, lease) in &cs.leases {
        (offset, lease.deadline_ms, lease.lease_gen).hash(&mut digest);
        if lease.deadline_ms > now_ms {
            next = next.into_iter().chain([lease.deadline_ms]).min();
        }
    }
    (digest.finish(), next)
}
