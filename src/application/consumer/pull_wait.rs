//! A consumer pull's wait between walks of its lineage (shared cells M1
//! and the cost review's 2.4). A pull with `waitMs` that found nothing to
//! lease parks until something can have made a message deliverable, not
//! for a fixed 50 ms between walks:
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
//! closed; otherwise the pull parks again. A walk that leases nothing
//! leaves the consumer's state as it was, so idle pulls never wake one
//! another.
//!
//! A wait that ends to walk again first takes the pull's coverage
//! reservation again (shared cells H3: released while it waited), waiting,
//! still parked, for room under its project's line and in the instance's
//! read memory until its deadline; with no room by then it answers empty.
//!
//! A pull pays for the walks that leased nothing (public-beta phase B C7):
//! each wait first has the pull's project pay the coverage those walks
//! read from its read quota, and a wait that ends to walk again first
//! waits, still parked, while the project is in read debt; with the debt
//! standing at its deadline it answers empty.
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
    /// Wait, parked, until `consumer` may have a deliverable message in
    /// `lineage` or `deadline` passes, having its project pay the
    /// coverage of the walks that leased nothing first. True: walk the
    /// lineage again; false: the deadline passed (already, or while the
    /// project was in read debt, or with no read memory free) and the pull
    /// answers empty.
    pub(crate) async fn wait(
        &self,
        service: &Arc<ConsumerService>,
        lineage: &[ConsumerSegment],
        consumer: &str,
        deadline: Instant,
    ) -> bool {
        crate::admission::park::pay_coverage();
        if Instant::now() >= deadline {
            return false;
        }
        let watched = watch(service, lineage).await;
        let _parked = crate::admission::park::park();
        let walk = match watched {
            Some(watched) => self.until_moved(&watched, consumer, deadline).await,
            None => poll(deadline).await,
        };
        walk && crate::admission::park::out_of_read_debt(deadline).await
            && crate::admission::park::resume(deadline).await
    }

    /// Wait on `watched` until a view moved, a lease expired or `deadline`
    /// passed: true, walk again.
    async fn until_moved(&self, watched: &[Watched], consumer: &str, deadline: Instant) -> bool {
        loop {
            // Registered before the view is read, so no commit after the
            // read goes unnoticed.
            let notified: Vec<_> = watched
                .iter()
                .map(|w| Box::pin(w.handle.notify.notified()))
                .collect();
            let Some((views, lease)) = observe(watched, consumer) else {
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

/// A multiplexed wait's watch on one consumer's lineage (layout 6A package
/// 2): the same segments, views and lease deadlines a waiting pull watches,
/// held across the request's walks, plus whether the consumer is drained
/// there. Drained is read from the segments' queue state, so a group whose
/// consumer has nothing to deliver or lease needs no walk at all: no
/// configuration read and no Receive through the committer.
pub(crate) struct LineageWatch {
    watched: Vec<Watched>,
    /// Each segment's sealed end; `None` for a live one.
    ends: Vec<Option<u64>>,
}

/// The views a walk was taken against (`LineageWatch::seen`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen(Vec<View>);

impl LineageWatch {
    /// The lineage of `desc`, when this instance serves every segment.
    pub(crate) async fn of(
        service: &Arc<ConsumerService>,
        desc: &crate::registry::StreamDesc,
    ) -> Option<Self> {
        let lineage = super::consumer_segments(desc);
        let watched = watch(service, &lineage).await?;
        Some(Self {
            watched,
            ends: lineage.iter().map(|segment| segment.3).collect(),
        })
    }

    /// Whether `consumer` has nothing left to lease on any segment: every
    /// segment's queue state is loaded and the consumer's cursor there has
    /// reached the segment's durable tail (or sealed end). A settle advances
    /// the cursor over every settled record at it (`settle`), so a cursor
    /// short of the tail names a record leased or still to lease. A
    /// consumer the loaded state does not hold has no rows there: cursor 0,
    /// as loading its rows would make it. A closed engine, a poisoned or
    /// unloaded state is not drained: the walk decides.
    pub(crate) fn drained(&self, consumer: &str) -> bool {
        self.watched.iter().zip(&self.ends).all(|(w, end)| {
            if w.engine.is_closed() {
                return false;
            }
            let Ok(state) = w.handle.state.lock() else {
                return false;
            };
            let tail = end.unwrap_or(state.durable.next);
            let cursor = state
                .queue
                .consumers
                .get(consumer)
                .map_or(0, |cs| cs.cursor);
            state.queue.loaded && cursor >= tail
        })
    }

    /// The payload bytes before every segment's durable tail: what a walk
    /// woken later reads at most beyond, with its framing.
    pub(crate) fn payload(&self) -> u64 {
        self.watched
            .iter()
            .filter_map(|w| w.handle.state.lock().ok().map(|s| s.durable.logical))
            .sum()
    }

    /// The views a walk taken now is taken against, and the earliest
    /// deadline among the consumer's live leases; `None` when an engine
    /// closed or a state is poisoned (walk again).
    pub(crate) fn seen(&self, consumer: &str) -> Option<(Seen, Option<Instant>)> {
        observe(&self.watched, consumer).map(|(views, lease)| (Seen(views), lease))
    }

    /// Every segment's commit notification, registered now.
    pub(crate) fn notified(&self) -> Vec<tokio::sync::futures::Notified<'_>> {
        self.watched
            .iter()
            .map(|w| w.handle.notify.notified())
            .collect()
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
        if lease.holds_key_at(now_ms) {
            next = next.into_iter().chain([lease.deadline_ms]).min();
        }
    }
    (digest.finish(), next)
}
