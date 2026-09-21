//! The feed's transition retry settles a closed tail; it never reads.
use super::{FakeSource, feed_with, outcome_name};
use crate::sse::feed::{
    DriveOutcome, FeedMemoryBudget, FeedSourceRead, LiveFeed, SourceCutoff, Take,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn budget() -> Arc<FeedMemoryBudget> {
    Arc::new(FeedMemoryBudget::new_for_test(1 << 20))
}

/// Seal the feed's source at `sealed_at` and stage a live successor
/// that already holds `ahead` durable records beyond it.
fn stage_successor(src: &FakeSource, sealed_at: u64, ahead: u64) {
    let next = FakeSource::new(sealed_at + ahead, 8);
    *next.sig_override.lock().unwrap() = Some(vec![(0, 0, Some(sealed_at)), (1, sealed_at, None)]);
    src.closed.store(true, Ordering::Relaxed);
    *src.next_result.lock().unwrap() = Some(Arc::new(next) as Arc<dyn FeedSourceRead>);
}

/// The subscriber's OWN drive must hand it exactly the offsets from
/// `from` up to `to`.
async fn expect_solo(feed: &LiveFeed, from: u64, to: u64) {
    match feed.drive_once().await {
        Some(DriveOutcome::Solo { records, scan_to }) => {
            let offsets: Vec<u64> = records.iter().map(|r| r.offset).collect();
            assert_eq!(offsets, (from..to).collect::<Vec<u64>>(), "solo offsets");
            assert_eq!(scan_to, to);
        }
        other => panic!("expected Solo from {from}, got {}", outcome_name(&other)),
    }
}

/// Scenario A (red): a singleton parked AT the head across a split
/// must not be lag-cut because the retry task read its records.
#[tokio::test(start_paused = true)]
async fn retry_install_leaves_the_successor_read_to_the_singleton() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 1 << 20, &budget);
    let (cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    assert_eq!(cursor, feed.head(), "the singleton is parked AT the head");
    stage_successor(&src, cursor, 2);
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "the retry settled the transition and ended"
    );
    assert_eq!(
        feed.source_snapshot().generation,
        1,
        "the retry installed the successor"
    );
    assert!(
        !matches!(feed.take_visible(cursor), Take::Lagged { .. }),
        "a never-slow singleton must not be lag-cut by the retry task"
    );
    assert_eq!(feed.head(), cursor, "the retry task never reads");
    assert_eq!(
        feed.source_read_count(),
        0,
        "the retry task issues no source read"
    );
    assert!(
        woken.has_changed().unwrap(),
        "a readable tail found under the retry's permit must wake the sessions"
    );
    expect_solo(&feed, 0, 2).await;
}

/// Scenario B (red): after a 2 -> 1 leave with a retained ring, the
/// survivor's next read must start exactly at the old head. A solo
/// read by the retry leaves the floor alone (the ring is non-empty),
/// so no typed lag fires and the offsets it read are never sent.
#[tokio::test(start_paused = true)]
async fn retry_never_skips_the_survivor_past_a_retained_ring() {
    let budget = budget();
    let (feed, src) = feed_with(3, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    let old_head = feed.head();
    assert_eq!(old_head, 3);
    assert!(feed.retained() > 0, "the shared batch is retained");
    stage_successor(&src, old_head, 2);
    assert_eq!(
        feed.leave_locked(),
        1,
        "2 -> 1: the survivor keeps the ring"
    );
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Without these two, the survivor's own drive below would install the
    // successor and read 3..5 itself, and a retry that did nothing at all
    // would pass.
    assert_eq!(
        feed.source_snapshot().generation,
        1,
        "the retry task installed the successor"
    );
    assert_eq!(feed.head(), old_head, "the retry task never reads");
    assert!(matches!(feed.take_visible(old_head), Take::AtHead));
    expect_solo(&feed, 3, 5).await;
}

/// Corrections 1+4 (red): the tick needs no install to reach a read -
/// an already-installed open source with a fresh append is enough.
/// The tick must leave the read to the session AND wake it: a session
/// that lost the permit to this tick has no other wake.
#[tokio::test(start_paused = true)]
async fn retry_tick_on_a_readable_tail_wakes_the_session_and_reads_nothing() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 1 << 20, &budget);
    let (cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    src.frontier.store(2, Ordering::Relaxed);
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(feed.head(), cursor, "the retry task never reads");
    assert_eq!(feed.source_read_count(), 0);
    assert!(
        !matches!(feed.take_visible(cursor), Take::Lagged { .. }),
        "the floor did not move under the session"
    );
    assert!(
        woken.has_changed().unwrap(),
        "the readable tail woke the session"
    );
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "an open source ends the retry"
    );
    expect_solo(&feed, 0, 2).await;
}

/// Red: a closed predecessor whose remainder the session has not read
/// yet. The retry must not read it, must wake the session, and must
/// KEEP ticking - the transition is still unresolved - then settle the
/// close once the subscriber has read to the cap.
#[tokio::test(start_paused = true)]
async fn retry_keeps_ticking_while_a_closed_tail_is_still_unread() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 1 << 20, &budget);
    let (cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    src.frontier.store(2, Ordering::Relaxed);
    src.closed.store(true, Ordering::Relaxed);
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(feed.head(), cursor, "the retry task never reads");
    assert!(
        woken.has_changed().unwrap(),
        "the readable tail woke the session"
    );
    assert!(
        feed.retry_scheduled.load(Ordering::SeqCst),
        "a closed tail is still unresolved: the ONE task keeps ticking"
    );
    expect_solo(&feed, 0, 2).await;
    // Tick 2 (500 ms): nothing beyond the head, closed, no successor.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(feed.lifecycle_for_test(), "Closed");
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "settled: the task ended"
    );
}

/// The loop contract (green pin): a contended permit and an in-flight
/// transition (`RetryLater`) keep the ONE task ticking; a settled one
/// ends it.
#[tokio::test(start_paused = true)]
async fn retry_keeps_ticking_until_the_transition_settles() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 4096, &budget);
    feed.subscribe_locked();
    stage_successor(&src, 0, 0);
    let permit = feed.acquire_permit().expect("the permit is free");
    feed.schedule_transition_retry();
    // Tick 1 (250 ms): a subscriber holds the permit. The successor is
    // READY, so a tick that ignored the held permit would install it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        feed.source_snapshot().generation,
        0,
        "only the permit holder may settle the tail"
    );
    assert!(
        src.next_result.lock().unwrap().is_some(),
        "the contended tick never asked the source for its successor"
    );
    assert!(
        feed.retry_scheduled.load(Ordering::SeqCst),
        "a contended attempt is not a settlement"
    );
    src.retry_later.store(true, Ordering::Relaxed);
    drop(permit);
    // Tick 2 (500 ms): the transition is still in flight.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(feed.source_snapshot().generation, 0);
    assert!(
        feed.retry_scheduled.load(Ordering::SeqCst),
        "RetryLater re-arms the tick"
    );
    // Tick 3 (750 ms): the successor is published and installed.
    src.retry_later.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(feed.source_snapshot().generation, 1);
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "settled: the task ended"
    );
}

/// Characterization pin (green): an incompatible successor is a typed
/// cutoff - one bump at the transition itself, a repeatable outcome
/// for every later drive, and the retry ends.
#[tokio::test(start_paused = true)]
async fn retry_cuts_the_feed_off_on_an_incompatible_successor() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 1 << 20, &budget);
    let (_cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    let next = FakeSource::new(0, 8);
    *next.sig_override.lock().unwrap() = Some(vec![(9, 0, None)]);
    src.closed.store(true, Ordering::Relaxed);
    *src.next_result.lock().unwrap() = Some(Arc::new(next) as Arc<dyn FeedSourceRead>);
    let v = feed.version();
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(feed.lifecycle_for_test(), "Gone");
    assert_eq!(feed.version(), v + 1, "the cutoff bumps exactly once");
    assert!(woken.has_changed().unwrap());
    assert!(!feed.retry_scheduled.load(Ordering::SeqCst));
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::IncarnationClosed(
            SourceCutoff::IncompatibleTopology
        ))
    ));
    assert_eq!(feed.version(), v + 1, "its observation never bumps again");
}

/// One permit hold settles at most FOUR swaps; a longer storm is left
/// `Unresolved`, so the task ticks again and the next hold continues
/// it. The budget was `swap_attempts >= 4` before it became this
/// loop's bound, and nothing pinned it on either side.
#[tokio::test(start_paused = true)]
async fn a_swap_storm_is_continued_by_the_next_attempt() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 4096, &budget);
    feed.subscribe_locked();
    // s0 -> s1 -> .. -> s5: five sealed hops, all at offset 0, then an
    // open tail. Hop k's signature lists every sealed span before it.
    let sig = |k: u32| -> Vec<(u32, u64, Option<u64>)> {
        let mut spans: Vec<_> = (0..k).map(|i| (i, 0, Some(0))).collect();
        spans.push((k, 0, None));
        spans
    };
    let mut next: Option<Arc<dyn FeedSourceRead>> = None;
    for k in (1..=5u32).rev() {
        let hop = FakeSource::new(0, 8);
        *hop.sig_override.lock().unwrap() = Some(sig(k));
        hop.closed.store(k < 5, Ordering::Relaxed);
        *hop.next_result.lock().unwrap() = next.take();
        next = Some(Arc::new(hop) as Arc<dyn FeedSourceRead>);
    }
    *src.sig_override.lock().unwrap() = Some(sig(0));
    src.closed.store(true, Ordering::Relaxed);
    *src.next_result.lock().unwrap() = next;

    feed.schedule_transition_retry();
    // Tick 1 (250 ms): four swaps in one hold, then the hold ends.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        feed.source_snapshot().generation,
        4,
        "one hold settles at most four swaps"
    );
    assert_eq!(feed.lifecycle_for_test(), "Active");
    assert!(
        feed.retry_scheduled.load(Ordering::SeqCst),
        "a storm cut short is unresolved: the task ticks again"
    );
    // Tick 2 (500 ms): the fifth swap lands on the open tail; settled.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(feed.source_snapshot().generation, 5);
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "an open tail is settled: the task ended"
    );
}

/// The task is the feed's own: it ends when nobody is left to wake, and
/// it must not settle a transition on behalf of subscribers who left.
#[tokio::test(start_paused = true)]
async fn retry_ends_when_the_last_subscriber_leaves() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 4096, &budget);
    feed.subscribe_locked();
    stage_successor(&src, 0, 0);
    feed.schedule_transition_retry();
    assert_eq!(feed.leave_locked(), 0, "1 -> 0: nobody is left");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "no subscribers: the task ended on its first tick"
    );
    assert_eq!(
        feed.source_snapshot().generation,
        0,
        "and it settled nothing for an audience that had gone"
    );
}

/// Teardown does not wait out the tick: firing the feed's cancel flag
/// ends the task at once, mid-sleep.
#[tokio::test(start_paused = true)]
async fn retry_ends_at_once_when_the_feed_is_cancelled() {
    let budget = budget();
    let (feed, src) = feed_with(0, 8, 4096, &budget);
    feed.subscribe_locked();
    stage_successor(&src, 0, 0);
    src.retry_later.store(true, Ordering::Relaxed);
    feed.schedule_transition_retry();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(feed.retry_scheduled.load(Ordering::SeqCst), "armed");
    feed.cancel.fire();
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(
        !feed.retry_scheduled.load(Ordering::SeqCst),
        "cancelled 239 ms before its tick was due"
    );
}
