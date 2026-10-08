//! Exact bookkeeping of one journal and of the registry: the cursor each wait
//! answers, the waiter index after a wake or a reap, the history key budget,
//! the flusher's reap cadence and a shard close. A journal built here has a
//! fixed epoch and no flusher, so each flush is the test's own.
#![cfg(test)]

use super::{
    BUCKET_KEY_CAP, BUCKET_MS, HISTORY_BUCKETS, HISTORY_KEY_BUDGET, Inner, TouchJournal,
    TouchRegistry, WaitOutcome,
};
use crate::crypto::RouteHash;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

const LONG: Duration = Duration::from_secs(3_600);

fn fixed(epoch: &str) -> Arc<TouchJournal> {
    Arc::new(TouchJournal {
        epoch: epoch.into(),
        inner: Mutex::new(Inner::default()),
    })
}

fn render(outcome: WaitOutcome) -> String {
    match outcome {
        WaitOutcome::Touched {
            cursor,
            end_offset,
            proven,
        } => format!("touched {cursor} end {end_offset} proven {proven}"),
        WaitOutcome::Timeout { cursor, end_offset } => format!("timeout {cursor} end {end_offset}"),
        WaitOutcome::Stale { cursor } => format!("stale {cursor}"),
    }
}

fn ready(poll: Poll<WaitOutcome>) -> String {
    match poll {
        Poll::Ready(outcome) => render(outcome),
        Poll::Pending => "pending".into(),
    }
}

fn index(journal: &TouchJournal) -> HashMap<u32, Vec<u64>> {
    journal.inner.lock().unwrap().key_index.clone()
}

fn waiter_ids(journal: &TouchJournal) -> Vec<u64> {
    let mut ids: Vec<u64> = journal
        .inner
        .lock()
        .unwrap()
        .waiters
        .keys()
        .copied()
        .collect();
    ids.sort_unstable();
    ids
}

/// (buckets, history keys, floor, oldest retained generation)
fn history(journal: &TouchJournal) -> (usize, usize, u64, Option<u64>) {
    let inner = journal.inner.lock().unwrap();
    let oldest = inner.history.front().map(|bucket| bucket.generation);
    (
        inner.history.len(),
        inner.history_keys,
        inner.history_floor,
        oldest,
    )
}

/// Registers a wait on key 7 and drops it, leaving a cancelled waiter.
async fn cancel_a_wait(journal: &Arc<TouchJournal>) {
    let mut wait = Box::pin(journal.wait("now", vec![7], LONG));
    assert!(futures_util::poll!(wait.as_mut()).is_pending());
}

#[tokio::test]
async fn a_woken_timed_out_or_closed_wait_answers_the_epoch_and_the_generation_it_saw() {
    let journal = fixed("cursor-epoch");
    let mut touched = Box::pin(journal.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(touched.as_mut())), "pending");
    journal.ingest(&[7], 9);
    journal.flush_bucket(false);
    assert_eq!(
        render(touched.await),
        "touched cursor-epoch:1 end 9 proven true"
    );
    assert_eq!(
        render(
            journal
                .wait("cursor-epoch:1", vec![7], Duration::ZERO)
                .await
        ),
        "timeout cursor-epoch:1 end 9"
    );
    let mut closed = Box::pin(journal.wait("cursor-epoch:1", vec![11], LONG));
    assert_eq!(ready(futures_util::poll!(closed.as_mut())), "pending");
    journal.ingest(&[7], 12);
    journal.flush_bucket(false);
    assert_eq!(ready(futures_util::poll!(closed.as_mut())), "pending");
    journal.close();
    assert_eq!(render(closed.await), "stale cursor-epoch:2");
}

#[tokio::test]
async fn a_woken_waiter_leaves_every_other_waiter_indexed_under_its_keys() {
    let journal = fixed("index-epoch");
    let mut first = Box::pin(journal.wait("now", vec![7], LONG));
    let mut second = Box::pin(journal.wait("now", vec![11], LONG));
    let mut both = Box::pin(journal.wait("now", vec![7, 11], LONG));
    for wait in [first.as_mut(), second.as_mut(), both.as_mut()] {
        assert_eq!(ready(futures_util::poll!(wait)), "pending");
    }
    assert_eq!(
        index(&journal),
        HashMap::from([(7, vec![0, 2]), (11, vec![1, 2])])
    );
    journal.ingest(&[7], 17);
    journal.flush_bucket(false);
    for wait in [first.as_mut(), both.as_mut()] {
        assert_eq!(
            ready(futures_util::poll!(wait)),
            "touched index-epoch:1 end 17 proven true"
        );
    }
    assert_eq!(index(&journal), HashMap::from([(11, vec![1])]));
    assert_eq!(waiter_ids(&journal), [1]);
    journal.ingest(&[11], 18);
    journal.flush_bucket(false);
    assert_eq!(
        ready(futures_util::poll!(second.as_mut())),
        "touched index-epoch:2 end 18 proven true"
    );
    assert_eq!(index(&journal), HashMap::new());
    assert_eq!(waiter_ids(&journal), [0u64; 0]);
}

#[tokio::test]
async fn reaping_a_cancelled_waiter_unindexes_that_waiter_alone() {
    let journal = fixed("reap-epoch");
    let mut live = Box::pin(journal.wait("now", vec![7, 11], LONG));
    assert_eq!(ready(futures_util::poll!(live.as_mut())), "pending");
    {
        let mut cancelled = Box::pin(journal.wait("now", vec![11], LONG));
        assert_eq!(ready(futures_util::poll!(cancelled.as_mut())), "pending");
    }
    assert_eq!(
        index(&journal),
        HashMap::from([(7, vec![0]), (11, vec![0, 1])])
    );
    journal.flush_bucket(true);
    assert_eq!(
        index(&journal),
        HashMap::from([(7, vec![0]), (11, vec![0])])
    );
    assert_eq!(waiter_ids(&journal), [0]);
    journal.ingest(&[11], 4);
    journal.flush_bucket(false);
    assert_eq!(
        ready(futures_util::poll!(live.as_mut())),
        "touched reap-epoch:1 end 4 proven true"
    );
}

#[tokio::test]
async fn history_holds_exactly_its_key_budget_and_evicts_the_oldest_bucket_only_above_it() {
    let journal = fixed("budget-epoch");
    let buckets = 32;
    let per_bucket = HISTORY_KEY_BUDGET / buckets;
    assert_eq!(per_bucket * buckets, HISTORY_KEY_BUDGET);
    assert!(per_bucket < BUCKET_KEY_CAP && buckets < HISTORY_BUCKETS);
    let keys: Vec<u32> = (0..u32::try_from(per_bucket).unwrap()).collect();
    for offset in 1..=u64::try_from(buckets).unwrap() {
        journal.ingest(&keys, offset);
        journal.flush_bucket(false);
    }
    assert_eq!(history(&journal), (32, 2_000_000, 0, Some(1)));
    assert_eq!(journal.inner.lock().unwrap().catch_up(0, &[99]), Some(true));
    journal.ingest(&[7], 33);
    journal.flush_bucket(false);
    assert_eq!(history(&journal), (32, 1_937_501, 1, Some(2)));
    let inner = journal.inner.lock().unwrap();
    assert_eq!(inner.catch_up(0, &[99]), Some(false));
    assert_eq!(inner.catch_up(1, &[99]), Some(true));
}

#[tokio::test]
async fn history_past_its_bucket_count_keeps_the_keys_of_the_buckets_it_retains() {
    let journal = fixed("count-epoch");
    for offset in 1..=u64::try_from(HISTORY_BUCKETS + 2).unwrap() {
        journal.ingest(&[7, 11], offset);
        journal.flush_bucket(false);
    }
    assert_eq!(
        history(&journal),
        (HISTORY_BUCKETS, 2 * HISTORY_BUCKETS, 2, Some(3))
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_flusher_reaps_cancelled_waiters_on_every_fortieth_tick_and_on_no_other() {
    let origin = tokio::time::Instant::now();
    let journal = TouchJournal::start(&crate::runtime::OsEntropy);
    cancel_a_wait(&journal).await;
    let mut alive = 1;
    let mut reaped_on = Vec::new();
    for tick in 1..=80u64 {
        // The first tick fires at once and tick n at (n - 1) * BUCKET_MS:
        // look halfway between tick n and tick n + 1.
        let halfway = BUCKET_MS * tick - BUCKET_MS / 2;
        tokio::time::sleep_until(origin + Duration::from_millis(halfway)).await;
        let waiters = waiter_ids(&journal).len();
        if waiters < alive {
            reaped_on.push(tick);
        }
        alive = waiters;
        if tick == 40 {
            cancel_a_wait(&journal).await;
            alive += 1;
        }
    }
    assert_eq!(reaped_on, [40, 80]);
    journal.close();
}

#[tokio::test]
async fn closing_a_shard_closes_and_forgets_exactly_the_journals_whose_route_is_in_its_prefix() {
    let registry = TouchRegistry::with_entropy(Arc::new(crate::runtime::OsEntropy));
    // Route bits 0100.. are in shard "01"; storage bits 0000.. are not.
    let (inside_hash, inside_route) = ([0x00; 16], RouteHash([0x40; 16]));
    // Storage bits 0100.. are in shard "01"; route bits 1000.. are not.
    let (outside_hash, outside_route) = ([0x40; 16], RouteHash([0x80; 16]));
    let inside = registry.journal(inside_hash, inside_route);
    let outside = registry.journal(outside_hash, outside_route);
    let mut inside_wait = Box::pin(inside.wait("now", vec![7], LONG));
    let mut outside_wait = Box::pin(outside.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(inside_wait.as_mut())), "pending");
    assert_eq!(ready(futures_util::poll!(outside_wait.as_mut())), "pending");

    registry.close_shard("01");

    assert_eq!(
        ready(futures_util::poll!(inside_wait.as_mut())),
        format!("stale {}:0", inside.epoch)
    );
    assert_eq!(
        render(inside.wait("now", vec![7], LONG).await),
        format!("stale {}:0", inside.epoch)
    );
    assert_eq!(ready(futures_util::poll!(outside_wait.as_mut())), "pending");
    assert!(Arc::ptr_eq(
        &registry.journal(outside_hash, outside_route),
        &outside
    ));
    let reopened = registry.journal(inside_hash, inside_route);
    assert!(!Arc::ptr_eq(&reopened, &inside));
    assert_eq!(registry.map.lock().unwrap().len(), 2);
    outside.ingest(&[7], 5);
    outside.flush_bucket(false);
    assert_eq!(
        ready(futures_util::poll!(outside_wait.as_mut())),
        format!("touched {}:1 end 5 proven true", outside.epoch)
    );
    outside.close();
    reopened.close();
}

/// Retiring a deleted incarnation's journal closes exactly that journal
/// (its waiter wakes stale), forgets it so its flusher ends and the next
/// journal for the identity is a fresh one, and leaves every other journal
/// waiting; retiring an identity with no journal changes nothing.
#[tokio::test]
async fn retiring_an_identity_closes_and_forgets_exactly_its_journal() {
    let registry = TouchRegistry::with_entropy(Arc::new(crate::runtime::OsEntropy));
    let route = RouteHash([0x40; 16]);
    let (deleted_hash, kept_hash) = ([0x01; 16], [0x02; 16]);
    let deleted = registry.journal(deleted_hash, route);
    let kept = registry.journal(kept_hash, route);
    let mut deleted_wait = Box::pin(deleted.wait("now", vec![7], LONG));
    let mut kept_wait = Box::pin(kept.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(deleted_wait.as_mut())), "pending");
    assert_eq!(ready(futures_util::poll!(kept_wait.as_mut())), "pending");

    registry.retire(deleted_hash);
    registry.retire([0x03; 16]);

    assert_eq!(
        ready(futures_util::poll!(deleted_wait.as_mut())),
        format!("stale {}:0", deleted.epoch)
    );
    assert_eq!(ready(futures_util::poll!(kept_wait.as_mut())), "pending");
    assert_eq!(registry.map.lock().unwrap().len(), 1);
    assert!(Arc::ptr_eq(&registry.journal(kept_hash, route), &kept));
    let reopened = registry.journal(deleted_hash, route);
    assert!(!Arc::ptr_eq(&reopened, &deleted));
    kept.close();
    reopened.close();
}

/// Shared cells Q10 (a): a journal with no waiter and no touch for ten
/// minutes is retired by the registry's idle sweep (which looks every
/// minute), so its flusher ends; a watcher that returns with its old
/// cursor is answered stale once, by a fresh journal, and resynchronises.
/// A journal with a parked waiter, or touched within the ten minutes,
/// stays. The test holds no journal it expects retired: a held one is
/// kept (the next test).
#[tokio::test(start_paused = true)]
async fn a_journal_idle_for_ten_minutes_is_retired_and_its_watchers_resynchronise() {
    let registry = TouchRegistry::with_entropy(Arc::new(crate::runtime::OsEntropy));
    let route = RouteHash([0x40; 16]);
    let (idle_hash, waited_hash, touched_hash) = ([0x01; 16], [0x02; 16], [0x03; 16]);
    let idle = registry.journal(idle_hash, route);
    idle.ingest(&[7], 1);
    let mut first = Box::pin(idle.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(first.as_mut())), "pending");
    let cursor = match first.await {
        WaitOutcome::Touched { cursor, .. } => cursor,
        other => panic!("the idle journal's touch: {}", render(other)),
    };
    let retired = Arc::downgrade(&idle);
    drop(idle);
    let waited = registry.journal(waited_hash, route);
    let mut parked = Box::pin(waited.wait("now", vec![7], 2 * LONG));
    assert_eq!(ready(futures_util::poll!(parked.as_mut())), "pending");
    registry.journal(touched_hash, route);

    tokio::time::sleep(Duration::from_secs(9 * 60)).await;
    registry.journal(touched_hash, route).ingest(&[7], 2);
    tokio::time::sleep(Duration::from_secs(3 * 60)).await;

    let held: Vec<[u8; 16]> = {
        let map = registry.map.lock().unwrap();
        let mut held: Vec<[u8; 16]> = map.keys().copied().collect();
        held.sort_unstable();
        held
    };
    assert_eq!(
        held,
        [waited_hash, touched_hash],
        "the idle journal is retired"
    );
    assert!(retired.upgrade().is_none(), "its flusher ended");
    assert_eq!(ready(futures_util::poll!(parked.as_mut())), "pending");
    let fresh = registry.journal(idle_hash, route);
    assert!(
        !std::ptr::eq(Arc::as_ptr(&fresh), retired.as_ptr()),
        "a returning watcher's journal is fresh"
    );
    assert_eq!(
        render(fresh.wait(&cursor, vec![7], Duration::ZERO).await),
        format!("stale {}:0", fresh.epoch)
    );
    for journal in [&fresh, &waited, &registry.journal(touched_hash, route)] {
        journal.close();
    }
}

/// An append resolves its stream's journal when it is admitted and feeds
/// the journal its touch only once the batch is durable. A journal that a
/// request still holds (here an admitted append's touch feed) is not idle,
/// however long it sat quiet before: the sweep keeps it, so the touch
/// reaches the journal a watcher arriving in between finds.
#[tokio::test(start_paused = true)]
async fn a_journal_an_admitted_append_holds_is_kept_until_its_touch_lands() {
    let registry = TouchRegistry::with_entropy(Arc::new(crate::runtime::OsEntropy));
    let (hash, route) = ([0x01; 16], RouteHash([0x40; 16]));
    registry.journal(hash, route);
    tokio::time::sleep(Duration::from_secs(9 * 60 + 30)).await;
    let feed = crate::shard::TouchFeed {
        journal: registry.journal(hash, route),
        key_ids: vec![7],
        next_offset: 9,
    };
    tokio::time::sleep(Duration::from_secs(2 * 60)).await;
    let watcher = registry.journal(hash, route);
    let mut wait = Box::pin(watcher.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(wait.as_mut())), "pending");
    feed.journal.ingest(&feed.key_ids, feed.next_offset);
    tokio::time::sleep(Duration::from_millis(2 * BUCKET_MS)).await;
    assert_eq!(
        ready(futures_util::poll!(wait.as_mut())),
        format!("touched {}:1 end 9 proven true", watcher.epoch),
        "the admitted append's touch reaches the journal its watcher found"
    );
    watcher.close();
}

/// The idle sweep counts a wait as activity: a journal whose watcher waits
/// (and times out) between every two looks is never idle, however long
/// that goes on; once the waits stop it is retired ten minutes after the
/// last one. A journal built here has no flusher: the test's own handle
/// stands where the flusher's would, so the journal counts as unheld.
#[tokio::test(start_paused = true)]
async fn a_journal_waited_on_between_looks_stays_until_ten_minutes_after_the_last_wait() {
    let hash = [0x01; 16];
    let journal = fixed("polled");
    let slot = (RouteHash([0x40; 16]), journal.clone());
    let map: super::JournalMap = Mutex::new(HashMap::from([(hash, slot)]));
    let mut seen = HashMap::new();
    let start = tokio::time::Instant::now();
    let look = |seen: &mut HashMap<[u8; 16], super::Seen>, minute: u64| {
        super::retire_idle(&map, seen, start + Duration::from_secs(60 * minute));
        map.lock().unwrap().contains_key(&hash)
    };
    for minute in 0..=20 {
        let timed_out = render(journal.wait("now", vec![7], Duration::ZERO).await);
        assert_eq!(timed_out, "timeout polled:0 end 0");
        journal.flush_bucket(true);
        assert!(look(&mut seen, minute), "waited on at minute {minute}");
    }
    let kept: Vec<u64> = (21..=31)
        .filter(|minute| look(&mut seen, *minute))
        .collect();
    assert_eq!(kept, (21..30).collect::<Vec<u64>>());
    assert!(
        journal.inner.lock().unwrap().closed,
        "retired as at a fence"
    );
}

/// The idle sweep retires only a journal with nothing left to deliver: one
/// with a parked waiter, or with a touch ingested and not yet published, is
/// not idle however long the sweep sees it unchanged (retiring it would
/// wake the waiter stale, or lose the touch). Once the waiter is answered
/// and the touch published, each is retired ten minutes later. A journal
/// built here has no flusher: the test's own handle stands where the
/// flusher's would, so each journal counts as unheld.
#[tokio::test(start_paused = true)]
async fn a_journal_with_a_parked_waiter_or_an_unpublished_touch_is_never_idle() {
    let (parked_hash, unpublished_hash) = ([0x01; 16], [0x02; 16]);
    let (parked, unpublished) = (fixed("parked"), fixed("unpublished"));
    let route = RouteHash([0x40; 16]);
    let map: super::JournalMap = Mutex::new(HashMap::from([
        (parked_hash, (route, parked.clone())),
        (unpublished_hash, (route, unpublished.clone())),
    ]));
    let mut wait = Box::pin(parked.wait("now", vec![7], LONG));
    assert_eq!(ready(futures_util::poll!(wait.as_mut())), "pending");
    unpublished.ingest(&[7], 1);
    let mut seen = HashMap::new();
    let start = tokio::time::Instant::now();
    let look = |seen: &mut HashMap<[u8; 16], super::Seen>, minute: u64| {
        super::retire_idle(&map, seen, start + Duration::from_secs(60 * minute));
        let map = map.lock().unwrap();
        [parked_hash, unpublished_hash].map(|hash| map.contains_key(&hash))
    };
    let retired: Vec<u64> = (0..=30)
        .filter(|minute| look(&mut seen, *minute) != [true, true])
        .collect();
    assert_eq!(retired, [0u64; 0], "neither journal is idle");
    assert_eq!(ready(futures_util::poll!(wait.as_mut())), "pending");
    parked.ingest(&[7], 2);
    parked.flush_bucket(false);
    assert_eq!(
        ready(futures_util::poll!(wait.as_mut())),
        "touched parked:1 end 2 proven true"
    );
    unpublished.flush_bucket(false);
    let kept: Vec<u64> = (31..=41)
        .filter(|minute| look(&mut seen, *minute) == [true, true])
        .collect();
    assert_eq!(kept, (31..41).collect::<Vec<u64>>());
    assert_eq!(map.lock().unwrap().len(), 0);
    assert!(parked.inner.lock().unwrap().closed && unpublished.inner.lock().unwrap().closed);
}
