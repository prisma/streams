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
