//! The roster-driven sweeper (docs/QUEUES-DESIGN.md §6.8) reads the roster a
//! page at a time, each tick the page after the last, so a shard with more
//! due rows on its roster than one page holds sweeps every one of them: the
//! first page alone held the rest unswept for as long as its rows stayed on
//! the roster (prisma/streams#150, findings). The roster is in deadline order
//! (layout 8), so the rows a page fills with are due ones.
#![cfg(test)]
use std::time::{Duration, Instant};

use super::engine;
use crate::native_queue::rows::StateRow;
use crate::native_queue::{NqOp, NqOut, NqRefusal, QueueConfig, WakeRequest, keys};

/// The hash range of the not-yet-due real queues (`..REAL_NOT_DUE`) and of the
/// due roster rows naming no queue (`REAL_NOT_DUE..`), which fill the first
/// page whole (`ROSTER_PAGE`), ahead of the real due queues in deadline order.
/// A sweep of a row naming no queue is refused, so they stay, and the due
/// queues are reached only by the next tick reading the page after.
const NOT_DUE: u16 = 4_096 + REAL_NOT_DUE;
/// Queues due now, after them.
const DUE: u16 = 40;
/// Real queues not yet due, at the roster's end.
const REAL_NOT_DUE: u16 = 8;
/// How long the sweeper may take: a handful of 1 s ticks.
const SWEPT_WITHIN: Duration = Duration::from_secs(15);

fn not_due(n: u16) -> [u8; 16] {
    let [hi, lo] = n.to_be_bytes();
    let mut hash = [0u8; 16];
    hash[..3].copy_from_slice(&[0x10, hi, lo]);
    hash
}

fn due(n: u16) -> [u8; 16] {
    let [hi, lo] = n.to_be_bytes();
    let mut hash = [0u8; 16];
    hash[..3].copy_from_slice(&[0xE0, hi, lo]);
    hash
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_sweeper_reaches_every_rostered_queue_past_its_first_page() {
    let (e, db) = engine("nq-roster-pages").await;
    let hashes = (0..DUE).map(due).chain((0..REAL_NOT_DUE).map(not_due));
    let creates = hashes.map(|hash| {
        let cfg = QueueConfig::default();
        e.submit_native_queue(hash, NqOp::Create { cfg })
    });
    for created in futures_util::future::join_all(creates).await {
        created.expect("created");
    }
    rostered_at(&db, (0..DUE).map(due), 1).await;
    // Real queues due in a thousand years, last; ahead of the due queues, a
    // page of due rows that name no queue.
    rostered_at(&db, (0..REAL_NOT_DUE).map(not_due), i64::MAX).await;
    for n in REAL_NOT_DUE..NOT_DUE {
        db.put(keys::roster(0, &not_due(n)), []).await.unwrap();
    }
    let deadline = Instant::now() + SWEPT_WITHIN;
    let mut left = usize::from(DUE);
    while left > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let rows = keys::roster_rows(&db).await;
        left = (0..DUE).filter(|n| rows.contains_key(&due(*n))).count();
    }
    assert_eq!(
        left, 0,
        "{left} of the {DUE} due queues past the first page were never swept"
    );
    for n in 0..DUE {
        let raw = db.get(keys::state(&due(n))).await.unwrap().unwrap();
        assert_eq!(
            StateRow::decode(&raw).unwrap().roster_at,
            None,
            "queue {n} is off the roster"
        );
    }
    let rows = keys::roster_rows(&db).await;
    for n in 0..NOT_DUE {
        let want = if n < REAL_NOT_DUE { i64::MAX } else { 0 };
        assert_eq!(
            rows.get(&not_due(n)).copied(),
            Some(want),
            "an undue queue's row, and a row naming no queue, stay: {n}"
        );
    }
    for n in 0..REAL_NOT_DUE {
        let raw = db.get(keys::state(&not_due(n))).await.unwrap().unwrap();
        assert_eq!(
            StateRow::decode(&raw).unwrap().roster_at,
            Some(i64::MAX),
            "undue queue {n} was not swept"
        );
    }
}

/// Put `hashes`' queues on the roster, due `at`, their state rows agreeing.
async fn rostered_at(db: &slatedb::Db, hashes: impl Iterator<Item = [u8; 16]>, at: i64) {
    for hash in hashes {
        let key = keys::state(&hash);
        let mut s = StateRow::decode(&db.get(&key).await.unwrap().unwrap()).unwrap();
        s.roster_at = Some(at);
        db.put(&key, s.encode()).await.unwrap();
        db.put(keys::roster(at, &hash), []).await.unwrap();
    }
}

/// A tick's due queues are swept together, not one after another: each
/// sweep waits for its group's durable write, so one at a time a shard
/// swept about one queue per commit window. With every state-row load
/// held open, as many sweeps are in flight at once as a group loads cold
/// queues together, and every one completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_sweeper_sweeps_a_ticks_due_queues_together() {
    const QUEUES: u16 = 80;
    let (e, db) = engine("nq-roster-together").await;
    let creates = (0..QUEUES).map(|n| {
        let cfg = QueueConfig::default();
        e.submit_native_queue(due(n), NqOp::Create { cfg })
    });
    for created in futures_util::future::join_all(creates).await {
        created.expect("created");
    }
    let held = crate::shard::test_support::hold_native_state_reads("nq-roster-together");
    rostered_at(&db, (0..QUEUES).map(due), 0).await;
    let deadline = Instant::now() + SWEPT_WITHIN;
    while held.in_flight() < 64 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let together = held.most_in_flight();
    held.release();
    assert_eq!(together, 64, "the due queues' sweeps run together");
    let mut left = usize::from(QUEUES);
    while left > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let rows = keys::roster_rows(&db).await;
        left = (0..QUEUES).filter(|n| rows.contains_key(&due(*n))).count();
    }
    assert_eq!(left, 0, "every due queue was swept");
}

/// The sweeper posts the wake-up a sweep asks for, to the queue it swept,
/// and none for a sweep that asks for none or was refused. (The post
/// itself goes only to https on 443 at a public address: the egress
/// policy keeps it out of a test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_sweeper_posts_the_wake_up_a_sweep_asks_for_and_no_other() {
    let (e, _db) = engine("nq-roster-wake").await;
    let wake = WakeRequest {
        url: "https://example.com/hook".into(),
        name: "jobs".into(),
        version: 3,
        due: 7,
        oldest_due_age_ms: 1_500,
    };
    let asked = NqOut::Swept {
        next_deadline: Some(9),
        wake: Some(wake),
    };
    let Some((hash, posted)) = e.wake_of(due(1), Ok(asked)) else {
        panic!("the wake-up the sweep asked for is posted");
    };
    assert_eq!(
        (
            hash,
            posted.url.as_str(),
            posted.name.as_str(),
            posted.version,
            posted.due,
            posted.oldest_due_age_ms
        ),
        (due(1), "https://example.com/hook", "jobs", 3, 7, 1_500)
    );
    let none = NqOut::Swept {
        next_deadline: Some(9),
        wake: None,
    };
    assert!(e.wake_of(due(1), Ok(none)).is_none(), "none asked");
    assert!(
        e.wake_of(due(1), Err(NqRefusal::NotFound)).is_none(),
        "a refused sweep posts none"
    );
}
