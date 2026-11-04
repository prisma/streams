//! The roster-driven sweeper (docs/QUEUES-DESIGN.md §6.8) reads the roster a
//! page at a time, each tick the page after the last, so a shard with more
//! queues on its roster than one page holds sweeps every one of them: the
//! first page alone held the rest unswept for as long as its queues stayed
//! on the roster (prisma/streams#150, findings).
#![cfg(test)]
use std::time::{Duration, Instant};

use super::engine;
use crate::native_queue::rows::StateRow;
use crate::native_queue::{NqOp, QueueConfig, keys};

/// Roster rows of queues not yet due, ahead of the due ones in hash order:
/// one page's worth (`ROSTER_PAGE`).
const NOT_DUE: u16 = 4_096;
/// Queues due now, after them.
const DUE: u16 = 40;
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
    let creates = (0..DUE).map(|n| {
        let cfg = QueueConfig::default();
        e.submit_native_queue(due(n), NqOp::Create { cfg })
    });
    for created in futures_util::future::join_all(creates).await {
        created.expect("created");
    }
    rostered_now(&db, (0..DUE).map(due)).await;
    // A page of queues due in a thousand years, ahead of them in hash order.
    for n in 0..NOT_DUE {
        let later = i64::MAX.to_be_bytes();
        db.put(keys::roster(&not_due(n)), later).await.unwrap();
    }
    let deadline = Instant::now() + SWEPT_WITHIN;
    let mut left = usize::from(DUE);
    while left > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        left = 0;
        for n in 0..DUE {
            left += usize::from(db.get(keys::roster(&due(n))).await.unwrap().is_some());
        }
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
    for n in 0..NOT_DUE {
        let row = db.get(keys::roster(&not_due(n))).await.unwrap();
        assert_eq!(
            row.as_deref(),
            Some(&i64::MAX.to_be_bytes()[..]),
            "an undue row stays"
        );
    }
}

/// Put `hashes`' queues on the roster, due now, their state rows agreeing.
async fn rostered_now(db: &slatedb::Db, hashes: impl Iterator<Item = [u8; 16]>) {
    for hash in hashes {
        let key = keys::state(&hash);
        let mut s = StateRow::decode(&db.get(&key).await.unwrap().unwrap()).unwrap();
        s.roster_at = Some(0);
        db.put(&key, s.encode()).await.unwrap();
        db.put(keys::roster(&hash), 0i64.to_be_bytes())
            .await
            .unwrap();
    }
}
