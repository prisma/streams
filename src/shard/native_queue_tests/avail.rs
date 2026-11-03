//! The lane and the scheduled index (S3b-DESIGN §4-§5): an untaken
//! immediate message has no row but its page, and a claim leases it from
//! the page at the frontier; a scheduled message (a future `deliverAt`)
//! has a scheduled entry carrying its facts and, when small, its page, so
//! the claim that takes it when due reads neither a row nor the page.
#![cfg(test)]
use super::{HASH, claim, clocked, msg, op, send_op, set_clock, state};
use crate::native_queue::keys::page as shard_page_key;
use crate::native_queue::range::RangeRow;
use crate::native_queue::rows::{DState, DeliveryRow, MessageFacts, decode_scheduled};
use crate::native_queue::{INLINE_PAGE_MAX, QueueConfig, SendMessage, keys};

/// The family byte of every row under the queue's prefix, in key order.
async fn families(db: &slatedb::Db) -> Vec<u8> {
    let from = crate::keyspace::queue_row(&HASH, 17);
    let mut to = from.clone();
    *to.last_mut().unwrap() += 1;
    let mut rows = db.scan(from..to).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = rows.next().await.unwrap() {
        out.push(kv.key[keys::FAMILY_AT]);
    }
    out
}

async fn delivery(db: &slatedb::Db, off: u64) -> DeliveryRow {
    let raw = db
        .get(keys::by_offset(&HASH, keys::DELIVERY, off))
        .await
        .unwrap()
        .unwrap();
    DeliveryRow::decode(&raw).unwrap()
}

async fn range(db: &slatedb::Db, off: u64) -> Option<RangeRow> {
    db.get(keys::range(&HASH, off))
        .await
        .unwrap()
        .map(|raw| RangeRow::decode(&raw).unwrap())
}

fn scheduled(body: &str, at: i64) -> SendMessage {
    SendMessage {
        deliver_at: Some(at),
        ..msg(body)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_untaken_immediate_message_has_no_row_but_its_page() {
    let (e, db, _c) = clocked("nq-lane-norow", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    assert_eq!(
        families(&db).await,
        [keys::PAGE, keys::PAGE, keys::PAGE, keys::STATE, b't'],
        "three pages, the state row and the queue stream's tail: no metadata, \
         availability or range row"
    );
    let s = state(&db).await;
    assert_eq!((s.frontier, s.queued_messages), (0, 3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_leases_the_lane_from_its_page_and_moves_the_frontier() {
    let (e, db, c) = clocked("nq-lane-claim", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    let page = db.get(shard_page_key(&HASH, 0)).await.unwrap().unwrap();
    let d = claim(&e, 1).await;
    assert_eq!(d.len(), 1);
    assert_eq!((d[0].offset, d[0].enqueued_at, d[0].available_at), (0, c, c));
    assert_eq!(d[0].page.as_deref(), Some(&page[..]), "the claim returns it");
    let row = delivery(&db, 0).await;
    assert_eq!(
        (row.state, row.facts(), row.scheduled, row.inline, row.attempts),
        (
            DState::Leased,
            MessageFacts {
                enqueued_at: c,
                available_at: c,
                bytes: u32::try_from(page.len()).unwrap(),
            },
            false,
            false,
            1
        )
    );
    let r = range(&db, 0).await.expect("the range row counts the delivery");
    assert_eq!((r.open, r.sched), (1, 0));
    let s = state(&db).await;
    assert_eq!(s.frontier, 1, "past the leased message only");
    assert_eq!(s.lane_expire_at, c + s.cfg.retention_ms);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_small_scheduled_message_rides_in_its_scheduled_entry() {
    let (e, db, c) = clocked("nq-sched-inline", QueueConfig::default()).await;
    let at = c + 60_000;
    op(&e, send_op(vec![scheduled("hello", at)])).await.unwrap();
    let page = db.get(shard_page_key(&HASH, 0)).await.unwrap().unwrap();
    let entry = db
        .get(keys::by_time(&HASH, keys::AVAIL, at, 0))
        .await
        .unwrap()
        .unwrap();
    let (facts, inline) = decode_scheduled(at, &entry).unwrap();
    assert_eq!(
        (facts.enqueued_at, facts.bytes, inline),
        (c, u32::try_from(page.len()).unwrap(), &page[..])
    );
    let by_offset = db
        .get(keys::by_offset(&HASH, keys::SCHEDULED_AT, 0))
        .await
        .unwrap();
    assert_eq!(by_offset.as_deref(), Some(&at.to_le_bytes()[..]));
    let r = range(&db, 0).await.unwrap();
    assert!(r.skip.contains(0) && r.sbits.contains(0) && r.sched == 1);
    assert!(claim(&e, 1).await.is_empty(), "not due yet");
    let s = state(&db).await;
    assert_eq!((s.frontier, s.scheduled), (1, 1), "the frontier passed it");
    let r = range(&db, 0).await.unwrap();
    assert!(!r.skip.contains(0) && r.sbits.contains(0), "unresolved still");
    set_clock(&db, at).await;
    let d = claim(&e, 1).await;
    assert_eq!(d.len(), 1, "taken from the scheduled entry when due");
    assert_eq!(d[0].page.as_deref(), Some(&page[..]), "with its inline page");
    let row = delivery(&db, 0).await;
    assert!(row.scheduled && row.inline);
    assert_eq!(row.available_at, at);
}

/// Incompressible text (pages are compressed before sealing).
fn noise(n: usize) -> String {
    let mut x = 0x9E37_79B9_u32;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            char::from(b'!' + u8::try_from(x % 90).unwrap())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_page_rides_inline_up_to_the_limit_exactly_and_not_a_byte_past_it() {
    let (e, db, c) = clocked("nq-avail-limit", QueueConfig::default()).await;
    let at = c + 60_000;
    // One send of every body size around the limit; each is its own page.
    let text = noise(2 * INLINE_PAGE_MAX);
    let sizes = INLINE_PAGE_MAX * 3 / 4..INLINE_PAGE_MAX * 5 / 4;
    op(&e, send_op(sizes.map(|n| scheduled(&text[..n], at)).collect()))
        .await
        .unwrap();
    let (mut at_limit, mut past) = (false, false);
    for off in 0..u64::try_from(INLINE_PAGE_MAX / 2).unwrap() {
        let page = db.get(shard_page_key(&HASH, off)).await.unwrap().unwrap();
        let entry = db
            .get(keys::by_time(&HASH, keys::AVAIL, at, off))
            .await
            .unwrap()
            .unwrap();
        let inline = decode_scheduled(at, &entry).unwrap().1.to_vec();
        if page.len() <= INLINE_PAGE_MAX {
            assert_eq!(inline, page.to_vec(), "{} bytes ride inline", page.len());
            at_limit |= page.len() == INLINE_PAGE_MAX;
        } else {
            assert!(inline.is_empty(), "{} bytes are read by key", page.len());
            past = true;
        }
    }
    assert!(at_limit && past, "both sides of the limit were sent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_of_a_large_scheduled_message_leaves_its_page_to_the_page_key() {
    let (e, db, c) = clocked("nq-avail-large", QueueConfig::default()).await;
    let at = c + 60_000;
    op(&e, send_op(vec![scheduled(&noise(2 * INLINE_PAGE_MAX), at)]))
        .await
        .unwrap();
    let page = db.get(shard_page_key(&HASH, 0)).await.unwrap().unwrap();
    assert!(page.len() > INLINE_PAGE_MAX);
    set_clock(&db, at).await;
    let d = claim(&e, 1).await;
    assert!(d[0].page.is_none());
    assert!(!delivery(&db, 0).await.inline);
}
