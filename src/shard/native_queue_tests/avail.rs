//! The availability entry carries what a claim of an untaken message needs
//! (docs/QUEUES-DESIGN.md §5.2, §6.1): the metadata as sent and, for a
//! small message, its sealed page, so the claim reads neither the metadata
//! row nor the page.
#![cfg(test)]
use super::{HASH, claim, clocked, msg, op, send_op};
use crate::crypto_page::queue_page_key as shard_page_key;
use crate::native_queue::rows::{MetaRow, decode_avail};
use crate::native_queue::{INLINE_PAGE_MAX, QueueConfig, keys};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_small_message_rides_in_its_availability_entry_with_its_metadata() {
    let (e, db, c) = clocked("nq-avail-inline", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("hello")])).await.unwrap();
    let page = db.get(shard_page_key(&HASH, 0)).await.unwrap().unwrap();
    let entry = db
        .get(keys::by_time(&HASH, keys::AVAIL, c, 0))
        .await
        .unwrap()
        .unwrap();
    assert!(
        entry.ends_with(&page),
        "the entry ends with the sealed page ({} bytes)",
        entry.len()
    );
    let meta = db
        .get(keys::by_offset(&HASH, keys::META, 0))
        .await
        .unwrap()
        .unwrap();
    let (carried, inline) = decode_avail(&entry).unwrap();
    assert_eq!(carried, MetaRow::decode(&meta).unwrap(), "metadata as sent");
    assert_eq!(inline, &page[..]);
    let d = claim(&e, 1).await;
    assert_eq!(
        d[0].page.as_deref(),
        Some(&page[..]),
        "the claim returns it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_of_an_untaken_message_never_reads_its_metadata_row() {
    let (e, db, _c) = clocked("nq-avail-no-meta", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    // Only the availability entry can tell the claim about the message.
    db.delete(keys::by_offset(&HASH, keys::META, 0))
        .await
        .unwrap();
    let d = claim(&e, 1).await;
    assert_eq!(d.len(), 1, "leased from the availability entry alone");
    let meta = db
        .get(keys::by_offset(&HASH, keys::META, 0))
        .await
        .unwrap()
        .map(|raw| MetaRow::decode(&raw).unwrap());
    assert_eq!(
        meta.map(|m| (m.taken, m.open)),
        Some((true, 1)),
        "the claim wrote the metadata row back, taken"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_rides_inline_up_to_the_limit_exactly_and_not_a_byte_past_it() {
    let (e, db, c) = clocked("nq-avail-limit", QueueConfig::default()).await;
    // One send of every body size around the limit; each is its own page.
    let text = noise(2 * INLINE_PAGE_MAX);
    let sizes = INLINE_PAGE_MAX * 3 / 4..INLINE_PAGE_MAX * 5 / 4;
    op(&e, send_op(sizes.map(|n| msg(&text[..n])).collect()))
        .await
        .unwrap();
    let (mut at_limit, mut past) = (false, false);
    for off in 0..u64::try_from(INLINE_PAGE_MAX / 2).unwrap() {
        let page = db.get(shard_page_key(&HASH, off)).await.unwrap().unwrap();
        let entry = db
            .get(keys::by_time(&HASH, keys::AVAIL, c, off))
            .await
            .unwrap()
            .unwrap();
        let inline = decode_avail(&entry).unwrap().1.to_vec();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_of_a_large_message_leaves_its_page_to_the_page_key() {
    let (e, db, _c) = clocked("nq-avail-large", QueueConfig::default()).await;
    op(&e, send_op(vec![msg(&noise(2 * INLINE_PAGE_MAX))]))
        .await
        .unwrap();
    let page = db.get(shard_page_key(&HASH, 0)).await.unwrap().unwrap();
    assert!(page.len() > INLINE_PAGE_MAX);
    assert!(claim(&e, 1).await[0].page.is_none());
}
