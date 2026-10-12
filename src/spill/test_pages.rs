//! Pages for the spill tests: real layout 5 pages (sealed with a fixed key,
//! so `CheckedPage::admit` checks every decoded page against its offsets),
//! or opaque byte slices of one shared pool (for the size-driven tests).
use bytes::Bytes;
use rand::Rng;

use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{PageCipher, PageLane, stamped};

use super::object::ChunkInput;

/// One real page of `records` starting at `first`, stamped `ts`: its last
/// offset and stored bytes.
pub(crate) fn sealed_page(hash: &[u8; 16], first: u64, records: &[Vec<u8>], ts: i64) -> (u64, Bytes) {
    let subkey = derive_subkey(&StreamKey([7; 32]), hash, "", 1);
    let lane = PageLane {
        key_version: 1,
        routing_key: "",
    };
    let page = PageCipher::new(&subkey, hash)
        .seal(&lane, first, &stamped(ts, records))
        .expect("seal");
    (page.last, Bytes::from(page.bytes))
}

/// `n` real pages from `first`, each of 1..=`max_records` records of
/// 1..=`max_len` bytes.
pub(crate) fn sealed_pages(
    rng: &mut impl Rng,
    hash: &[u8; 16],
    first: u64,
    n: usize,
    max_records: usize,
    max_len: usize,
) -> Vec<(u64, Bytes)> {
    let mut out = Vec::with_capacity(n);
    let mut next = first;
    for _ in 0..n {
        let count = rng.random_range(1..=max_records);
        let records: Vec<Vec<u8>> = (0..count)
            .map(|_| {
                let mut r = vec![0u8; rng.random_range(1..=max_len)];
                rng.fill(&mut r[..]);
                r
            })
            .collect();
        let (last, page) = sealed_page(hash, next, &records, 1_000 + next as i64);
        out.push((last, page));
        next = last + 1;
    }
    out
}

/// A pool of random bytes the opaque pages are sliced from.
pub(crate) fn pool(rng: &mut impl Rng, len: usize) -> Bytes {
    let mut v = vec![0u8; len];
    rng.fill(&mut v[..]);
    Bytes::from(v)
}

/// Opaque pages from `first`: `lens[i]` bytes and `counts[i]` records each,
/// sliced from `pool` starting at `*at` (wrapping).
pub(crate) fn opaque_pages(pool: &Bytes, at: &mut usize, first: u64, pages: &[(u64, usize)]) -> Vec<(u64, Bytes)> {
    let mut out = Vec::with_capacity(pages.len());
    let mut next = first;
    for &(count, len) in pages {
        if *at + len > pool.len() {
            *at = 0;
        }
        out.push((next + count - 1, pool.slice(*at..*at + len)));
        *at += len;
        next += count;
    }
    out
}

/// `chunks` opaque chunks of about `total` page bytes together, in pages of
/// about `page_len` bytes holding `records` records each.
pub(crate) fn opaque_object(
    rng: &mut impl Rng,
    pool: &Bytes,
    chunks: usize,
    total: usize,
    page_len: usize,
    records: u64,
) -> Vec<ChunkInput> {
    let per_chunk = total / chunks;
    let mut at = 0usize;
    (0..chunks)
        .map(|i| {
            let mut hash = [0u8; 16];
            rng.fill(&mut hash[..]);
            let mut lens = Vec::new();
            let mut left = per_chunk;
            while left > 0 {
                let len = left.min(page_len);
                lens.push((records, len));
                left -= len;
            }
            let first = rng.random_range(0..1u64 << 40);
            ChunkInput {
                hash,
                first,
                max_ts: 1_700_000_000_000 + i as i64,
                pages: opaque_pages(pool, &mut at, first, &lens),
            }
        })
        .collect()
}
