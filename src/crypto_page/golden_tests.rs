//! Golden vectors: the page key, a raw and a compressed page under a fixed
//! nonce, and both row keys, pinned as hex. The raw page and the key were
//! reproduced byte for byte by an independent implementation (OpenSSL's
//! HKDF and AES-256-GCM-SIV over a header built from the format text).
//! The compressed vector also pins zstd's level-1 output at the locked
//! version: a zstd upgrade may change the encode bytes, never the decode.
#![cfg(test)]

use bytes::Bytes;

use super::tests::Opened;
use super::{CheckedPage, PageCipher, PageLane, history_page_key, page_key, shard_page_key};
use crate::crypto::{RouteHash, SegmentHash, hex, unhex};

const SUBKEY: [u8; 32] = [0x11; 32];
const SEGMENT: [u8; 16] = [0x22; 16];
const NONCE: [u8; 12] = [0x33; 12];
const FIRST: u64 = 4096;

const PAGE_KEY: &str = "ea38695bdd3a848131c9ae0ae925ef536c65eddd5324095428236e4be2d7000e";
// ver 6, first 4096, count 2, ts_ms, key version 1, "rk", nonce, ct_len 31,
// then the 15-byte body (05 06 00 00 "hello" "world!") encrypted, and its tag.
const RAW_PAGE: &str = concat!(
    "0600000000000010000200000199c82cc000000000010002726b333333333333",
    "3333333333330000001f53ae0fedbf3de8d4d31f7ff007f3e26a5b97169ed227",
    "45f1de9a3fec1e27d0",
);
// ver 7, count 6: a 366-byte body stored as 98 zstd bytes, and its tag.
const COMPRESSED_PAGE: &str = concat!(
    "0700000000000010000600000199c82cc000000000010002726b333333333333",
    "33333333333300000072c878f82220f0264b7504a7c8b2d4a66c79b9e6373de0",
    "305ee3e77fc92738b656d14d0db7fe3bbd6fd0ec4cc50b22e18eb2ed9546b003",
    "7af326a64dee7459c47ce2e9a66d656d883adeb692ba10b7c5c75a9b5c9a9f01",
    "93245fbf899506b37dfc9d75d0426d43a0a17f27f73923a31aad784c",
);

fn lane() -> PageLane<'static> {
    PageLane {
        ts_ms: 1_760_000_000_000,
        key_version: 1,
        routing_key: "rk",
    }
}

fn raw_records() -> Vec<Vec<u8>> {
    vec![b"hello".to_vec(), b"world!".to_vec()]
}

fn compressed_records() -> Vec<Vec<u8>> {
    (0..6)
        .map(|index| {
            format!(r#"{{"id":{index},"event":"page_view","path":"/products/7","ok":true}}"#)
                .into_bytes()
        })
        .collect()
}

/// Seal `records` from FIRST under the fixed nonce.
fn sealed(records: &[Vec<u8>]) -> String {
    let cipher = PageCipher::new(&SUBKEY, &SEGMENT);
    let page = cipher.seal_with_nonce(&lane(), FIRST, records, NONCE);
    hex(&page.unwrap().bytes)
}

/// Open a pinned page as stored under its last offset.
fn opened(pinned: &str, last: u64) -> (bool, Vec<Opened>) {
    let page = CheckedPage::admit(Bytes::from(unhex(pinned).unwrap()), last).unwrap();
    let records = PageCipher::new(&SUBKEY, &SEGMENT).open(&page).unwrap();
    let records = records
        .records()
        .map(|record| (record.offset, record.ts_ms, record.payload.to_vec()))
        .collect();
    (page.is_compressed(), records)
}

fn expected(records: Vec<Vec<u8>>) -> Vec<Opened> {
    records
        .into_iter()
        .zip(FIRST..)
        .map(|(record, offset)| (offset, lane().ts_ms, record))
        .collect()
}

#[test]
fn the_page_key_is_pinned() {
    assert_eq!(hex(&page_key(&SUBKEY, &SEGMENT)), PAGE_KEY);
}

#[test]
fn a_raw_page_is_pinned_and_decodes() {
    assert_eq!(sealed(&raw_records()), RAW_PAGE);
    assert_eq!(opened(RAW_PAGE, 4097), (false, expected(raw_records())));
}

#[test]
fn a_compressed_page_is_pinned_and_decodes() {
    assert_eq!(sealed(&compressed_records()), COMPRESSED_PAGE);
    assert_eq!(
        opened(COMPRESSED_PAGE, 4101),
        (true, expected(compressed_records()))
    );
}

#[test]
fn row_keys_are_pinned() {
    let last = 0x0102_0304_0506_0708;
    assert_eq!(
        hex(&shard_page_key(&[0xaa; 16], last)),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa700102030405060708"
    );
    let history = history_page_key(RouteHash([0xbb; 16]), SegmentHash([0xcc; 16]), last);
    assert_eq!(
        hex(&history),
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccccc670102030405060708"
    );
}
