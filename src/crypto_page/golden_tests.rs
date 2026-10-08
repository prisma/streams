//! Golden vectors: the page key, a raw and a compressed page under a fixed
//! nonce, a stamped page from a derived key, and both row keys, pinned as
//! hex. The raw pages and the keys were reproduced byte for byte by an
//! independent implementation (stdlib HMAC-HKDF and OpenSSL's
//! AES-256-GCM-SIV over a header and body built from the format text,
//! analysis/layout5-proto/crosscheck_vectors.py and crosscheck_stamped.py);
//! the compressed pages were authenticated and zstd-CLI-decoded to the
//! format's body there. The compressed vectors also pin zstd's level-1
//! output at the locked version: a zstd upgrade may change the encode
//! bytes, never the decode.
#![cfg(test)]

use super::stamped;
use bytes::Bytes;

use super::body::build_body;
use super::header::{HeaderFields, encode_header};
use super::tests::{Opened, noise};
use super::{
    CheckedPage, PAGE_VER, PageCipher, PageLane, SealRecord, history_page_key, page_key,
    shard_page_key,
};
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

/// The fixture records' timestamp.
const TS_MS: i64 = 1_760_000_000_000;

fn lane() -> PageLane<'static> {
    PageLane {
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
    let page = cipher.seal_with_nonce(&lane(), FIRST, &stamped(TS_MS, records), NONCE);
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
        .map(|(record, offset)| (offset, TS_MS, record))
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

/// The stamped vector's lane: a stream key, epoch, UTF-8 routing key and
/// key version run through `derive_subkey` to the page key.
const STAMPED_KEY: [u8; 32] = [0x42; 32];
const STAMPED_EPOCH: [u8; 16] = [0x5e; 16];
const STAMPED_ROUTING_KEY: &str = "tenant/ü-key";
const STAMPED_KEY_VERSION: u32 = 0x0102_0304;
const STAMPED_SEGMENT: [u8; 16] = [0x6d; 16];
const STAMPED_NONCE: [u8; 12] = [0x7c; 12];
const STAMPED_FIRST: u64 = 1_000_000;
const STAMPED_TS: i64 = -1_234_567;
/// The deltas the records take in turn after the first: one, two, three
/// and four varint bytes, crossing every width boundary.
const STAMPED_STEPS: [i64; 8] = [0, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152];

const STAMPED_SUBKEY: &str = "929af616dbdcf749deb96efef102e75c6fd1c60c5c9f8862f76e484741432bc2";
const STAMPED_PAGE_KEY: &str = "93babc205ab19576352d583ed6201894d7036fbd026113d6e3029f0223ab7100";
/// The raw page's clear header, ver through ct_len: ver 6, first 1,000,000,
/// count 130 (two varint bytes), ts_ms -1,234,567, key version 0x01020304,
/// the 13-byte routing key, the nonce and ct_len.
const STAMPED_RAW_HEAD: &str = concat!(
    "0600000000000f42408201ffffffffffed297901020304000d74656e616e742fc3bc2d",
    "6b65797c7c7c7c7c7c7c7c7c7c7c7c0000442a",
);
const STAMPED_RAW_SHA256: &str = "7c060f5df8f00d7f1060ab5206d20f78fc8d8bfddb90fda8da96e07d57924e84";
/// The same records through the seal path: their tables compress, so the
/// writer stores them as version 7.
const STAMPED_SEALED_SHA256: &str =
    "92289707d2061d33ba2618b223b62cf7644f07cdd6462773060d59ad0241c7c9";

/// 130 records of 130 bytes zstd cannot shrink (`noise(i, 130)`), each
/// stamped `STAMPED_STEPS[i % 8]` ms after the one before, from STAMPED_TS.
fn stamped_payloads() -> Vec<(i64, Vec<u8>)> {
    let mut ts_ms = STAMPED_TS;
    (0..130u64)
        .zip(STAMPED_STEPS.iter().cycle())
        .map(|(index, step)| {
            if index > 0 {
                ts_ms = ts_ms.checked_add(*step).unwrap();
            }
            (ts_ms, noise(index, 130))
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex(&sha2::Sha256::digest(bytes))
}

/// A page of 130 records of 130 bytes under a key derived from a stream
/// key, with a two-byte count, two-byte lengths and timestamp deltas of one
/// to four bytes from a negative first timestamp, pinned raw (built body,
/// version 6) and as the seal path stores it (version 7); both open to
/// every record with its own timestamp.
#[test]
fn a_stamped_page_from_a_derived_key_is_pinned_and_decodes() {
    let key = crate::crypto::StreamKey(STAMPED_KEY);
    let subkey = crate::crypto::derive_subkey(
        &key,
        &STAMPED_EPOCH,
        STAMPED_ROUTING_KEY,
        STAMPED_KEY_VERSION,
    );
    assert_eq!(hex(&subkey), STAMPED_SUBKEY);
    assert_eq!(hex(&page_key(&subkey, &STAMPED_SEGMENT)), STAMPED_PAGE_KEY);
    let payloads = stamped_payloads();
    let records: Vec<SealRecord<'_>> = payloads
        .iter()
        .map(|(ts_ms, payload)| SealRecord {
            ts_ms: *ts_ms,
            payload,
        })
        .collect();
    let lane = PageLane {
        key_version: STAMPED_KEY_VERSION,
        routing_key: STAMPED_ROUTING_KEY,
    };
    let cipher = PageCipher::new(&subkey, &STAMPED_SEGMENT);
    let fields = HeaderFields {
        ver: PAGE_VER,
        first: STAMPED_FIRST,
        count: records.len(),
        ts_ms: STAMPED_TS,
        lane: &lane,
        nonce: STAMPED_NONCE,
    };
    let raw = cipher
        .seal_message(&fields, &build_body(&records).unwrap())
        .unwrap();
    let head = encode_header(&fields).unwrap().len() + 4;
    assert_eq!(hex(raw.get(..head).unwrap()), STAMPED_RAW_HEAD);
    assert_eq!(sha256_hex(&raw), STAMPED_RAW_SHA256);
    let sealed = cipher
        .seal_with_nonce(&lane, STAMPED_FIRST, &records, STAMPED_NONCE)
        .unwrap();
    assert_eq!(sha256_hex(&sealed.bytes), STAMPED_SEALED_SHA256);
    let last = STAMPED_FIRST + 129;
    assert_eq!(sealed.last, last);
    let want: Vec<Opened> = payloads
        .iter()
        .zip(STAMPED_FIRST..)
        .map(|((ts_ms, payload), offset)| (offset, *ts_ms, payload.clone()))
        .collect();
    for (bytes, compressed) in [(raw, false), (sealed.bytes, true)] {
        let page = CheckedPage::admit(Bytes::from(bytes), last).unwrap();
        assert_eq!(page.is_compressed(), compressed);
        let opened: Vec<Opened> = cipher
            .open(&page)
            .unwrap()
            .records()
            .map(|record| (record.offset, record.ts_ms, record.payload.to_vec()))
            .collect();
        assert_eq!(opened, want);
    }
}
