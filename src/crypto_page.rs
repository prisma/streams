#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Layout 5 page codec. A page is one append request's records of one
//! routing key and key version, cut at `PAGE_TARGET_PLAINTEXT`, compressed
//! together when that pays and encrypted once (AES-256-GCM-SIV) under a page
//! key derived from the routing key's subkey. Pages are the stored unit only:
//! the wire keeps the v4/v5 frames of `crate::crypto`.
//!
//! ```text
//! [ver u8: 6 raw | 7 zstd-1][first u64][count varint][ts_ms i64]
//! [key_version u32][rk_len u16][routing key][nonce 12]
//! [ct_len u32][ciphertext || tag 16]
//!
//! body, the plaintext (compressed when ver is 7):
//!   count x record length         varint
//!   count x timestamp delta (ms)  varint, after ts_ms; all 0 in layout 5
//!   payloads, concatenated
//!
//! AAD      = segment identity (16 B) || header bytes from ver through nonce
//! page key = HKDF-SHA256(salt = segment identity, ikm = routing-key subkey,
//!                        info = "prisma-streams/page/v6/aes-256-gcm-siv")
//! row key  = hash16 || 'p' || last offset              (shard log)
//!            route16 || inc16 || 'p' || last offset    (history)
//! ```
//!
//! Integers are big-endian. A varint is minimal LEB128: seven bits per byte,
//! least significant group first, and no encoding has a redundant zero group,
//! so every page has exactly one byte string. `ct_len` counts the ciphertext
//! and its tag. A page is admitted without its key (`CheckedPage`) and
//! publishes nothing until it authenticates, decompresses within its cap and
//! its tables parse exactly (`PageCipher::open`).

use aes_gcm_siv::Aes256GcmSiv;
use aes_gcm_siv::aead::KeyInit;
use hkdf::Hkdf;
use sha2::Sha256;

use crate::crypto::{KEY_LEN, MAX_RECORD_PLAINTEXT, RouteHash, SegmentHash};

mod body;
mod header;
mod open;
mod seal;

pub(crate) use header::CheckedPage;
pub(crate) use open::OpenedPage;

/// Version byte of a page whose body is stored raw.
pub(crate) const PAGE_VER: u8 = 6;
/// Version byte of a page whose body is zstd level 1 (compress-then-encrypt).
pub(crate) const PAGE_VER_Z: u8 = 7;
/// The row-key tag between a page row's namespace and its last offset.
pub(crate) const PAGE_TAG: u8 = b'p';
/// The body cap of a page that holds more than one record, and the size at
/// which a request is cut into pages.
pub(crate) const PAGE_TARGET_PLAINTEXT: usize = 64 << 10;
/// The most records one page holds, so a scan from the key of offset x meets
/// the page holding x before the key of x + PAGE_MAX_RECORDS.
pub(crate) const PAGE_MAX_RECORDS: usize = 4096;
/// Bodies below this size are stored raw without a compression attempt.
const PAGE_COMPRESS_MIN_BYTES: usize = 256;
/// The table bytes of one record: a length below 2^28 (four varint bytes)
/// and any timestamp delta (ten).
const RECORD_TABLE_MAX: usize = 14;
/// The body cap of a single-record page: one record at the record cap.
const SINGLE_RECORD_BODY_MAX: usize = MAX_RECORD_PLAINTEXT + RECORD_TABLE_MAX;
const PAGE_KEY_INFO: &[u8] = b"prisma-streams/page/v6/aes-256-gcm-siv";
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// The most body bytes a page of `count` records may hold, before
/// compression: what admission bounds its ciphertext by and what opening
/// decompresses at most.
pub(crate) const fn body_cap(count: usize) -> usize {
    if count == 1 {
        SINGLE_RECORD_BODY_MAX
    } else {
        PAGE_TARGET_PLAINTEXT
    }
}

/// The last offset of a page of `count` records starting at `first`, or
/// None when there is no record or the offset space ends inside the page.
pub(crate) fn last_offset(first: u64, count: usize) -> Option<u64> {
    let extra = u64::try_from(count.checked_sub(1)?).ok()?;
    first.checked_add(extra)
}

/// Shard-log row key of the page whose last offset is `last`.
pub(crate) fn shard_page_key(hash: &[u8; 16], last: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.extend_from_slice(&shard_page_prefix(hash));
    key.extend_from_slice(&last.to_be_bytes());
    key
}

/// The canonical prefix of a segment's shard-log page keys: its hash and
/// the page tag. `CheckedPage::from_row` admits a row against it.
pub(crate) fn shard_page_prefix(hash: &[u8; 16]) -> [u8; 17] {
    let mut prefix = [PAGE_TAG; 17];
    let (namespace, _) = prefix.split_at_mut(16);
    namespace.copy_from_slice(hash);
    prefix
}

/// History row key of the page whose last offset is `last`.
pub(crate) fn history_page_key(route: RouteHash, inc: SegmentHash, last: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(41);
    key.extend_from_slice(&route.0);
    key.extend_from_slice(&inc.0);
    key.push(PAGE_TAG);
    key.extend_from_slice(&last.to_be_bytes());
    key
}

/// The lane fields every page of one request carries in its clear header.
pub(crate) struct PageLane<'a> {
    pub(crate) ts_ms: i64,
    pub(crate) key_version: u32,
    pub(crate) routing_key: &'a str,
}

/// One sealed page of a request and the row key offset it is stored under.
#[derive(Debug)]
pub(crate) struct SealedPage {
    pub(crate) last: u64,
    pub(crate) bytes: Vec<u8>,
}

/// Why a request could not be sealed. Nothing of it was sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SealError {
    Empty,
    TooManyRecords,
    RecordTooLarge,
    PageTooLarge,
    RoutingKeyTooLong,
    OffsetOverflow,
    Cipher,
}

/// Why a stored page was refused before decryption. Every refusal is
/// corruption: the writer never produces such a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageCorruption {
    /// The prefix the caller selected is not a page namespace.
    RowTag,
    KeyWidth,
    Namespace,
    Version,
    Count,
    /// A header field, or the ciphertext its length names, is cut short.
    Truncated,
    RoutingKey,
    /// The ciphertext is shorter than its tag.
    Tag,
    /// The ciphertext is longer than the body cap of its record count.
    Oversized,
    Trailing,
    OffsetOverflow,
    Offset {
        key: u64,
        page: u64,
    },
}

/// Why an admitted page did not open. Nothing of it is returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpenError {
    Authentication,
    Decompression,
    BodyTooLarge,
    Body(BodyError),
}

/// Why an authenticated body did not parse exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyError {
    LengthTable,
    DeltaTable,
    Timestamp,
    PayloadShort,
    Trailing,
}

/// One page key schedule per (routing-key subkey, segment), reused for
/// every page of that lane that is sealed or opened. The segment identity
/// salts the key and leads the AAD, so it is bound twice.
pub(crate) struct PageCipher {
    cipher: Aes256GcmSiv,
    segment: [u8; 16],
}

impl PageCipher {
    pub(crate) fn new(subkey: &[u8; KEY_LEN], segment: &[u8; 16]) -> Self {
        Self {
            cipher: Aes256GcmSiv::new((&page_key(subkey, segment)).into()),
            segment: *segment,
        }
    }

    fn aad(&self, header: &[u8]) -> Vec<u8> {
        [self.segment.as_slice(), header].concat()
    }
}

/// A domain of its own: no page key equals a frame key of the same subkey
/// and segment.
#[expect(
    clippy::expect_used,
    reason = "page_key; the output is a fixed 32 bytes, far below HKDF-SHA256's 8,160-byte limit, so expansion cannot fail; a fallible derivation would add an error path no input reaches"
)]
fn page_key(subkey: &[u8; KEY_LEN], segment: &[u8; 16]) -> [u8; KEY_LEN] {
    let hk = Hkdf::<Sha256>::new(Some(segment), subkey);
    let mut key = [0; KEY_LEN];
    hk.expand(PAGE_KEY_INFO, &mut key)
        .expect("fixed HKDF length");
    key
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
mod tamper_tests;

#[cfg(test)]
mod golden_tests;

#[cfg(test)]
mod properties;

#[cfg(kani)]
mod proofs;
