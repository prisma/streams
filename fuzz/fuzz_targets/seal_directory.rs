#![no_main]
//! The seal directory's `'K'` row codec (`spill::directory::row`), which
//! every shard open reads back from the log to rebuild the directory that
//! sealed-range reads consult. The first input byte picks a mode:
//!
//! - decode: the rest is a row value as the log returned it; whatever
//!   decodes must satisfy the format, and `encode_row` must write exactly
//!   the same bytes back from the decoded entries (the codec is canonical);
//! - encode: the rest describes up to eight chunks with their key
//!   summaries and anchors, within the encoder's domain; their row must
//!   decode back to exactly those entries, and a truncated or extended row
//!   must be refused.
//!
//! A decoded row's chunks tile the object from offset 0, each covers
//! `first..=last`, its anchors lie at or after `first`, and its summary
//! names 1 to `MAX_SUMMARY_KEYS` fingerprints, sorted and distinct, or a
//! bloom of 1 to `MAX_BLOOM_BYTES` bytes. Decoding never panics, and its
//! allocations are bounded whatever the lengths claim: the entry vector's
//! reservation is capped at 4,096 entries, and everything else grows only
//! from bytes that are present.
//!
//! Compiles the actual private production owners by path: no copied codec
//! or verification-only public API. The directory key parser and the
//! summary builder stay in `spill::directory`, which needs the shard log's
//! keyspace and the page decoder, outside this target; the bloom's
//! never-excludes-a-held-key property is KANI-099's.
#[path = "../../src/spill/crc32c.rs"]
mod crc32c;
#[allow(
    dead_code,
    reason = "seal directory fuzz target; compile the actual segment object owner for its chunk types; its encoder and decoder have their own target"
)]
#[path = "../../src/spill/object.rs"]
mod object;
#[allow(
    dead_code,
    reason = "seal directory fuzz target; compile the actual row codec; the bloom probes and anchor lookup serve the directory owner and KANI-099"
)]
#[path = "../../src/spill/directory/row.rs"]
mod row;
#[path = "../../src/crypto_page/varint.rs"]
mod varint;

/// The service crate's paths the included owners import through:
/// `crate::crypto_page`'s varint re-export and `crate::spill::object`.
mod crypto_page {
    pub(crate) use super::varint::{get_varint, put_varint};
}
mod spill {
    pub(crate) use super::object;
}

use object::{Anchor, ChunkEntry, EncodedChunk, ObjectId};
use row::{DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS, decode_row, encode_row};

const OBJECT: ObjectId = ObjectId { epoch: 3, seq: 9 };

libfuzzer_sys::fuzz_target!(|raw: &[u8]| {
    let Some((&mode, rest)) = raw.split_first() else {
        return;
    };
    if mode % 2 == 0 {
        check_decode(rest);
    } else {
        check_encode(rest);
    }
});

/// Decode `raw`; whatever decodes satisfies the format and encodes back to
/// exactly `raw`.
fn check_decode(raw: &[u8]) {
    let Some(rows) = decode_row(OBJECT, raw) else {
        return;
    };
    check_rows(&rows);
    let chunks: Vec<EncodedChunk> = rows.iter().map(|(hash, e)| chunk_of(*hash, e)).collect();
    let pairs: Vec<(&EncodedChunk, &KeySummary)> = chunks
        .iter()
        .zip(rows.iter().map(|(_, e)| &e.keys))
        .collect();
    assert_eq!(encode_row(&pairs), raw, "the codec is canonical");
}

/// The format's invariants over a row `decode_row` accepted.
fn check_rows(rows: &[([u8; 16], DirEntry)]) {
    let mut at = Some(0u64);
    for (_, entry) in rows {
        assert_eq!(entry.object, OBJECT, "every entry names the row's object");
        assert_eq!(Some(entry.chunk_off), at, "the chunks tile the object");
        at = entry.chunk_off.checked_add(u64::from(entry.chunk_len));
        assert!(entry.first <= entry.last, "a chunk covers first..=last");
        for anchor in &entry.anchors {
            assert!(
                anchor.offset >= entry.first,
                "an anchor lies at or after first"
            );
        }
        match &entry.keys {
            KeySummary::Keys(keys) => {
                assert!((1..=MAX_SUMMARY_KEYS).contains(&keys.len()));
                assert!(
                    keys.is_sorted_by(|a, b| a < b),
                    "fingerprints are sorted and distinct"
                );
            }
            KeySummary::Bloom(bloom) => assert!((1..=MAX_BLOOM_BYTES).contains(&bloom.len())),
        }
    }
    assert!(at.is_some(), "the chunks end within u64");
}

/// The chunk an entry was written from (its stored bytes and timestamp are
/// not in the row).
fn chunk_of(hash: [u8; 16], entry: &DirEntry) -> EncodedChunk {
    EncodedChunk {
        entry: ChunkEntry {
            hash,
            first: entry.first,
            last: entry.last,
            offset: entry.chunk_off,
            len: entry.chunk_len,
            page_count: entry.page_count,
            stored: 0,
            max_ts: 0,
        },
        anchors: entry.anchors.clone(),
    }
}

/// Encode the chunks the input describes; their row decodes back to exactly
/// their entries, and no prefix or extension of it decodes.
fn check_encode(raw: &[u8]) {
    let mut input = Input(raw);
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    let count = input.byte().map_or(0, |b| b % 9);
    for _ in 0..count {
        let Some((chunk, keys)) = input.chunk(offset) else {
            break;
        };
        offset += u64::from(chunk.entry.len);
        chunks.push((chunk, keys));
    }
    let pairs: Vec<(&EncodedChunk, &KeySummary)> = chunks.iter().map(|(c, k)| (c, k)).collect();
    let row = encode_row(&pairs);
    let expected: Vec<([u8; 16], DirEntry)> = chunks
        .iter()
        .map(|(c, k)| (c.entry.hash, DirEntry::of(OBJECT, c, k.clone())))
        .collect();
    assert_eq!(
        decode_row(OBJECT, &row),
        Some(expected),
        "a row decodes back to its entries"
    );
    for cut in [1, 2, row.len() / 2] {
        if let Some(prefix) = row.len().checked_sub(cut).and_then(|n| row.get(..n)) {
            assert_eq!(
                decode_row(OBJECT, prefix),
                None,
                "a truncated row is refused"
            );
        }
    }
    let mut extended = row;
    extended.push(input.byte().unwrap_or_default());
    assert_eq!(
        decode_row(OBJECT, &extended),
        None,
        "trailing bytes are refused"
    );
}

/// The encode-mode description: a little-endian reader that yields nothing
/// once the input runs out.
struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn bytes<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn byte(&mut self) -> Option<u8> {
        self.bytes::<1>().map(|[b]| b)
    }

    fn u32(&mut self) -> Option<u32> {
        self.bytes().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.bytes().map(u64::from_le_bytes)
    }

    /// One chunk at `offset` within the encoder's domain: `last` at or
    /// after `first`, anchors at or after `first`, and a well-formed summary.
    fn chunk(&mut self, offset: u64) -> Option<(EncodedChunk, KeySummary)> {
        let hash = self.bytes::<16>()?;
        let first = self.u64()?;
        let last = first.saturating_add(self.u64()?);
        let len = self.u32()?;
        let page_count = self.u32()?;
        let keys = self.summary()?;
        let anchors = (0..self.byte()? % 5)
            .map_while(|_| {
                Some(Anchor {
                    page: self.u32()?,
                    offset: first.saturating_add(self.u64()?),
                    byte: self.u32()?,
                })
            })
            .collect();
        let entry = ChunkEntry {
            hash,
            first,
            last,
            offset,
            len,
            page_count,
            stored: 0,
            max_ts: 0,
        };
        Some((EncodedChunk { entry, anchors }, keys))
    }

    /// A summary as `KeySummary::of` builds one: 1 to `MAX_SUMMARY_KEYS`
    /// sorted distinct fingerprints, or a bloom of 1 to `MAX_BLOOM_BYTES`.
    fn summary(&mut self) -> Option<KeySummary> {
        let shape = self.byte()?;
        if shape % 2 == 0 {
            let n = usize::from(shape / 2) % MAX_SUMMARY_KEYS + 1;
            let mut keys: Vec<u64> = (0..n).map_while(|_| self.u64()).collect();
            keys.sort_unstable();
            keys.dedup();
            return (!keys.is_empty()).then_some(KeySummary::Keys(keys));
        }
        let len = usize::from(self.byte()?) % MAX_BLOOM_BYTES + 1;
        let (bloom, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(KeySummary::Bloom(bloom.to_vec()))
    }
}
