//! The seal directory's row codec (layout 6, DESIGN-6A §2.3): one `'K'`
//! row's value, written by `encode_row` and read back by `decode_row`, and
//! a chunk's key summary with its bloom probes. A leaf: it needs only the
//! segment object's chunk types and the page varint, so the seal-directory
//! fuzz target compiles it unchanged (`fuzz/fuzz_targets/seal_directory.rs`).
//!
//! Value (minimal LEB128 varints): `ver:u8=1 chunks:varint`, then per chunk,
//! in object order (each chunk starts where the previous one ended, so no
//! offset is stored): `hash[16] first:varint (last − first):varint
//! len:varint page_count:varint summary anchors:varint (page:varint
//! (offset − first):varint byte:varint)*`. The summary names the chunk's
//! routing keys by `crypto::RoutingKeyHash`, the hash package 5's per-run blooms
//! take: `n:varint` then `n` fingerprints (each key's first 8 hash bytes,
//! u64 BE, sorted, distinct) when the chunk holds 1 to `MAX_SUMMARY_KEYS`
//! keys; or `0`, `len:varint` and a `len`-byte bloom of about
//! `BLOOM_BITS_PER_KEY` bits per key, at most `MAX_BLOOM_BYTES`, probed
//! `BLOOM_HASHES` times by double hashing the key's two 8-byte halves, so a
//! chunk of many keys cannot bloat the directory. A keyed read skips a chunk
//! the summary excludes without a GET, and filters a fetched chunk's pages
//! by their headers (a bloom's false positive costs one GET that serves
//! nothing). About 31-35 B per chunk of one key's single-record pages.

use crate::crypto_page::{get_varint, put_varint};
use crate::spill::object::{Anchor, EncodedChunk, ObjectId};

pub(super) const VERSION: u8 = 1;
/// The most routing keys a chunk's summary names one by one.
pub(crate) const MAX_SUMMARY_KEYS: usize = 16;

/// A chunk's routing keys: their fingerprints, sorted and distinct, or a
/// bloom of them when there are more than `MAX_SUMMARY_KEYS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeySummary {
    Keys(Vec<u64>),
    Bloom(Vec<u8>),
}

/// Bits per key a chunk's bloom aims for.
pub(super) const BLOOM_BITS_PER_KEY: usize = 10;
/// The most bytes one chunk's bloom takes.
pub(crate) const MAX_BLOOM_BYTES: usize = 256;
/// Probes per key.
pub(super) const BLOOM_HASHES: u64 = 7;

/// A key hash's two 8-byte halves, big-endian.
pub(super) fn halves(hash: &[u8; 16]) -> (u64, u64) {
    let (head, tail) = hash.split_at(8);
    let word = |bytes: &[u8]| u64::from_be_bytes(bytes.try_into().unwrap_or([0; 8]));
    (word(head), word(tail))
}

/// The bits a key sets in a bloom of `len` bytes: `h1 + i·h2` for
/// `i < BLOOM_HASHES`, modulo its bit count.
pub(super) fn probes(hash: &[u8; 16], len: usize) -> impl Iterator<Item = usize> {
    let (h1, h2) = halves(hash);
    let bits = u64::try_from(len * 8).unwrap_or(u64::MAX).max(1);
    (0..BLOOM_HASHES)
        .map(move |i| usize::try_from(h1.wrapping_add(i.wrapping_mul(h2)) % bits).unwrap_or(0))
}

/// Set a key hash's probe bits in `bloom`.
pub(super) fn set_bits(bloom: &mut [u8], hash: &[u8; 16]) {
    set_probe_bits(bloom, probes(hash, bloom.len()));
}

/// Set each probe bit in `bloom` (KANI-099 proves it over any probes inside
/// the bloom, apart from where `probes` puts them).
pub(super) fn set_probe_bits(bloom: &mut [u8], probes: impl Iterator<Item = usize>) {
    for bit in probes {
        if let Some(byte) = bloom.get_mut(bit / 8) {
            *byte |= 1 << (bit % 8);
        }
    }
}

/// Whether every probe bit of a key hash is set in `bloom`: true for every
/// hash `set_bits` set (KANI-099), so a bloom never excludes a held key.
pub(super) fn holds_bits(bloom: &[u8], hash: &[u8; 16]) -> bool {
    holds_probe_bits(bloom, probes(hash, bloom.len()))
}

/// Whether every probe bit is set in `bloom` (KANI-099, as `set_probe_bits`).
pub(super) fn holds_probe_bits(bloom: &[u8], mut probes: impl Iterator<Item = usize>) -> bool {
    probes.all(|bit| {
        bloom
            .get(bit / 8)
            .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
    })
}

/// One sealed chunk as the directory holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DirEntry {
    pub(crate) object: ObjectId,
    pub(crate) chunk_off: u64,
    pub(crate) chunk_len: u32,
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) page_count: u32,
    pub(crate) keys: KeySummary,
    pub(crate) anchors: Vec<Anchor>,
}

impl DirEntry {
    pub(crate) fn of(object: ObjectId, chunk: &EncodedChunk, keys: KeySummary) -> Self {
        Self {
            object,
            chunk_off: chunk.entry.offset,
            chunk_len: chunk.entry.len,
            first: chunk.entry.first,
            last: chunk.entry.last,
            page_count: chunk.entry.page_count,
            keys,
            anchors: chunk.anchors.clone(),
        }
    }

    /// The last anchor at or before `offset`, if any.
    pub(crate) fn start_for(&self, offset: u64) -> Option<Anchor> {
        self.anchors
            .iter()
            .rev()
            .find(|a| a.offset <= offset)
            .copied()
    }
}

/// Encode one seal's directory row from its chunks and their key
/// summaries, in object order.
pub(crate) fn encode_row(chunks: &[(&EncodedChunk, &KeySummary)]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 35 * chunks.len());
    v.push(VERSION);
    put_varint(&mut v, chunks.len() as u64);
    for (c, keys) in chunks {
        v.extend_from_slice(&c.entry.hash);
        put_varint(&mut v, c.entry.first);
        put_varint(&mut v, c.entry.last - c.entry.first);
        put_varint(&mut v, u64::from(c.entry.len));
        put_varint(&mut v, u64::from(c.entry.page_count));
        match keys {
            KeySummary::Bloom(bloom) => {
                put_varint(&mut v, 0);
                put_varint(&mut v, bloom.len() as u64);
                v.extend_from_slice(bloom);
            }
            KeySummary::Keys(keys) => {
                put_varint(&mut v, keys.len() as u64);
                for key in keys {
                    v.extend_from_slice(&key.to_be_bytes());
                }
            }
        }
        put_varint(&mut v, c.anchors.len() as u64);
        for a in &c.anchors {
            put_varint(&mut v, u64::from(a.page));
            put_varint(&mut v, a.offset - c.entry.first);
            put_varint(&mut v, u64::from(a.byte));
        }
    }
    v
}

/// Decode one directory row into (stream, entry) pairs; `None` for any
/// byte sequence `encode_row` does not write.
pub(crate) fn decode_row(object: ObjectId, raw: &[u8]) -> Option<Vec<([u8; 16], DirEntry)>> {
    let (&ver, mut input) = raw.split_first()?;
    if ver != VERSION {
        return None;
    }
    let n = get_varint(&mut input)?;
    let mut out = Vec::with_capacity(usize::try_from(n.min(4096)).ok()?);
    let mut off = 0u64;
    for _ in 0..n {
        let (hash, rest) = input.split_first_chunk::<16>()?;
        input = rest;
        let first = get_varint(&mut input)?;
        let last = first.checked_add(get_varint(&mut input)?)?;
        let len = u32::try_from(get_varint(&mut input)?).ok()?;
        let page_count = u32::try_from(get_varint(&mut input)?).ok()?;
        let keys = decode_keys(&mut input)?;
        let anchors_n = get_varint(&mut input)?;
        let mut anchors = Vec::new();
        for _ in 0..anchors_n {
            let page = u32::try_from(get_varint(&mut input)?).ok()?;
            let offset = first.checked_add(get_varint(&mut input)?)?;
            let byte = u32::try_from(get_varint(&mut input)?).ok()?;
            anchors.push(Anchor { page, offset, byte });
        }
        let entry = DirEntry {
            object,
            chunk_off: off,
            chunk_len: len,
            first,
            last,
            page_count,
            keys,
            anchors,
        };
        out.push((*hash, entry));
        off = off.checked_add(u64::from(len))?;
    }
    input.is_empty().then_some(out)
}

fn decode_keys(input: &mut &[u8]) -> Option<KeySummary> {
    let n = usize::try_from(get_varint(input)?).ok()?;
    if n == 0 {
        let len = usize::try_from(get_varint(input)?).ok()?;
        if !(1..=MAX_BLOOM_BYTES).contains(&len) {
            return None;
        }
        let (bloom, rest) = input.split_at_checked(len)?;
        *input = rest;
        return Some(KeySummary::Bloom(bloom.to_vec()));
    }
    if n > MAX_SUMMARY_KEYS {
        return None;
    }
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        let (key, rest) = input.split_first_chunk::<8>()?;
        *input = rest;
        keys.push(u64::from_be_bytes(*key));
    }
    keys.is_sorted_by(|a, b| a < b)
        .then_some(KeySummary::Keys(keys))
}
