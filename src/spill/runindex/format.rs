//! The run-index object's codec (layout 6, DESIGN-6A §2.3): the immutable
//! per-shard object a fold writes from one fold period's seal directory
//! rows, and a merge writes from several such objects. A leaf: it needs
//! only the CRC-32C, the page varint and the directory's entry types, so the
//! run-index fuzz target compiles it unchanged
//! (`fuzz/fuzz_targets/run_index.rs`).
//!
//! Layout (fixed-width integers little-endian, varints minimal LEB128):
//!
//! ```text
//! object  := run* footer trailer
//! run     := count:varint entry* crc:u32           CRC-32C of the run before it
//! entry   := epoch:varint seq:varint chunk_off:varint chunk_len:varint
//!            first:varint (last − first):varint page_count:varint
//!            summary anchors
//!   summary := n:varint fp:u64be×n                  1 ≤ n ≤ MAX_SUMMARY_KEYS, sorted, distinct
//!            | 0 len:varint bloom[len]              1 ≤ len ≤ MAX_BLOOM_BYTES
//!   anchors := n:varint (page:varint (offset − first):varint byte:varint)×n
//! footer  := (hash[16] run_off:u64 run_len:u32 first:u64 last:u64
//!             entries:u32 flags:u8 bloom_len:u16 bloom[bloom_len])×runs
//!   flags   := bit 0 `DENSE`: the run's entries tile `first..=last` with no
//!              gap (each starts right after the previous one's last)
//! trailer (40 B) := footer_off:u64 footer_len:u32 runs:u32 entries:u64
//!                   version:u16 level:u8 flags:u8 footer_crc:u32
//!                   trailer_crc:u32 magic[4] = "L6ri"
//! ```
//!
//! A run holds one stream's chunks (the seal directory's entries, each
//! naming its segment object), sorted by first offset with first and last
//! offsets both strictly increasing: no entry contains another (a fold and
//! a merge drop an entry a consolidated chunk covers). Entries may leave
//! gaps (a consolidated chunk folds after the chunks it copies) and may
//! overlap where a consolidation spans part of another; readers resolve
//! both (`super::tiling`). A keyed read may skip a dense run its bloom
//! excludes as a whole, without a GET; a run with gaps it must fetch, since
//! another object's chunks fill them. Runs follow each other from byte 0, sorted by
//! stream hash, strictly; the footer names each one with its offsets and
//! its routing-key bloom.
//!
//! The run bloom is over the routing-key fingerprints the entries' summaries
//! name (`RoutingKeyHash`'s first 8 bytes), `RUN_BLOOM_BITS_PER_KEY` bits per
//! key and `RUN_BLOOM_HASHES` probes by double hashing (`h1` the fingerprint,
//! `h2` its splitmix64 mix, odd). It is empty (any key may be there) when a
//! chunk of the run summarises its keys by a bloom (the keys are not known)
//! or the run names more keys than `MAX_RUN_BLOOM_BYTES` holds at that rate.
//!
//! Every byte is under exactly one check, each locating the next: the magic
//! is compared, the trailer CRC covers the trailer's first 32 bytes, the
//! footer CRC the footer the trailer locates, each run's CRC the run the
//! footer locates. A ranged GET of one run checks it without the footer.

use crate::crypto_page::{get_varint, put_varint};
use crate::spill::crc32c::crc32c;
use crate::spill::directory::{DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS};
use crate::spill::object::{Anchor, ObjectId};

pub(crate) const RUN_INDEX_MAGIC: [u8; 4] = *b"L6ri";
pub(crate) const RUN_INDEX_VERSION: u16 = 1;
pub(crate) const TRAILER_LEN: usize = 40;
/// Footer flag: the run's entries tile its span with no gap.
pub(crate) const DENSE: u8 = 1;
pub(crate) const RUN_CRC_LEN: usize = 4;
/// The most bytes one run's bloom takes (8,192 bits: about 819 keys).
pub(crate) const MAX_RUN_BLOOM_BYTES: usize = 1024;
pub(crate) const RUN_BLOOM_BITS_PER_KEY: usize = 10;
const RUN_BLOOM_HASHES: u64 = 7;
/// The highest level a merge writes (0 a fold, 1 hourly, 2 daily).
pub(crate) const MAX_LEVEL: u8 = 2;
/// What a decoder reserves at most before the bytes justify more.
const RESERVE_CAP: u64 = 4096;

/// One run's footer entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FooterEntry {
    pub(crate) hash: [u8; 16],
    pub(crate) run_off: u64,
    pub(crate) run_len: u32,
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) entries: u32,
    pub(crate) flags: u8,
    pub(crate) bloom: Vec<u8>,
}

impl FooterEntry {
    /// Whether the run's entries tile its span with no gap.
    pub(crate) fn dense(&self) -> bool {
        self.flags & DENSE != 0
    }

    /// Whether the run may hold records of the routing key whose
    /// fingerprint is `fp` (an empty bloom: any key).
    pub(crate) fn may_hold(&self, fp: u64) -> bool {
        self.bloom.is_empty() || bloom_holds(&self.bloom, fp)
    }
}

/// The trailer's fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub(crate) footer_off: u64,
    pub(crate) footer_len: u32,
    pub(crate) runs: u32,
    pub(crate) entries: u64,
    pub(crate) level: u8,
}

/// Why an object could not be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FormatError {
    /// A run with no entries, or entries out of order (first and last must
    /// both increase strictly).
    RunOrder,
    /// Runs out of hash order.
    HashOrder,
    /// A run, the footer or the object larger than its length fields hold.
    TooLarge,
    /// A level above `MAX_LEVEL`.
    Level,
}

fn splitmix(fp: u64) -> u64 {
    let mut z = fp.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The bits a fingerprint sets in a bloom of `len` bytes.
fn bloom_probes(fp: u64, len: usize) -> impl Iterator<Item = usize> {
    let h2 = splitmix(fp) | 1;
    let bits = u64::try_from(len.saturating_mul(8))
        .unwrap_or(u64::MAX)
        .max(1);
    (0..RUN_BLOOM_HASHES)
        .map(move |i| usize::try_from(fp.wrapping_add(i.wrapping_mul(h2)) % bits).unwrap_or(0))
}

/// Set a fingerprint's probe bits in `bloom`.
pub(crate) fn bloom_set(bloom: &mut [u8], fp: u64) {
    for bit in bloom_probes(fp, bloom.len()) {
        if let Some(byte) = bloom.get_mut(bit / 8) {
            *byte |= 1 << (bit % 8);
        }
    }
}

/// Whether every probe bit of `fp` is set: true for every fingerprint
/// `bloom_set` set, so a run bloom never excludes a key its run holds.
pub(crate) fn bloom_holds(bloom: &[u8], fp: u64) -> bool {
    bloom_probes(fp, bloom.len()).all(|bit| {
        bloom
            .get(bit / 8)
            .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
    })
}

/// The bloom of a run's entries: empty when a chunk summarises its keys by
/// a bloom, or when the keys need more than `MAX_RUN_BLOOM_BYTES`.
pub(crate) fn run_bloom(entries: &[DirEntry]) -> Vec<u8> {
    let mut fps = Vec::new();
    for e in entries {
        match &e.keys {
            KeySummary::Keys(keys) => fps.extend_from_slice(keys),
            KeySummary::Bloom(_) => return Vec::new(),
        }
    }
    fps.sort_unstable();
    fps.dedup();
    let len = (fps.len() * RUN_BLOOM_BITS_PER_KEY).div_ceil(8).max(1);
    if len > MAX_RUN_BLOOM_BYTES {
        return Vec::new();
    }
    let mut bloom = vec![0u8; len];
    for fp in fps {
        bloom_set(&mut bloom, fp);
    }
    bloom
}

/// Whether `entries` may form a run: at least one, first and last both
/// strictly increasing.
pub(crate) fn run_ordered(entries: &[DirEntry]) -> bool {
    !entries.is_empty()
        && entries
            .windows(2)
            .all(|w| w[0].first < w[1].first && w[0].last < w[1].last)
}

fn put_summary(v: &mut Vec<u8>, keys: &KeySummary) {
    match keys {
        KeySummary::Bloom(bloom) => {
            put_varint(v, 0);
            put_varint(v, bloom.len() as u64);
            v.extend_from_slice(bloom);
        }
        KeySummary::Keys(keys) => {
            put_varint(v, keys.len() as u64);
            for key in keys {
                v.extend_from_slice(&key.to_be_bytes());
            }
        }
    }
}

fn get_summary(input: &mut &[u8]) -> Option<KeySummary> {
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

fn put_entry(v: &mut Vec<u8>, e: &DirEntry) {
    put_varint(v, e.object.epoch);
    put_varint(v, e.object.seq);
    put_varint(v, e.chunk_off);
    put_varint(v, u64::from(e.chunk_len));
    put_varint(v, e.first);
    put_varint(v, e.last - e.first);
    put_varint(v, u64::from(e.page_count));
    put_summary(v, &e.keys);
    put_varint(v, e.anchors.len() as u64);
    for a in &e.anchors {
        put_varint(v, u64::from(a.page));
        put_varint(v, a.offset - e.first);
        put_varint(v, u64::from(a.byte));
    }
}

fn get_u32(input: &mut &[u8]) -> Option<u32> {
    u32::try_from(get_varint(input)?).ok()
}

fn get_entry(input: &mut &[u8]) -> Option<DirEntry> {
    let object = ObjectId {
        epoch: get_varint(input)?,
        seq: get_varint(input)?,
    };
    let chunk_off = get_varint(input)?;
    let chunk_len = get_u32(input)?;
    let first = get_varint(input)?;
    let last = first.checked_add(get_varint(input)?)?;
    let page_count = get_u32(input)?;
    let keys = get_summary(input)?;
    let anchors_n = get_varint(input)?;
    let mut anchors = Vec::with_capacity(usize::try_from(anchors_n.min(RESERVE_CAP)).ok()?);
    for _ in 0..anchors_n {
        let page = get_u32(input)?;
        let offset = first.checked_add(get_varint(input)?)?;
        let byte = get_u32(input)?;
        anchors.push(Anchor { page, offset, byte });
    }
    Some(DirEntry {
        object,
        chunk_off,
        chunk_len,
        first,
        last,
        page_count,
        keys,
        anchors,
    })
}

/// One run's bytes, its CRC included.
pub(crate) fn encode_run(entries: &[DirEntry]) -> Result<Vec<u8>, FormatError> {
    if !run_ordered(entries) {
        return Err(FormatError::RunOrder);
    }
    let mut v = Vec::with_capacity(4 + 40 * entries.len());
    put_varint(&mut v, entries.len() as u64);
    for e in entries {
        put_entry(&mut v, e);
    }
    let crc = crc32c(&v);
    v.extend_from_slice(&crc.to_le_bytes());
    u32::try_from(v.len()).map_err(|_| FormatError::TooLarge)?;
    Ok(v)
}

/// One run's entries from its bytes; `None` for any byte sequence
/// `encode_run` does not write (a failed CRC included).
pub(crate) fn decode_run(raw: &[u8]) -> Option<Vec<DirEntry>> {
    let (body, crc) = raw.split_last_chunk::<RUN_CRC_LEN>()?;
    if crc32c(body) != u32::from_le_bytes(*crc) {
        return None;
    }
    let mut input = body;
    let n = get_varint(&mut input)?;
    let mut out = Vec::with_capacity(usize::try_from(n.min(RESERVE_CAP)).ok()?);
    for _ in 0..n {
        out.push(get_entry(&mut input)?);
    }
    (input.is_empty() && run_ordered(&out)).then_some(out)
}

fn put_footer_entry(v: &mut Vec<u8>, f: &FooterEntry) {
    v.extend_from_slice(&f.hash);
    v.extend_from_slice(&f.run_off.to_le_bytes());
    v.extend_from_slice(&f.run_len.to_le_bytes());
    v.extend_from_slice(&f.first.to_le_bytes());
    v.extend_from_slice(&f.last.to_le_bytes());
    v.extend_from_slice(&f.entries.to_le_bytes());
    v.push(f.flags);
    // A bloom is at most MAX_RUN_BLOOM_BYTES (the writer's check).
    v.extend_from_slice(&u16::try_from(f.bloom.len()).unwrap_or(0).to_le_bytes());
    v.extend_from_slice(&f.bloom);
}

fn take<const N: usize>(input: &mut &[u8]) -> Option<[u8; N]> {
    let (head, rest) = input.split_first_chunk::<N>()?;
    *input = rest;
    Some(*head)
}

fn get_footer_entry(input: &mut &[u8]) -> Option<FooterEntry> {
    let hash = take::<16>(input)?;
    let run_off = u64::from_le_bytes(take(input)?);
    let run_len = u32::from_le_bytes(take(input)?);
    let first = u64::from_le_bytes(take(input)?);
    let last = u64::from_le_bytes(take(input)?);
    let entries = u32::from_le_bytes(take(input)?);
    let [flags] = take::<1>(input)?;
    if flags & !DENSE != 0 {
        return None;
    }
    let bloom_len = usize::from(u16::from_le_bytes(take(input)?));
    if bloom_len > MAX_RUN_BLOOM_BYTES {
        return None;
    }
    let (bloom, rest) = input.split_at_checked(bloom_len)?;
    *input = rest;
    Some(FooterEntry {
        hash,
        run_off,
        run_len,
        first,
        last,
        entries,
        flags,
        bloom: bloom.to_vec(),
    })
}

/// The footer flags of a run's entries.
pub(crate) fn run_flags(entries: &[DirEntry]) -> u8 {
    let dense = entries
        .windows(2)
        .all(|w| w[0].last.checked_add(1) == Some(w[1].first));
    if dense { DENSE } else { 0 }
}

/// The footer's entries (its CRC already checked): runs sorted by hash,
/// strictly, each starting where the previous one ended from byte 0, its
/// span and entry count non-empty, and the last run ending at the
/// trailer's `footer_off`. `None` otherwise.
fn footer_entries(raw: &[u8], trailer: &Trailer) -> Option<Vec<FooterEntry>> {
    let mut input = raw;
    let n = u64::from(trailer.runs);
    let mut out: Vec<FooterEntry> = Vec::with_capacity(usize::try_from(n.min(RESERVE_CAP)).ok()?);
    let (mut at, mut entries) = (0u64, 0u64);
    for _ in 0..n {
        let f = get_footer_entry(&mut input)?;
        let sorted = out.last().is_none_or(|prev| prev.hash < f.hash);
        let fits = f.run_off == at && f.entries > 0 && f.first <= f.last;
        if !sorted || !fits || (f.run_len as usize) <= RUN_CRC_LEN {
            return None;
        }
        at = at.checked_add(u64::from(f.run_len))?;
        entries = entries.checked_add(u64::from(f.entries))?;
        out.push(f);
    }
    let whole = input.is_empty() && at == trailer.footer_off && entries == trailer.entries;
    whole.then_some(out)
}

/// The trailer's bytes.
fn encode_trailer(t: &Trailer, footer_crc: u32) -> [u8; TRAILER_LEN] {
    let mut v = Vec::with_capacity(TRAILER_LEN);
    v.extend_from_slice(&t.footer_off.to_le_bytes());
    v.extend_from_slice(&t.footer_len.to_le_bytes());
    v.extend_from_slice(&t.runs.to_le_bytes());
    v.extend_from_slice(&t.entries.to_le_bytes());
    v.extend_from_slice(&RUN_INDEX_VERSION.to_le_bytes());
    v.push(t.level);
    v.push(0);
    v.extend_from_slice(&footer_crc.to_le_bytes());
    let crc = crc32c(&v);
    v.extend_from_slice(&crc.to_le_bytes());
    v.extend_from_slice(&RUN_INDEX_MAGIC);
    let mut out = [0u8; TRAILER_LEN];
    out.copy_from_slice(&v);
    out
}

/// The trailer and the footer CRC it stores, from an object's last
/// `TRAILER_LEN` bytes; `None` unless magic, version, flags, level and CRC
/// all hold.
pub(crate) fn decode_trailer(raw: &[u8]) -> Option<(Trailer, u32)> {
    let raw: &[u8; TRAILER_LEN] = raw.try_into().ok()?;
    let (body, tail) = raw.split_at(32);
    let (crc, magic) = tail.split_at(4);
    if magic != RUN_INDEX_MAGIC || crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let mut input = body;
    let t = Trailer {
        footer_off: u64::from_le_bytes(take(&mut input)?),
        footer_len: u32::from_le_bytes(take(&mut input)?),
        runs: u32::from_le_bytes(take(&mut input)?),
        entries: u64::from_le_bytes(take(&mut input)?),
        level: 0,
    };
    let version = u16::from_le_bytes(take(&mut input)?);
    let [level, flags] = take::<2>(&mut input)?;
    let footer_crc = u32::from_le_bytes(take(&mut input)?);
    let valid = version == RUN_INDEX_VERSION && flags == 0 && level <= MAX_LEVEL && t.runs > 0;
    valid.then_some((Trailer { level, ..t }, footer_crc))
}

/// A footer, checked against the CRC the trailer stores.
pub(crate) fn checked_footer(
    raw: &[u8],
    trailer: &Trailer,
    footer_crc: u32,
) -> Option<Vec<FooterEntry>> {
    if crc32c(raw) != footer_crc || raw.len() != trailer.footer_len as usize {
        return None;
    }
    footer_entries(raw, trailer)
}

/// One run of a decoded object: its footer entry and its entries.
#[cfg(test)]
pub(crate) type DecodedRun = (FooterEntry, Vec<DirEntry>);

/// A written object: its bytes and its trailer.
pub(crate) type Finished = (Vec<u8>, Trailer);

/// Writes one object run by run, in hash order.
pub(crate) struct ObjectWriter {
    ready: Vec<u8>,
    footer: Vec<FooterEntry>,
    entries: u64,
}

impl ObjectWriter {
    pub(crate) fn new() -> Self {
        Self {
            ready: Vec::new(),
            footer: Vec::new(),
            entries: 0,
        }
    }

    /// Append `hash`'s run.
    pub(crate) fn push(&mut self, hash: [u8; 16], entries: &[DirEntry]) -> Result<(), FormatError> {
        if self.footer.last().is_some_and(|prev| prev.hash >= hash) {
            return Err(FormatError::HashOrder);
        }
        let run = encode_run(entries)?;
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Err(FormatError::RunOrder);
        };
        let run_len = u32::try_from(run.len()).map_err(|_| FormatError::TooLarge)?;
        let count = u32::try_from(entries.len()).map_err(|_| FormatError::TooLarge)?;
        self.footer.push(FooterEntry {
            hash,
            run_off: self.ready.len() as u64,
            run_len,
            first: first.first,
            last: last.last,
            entries: count,
            flags: run_flags(entries),
            bloom: run_bloom(entries),
        });
        self.entries += u64::from(count);
        self.ready.extend_from_slice(&run);
        Ok(())
    }

    /// The object's bytes and its trailer; `None` for an object of no run.
    pub(crate) fn finish(mut self, level: u8) -> Result<Option<Finished>, FormatError> {
        if level > MAX_LEVEL {
            return Err(FormatError::Level);
        }
        if self.footer.is_empty() {
            return Ok(None);
        }
        let footer_off = self.ready.len() as u64;
        let mut footer = Vec::new();
        for f in &self.footer {
            put_footer_entry(&mut footer, f);
        }
        let trailer = Trailer {
            footer_off,
            footer_len: u32::try_from(footer.len()).map_err(|_| FormatError::TooLarge)?,
            runs: u32::try_from(self.footer.len()).map_err(|_| FormatError::TooLarge)?,
            entries: self.entries,
            level,
        };
        let crc = crc32c(&footer);
        self.ready.extend_from_slice(&footer);
        self.ready.extend_from_slice(&encode_trailer(&trailer, crc));
        Ok(Some((std::mem::take(&mut self.ready), trailer)))
    }
}

/// A whole object, every check verified: its trailer and each run with its
/// footer entry, in hash order. `None` for any byte sequence `ObjectWriter`
/// does not write (tests; the fuzz target checks the same layers).
#[cfg(test)]
pub(crate) fn decode_object(raw: &[u8]) -> Option<(Trailer, Vec<DecodedRun>)> {
    let split = raw.len().checked_sub(TRAILER_LEN)?;
    let (rest, tail) = raw.split_at(split);
    let (trailer, footer_crc) = decode_trailer(tail)?;
    let footer_off = usize::try_from(trailer.footer_off).ok()?;
    let (runs, footer) = rest.split_at_checked(footer_off)?;
    let footer = checked_footer(footer, &trailer, footer_crc)?;
    let mut out = Vec::with_capacity(footer.len());
    for f in footer {
        let start = usize::try_from(f.run_off).ok()?;
        let run = runs.get(start..start.checked_add(f.run_len as usize)?)?;
        let entries = decode_run(run)?;
        let agrees = entries.len() == f.entries as usize
            && entries.first().map(|e| e.first) == Some(f.first)
            && entries.last().map(|e| e.last) == Some(f.last)
            && f.flags == run_flags(&entries)
            && f.bloom == run_bloom(&entries);
        if !agrees {
            return None;
        }
        out.push((f, entries));
    }
    Some((trailer, out))
}
