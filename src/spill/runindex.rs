//! Run-index objects (layout 6, DESIGN-6A §2.3): what the fold turns one
//! fold period's seal directory rows into, so the shard log holds one
//! pointer row per object instead of one directory row per seal forever,
//! and the in-memory directory holds at most one fold period.
//!
//! - The object (`format`): one run of chunks per stream, sorted by stream,
//!   with a footer index and a routing-key bloom per run, at
//!   `<shard_db_path>/ridx/<epoch:016x>/<seq:016x>`, named by the writer
//!   epoch of the shard DB opening that wrote it, so a fenced folder's
//!   object is never referenced (an orphan for package 6's sweep).
//! - The pointer row: `'S' ‖ 0xFF×16 ‖ 'I' ‖ epoch BE ‖ seq BE` →
//!   `ver:u8=1 level:u8 len:varint footer_off:varint footer_len:varint
//!   runs:varint entries:varint seals:varint orphan_chunks:varint
//!   orphan_bytes:varint created_ms:varint`. `seals` counts the directory
//!   rows the object's lineage folded; `orphan_*` the chunks a consolidation
//!   superseded (still stored until package 6 frees them).
//! - The orphan row: `'S' ‖ 0xFF×16 ‖ 'O' ‖ epoch BE ‖ seq BE` →
//!   `ver:u8=1 chunks:varint bytes:varint`, written by a consolidation
//!   beside its directory row (the chunks it superseded) and folded into
//!   the next pointer row's counters.
//! - How chunks tile offsets across the directory and the objects
//!   (`tiling`, KANI-105).

use crate::crypto_page::{get_varint, put_varint};
use crate::spill::directory::sentinel_key;
use crate::spill::object::ObjectId;

pub(crate) mod format;
pub(crate) mod tiling;

#[cfg(test)]
mod format_tests;
#[cfg(test)]
mod tiling_tests;

/// Every tag here sorts below the usage month-finals' `'V'`: the drain
/// scans `'V' ‖ hash` for each billed segment, and a key past it would make
/// every SST holding one overlap all those scans (each then a GET; SlateDB
/// scans do not fill the block cache).
pub(crate) const POINTER_TAG: u8 = b'I';
pub(crate) const ORPHAN_TAG: u8 = b'O';
const VERSION: u8 = 1;

/// `<shard_db_path>/ridx/<epoch:016x>/<seq:016x>`.
pub(crate) fn run_index_path(shard_db_path: &str, id: ObjectId) -> String {
    format!("{shard_db_path}/ridx/{:016x}/{:016x}", id.epoch, id.seq)
}

/// `'S' ‖ 0xFF×16 ‖ tag`: every row of that tag.
pub(crate) fn sentinel_prefix(tag: u8) -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(tag);
    k
}

/// The object a sentinel row of `tag` names.
pub(crate) fn parse_sentinel_key(tag: u8, key: &[u8]) -> Option<ObjectId> {
    let rest = key.strip_prefix(&sentinel_prefix(tag)[..])?;
    let (epoch, seq) = rest.split_first_chunk::<8>()?;
    Some(ObjectId {
        epoch: u64::from_be_bytes(*epoch),
        seq: u64::from_be_bytes(seq.try_into().ok()?),
    })
}

pub(crate) fn pointer_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(POINTER_TAG, id)
}

pub(crate) fn orphan_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(ORPHAN_TAG, id)
}

/// Chunks a consolidation superseded: stored, never read again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Orphans {
    pub(crate) chunks: u64,
    pub(crate) bytes: u64,
}

impl Orphans {
    pub(crate) fn add(self, other: Self) -> Self {
        Self {
            chunks: self.chunks.saturating_add(other.chunks),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

pub(crate) fn encode_orphans(o: Orphans) -> Vec<u8> {
    let mut v = vec![VERSION];
    put_varint(&mut v, o.chunks);
    put_varint(&mut v, o.bytes);
    v
}

pub(crate) fn decode_orphans(raw: &[u8]) -> Option<Orphans> {
    let (&ver, mut input) = raw.split_first()?;
    let o = Orphans {
        chunks: get_varint(&mut input)?,
        bytes: get_varint(&mut input)?,
    };
    (ver == VERSION && input.is_empty()).then_some(o)
}

/// One live run-index object, as its pointer row describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pointer {
    pub(crate) id: ObjectId,
    pub(crate) level: u8,
    /// The object's length.
    pub(crate) len: u64,
    pub(crate) footer_off: u64,
    pub(crate) footer_len: u32,
    pub(crate) runs: u64,
    pub(crate) entries: u64,
    pub(crate) seals: u64,
    pub(crate) orphans: Orphans,
    pub(crate) created_ms: u64,
}

pub(crate) fn encode_pointer(p: &Pointer) -> Vec<u8> {
    let mut v = vec![VERSION, p.level];
    for n in [
        p.len,
        p.footer_off,
        u64::from(p.footer_len),
        p.runs,
        p.entries,
        p.seals,
        p.orphans.chunks,
        p.orphans.bytes,
        p.created_ms,
    ] {
        put_varint(&mut v, n);
    }
    v
}

/// A pointer row's value; `None` for any byte sequence `encode_pointer`
/// does not write, or one whose footer does not end the object's body
/// before its trailer.
pub(crate) fn decode_pointer(id: ObjectId, raw: &[u8]) -> Option<Pointer> {
    let (&[ver, level], mut input) = raw.split_first_chunk::<2>()?;
    let mut next = || get_varint(&mut input);
    let p = Pointer {
        id,
        level,
        len: next()?,
        footer_off: next()?,
        footer_len: u32::try_from(next()?).ok()?,
        runs: next()?,
        entries: next()?,
        seals: next()?,
        orphans: Orphans {
            chunks: next()?,
            bytes: next()?,
        },
        created_ms: next()?,
    };
    let body = p.footer_off.checked_add(u64::from(p.footer_len))?;
    let fits = body.checked_add(format::TRAILER_LEN as u64) == Some(p.len);
    let valid = ver == VERSION && level <= format::MAX_LEVEL && fits && p.runs > 0;
    (valid && input.is_empty()).then_some(p)
}
