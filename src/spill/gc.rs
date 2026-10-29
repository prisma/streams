//! Physical reclamation's rows and rules (layout 6, DESIGN-6A §2.5,
//! package 6): what the shard log records about each stored object's
//! references, and which of a stream's index entries are its live chunks.
//! The engine's side (the refcount lock, reader pins, the purge, the
//! garbage deletes and the orphan sweep) is `shard::seal::gc`.
//!
//! Rows, all under the sentinel (`'S' ‖ 0xFF×16 ‖ tag ‖ ...`, 34 B keys):
//! - `'R' ‖ epoch BE ‖ seq BE`, one per segment object with a live chunk →
//!   `ver:u8=1 live_bytes:varint live_chunks:varint created_ms:varint
//!   object_len:varint`. `live_bytes` sums the encoded lengths of the
//!   object's unreclaimed chunks (the unit a directory entry carries, so a
//!   decrement is exact).
//! - `'G' ‖ epoch BE ‖ seq BE`, one per object awaiting its DELETE →
//!   `ver:u8=1 kind:u8 object_len:varint due_ms:varint`, written in the
//!   batch that removed the object's last reference.
//! - `'H' ‖ hash[16]`, one per dead stream → `ver:u8=1 state:u8
//!   not_before_ms:varint head:varint reclaimed:varint`: every chunk of the
//!   stream ending below `head` is dead; every dead chunk starting below
//!   `reclaimed` was decremented already.
//! - `'L' ‖ kind:u8` → the orphan sweep's LIST cursor for that prefix.
//!
//! Every tag sorts below the usage month-finals' `'V'` (see `SWEEP_TAG`).

use crate::crypto_page::{get_varint, put_varint};
use crate::spill::directory::{DirEntry, REFCOUNT_TAG, sentinel_key};
use crate::spill::object::ObjectId;
use crate::spill::runindex::tiling::{Span, drop_covered, sort_for_drop};

pub(crate) const GARBAGE_TAG: u8 = b'G';
pub(crate) const HEAD_TAG: u8 = b'H';
/// Every sentinel tag sorts below the usage month-finals' `'V'`: the drain
/// scans `'V'` per billed segment, and a key above it would make every
/// such scan read a data block of each SST holding it.
pub(crate) const SWEEP_TAG: u8 = b'L';
const VERSION: u8 = 1;

/// A segment object's unreclaimed chunks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Refcount {
    pub(crate) live_bytes: u64,
    pub(crate) live_chunks: u64,
    pub(crate) created_ms: u64,
    pub(crate) object_len: u64,
}

pub(crate) fn refcount_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(REFCOUNT_TAG, id)
}

pub(crate) fn encode_refcount(r: &Refcount) -> Vec<u8> {
    let mut v = vec![VERSION];
    for n in [r.live_bytes, r.live_chunks, r.created_ms, r.object_len] {
        put_varint(&mut v, n);
    }
    v
}

pub(crate) fn decode_refcount(raw: &[u8]) -> Option<Refcount> {
    let (&ver, mut input) = raw.split_first()?;
    let mut next = || get_varint(&mut input);
    let r = Refcount {
        live_bytes: next()?,
        live_chunks: next()?,
        created_ms: next()?,
        object_len: next()?,
    };
    let valid = ver == VERSION && r.live_chunks > 0 && r.live_bytes <= r.object_len;
    (valid && input.is_empty()).then_some(r)
}

/// Which store prefix an object lives under.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ObjectKind {
    /// `hseg/`: a seal's or a consolidation's segment object.
    Segment,
    /// `ridx/`: a fold's or a merge's run-index object.
    RunIndex,
}

impl ObjectKind {
    pub(crate) const ALL: [Self; 2] = [Self::Segment, Self::RunIndex];

    fn byte(self) -> u8 {
        match self {
            Self::Segment => 0,
            Self::RunIndex => 1,
        }
    }

    fn of(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Segment),
            1 => Some(Self::RunIndex),
            _ => None,
        }
    }

    /// The store directory under the shard's path.
    pub(crate) fn dir(self) -> &'static str {
        match self {
            Self::Segment => "hseg",
            Self::RunIndex => "ridx",
        }
    }
}

/// An object whose last reference is gone, deleted once `due_ms` passed
/// and no read that may hold it is running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Garbage {
    pub(crate) kind: ObjectKind,
    pub(crate) object_len: u64,
    pub(crate) due_ms: u64,
}

pub(crate) fn garbage_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(GARBAGE_TAG, id)
}

pub(crate) fn encode_garbage(g: &Garbage) -> Vec<u8> {
    let mut v = vec![VERSION, g.kind.byte()];
    put_varint(&mut v, g.object_len);
    put_varint(&mut v, g.due_ms);
    v
}

pub(crate) fn decode_garbage(raw: &[u8]) -> Option<Garbage> {
    let (&[ver, kind], mut input) = raw.split_first_chunk::<2>()?;
    let g = Garbage {
        kind: ObjectKind::of(kind)?,
        object_len: get_varint(&mut input)?,
        due_ms: get_varint(&mut input)?,
    };
    (ver == VERSION && input.is_empty()).then_some(g)
}

/// Where a dead stream's purge stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeadState {
    /// The purge was asked for; it runs once `not_before_ms` passed.
    Requested,
    /// The stream's tail and pending rows are gone; `head` was its next
    /// offset then.
    Purged,
}

/// A dead stream's reclamation state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Head {
    pub(crate) state: HeadState,
    pub(crate) not_before_ms: u64,
    /// Every chunk ending below it is dead.
    pub(crate) head: u64,
    /// Every dead chunk starting below it was decremented; at most `head`.
    pub(crate) reclaimed: u64,
}

impl Head {
    pub(crate) fn requested(not_before_ms: u64) -> Self {
        Self {
            state: HeadState::Requested,
            not_before_ms,
            head: 0,
            reclaimed: 0,
        }
    }

    /// Whether dead chunks wait for their decrements.
    pub(crate) fn owes(&self) -> bool {
        self.reclaimed < self.head
    }
}

/// `'S' ‖ 0xFF×16 ‖ 'H' ‖ hash`.
pub(crate) fn head_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(HEAD_TAG);
    k.extend_from_slice(hash);
    k
}

/// The stream a head key names.
pub(crate) fn parse_head_key(key: &[u8]) -> Option<[u8; 16]> {
    let rest = key.strip_prefix(&crate::keyspace::SENTINEL[..])?;
    let (&tag, hash) = rest.split_first()?;
    (tag == HEAD_TAG).then_some(())?;
    hash.try_into().ok()
}

pub(crate) fn encode_head(h: &Head) -> Vec<u8> {
    let state = match h.state {
        HeadState::Requested => 0,
        HeadState::Purged => 1,
    };
    let mut v = vec![VERSION, state];
    for n in [h.not_before_ms, h.head, h.reclaimed] {
        put_varint(&mut v, n);
    }
    v
}

pub(crate) fn decode_head(raw: &[u8]) -> Option<Head> {
    let (&[ver, state], mut input) = raw.split_first_chunk::<2>()?;
    let state = match state {
        0 => HeadState::Requested,
        1 => HeadState::Purged,
        _ => return None,
    };
    let mut next = || get_varint(&mut input);
    let h = Head {
        state,
        not_before_ms: next()?,
        head: next()?,
        reclaimed: next()?,
    };
    let valid = ver == VERSION && h.reclaimed <= h.head;
    (valid && input.is_empty()).then_some(h)
}

/// `'S' ‖ 0xFF×16 ‖ 'L' ‖ kind`: the orphan sweep's cursor for `kind`.
pub(crate) fn sweep_key(kind: ObjectKind) -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(SWEEP_TAG);
    k.push(kind.byte());
    k
}

/// The live chunks among a stream's index entries (the directory's and
/// every live run-index object's): equal entries counted once, and every
/// entry another contains dropped (a consolidated chunk supersedes the
/// chunks it copied). Two different chunks of the same span are not a
/// state any writer makes: an error.
pub(crate) fn live_entries(mut entries: Vec<DirEntry>) -> Result<Vec<DirEntry>, String> {
    sort_for_drop(&mut entries, span_of);
    let mut distinct: Vec<DirEntry> = Vec::with_capacity(entries.len());
    for e in entries {
        match distinct.last() {
            Some(prev) if span_of(prev) == span_of(&e) => {
                if prev.object != e.object || prev.chunk_off != e.chunk_off {
                    return Err(format!(
                        "two chunks hold [{}, {}]: {:?} and {:?}",
                        e.first, e.last, prev.object, e.object
                    ));
                }
            }
            _ => distinct.push(e),
        }
    }
    let spans: Vec<Span> = distinct.iter().map(span_of).collect();
    let keep = drop_covered(&spans);
    let mut keep = keep.into_iter().peekable();
    Ok(distinct
        .into_iter()
        .enumerate()
        .filter(|(i, _)| keep.next_if_eq(i).is_some())
        .map(|(_, e)| e)
        .collect())
}

fn span_of(e: &DirEntry) -> Span {
    Span {
        first: e.first,
        last: e.last,
    }
}

/// Decrements per object: (bytes, chunks).
pub(crate) type Decrements = std::collections::BTreeMap<ObjectId, (u64, u64)>;

/// Add one chunk's decrement.
pub(crate) fn decrement(d: &mut Decrements, e: &DirEntry) {
    let slot = d.entry(e.object).or_default();
    slot.0 += u64::from(e.chunk_len);
    slot.1 += 1;
}

/// `r` less `by`; `None` when it would go below zero (a decrement the
/// object never counted).
pub(crate) fn less(r: Refcount, by: (u64, u64)) -> Option<Refcount> {
    Some(Refcount {
        live_bytes: r.live_bytes.checked_sub(by.0)?,
        live_chunks: r.live_chunks.checked_sub(by.1)?,
        ..r
    })
}
