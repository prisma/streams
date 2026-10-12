//! The index row (design B §1.1): one row per sealed chunk, in the shard DB,
//! written by the seal's commit in the same group as the absorbed advance.
//!
//! Key: `hash16 ‖ 'i' ‖ last_offset BE` (25 B). The tag `'i'` is free in
//! today's shard keyspace (survey in `spill::index_tests`: stream rows use
//! `B C F G c l p q s t x` after the hash; the 0xFF×16 sentinel uses
//! `D M U V`). Keyed by the chunk's LAST offset, so a seek to
//! `hash ‖ 'i' ‖ x` lands on the chunk holding `x` when the rows tile.
//!
//! Value v1 (little-endian, 67 B + 12 B per anchor):
//! `ver:u8=1 epoch:u64 seq:u64 chunk_off:u64 chunk_len:u32 first:u64 last:u64
//!  page_count:u32 stored:u64 max_ts:i64 anchors:u16 (page:u32 offset−first:u32
//!  byte:u32)*`.
//!
//! Refcount rows (built in M8, coded here) sit under the 0xFF×16 sentinel,
//! never under a leading tag (DESIGN §3.2 item 1, rBc G3-K):
//! `0xFF×16 ‖ 'R' ‖ epoch BE ‖ seq BE` → `ver:u8=1 live_bytes:u64
//! live_chunks:u32 created_ms:i64`.

use super::object::{Anchor, EncodedChunk, ObjectId};

pub(crate) const INDEX_TAG: u8 = b'i';
pub(crate) const REFCOUNT_SENTINEL: [u8; 16] = [0xFF; 16];
pub(crate) const REFCOUNT_TAG: u8 = b'R';
const INDEX_ROW_VERSION: u8 = 1;
pub(crate) const INDEX_FIXED_LEN: usize = 67;
const ANCHOR_LEN: usize = 12;
const REFCOUNT_ROW_VERSION: u8 = 1;
const REFCOUNT_ROW_LEN: usize = 21;

/// `hash16 ‖ 'i'`: one stream's index rows, and nothing else.
pub(crate) fn index_prefix(hash: &[u8; 16]) -> [u8; 17] {
    let mut prefix = [INDEX_TAG; 17];
    prefix[..16].copy_from_slice(hash);
    prefix
}

pub(crate) fn index_key(hash: &[u8; 16], last: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.extend_from_slice(&index_prefix(hash));
    key.extend_from_slice(&last.to_be_bytes());
    key
}

/// The stream and last offset an index key names, or None for any other row.
pub(crate) fn parse_index_key(key: &[u8]) -> Option<([u8; 16], u64)> {
    let (head, last) = key.split_last_chunk::<8>()?;
    let (hash, tag) = head.split_first_chunk::<16>()?;
    (tag == [INDEX_TAG]).then(|| (*hash, u64::from_be_bytes(*last)))
}

pub(crate) fn refcount_key(id: ObjectId) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.extend_from_slice(&REFCOUNT_SENTINEL);
    key.push(REFCOUNT_TAG);
    key.extend_from_slice(&id.epoch.to_be_bytes());
    key.extend_from_slice(&id.seq.to_be_bytes());
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexError {
    Version,
    Length,
    /// The anchors are not inside the chunk, in order.
    Anchors,
    /// first > last, or a field that cannot hold for a chunk.
    Range,
}

/// One sealed chunk's index row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexRow {
    pub(crate) object: ObjectId,
    pub(crate) chunk_off: u64,
    pub(crate) chunk_len: u32,
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) page_count: u32,
    pub(crate) stored: u64,
    pub(crate) max_ts: i64,
    pub(crate) anchors: Vec<Anchor>,
}

impl IndexRow {
    pub(crate) fn for_chunk(object: ObjectId, chunk: &EncodedChunk) -> Self {
        let e = &chunk.entry;
        Self {
            object,
            chunk_off: e.offset,
            chunk_len: e.len,
            first: e.first,
            last: e.last,
            page_count: e.page_count,
            stored: u64::from(e.stored),
            max_ts: e.max_ts,
            anchors: chunk.anchors.clone(),
        }
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, IndexError> {
        let anchors = u16::try_from(self.anchors.len()).map_err(|_| IndexError::Anchors)?;
        let mut v = Vec::with_capacity(INDEX_FIXED_LEN + ANCHOR_LEN * self.anchors.len());
        v.push(INDEX_ROW_VERSION);
        v.extend_from_slice(&self.object.epoch.to_le_bytes());
        v.extend_from_slice(&self.object.seq.to_le_bytes());
        v.extend_from_slice(&self.chunk_off.to_le_bytes());
        v.extend_from_slice(&self.chunk_len.to_le_bytes());
        v.extend_from_slice(&self.first.to_le_bytes());
        v.extend_from_slice(&self.last.to_le_bytes());
        v.extend_from_slice(&self.page_count.to_le_bytes());
        v.extend_from_slice(&self.stored.to_le_bytes());
        v.extend_from_slice(&self.max_ts.to_le_bytes());
        v.extend_from_slice(&anchors.to_le_bytes());
        for a in &self.anchors {
            let delta = a
                .offset
                .checked_sub(self.first)
                .and_then(|d| u32::try_from(d).ok())
                .ok_or(IndexError::Anchors)?;
            v.extend_from_slice(&a.page.to_le_bytes());
            v.extend_from_slice(&delta.to_le_bytes());
            v.extend_from_slice(&a.byte.to_le_bytes());
        }
        Ok(v)
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self, IndexError> {
        let (fixed, rest) = raw
            .split_first_chunk::<INDEX_FIXED_LEN>()
            .ok_or(IndexError::Length)?;
        if fixed[0] != INDEX_ROW_VERSION {
            return Err(IndexError::Version);
        }
        let u64_at = |at: usize| u64::from_le_bytes(fixed[at..at + 8].try_into().unwrap_or([0; 8]));
        let u32_at = |at: usize| u32::from_le_bytes(fixed[at..at + 4].try_into().unwrap_or([0; 4]));
        let row_first = u64_at(29);
        let row = Self {
            object: ObjectId {
                epoch: u64_at(1),
                seq: u64_at(9),
            },
            chunk_off: u64_at(17),
            chunk_len: u32_at(25),
            first: row_first,
            last: u64_at(37),
            page_count: u32_at(45),
            stored: u64_at(49),
            max_ts: u64_at(57) as i64,
            anchors: Vec::new(),
        };
        let count = usize::from(u16::from_le_bytes([fixed[65], fixed[66]]));
        if rest.len() != count * ANCHOR_LEN {
            return Err(IndexError::Length);
        }
        if row.first > row.last || row.page_count == 0 {
            return Err(IndexError::Range);
        }
        let mut anchors = Vec::with_capacity(count);
        let (mut prev_page, mut prev_offset, mut prev_byte) = (0u32, row.first, 0u32);
        for a in rest.chunks_exact(ANCHOR_LEN) {
            let page = u32::from_le_bytes([a[0], a[1], a[2], a[3]]);
            let offset = row.first + u64::from(u32::from_le_bytes([a[4], a[5], a[6], a[7]]));
            let byte = u32::from_le_bytes([a[8], a[9], a[10], a[11]]);
            if page <= prev_page
                || page >= row.page_count
                || offset <= prev_offset
                || offset > row.last
                || byte <= prev_byte
                || byte >= row.chunk_len
            {
                return Err(IndexError::Anchors);
            }
            (prev_page, prev_offset, prev_byte) = (page, offset, byte);
            anchors.push(Anchor { page, offset, byte });
        }
        Ok(Self { anchors, ..row })
    }

    /// Where a read starting at `offset` (inside this chunk) begins: the
    /// last anchor at or before it, or page 0 (`byte` = 0 then means "from
    /// the chunk's header", which the reader needs for the directory anyway).
    pub(crate) fn start_for(&self, offset: u64) -> Option<Anchor> {
        self.anchors.iter().rev().find(|a| a.offset <= offset).copied()
    }
}

/// An object's refcount row (M8): its live bytes and chunks, and when it
/// was created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RefcountRow {
    pub(crate) live_bytes: u64,
    pub(crate) live_chunks: u32,
    pub(crate) created_ms: i64,
}

impl RefcountRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(REFCOUNT_ROW_LEN);
        v.push(REFCOUNT_ROW_VERSION);
        v.extend_from_slice(&self.live_bytes.to_le_bytes());
        v.extend_from_slice(&self.live_chunks.to_le_bytes());
        v.extend_from_slice(&self.created_ms.to_le_bytes());
        v
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self, IndexError> {
        let r: &[u8; REFCOUNT_ROW_LEN] = raw.try_into().map_err(|_| IndexError::Length)?;
        if r[0] != REFCOUNT_ROW_VERSION {
            return Err(IndexError::Version);
        }
        Ok(Self {
            live_bytes: u64::from_le_bytes(r[1..9].try_into().unwrap_or([0; 8])),
            live_chunks: u32::from_le_bytes(r[9..13].try_into().unwrap_or([0; 4])),
            created_ms: i64::from_le_bytes(r[13..21].try_into().unwrap_or([0; 8])),
        })
    }
}
