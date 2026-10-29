//! The seal directory (layout 6, DESIGN-6A §2.3): one `'K'` row per seal in
//! the shard log, and the in-memory directory every sealed-range read
//! consults, rebuilt at open from the `'K'` range. Package 5 folds the rows
//! into run-index objects; the row format is final for it.
//!
//! Key: `'S' ‖ 0xFF×16 ‖ 'K' ‖ epoch BE ‖ seq BE` (34 B; the sentinel sorts
//! after every stream's rows). The object the row describes is named by the
//! same epoch and seq (`object::object_path`).
//!
//! Value: the row codec in `row`, a leaf the seal-directory fuzz target
//! compiles unchanged (`fuzz/fuzz_targets/seal_directory.rs`).
//!
//! The refcount row (`'S' ‖ 0xFF×16 ‖ 'R' ‖ epoch BE ‖ seq BE` →
//! `ver:u8=1 live_bytes:varint live_chunks:varint created_ms:varint`) rides
//! the same commit; garbage collection reads it (package 6).

use std::collections::{BTreeMap, HashMap};

use super::object::{ChunkInput, ObjectId};
use crate::crypto_page::put_varint;

#[cfg(kani)]
mod proofs;
mod row;

use row::{BLOOM_BITS_PER_KEY, VERSION, halves, holds_bits, set_bits};
pub(crate) use row::{
    DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS, decode_row, encode_row,
};

pub(crate) const DIRECTORY_TAG: u8 = b'K';
pub(crate) const REFCOUNT_TAG: u8 = b'R';

/// `'S' ‖ 0xFF×16 ‖ tag ‖ epoch BE ‖ seq BE`: a row about one object.
pub(crate) fn sentinel_key(tag: u8, id: ObjectId) -> Vec<u8> {
    let mut k = Vec::with_capacity(34);
    k.extend_from_slice(&crate::keyspace::SENTINEL);
    k.push(tag);
    k.extend_from_slice(&id.epoch.to_be_bytes());
    k.extend_from_slice(&id.seq.to_be_bytes());
    k
}

pub(crate) fn directory_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(DIRECTORY_TAG, id)
}

/// `'S' ‖ 0xFF×16 ‖ 'K'`: every directory row of the shard.
pub(crate) fn directory_prefix() -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(DIRECTORY_TAG);
    k
}

pub(crate) fn refcount_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(REFCOUNT_TAG, id)
}

pub(crate) fn refcount_value(live_bytes: u64, live_chunks: u64, created_ms: u64) -> Vec<u8> {
    let mut v = vec![VERSION];
    put_varint(&mut v, live_bytes);
    put_varint(&mut v, live_chunks);
    put_varint(&mut v, created_ms);
    v
}

/// The object a directory key names.
pub(crate) fn parse_directory_key(key: &[u8]) -> Option<ObjectId> {
    let rest = key.strip_prefix(&directory_prefix()[..])?;
    let (epoch, seq) = rest.split_first_chunk::<8>()?;
    Some(ObjectId {
        epoch: u64::from_be_bytes(*epoch),
        seq: u64::from_be_bytes(seq.try_into().ok()?),
    })
}

impl KeySummary {
    /// The summary of `chunk`'s pages, read from their clear headers.
    pub(crate) fn of(chunk: &ChunkInput) -> Result<Self, String> {
        let mut keys = std::collections::BTreeMap::new();
        for (last, page) in &chunk.pages {
            let page = crate::crypto_page::CheckedPage::admit(page.clone(), *last)
                .map_err(|e| format!("a sealed page at {last}: {e:?}"))?;
            let hash = crate::crypto::RoutingKeyHash::of(page.routing_key()).0;
            keys.insert(halves(&hash).0, hash);
        }
        if keys.len() <= MAX_SUMMARY_KEYS {
            return Ok(Self::Keys(keys.into_keys().collect()));
        }
        let len = (keys.len() * BLOOM_BITS_PER_KEY)
            .div_ceil(8)
            .clamp(1, MAX_BLOOM_BYTES);
        let mut bloom = vec![0u8; len];
        for hash in keys.values() {
            set_bits(&mut bloom, hash);
        }
        Ok(Self::Bloom(bloom))
    }

    /// Whether the chunk may hold records of `routing_key` (a bloom may
    /// answer yes for a key it does not hold, never no for one it does).
    pub(crate) fn may_hold(&self, routing_key: &str) -> bool {
        let hash = crate::crypto::RoutingKeyHash::of(routing_key).0;
        match self {
            Self::Keys(keys) => keys.binary_search(&halves(&hash).0).is_ok(),
            Self::Bloom(bloom) => holds_bits(bloom, &hash),
        }
    }
}

/// A routing key's fingerprint: the first 8 bytes of its hash (what a
/// summary names, and what a run-index bloom holds).
pub(crate) fn fingerprint(routing_key: &str) -> u64 {
    halves(&crate::crypto::RoutingKeyHash::of(routing_key).0).0
}

/// The in-memory directory: each stream's sealed chunks by last offset and
/// object (a consolidated chunk may end where one of the chunks it copied
/// ends). The fold keeps it to the seals of at most one fold period
/// (`shard::seal::fold`).
/// One stream's entries, by (last offset, object).
type StreamEntries = BTreeMap<(u64, ObjectId), DirEntry>;

#[derive(Default)]
pub(crate) struct Directory {
    streams: HashMap<[u8; 16], StreamEntries>,
    entries: u64,
    /// The entries' memory, by `entry_heap` (the seal stats' gauge, and
    /// this directory's share of `process_bytes`).
    bytes: u64,
}

/// Every live directory's memory in the process (the fold's process-wide
/// trigger, `spill::budget::PROCESS_DIRECTORY_BYTES`).
static PROCESS_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Every live directory's memory in the process.
pub(crate) fn process_bytes() -> u64 {
    PROCESS_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

impl Drop for Directory {
    fn drop(&mut self) {
        PROCESS_BYTES.fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One entry's memory, an upper estimate: the entry, its map key, its key
/// summary's and anchors' heap.
fn entry_heap(e: &DirEntry) -> u64 {
    let summary = match &e.keys {
        KeySummary::Keys(keys) => keys.len() * size_of::<u64>(),
        KeySummary::Bloom(bits) => bits.len(),
    };
    let bytes = size_of::<DirEntry>()
        + size_of::<u64>()
        + summary
        + e.anchors.len() * size_of::<super::object::Anchor>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

impl Directory {
    fn grow(&mut self, added: u64) {
        self.bytes += added;
        PROCESS_BYTES.fetch_add(added, std::sync::atomic::Ordering::Relaxed);
    }

    fn shrink(&mut self, removed: u64) {
        self.bytes -= removed;
        PROCESS_BYTES.fetch_sub(removed, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn insert(&mut self, hash: [u8; 16], entry: DirEntry) {
        let added = entry_heap(&entry);
        let key = (entry.last, entry.object);
        match self.streams.entry(hash).or_default().insert(key, entry) {
            None => self.entries += 1,
            Some(replaced) => self.shrink(entry_heap(&replaced)),
        }
        self.grow(added);
    }

    /// Remove the entry of `hash` ending at `last` in `object`, if present
    /// (a failed seal's, or one the fold moved into a run-index object).
    pub(crate) fn remove(&mut self, hash: &[u8; 16], last: u64, object: ObjectId) {
        let Some(m) = self.streams.get_mut(hash) else {
            return;
        };
        let Some(removed) = m.remove(&(last, object)) else {
            return;
        };
        if m.is_empty() {
            self.streams.remove(hash);
        }
        self.entries -= 1;
        self.shrink(entry_heap(&removed));
    }

    /// The chunks of `hash` ending at or after `from`, by last offset, at
    /// most `max` of them.
    pub(crate) fn from(&self, hash: &[u8; 16], from: u64, max: usize) -> Vec<DirEntry> {
        let start = (from, ObjectId { epoch: 0, seq: 0 });
        self.streams.get(hash).map_or_else(Vec::new, |m| {
            m.range(start..).take(max).map(|(_, e)| e.clone()).collect()
        })
    }

    pub(crate) fn entries(&self) -> u64 {
        self.entries
    }

    pub(crate) fn streams(&self) -> usize {
        self.streams.len()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Tests: each entry's memory (`entry_heap`).
    #[cfg(test)]
    pub(crate) fn entry_bytes(&self) -> impl Iterator<Item = u64> + '_ {
        self.streams
            .values()
            .flat_map(BTreeMap::values)
            .map(entry_heap)
    }
}
