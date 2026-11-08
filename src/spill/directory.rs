//! The seal directory (layout 6, DESIGN-6A §2.3): one `'K'` row per seal in
//! the shard log, and the in-memory directory every sealed-range read
//! consults, rebuilt at open from the `'K'` range. Package 5 folds the rows
//! into run-index objects; the row format is final for it.
//!
//! Key: `0xFF×16 ‖ 'K' ‖ epoch BE ‖ seq BE` (33 B; the sentinel sorts
//! after every stream's rows). The object the row describes is named by the
//! same epoch and seq (`object::object_path`).
//!
//! Value: the row codec in `row`, a leaf the seal-directory fuzz target
//! compiles unchanged (`fuzz/fuzz_targets/seal_directory.rs`).
//!
//! The refcount row (`0xFF×16 ‖ 'R' ‖ epoch BE ‖ seq BE`, codec in
//! `spill::gc`) rides the same commit; garbage collection keeps it exact.

use std::collections::HashMap;

use super::object::{ChunkInput, ObjectId};

#[cfg(kani)]
mod proofs;
mod row;

use row::{BLOOM_BITS_PER_KEY, halves, holds_bits, set_bits};
pub(crate) use row::{
    DirEntry, KeySummary, MAX_BLOOM_BYTES, MAX_SUMMARY_KEYS, decode_row, encode_row,
};

pub(crate) const DIRECTORY_TAG: u8 = crate::keyspace::tag::DIRECTORY;
pub(crate) const REFCOUNT_TAG: u8 = crate::keyspace::tag::REFCOUNT;

/// `0xFF×16 ‖ tag ‖ home BE ‖ epoch BE ‖ seq BE`: a row about one object (S3a §3.2), fixed-width
/// so one home's rows of a tag are one range.
pub(crate) fn sentinel_key(tag: u8, id: ObjectId) -> Vec<u8> {
    let mut k = Vec::with_capacity(37);
    k.extend_from_slice(&crate::keyspace::SENTINEL);
    k.push(tag);
    k.extend_from_slice(&id.home.to_be_bytes());
    k.extend_from_slice(&id.epoch.to_be_bytes());
    k.extend_from_slice(&id.seq.to_be_bytes());
    k
}

/// The object a row of `tag` names (`sentinel_key`'s inverse); `None` for another key.
pub(crate) fn parse_object_key(tag: u8, key: &[u8]) -> Option<ObjectId> {
    let rest = key
        .strip_prefix(&crate::keyspace::SENTINEL)?
        .strip_prefix(&[tag])?;
    let (home, rest) = rest.split_first_chunk::<4>()?;
    let (epoch, seq) = rest.split_first_chunk::<8>()?;
    Some(ObjectId {
        home: u32::from_be_bytes(*home),
        epoch: u64::from_be_bytes(*epoch),
        seq: u64::from_be_bytes(seq.try_into().ok()?),
    })
}

pub(crate) fn directory_key(id: ObjectId) -> Vec<u8> {
    sentinel_key(DIRECTORY_TAG, id)
}

/// `0xFF×16 ‖ 'K'`: every directory row of the shard.
pub(crate) fn directory_prefix() -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(DIRECTORY_TAG);
    k
}

/// The object a directory key names.
pub(crate) fn parse_directory_key(key: &[u8]) -> Option<ObjectId> {
    parse_object_key(DIRECTORY_TAG, key)
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
/// One stream's entries, sorted by (last offset, object). A `BTreeMap` here
/// allocates a whole 1.5 KiB leaf node for a stream's one chunk.
type StreamEntries = Vec<DirEntry>;

#[derive(Default)]
pub(crate) struct Directory {
    streams: HashMap<[u8; 16], StreamEntries>,
    entries: u64,
    /// What the streams allocate, by `stream_charge` and `entry_own_heap` (the
    /// seal stats' gauge, and this directory's share of `process_bytes`).
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

/// What a stream holds beyond its entries' own heap: its slot in the map (key,
/// `Vec` header, a control byte) and its entries' capacity.
pub(crate) fn stream_charge(capacity: usize) -> u64 {
    u64::try_from(
        size_of::<[u8; 16]>() + size_of::<StreamEntries>() + 1 + capacity * size_of::<DirEntry>(),
    )
    .unwrap_or(u64::MAX)
}

/// An entry's own heap: its key summary and anchors.
pub(crate) fn entry_own_heap(e: &DirEntry) -> u64 {
    let summary = match &e.keys {
        KeySummary::Keys(keys) => keys.len() * size_of::<u64>(),
        KeySummary::Bloom(bits) => bits.len(),
    };
    let bytes = summary + e.anchors.len() * size_of::<super::object::Anchor>();
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
        let added = entry_own_heap(&entry);
        let key = (entry.last, entry.object);
        let stream = self.streams.entry(hash).or_default();
        // A stream is new exactly when it has no capacity: it keeps its
        // capacity until its last entry leaves.
        let before = match stream.capacity() {
            0 => 0,
            capacity => stream_charge(capacity),
        };
        let mut replaced = 0;
        match stream.binary_search_by_key(&key, |e| (e.last, e.object)) {
            Ok(at) => {
                if let Some(held) = stream.get_mut(at) {
                    replaced = entry_own_heap(held);
                    *held = entry;
                }
            }
            Err(at) => {
                // From one entry, doubling: most streams hold one chunk
                // between folds, and `Vec`'s own first allocation is four.
                if stream.len() == stream.capacity() {
                    stream.reserve_exact(stream.capacity().max(1));
                }
                stream.insert(at, entry);
                self.entries += 1;
            }
        }
        let after = stream_charge(stream.capacity());
        self.grow(added + after - before);
        self.shrink(replaced);
    }

    /// Remove the entry of `hash` ending at `last` in `object`, if present
    /// (a failed seal's, or one the fold moved into a run-index object).
    pub(crate) fn remove(&mut self, hash: &[u8; 16], last: u64, object: ObjectId) {
        let Some(stream) = self.streams.get_mut(hash) else {
            return;
        };
        let Ok(at) = stream.binary_search_by_key(&(last, object), |e| (e.last, e.object)) else {
            return;
        };
        let removed = stream.remove(at);
        let mut released = entry_own_heap(&removed);
        if stream.is_empty() {
            released += stream_charge(stream.capacity());
            self.streams.remove(hash);
        }
        self.entries -= 1;
        self.shrink(released);
    }

    /// The chunks of `hash` ending at or after `from`, by last offset, at
    /// most `max` of them.
    pub(crate) fn from(&self, hash: &[u8; 16], from: u64, max: usize) -> Vec<DirEntry> {
        self.streams.get(hash).map_or_else(Vec::new, |stream| {
            let start = stream.partition_point(|e| e.last < from);
            stream.iter().skip(start).take(max).cloned().collect()
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

    /// Tests: the gauge recomputed from what the streams hold.
    #[cfg(test)]
    pub(crate) fn recounted_bytes(&self) -> u64 {
        let streams = self.streams.values();
        streams
            .map(|s| stream_charge(s.capacity()) + s.iter().map(entry_own_heap).sum::<u64>())
            .sum()
    }

    /// Tests: each entry's own heap.
    #[cfg(test)]
    pub(crate) fn entry_bytes(&self) -> impl Iterator<Item = u64> + '_ {
        self.streams.values().flatten().map(entry_own_heap)
    }
}
