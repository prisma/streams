//! The segment cleaner (layout 6, package 6's follow-up; the lead's ruling of
//! 2026-10-31 under the owner's delegation, DESIGN-cleaner rev 2 after Fable
//! 5.1's review): an object kept stored by a few live chunks (a long-lived
//! stream's, sealed beside streams that died) is rewritten. Its live chunks
//! are copied into a new object, which supersedes them (a newer object wins
//! an equal span, `spill::runindex::tiling`; a stream's contiguous chunks are
//! consolidated into one), and the old object is released through the
//! collector's decrements, pins and grace.
//!
//! The rule, `clean_pays`: rewrite when a month of the dead bytes' storage
//! costs more than the copy. For an object of `object_len` O bytes holding
//! `live_chunks` k chunks of `live_bytes` L bytes, the copy is one GET of the
//! old object's table, one ranged GET per live chunk, the copy object's PUT
//! and the batch's WAL flush (a second PUT), moving L bytes in (the cost
//! model charges transfer on PUT bytes only); it saves the D = O - L dead
//! bytes, for at least a month (a ledger's chunk pins its object for the
//! ledger's whole life, so the month is the floor of the saving). At the cost
//! model's -50% basis (`bench/k2cost/packing.py`: Class A $3.75e-6 and Class B
//! $0.375e-6 at the ab50 multiplier 0.5, storage $0.015 per GiB-month,
//! transfer $0.01 per GB):
//!
//! ```text
//! rewrite  <=>  D * s  >  2 * c_put + (k + 1) * c_get + t * L
//! s = 0.015 / 2^30 $/B-month,  t = 0.01 / 1e9 $/B,
//! c_put = 1.875e-6 $,  c_get = 1.875e-7 $
//! ```
//!
//! So an object needs at least ~295 KB dead (L -> 0, k = 1); a large one
//! would pay up to s / (s + t) ~ 58% live, and the rule refuses an object
//! half live or more, whatever the prices. Prices are code: a change of them
//! is a change of this rule and its test.
//!
//! The pass (`clean_pass`, one per collector tick while candidates wait):
//! - candidates come from the decrements (`stage_decrements` offers a row
//!   left non-zero) and the hourly recount, in a bounded set; an object is
//!   due once `purge_delay` passed since its last decrement (an expiry wave
//!   settles first), in object order;
//! - per due object: its table from one ranged GET of its tail; per stream
//!   of it, an `'H'` row skips the stream (its chunks are the purge's), else
//!   the reclaim's own live set (`live_entries` of `index_entries`) decides
//!   which chunks still count; an object whose remaining live chunks are all
//!   `'H'`-pinned backs off (from a tick or the purge delay, doubling to the
//!   recount's interval);
//! - the live chunks are fetched (ranged GETs, self-verifying) and the copy
//!   object PUT before the refcount lock, within a pass budget of bytes and
//!   GETs drawn from the process's consolidation permits;
//! - under the lock every copied chunk is checked again (no `'H'` row, still
//!   in its stream's live set); any change abandons the copy object whole
//!   (its claim drops, the sweep deletes it). Then one batch: the old
//!   objects' decrements, the copy's `'K'`, `'R'` and `'O'` rows; the copy's
//!   entries are published before it and withdrawn if it fails.
//!
//! No pass while the shard log is at its L0 watermark's yield mark.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use slatedb::WriteBatch;

use super::{GcError, duration_ms, lock};
use crate::shard::ShardEngine;
use crate::spill::directory::{self, DirEntry, KeySummary};
use crate::spill::gc::{self, Decrements, Refcount};
use crate::spill::object::{self, ObjectId};
use crate::spill::object_table::{self, Table};
use crate::spill::runindex::{self, Orphans};

// The rule in integers: both sides times 1e15 * 2^30 (no rounding):
//   D * 0.015 * 1e15  >  (2 * 1.875e-6 + (k + 1) * 1.875e-7 + L * 1e-11) * 1e15 * 2^30
/// Storage of one byte for a month, times 1e15 * 2^30 ($0.015 per GiB-month).
const STORAGE_BYTE_MONTH: u128 = 15_000_000_000_000;
/// One PUT, times 1e15 (Class A $3.75e-6 at the -50% basis); times 2^30 below.
const PUT: u128 = 1_875_000_000;
/// One GET, times 1e15 (Class B $0.375e-6 at the -50% basis).
const GET: u128 = 187_500_000;
/// Moving one byte in, times 1e15 ($0.01 per GB).
const TRANSFER_BYTE: u128 = 10_000;
const GIB: u128 = 1 << 30;

/// Whether rewriting an object of `object_len` bytes that holds `live_chunks`
/// chunks of `live_bytes` bytes saves more in a month than the copy costs,
/// and the object is less than half live.
pub(crate) fn clean_pays(object_len: u64, live_bytes: u64, live_chunks: u64) -> bool {
    if live_chunks == 0 || 2 * u128::from(live_bytes) >= u128::from(object_len) {
        return false;
    }
    let dead = u128::from(object_len - live_bytes);
    let copy =
        2 * PUT + (u128::from(live_chunks) + 1) * GET + TRANSFER_BYTE * u128::from(live_bytes);
    dead * STORAGE_BYTE_MONTH > copy * GIB
}

/// A rewrite rule (`clean_pays`; tests rewrite small objects).
pub(crate) type Rule = fn(u64, u64, u64) -> bool;

/// Candidates held at most (the smallest dead bytes leave first).
const CANDIDATES: usize = 4096;
/// One pass's copies at most: live bytes and GETs.
const PASS_BYTES: u64 = 4 << 20;
const PASS_GETS: u64 = 64;

#[derive(Clone, Copy, Debug)]
struct Candidate {
    row: Refcount,
    /// When its row was last decremented (a wave settles first).
    decremented_ms: u64,
    /// Skipped until then (its live chunks were all `'H'`-pinned).
    skip_until_ms: u64,
    backoff_ms: u64,
}

/// The partly dead objects the rule would rewrite (part of `GcState`).
#[derive(Default)]
pub(crate) struct Candidates {
    set: Mutex<BTreeMap<ObjectId, Candidate>>,
}

impl Candidates {
    /// A decrement left `id`'s row at `row` at `now_ms`.
    pub(crate) fn offer(&self, id: ObjectId, row: Refcount, now_ms: u64, pays: Rule) {
        let mut set = lock(&self.set);
        if !pays(row.object_len, row.live_bytes, row.live_chunks) {
            set.remove(&id);
            return;
        }
        set.insert(
            id,
            Candidate {
                row,
                decremented_ms: now_ms,
                skip_until_ms: 0,
                backoff_ms: 0,
            },
        );
        if set.len() > CANDIDATES {
            let dead = |c: &Candidate| c.row.object_len - c.row.live_bytes;
            let smallest = set.iter().min_by_key(|(_, c)| dead(c)).map(|(id, _)| *id);
            if let Some(id) = smallest {
                set.remove(&id);
            }
        }
    }

    /// The recount found `id` at `row`: a candidate it did not know yet is
    /// offered (one it knows keeps its times).
    pub(crate) fn seed(&self, id: ObjectId, row: Refcount, now_ms: u64, pays: Rule) {
        if !lock(&self.set).contains_key(&id) {
            self.offer(id, row, now_ms, pays);
        }
    }

    pub(crate) fn forget(&self, id: ObjectId) {
        lock(&self.set).remove(&id);
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.set).len()
    }

    /// The candidates due at `now_ms`, in object order.
    fn due(&self, now_ms: u64, quiet_ms: u64) -> Vec<(ObjectId, Refcount)> {
        lock(&self.set)
            .iter()
            .filter(|(_, c)| {
                c.decremented_ms.saturating_add(quiet_ms) <= now_ms && c.skip_until_ms <= now_ms
            })
            .map(|(id, c)| (*id, c.row))
            .collect()
    }

    /// `id`'s live chunks were all `'H'`-pinned: skip it a while, doubling
    /// from `first_ms` up to `most_ms`.
    fn back_off(&self, id: ObjectId, now_ms: u64, first_ms: u64, most_ms: u64) {
        if let Some(c) = lock(&self.set).get_mut(&id) {
            c.backoff_ms = (c.backoff_ms.saturating_mul(2)).clamp(first_ms.max(1), most_ms.max(1));
            c.skip_until_ms = now_ms.saturating_add(c.backoff_ms);
        }
    }
}

/// What one object holds that still counts.
enum Live {
    /// Its live chunks (none `'H'`-pinned).
    Chunks(Vec<DirEntry>),
    /// Every live chunk is a dead stream's (the purge's to reclaim).
    Pinned,
    /// Nothing to copy (its row zeroes, or it changed).
    Nothing,
}

/// A stream's live entries, or `None` when it has an `'H'` row.
type LiveSets = HashMap<[u8; 16], Option<Vec<DirEntry>>>;

/// What a pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CleanReport {
    /// Old objects whose rows the copy zeroed.
    pub(crate) released: u64,
    pub(crate) copied_chunks: u64,
    pub(crate) abandoned: bool,
}

/// The chunks a pass copies: (old entry, its whole chunk's bytes).
type Copied = Vec<(DirEntry, Bytes)>;
/// The copied chunks by stream.
type ByStream = BTreeMap<[u8; 16], Copied>;

impl ShardEngine {
    /// One cleaning pass (see the module).
    pub(crate) async fn clean_pass(&self) -> Result<CleanReport, GcError> {
        let gc = self.gc_state();
        if gc.clean.len() == 0 || self.layout.l0.at_yield_mark() {
            return Ok(CleanReport::default());
        }
        let (copied, bytes) = self.gather().await?;
        if copied.is_empty() {
            return Ok(CleanReport::default());
        }
        let kib = u32::try_from(bytes.div_ceil(1024)).unwrap_or(u32::MAX);
        let Ok(_permit) = crate::spill::budget::consolidation_permits().try_acquire_many(kib)
        else {
            return Ok(CleanReport::default());
        };
        self.write_copy(copied).await
    }

    /// The due candidates' live chunks, fetched, within the pass budget;
    /// and their bytes.
    async fn gather(&self) -> Result<(Copied, u64), GcError> {
        let gc = self.gc_state();
        let s = gc.settings();
        let now = crate::shard::now_ms().unsigned_abs();
        let mut sets = LiveSets::new();
        let mut copied = Copied::new();
        let (mut bytes, mut gets) = (0u64, 0u64);
        for (id, row) in gc.clean.due(now, duration_ms(s.purge_delay)) {
            if bytes >= PASS_BYTES || gets >= PASS_GETS {
                break;
            }
            let chunks = match self.live_of(id, row, &mut sets).await? {
                Live::Chunks(chunks) => chunks,
                Live::Pinned => {
                    let first = duration_ms(s.purge_delay).max(duration_ms(s.tick));
                    let most = duration_ms(s.totals_every);
                    gc.clean.back_off(id, now, first, most);
                    continue;
                }
                Live::Nothing => {
                    gc.clean.forget(id);
                    continue;
                }
            };
            gets += 1 + chunks.len() as u64;
            bytes += chunks.iter().map(|e| u64::from(e.chunk_len)).sum::<u64>();
            copied.extend(self.fetch_chunks(chunks).await?);
        }
        Ok((copied, bytes))
    }

    async fn fetch_chunks(&self, chunks: Vec<DirEntry>) -> Result<Copied, GcError> {
        let mut out = Copied::with_capacity(chunks.len());
        for e in chunks {
            let body = self.chunk_bytes(&e).await?;
            out.push((e, body));
        }
        Ok(out)
    }

    /// What of `id` (whose row is `row`) still counts: per stream of its
    /// table, the reclaim's live set.
    async fn live_of(
        &self,
        id: ObjectId,
        row: Refcount,
        sets: &mut LiveSets,
    ) -> Result<Live, GcError> {
        let entries = self.table_of(id, row.object_len).await?;
        let (mut live, mut pinned) = (Vec::new(), 0usize);
        for entry in entries {
            let Some(set) = self.live_set(entry.hash, sets).await? else {
                pinned += 1;
                continue;
            };
            if let Some(e) = set
                .iter()
                .find(|e| e.object == id && e.chunk_off == entry.offset)
            {
                live.push(e.clone());
            }
        }
        Ok(match (live.is_empty(), pinned) {
            (false, _) => Live::Chunks(live),
            (true, 0) => Live::Nothing,
            (true, _) => Live::Pinned,
        })
    }

    /// `id`'s chunk table: one ranged GET of its tail, a second only for a
    /// table longer than the tail.
    async fn table_of(
        &self,
        id: ObjectId,
        object_len: u64,
    ) -> Result<Vec<object::ChunkEntry>, GcError> {
        let store = |e: object_store::Error| GcError::Store(e.to_string());
        let path = object_store::path::Path::from(self.object_path_of(gc::ObjectKind::Segment, id));
        let mut from = object_len.saturating_sub(object_table::TAIL_BYTES);
        for _ in 0..2 {
            let tail = self
                .data_store
                .get_range(&path, from..object_len)
                .await
                .map_err(store)?;
            match object_table::decode_table(&tail, object_len).map_err(GcError::Index)? {
                Table::Chunks(entries) => return Ok(entries),
                Table::StartsAt(at) => from = at,
            }
        }
        Err(GcError::Index(format!("{id:?}: no table")))
    }

    /// `hash`'s live entries (cached in `sets`), or `None` when it has an
    /// `'H'` row.
    async fn live_set<'a>(
        &self,
        hash: [u8; 16],
        sets: &'a mut LiveSets,
    ) -> Result<Option<&'a Vec<DirEntry>>, GcError> {
        if let std::collections::hash_map::Entry::Vacant(slot) = sets.entry(hash) {
            let set = if self.head_row(&hash).await?.is_some() {
                None
            } else {
                let entries = self.index_entries(hash).await?;
                Some(gc::live_entries(entries).map_err(GcError::Index)?)
            };
            slot.insert(set);
        }
        Ok(sets.get(&hash).and_then(Option::as_ref))
    }

    /// One chunk's bytes (a ranged GET, verified).
    async fn chunk_bytes(&self, e: &DirEntry) -> Result<Bytes, GcError> {
        let path =
            object_store::path::Path::from(self.object_path_of(gc::ObjectKind::Segment, e.object));
        let body = self
            .data_store
            .get_range(&path, e.chunk_off..e.chunk_off + u64::from(e.chunk_len))
            .await
            .map_err(|err| GcError::Store(err.to_string()))?;
        object::decode_chunk(&body)
            .map_err(|err| GcError::Index(format!("{:?}: {err:?}", e.object)))?;
        Ok(body)
    }

    /// Write `copied` as one copy object and commit it in place of them;
    /// abandoned whole when a copied chunk changed meanwhile.
    async fn write_copy(&self, copied: Copied) -> Result<CleanReport, GcError> {
        let (by_stream, inputs) = copy_inputs(copied)?;
        let index = GcError::Index;
        let keys: Vec<KeySummary> = inputs
            .iter()
            .map(KeySummary::of)
            .collect::<Result<_, _>>()
            .map_err(index)?;
        let encoded = object::encode(&inputs).map_err(|e| GcError::Index(format!("{e:?}")))?;
        let claim = self.claim_object();
        let id = claim.id;
        let path = object_store::path::Path::from(self.object_path_of(gc::ObjectKind::Segment, id));
        self.data_store
            .put_opts(
                &path,
                encoded.parts.iter().cloned().collect::<PutPayload>(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        #[cfg(test)]
        self.gc_state().after_clean_put().await?;
        let gc = self.gc_state();
        let _refs = gc.refs.lock().await;
        if !self.still_live(&by_stream).await? {
            gc.counters.clean_abandoned.fetch_add(1, Ordering::Relaxed);
            return Ok(CleanReport {
                abandoned: true,
                ..CleanReport::default()
            });
        }
        let report = self.commit_copy(id, &encoded, &keys, &by_stream).await?;
        drop(claim);
        Ok(report)
    }

    /// The copy's batch (the caller holds `refs`): the old chunks'
    /// decrements, the copy's `'K'`, `'R'` and `'O'` rows; its entries
    /// published before it, withdrawn if it fails.
    async fn commit_copy(
        &self,
        id: ObjectId,
        encoded: &object::EncodedObject,
        keys: &[KeySummary],
        by_stream: &ByStream,
    ) -> Result<CleanReport, GcError> {
        let old: Vec<&DirEntry> = by_stream.values().flatten().map(|(e, _)| e).collect();
        let mut superseded = Decrements::new();
        for e in &old {
            gc::decrement(&mut superseded, e);
        }
        let mut batch = WriteBatch::new();
        let zeroed = self.stage_decrements(&mut batch, &superseded).await?;
        let rows: Vec<(&object::EncodedChunk, &KeySummary)> =
            encoded.chunks.iter().zip(keys).collect();
        batch.put(directory::directory_key(id), directory::encode_row(&rows));
        let row = Refcount {
            live_bytes: encoded.chunks.iter().map(|c| u64::from(c.entry.len)).sum(),
            live_chunks: encoded.chunks.len() as u64,
            created_ms: crate::shard::now_ms().unsigned_abs(),
            object_len: encoded.len as u64,
        };
        batch.put(gc::refcount_key(id), gc::encode_refcount(&row));
        let orphans = Orphans {
            chunks: old.len() as u64,
            bytes: old.iter().map(|e| u64::from(e.chunk_len)).sum(),
        };
        batch.put(runindex::orphan_key(id), runindex::encode_orphans(orphans));
        let entries: Vec<([u8; 16], DirEntry)> = encoded
            .chunks
            .iter()
            .zip(keys)
            .map(|(c, k)| (c.entry.hash, DirEntry::of(id, c, k.clone())))
            .collect();
        for (hash, e) in &entries {
            self.publish_consolidated(*hash, e, true);
        }
        if let Err(e) = self.write_decrements(batch, &zeroed).await {
            for (hash, e) in &entries {
                self.publish_consolidated(*hash, e, false);
            }
            return Err(e);
        }
        self.layout.seal.fold.note_orphans(id, orphans);
        let c = &self.gc_state().counters;
        c.cleaned.fetch_add(zeroed.len() as u64, Ordering::Relaxed);
        c.cleaned_chunks
            .fetch_add(orphans.chunks, Ordering::Relaxed);
        c.clean_copied_bytes
            .fetch_add(encoded.len as u64, Ordering::Relaxed);
        c.clean_passes.fetch_add(1, Ordering::Relaxed);
        Ok(CleanReport {
            released: zeroed.len() as u64,
            copied_chunks: orphans.chunks,
            abandoned: false,
        })
    }

    /// Under the refcount lock: every copied chunk's stream has no `'H'` row
    /// and still counts the chunk.
    async fn still_live(&self, by_stream: &ByStream) -> Result<bool, GcError> {
        for (hash, chunks) in by_stream {
            if self.head_row(hash).await?.is_some() {
                return Ok(false);
            }
            let live =
                gc::live_entries(self.index_entries(*hash).await?).map_err(GcError::Index)?;
            let counted = |e: &DirEntry| {
                live.iter()
                    .any(|l| l.object == e.object && l.chunk_off == e.chunk_off)
            };
            if !chunks.iter().all(|(e, _)| counted(e)) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The copied chunks by stream, and the copy's inputs: a stream's chunks
/// that continue each other become one chunk.
fn copy_inputs(mut copied: Copied) -> Result<(ByStream, Vec<object::ChunkInput>), GcError> {
    copied.sort_by_key(|(e, _)| (e.first, e.object));
    let mut by_stream = ByStream::new();
    for (e, body) in copied {
        let hash = object::decode_chunk(&body)
            .map_err(|err| GcError::Index(format!("{err:?}")))?
            .hash;
        by_stream.entry(hash).or_default().push((e, body));
    }
    let mut inputs = Vec::new();
    for (hash, chunks) in &by_stream {
        let mut start = 0;
        for i in 1..=chunks.len() {
            let ends = chunks
                .get(i)
                .is_none_or(|(e, _)| chunks[i - 1].0.last.saturating_add(1) != e.first);
            if ends {
                let run = &chunks[start..i];
                inputs.push(
                    super::super::consolidate::consolidated_input(*hash, run)
                        .map_err(GcError::Index)?,
                );
                start = i;
            }
        }
    }
    Ok((by_stream, inputs))
}

#[cfg(test)]
mod tests;
