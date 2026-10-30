//! Read-triggered consolidation (layout 6, DESIGN-6A §2.4 (d)): a sealed
//! read that fetched a run of small whole chunks of one stream hands them
//! to the engine's consolidator, which copies their pages, byte for byte
//! and never re-encrypted, into one chunk of a new segment object, so the
//! next cold walk of those offsets costs one GET instead of one per chunk.
//! The read already holds the bytes: consolidating costs no extra GET.
//!
//! One job (`consolidate_one`, under the collector's `refs` lock): if the
//! offsets still resolve to exactly the job's chunks, the pages are checked to continue each other, encoded as one
//! chunk (one object, `PutMode::Create`, the shared object sequence), the
//! chunk is published in the directory, then one batch writes its `'K'`
//! row, its `'R'` row and an `'O'` row counting the chunks it superseded
//! (WAL-flushed, durable; an error withdraws the entry and closes the
//! engine). The same batch decrements each superseded chunk's object
//! (`seal::gc`): an object left with no live chunk is deleted after its
//! grace, once no read that may still hold it runs. Readers prefer the
//! consolidated chunk (`tiling::tile`). The `'O'` rows count the superseded
//! chunks (`seal.fold.orphan_*`, cumulative).
//!
//! It races nothing it can break: seals only add chunks above a stream's
//! boundary while it copies chunks below it; a fold folds its `'K'` row
//! now or at the next fold (the entry is published before the row commits,
//! as a seal's); a read that planned with the copied chunks fetches them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use object_store::{PutMode, PutOptions, PutPayload};
use slatedb::WriteBatch;

use crate::shard::{RingScan, ShardEngine};
use crate::spill::directory::{self, DirEntry, KeySummary};
use crate::spill::gc::{self, Decrements};
use crate::spill::object::{self, ChunkInput};
use crate::spill::runindex::{self, Orphans};

/// A chunk this small is worth consolidating.
pub(crate) const SMALL_CHUNK: u32 = 64 << 10;
/// A read offers at least this many contiguous small chunks.
pub(crate) const MIN_CHUNKS: usize = 8;
/// One consolidated chunk holds at most this many page bytes.
pub(crate) const MAX_BYTES: u64 = 1 << 20;
/// Jobs queued per engine; a read finding the queue full offers nothing.
const QUEUE: usize = 4;

/// One job: a stream's contiguous small chunks, each fetched whole.
pub(crate) struct Job {
    hash: [u8; 16],
    chunks: Vec<(DirEntry, Bytes)>,
    _permit: tokio::sync::SemaphorePermit<'static>,
}

/// The consolidator's queue and counters (part of `FoldState`).
pub(crate) struct Consolidator {
    tx: tokio::sync::mpsc::Sender<Job>,
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<Job>>>,
    pub(crate) offered: AtomicU64,
    pub(crate) done: AtomicU64,
    pub(crate) skipped: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) chunks_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
}

impl Default for Consolidator {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(QUEUE);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            offered: AtomicU64::new(0),
            done: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            chunks_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }
}

impl Consolidator {
    pub(crate) fn json(&self) -> serde_json::Value {
        let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
        serde_json::json!({
            "offered": n(&self.offered),
            "done": n(&self.done),
            "skipped": n(&self.skipped),
            "failed": n(&self.failed),
            "chunks_in": n(&self.chunks_in),
            "bytes_out": n(&self.bytes_out),
        })
    }
}

/// The runs worth consolidating among a read's whole chunks: maximal
/// contiguous runs of small chunks, cut at `MAX_BYTES`, of at least
/// `MIN_CHUNKS`.
pub(crate) fn runs_to_consolidate(chunks: Vec<(DirEntry, Bytes)>) -> Vec<Vec<(DirEntry, Bytes)>> {
    let mut out = Vec::new();
    let mut run: Vec<(DirEntry, Bytes)> = Vec::new();
    let mut bytes = 0u64;
    for (e, body) in chunks {
        let continues = run
            .last()
            .is_some_and(|(prev, _)| prev.last.checked_add(1) == Some(e.first));
        let fits = bytes + u64::from(e.chunk_len) <= MAX_BYTES;
        if !continues || !fits || e.chunk_len >= SMALL_CHUNK {
            if run.len() >= MIN_CHUNKS {
                out.push(std::mem::take(&mut run));
            }
            run.clear();
            bytes = 0;
        }
        if e.chunk_len < SMALL_CHUNK {
            bytes += u64::from(e.chunk_len);
            run.push((e, body));
        }
    }
    if run.len() >= MIN_CHUNKS {
        out.push(run);
    }
    out
}

impl ShardEngine {
    fn consolidator(&self) -> &Consolidator {
        &self.layout.seal.fold.consolidator
    }

    /// A sealed read's whole chunks of `hash`, in offset order: queue each
    /// run worth consolidating, within the process's consolidation budget;
    /// none when the queue or the budget is full.
    pub(crate) fn offer_consolidation(&self, hash: [u8; 16], chunks: Vec<(DirEntry, Bytes)>) {
        if chunks.len() < MIN_CHUNKS {
            return;
        }
        for run in runs_to_consolidate(chunks) {
            let kib: u32 = run.iter().map(|(e, _)| e.chunk_len.div_ceil(1024)).sum();
            let Ok(permit) = crate::spill::budget::consolidation_permits().try_acquire_many(kib)
            else {
                return;
            };
            let job = Job {
                hash,
                chunks: run,
                _permit: permit,
            };
            if self.consolidator().tx.try_send(job).is_err() {
                return;
            }
            self.consolidator().offered.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The consolidator: one job at a time until the engine closes.
    pub(in crate::shard) async fn consolidate_loop(self: Arc<Self>) {
        let rx = self
            .consolidator()
            .rx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(mut rx) = rx else {
            return;
        };
        loop {
            let job = tokio::select! {
                () = self.closed() => None,
                job = rx.recv() => job,
            };
            let Some(job) = job else {
                return;
            };
            if self.is_closed() {
                return;
            }
            let c = self.consolidator();
            match self.consolidate_one(job).await {
                Ok(true) => c.done.fetch_add(1, Ordering::Relaxed),
                Ok(false) => c.skipped.fetch_add(1, Ordering::Relaxed),
                Err(e) => {
                    tracing::debug!(shard = %self.prefix, "consolidation failed: {e}");
                    c.failed.fetch_add(1, Ordering::Relaxed)
                }
            };
        }
    }

    /// One job; false when its offsets no longer resolve to exactly its
    /// chunks, or its stream is dead. Under the collector's `refs` lock,
    /// from the re-check until the batch is durable: the batch decrements
    /// every superseded chunk's object (`seal::gc`), so no two jobs may
    /// supersede one chunk.
    pub(crate) async fn consolidate_one(&self, job: Job) -> Result<bool, String> {
        let _refs = self.gc_state().refs.lock().await;
        if !self.still_picked(&job).await? {
            return Ok(false);
        }
        let input = consolidated_input(job.hash, &job.chunks)?;
        let keys = KeySummary::of(&input)?;
        let encoded = object::encode(std::slice::from_ref(&input)).map_err(|e| format!("{e:?}"))?;
        let claim = self.claim_object();
        let id = claim.id;
        let path = object::object_path(&crate::sharddir::shard_db_path(&self.prefix), id);
        self.data_store
            .put_opts(
                &object_store::path::Path::from(path),
                encoded.parts.iter().cloned().collect::<PutPayload>(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let chunk = encoded.chunks.first().ok_or("an object of no chunk")?;
        let entry = DirEntry::of(id, chunk, keys.clone());
        let orphans = Orphans {
            chunks: job.chunks.len() as u64,
            bytes: job.chunks.iter().map(|(e, _)| u64::from(e.chunk_len)).sum(),
        };
        let mut batch = WriteBatch::new();
        let mut superseded = Decrements::new();
        for (e, _) in &job.chunks {
            gc::decrement(&mut superseded, e);
        }
        let zeroed = self
            .stage_decrements(&mut batch, &superseded)
            .await
            .map_err(|e| e.to_string())?;
        batch.put(
            directory::directory_key(id),
            directory::encode_row(&[(chunk, &keys)]),
        );
        batch.put(
            gc::refcount_key(id),
            gc::encode_refcount(&gc::Refcount {
                live_bytes: u64::from(chunk.entry.len),
                live_chunks: 1,
                created_ms: crate::shard::now_ms().unsigned_abs(),
                object_len: encoded.len as u64,
            }),
        );
        batch.put(runindex::orphan_key(id), runindex::encode_orphans(orphans));
        self.publish_consolidated(job.hash, &entry, true);
        if let Err(e) = self.write_decrements(batch, &zeroed).await {
            self.publish_consolidated(job.hash, &entry, false);
            return Err(e.to_string());
        }
        self.layout.seal.fold.note_orphans(id, orphans);
        self.gc_state()
            .counters
            .superseded_chunks
            .fetch_add(orphans.chunks, Ordering::Relaxed);
        let c = self.consolidator();
        c.chunks_in.fetch_add(orphans.chunks, Ordering::Relaxed);
        c.bytes_out.fetch_add(encoded.len as u64, Ordering::Relaxed);
        Ok(true)
    }

    /// Whether the job's offsets still resolve to exactly its chunks (the
    /// same objects, offsets and spans) and its stream is not dead: a chunk
    /// another job already superseded, or a range partly consolidated, is
    /// left alone.
    async fn still_picked(&self, job: &Job) -> Result<bool, String> {
        let (Some((first, _)), Some((last, _))) = (job.chunks.first(), job.chunks.last()) else {
            return Ok(false);
        };
        if self
            .head_row(&job.hash)
            .await
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Ok(false);
        }
        let scan = RingScan {
            from: first.first,
            to: last.last.saturating_add(1),
            max_bytes: usize::MAX,
        };
        let picks = self
            .sealed_pick_entries(job.hash, scan)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let same = |(a, (b, _)): (&DirEntry, &(DirEntry, Bytes))| {
            a.object == b.object
                && a.chunk_off == b.chunk_off
                && a.first == b.first
                && a.last == b.last
        };
        Ok(picks.len() == job.chunks.len() && picks.iter().zip(&job.chunks).all(same))
    }

    /// Publish (or withdraw) a consolidated chunk's directory entry.
    fn publish_consolidated(&self, hash: [u8; 16], entry: &DirEntry, publish: bool) {
        let mut dir = self
            .layout
            .seal
            .directory
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if publish {
            dir.insert(hash, entry.clone());
        } else {
            dir.remove(&hash, entry.last, entry.object);
        }
    }
}

/// The consolidated chunk's input: every page of `chunks`, which must
/// continue each other from the first chunk's first offset.
pub(super) fn consolidated_input(
    hash: [u8; 16],
    chunks: &[(DirEntry, Bytes)],
) -> Result<ChunkInput, String> {
    let mut pages = Vec::new();
    let mut max_ts = i64::MIN;
    let first = chunks.first().map_or(0, |(e, _)| e.first);
    let mut due = first;
    for (e, body) in chunks {
        let chunk = object::decode_chunk(body).map_err(|err| format!("{err:?}"))?;
        if chunk.hash != hash
            || chunk.first != due
            || chunk.first != e.first
            || chunk.last != e.last
        {
            return Err(format!(
                "chunk [{}, {}] does not continue at {due}",
                chunk.first, chunk.last
            ));
        }
        due = chunk.last.saturating_add(1);
        max_ts = max_ts.max(chunk.max_ts);
        pages.extend(chunk.pages);
    }
    Ok(ChunkInput {
        hash,
        first,
        max_ts,
        pages,
    })
}
