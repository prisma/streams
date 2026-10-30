//! The fold (layout 6, DESIGN-6A §2.3): every fold period, or sooner when
//! the directory outgrows its budget line, the seal directory's `'K'` rows
//! become one run-index object (`spill::runindex`), so the shard log keeps
//! one pointer row per object instead of a directory row per seal, and the
//! in-memory directory holds at most one fold period's chunks.
//!
//! One fold, one at a time (`fold_once`):
//! - **F1** scan the `'K'` rows (at most `max_rows`, oldest first) and the
//!   `'O'` rows. Every `'K'` row it finds already has its entries in the
//!   in-memory directory: a seal and a consolidation publish theirs before
//!   their commit (`seal::steps` step 5). A row that does not decode is
//!   counted and left where it is.
//! - **F2** build the object: each stream's entries, those a consolidated
//!   chunk contains dropped (`tiling::drop_covered`), in stream order.
//! - **F3** PUT it (`PutMode::Create`) under this opening's writer epoch.
//! - **F4** GET its trailer and footer back and decode them: the object is
//!   about to become the only index of its chunks.
//! - **F5** one shard-log batch: the pointer row put, every folded `'K'`
//!   and `'O'` row deleted, then a WAL flush and the batch durable. A
//!   fenced writer's batch fails, so its object is never referenced (an
//!   orphan). Any error leaves the batch's outcome unknown (SlateDB may
//!   apply a batch and still answer an error, TLA-005-F5): the engine
//!   closes, and the next owner's open reads whichever rows landed.
//! - **F6** install the object in the run-index set, then **F7** remove the
//!   folded entries from the directory. A reader takes the directory
//!   before the set (`seal::read`), so it finds every chunk in one of the
//!   two (a chunk in both resolves to either, byte for byte the same).
//!
//! A crash between any two steps leaves either the `'K'` rows (before F5:
//! the object an orphan) or the pointer row (after), never neither; the
//! open rebuilds the directory from the `'K'` rows left and the set from
//! the pointer rows (`seal::open`). Merges of folded objects are in
//! `merge`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use object_store::{PutMode, PutOptions, PutPayload};
use slatedb::WriteBatch;

use crate::shard::ShardEngine;
use crate::spill::budget::{DIRECTORY_FOLD_BYTES, MERGE_BUFFER_BYTES, PROCESS_DIRECTORY_BYTES};
use crate::spill::directory::{self, DirEntry};
use crate::spill::object::ObjectId;
use crate::spill::runindex::format::{self, ObjectWriter, TRAILER_LEN};
use crate::spill::runindex::tiling::{Span, drop_covered, sort_for_drop};
use crate::spill::runindex::{
    self, ORPHAN_TAG, Orphans, POINTER_TAG, Pointer, parse_sentinel_key, sentinel_prefix,
};

mod merge;
#[cfg(test)]
mod merge_tests;
#[cfg(test)]
pub(crate) mod test_hooks;

/// When the fold and its merges run.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FoldSettings {
    /// Fold at least this often while anything is unfolded.
    pub(crate) period: Duration,
    /// Fold at once when the shard's directory holds this many bytes.
    pub(crate) directory_bytes: u64,
    /// One fold takes at most this many directory rows (the rest wait for
    /// the next, which follows at once).
    pub(crate) max_rows: usize,
    /// Merge this many objects of level 0 into one of level 1 ("hourly"),
    /// and of level 1 into one of level 2 ("daily").
    pub(crate) fan_in: [usize; 2],
    /// One merge's memory (`spill::budget::MERGE_BUFFER_BYTES`): its inputs
    /// take at most half.
    pub(crate) merge_bytes: u64,
    /// How often the loop evaluates.
    pub(crate) tick: Duration,
    /// Whether the loop folds and merges by itself.
    pub(crate) auto: bool,
}

impl Default for FoldSettings {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(15 * 60),
            directory_bytes: DIRECTORY_FOLD_BYTES,
            max_rows: 4096,
            fan_in: [4, 24],
            merge_bytes: MERGE_BUFFER_BYTES,
            tick: Duration::from_secs(1),
            auto: true,
        }
    }
}

/// The fold's counters (`GET /v1/debug/timings`, each shard's
/// `seal.fold`, and tests).
#[derive(Default)]
pub(crate) struct FoldCounters {
    pub(crate) folds: AtomicU64,
    pub(crate) failures: AtomicU64,
    pub(crate) rows_folded: AtomicU64,
    pub(crate) entries_folded: AtomicU64,
    /// Entries a fold or merge dropped because a consolidated chunk in the
    /// same object contains them.
    pub(crate) entries_dropped: AtomicU64,
    pub(crate) objects: AtomicU64,
    pub(crate) object_bytes: AtomicU64,
    pub(crate) merges: AtomicU64,
    pub(crate) merge_failures: AtomicU64,
    pub(crate) merged_inputs: AtomicU64,
    /// `'K'`, `'I'` or `'O'` rows that did not decode (left in place).
    pub(crate) corrupt_rows: AtomicU64,
    /// Of them, `'K'` and `'I'` rows: the ones that may have named chunks,
    /// so a read meeting a gap answers corruption (edge #222).
    pub(crate) corrupt_index_rows: AtomicU64,
    pub(crate) footer_gets: AtomicU64,
    pub(crate) run_gets: AtomicU64,
    /// Runs a keyed read skipped by their bloom, without a GET.
    pub(crate) run_bloom_skips: AtomicU64,
}

/// The live run-index objects, by id.
pub(crate) type RunSet = Arc<BTreeMap<ObjectId, Pointer>>;

/// The fold's state on one engine (part of `SealState`).
#[derive(Default)]
pub(crate) struct FoldState {
    settings: Mutex<Option<FoldSettings>>,
    /// One fold or merge at a time.
    serial: tokio::sync::Mutex<()>,
    set: RwLock<RunSet>,
    /// Orphan rows not yet folded, by the consolidation that wrote them.
    unfolded_orphans: Mutex<BTreeMap<ObjectId, Orphans>>,
    last_fold: Mutex<Option<Instant>>,
    /// Wakes the fold loop when its settings change, so a new tick or
    /// period applies at once rather than after the old tick.
    wake: tokio::sync::Notify,
    /// This engine's scope in the process's index and chunk caches.
    pub(crate) cache_scope: crate::spill::cache::Scope,
    /// The keys of rows the open or a fold skipped, each counted once.
    corrupt: Mutex<std::collections::BTreeSet<Vec<u8>>>,
    pub(crate) counters: FoldCounters,
    /// Read-triggered consolidation's queue and counters.
    pub(crate) consolidator: super::consolidate::Consolidator,
    #[cfg(test)]
    pub(crate) hooks: test_hooks::FoldHooks,
}

// The fold state's locks guard plain values each replaced whole (a set
// swapped for a new one, a counter map entry inserted or removed), so a
// poisoned lock's value is consistent as it stands.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FoldState {
    pub(crate) fn settings(&self) -> FoldSettings {
        lock(&self.settings).unwrap_or_default()
    }

    /// Change the settings (tests).
    #[cfg(test)]
    pub(crate) fn configure(&self, change: impl FnOnce(&mut FoldSettings)) {
        let mut s = lock(&self.settings);
        let mut now = s.unwrap_or_default();
        change(&mut now);
        *s = Some(now);
        drop(s);
        self.wake.notify_one();
    }

    /// The live objects now.
    pub(crate) fn set(&self) -> RunSet {
        self.set
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn replace_set(&self, change: impl FnOnce(&mut BTreeMap<ObjectId, Pointer>)) {
        let mut set = self.set.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = (**set).clone();
        change(&mut next);
        *set = Arc::new(next);
    }

    /// Superseded chunks: the live pointers' counters and the unfolded
    /// orphan rows.
    pub(crate) fn orphans(&self) -> Orphans {
        let folded = self
            .set()
            .values()
            .fold(Orphans::default(), |a, p| a.add(p.orphans));
        lock(&self.unfolded_orphans)
            .values()
            .fold(folded, |a, o| a.add(*o))
    }

    /// A consolidation's orphan row was written.
    pub(crate) fn note_orphans(&self, id: ObjectId, o: Orphans) {
        lock(&self.unfolded_orphans).insert(id, o);
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let c = &self.counters;
        let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let set = self.set();
        let orphans = self.orphans();
        let levels: Vec<usize> = (0..=format::MAX_LEVEL)
            .map(|l| set.values().filter(|p| p.level == l).count())
            .collect();
        serde_json::json!({
            "objects_live": set.len(),
            "objects_by_level": levels,
            "index_bytes_live": set.values().map(|p| p.len).sum::<u64>(),
            "orphan_chunks": orphans.chunks,
            "orphan_bytes": orphans.bytes,
            "folds": n(&c.folds),
            "failures": n(&c.failures),
            "rows_folded": n(&c.rows_folded),
            "entries_folded": n(&c.entries_folded),
            "entries_dropped": n(&c.entries_dropped),
            "objects": n(&c.objects),
            "object_bytes": n(&c.object_bytes),
            "merges": n(&c.merges),
            "merge_failures": n(&c.merge_failures),
            "merged_inputs": n(&c.merged_inputs),
            "corrupt_rows": n(&c.corrupt_rows),
            "corrupt_index_rows": n(&c.corrupt_index_rows),
            "footer_gets": n(&c.footer_gets),
            "run_gets": n(&c.run_gets),
            "run_bloom_skips": n(&c.run_bloom_skips),
        })
    }
}

/// Why a fold or merge stopped.
#[derive(Debug)]
pub(crate) enum FoldError {
    Store(String),
    Format(format::FormatError),
    /// The object read back is not what was written.
    Verify(String),
    /// The batch's outcome is unknown; the engine closes.
    Batch(String),
    #[cfg(test)]
    Aborted(test_hooks::FoldStep),
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "store: {e}"),
            Self::Format(e) => write!(f, "format: {e:?}"),
            Self::Verify(e) => write!(f, "verify: {e}"),
            Self::Batch(e) => write!(f, "batch outcome unknown: {e}"),
            #[cfg(test)]
            Self::Aborted(step) => write!(f, "aborted after {step:?} (test)"),
        }
    }
}

/// What one fold did.
#[derive(Clone, Debug, Default)]
pub(crate) struct FoldReport {
    pub(crate) object: Option<ObjectId>,
    pub(crate) rows: usize,
    pub(crate) entries: u64,
    pub(crate) dropped: u64,
    pub(crate) bytes: u64,
    /// Rows left for the next fold (`max_rows`).
    pub(crate) more: bool,
}

/// One directory row: its object and its streams' entries.
type DirRow = (ObjectId, Vec<([u8; 16], DirEntry)>);

/// What F1 found.
#[derive(Default)]
struct Scan {
    rows: Vec<DirRow>,
    orphans: Vec<(ObjectId, Orphans)>,
    more: bool,
}

/// An object ready to PUT, and what its pointer row says of it.
struct Built {
    bytes: Vec<u8>,
    pointer: Pointer,
    dropped: u64,
}

impl ShardEngine {
    fn fold_state(&self) -> &FoldState {
        &self.layout.seal.fold
    }

    /// The fold loop: once the sealer's open finished, a fold whenever the
    /// period passed with anything unfolded or a directory outgrew its line,
    /// then the merges (`gc` deletes the objects they merged away).
    pub(in crate::shard) async fn fold_loop(self: Arc<Self>) {
        if self.seal_ready().await.is_err() {
            return;
        }
        let fs = self.fold_state();
        *lock(&fs.last_fold) = Some(Instant::now());
        loop {
            let tick = fs.settings().tick;
            tokio::select! {
                () = self.closed() => {}
                () = tokio::time::sleep(tick) => {}
                () = fs.wake.notified() => {}
            }
            if self.is_closed() {
                return;
            }
            if !fs.settings().auto {
                continue;
            }
            if self.fold_due() {
                self.until_closed(self.fold_logged()).await;
            }
            self.until_closed(self.merge_logged()).await;
        }
    }

    /// Whether a fold is due: the period passed with anything in the
    /// directory, or this shard's directory or the process's directories
    /// are past their lines (this shard then holding more than its share).
    pub(super) fn fold_due(&self) -> bool {
        let s = self.fold_state().settings();
        let (bytes, entries) = match self.layout.seal.directory.read() {
            Ok(d) => (d.bytes(), d.entries()),
            Err(_) => return false,
        };
        let elapsed = lock(&self.fold_state().last_fold).is_none_or(|t| t.elapsed() >= s.period);
        let unfolded = entries > 0 || !lock(&self.fold_state().unfolded_orphans).is_empty();
        fold_rule(
            elapsed && unfolded,
            bytes,
            directory::process_bytes(),
            s.directory_bytes,
        )
    }

    /// Folds until nothing is left for the next (`max_rows`).
    pub(super) async fn fold_logged(self: &Arc<Self>) {
        loop {
            match self.fold_once().await {
                Ok(r) if r.more => continue,
                Ok(r) => {
                    tracing::debug!(shard = %self.prefix, object = ?r.object, rows = r.rows, entries = r.entries, bytes = r.bytes, "folded");
                }
                Err(e) => {
                    tracing::warn!(shard = %self.prefix, closed = self.is_closed(), "fold failed: {e}");
                }
            }
            return;
        }
    }

    /// One fold (F1-F7).
    pub(crate) async fn fold_once(self: &Arc<Self>) -> Result<FoldReport, FoldError> {
        let fs = self.fold_state();
        let _one = fs.serial.lock().await;
        self.seal_ready()
            .await
            .map_err(|e| FoldError::Store(e.to_string()))?;
        let result = self.fold_steps().await;
        *lock(&fs.last_fold) = Some(Instant::now());
        let counter = if result.is_ok() {
            &fs.counters.folds
        } else {
            &fs.counters.failures
        };
        counter.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn fold_steps(self: &Arc<Self>) -> Result<FoldReport, FoldError> {
        let fs = self.fold_state();
        // (F1)
        let scan = self.scan_unfolded(fs.settings().max_rows).await?;
        if scan.rows.is_empty() {
            return Ok(FoldReport::default());
        }
        // (F2)
        let claim = self.claim_object();
        let id = claim.id;
        let hashes: std::collections::BTreeSet<[u8; 16]> = scan
            .rows
            .iter()
            .flat_map(|(_, entries)| entries.iter().map(|(hash, _)| *hash))
            .collect();
        let reclaimed = self.reclaimed_heads(hashes).await?;
        let built = build_fold(id, &scan, &reclaimed)?;
        // (F3), (F4)
        if let Some(built) = &built {
            self.put_verified(&built.bytes, &built.pointer).await?;
        }
        #[cfg(test)]
        fs.check_abort(test_hooks::FoldStep::Verified)?;
        // (F5)
        let mut batch = WriteBatch::new();
        if let Some(built) = &built {
            batch.put(
                runindex::pointer_key(id),
                runindex::encode_pointer(&built.pointer),
            );
        }
        for (row, _) in &scan.rows {
            batch.delete(directory::directory_key(*row));
        }
        for (row, _) in &scan.orphans {
            batch.delete(runindex::orphan_key(*row));
        }
        self.write_index_batch(batch).await?;
        #[cfg(test)]
        fs.check_abort(test_hooks::FoldStep::Committed)?;
        // (F6), (F7)
        if let Some(built) = &built {
            fs.replace_set(|set| {
                set.insert(id, built.pointer.clone());
            });
        }
        self.unpublish_folded(&scan);
        {
            let mut unfolded = lock(&fs.unfolded_orphans);
            for (row, _) in &scan.orphans {
                unfolded.remove(row);
            }
        }
        let report = FoldReport {
            object: built.as_ref().map(|_| id),
            rows: scan.rows.len(),
            entries: built.as_ref().map_or(0, |b| b.pointer.entries),
            dropped: built.as_ref().map_or(0, |b| b.dropped),
            bytes: built.as_ref().map_or(0, |b| b.pointer.len),
            more: scan.more,
        };
        let c = &fs.counters;
        c.rows_folded
            .fetch_add(report.rows as u64, Ordering::Relaxed);
        c.entries_folded
            .fetch_add(report.entries, Ordering::Relaxed);
        c.entries_dropped
            .fetch_add(report.dropped, Ordering::Relaxed);
        Ok(report)
    }

    /// The `reclaimed` offset of each of `hashes` that is a dead stream
    /// (`spill::gc`): a fold or merge drops the entries ending below it,
    /// every one of them already decremented. One point read per stream
    /// (an absent `'H'` row costs a bloom probe).
    pub(crate) async fn reclaimed_heads(
        &self,
        hashes: impl IntoIterator<Item = [u8; 16]>,
    ) -> Result<Reclaimed, FoldError> {
        let mut out = Reclaimed::new();
        if !self.gc_state().heads_exist() {
            return Ok(out);
        }
        for hash in hashes {
            let head = self
                .head_row(&hash)
                .await
                .map_err(|e| FoldError::Store(e.to_string()))?;
            if let Some(h) = head.filter(|h| h.reclaimed > 0) {
                out.insert(hash, h.reclaimed);
            }
        }
        Ok(out)
    }

    /// F1: the oldest `max` directory rows and every orphan row; rows that
    /// do not decode are counted and skipped.
    async fn scan_unfolded(&self, max: usize) -> Result<Scan, FoldError> {
        let store = |e: slatedb::Error| FoldError::Store(e.to_string());
        let mut scan = Scan::default();
        let prefix = directory::directory_prefix();
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        while let Some(kv) = rows.next().await.map_err(store)? {
            if scan.rows.len() >= max {
                scan.more = true;
                break;
            }
            let decoded = directory::parse_directory_key(&kv.key)
                .and_then(|id| Some((id, directory::decode_row(id, &kv.value)?)));
            match decoded {
                Some(row) => scan.rows.push(row),
                None => self.note_corrupt_index_row("directory", &kv.key),
            }
        }
        let prefix = sentinel_prefix(ORPHAN_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        while let Some(kv) = rows.next().await.map_err(store)? {
            let decoded = parse_sentinel_key(ORPHAN_TAG, &kv.key)
                .and_then(|id| Some((id, runindex::decode_orphans(&kv.value)?)));
            match decoded {
                Some(row) => scan.orphans.push(row),
                None => self.note_corrupt_index_row("orphan", &kv.key),
            }
        }
        Ok(scan)
    }

    /// A row of the index the open or a fold could not decode: counted and
    /// logged once per key, left in place.
    pub(crate) fn note_corrupt_index_row(&self, kind: &str, key: &[u8]) {
        if !lock(&self.fold_state().corrupt).insert(key.to_vec()) {
            return;
        }
        let c = &self.fold_state().counters;
        c.corrupt_rows.fetch_add(1, Ordering::Relaxed);
        if kind != "orphan" {
            c.corrupt_index_rows.fetch_add(1, Ordering::Relaxed);
        }
        tracing::error!(shard = %self.prefix, key = %crate::crypto::hex(key), "skipped a {kind} row that does not decode");
    }

    /// F3 and F4: PUT the object, then read its trailer and footer back.
    pub(crate) async fn put_verified(
        &self,
        bytes: &[u8],
        pointer: &Pointer,
    ) -> Result<(), FoldError> {
        let fs = self.fold_state();
        #[cfg(test)]
        fs.before_put()?;
        let path = object_store::path::Path::from(self.run_index_path(pointer.id));
        self.data_store
            .put_opts(
                &path,
                PutPayload::from(bytes.to_vec()),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| FoldError::Store(e.to_string()))?;
        fs.counters.objects.fetch_add(1, Ordering::Relaxed);
        fs.counters
            .object_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        #[cfg(test)]
        fs.check_abort(test_hooks::FoldStep::Put)?;
        let footer = self
            .fetch_footer(pointer)
            .await
            .map_err(|e| FoldError::Verify(format!("{e:?}")))?;
        if footer.len() as u64 != pointer.runs {
            return Err(FoldError::Verify(format!(
                "{} runs read back, {} written",
                footer.len(),
                pointer.runs
            )));
        }
        Ok(())
    }

    pub(crate) fn run_index_path(&self, id: ObjectId) -> String {
        runindex::run_index_path(&crate::sharddir::shard_db_path(&self.prefix), id)
    }

    /// One shard-log batch outside the committer (a fold, a merge, a
    /// consolidation): applied, then a WAL flush (no commit group may be
    /// pending to flush it), then durable. Any error leaves its outcome
    /// unknown, so the engine closes.
    pub(crate) async fn write_index_batch(&self, batch: WriteBatch) -> Result<(), FoldError> {
        #[cfg(test)]
        self.fold_state().before_batch().await?;
        let result = async {
            let handle = self.db.write(batch).await?;
            self.wal_flush().await?;
            handle.await_durable().await
        }
        .await;
        result.map_err(|e| {
            tracing::error!(shard = %self.prefix, "an index batch failed, its outcome unknown; closing: {e}");
            self.begin_close();
            FoldError::Batch(e.to_string())
        })
    }

    /// F7: the folded rows' entries leave the directory.
    fn unpublish_folded(&self, scan: &Scan) {
        let mut dir = self
            .layout
            .seal
            .directory
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for (_, entries) in &scan.rows {
            for (hash, e) in entries {
                dir.remove(hash, e.last, e.object);
            }
        }
    }

    /// The open's run-index set from the pointer rows, and the unfolded
    /// orphan rows; rows that do not decode are counted and skipped.
    pub(in crate::shard::seal) async fn rebuild_run_index(&self) -> anyhow::Result<()> {
        let fs = self.fold_state();
        let mut set = BTreeMap::new();
        let prefix = sentinel_prefix(POINTER_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await?;
        while let Some(kv) = rows.next().await? {
            let decoded = parse_sentinel_key(POINTER_TAG, &kv.key)
                .and_then(|id| runindex::decode_pointer(id, &kv.value));
            match decoded {
                Some(p) => {
                    set.insert(p.id, p);
                }
                None => self.note_corrupt_index_row("pointer", &kv.key),
            }
        }
        let mut orphans = BTreeMap::new();
        let prefix = sentinel_prefix(ORPHAN_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await?;
        while let Some(kv) = rows.next().await? {
            let decoded = parse_sentinel_key(ORPHAN_TAG, &kv.key)
                .and_then(|id| Some((id, runindex::decode_orphans(&kv.value)?)));
            match decoded {
                Some((id, o)) => {
                    orphans.insert(id, o);
                }
                None => self.note_corrupt_index_row("orphan", &kv.key),
            }
        }
        *fs.set.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(set);
        *lock(&fs.unfolded_orphans) = orphans;
        Ok(())
    }
}

/// Whether a fold is due: the period passed with anything unfolded, the
/// shard's directory holds `directory_bytes`, or the process's directories
/// are past their line and this shard holds more than an eighth of its own.
pub(super) fn fold_rule(period_due: bool, bytes: u64, process: u64, directory_bytes: u64) -> bool {
    period_due
        || bytes >= directory_bytes
        || (process >= PROCESS_DIRECTORY_BYTES && bytes >= DIRECTORY_FOLD_BYTES / 8)
}

/// Each dead stream's `reclaimed` offset.
pub(crate) type Reclaimed = BTreeMap<[u8; 16], u64>;

/// F2: one stream's entries, sorted for and stripped of contained ones and
/// of those ending below its `reclaimed` offset.
pub(super) fn kept_entries(mut entries: Vec<DirEntry>, reclaimed: u64) -> (Vec<DirEntry>, u64) {
    let before = entries.len();
    entries.retain(|e| e.last >= reclaimed);
    let dead = (before - entries.len()) as u64;
    sort_for_drop(&mut entries, span_of);
    let spans: Vec<Span> = entries.iter().map(span_of).collect();
    let keep = drop_covered(&spans);
    let dropped = (entries.len() - keep.len()) as u64 + dead;
    let mut keep = keep.into_iter().peekable();
    let kept = entries
        .into_iter()
        .enumerate()
        .filter(|(i, _)| keep.next_if_eq(i).is_some())
        .map(|(_, e)| e)
        .collect();
    (kept, dropped)
}

pub(crate) fn span_of(e: &DirEntry) -> Span {
    Span {
        first: e.first,
        last: e.last,
    }
}

/// F2: the object of one fold's rows, and its pointer row; `None` when no
/// entry is left.
fn build_fold(
    id: ObjectId,
    scan: &Scan,
    reclaimed: &Reclaimed,
) -> Result<Option<Built>, FoldError> {
    let mut streams: BTreeMap<[u8; 16], Vec<DirEntry>> = BTreeMap::new();
    for (_, entries) in &scan.rows {
        for (hash, e) in entries {
            streams.entry(*hash).or_default().push(e.clone());
        }
    }
    let orphans = scan
        .orphans
        .iter()
        .fold(Orphans::default(), |a, (_, o)| a.add(*o));
    let mut writer = ObjectWriter::new();
    let mut dropped = 0;
    for (hash, entries) in streams {
        let (kept, d) = kept_entries(entries, reclaimed.get(&hash).copied().unwrap_or(0));
        dropped += d;
        if !kept.is_empty() {
            writer.push(hash, &kept).map_err(FoldError::Format)?;
        }
    }
    let seals = scan.rows.len() as u64;
    let built = finish(id, writer, 0, seals, orphans)?;
    Ok(built.map(|b| Built { dropped, ..b }))
}

/// The object's last bytes and its pointer row; `None` for an object of
/// no run.
fn finish(
    id: ObjectId,
    writer: ObjectWriter,
    level: u8,
    seals: u64,
    orphans: Orphans,
) -> Result<Option<Built>, FoldError> {
    let Some((bytes, trailer)) = writer.finish(level).map_err(FoldError::Format)? else {
        return Ok(None);
    };
    let pointer = Pointer {
        id,
        level,
        len: trailer.footer_off + u64::from(trailer.footer_len) + TRAILER_LEN as u64,
        footer_off: trailer.footer_off,
        footer_len: trailer.footer_len,
        runs: u64::from(trailer.runs),
        entries: trailer.entries,
        seals,
        orphans,
        created_ms: crate::shard::now_ms().unsigned_abs(),
    };
    Ok(Some(Built {
        bytes,
        pointer,
        dropped: 0,
    }))
}
