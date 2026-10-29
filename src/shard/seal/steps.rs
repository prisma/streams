//! One seal's steps (`ShardEngine::seal_once`, the module's steps 1-7).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use object_store::{PutMode, PutOptions, PutPayload};

use super::{SealState, Trigger};
use crate::crypto_page::CheckedPage;
use crate::shard::{CommitOp, ShardEngine, StreamHandle};
use crate::spill::directory::{self, DirEntry, KeySummary};
use crate::spill::object::{self, ChunkInput, ObjectError, ObjectId};
use crate::spill::pending::PendingPage;

/// One stream's advance in a seal's commit.
#[derive(Clone, Debug)]
pub(crate) struct SealAdvance {
    pub(crate) hash: [u8; 16],
    /// The stream's sealed boundary the snapshot found.
    pub(crate) from: u64,
    /// Its new boundary: every page below it is in the object.
    pub(crate) upto: u64,
    /// The sealed pages' stored bytes.
    pub(crate) bytes: u64,
    /// Tests (REVIEW-6A F1 negative control): the remainder as a seal task
    /// would pre-build it, from the pages the snapshot knew.
    #[cfg(test)]
    pub(crate) snapshot_remainder: Vec<PendingPage>,
}

/// A seal's commit, staged by the committer (`transaction::seal`).
pub(crate) struct SealCommit {
    pub(crate) dir_key: Vec<u8>,
    pub(crate) dir_value: Vec<u8>,
    pub(crate) ref_key: Vec<u8>,
    pub(crate) ref_value: Vec<u8>,
    pub(crate) streams: Vec<SealAdvance>,
    /// Answered with the refusal when the committer refuses the seal;
    /// dropped with the group's durable effects when it accepts it.
    pub(crate) resp: tokio::sync::oneshot::Sender<String>,
}

#[derive(Debug)]
pub(crate) enum SealError {
    Closed,
    Store(String),
    Encode(ObjectError),
    /// The committer refused the commit: a stream moved since the snapshot.
    Refused(String),
    /// A test stopped the seal at a step.
    #[cfg(test)]
    Aborted(super::test_hooks::SealStep),
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "the engine closed"),
            Self::Store(e) => write!(f, "store: {e}"),
            Self::Encode(e) => write!(f, "object encoding: {e:?}"),
            Self::Refused(e) => write!(f, "commit refused: {e}"),
            #[cfg(test)]
            Self::Aborted(step) => write!(f, "aborted at {step:?}"),
        }
    }
}

/// What one seal did.
#[derive(Clone, Debug, Default)]
pub(crate) struct SealReport {
    pub(crate) object: Option<ObjectId>,
    pub(crate) object_bytes: u64,
    pub(crate) streams: usize,
    pub(crate) chunks: usize,
    pub(crate) pages: u64,
    pub(crate) page_bytes: u64,
    /// The bytes of the pending rows' remainders the commit put.
    pub(crate) remainder_bytes: u64,
}

/// One stream's window to seal: its durable unsealed pages.
struct Window {
    handle: Arc<StreamHandle>,
    from: u64,
    upto: u64,
    /// The budget cut the window short of the stream's durable end.
    cut: bool,
    pages: Vec<PendingPage>,
    #[cfg(test)]
    applied_beyond: Vec<PendingPage>,
}

impl ShardEngine {
    /// One seal of this shard (steps 1-7), one at a time.
    pub(crate) async fn seal_once(
        self: &Arc<Self>,
        trigger: Trigger,
    ) -> Result<SealReport, SealError> {
        let sp = &self.layout.seal;
        let _serial = sp.serial.lock().await;
        tracing::trace!(shard = %self.prefix, ?trigger, "seal starts");
        let result = self.seal_steps(sp).await;
        let counter = match &result {
            Ok(r) if r.object.is_none() => &sp.counters.empty_seals,
            Ok(_) => &sp.counters.seals,
            Err(SealError::Refused(_)) => &sp.counters.refused,
            Err(_) => &sp.counters.failures,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn seal_steps(self: &Arc<Self>, sp: &SealState) -> Result<SealReport, SealError> {
        // (1)
        self.catch_up_durable().await?;
        #[cfg(test)]
        sp.check_abort(super::test_hooks::SealStep::WalFlushed)?;
        // (2)
        let snapshot_at = Instant::now();
        let windows = self.seal_snapshot();
        let mut report = SealReport::default();
        if !windows.is_empty() {
            self.seal_windows(sp, &windows, &mut report).await?;
        }
        // (7b), after (7a) in `seal_windows`.
        #[cfg(test)]
        sp.check_abort(super::test_hooks::SealStep::Durable)?;
        // An empty seal's memtable (queue rows, a degraded window's
        // operands) takes step 7a in the flush itself: REVIEW-6A F2.
        self.forced_flush(windows.is_empty())
            .await
            .map_err(SealError::Store)?;
        for w in &windows {
            self.layout.release_sealed(&w.handle.hash);
        }
        self.note_leftover(snapshot_at);
        Ok(report)
    }

    /// Step 1: one awaited WAL flush, then a wait (at most 1 s) until
    /// durable dispatch published every group it covered, so each stream's
    /// durable frontier meets its applied one when step 2 reads it. The
    /// dispatch gate is taken last: an empty handoff does not mean the
    /// frontiers are published.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::catch_up_durable; a poisoned handoff may hold a half-published group; recovering it could snapshot a frontier no dispatch published, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    async fn catch_up_durable(&self) -> Result<(), SealError> {
        let target = self
            .in_flight
            .lock()
            .unwrap()
            .pending()
            .last()
            .map(|g| g.seq);
        self.wal_flush()
            .await
            .map_err(|e| SealError::Store(e.to_string()))?;
        let until = Instant::now() + Duration::from_secs(1);
        while let Some(target) = target {
            let behind = self
                .in_flight
                .lock()
                .unwrap()
                .pending()
                .first()
                .is_some_and(|g| g.seq <= target);
            if !behind || Instant::now() >= until {
                break;
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(self.dispatch_gate.lock().await);
        Ok(())
    }

    /// Step 2: each unsealed stream's durable pages, from its handle (never
    /// from the store).
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::seal_snapshot; a poisoned stream state may hold half-applied pages or a half-advanced boundary; recovering it could seal pages no commit made durable, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn seal_snapshot(&self) -> Vec<Window> {
        let sp = &self.layout.seal;
        let settings = sp.settings();
        let mut budget = Budget::new(settings.max_object_bytes, settings.max_streams);
        let mut out = Vec::new();
        let mut capped = false;
        for handle in self.rotated_unsealed() {
            if handle.native_queue.load(Ordering::Relaxed) {
                continue;
            }
            if budget.spent() {
                capped = true;
                break;
            }
            let Some(window) = self.window_of(handle, &mut budget) else {
                continue;
            };
            capped |= window.cut;
            out.push(window);
        }
        *sp.cursor.lock().unwrap() = capped.then(|| out.last().map(|w| w.handle.hash)).flatten();
        if capped {
            sp.deferred.store(true, Ordering::Release);
        }
        out
    }

    /// The unsealed streams, starting after the last stream a capped seal
    /// took.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::rotated_unsealed; a poisoned cursor may hold a half-written stream; recovering it could start every seal at the same stream and starve the others, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn rotated_unsealed(&self) -> Vec<Arc<StreamHandle>> {
        let mut handles = self.layout.unsealed();
        if let Some(after) = *self.layout.seal.cursor.lock().unwrap() {
            let split = handles.partition_point(|h| h.hash <= after);
            handles.rotate_left(split);
        }
        handles
    }

    /// One stream's durable unsealed pages within what is left of the
    /// budget; None when it has none, or they do not tile its window.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::window_of; a poisoned stream state may hold half-applied pages or a half-advanced boundary; recovering it could seal pages no commit made durable, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn window_of(&self, handle: Arc<StreamHandle>, budget: &mut Budget) -> Option<Window> {
        let st = handle.state.lock().unwrap();
        let (from, next) = (st.durable.absorbed, st.durable.next);
        if from >= next {
            return None;
        }
        let (mut pages, due) = durable_pages(&st.pending, from, next);
        if due != next {
            // The pages tile [absorbed, next) while the handle holds them
            // and the durable frontier trails the applied one.
            self.layout
                .seal
                .counters
                .snapshot_gaps
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                shard = %self.prefix,
                stream = %crate::crypto::hex(&handle.hash[..4]),
                "unsealed pages do not tile [{from}, {next}) at {due}; not sealed"
            );
            return None;
        }
        pages.truncate(budget.take(&pages));
        let upto = pages.last().map(|(_, last, _)| last + 1)?;
        #[cfg(test)]
        let applied_beyond = st.pending.from(upto).cloned().collect();
        drop(st);
        Some(Window {
            handle,
            from,
            upto,
            cut: upto < next,
            pages,
            #[cfg(test)]
            applied_beyond,
        })
    }

    /// Steps 3-7a for a non-empty snapshot.
    async fn seal_windows(
        self: &Arc<Self>,
        sp: &SealState,
        windows: &[Window],
        report: &mut SealReport,
    ) -> Result<(), SealError> {
        // (3)
        let inputs = chunk_inputs(windows, sp.settings().max_pages_per_chunk.max(1));
        let summaries = inputs
            .iter()
            .map(KeySummary::of)
            .collect::<Result<Vec<_>, _>>()
            .map_err(SealError::Store)?;
        let encoded = object::encode(&inputs).map_err(SealError::Encode)?;
        let id = ObjectId {
            epoch: self.writer_epoch,
            seq: sp.next_seq.fetch_add(1, Ordering::Relaxed) + 1,
        };
        // (4)
        self.put_object(sp, id, &encoded).await?;
        *report = SealReport {
            object: Some(id),
            object_bytes: encoded.len as u64,
            streams: windows.len(),
            chunks: inputs.len(),
            ..SealReport::default()
        };
        #[cfg(test)]
        sp.check_abort(super::test_hooks::SealStep::Put)?;
        #[cfg(test)]
        sp.before_commit().await;
        // (5)
        let entries: Vec<([u8; 16], DirEntry)> = encoded
            .chunks
            .iter()
            .zip(&summaries)
            .map(|(c, keys)| (c.entry.hash, DirEntry::of(id, c, keys.clone())))
            .collect();
        #[cfg(test)]
        let late = sp.late_directory();
        #[cfg(not(test))]
        let late = false;
        if !late {
            self.publish_directory(&entries);
        }
        // (6)
        let rows: Vec<_> = encoded.chunks.iter().zip(&summaries).collect();
        let streams = advances(windows, report);
        sp.counters
            .page_bytes
            .fetch_add(report.page_bytes, Ordering::Relaxed);
        let remainder_before = sp.counters.remainder_bytes.load(Ordering::Relaxed);
        let (resp, answer) = tokio::sync::oneshot::channel();
        let commit = SealCommit {
            dir_key: directory::directory_key(id),
            dir_value: directory::encode_row(&rows),
            ref_key: directory::refcount_key(id),
            ref_value: directory::refcount_value(
                report.page_bytes,
                inputs.len() as u64,
                crate::shard::now_ms().unsigned_abs(),
            ),
            streams,
            resp,
        };
        if let Err(e) = self.submit_seal(commit, answer, windows).await {
            if !late {
                self.withdraw_directory(&entries);
            }
            return Err(e);
        }
        report.remainder_bytes = sp
            .counters
            .remainder_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(remainder_before);
        #[cfg(test)]
        sp.check_abort(super::test_hooks::SealStep::Applied)?;
        // (7a) The commit is applied; make it durable and published.
        #[cfg(test)]
        let with_7a = !sp.skips_7a();
        #[cfg(not(test))]
        let with_7a = true;
        if with_7a {
            self.wal_flush()
                .await
                .map_err(|e| SealError::Store(e.to_string()))?;
            self.wait_sealed_durable(windows).await?;
        }
        if late {
            self.publish_directory(&entries);
        }
        Ok(())
    }

    /// Step 4: the segment object's PUT, create-only, metered.
    async fn put_object(
        &self,
        sp: &SealState,
        id: ObjectId,
        encoded: &object::EncodedObject,
    ) -> Result<(), SealError> {
        #[cfg(test)]
        sp.before_put().await?;
        let path = object::object_path(&crate::sharddir::shard_db_path(&self.prefix), id);
        self.data_store
            .put_opts(
                &object_store::path::Path::from(path),
                // The body's slices as one payload, still without a copy.
                encoded.parts.iter().cloned().collect::<PutPayload>(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SealError::Store(e.to_string()))?;
        sp.counters.objects.fetch_add(1, Ordering::Relaxed);
        sp.counters
            .object_bytes
            .fetch_add(encoded.len as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Step 6: the seal's commit, until it is applied. The committer
    /// answers a refusal at once and drops an accepted seal's reply with
    /// its group, so a reply dropped before every stream's applied
    /// boundary moved is a group that never applied.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::submit_seal; a poisoned stream state may hold a half-advanced boundary; recovering it could report a seal applied that no group applied, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    async fn submit_seal(
        &self,
        commit: SealCommit,
        mut answer: tokio::sync::oneshot::Receiver<String>,
        windows: &[Window],
    ) -> Result<(), SealError> {
        use tokio::sync::oneshot::error::TryRecvError;
        self.tx
            .send(CommitOp::Seal(Box::new(commit)))
            .await
            .map_err(|_| SealError::Closed)?;
        #[cfg(test)]
        self.layout
            .seal
            .hooks
            .commits_sent
            .fetch_add(1, Ordering::Release);
        let applied = || {
            windows
                .iter()
                .all(|w| w.handle.state.lock().unwrap().applied.absorbed >= w.upto)
        };
        loop {
            if applied() {
                return Ok(());
            }
            match answer.try_recv() {
                Ok(why) => return Err(SealError::Refused(why)),
                Err(TryRecvError::Closed) if applied() => return Ok(()),
                Err(TryRecvError::Closed) => {
                    return Err(SealError::Store("the seal's group was not applied".into()));
                }
                Err(TryRecvError::Empty) => {}
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::publish_directory; a poisoned directory may hold a half-inserted seal; recovering it could serve a sealed range from chunks no seal finished publishing, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn publish_directory(&self, entries: &[([u8; 16], DirEntry)]) {
        let mut dir = self.layout.seal.directory.write().unwrap();
        for (hash, entry) in entries {
            dir.insert(*hash, entry.clone());
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::withdraw_directory; a poisoned directory may hold a half-inserted seal; recovering it could serve a sealed range from chunks no seal finished publishing, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn withdraw_directory(&self, entries: &[([u8; 16], DirEntry)]) {
        let mut dir = self.layout.seal.directory.write().unwrap();
        for (hash, entry) in entries {
            dir.remove(hash, entry.last, entry.object);
        }
    }

    /// Wait until durable dispatch published each sealed stream's new
    /// boundary.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::wait_sealed_durable; a poisoned stream state may hold a half-advanced boundary; recovering it could flush the memtable before the seal is durable, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    async fn wait_sealed_durable(&self, windows: &[Window]) -> Result<(), SealError> {
        loop {
            let done = windows
                .iter()
                .all(|w| w.handle.state.lock().unwrap().durable.absorbed >= w.upto);
            if done {
                return Ok(());
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// After a seal: the oldest unsealed page is at most as old as the
    /// snapshot while any held stream still has a page in its pending row,
    /// applied or durable. A stream first applied during the seal is not
    /// durable yet, and its group's durability stamps no age, so counting
    /// only durable pages would leave its pages with no age trigger or
    /// deadline until another write (S16c).
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::note_leftover; a poisoned age lock or stream state may hold a half-written value; recovering it could let the age trigger and deadline miss an unsealed page, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn note_leftover(&self, snapshot_at: Instant) {
        let leftover = self.layout.unsealed().iter().any(|h| {
            let st = h.state.lock().unwrap();
            #[cfg(test)]
            if let Some(control) = self.layout.seal.durable_only_leftover(&st) {
                return control;
            }
            !st.pending.is_empty()
        });
        *self.layout.seal.oldest_unsealed.lock().unwrap() = leftover.then_some(snapshot_at);
    }
}

/// What is left of one seal's budget.
struct Budget {
    bytes: u64,
    streams: usize,
    first: bool,
}

impl Budget {
    fn new(bytes: u64, streams: usize) -> Self {
        Self {
            bytes,
            streams,
            first: true,
        }
    }

    fn spent(&self) -> bool {
        self.bytes == 0 || self.streams == 0
    }

    /// How many of `pages` (a stream's, oldest first) fit what is left,
    /// taking them out of it; the first stream of a seal takes at least one
    /// page, so every seal makes progress.
    fn take(&mut self, pages: &[PendingPage]) -> usize {
        let mut n = 0;
        for (_, _, raw) in pages {
            let len = raw.len() as u64;
            if len > self.bytes && !(self.first && n == 0) {
                break;
            }
            self.bytes = self.bytes.saturating_sub(len);
            n += 1;
        }
        if n > 0 {
            self.streams -= 1;
            self.first = false;
        }
        n
    }
}

/// The pages tiling `[from, upto)` from `from` on, and the offset after the
/// last of them (`upto` when they tile it whole).
fn durable_pages(
    pending: &crate::spill::pending::PendingPages,
    from: u64,
    upto: u64,
) -> (Vec<PendingPage>, u64) {
    let mut pages = Vec::new();
    let mut due = from;
    for (first, last, raw) in pending.from(from) {
        if *first != due || *last >= upto {
            break;
        }
        pages.push((*first, *last, raw.clone()));
        due = last + 1;
    }
    (pages, due)
}

/// One chunk per run of at most `max_pages` pages of a stream.
fn chunk_inputs(windows: &[Window], max_pages: usize) -> Vec<ChunkInput> {
    let mut inputs = Vec::new();
    for window in windows {
        for run in window.pages.chunks(max_pages) {
            let Some(&(first, _, _)) = run.first() else {
                continue;
            };
            let max_ts = run
                .iter()
                .filter_map(|(_, last, raw)| CheckedPage::admit(raw.clone(), *last).ok())
                .map(|p| p.ts_ms())
                .max()
                .unwrap_or(0);
            inputs.push(ChunkInput {
                hash: window.handle.hash,
                first,
                max_ts,
                pages: run
                    .iter()
                    .map(|(_, last, raw)| (*last, raw.clone()))
                    .collect(),
            });
        }
    }
    inputs
}

/// Each window's advance, counted into `report`.
fn advances(windows: &[Window], report: &mut SealReport) -> Vec<SealAdvance> {
    windows
        .iter()
        .map(|w| {
            let bytes: u64 = w.pages.iter().map(|(_, _, raw)| raw.len() as u64).sum();
            report.pages += w.pages.len() as u64;
            report.page_bytes += bytes;
            SealAdvance {
                hash: w.handle.hash,
                from: w.from,
                upto: w.upto,
                bytes,
                #[cfg(test)]
                snapshot_remainder: w.applied_beyond.clone(),
            }
        })
        .collect()
}
