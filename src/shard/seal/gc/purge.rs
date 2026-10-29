//! A dead incarnation's purge (DESIGN-pkg6 §3, §6): asked for durably
//! (`request_purge`, an `'H'` row in state Requested), then, once its time
//! came, two steps:
//! 1. **Purge** (the committer, `transaction::seal`): the stream's tail and
//!    pending rows deleted and its `'H'` row Purged with `head` = its next
//!    offset, in one group; its handle leaves the engine, and a later load
//!    of the stream fails (`ShardEngine::purged_stream`), so no append
//!    revives it. Refused while the stream has another op in the group or
//!    its usage awaits the ledger (f3 keeps the counters on the tail).
//! 2. **Reclaim** (under `refs`): every unreclaimed chunk of the stream
//!    ending below `head` decremented on its object, and `reclaimed` moved
//!    to `head`, in one batch. A crash between the steps leaves the `'H'`
//!    row owing, and the next pass reclaims.
//!
//! Folds and merges drop a stream's entries below `reclaimed` (they were
//! decremented), never below `head`.

use std::sync::atomic::Ordering;

use slatedb::WriteBatch;

use super::GcError;
use crate::shard::ShardEngine;
use crate::shard::seal::SealCommit;
use crate::spill::directory::DirEntry;
use crate::spill::gc::{self, Decrements, Head, HeadState};

/// What the committer stages for a purge (`SealCommit::Purge`).
pub(crate) struct PurgeStep {
    pub(crate) hash: [u8; 16],
    /// The `'H'` row as the purge found it (Requested, or Purged with
    /// nothing owed).
    pub(crate) prior: Head,
    /// Answered with the refusal; dropped with the group's durable effects
    /// when accepted.
    pub(crate) resp: tokio::sync::oneshot::Sender<String>,
}

/// How a purge went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PurgeOutcome {
    /// Nothing asked for, or its time has not come.
    NotDue,
    /// The committer refused (retried next pass).
    Refused,
    /// Purged and reclaimed: `chunks` decremented.
    Reclaimed { chunks: u64 },
}

impl ShardEngine {
    /// Ask for the purge of a dead incarnation's segment `hash`, closed at
    /// `close_ms`: it runs once `purge_delay` passed that instant.
    /// Idempotent: a row already there is kept (an earlier instant stays; a
    /// purged stream stays purged).
    pub(crate) async fn request_purge(&self, hash: [u8; 16], close_ms: u64) -> Result<(), GcError> {
        let gc = self.gc_state();
        let not_before_ms = close_ms.saturating_add(super::duration_ms(gc.settings().purge_delay));
        let _refs = gc.refs.lock().await;
        if self.head_row(&hash).await?.is_some() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        batch.put(
            gc::head_key(&hash),
            gc::encode_head(&Head::requested(not_before_ms)),
        );
        self.write_index_batch(batch)
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        gc.signal_heads();
        gc.counters.purge_requests.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// The stream's `'H'` row, if any.
    pub(crate) async fn head_row(&self, hash: &[u8; 16]) -> Result<Option<Head>, GcError> {
        let raw = self
            .db
            .get(gc::head_key(hash))
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        Ok(raw.as_deref().and_then(gc::decode_head))
    }

    /// The open's probe: whether any `'H'` row exists (one seek), so folds
    /// and merges of a shard that never purged anything read none.
    pub(crate) async fn probe_heads(&self) -> Result<(), slatedb::Error> {
        let prefix = crate::spill::runindex::sentinel_prefix(gc::HEAD_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await?;
        if rows.next().await?.is_some() {
            self.gc_state().signal_heads();
        }
        Ok(())
    }

    /// Whether `hash` was purged: a load of its handle fails then.
    pub(crate) async fn purged_stream(&self, hash: &[u8; 16]) -> Result<bool, slatedb::Error> {
        if self.gc_state().purged(hash) {
            return Ok(true);
        }
        let raw = self.db.get(gc::head_key(hash)).await?;
        Ok(raw
            .as_deref()
            .and_then(gc::decode_head)
            .is_some_and(|h| h.state == HeadState::Purged))
    }

    /// Every `'H'` row, by stream.
    pub(crate) async fn head_rows(&self) -> Result<Vec<([u8; 16], Head)>, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        let prefix = crate::spill::runindex::sentinel_prefix(gc::HEAD_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        let mut out = Vec::new();
        while let Some(kv) = rows.next().await.map_err(store)? {
            match gc::parse_head_key(&kv.key).zip(gc::decode_head(&kv.value)) {
                Some(row) => out.push(row),
                None => {
                    self.gc_state()
                        .counters
                        .corrupt_rows
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(out)
    }

    /// Every purge whose time came, and every reclaim owed.
    /// Returns how many dead incarnations still wait (not yet due, refused,
    /// or owing).
    pub(crate) async fn purge_due(&self) -> Result<u64, GcError> {
        let now = crate::shard::now_ms().unsigned_abs();
        let mut waiting = 0;
        for (hash, head) in self.head_rows().await? {
            let due = head.state == HeadState::Requested && head.not_before_ms <= now;
            if !due && !head.owes() {
                waiting += u64::from(head.state == HeadState::Requested);
                continue;
            }
            match self.purge_stream(hash).await {
                Ok(PurgeOutcome::Reclaimed { .. }) => {}
                Ok(_) => waiting += 1,
                Err(e) => {
                    // One stream's failure never holds the others back.
                    waiting += 1;
                    tracing::warn!(shard = %self.prefix, stream = %crate::crypto::hex(&hash[..4]), "purge stopped: {e}");
                }
            }
        }
        Ok(waiting)
    }

    /// Purge `hash` if its time came, then reclaim what it owes.
    pub(crate) async fn purge_stream(&self, hash: [u8; 16]) -> Result<PurgeOutcome, GcError> {
        let gc = self.gc_state();
        let _refs = gc.refs.lock().await;
        let Some(mut head) = self.head_row(&hash).await? else {
            return Ok(PurgeOutcome::NotDue);
        };
        if head.state == HeadState::Requested {
            if head.not_before_ms > crate::shard::now_ms().unsigned_abs() {
                return Ok(PurgeOutcome::NotDue);
            }
            if self.billing_open(hash).await? || !self.purge_step(hash, head).await? {
                gc.counters.purges_refused.fetch_add(1, Ordering::Relaxed);
                return Ok(PurgeOutcome::Refused);
            }
            gc.counters.purges.fetch_add(1, Ordering::Relaxed);
            // A load that raced the purge's staging may have published a
            // handle after it left the engine: none may stay.
            self.forget_stream(&hash);
            head = self
                .head_row(&hash)
                .await?
                .ok_or_else(|| GcError::Index("a purge left no head row".into()))?;
        }
        let chunks = self.reclaim_locked(hash, head).await?;
        Ok(PurgeOutcome::Reclaimed { chunks })
    }

    /// Whether `hash`'s billing gauge is still open: the purge waits for its
    /// close (after it, the close could not load the stream).
    async fn billing_open(&self, hash: [u8; 16]) -> Result<bool, GcError> {
        let meta = self
            .load_billing_meta(hash)
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        Ok(meta.is_some_and(|m| m.owned_frame_bytes_current > 0))
    }

    /// Step 1 through the committer: true once the group is applied.
    async fn purge_step(&self, hash: [u8; 16], prior: Head) -> Result<bool, GcError> {
        let (resp, mut answer) = tokio::sync::oneshot::channel();
        let step = PurgeStep { hash, prior, resp };
        self.tx
            .send(crate::shard::CommitOp::Seal(Box::new(SealCommit::Purge(
                step,
            ))))
            .await
            .map_err(|_| GcError::Store("the committer closed".into()))?;
        let refusal = loop {
            match answer.try_recv() {
                Ok(why) => break Some(why),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break None,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
            if self.is_closed() {
                return Err(GcError::Store("the engine closed".into()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        };
        if let Some(why) = refusal {
            tracing::debug!(shard = %self.prefix, stream = %crate::crypto::hex(&hash[..4]), "purge refused: {why}");
            return Ok(false);
        }
        // Dropped with the group: applied (then durable or the engine
        // closes), or a group that failed (the engine closes).
        Ok(self
            .head_row(&hash)
            .await?
            .is_some_and(|h| h.state == HeadState::Purged))
    }

    /// Step 2 (the caller holds `refs`): the stream's remaining rows
    /// deleted (`delete_stream_rows`), then every unreclaimed chunk of
    /// `hash` ending below `head` decremented, in offset order, at most
    /// `reclaim_batch` chunks per batch, each batch moving `reclaimed` past
    /// its last chunk (the last to `head`): a crash between batches resumes
    /// exactly where the last one ended.
    async fn reclaim_locked(&self, hash: [u8; 16], head: Head) -> Result<u64, GcError> {
        if !head.owes() {
            return Ok(0);
        }
        self.delete_stream_rows(hash).await?;
        let gc = self.gc_state();
        let batch_max = gc.settings().reclaim_batch.max(1);
        let live = self.live_chunks(hash).await?;
        // A seal in flight publishes its entries before its commit, which the
        // purge refuses: their objects count nothing yet.
        let in_flight = gc.in_flight();
        let counted = |e: &&DirEntry| {
            e.object.epoch != self.writer_epoch || !in_flight.contains(&e.object.seq)
        };
        let due: Vec<&DirEntry> = live
            .iter()
            .filter(|e| e.first >= head.reclaimed && e.last < head.head)
            .filter(counted)
            .collect();
        let mut done = head;
        let mut chunks = 0u64;
        let batches = due.chunks(batch_max);
        let last_batch = batches.len().saturating_sub(1);
        for (n, part) in due.chunks(batch_max).enumerate() {
            let mut decrements = Decrements::new();
            for e in part {
                gc::decrement(&mut decrements, e);
            }
            done.reclaimed = if n == last_batch {
                head.head
            } else {
                part.last().map_or(head.head, |e| e.last.saturating_add(1))
            };
            self.write_reclaim(hash, &decrements, done).await?;
            chunks += part.len() as u64;
            let bytes: u64 = part.iter().map(|e| u64::from(e.chunk_len)).sum();
            gc.counters
                .reclaimed_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        if due.is_empty() {
            done.reclaimed = head.head;
            self.write_reclaim(hash, &Decrements::new(), done).await?;
        }
        gc.counters.reclaims.fetch_add(1, Ordering::Relaxed);
        gc.counters
            .reclaimed_chunks
            .fetch_add(chunks, Ordering::Relaxed);
        Ok(chunks)
    }

    /// One reclaim batch: `decrements` and the `'H'` row at `head`.
    async fn write_reclaim(
        &self,
        hash: [u8; 16],
        decrements: &Decrements,
        head: Head,
    ) -> Result<(), GcError> {
        let mut batch = WriteBatch::new();
        let zeroed = self.stage_decrements(&mut batch, decrements).await?;
        batch.put(gc::head_key(&hash), gc::encode_head(&head));
        self.write_decrements(batch, &zeroed).await
    }

    /// Delete every row of a purged stream but its billing row (the ledger
    /// owns it), at most `reclaim_batch` keys per batch: producers, seq
    /// lanes, the seal fence. Nothing writes them once the stream is purged
    /// (its load fails), so this races no committer group.
    async fn delete_stream_rows(&self, hash: [u8; 16]) -> Result<u64, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        if hash == [0xFF; 16] {
            // Its rows' prefix is the sentinel's: never scanned for deletes.
            return Ok(0);
        }
        let batch_max = self.gc_state().settings().reclaim_batch.max(1);
        let mut deleted = 0u64;
        loop {
            let keys = self.stream_row_keys(hash, batch_max).await.map_err(store)?;
            if keys.is_empty() {
                return Ok(deleted);
            }
            let mut batch = WriteBatch::new();
            for k in &keys {
                batch.delete(k);
            }
            self.write_index_batch(batch)
                .await
                .map_err(|e| GcError::Store(e.to_string()))?;
            deleted += keys.len() as u64;
        }
    }

    /// At most `max` keys of `hash`'s rows, its billing row excepted.
    async fn stream_row_keys(
        &self,
        hash: [u8; 16],
        max: usize,
    ) -> Result<Vec<Vec<u8>>, slatedb::Error> {
        let keep = crate::billing::billing_meta_key(&hash);
        let prefix = crate::keyspace::stream_row(&hash, 0);
        let mut keys = Vec::new();
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await?;
        while keys.len() < max {
            let Some(kv) = rows.next().await? else {
                break;
            };
            if kv.key.as_ref() != keep.as_slice() {
                keys.push(kv.key.to_vec());
            }
        }
        Ok(keys)
    }

    /// Every index entry of `hash`: the directory's, then every live
    /// run-index object's run (the caller pins the objects it reads).
    pub(crate) async fn index_entries(&self, hash: [u8; 16]) -> Result<Vec<DirEntry>, GcError> {
        let mut out = self
            .layout
            .seal
            .directory
            .read()
            .map_err(|_| GcError::Index("the seal directory's lock is poisoned".into()))?
            .from(&hash, 0, usize::MAX);
        let runs = self
            .runs_of(&hash, 0, u64::MAX)
            .await
            .map_err(|e| GcError::Store(format!("{e:?}")))?;
        for (pointer, footer) in runs {
            let run = self
                .fetch_run(&pointer, &footer)
                .await
                .map_err(|e| GcError::Store(format!("{e:?}")))?;
            out.extend(run.iter().cloned());
        }
        Ok(out)
    }

    /// The chunks of `hash` no consolidation superseded (`spill::gc::
    /// live_entries`), in offset order, including dead ones not yet
    /// reclaimed.
    pub(crate) async fn live_chunks(&self, hash: [u8; 16]) -> Result<Vec<DirEntry>, GcError> {
        let _pin = self.gc_state().pin();
        let entries = self.index_entries(hash).await?;
        gc::live_entries(entries).map_err(GcError::Index)
    }

    /// The stream's physical size (SPEC G10): its unreclaimed chunks'
    /// lengths plus its unsealed page bytes (tests and the audit; no
    /// surface reports it yet).
    #[cfg(test)]
    pub(crate) async fn physical_bytes(&self, hash: [u8; 16]) -> Result<u64, GcError> {
        let reclaimed = self.head_row(&hash).await?.map_or(0, |h| h.reclaimed);
        let sealed: u64 = self
            .live_chunks(hash)
            .await?
            .iter()
            .filter(|e| e.first >= reclaimed)
            .map(|e| u64::from(e.chunk_len))
            .sum();
        let unsealed = if self.purged_stream(&hash).await.unwrap_or(false) {
            0
        } else {
            self.stream_handle(hash)
                .await
                .map_err(|e| GcError::Store(e.to_string()))?
                .state
                .lock()
                .map_err(|_| GcError::Index("a stream state is poisoned".into()))?
                .applied
                .unabsorbed_bytes
        };
        Ok(sealed + unsealed)
    }
}
