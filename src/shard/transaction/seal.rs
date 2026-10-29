//! A seal's commit, staged by the committer (layout 6, REVIEW-6A F1).
//!
//! The sealer built the segment object from a snapshot of each stream's
//! durable unsealed pages `[from, upto)` and PUT it; the commit makes the
//! object the readers' source for those offsets. The committer stages it
//! under its own serialization, against each stream's **live** tail:
//! - every stream must still be exactly where the snapshot found it
//!   (`absorbed == from`, `upto <= next`, the sealed bytes within its
//!   unsealed ledger), or the whole commit is refused and changes nothing;
//! - each stream's boundary moves to `upto` and its sealed bytes leave the
//!   ledger;
//! - the group's finalization puts the stream's pending row with the
//!   **remainder**: every page in `[upto, next)` at that moment, the pages
//!   applied after the snapshot and this very group's included, so the put
//!   shadows only operands whose pages the object holds;
//! - the seal's directory row and refcount row ride the same batch.
//!
//! A refusal is answered at once; an accepted seal's reply is dropped with
//! the group's durable effects (after durable dispatch, or with the group's
//! refusal), and the sealer reads the outcome from the streams' boundaries.
use super::*;
use crate::shard::commit_plan::{apply_seal, seal_fits};
use crate::shard::seal::gc::purge::PurgeStep;
use crate::shard::seal::{SealAdvance, SealBatch, SealCommit};

/// What a seal left of a stream's pending row: the stream's applied pages
/// at or above the boundary it moved to when the seal was staged (the
/// live remainder, REVIEW-6A F1: a stream's applied pages change only when
/// a written group publishes, so they are the same when the group writes)
/// and, for the F1 negative control, the remainder a seal task would have
/// pre-built.
pub(super) struct SealedAt {
    pub applied: Vec<crate::spill::pending::PendingPage>,
    #[cfg(test)]
    pub prebuilt: Option<Vec<crate::spill::pending::PendingPage>>,
}

impl CommitTransaction<'_> {
    pub(super) async fn stage_seal(&mut self, commit: SealCommit) {
        let seal = match commit {
            SealCommit::Seal(seal) => seal,
            SealCommit::Purge(step) => return self.stage_purge(step).await,
        };
        let SealBatch {
            dir_key,
            dir_value,
            ref_key,
            ref_value,
            streams,
            resp,
        } = seal;
        let mut overlays = Vec::with_capacity(streams.len());
        let mut remainders = Vec::with_capacity(streams.len());
        let mut refusal = None;
        for adv in &streams {
            let local = match self.seal_overlay(adv.hash).await {
                Ok(local) => local,
                Err(error) => {
                    refusal.get_or_insert(error);
                    continue;
                }
            };
            refusal = refusal.or_else(|| moved_since_snapshot(&local, adv));
            match applied_from(&local.handle, adv.upto) {
                Ok(pages) => remainders.push(pages),
                Err(why) => {
                    refusal.get_or_insert(why);
                }
            }
            overlays.push(local);
        }
        if let Some(why) = refusal {
            for local in overlays {
                self.streams.insert(local.handle.hash, local);
            }
            if resp.send(why).is_err() {
                tracing::debug!("the sealer stopped waiting for its refusal");
            }
            return;
        }
        for ((mut local, adv), applied) in overlays.into_iter().zip(streams).zip(remainders) {
            apply_seal(&mut local.fields, adv.from, adv.upto, adv.bytes);
            local.frames.retired_bytes += adv.bytes;
            #[cfg(test)]
            let prebuilt = self
                .engine
                .layout
                .seal
                .prebuilt_remainder()
                .then_some(adv.snapshot_remainder);
            local.frames.sealed = Some(SealedAt {
                applied,
                #[cfg(test)]
                prebuilt,
            });
            self.streams.insert(adv.hash, local);
        }
        self.batch.put(dir_key, dir_value);
        self.batch.put(ref_key, ref_value);
        self.extra_writes = true;
        self.effects.seals.push(resp);
    }

    /// The group's overlay of `hash`, taken out of the group's map, or new
    /// (and then the seal is the group's only op on it so far).
    async fn seal_overlay(&mut self, hash: [u8; 16]) -> Result<StreamOverlay, String> {
        if let Some(local) = self.streams.remove(&hash) {
            return Ok(local);
        }
        let handle = self
            .engine
            .stream_handle(hash)
            .await
            .map_err(|e| e.to_string())?;
        let mut local = StreamOverlay::new(handle, self.billing_rows.remove(&hash).flatten());
        local.frames.seal_only = true;
        Ok(local)
    }
}

/// The stream's applied pages at or above `upto`; a poisoned stream state
/// refuses the seal whole rather than guess at the remainder.
fn applied_from(
    handle: &StreamHandle,
    upto: u64,
) -> Result<Vec<crate::spill::pending::PendingPage>, String> {
    let st = handle
        .state
        .lock()
        .map_err(|_| "a sealed stream's state is poisoned".to_owned())?;
    Ok(st.pending.from(upto).cloned().collect())
}

/// Why a stream's live tail no longer matches its snapshot, if it does not.
fn moved_since_snapshot(local: &StreamOverlay, adv: &SealAdvance) -> Option<String> {
    let f = &local.fields;
    let moved = local.handle.native_queue.load(Ordering::Relaxed)
        || local.frames.sealed.is_some()
        || !seal_fits(f, adv.from, adv.upto, adv.bytes);
    moved.then(|| {
        format!(
            "stream {} moved since its snapshot: absorbed {} next {} unsealed {} B, seal [{}, {}) {} B",
            crate::crypto::hex(&adv.hash[..4]),
            f.absorbed,
            f.next,
            f.unabsorbed_bytes,
            adv.from,
            adv.upto,
            adv.bytes
        )
    })
}

impl CommitTransaction<'_> {
    /// A dead stream's purge (`seal::gc::purge`, step 1): its tail and
    /// pending rows deleted and its `'H'` row Purged at its live next
    /// offset, in this group; its handle leaves the engine, and a later
    /// load of it fails. Refused, changing nothing, while the stream has
    /// another op in this group (it would put the tail back), while its
    /// usage awaits the ledger (the counters live on the tail). A native
    /// queue seals nothing: it is marked purged and its queue-segment rows
    /// stay.
    pub(super) async fn stage_purge(&mut self, step: PurgeStep) {
        let PurgeStep { hash, prior, resp } = step;
        match self.purge_refusal(hash).await {
            Ok((next, queue)) => {
                let purged = crate::spill::gc::Head {
                    state: crate::spill::gc::HeadState::Purged,
                    head: next.max(prior.head),
                    ..prior
                };
                if !queue {
                    self.batch.delete(crate::shard::tail_key(&hash));
                    self.batch.delete(crate::spill::pending::pending_key(&hash));
                }
                self.batch.put(
                    crate::spill::gc::head_key(&hash),
                    crate::spill::gc::encode_head(&purged),
                );
                self.extra_writes = true;
                self.engine.gc_state().note_purged(hash);
                self.engine.forget_stream(&hash);
                self.effects.seals.push(resp);
            }
            Err(why) => {
                if resp.send(why).is_err() {
                    tracing::debug!("the collector stopped waiting for its purge's refusal");
                }
            }
        }
    }

    /// The stream's live next offset and whether it is a native queue, or
    /// why it cannot be purged now.
    async fn purge_refusal(&mut self, hash: [u8; 16]) -> Result<(u64, bool), String> {
        if self.streams.contains_key(&hash) {
            return Err("the stream has another op in this group".into());
        }
        if self.engine.layout.usage_pending(&hash) {
            return Err("the stream's usage awaits the ledger".into());
        }
        let handle = self
            .engine
            .stream_handle(hash)
            .await
            .map_err(|e| e.to_string())?;
        if handle.native_queue.load(Ordering::Relaxed) {
            return Ok((0, true));
        }
        let st = handle
            .state
            .lock()
            .map_err(|_| "a purged stream's state is poisoned".to_owned())?;
        Ok((st.applied.next.max(st.durable.next), false))
    }
}
