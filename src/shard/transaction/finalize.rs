use super::*;
use crate::billing::stored_meta::{UsageCounters, encode_static};
use crate::shard::layout::StaticBilling;
use crate::spill::pending::{concat_pages, pending_key};
impl CommitTransaction<'_> {
    pub(super) async fn finish(mut self) {
        self.sync_usage();
        self.stage_stream_rows();
        self.stage_pending_rows();
        if let Some(message) = &self.accounting_diverged {
            tracing::error!("maintenance accounting diverged: {message}");
            self.reject("maintenance accounting diverged");
            return;
        }
        // A no-write group observes prior applied truth. Attach all its
        // replies (including fences/refusals) to the newest existing barrier.
        if !self.has_writes() {
            self.keep_hot_rows();
            #[cfg(test)]
            self.engine
                .completion_checkpoint(retirement_tests::CompletionPhase::NoWrite)
                .await;
            self.join_prior_barrier().await;
            return;
        }
        #[cfg(test)]
        if self.failpoint_tripped() || self.seal_failpoint_tripped() {
            self.reject("failpoint: group write failed");
            return;
        }
        let maintenance = match self.stage_maintenance() {
            Ok(value) => value,
            Err(error) => {
                tracing::error!("maintenance accounting diverged: {error}");
                self.reject("maintenance accounting diverged");
                return;
            }
        };
        self.keep_hot_rows();
        self.write(maintenance).await;
    }
    /// Every stream whose billing meta this group read takes its usage
    /// counters into the tail it stages, and one whose billing it changed
    /// its static meta too, and marks it in the usage dirty set (layout 6,
    /// f3): the applied tail carries both for the next group, the durable
    /// tail for the usage drain.
    fn sync_usage(&mut self) {
        for (hash, local) in &mut self.streams {
            if let Some(usage) = local.billing.meta.as_ref().and_then(UsageCounters::of) {
                local.fields.usage = Some(usage);
            }
            // Marked before the write: a write that fails retires the
            // engine and its set with it, and a mark is never lost.
            if local.billing.dirty
                && let Some(bm) = &local.billing.meta
            {
                local.fields.billing = StaticBilling::Read(Some(bm.clone()));
                self.engine.layout.mark_usage(*hash, bm.usage_version);
            }
        }
    }
    /// Every stream that took pages in this group takes them as one merge
    /// operand on its pending row, and is held for the sealer (layout 6
    /// b1). A stream this group seals instead takes a put of its remainder
    /// (`transaction::seal`, REVIEW-6A F1). Held before the write, like a
    /// usage mark: a write that fails retires the engine and its set with
    /// it, and a held stream with no unsealed page is released by the
    /// sealer.
    fn stage_pending_rows(&mut self) {
        for (hash, local) in &self.streams {
            if let Some(sealed) = &local.frames.sealed {
                let bytes = stage_remainder(&mut self.batch, hash, local, sealed);
                self.engine.note_remainder(&local.handle, bytes);
                continue;
            }
            if local.frames.pending.is_empty() {
                continue;
            }
            let pages = local.frames.pending.iter().map(|(_, _, page)| page);
            self.batch.merge(pending_key(hash), concat_pages(pages));
            self.engine.layout.hold_unsealed(&local.handle);
            self.engine.layout.seal.note_unsealed();
        }
    }
    fn has_writes(&self) -> bool {
        self.changed
            || self.stats.records > 0
            || !self.effects.touches.is_empty()
            || self.extra_writes
    }
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::join_prior_barrier; a poisoned in-flight handoff may hold a half-attached publication; recovering it could publish effects a prior barrier never covered"
    )]
    async fn join_prior_barrier(mut self) {
        // Complete any prior durable publications before treating an OPEN
        // empty queue as completed truth. Retirement never needs this gate.
        let _dispatch = self.engine.dispatch_gate.lock().await;
        let attachment = self
            .engine
            .in_flight
            .lock()
            .unwrap()
            .attach(&mut self.effects);
        match attachment {
            Attachment::Pending => {}
            Attachment::Durable => self.effects.reply(),
            Attachment::Retired => self.effects.reject(AppendErr::Moved),
        }
    }
    fn stage_stream_rows(&mut self) {
        for (hash, local) in &mut self.streams {
            if !local.frames.ring.is_empty() {
                self.effects
                    .ring_pub
                    .push((local.handle.clone(), std::mem::take(&mut local.frames.ring)));
            }
            let f = &local.fields;
            let b = &local.base;
            self.maintenance_added = self
                .maintenance_added
                .saturating_add(local.frames.added_bytes);
            self.maintenance_retired = self
                .maintenance_retired
                .saturating_add(local.frames.retired_bytes);
            let net = f.unabsorbed_bytes as i128 - b.unabsorbed_bytes as i128;
            let actual = local.frames.added_bytes as i128 - local.frames.retired_bytes as i128;
            if net != actual && self.accounting_diverged.is_none() {
                self.accounting_diverged = Some(format!(
                    "maintenance actuals do not reconcile with the tail: stream={} \
             net={net} appended={} retired={}",
                    crate::crypto::hex(&hash[..4]),
                    local.frames.added_bytes,
                    local.frames.retired_bytes,
                ));
            }
            if f.stored_differs(b) {
                self.batch.put(local.handle.tail_row_key(), encode_tail(f));
                self.changed = true;
            }
            self.changed |= stage_billing_rows(&mut self.batch, hash, local, b);
            self.effects
                .tails
                .push((local.handle.clone(), f.clone(), !local.frames.seal_only));
            if local.frames.payload_bytes > 0 {
                self.stats.appended_bytes = self
                    .stats
                    .appended_bytes
                    .saturating_add(local.frames.payload_bytes);
            }
        }
    }
    fn stage_maintenance(&mut self) -> Result<Option<ShardMaintenance>, String> {
        if self.maintenance_added == 0 && self.maintenance_retired == 0 {
            return Ok(None);
        }
        let maintenance = self
            .engine
            .maintenance_snapshot()
            .apply_delta(self.maintenance_added, self.maintenance_retired, now_ms())
            .map_err(|error| error.to_string())?;
        Ok(Some(maintenance))
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "CommitTransaction::write; the elapsed microseconds are clamped to u32::MAX before the cast; a checked conversion would only restate the clamp"
    )]
    async fn write(mut self, maintenance: Option<ShardMaintenance>) {
        let encode_us = self.started.elapsed().as_micros().min(u32::MAX as u128) as u32;
        let started = std::time::Instant::now();
        self.engine
            .commit_write_started_ms
            .store(now_ms(), Ordering::SeqCst);
        // This is the only write. Taking the batch consumes the accumulated
        // transaction; the empty replacement can never be staged or written.
        let batch = std::mem::replace(&mut self.batch, WriteBatch::new());
        let result = self
            .engine
            .db
            .write_with_options(batch, &WriteOptions::default())
            .await;
        self.engine
            .commit_write_started_ms
            .store(0, Ordering::SeqCst);
        let write_us = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
        match result {
            Ok(handle) => {
                #[cfg(test)]
                self.engine
                    .completion_checkpoint(retirement_tests::CompletionPhase::Written)
                    .await;
                self.publish(handle.seqnum(), maintenance, encode_us, write_us);
            }
            Err(error) => self.write_failed(&error),
        }
    }
    /// TLA-005-F5: SlateDB can answer Err after applying the batch and making
    /// it readable, before the Db reports closed. No later group may stage
    /// from that batch, so the engine retires before the failure is answered.
    fn write_failed(self, error: &slatedb::Error) {
        tracing::error!(shard = %self.engine.prefix, "commit write failed: {error}");
        self.engine.begin_close();
        self.reject(&error.to_string());
    }
    /// Tests: whether this group stages a seal and the seal failpoint is
    /// armed (one-shot).
    #[cfg(test)]
    fn seal_failpoint_tripped(&self) -> bool {
        let tripped = !self.effects.seals.is_empty()
            && self
                .engine
                .layout
                .seal
                .hooks
                .fail_next_group
                .swap(false, Ordering::SeqCst);
        if tripped {
            self.engine
                .fail_group_tripped
                .fetch_add(1, Ordering::SeqCst);
        }
        tripped
    }
    #[cfg(test)]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::failpoint_tripped; the tripped check nests the hit removal inside the armed set's branch; flattening it would separate the removal from the hits it consumes"
    )]
    fn failpoint_tripped(&self) -> bool {
        let tripped = {
            let mut armed = self.engine.fail_group_for.lock().unwrap();
            match armed.as_mut() {
                Some(set) => {
                    let hits: Vec<_> = set
                        .iter()
                        .filter(|hash| self.client_append_hashes.contains(*hash))
                        .copied()
                        .collect();
                    for hash in &hits {
                        set.remove(hash);
                    }
                    !hits.is_empty()
                }
                None => false,
            }
        };
        if tripped {
            self.engine
                .fail_group_tripped
                .fetch_add(1, Ordering::SeqCst);
        }
        tripped
    }
}

/// A billed stream's rows beside its tail (layout 6, f3): its static row,
/// only when its identity, month or retention flag changed from the static
/// meta the group started from (`base`'s), and the month finals the group
/// closed. Whether anything was staged.
fn stage_billing_rows(
    batch: &mut WriteBatch,
    hash: &[u8; 16],
    local: &StreamOverlay,
    base: &TailFields,
) -> bool {
    let billing = &local.billing;
    if !billing.dirty {
        return false;
    }
    let mut staged = false;
    if let Some(bm) = &billing.meta {
        let row = encode_static(bm);
        let current = base.billing.clone().into_meta().as_ref().map(encode_static);
        if current.as_deref() != Some(&row[..]) {
            #[cfg(test)]
            crate::shard::test_support::count_static_row_staged(local.handle.owner.as_ptr());
            batch.put(crate::billing::billing_meta_key(hash), row);
            staged = true;
        }
    }
    for snap in &billing.month_finals {
        if let Some((y, m)) = crate::billing::parse_month(&snap.month) {
            batch.put(
                crate::billing::usage_month_final_key(hash, y, m),
                serde_json::to_vec(snap).unwrap_or_default(),
            );
            staged = true;
        }
    }
    staged
}

/// Put a sealed stream's remainder: its applied pages at or above the
/// new boundary (as the seal's staging read them) and this group's.
/// Returns its bytes.
fn stage_remainder(
    batch: &mut WriteBatch,
    hash: &[u8; 16],
    local: &StreamOverlay,
    sealed: &super::seal::SealedAt,
) -> u64 {
    let applied = &sealed.applied;
    #[cfg(test)]
    let applied = sealed.prebuilt.as_ref().unwrap_or(applied);
    let pages: Vec<_> = applied
        .iter()
        .chain(&local.frames.pending)
        .map(|(_, _, page)| page)
        .collect();
    if pages.is_empty() {
        batch.delete(pending_key(hash));
        return 0;
    }
    let remainder = concat_pages(pages);
    let bytes = remainder.len() as u64;
    batch.put(pending_key(hash), remainder);
    bytes
}
