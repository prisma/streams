use super::*;
impl CommitTransaction<'_> {
    pub(super) async fn finish(mut self) {
        self.stage_stream_rows();
        if let Some(message) = &self.accounting_diverged {
            tracing::error!("maintenance accounting diverged: {message}");
            self.reject("maintenance accounting diverged");
            return;
        }
        // A no-write group observes prior applied truth. Attach all its
        // replies (including fences/refusals) to the newest existing barrier.
        if !self.has_writes() {
            #[cfg(test)]
            self.engine
                .completion_checkpoint(retirement_tests::CompletionPhase::NoWrite)
                .await;
            self.join_prior_barrier().await;
            return;
        }
        #[cfg(test)]
        if self.failpoint_tripped() {
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
        self.write(maintenance).await;
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
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::stage_stream_rows; the row staging nests the dirty mark and the billing rows inside each changed stream's write; flattening it would separate the marks from the tail they qualify"
    )]
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
            if f.next != b.next
                || f.absorbed != b.absorbed
                || f.trimmed != b.trimmed
                || f.trim_safe_to != b.trim_safe_to
                || f.unabsorbed_bytes != b.unabsorbed_bytes
                || f.seq != b.seq
                || f.closed != b.closed
            {
                self.batch.put(tail_key(hash), encode_tail(f));
                let was_marked = b.absorbed < b.next || b.trimmed < b.trim_safe_to;
                let is_marked = f.absorbed < f.next || f.trimmed < f.trim_safe_to;
                if is_marked {
                    self.batch.put(
                        dirty_key(hash),
                        dirty_value(&StreamMaintenance {
                            absorbed: f.absorbed,
                            next: f.next,
                            unabsorbed_bytes: f.unabsorbed_bytes,
                            ..Default::default()
                        }),
                    );
                } else if was_marked {
                    self.batch.delete(dirty_key(hash));
                }
                self.changed = true;
            }
            if local.billing.dirty {
                if let Some(bm) = &local.billing.meta {
                    self.batch.put(
                        crate::billing::billing_meta_key(hash),
                        serde_json::to_vec(bm).unwrap_or_default(),
                    );
                    self.batch.put(
                        crate::billing::usage_dirty_key(hash),
                        &bm.usage_version.to_le_bytes()[..],
                    );
                    self.changed = true;
                }
                for snap in &local.billing.month_finals {
                    if let Some((y, m)) = crate::billing::parse_month(&snap.month) {
                        self.batch.put(
                            crate::billing::usage_month_final_key(hash, y, m),
                            serde_json::to_vec(snap).unwrap_or_default(),
                        );
                    }
                }
            }
            self.effects.tails.push((local.handle.clone(), f.clone()));
            if local.frames.payload_bytes > 0 {
                self.stats.appended_bytes = self
                    .stats
                    .appended_bytes
                    .saturating_add(local.frames.payload_bytes);
            }
            if local.frames.added_bytes > 0 {
                self.effects.signals.push(AbsorbSignal {
                    hash: *hash,
                    appended_bytes: local.frames.added_bytes,
                });
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
        self.batch
            .put(shard_maint_key(), encode_shard_maint(&maintenance));
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
            Err(error) => self.reject(&error.to_string()),
        }
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
        let tripped = tripped
            || (self.group_has_absorbed
                && self
                    .engine
                    .fail_next_absorbed_group
                    .swap(false, Ordering::SeqCst));
        if tripped {
            self.engine
                .fail_group_tripped
                .fetch_add(1, Ordering::SeqCst);
        }
        tripped
    }
}
