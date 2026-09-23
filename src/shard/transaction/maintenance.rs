use super::*;
impl CommitTransaction<'_> {
    pub(super) fn usage_ack(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        scope: UsageAckScope,
        month_final_keys: Vec<Vec<u8>>,
    ) {
        let cur = local.billing.meta.as_ref().map(|bm| bm.usage_version);
        if decide_billing_ack(cur, scope) == BillingAckDecision::ClearDirty {
            self.batch.delete(crate::billing::usage_dirty_key(&hash));
            self.extra_writes = true;
        }
        for k in month_final_keys {
            self.batch.delete(k);
            self.extra_writes = true;
        }
    }
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::billing_close; the billing meta was inserted just above when absent; a fallible read would add a branch no close reaches"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "CommitTransaction::billing_close; the close nests the storage-clock advance inside the loaded-meta branch; flattening it would separate the advance from the meta it closes"
    )]
    pub(super) fn billing_close(
        &mut self,
        local: &mut StreamOverlay,
        _hash: [u8; 16],
        close_ms: i64,
    ) {
        if local.billing.meta.is_none() {
            let loaded = crate::billing::SegmentBillingMetaV1::default();
            local.billing.meta = Some(loaded);
        }
        {
            let bm = local.billing.meta.as_mut().unwrap();
            if bm.stream_id.is_empty() {
                local.billing.meta = None;
            } else {
                let at = if close_ms > 0 {
                    close_ms
                } else {
                    crate::billing::billing_now_ms()
                };
                let finals = &mut local.billing.month_finals;
                bm.advance_storage_clock(at, |closed| {
                    finals.push(closed.to_snapshot(true));
                });
                bm.owned_frame_bytes_current = 0;
                bm.usage_version += 1;
                local.billing.dirty = true;
            }
        }
    }
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::billing_retained; the billing meta was inserted just above when absent; a fallible read would add a branch no retention pass reaches"
    )]
    pub(super) fn billing_retained(
        &mut self,
        local: &mut StreamOverlay,
        _hash: [u8; 16],
        retained: bool,
    ) {
        if local.billing.meta.is_none() {
            let loaded = crate::billing::SegmentBillingMetaV1::default();
            local.billing.meta = Some(loaded);
        }
        {
            let bm = local.billing.meta.as_mut().unwrap();
            if bm.stream_id.is_empty() {
                local.billing.meta = None;
            } else if bm.retained_by_forks != retained {
                bm.retained_by_forks = retained;
                bm.usage_version += 1;
                local.billing.dirty = true;
            }
        }
    }
    /// The segment's seal fence: the engine cache, or on first use the
    /// durable row that a fence in this or any earlier engine wrote.
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::seal_fence; a poisoned seal-fence map may hold a half-raised generation; recovering it could admit a close the fence already superseded"
    )]
    pub(super) async fn seal_fence(&self, hash: [u8; 16]) -> Result<u64, AppendErr> {
        if let Some(fence) = self.engine.seal_fences.lock().unwrap().get(&hash).copied() {
            return Ok(fence);
        }
        let unverified =
            |error: String| AppendErr::Internal(format!("seal_fence_unverified: {error}"));
        let durable = match self.engine.db.get(seal_fence_key(&hash)).await {
            Ok(Some(raw)) => {
                crate::queue::decode_counter(&raw).map_err(|e| unverified(e.into()))?
            }
            // Never fenced: nothing to cache, so the map keeps holding only
            // fenced segments (seal_fence_stats counts them).
            Ok(None) => return Ok(0),
            Err(error) => return Err(unverified(error.to_string())),
        };
        let mut fences = self.engine.seal_fences.lock().unwrap();
        let fence = fences.entry(hash).or_insert(durable);
        *fence = (*fence).max(durable);
        Ok(*fence)
    }
    /// A closing or claim-authorized append carries a generation at or above
    /// the segment's seal fence; an untagged ordinary append is not a seal
    /// decision.
    pub(super) async fn seal_authorizes(
        &self,
        hash: [u8; 16],
        req: &AppendReq,
    ) -> Result<(), AppendErr> {
        let closing = req.finish == AppendFinish::Close;
        if req.seal_gen.is_none() && !closing {
            return Ok(());
        }
        let fence = self.seal_fence(hash).await?;
        if seal_authorized(req.seal_gen, closing, fence) {
            Ok(())
        } else {
            Err(AppendErr::SealSuperseded)
        }
    }
    #[expect(
        clippy::unwrap_used,
        reason = "CommitTransaction::fence; a poisoned seal-fence map may hold a half-raised generation; recovering it could admit a close the fence already superseded, and the durable row it writes is only ever raised from that cache"
    )]
    #[expect(
        clippy::let_underscore_must_use,
        reason = "CommitTransaction::fence; a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits"
    )]
    pub(super) async fn fence(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        req: SealFenceReq,
    ) {
        let current = match self.seal_fence(hash).await {
            Ok(fence) => fence,
            Err(error) => {
                let _ = req.resp.send(Err(error));
                return;
            }
        };
        // Every fence writes its row in this group, so the reply that lets a
        // takeover install waits for the fence's own durability. The cache may
        // already hold a generation whose group failed; rewriting the maximum
        // makes the row catch up instead of trusting that cache.
        let fence = current.max(req.generation);
        self.engine.seal_fences.lock().unwrap().insert(hash, fence);
        self.batch.put(seal_fence_key(&hash), fence.to_le_bytes());
        self.extra_writes = true;
        self.effects.acks.push((
            req.resp,
            Ok(AppendAck {
                last_offset: local.fields.next.wrapping_sub(1),
                next_offset: local.fields.next,
                closed: local.fields.closed,
                producer: None,
                duplicate: false,
            }),
        ));
    }
    #[expect(
        clippy::let_underscore_must_use,
        reason = "CommitTransaction::close; a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits"
    )]
    pub(super) async fn close(&mut self, local: &mut StreamOverlay, hash: [u8; 16], req: CloseReq) {
        #[cfg(test)]
        self.client_append_hashes.insert(hash);
        // An already-closed segment answers its idempotent re-close without
        // consulting the fence (a resumed run_seal re-closes every segment).
        if !local.fields.closed {
            let fence = match self.seal_fence(hash).await {
                Ok(fence) => fence,
                Err(error) => {
                    let _ = req.resp.send(Err(error));
                    return;
                }
            };
            if !seal_authorized(req.generation, true, fence) {
                let _ = req.resp.send(Err(AppendErr::SealSuperseded));
                return;
            }
        }
        local.fields.closed = true;
        self.effects.acks.push((
            req.resp,
            Ok(AppendAck {
                last_offset: local.fields.next.wrapping_sub(1),
                next_offset: local.fields.next,
                closed: true,
                producer: None,
                duplicate: false,
            }),
        ));
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "CommitTransaction::absorbed; the absorbed boundary carries the stream, its new boundary, the retired bytes and the layout flag as the absorber reported them; a report struct would exist only for this signature"
    )]
    #[cfg_attr(
        test,
        expect(
            clippy::disallowed_methods,
            reason = "CommitTransaction::absorbed; the drain trace is switched on by the DST harness through the process environment; carrying a debugging switch in the engine's configuration would put it on the production path"
        )
    )]
    pub(super) fn absorbed(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        upto: u64,
        bytes: u64,
        v2: bool,
    ) {
        // The first advancing boundary seals the history layout. Duplicates
        // never raise the one-pass-lag trim target or retire bytes again.
        let prev_absorbed = local.fields.absorbed;
        #[cfg(test)]
        if std::env::var("DST_DRAIN_TRACE").is_ok() {
            eprintln!(
                "ADVANCE {} prev={prev_absorbed} upto={upto} v2={v2} next={} trimmed={} flag={}",
                crate::crypto::hex(&hash[..4]),
                local.fields.next,
                local.fields.trimmed,
                local.fields.history_v2
            );
        }
        let lane_ok = if v2 {
            prev_absorbed == 0 || local.fields.history_v2
        } else {
            !local.fields.history_v2
        };
        if !lane_ok && upto > prev_absorbed {
            self.engine
                .absorb_lane_dropped
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                shard = %self.engine.prefix,
                v2,
                upto,
                prev = prev_absorbed,
                "dropped cross-layout absorb advance (layout sealed)"
            );
        }
        if lane_ok && upto > prev_absorbed {
            let Some(remaining) = local.fields.unabsorbed_bytes.checked_sub(bytes) else {
                self.accounting_diverged = Some(format!(
                    "absorbed-boundary retirement exceeds the stream ledger: \
             stream={} upto={upto} retire_bytes={bytes} ledger={}",
                    crate::crypto::hex(&hash[..4]),
                    local.fields.unabsorbed_bytes,
                ));
                return;
            };
            #[cfg(test)]
            {
                self.group_has_absorbed = true;
            }
            local.fields.absorbed = upto.min(local.fields.next);
            local.fields.unabsorbed_bytes = remaining;
            local.frames.retired_bytes += bytes;
            if v2 {
                local.fields.history_v2 = true;
            }
            local.fields.trim_safe_to = local.fields.trim_safe_to.max(prev_absorbed);
            let allowed = self.trim_budget.min(self.cfg.max_trim_per_op);
            let trim_to = local
                .fields
                .trim_safe_to
                .min(local.fields.trimmed + allowed);
            for off in local.fields.trimmed..trim_to {
                self.batch.delete(record_key(&hash, off));
            }
            self.trim_budget -= trim_to.saturating_sub(local.fields.trimmed);
            local.fields.trimmed = local.fields.trimmed.max(trim_to);
        }
    }
    pub(super) fn trim(&mut self, local: &mut StreamOverlay, hash: [u8; 16]) {
        let target = local.fields.trim_safe_to.min(local.fields.absorbed);
        let allowed = self.trim_budget.min(self.cfg.max_trim_per_op);
        let trim_to = target.min(local.fields.trimmed + allowed);
        for off in local.fields.trimmed..trim_to {
            self.batch.delete(record_key(&hash, off));
        }
        self.trim_budget -= trim_to.saturating_sub(local.fields.trimmed);
        local.fields.trimmed = local.fields.trimmed.max(trim_to);
    }
}
