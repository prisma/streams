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
    pub(super) fn fence(&mut self, local: &mut StreamOverlay, hash: [u8; 16], req: SealFenceReq) {
        let mut fences = self.engine.seal_fences.lock().unwrap();
        let current = fences.entry(hash).or_insert(0);
        *current = (*current).max(req.generation);
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
    pub(super) fn close(&mut self, local: &mut StreamOverlay, hash: [u8; 16], req: CloseReq) {
        #[cfg(test)]
        self.client_append_hashes.insert(hash);
        let fence = self
            .engine
            .seal_fences
            .lock()
            .unwrap()
            .get(&hash)
            .copied()
            .unwrap_or(0);
        if !local.fields.closed && !seal_authorized(req.generation, true, fence) {
            let _ = req.resp.send(Err(AppendErr::SealSuperseded));
            return;
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
