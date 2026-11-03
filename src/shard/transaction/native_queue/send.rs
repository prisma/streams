//! Send (docs/QUEUES-DESIGN.md §5.1-§5.2, §6.6, §8.4; S3b-DESIGN §5.1):
//! each message becomes one sealed page in the group's batch, staged so the
//! group's later claims read it. An immediate message writes nothing else:
//! the lane's frontier finds it by its page. A scheduled one (a future
//! `deliverAt`) also writes its scheduled entry (with its page inline when
//! small and the queue's inline budget allows), its by-offset row and its
//! range row's bits. The queue's tail stays "absorbed in place"
//! (`absorbed = next`, §5.6), so no absorption consumer ever sees a queue.
//! The queued gauges count each message's stored page.
use super::stage::NativeStage;
use super::*;
use crate::crypto_page::SealedPage;
use crate::native_queue::rows::{decode_dedup, encode_dedup, encode_scheduled};
use crate::native_queue::{SendMessage, SendResult};

type SendArgs = (Vec<SendMessage>, [u8; 32], u32, i64);

impl CommitTransaction<'_> {
    pub(super) async fn nq_send(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: SendArgs,
    ) -> NqResult {
        let (messages, subkey, key_version, t) = args;
        let cfg = st.state.cfg.clone();
        // Duplicates first: they write nothing and take no budget.
        let mut results: Vec<Option<SendResult>> = Vec::with_capacity(messages.len());
        let mut fresh_tags = 0u64;
        for m in &messages {
            let dup = match &m.dedup_tag {
                Some(tag) => self.nq_dedup_hit(hash, st, tag, t).await?,
                None => None,
            };
            if dup.is_none() && m.dedup_tag.is_some() {
                fresh_tags += 1;
            }
            results.push(dup);
        }
        let new: Vec<&SendMessage> = messages
            .iter()
            .zip(&results)
            .filter(|(_, r)| r.is_none())
            .map(|(m, _)| m)
            .collect();
        for m in &new {
            if m.deliver_at.is_some_and(|at| at > t + cfg.retention_ms) {
                return Err(NqRefusal::DeliverAtBeyondRetention);
            }
        }
        let scheduled = new
            .iter()
            .filter(|m| m.deliver_at.is_some_and(|at| at > t))
            .count() as u64;
        if scheduled > 0 && st.state.scheduled + scheduled > cfg.max_scheduled_messages {
            return Err(NqRefusal::ScheduledFull);
        }
        // Seal before the budget checks: the gauges count stored pages.
        let pages = self.nq_seal(local, hash, &new, (&subkey, key_version, t))?;
        let bytes: u64 = pages.iter().map(|p| p.bytes.len() as u64).sum();
        let s = &st.state;
        if s.queued_messages + new.len() as u64 > cfg.max_queued_messages.unwrap_or(u64::MAX)
            || s.queued_bytes + bytes > cfg.max_queued_bytes.unwrap_or(u64::MAX)
        {
            return Err(NqRefusal::QueueFull);
        }
        if s.dedup_entries + fresh_tags > cfg.max_dedup_entries {
            return Err(NqRefusal::DedupFull);
        }
        let mut sent = Vec::with_capacity(new.len());
        for (m, page) in new.into_iter().zip(pages) {
            sent.push(self.nq_send_one(local, hash, st, (m, page, t)).await?);
        }
        let mut sent = sent.into_iter();
        let out = results
            .into_iter()
            .filter_map(|r| r.or_else(|| sent.next()))
            .collect();
        Self::nq_tick(st, t);
        Ok(NqOut::Sent(out))
    }

    /// The original offset when `tag` was sent within its window.
    async fn nq_dedup_hit(
        &self,
        hash: [u8; 16],
        st: &mut NativeStage,
        tag: &[u8; 16],
        t: i64,
    ) -> Result<Option<SendResult>, NqRefusal> {
        let raw = self
            .nq_get(st, &keys::dedup(&hash, tag))
            .await
            .map_err(NqRefusal::Internal)?;
        let Some(raw) = raw else { return Ok(None) };
        let (offset, expires_at) = decode_dedup(&raw).map_err(NqRefusal::Internal)?;
        if expires_at <= t {
            return Ok(None);
        }
        st.state.counters.duplicates += 1;
        st.state_dirty = true;
        Ok(Some(SendResult {
            offset,
            enqueued_at: 0,
            available_at: 0,
            duplicate: true,
        }))
    }

    /// One page per new message, at the offsets the batch will take.
    fn nq_seal(
        &self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        new: &[&SendMessage],
        key: (&[u8; 32], u32, i64),
    ) -> Result<Vec<SealedPage>, NqRefusal> {
        let (subkey, key_version, t) = key;
        let lane = crate::crypto_page::PageLane {
            key_version,
            routing_key: "",
        };
        let mut out = Vec::with_capacity(new.len());
        for (offset, m) in (local.fields.next..).zip(new) {
            let record = [crate::crypto_page::SealRecord {
                ts_ms: t,
                payload: &m.envelope,
            }];
            let mut pages = local
                .frames
                .cipher(subkey, &hash)
                .seal_request(&lane, offset, &record)
                .map_err(|e| NqRefusal::Internal(format!("page seal refused: {e:?}")))?;
            let (Some(page), true) = (pages.pop(), pages.is_empty()) else {
                return Err(NqRefusal::Internal("a message sealed to several pages".into()));
            };
            out.push(page);
        }
        Ok(out)
    }

    async fn nq_send_one(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: (&SendMessage, SealedPage, i64),
    ) -> Result<SendResult, NqRefusal> {
        let (m, page, t) = args;
        let offset = local.fields.next;
        let cfg = st.state.cfg.clone();
        let available_at = m.deliver_at.unwrap_or(t).max(t);
        let bytes = u32::try_from(page.bytes.len()).unwrap_or(u32::MAX);
        if available_at > t {
            // Inline while small and under the queue's inline budget.
            let fits = page.bytes.len() <= crate::native_queue::INLINE_PAGE_MAX
                && st.state.inline_bytes + u64::from(bytes)
                    <= crate::native_queue::SCHEDULED_INLINE_BUDGET_BYTES;
            let inline: &[u8] = if fits { &page.bytes } else { &[] };
            if fits {
                st.state.inline_bytes += u64::from(bytes);
            }
            self.nq_put(
                st,
                keys::by_time(&hash, keys::AVAIL, available_at, offset),
                encode_scheduled(t, bytes, inline),
            );
            self.nq_put(
                st,
                keys::by_offset(&hash, keys::SCHEDULED_AT, offset),
                available_at.to_le_bytes().to_vec(),
            );
            let mut range = self.nq_range(&hash, st, offset).await?;
            range.skip.set(offset);
            range.sbits.set(offset);
            range.sched += 1;
            Self::nq_set_range(st, offset, range);
            st.state.scheduled += 1;
        } else if st.state.lane_expire_at == 0 {
            // No untaken immediate message was due to expire: this one is
            // the lane's first, and expires here.
            st.state.lane_expire_at = t + cfg.retention_ms;
        }
        self.nq_put(
            st,
            keys::page(&hash, page.last),
            page.bytes,
        );
        if let Some(tag) = &m.dedup_tag {
            let expires = t + cfg.dedup_window_ms;
            self.nq_put(st, keys::dedup(&hash, tag), encode_dedup(offset, expires));
            self.nq_put(st, keys::dedup_expiry(&hash, expires, tag), Vec::new());
            st.state.dedup_entries += 1;
        }
        local.fields.next = offset + 1;
        local.fields.absorbed = local.fields.next;
        local.fields.logical += m.envelope.len() as u64;
        local.fields.ts = local.fields.ts.max(t);
        st.state.queued_messages += 1;
        st.state.queued_bytes += u64::from(bytes);
        st.state.counters.sent += 1;
        self.stats.records += 1;
        Ok(SendResult {
            offset,
            enqueued_at: t,
            available_at,
            duplicate: false,
        })
    }
}
