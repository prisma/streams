//! Send (docs/QUEUES-DESIGN.md §5.1-§5.2, §6.6, §8.4): each message becomes
//! one sealed page, a metadata row and an availability entry, in the
//! group's batch; the queue's tail stays "absorbed in place"
//! (`absorbed = next`, §5.6), so no absorption consumer ever sees a queue.
use super::stage::NativeStage;
use super::*;
use crate::native_queue::rows::{MetaRow, decode_dedup, encode_avail, encode_dedup};
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
        let bytes: u64 = new.iter().map(|m| m.envelope.len() as u64).sum();
        let s = &st.state;
        if s.queued_messages + new.len() as u64 > cfg.max_queued_messages
            || s.queued_bytes + bytes > cfg.max_queued_bytes
        {
            return Err(NqRefusal::QueueFull);
        }
        if s.dedup_entries + fresh_tags > cfg.max_dedup_entries {
            return Err(NqRefusal::DedupFull);
        }
        for m in &new {
            if m.deliver_at.is_some_and(|at| at > t + cfg.retention_ms) {
                return Err(NqRefusal::DeliverAtBeyondRetention);
            }
        }
        let mut sent = Vec::with_capacity(new.len());
        for m in new {
            sent.push(self.nq_send_one(local, hash, st, (m, &subkey, key_version, t))?);
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

    fn nq_send_one(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        args: (&SendMessage, &[u8; 32], u32, i64),
    ) -> Result<SendResult, NqRefusal> {
        let (m, subkey, key_version, t) = args;
        let offset = local.fields.next;
        let lane = crate::crypto_page::PageLane {
            key_version,
            routing_key: "",
        };
        let record = [crate::crypto_page::SealRecord {
            ts_ms: t,
            payload: &m.envelope,
        }];
        let pages = local
            .frames
            .cipher(subkey, &hash)
            .seal_request(&lane, offset, &record)
            .map_err(|e| NqRefusal::Internal(format!("page seal refused: {e:?}")))?;
        let inline = match pages.as_slice() {
            [page] if page.bytes.len() <= crate::native_queue::INLINE_PAGE_MAX => {
                page.bytes.clone()
            }
            _ => Vec::new(),
        };
        for page in pages {
            self.batch.put(
                crate::crypto_page::queue_page_key(&hash, page.last),
                page.bytes,
            );
        }
        let cfg = st.state.cfg.clone();
        let available_at = m.deliver_at.unwrap_or(t).max(t);
        let retention_end = available_at + cfg.retention_ms;
        let meta = MetaRow {
            enqueued_at: t,
            available_at,
            retention_end,
            hard_expires_at: retention_end + cfg.failure_retention_ms,
            bytes: u32::try_from(m.envelope.len()).unwrap_or(u32::MAX),
            open: 0,
            cancelled: false,
            taken: false,
            dedup_tag: m.dedup_tag,
        };
        self.nq_put(
            st,
            keys::by_offset(&hash, keys::META, offset),
            meta.encode(),
        );
        self.nq_put(
            st,
            keys::by_time(&hash, keys::AVAIL, available_at, offset),
            encode_avail(&meta, &inline),
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
        st.state.queued_bytes += u64::from(meta.bytes);
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
