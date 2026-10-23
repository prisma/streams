//! The maintenance sweep (docs/QUEUES-DESIGN.md §6.8): keyless, bounded
//! steps that move a queue forward on time alone — expired leases back to
//! retry, retries past their cycle into failure, untaken messages past
//! retention into the gap, failed messages past their failure expiry
//! deleted, and dedup entries past their window deleted. It writes only
//! when something moved.
use super::stage::{NativeStage, family_range};
use super::*;
use crate::native_queue::SWEEP_STEP;
use crate::native_queue::rows::{Cause, DState, decode_dedup};

impl CommitTransaction<'_> {
    pub(super) async fn nq_sweep(
        &mut self,
        hash: [u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> NqResult {
        let steps = [
            self.nq_sweep_leases(&hash, st, t).await?,
            self.nq_sweep_retries(&hash, st, t).await?,
            self.nq_sweep_untaken(&hash, st, t).await?,
            self.nq_sweep_failures(&hash, st, t).await?,
            self.nq_sweep_dedup(&hash, st, t).await?,
        ];
        let moved: usize = steps.iter().sum();
        // A step that filled its budget has more due now: look again in a
        // second rather than at the next deadline.
        let more_due = steps.iter().any(|n| *n >= SWEEP_STEP);
        if moved > 0 {
            Self::nq_tick(st, t);
        }
        let mut next = self.nq_next_deadline(&hash, st).await?;
        let wake = self.nq_wake(&hash, st, t).await?;
        if let Some(at) = Self::nq_next_notify(st) {
            next = Some(next.map_or(at, |n| n.min(at)));
        }
        if more_due {
            next = Some(t + 1_000);
        }
        Ok(NqOut::Swept {
            next_deadline: next,
            wake,
        })
    }

    /// Heads of a time-ordered family due at `t`.
    async fn nq_due(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        fam: u8,
        t: i64,
    ) -> Result<Vec<(i64, u64)>, NqRefusal> {
        let prefix = keys::family(hash, fam).len();
        let rows = self
            .nq_scan(st, family_range(hash, fam), SWEEP_STEP)
            .await
            .map_err(NqRefusal::Internal)?;
        Ok(rows
            .iter()
            .filter_map(|(k, _)| keys::decode_time_key(prefix, k))
            .take_while(|(at, _)| *at <= t)
            .collect())
    }

    /// An expired lease is a failed attempt (`lease_expired`, §6.1).
    async fn nq_sweep_leases(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<usize, NqRefusal> {
        let due = self.nq_due(hash, st, keys::LEASES, t).await?;
        for (at, off) in &due {
            let (Some(meta), Some(mut row)) = (
                self.nq_meta(hash, st, *off).await?,
                self.nq_delivery(hash, st, *off).await?,
            ) else {
                self.nq_delete(st, keys::by_time(hash, keys::LEASES, *at, *off));
                continue;
            };
            if row.state != DState::Leased {
                continue;
            }
            self.nq_unindex(hash, st, *off, &row);
            row.releases = 0;
            row.code = Some("lease_expired".into());
            self.nq_retry_or_fail(
                hash,
                st,
                (*off, &mut row, &meta),
                (Some("lease_expired".into()), t),
            );
        }
        Ok(due.len())
    }

    /// A retry still waiting at its cycle deadline fails (§6.3).
    async fn nq_sweep_retries(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<usize, NqRefusal> {
        let due = self.nq_due(hash, st, keys::RETRIES, t).await?;
        let mut moved = 0;
        for (_, off) in due {
            let (Some(meta), Some(mut row)) = (
                self.nq_meta(hash, st, off).await?,
                self.nq_delivery(hash, st, off).await?,
            ) else {
                continue;
            };
            if row.state == DState::Retrying && t >= row.cycle_deadline {
                self.nq_unindex(hash, st, off, &row);
                self.nq_fail(
                    hash,
                    st,
                    (off, &mut row, &meta),
                    (Cause::Retention, None, t),
                );
                moved += 1;
            }
        }
        Ok(moved)
    }

    /// Untaken messages past retention: the cursor skips them into the
    /// gap and they are deleted (§8.1). Retention is fixed per queue, so
    /// the availability order past the cursor is the expiry order.
    async fn nq_sweep_untaken(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<usize, NqRefusal> {
        let horizon = t - st.state.cfg.retention_ms;
        let s = &st.state;
        let range = if s.cursor_started {
            super::stage::time_range_from(
                hash,
                keys::AVAIL,
                s.cursor_at,
                s.cursor_off.saturating_add(1),
            )
        } else {
            family_range(hash, keys::AVAIL)
        };
        let prefix = keys::family(hash, keys::AVAIL).len();
        let rows = self
            .nq_scan(st, range, SWEEP_STEP)
            .await
            .map_err(NqRefusal::Internal)?;
        let mut moved = 0;
        for (k, _) in rows {
            let Some((at, off)) = keys::decode_time_key(prefix, &k) else {
                break;
            };
            if at > horizon {
                break;
            }
            st.state.cursor_at = at;
            st.state.cursor_off = off;
            st.state.cursor_started = true;
            if let Some(meta) = self.nq_meta(hash, st, off).await? {
                st.state.gap_count += 1;
                st.state.counters.expired += 1;
                self.nq_delete_message(hash, st, (off, &meta), (!meta.cancelled, t));
            }
            moved += 1;
        }
        Ok(moved)
    }

    /// Failed deliveries past their failure expiry are discarded (§7.1).
    async fn nq_sweep_failures(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<usize, NqRefusal> {
        let due = self.nq_due(hash, st, keys::FAIL_EXPIRY, t).await?;
        for (_, off) in &due {
            if let (Some(meta), Some(row)) = (
                self.nq_meta(hash, st, *off).await?,
                self.nq_delivery(hash, st, *off).await?,
            ) {
                self.nq_unindex(hash, st, *off, &row);
                self.nq_delete_message(hash, st, (*off, &meta), (false, t));
                st.state.counters.discarded += 1;
            }
        }
        Ok(due.len())
    }

    /// Dedup entries past their window are deleted (§8.4).
    async fn nq_sweep_dedup(
        &mut self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<usize, NqRefusal> {
        let prefix = keys::family(hash, keys::DEDUP_EXPIRY).len();
        let rows = self
            .nq_scan(st, family_range(hash, keys::DEDUP_EXPIRY), SWEEP_STEP)
            .await
            .map_err(NqRefusal::Internal)?;
        let mut moved = 0;
        for (k, _) in rows {
            let Some(rest) = k.get(prefix..) else { break };
            let (Ok(at_raw), Ok(tag)) = (
                <[u8; 8]>::try_from(&rest[..8.min(rest.len())]),
                <[u8; 16]>::try_from(rest.get(8..).unwrap_or(&[])),
            ) else {
                break;
            };
            let at = i64::try_from(u64::from_be_bytes(at_raw)).unwrap_or(i64::MAX);
            if at > t {
                break;
            }
            let entry = self
                .nq_get(st, &keys::dedup(hash, &tag))
                .await
                .map_err(NqRefusal::Internal)?;
            if entry
                .as_deref()
                .map(decode_dedup)
                .and_then(Result::ok)
                .is_some_and(|(_, exp)| exp <= t)
            {
                self.nq_delete(st, keys::dedup(hash, &tag));
            }
            self.nq_delete(st, k);
            st.state.dedup_entries = st.state.dedup_entries.saturating_sub(1);
            Self::nq_debt(st, 64, t);
            moved += 1;
        }
        Ok(moved)
    }

    /// The wake-up predicate (§9.2): claimable work remains and nobody
    /// claimed usefully for a backoff interval since the later of the last
    /// useful claim and the last notification. A notification followed by
    /// no useful claim doubles the backoff (1 s to 60 s).
    async fn nq_wake(
        &self,
        hash: &[u8; 16],
        st: &mut NativeStage,
        t: i64,
    ) -> Result<Option<crate::native_queue::WakeRequest>, NqRefusal> {
        let Some(url) = st.state.wakeup_url.clone() else {
            return Ok(None);
        };
        if st.state.paused || st.state.leased >= u64::from(st.state.cfg.group.max_in_flight) {
            return Ok(None);
        }
        let s = &st.state;
        if t < s.last_useful_claim_at.max(s.last_notified_at) + s.notify_backoff_ms {
            return Ok(None);
        }
        let Some(oldest) = self.nq_oldest_due(hash, st, t).await? else {
            return Ok(None);
        };
        let unanswered = st.state.last_notified_at > st.state.last_useful_claim_at;
        st.state.notify_backoff_ms = if unanswered {
            (st.state.notify_backoff_ms.max(500) * 2).min(60_000)
        } else {
            1_000
        };
        st.state.last_notified_at = t;
        st.state_dirty = true;
        Ok(Some(crate::native_queue::WakeRequest {
            url,
            name: st.state.wakeup_name.clone(),
            version: st.state.wakeup_version,
            due: st.state.queued_messages,
            oldest_due_age_ms: t - oldest,
        }))
    }

    /// When the next notification may go, if a subscription exists.
    fn nq_next_notify(st: &NativeStage) -> Option<i64> {
        let s = &st.state;
        (s.wakeup_url.is_some() && s.queued_messages > 0).then(|| {
            s.last_useful_claim_at.max(s.last_notified_at) + s.notify_backoff_ms.max(1_000)
        })
    }

    /// The due time of the oldest claimable entry, if any is due.
    async fn nq_oldest_due(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        t: i64,
    ) -> Result<Option<i64>, NqRefusal> {
        let s = &st.state;
        let a_range = if s.cursor_started {
            super::stage::time_range_from(
                hash,
                keys::AVAIL,
                s.cursor_at,
                s.cursor_off.saturating_add(1),
            )
        } else {
            family_range(hash, keys::AVAIL)
        };
        let a_prefix = keys::family(hash, keys::AVAIL).len();
        let a = self
            .nq_scan(st, a_range, 1)
            .await
            .map_err(NqRefusal::Internal)?;
        let a_due = a
            .first()
            .and_then(|(k, _)| keys::decode_time_key(a_prefix, k))
            .map(|x| x.0);
        let r_due = self.nq_head_time(hash, st, keys::RETRIES).await?;
        Ok([a_due, r_due]
            .into_iter()
            .flatten()
            .filter(|at| *at <= t)
            .min())
    }

    /// The earliest time the sweep has work again, if any.
    async fn nq_next_deadline(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
    ) -> Result<Option<i64>, NqRefusal> {
        let mut next: Option<i64> = None;
        for fam in [
            keys::LEASES,
            keys::RETRIES,
            keys::FAIL_EXPIRY,
            keys::DEDUP_EXPIRY,
        ] {
            if let Some(at) = self.nq_head_time(hash, st, fam).await? {
                next = Some(next.map_or(at, |n: i64| n.min(at)));
            }
        }
        if let Some(exp) = self.nq_untaken_expiry(hash, st).await? {
            next = Some(next.map_or(exp, |n| n.min(exp)));
        }
        Ok(next)
    }

    /// The time of a time-ordered family's first entry.
    async fn nq_head_time(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
        fam: u8,
    ) -> Result<Option<i64>, NqRefusal> {
        let prefix = keys::family(hash, fam).len();
        let rows = self
            .nq_scan(st, family_range(hash, fam), 1)
            .await
            .map_err(NqRefusal::Internal)?;
        Ok(rows
            .first()
            .and_then(|(k, _)| k.get(prefix..prefix + 8))
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map(|b| i64::try_from(u64::from_be_bytes(b)).unwrap_or(i64::MAX)))
    }

    /// When the first untaken message expires at retention.
    async fn nq_untaken_expiry(
        &self,
        hash: &[u8; 16],
        st: &NativeStage,
    ) -> Result<Option<i64>, NqRefusal> {
        let s = &st.state;
        if s.queued_messages == 0 {
            return Ok(None);
        }
        let range = if s.cursor_started {
            super::stage::time_range_from(
                hash,
                keys::AVAIL,
                s.cursor_at,
                s.cursor_off.saturating_add(1),
            )
        } else {
            family_range(hash, keys::AVAIL)
        };
        let prefix = keys::family(hash, keys::AVAIL).len();
        let rows = self
            .nq_scan(st, range, 1)
            .await
            .map_err(NqRefusal::Internal)?;
        Ok(rows
            .first()
            .and_then(|(k, _)| keys::decode_time_key(prefix, k))
            .map(|(at, _)| at + s.cfg.retention_ms))
    }
}
