//! The committer side of native queues (docs/QUEUES-DESIGN.md §5-§8): every
//! queue operation reads the queue's rows through the group's stage and
//! writes them into the group's single batch, so it is durable on the
//! group's watermark and its reply is released only then (G1).
use super::*;
use crate::native_queue::{NqOp, NqOut, NqRefusal, NqResult, keys};

mod admin;
mod claim;
mod deliveries;
mod send;
mod settle;
pub(super) mod stage;
mod sweep;

use stage::NativeStage;

pub(super) type NativeReply = oneshot::Sender<NqResult>;

/// Who sets the roster: a sweep, with the deadline it computed, or any
/// other operation, which only brings it forward or takes it off.
#[derive(Clone, Copy)]
enum RosterBy {
    Op,
    Sweep(Option<i64>),
}

impl CommitTransaction<'_> {
    pub(super) async fn native_queue(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        op: NqOp,
        resp: NativeReply,
    ) {
        let mut st = match local.queue.native.take() {
            Some(st) => st,
            None => match self.nq_load(&hash).await {
                Ok(st) => st,
                Err(e) => {
                    self.effects
                        .native_acks
                        .push((resp, Err(NqRefusal::Internal(e))));
                    return;
                }
            },
        };
        let t = now_ms().max(st.state.clock);
        let out = self.nq_dispatch(local, hash, &mut st, op).await;
        if st.exists {
            let by = match &out {
                Ok(NqOut::Swept { next_deadline, .. }) => RosterBy::Sweep(*next_deadline),
                _ => RosterBy::Op,
            };
            self.nq_roster(&hash, &mut st, by, t);
        }
        self.nq_flush_state(&hash, &mut st);
        local.queue.native = Some(st);
        self.effects.native_acks.push((resp, out));
    }

    async fn nq_dispatch(
        &mut self,
        local: &mut StreamOverlay,
        hash: [u8; 16],
        st: &mut NativeStage,
        op: NqOp,
    ) -> NqResult {
        if let NqOp::Create { cfg } = op {
            return self.nq_create(local, hash, st, cfg);
        }
        if !st.exists {
            return Err(NqRefusal::NotFound);
        }
        let t = now_ms().max(st.state.clock);
        if st.state.deleting
            && !matches!(op, NqOp::DeleteStep { .. } | NqOp::Get | NqOp::BeginDelete)
        {
            return Err(NqRefusal::Deleting);
        }
        match op {
            NqOp::Create { .. } => unreachable!("handled above"),
            NqOp::Get => Ok(NqOut::Stats(Box::new(st.state.clone()))),
            NqOp::Update {
                group,
                max_queued_bytes,
                max_queued_messages,
            } => self.nq_update(st, group, max_queued_bytes, max_queued_messages),
            NqOp::BeginDelete => Ok(Self::nq_begin_delete(st, t)),
            NqOp::DeleteStep { max_rows } => self.nq_delete_step(local, hash, st, max_rows).await,
            NqOp::Send {
                messages,
                subkey,
                key_version,
            } => {
                self.nq_send(local, hash, st, (messages, subkey, key_version, t))
                    .await
            }
            NqOp::Claim {
                max,
                max_bytes,
                lease_ms,
            } => self.nq_claim(hash, st, (max, max_bytes, lease_ms), t).await,
            NqOp::Settle { items } => self.nq_settle(hash, st, items, t).await,
            NqOp::Pause { paused } => {
                st.state.paused = paused;
                st.state_dirty = true;
                Ok(NqOut::Paused(paused))
            }
            NqOp::Cancel { offsets } => self.nq_cancel(hash, st, offsets, t).await,
            NqOp::Redrive {
                offsets,
                all,
                limit,
            } => self.nq_redrive(hash, st, (offsets, all, limit), t).await,
            NqOp::Discard { offsets } => self.nq_discard(hash, st, offsets, t).await,
            NqOp::ListFailed { after, limit } => self.nq_list_failed(hash, st, after, limit).await,
            NqOp::Peek { after, limit } => self.nq_peek(hash, st, after, limit).await,
            NqOp::Message { offset } => self.nq_message(hash, st, offset).await,
            NqOp::Sweep => self.nq_sweep(hash, st, t).await,
            NqOp::ClearDebt {
                generation,
                bytes,
                captured_at,
            } => {
                if st.state.debt_generation == generation {
                    st.state.debt_bytes = 0;
                    st.state.debt_oldest_at = 0;
                } else {
                    // Debt recorded after the capture stays, aged from no
                    // earlier than the capture: the pass covered everything
                    // older.
                    st.state.debt_bytes = st.state.debt_bytes.saturating_sub(bytes);
                    st.state.debt_oldest_at = st.state.debt_oldest_at.max(captured_at);
                }
                st.state_dirty = true;
                Ok(NqOut::Count(st.state.debt_bytes))
            }
            NqOp::SetWakeup { url, name } => {
                st.state.wakeup_version = st.state.wakeup_version.wrapping_add(1);
                st.state.wakeup_url = url.clone();
                st.state.wakeup_name = name;
                st.state_dirty = true;
                Ok(NqOut::Wakeup {
                    url,
                    version: st.state.wakeup_version,
                })
            }
        }
    }

    /// Advance the queue clock to `t` (§5.5); called by every operation
    /// that changes queue state, never by one that only reads.
    pub(super) fn nq_tick(st: &mut NativeStage, t: i64) {
        st.state.clock = st.state.clock.max(t);
        st.state_dirty = true;
    }

    /// Record deleted bytes as collection debt (§8.2 stage 2).
    pub(super) fn nq_debt(st: &mut NativeStage, bytes: u64, t: i64) {
        if st.state.debt_bytes == 0 {
            st.state.debt_oldest_at = t;
        }
        st.state.debt_bytes = st.state.debt_bytes.saturating_add(bytes);
        st.state.debt_generation = st.state.debt_generation.wrapping_add(1);
        st.state_dirty = true;
    }

    /// The queue's roster row (§6.8): present while it has work that time
    /// alone moves on or debt to collect, so the sweep and the collector
    /// find it after a restart or move. Its value is when the sweep should
    /// look next. A sweep sets it to the deadline it computed; any other
    /// operation only brings it forward (a new earlier deadline), takes
    /// the queue off when nothing is pending, or puts it on. The state row
    /// records the value (`roster_at`) in the same batch, so no read of
    /// the roster row is needed.
    fn nq_roster(&mut self, hash: &[u8; 16], st: &mut NativeStage, by: RosterBy, t: i64) {
        let stored = st.state.roster_at;
        let desired = match by {
            RosterBy::Sweep(target) => target,
            RosterBy::Op => {
                Self::nq_roster_target(st, t, None).map(|new| stored.map_or(new, |at| at.min(new)))
            }
        };
        if desired == stored {
            return;
        }
        match desired {
            Some(at) => self.batch.put(keys::roster(hash), at.to_be_bytes()),
            None => self.batch.delete(keys::roster(hash)),
        }
        st.state.roster_at = desired;
        st.state_dirty = true;
        self.extra_writes = true;
    }

    /// When the sweep should look next, from the state row alone (no
    /// scans): the earliest of the deadline floors (lease expiry, a retry's
    /// cycle deadline, failure expiry, dedup expiry), the first untaken
    /// message's retention end and the next wake-up, or an hour out when
    /// none applies (only debt, or counts no floor accounts for); `None` when the queue has nothing pending and no
    /// debt. `sweep` is set when a sweep computes it: `Some(true)` when a
    /// step filled its budget (look again in a second), and a term already
    /// due that only a claim moves (due retries past the sweep's page, a
    /// wake-up with nothing claimable) waits a minute rather than a second.
    pub(super) fn nq_roster_target(st: &NativeStage, t: i64, sweep: Option<bool>) -> Option<i64> {
        let s = &st.state;
        let pending = s.queued_messages > 0
            || s.leased > 0
            || s.retrying > 0
            || s.failed > 0
            || s.dedup_entries > 0
            || s.deleting;
        if !pending && s.debt_bytes == 0 {
            return None;
        }
        if sweep == Some(true) {
            return Some(t + 1_000);
        }
        let f = &s.floors;
        let untaken = (f.avail != i64::MAX).then(|| {
            let from = if s.cursor_started {
                f.avail.max(s.cursor_at)
            } else {
                f.avail
            };
            from.saturating_add(s.cfg.retention_ms)
        });
        let parked = |at: i64| {
            if sweep.is_some() && at <= t {
                t + 60_000
            } else {
                at
            }
        };
        let terms = [
            Some(f.leases),
            Some(parked(f.retry_cycle)),
            Some(f.fail_expiry),
            Some(f.dedup_expiry),
            untaken,
            Self::nq_next_notify(st).map(parked),
        ];
        let next = terms
            .into_iter()
            .flatten()
            .filter(|at| *at != i64::MAX)
            .min();
        Some(next.unwrap_or(t + crate::native_queue::HOUR_MS))
    }
}
