//! The shard custody the tombstone walk and the closure-debt pass
//! (`replaced.rs`) share: one segment's engine, opened within the sweep's
//! resident budget, or skipped when the ring gives its shard to another
//! instance. Every instance walks the same registry and the same debts, and
//! closes only what it owns.
use super::{
    AppState, WALK_CLOSE_SUBMITS, WALK_DEFERRED, billing_now_ms, mark, scheduler_held,
    sweep_resident_budget, walk_settle,
};
use crate::registry::StreamDesc;
use crate::shard::ShardEngine;
use std::sync::Arc;

/// Whether a pass goes on to the next segment or debt, or stops until the
/// next sweep, which resumes it at the same place (a paused read, a
/// deferred open).
#[derive(PartialEq)]
pub(super) enum Pass {
    Next,
    Stop,
}

/// R29: the walk shares the scheduler budget BEFORE opening. A shard the
/// ring gives to another instance is `Err(Pass::Next)`: skipped, never
/// opened, and closed by its owner's own pass. An engine already resident
/// is used quietly (no adoption stamp — an internal touch must not leak it
/// out of the rotation). A cold route opens only while scheduler-held
/// engines are under budget, takes custody like any discovery open, and is
/// closed (or retained as an indebted resident) by the caller right after
/// its step on that segment, whatever the step found — unless the step's
/// committer op (a close or a retention flag) was applied there: the op is
/// answered once its group is durable, so that shard holds the op's dirty
/// row, stays scheduler-held, and the next sweep's phase 1 rotates it like
/// any indebted resident. An op the committer refused (an engine that
/// retired before applying it) stops the pass, which replays the page next
/// sweep. Over budget, or an open that does not complete here, is
/// `Err(Pass::Stop)`: the pass is DEFERRED and resumes at the same place
/// next sweep, which is the continuation.
pub(super) async fn walk_engine_budgeted(
    state: &Arc<AppState>,
    route: &[u8; 16],
    budget: usize,
) -> Result<(Arc<ShardEngine>, bool), Pass> {
    let prefix = state.shards.prefix_for(route);
    if state.ownership.foreign_owner(&prefix).is_some() {
        return Err(Pass::Next);
    }
    if let Some(e) = state.shards.open(&prefix) {
        return Ok((e, false)); // resident: quiet use, not ours to close
    }
    if scheduler_held(state) >= budget {
        WALK_DEFERRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Err(Pass::Stop);
    }
    let engine = state
        .engine_for_quiet(route)
        .await
        .map_err(|_| Pass::Stop)?;
    if mark(state, &prefix, &engine) {
        Ok((engine, true)) // scheduler-held: caller settles custody
    } else {
        // Custody declined (customer raced in): quiet use.
        Ok((engine, false))
    }
}

/// The tombstone walk's step for segment `sid` of `d`, a descriptor it
/// found terminal (`terminal`) or else fork-retained, on the engine of the
/// segment's shard (`close_or_retain`). A segment on another instance's
/// shard is `Pass::Next`, skipped: its owner's walk closes it. A deferred
/// or contended open, or a segment the descriptor does not route, is
/// `Pass::Stop`: the walk resumes at this page next sweep, as it does when
/// the shard refused the step's committer op. A shard this step cold-opened
/// is settled after the step whatever it found, before the walk moves on,
/// unless the step's committer op was applied there: that shard stays
/// scheduler-held with the op's dirty row.
pub(super) async fn walk_segment(
    state: &Arc<AppState>,
    d: &StreamDesc,
    sid: u32,
    terminal: bool,
) -> Pass {
    let Some(route) = d.segment_route_by_id(sid) else {
        tracing::error!(
            segment = sid,
            "billing sweep encountered missing validated segment"
        );
        return Pass::Stop;
    };
    let budget = sweep_resident_budget(&state.config.billing);
    let (engine, ours) = match walk_engine_budgeted(state, &route, budget).await {
        Ok(acquired) => acquired,
        Err(pass) => return pass,
    };
    let (pass, applied) = close_or_retain(d, sid, terminal, &engine).await;
    if ours && !applied {
        // Scheduler-opened for this segment: close it or keep it as an
        // indebted budgeted resident NOW, on every outcome of a step whose
        // committer applied nothing (no row, another incarnation's row, a
        // failed read, a refused op) — never accumulate walk opens across
        // the page. A shard the step applied an op on keeps it (see
        // `walk_engine_budgeted`).
        walk_settle(state, &state.shards.prefix_for(&route)).await;
    }
    pass
}

/// The row of `d`'s incarnation in segment `sid` on `engine`: closed at
/// the PERSISTED logical time while its gauge is open (`terminal`), or
/// flagged `retained_by_forks`. No row, or another incarnation's, is
/// `Pass::Next`; a failed metadata read, or a close or flag the shard
/// refused unapplied, is `Pass::Stop`, so the walk replays the page next
/// sweep. The flag says whether the committer applied the step's op (the
/// close or the retention flag), durably.
async fn close_or_retain(
    d: &StreamDesc,
    sid: u32,
    terminal: bool,
    engine: &ShardEngine,
) -> (Pass, bool) {
    let hash = d.dynamic_segment_identity(sid);
    let meta = match engine.load_billing_meta(hash).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return (Pass::Next, false),
        Err(error) => {
            tracing::error!("billing sweep metadata read failed: {error}");
            return (Pass::Stop, false);
        }
    };
    if meta.stream_id != d.stream_epoch {
        return (Pass::Next, false);
    }
    let applied = if terminal && meta.owned_frame_bytes_current > 0 {
        let close_ms = if d.deleted {
            d.logical_close_ms.unwrap_or_else(billing_now_ms)
        } else {
            d.expires_at_ms.unwrap_or_else(billing_now_ms)
        };
        tracing::info!(
            "tombstone walk: closing {}#{sid} ({} B) at persisted {}",
            d.sref(),
            meta.owned_frame_bytes_current,
            close_ms
        );
        if let Err(e) = engine.submit_billing_close(hash, close_ms).await {
            tracing::warn!(
                "tombstone-walk close of {} refused, the page replays: {e}",
                d.sref()
            );
            return (Pass::Stop, false);
        }
        WALK_CLOSE_SUBMITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    } else if d.soft_deleted && !d.deleted && !meta.retained_by_forks {
        if let Err(e) = engine.submit_billing_retained(hash, true).await {
            tracing::warn!(
                "tombstone-walk retain of {} refused, the page replays: {e}",
                d.sref()
            );
            return (Pass::Stop, false);
        }
        true
    } else {
        false
    };
    (Pass::Next, applied)
}
