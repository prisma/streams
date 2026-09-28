//! The shard custody the tombstone walk and the closure-debt pass
//! (`replaced.rs`) share: one segment's engine, opened within the sweep's
//! resident budget, or skipped when the ring gives its shard to another
//! instance. Every instance walks the same registry and the same debts, and
//! closes only what it owns.
use super::{AppState, WALK_DEFERRED, mark, scheduler_held};
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
/// closed (or retained as an indebted resident) right after its closures
/// are submitted. Over budget, or an open that does not complete here, is
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
