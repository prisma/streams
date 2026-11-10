//! A seeded clone (the fork's `CloneBuilder::with_initial_rows`, a split's child or a merge's
//! `M`), built again when the fork refuses it for a clock skew.
//!
//! The fork has refused a merge's clone with `InvalidClockTick` (`invalid clock tick, must be
//! monotonic. last_tick=…, next_tick=…`, 1 ms apart): its `l0_ulid_cutoff` manifest invariant,
//! which every manifest update checks, refused an L0 table newly added below the current
//! manifest's newest. The refused update is not yet named (NEXT-WORK §19): a seed minted behind
//! its sources' clocks is not it, as `dst_tests::topology_clone_clock` shows. A refused clone was
//! a rolled-back split or merge, which is safe; this retry is defensive.
//!
//! [`seeded`] builds the clone again, with a fresh SlateDB clock, after the skew's wait: twice the
//! gap plus 1 ms (`MonotonicClock::now`'s rule), within [`SKEW_BUDGET`] in all. A retry finds
//! the clone's manifest stored (`create_clone_manifest`'s `try_load`) and redoes only what the
//! refused attempt left undone, from manifests it loads again. A skew past the budget, and any
//! other error, is refused at once, and the operation rolls back as before. The clone runs inside
//! the operation's pause, its barriers held, so the waits add at most [`SKEW_BUDGET`] to the
//! pause; past the barrier's 2 s hold, held operations are answered as any long pause's are.

use std::sync::Arc;
use std::time::Duration;

use slatedb_common::clock::{DefaultSystemClock, SystemClock};

/// The most a clone waits out clock skews in all: what the retry may add to a pause.
pub(crate) const SKEW_BUDGET: Duration = Duration::from_secs(1);

/// Build the seeded clone at `path` in `store` with `build`, given a fresh SlateDB clock each
/// attempt, again after each clock-skew refusal within [`SKEW_BUDGET`] (see the module).
pub(crate) async fn seeded<F, Fut>(
    store: &dyn object_store::ObjectStore,
    path: &str,
    build: F,
) -> Result<(), String>
where
    F: Fn(Arc<dyn SystemClock>) -> Fut,
    Fut: std::future::Future<Output = Result<(), slatedb::Error>>,
{
    let mut waited = Duration::ZERO;
    loop {
        let error = match build(clock_for(lag_of(store))).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let within = skew_wait(&error).filter(|wait| waited.saturating_add(*wait) <= SKEW_BUDGET);
        let Some(wait) = within else {
            return Err(format!("clone {path}: {error}"));
        };
        tracing::warn!(
            path,
            wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            "a seeded clone met a clock skew; it is built again once its clock passes it"
        );
        tokio::time::sleep(wait).await;
        waited = waited.saturating_add(wait);
    }
}

/// The wait past the skew a refusal names: twice its gap plus 1 ms, for the fork's
/// `InvalidClockTick` (an `Invalid` error naming `last_tick` above `next_tick`); `None` for any
/// other error.
fn skew_wait(error: &slatedb::Error) -> Option<Duration> {
    if !matches!(error.kind(), slatedb::ErrorKind::Invalid) {
        return None;
    }
    let text = error.to_string();
    if !text.contains("invalid clock tick") {
        return None;
    }
    let gap = tick(&text, "last_tick=`")?.checked_sub(tick(&text, "next_tick=`")?)?;
    let gap = u64::try_from(gap).ok().filter(|gap| *gap > 0)?;
    Some(Duration::from_millis(
        gap.saturating_mul(2).saturating_add(1),
    ))
}

/// The tick `text` names after `label`, up to its closing backtick.
fn tick(text: &str, label: &str) -> Option<i64> {
    let rest = text.get(text.find(label)?.checked_add(label.len())?..)?;
    rest.get(..rest.find('`')?)?.parse().ok()
}

/// The clock one attempt builds with: a fresh SlateDB clock, or (tests) one running `lag` behind.
fn clock_for(lag: Option<Duration>) -> Arc<dyn SystemClock> {
    match lag {
        #[cfg(test)]
        Some(lag) => Arc::new(test_lag::Lagging::new(lag)),
        _ => Arc::new(DefaultSystemClock::new()),
    }
}

/// How far behind a fresh clock the clones in `store` build: never, outside tests.
#[cfg(not(test))]
fn lag_of(_store: &dyn object_store::ObjectStore) -> Option<Duration> {
    None
}

/// Tests: the lag a live [`Lag`] set for `store`.
#[cfg(test)]
fn lag_of(store: &dyn object_store::ObjectStore) -> Option<Duration> {
    test_lag::of(test_lag::key(store))
}

/// Tests: while it lives, every seeded clone in its store builds with a clock running behind a
/// fresh one by its lag, as a clone whose sources' clocks run ahead of its own.
#[cfg(test)]
pub(crate) use test_lag::Lag;

#[cfg(test)]
mod test_lag;

#[cfg(test)]
mod tests;
