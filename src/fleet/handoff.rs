//! A planned move hands off giver first (meas149-rerun F6; S5 addendum F6 rev 2;
//! GATE-DECISION-f6-addendum-2026-11-17 D1-D9, GATE-DECISION-f6-prep-rows-rev2-2026-11-17 H1-H8).
//!
//! The giver parks the shard, flushes it so the target replays no WAL, takes it out of its serving
//! map under a hold every resolve of the prefix waits on, releases the parked queue into that hold,
//! closes the engine through its one owned shutdown, and only then writes the overrides change.
//! Once its own mirror names the new owner the hold ends and every waiter is forwarded there.
//!
//! **One runner per runtime (H1).** Every planned move (return-home, the lag move, count balance,
//! the room move, a co-location's half, the planned drain) asks [`hand_off`], which queues the move
//! on this runtime's `fleet-handoff` task and waits for its end. A caller whose own pass deadline
//! cuts that wait leaves the move running to its end, moved or kept: once the barrier parked,
//! nothing but the runtime's shutdown stops it, and then every guard it holds ends it as kept and
//! closes the engine through its own shutdown handle.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout};

use super::{AppState, FleetDocument, Overrides};
use crate::shard::ShardEngine;
use crate::sharddir::handoff::End;
use crate::tasks::{Policy, TaskResult, TaskSupervisor};

/// How long a planned move holds a request: the barrier's own hold
/// (`shard::transaction::barrier::HOLD`, 2 s), counted from the park.
const HOLD: Duration = Duration::from_secs(2);
/// Step 0's flush, unparked; skipped past it (D3).
const PRE_FLUSH: Duration = Duration::from_secs(2);
/// Step 2: the barrier's park (D4).
const PARK: Duration = Duration::from_millis(200);
/// Step 3: the flush of the delta under the park (D4).
const FLUSH: Duration = Duration::from_millis(300);
/// Step 4: the released queue answered (H2).
const QUEUE: Duration = Duration::from_millis(100);
/// Step 5: the engine's close, to its verdict (D1).
const CLOSE: Duration = Duration::from_millis(300);
/// Step 6: the overrides CAS, its three tries together (D4).
const CAS: Duration = Duration::from_millis(400);
/// Moves waiting for the runner at once.
const QUEUED: usize = 16;

/// The overrides change a planned move writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// An entry naming `to` (a lag move, count balance, the room move, a co-location, a drain).
    To(String),
    /// The entry dropped: the shard returns to its ring home (return-home).
    Return,
}

/// How a planned move ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The new owner is named and this instance serves the shard no more.
    Moved,
    /// The move was dropped after the hold began; the shard serves here again on demand.
    Kept,
    /// Nothing was held: another move or a split holds the engine, a live intent names it, or a
    /// park or flush missed its bound.
    NotStarted,
    /// A lag victim's park or flush missed its bound (D4, H4): the caller evacuates it by today's
    /// CAS and retirement.
    Evacuate,
}

/// One move for the runner.
pub(crate) struct Request {
    prefix: String,
    change: Change,
    /// A lag victim: a park or flush past its bound is answered [`Outcome::Evacuate`].
    evacuable: bool,
    done: oneshot::Sender<Outcome>,
}

/// The runner's queue (`Standing`), once this runtime's fleet started.
pub(crate) type Queue = mpsc::Sender<Request>;

/// Move `prefix` by `change`, giver first; its end. Without a runner (a fleet-off runtime, or one
/// whose runner stopped) nothing is moved.
pub(crate) async fn hand_off(
    fleet: &super::FleetRepository,
    prefix: &str,
    change: Change,
    evacuable: bool,
) -> Outcome {
    let Some(queue) = fleet.standing().handoffs() else {
        return Outcome::NotStarted;
    };
    let (done, end) = oneshot::channel();
    let prefix = prefix.to_string();
    let request = Request {
        prefix,
        change,
        evacuable,
        done,
    };
    if queue.try_send(request).is_err() {
        return Outcome::NotStarted;
    }
    end.await.unwrap_or(Outcome::Kept)
}

/// Start this runtime's handoff runner beside its fleet tick.
pub(super) fn start(state: Arc<AppState>, tasks: &TaskSupervisor) {
    let (queue, mut requests) = mpsc::channel(QUEUED);
    state.fleet.standing().set_handoffs(queue);
    let spawned = tasks.spawn("fleet-handoff", Policy::Critical, move |cancel| async move {
        loop {
            let request = tokio::select! {
                _ = cancel.cancelled() => return TaskResult::Done,
                request = requests.recv() => request,
            };
            let Some(request) = request else {
                return TaskResult::Done;
            };
            let outcome = tokio::select! {
                _ = cancel.cancelled() => return TaskResult::Done,
                outcome = run(&state, &request) => outcome,
            };
            if request.done.send(outcome).is_err() {
                tracing::debug!(shard = %request.prefix, ?outcome, "handoff: its caller left");
            }
        }
    });
    if spawned.is_err() {
        tracing::warn!("handoff: the runtime is stopping; no runner");
    }
}

/// One move, steps 0 to 7, 9 and 10 (rev 2).
async fn run(state: &AppState, request: &Request) -> Outcome {
    let prefix = request.prefix.as_str();
    let Some(engine) = state.shards.open(prefix) else {
        return without_engine(state, request).await;
    };
    // Step 0: flush unparked, so the flush under the park carries only the delta (D3).
    if !matches!(timeout(PRE_FLUSH, engine.flush_memtable()).await, Ok(Ok(()))) {
        tracing::debug!(shard = %prefix, "handoff: the unparked flush did not finish; going on");
    }
    // Step 1: claim the engine; no split starts on it while the move holds it.
    let gate = &engine.gc_state().gate;
    if state.fleet.standing().return_home_skips(prefix, i64::MAX, 0) || !gate.claim_move() {
        return Outcome::NotStarted;
    }
    let claim = Claim(Arc::clone(&engine));
    // Step 2: park.
    let started = Instant::now();
    let Ok(Ok(barrier)) = timeout(PARK, engine.barrier()).await else {
        return missed(request, "park");
    };
    let parked_at = Instant::now();
    let until = parked_at + HOLD;
    // Step 3: flush the delta under the park.
    if !matches!(timeout(FLUSH, engine.flush_memtable()).await, Ok(Ok(()))) {
        barrier.resume();
        return missed(request, "flush");
    }
    let flushed_at = Instant::now();
    // Step 4: the directory's hold, the resident out of the serving map, the queue released.
    let Some((mark, resident)) = state.shards.begin_handoff(prefix, until) else {
        barrier.resume();
        return Outcome::NotStarted;
    };
    barrier.hand_off();
    answered(&engine).await;
    // Step 5: the one owned close, to its verdict (D1).
    let fenced = !close(state, prefix, &engine, resident.as_ref()).await;
    let closed_at = Instant::now();
    drop(claim);
    // Steps 6 and 7, or 10.
    let end = if commit(state, prefix, &request.change).await {
        End::Moved
    } else {
        End::Kept
    };
    mark.end(end);
    state.livefeed.wake_all_sessions();
    let phases = Phases {
        park: parked_at.duration_since(started),
        flush: flushed_at.duration_since(parked_at),
        close: closed_at.duration_since(flushed_at),
        release: parked_at.elapsed(),
    };
    phases.report(state, prefix, fenced, end);
    match end {
        End::Moved => Outcome::Moved,
        End::Kept => Outcome::Kept,
    }
}

/// One planned move's phases: to the park, the flush under it, from the flush to the close's
/// verdict, and from the park to the hold's end (the release, the bound the ruling holds at 0.6 s).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Phases {
    pub(crate) park: Duration,
    pub(crate) flush: Duration,
    pub(crate) close: Duration,
    pub(crate) release: Duration,
}

impl Phases {
    /// One line per planned move, with stable keys the rig and the measurement legs read
    /// (`handoff shard=<p> fenced=<bool> park_ms= flush_ms= close_ms= release_ms= end=`).
    fn report(self, state: &AppState, prefix: &str, fenced: bool, end: End) {
        tracing::info!(
            shard = %prefix,
            fenced,
            park_ms = self.park.as_millis(),
            flush_ms = self.flush.as_millis(),
            close_ms = self.close.as_millis(),
            release_ms = self.release.as_millis(),
            end = ?end,
            "handoff"
        );
        state.fleet.standing().record_handoff(self, Instant::now());
    }
}

/// D6: the prefix has no engine here. Its hold keeps any open from starting while the document
/// changes, then the waiters resolve on the new mirror.
async fn without_engine(state: &AppState, request: &Request) -> Outcome {
    let prefix = request.prefix.as_str();
    let Some((mark, resident)) = state.shards.begin_handoff(prefix, Instant::now() + HOLD) else {
        return Outcome::NotStarted;
    };
    if let Some(raced) = resident {
        // An open finished after the caller looked: it is closed as the move's.
        raced.begin_close();
    }
    let moved = commit(state, prefix, &request.change).await;
    mark.end(if moved { End::Moved } else { End::Kept });
    if moved { Outcome::Moved } else { Outcome::Kept }
}

/// A park or flush past its bound: a lag victim is evacuated (H4); any other move does not start.
fn missed(request: &Request, step: &str) -> Outcome {
    tracing::info!(shard = %request.prefix, step, "handoff: missed its bound; nothing held");
    if request.evacuable {
        Outcome::Evacuate
    } else {
        Outcome::NotStarted
    }
}

/// H2: the released committer answers every queued operation before the close begins.
async fn answered(engine: &ShardEngine) {
    let deadline = Instant::now() + QUEUE;
    while engine.queued_ops() > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Step 5: close `engine` (and a different resident the hold took, if any) through its one owned
/// shutdown and await the verdict. A close that failed or passed its bound leaves the move a
/// takeover by fence: logged at WARN and counted (`handoff_fenced`, E5). Whether it ended clean.
async fn close(
    state: &AppState,
    prefix: &str,
    engine: &ShardEngine,
    resident: Option<&Arc<ShardEngine>>,
) -> bool {
    if let Some(other) = resident.filter(|r| !std::ptr::eq(r.as_ref(), engine)) {
        other.begin_close();
    }
    engine.begin_close();
    let verdict = engine.shutdown_handle().settle(CLOSE).await;
    let clean = matches!(verdict, Some(Ok(())));
    if !clean {
        state.fleet.standing().note_handoff_fenced();
        tracing::warn!(shard = %prefix, ?verdict, "handoff: the close did not end clean; the move is a takeover by fence");
    }
    clean
}

/// Steps 6 and 7: write `change` for `prefix` and mirror the document, within [`CAS`]. Each try
/// first mirrors the document it read: one that already places the shard elsewhere (this write
/// landed with its answer lost, or another writer moved it) is the move done (D5).
async fn commit(state: &AppState, prefix: &str, change: &Change) -> bool {
    let tries = async {
        for _ in 0..3 {
            if let Some(moved) = try_once(state, prefix, change).await {
                return moved;
            }
        }
        false
    };
    timeout(CAS, tries).await.unwrap_or(false)
}

/// One try of [`commit`]: whether the move is done, or `None` to try again (an unreadable
/// document, or a CAS another writer won).
async fn try_once(state: &AppState, prefix: &str, change: &Change) -> Option<bool> {
    let (doc, version) = state.fleet.read_overrides().await.ok()?;
    if mirror(state, &doc, version.as_ref(), prefix).await {
        return Some(true);
    }
    let next = changed(state, doc, prefix, change).await;
    let body = serde_json::to_vec(&next).ok()?;
    if !state.fleet.replace_document(FleetDocument::Overrides, body, version).await {
        return None;
    }
    // The document as stored, with its new version for the forward header (step 8).
    if super::view::refreshed(state).await.is_none() {
        return Some(mirror(state, &next, None, prefix).await);
    }
    Some(placed_elsewhere(state, prefix))
}

/// `doc` with `change` applied to `prefix`, the entries naming no stored leaf dropped (`stale`).
async fn changed(state: &AppState, doc: Overrides, prefix: &str, change: &Change) -> Overrides {
    let doc = super::stale::pruned(doc, &*state.data_store).await;
    let now = crate::shard::now_ms();
    match change {
        Change::To(to) => super::count_balance::moved_document(doc, prefix, to, now),
        Change::Return => super::moves::returned(doc, &[prefix.to_string()], now),
    }
}

/// Publish `doc`'s overrides as this instance's mirror, under the directory's revalidation lock
/// (F2 A1); whether the mirror now places `prefix` on another instance.
async fn mirror(
    state: &AppState,
    doc: &Overrides,
    version: Option<&object_store::UpdateVersion>,
    prefix: &str,
) -> bool {
    let entries = doc.entries.iter().map(|(p, e)| (p.clone(), e.to.clone()));
    {
        let _one = state.shards.revalidation().lock().await;
        let active = state.ownership.ring_active();
        state.ownership.set_view(active, entries.collect());
        let tag = super::view::tag_of(version);
        state.fleet.standing().set_overrides_tag(tag);
    }
    placed_elsewhere(state, prefix)
}

/// Whether this instance's mirror places `prefix` on another instance.
fn placed_elsewhere(state: &AppState, prefix: &str) -> bool {
    let me = state.config.cli.instance_name.as_str();
    state
        .ownership
        .effective_owner(prefix)
        .is_some_and(|owner| owner != me)
}

/// The move's claim on its engine, released when the move no longer holds it.
struct Claim(Arc<ShardEngine>);

impl Drop for Claim {
    fn drop(&mut self) {
        self.0.gc_state().gate.release_move();
    }
}
