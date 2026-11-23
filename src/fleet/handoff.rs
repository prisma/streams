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
use tokio::time::{Instant, timeout, timeout_at};

use super::{AppState, FleetDocument, Overrides};
use crate::shard::ShardEngine;
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
pub(crate) const QUEUE: Duration = Duration::from_millis(100);
/// Step 5: the engine's close, to its verdict (D1).
const CLOSE: Duration = Duration::from_millis(300);
/// Steps 6 and 7 end this long before the hold does, for the mirror and the wake: the commit's three
/// tries share the hold's remainder less this (GATE-DECISION-s22-join-2026-11-20 Addendum 26 (a);
/// it was a fixed 400 ms, so no move committed on a store slower than that).
const MARGIN: Duration = Duration::from_millis(200);
/// Moves waiting for the runner at once.
const QUEUED: usize = 16;
/// One planned move's bound, end to end: step 0's flush, then the hold from the park, which
/// bounds steps 2 to 7.
pub(crate) const BOUND: Duration = PRE_FLUSH.saturating_add(HOLD);

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
    /// Nothing was held: the last move of this shard ended by its commit's bound and the back-off
    /// has not run out (Addendum 26 (c)).
    BackedOff,
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
    /// A planner's move, paced by the back-off ([`hand_off_paced`]); a drain's or a co-location's
    /// is not.
    paced: bool,
    done: oneshot::Sender<Outcome>,
}

/// The runner's queue (`Standing`), once this runtime's fleet started.
pub(crate) type Queue = mpsc::Sender<Request>;

/// Move `prefix` by `change`, giver first; its end. Without a runner (a fleet-off runtime, or one
/// whose runner stopped) nothing is moved, and neither is anything once this runtime's planned
/// drain has begun: from then on only the drain moves its shards (the drain addendum G5,
/// [`drained`]).
pub(crate) async fn hand_off(
    fleet: &super::FleetRepository,
    prefix: &str,
    change: Change,
    evacuable: bool,
) -> Outcome {
    if fleet.standing().drain_begun() {
        return not_started(fleet.standing(), "drain");
    }
    queued(fleet, prefix, change, (evacuable, false)).await
}

/// [`hand_off`] for return-home, count balance, the lag move and the room move: a shard whose last
/// move ended by its commit's bound is not tried again until its back-off runs out
/// ([`Outcome::BackedOff`]; Addendum 26 (c)).
pub(crate) async fn hand_off_paced(
    fleet: &super::FleetRepository,
    prefix: &str,
    change: Change,
    evacuable: bool,
) -> Outcome {
    if fleet.standing().drain_begun() {
        return not_started(fleet.standing(), "drain");
    }
    queued(fleet, prefix, change, (evacuable, true)).await
}

/// A planned drain's move of `prefix` by `change`, giver first (G1); its end.
pub(crate) async fn drained(
    fleet: &super::FleetRepository,
    prefix: &str,
    change: Change,
) -> Outcome {
    queued(fleet, prefix, change, (false, false)).await
}

/// One move through this runtime's runner; its end.
async fn queued(
    fleet: &super::FleetRepository,
    prefix: &str,
    change: Change,
    (evacuable, paced): (bool, bool),
) -> Outcome {
    let Some(queue) = fleet.standing().handoffs() else {
        return not_started(fleet.standing(), "runner");
    };
    let (done, end) = oneshot::channel();
    let prefix = prefix.to_string();
    let request = Request {
        prefix,
        change,
        evacuable,
        paced,
        done,
    };
    if queue.try_send(request).is_err() {
        return not_started(fleet.standing(), "queue");
    }
    end.await.unwrap_or(Outcome::Kept)
}

/// Start this runtime's handoff runner beside its fleet tick, and install the fleet's overrides
/// view on its peer client (the peer read hop).
pub(crate) fn start(state: Arc<AppState>, tasks: &TaskSupervisor) {
    super::view::watch(&state);
    let (queue, mut requests) = mpsc::channel(QUEUED);
    state.fleet.standing().set_handoffs(queue);
    let spawned = tasks.spawn(
        "fleet-handoff",
        Policy::Critical,
        move |cancel| async move {
            // In process: a restart clears it (Addendum 26 (c)).
            let mut paces = backoff::Paces::default();
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
                    outcome = paced(&state, &request, &mut paces) => outcome,
                };
                if request.done.send(outcome).is_err() {
                    tracing::debug!(shard = %request.prefix, ?outcome, "handoff: its caller left");
                }
            }
        },
    );
    if spawned.is_err() {
        tracing::warn!("handoff: the runtime is stopping; no runner");
    }
}

/// One move under the back-off: a paced request for a shard still backed off is answered
/// [`Outcome::BackedOff`] with nothing held; a commit that ended by its bound backs the shard off,
/// and a move that ended `Moved` clears it. The view's members and count are the back-off's epoch.
async fn paced(state: &AppState, request: &Request, paces: &mut backoff::Paces) -> Outcome {
    let prefix = request.prefix.as_str();
    let epoch = (
        state.ownership.ring_active(),
        state.fleet.standing().pass_count(),
    );
    paces.at_epoch(epoch);
    if request.paced && paces.holds(prefix, Instant::now()) {
        tracing::debug!(shard = %prefix, "handoff: backed off after its commit's bound");
        state
            .fleet
            .standing()
            .note_attempt(Outcome::BackedOff, "backoff");
        return Outcome::BackedOff;
    }
    let (outcome, committed) = run(state, request).await;
    paces.ended(prefix, request.paced, (outcome, committed), Instant::now());
    outcome
}

/// One move, steps 0 to 7, 9 and 10 (rev 2), and how its commit ended, if it reached one.
async fn run(state: &AppState, request: &Request) -> (Outcome, Option<Committed>) {
    let prefix = request.prefix.as_str();
    let Some(engine) = state.shards.open(prefix) else {
        return without_engine(state, request).await;
    };
    // Step 0: flush unparked, so the flush under the park carries only the delta (D3). One that
    // does not finish within its bound leaves more to the flush under the park, nothing else.
    let pre_flushed = matches!(
        timeout(PRE_FLUSH, engine.flush_memtable()).await,
        Ok(Ok(()))
    );
    tracing::debug!(shard = %prefix, pre_flushed, "handoff: the unparked flush");
    // Step 1: claim the engine; no split starts on it while the move holds it.
    let gate = &engine.gc_state().gate;
    if state
        .fleet
        .standing()
        .return_home_skips(prefix, i64::MAX, 0)
        || !gate.claim_move()
    {
        return (not_started(state.fleet.standing(), "claim"), None);
    }
    let claim = Claim(Arc::clone(&engine));
    // Step 2: park.
    let started = Instant::now();
    let Ok(Ok(barrier)) = timeout(PARK, engine.barrier()).await else {
        return (missed(state, request, "park"), None);
    };
    let parked_at = Instant::now();
    let until = parked_at + HOLD;
    // Step 3: flush the delta under the park.
    let flushed = matches!(timeout(FLUSH, engine.flush_memtable()).await, Ok(Ok(())));
    // Tests only: the failpoint `Standing::miss_parked_flushes` fails it once it ended.
    #[cfg(test)]
    let flushed = flushed && !state.fleet.standing().take_parked_flush_miss();
    if !flushed {
        barrier.resume();
        return (missed(state, request, "flush"), None);
    }
    let flushed_at = Instant::now();
    // Step 4: the directory's hold, the resident out of the serving map, the queue released.
    let Some((mark, resident)) = state.shards.begin_handoff(prefix, until) else {
        barrier.resume();
        return (not_started(state.fleet.standing(), "hold"), None);
    };
    barrier.hand_off();
    answered(&engine).await;
    // Step 5: the one owned close, to its verdict (D1).
    let fenced = !close(state, prefix, &engine, resident.as_ref()).await;
    let closed_at = Instant::now();
    drop(claim);
    // Steps 6 and 7, or 10, within the hold's remainder.
    let committed = commit(state, prefix, &request.change, deadline(until)).await;
    let outcome = committed.outcome();
    drop(mark);
    state.livefeed.wake_all_sessions();
    let phases = Phases {
        park: parked_at.duration_since(started),
        flush: flushed_at.duration_since(parked_at),
        close: closed_at.duration_since(flushed_at),
        release: parked_at.elapsed(),
        fenced,
        h1_kept: committed == Committed::KeptByH1,
    };
    phases.report(state, prefix, outcome);
    (outcome, Some(committed))
}

/// One planned move's phases: to the park, the flush under it, from the flush to the close's
/// verdict, and from the park to the hold's end (the release, the bound the ruling holds at 0.6 s).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Phases {
    pub(crate) park: Duration,
    pub(crate) flush: Duration,
    pub(crate) close: Duration,
    pub(crate) release: Duration,
    /// Whether the close did not end clean, so the move was a takeover by fence (E5).
    pub(crate) fenced: bool,
    /// Whether H1 kept the move: the document gave the leaf to no listed other instance.
    pub(crate) h1_kept: bool,
}

impl Phases {
    /// One line per planned move, with stable keys the rig and the measurement legs read
    /// (`handoff shard=<p> fenced=<bool> h1_kept=<bool> park_ms= flush_ms= close_ms= release_ms=
    /// end=`).
    fn report(self, state: &AppState, prefix: &str, end: Outcome) {
        tracing::info!(
            shard = %prefix,
            fenced = self.fenced,
            h1_kept = self.h1_kept,
            park_ms = self.park.as_millis(),
            flush_ms = self.flush.as_millis(),
            close_ms = self.close.as_millis(),
            release_ms = self.release.as_millis(),
            end = ?end,
            "handoff"
        );
        let standing = state.fleet.standing();
        standing.record_handoff(self, Instant::now());
        standing.note_attempt(end, "commit");
    }
}

/// D6: the prefix has no engine here. Its hold keeps any open from starting while the document
/// changes, then the waiters resolve on the new mirror.
async fn without_engine(state: &AppState, request: &Request) -> (Outcome, Option<Committed>) {
    let prefix = request.prefix.as_str();
    let until = Instant::now() + HOLD;
    let Some((mark, resident)) = state.shards.begin_handoff(prefix, until) else {
        return (not_started(state.fleet.standing(), "hold"), None);
    };
    if let Some(raced) = resident {
        // An open finished after the caller looked: it is closed as the move's.
        raced.begin_close();
    }
    let committed = commit(state, prefix, &request.change, deadline(until)).await;
    drop(mark);
    let end = committed.outcome();
    if committed == Committed::KeptByH1 {
        tracing::info!(shard = %prefix, h1_kept = true, ?end, "handoff: without an engine");
    }
    state.fleet.standing().note_attempt(end, "commit");
    (end, Some(committed))
}

/// A park or flush past its bound: a lag victim is evacuated (H4); any other move does not start.
fn missed(state: &AppState, request: &Request, step: &'static str) -> Outcome {
    tracing::info!(shard = %request.prefix, step, "handoff: missed its bound; nothing held");
    let end = if request.evacuable {
        Outcome::Evacuate
    } else {
        Outcome::NotStarted
    };
    state.fleet.standing().note_attempt(end, step);
    end
}

/// A move that did not start at `step`, noted as its attempt's end (Addendum 28).
fn not_started(standing: &super::standing::Standing, step: &'static str) -> Outcome {
    standing.note_attempt(Outcome::NotStarted, step);
    Outcome::NotStarted
}

/// H2: the released committer answers every queued operation before the close begins: wait
/// while any is queued, at most `QUEUE`.
pub(crate) async fn answered(engine: &ShardEngine) {
    let drained = async {
        while engine.queued_ops() > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    if timeout(QUEUE, drained).await.is_err() {
        tracing::debug!(shard = %engine.prefix, "handoff: the released queue outlived its bound");
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
    // The hold's resident is the move's engine unless an open raced it; a second `begin_close`
    // of one engine only wakes its parked readers again.
    if let Some(resident) = resident {
        resident.begin_close();
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

/// How a move's commit (steps 6 and 7) ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Committed {
    Moved,
    /// Three tries lost to another writer, or the document unreadable: the move is `Kept`.
    Kept,
    /// The commit's bound ran out first: the move is `Kept`, and backed off (Addendum 26 (c)).
    Bound,
    /// H1: nothing was written, and the move is `Kept`.
    KeptByH1,
}

impl Committed {
    fn outcome(self) -> Outcome {
        if self == Self::Moved {
            Outcome::Moved
        } else {
            Outcome::Kept
        }
    }
}

/// The commit's deadline under a hold that ends at `until`: its remainder less [`MARGIN`].
fn deadline(until: Instant) -> Instant {
    until.checked_sub(MARGIN).unwrap_or(until)
}

/// How one try of [`commit`] ended.
enum Try {
    /// The document read already places the shard elsewhere (D5).
    Placed,
    /// This try's CAS won: the document it wrote.
    Written(Overrides),
    /// H1: the document gives the shard to no listed other instance; nothing was written.
    KeptByH1,
    /// An unreadable document, or a CAS another writer won.
    Again,
}

/// Steps 6 and 7: write `change` for `prefix` and mirror the document, by `deadline`. Each try
/// first mirrors the document it read: one that already places the shard elsewhere (this write
/// landed with its answer lost, or another writer moved it) is the move done (D5). A won CAS is
/// the move (Addendum 26 (a)): the view is read again for the forward header's version (step 8)
/// while the deadline allows, and otherwise mirrored from the document just written.
async fn commit(state: &AppState, prefix: &str, change: &Change, deadline: Instant) -> Committed {
    let tries = async {
        for _ in 0..3 {
            match try_once(state, prefix, change).await {
                Try::Again => {}
                ended => return ended,
            }
        }
        Try::Again
    };
    match timeout_at(deadline, tries).await {
        Err(_) => Committed::Bound,
        Ok(Try::Again) => Committed::Kept,
        Ok(Try::Placed) => Committed::Moved,
        Ok(Try::KeptByH1) => Committed::KeptByH1,
        Ok(Try::Written(next)) => {
            let read = timeout_at(deadline, super::view::refreshed(state)).await;
            if !matches!(read, Ok(Some(_))) {
                mirror(state, &next, None, prefix).await;
            }
            Committed::Moved
        }
    }
}

/// One try of [`commit`].
async fn try_once(state: &AppState, prefix: &str, change: &Change) -> Try {
    let Ok((doc, version)) = state.fleet.read_overrides().await else {
        return Try::Again;
    };
    if mirror(state, &doc, version.as_ref(), prefix).await {
        return Try::Placed;
    }
    if !gives_to_a_listed_other(
        &doc,
        prefix,
        change,
        state.config.cli.instance_name.as_str(),
    ) {
        let why = "handoff: the document lists no other target (H1); nothing written";
        tracing::info!(shard = %prefix, ?change, "{why}");
        state.fleet.standing().note_h1_kept();
        return Try::KeptByH1;
    }
    let next = changed(state, doc, prefix, change).await;
    let Ok(body) = serde_json::to_vec(&next) else {
        return Try::Again;
    };
    if state
        .fleet
        .replace_document(FleetDocument::Overrides, body, version)
        .await
    {
        Try::Written(next)
    } else {
        Try::Again
    }
}

/// H1 (GATE-DECISION-tla049-r1-r3-2026-11-20 §3): whether the document `doc` lets `change` give
/// `prefix` to a listed instance other than the giver `me`, judged over its members: `To(to)` needs
/// `to` listed; `Return` needs the home over the members to be another. An older cell's document
/// (no `members`) is judged as before. Otherwise nothing is written and the move ends `Kept`.
fn gives_to_a_listed_other(doc: &Overrides, prefix: &str, change: &Change, me: &str) -> bool {
    let Some(listed) = doc.members.as_deref() else {
        return true;
    };
    let names: Vec<String> = listed.iter().map(|member| member.name.clone()).collect();
    match change {
        Change::To(to) => to != me && names.contains(to),
        Change::Return => {
            !names.is_empty() && names[crate::ownership::ring_pick(prefix, &names)] != me
        }
    }
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

/// Publish `doc`'s overrides with its own ring as this instance's mirror, under the directory's
/// revalidation lock (F2 A1); whether the mirror now places `prefix` on another instance.
async fn mirror(
    state: &AppState,
    doc: &Overrides,
    version: Option<&object_store::UpdateVersion>,
    prefix: &str,
) -> bool {
    let entries = doc.entries.iter().map(|(p, e)| (p.clone(), e.to.clone()));
    {
        let _one = state.shards.revalidation().lock().await;
        // The document's own ring over the tick's last candidates (§22 C2).
        let ring = state.fleet.standing().ring_of(doc);
        let active = ring.unwrap_or_else(|| state.ownership.ring_active());
        state.ownership.set_view(active, entries.collect());
        let tag = super::view::tag_of(version);
        state.fleet.standing().tags().set(tag);
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

mod backoff {
    //! The back-off of a planned move whose commit ran out of its bound
    //! (GATE-DECISION-s22-join-2026-11-20 Addendum 26 (c)).
    //!
    //! A store too slow for a move's commit to fit its hold fails every try after the engine is
    //! closed: each costs a close and a reopen and moves nothing. Such a shard is not tried again at
    //! the next pass: the next try waits twice the gap since the last (at least one pass), at most a
    //! minute. Per shard and in process; a move of the shard that ends `Moved`, a change of the view's
    //! members or count, and a restart clear it. Return-home, count balance, the lag move and the room
    //! move are paced by it ([`super::hand_off_paced`]); a drain and a co-location are not.

    use std::collections::HashMap;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{Committed, Outcome};

    /// The longest a shard is backed off.
    const CEILING: Duration = Duration::from_secs(60);
    /// The shortest gap a back-off doubles: one pass of the fleet tick.
    const PASS: Duration = super::super::planning::TICK_PERIOD;

    /// The view the back-offs were recorded under: its ring's members and the count its pass read.
    pub(super) type Epoch = (Vec<String>, Option<u64>);

    /// One shard's back-off: when it was last tried, and until when it is not tried again.
    #[derive(Clone, Copy, Debug)]
    struct Pace {
        tried: Instant,
        until: Instant,
    }

    /// The back-offs of this runtime's planned moves.
    #[derive(Debug, Default)]
    pub(super) struct Paces {
        epoch: Option<Epoch>,
        // mt-lint: allow(name-keyed-map): shard prefix -> its planned move's back-off
        leaves: HashMap<String, Pace>,
    }

    impl Paces {
        /// The view is now `epoch`: a change of its members or its count clears every back-off.
        pub(super) fn at_epoch(&mut self, epoch: Epoch) {
            if self.epoch.as_ref() != Some(&epoch) {
                self.leaves.clear();
                self.epoch = Some(epoch);
            }
        }

        /// Whether `prefix` is backed off at `now`.
        pub(super) fn holds(&self, prefix: &str, now: Instant) -> bool {
            self.leaves.get(prefix).is_some_and(|pace| now < pace.until)
        }

        /// `prefix`'s move ran out of its commit's bound at `now`: the next try waits twice the gap
        /// since its last (one pass when it had none, never less), at most [`CEILING`].
        pub(super) fn kept(&mut self, prefix: &str, now: Instant) {
            let since = |pace: &Pace| now.saturating_duration_since(pace.tried);
            let gap = self.leaves.get(prefix).map_or(PASS, since).max(PASS);
            let wait = gap.saturating_mul(2).min(CEILING);
            let pace = Pace {
                tried: now,
                until: now + wait,
            };
            self.leaves.insert(prefix.to_string(), pace);
        }

        /// `prefix` moved: its back-off clears.
        pub(super) fn moved(&mut self, prefix: &str) {
            self.leaves.remove(prefix);
        }

        /// `prefix`'s move ended `end` at `now`: a paced move whose commit ran out of its bound backs
        /// it off, a move that ended `Moved` clears it, and any other end leaves it as it was.
        pub(super) fn ended(
            &mut self,
            prefix: &str,
            paced: bool,
            end: (Outcome, Option<Committed>),
            now: Instant,
        ) {
            match end {
                (_, Some(Committed::Bound)) if paced => self.kept(prefix, now),
                (Outcome::Moved, _) => self.moved(prefix),
                _ => {}
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{CEILING, Committed, Outcome, PASS, Paces};
        use std::time::Duration;
        use tokio::time::Instant;

        fn epoch(members: &[&str], count: u64) -> super::Epoch {
            let members = members.iter().map(|name| (*name).to_string()).collect();
            (members, Some(count))
        }

        /// A first bound backs a shard off for two passes; each later one for twice the gap since the
        /// shard's last try, never less than two passes, never more than a minute; another shard is
        /// not backed off.
        #[test]
        fn a_shard_backs_off_twice_the_gap_since_its_last_try_up_to_a_minute() {
            let mut paces = Paces::default();
            let t0 = Instant::now();
            paces.kept("0", t0);
            let first = 2 * PASS;
            let before = |at: Instant, by: Duration| at.checked_sub(by).unwrap();
            let held = |paces: &Paces, at: Instant| (paces.holds("0", at), paces.holds("1", at));
            assert_eq!(
                held(&paces, before(t0 + first, Duration::from_millis(1))),
                (true, false)
            );
            assert_eq!(held(&paces, t0 + first), (false, false));
            let t1 = t0 + Duration::from_secs(10);
            paces.kept("0", t1);
            assert!(
                paces.holds("0", t1 + Duration::from_millis(19_999)),
                "twice a 10 s gap"
            );
            assert!(!paces.holds("0", t1 + Duration::from_secs(20)));
            paces.kept("0", t1 + Duration::from_millis(500));
            assert!(paces.holds(
                "0",
                t1 + Duration::from_millis(500) + first - Duration::from_millis(1)
            ));
            assert!(
                !paces.holds("0", t1 + Duration::from_millis(500) + first),
                "a pass at least"
            );
            let t2 = t1 + Duration::from_secs(300);
            paces.kept("0", t2);
            assert!(paces.holds("0", t2 + CEILING - Duration::from_millis(1)));
            assert!(!paces.holds("0", t2 + CEILING), "a minute at most");
        }

        /// A move that ended `Moved` clears its shard's back-off and no other; a change of the view's
        /// members, or of its count, clears every one; the same view clears none.
        #[test]
        fn a_back_off_clears_on_a_move_and_on_a_change_of_members_or_count() {
            let now = Instant::now();
            let mut paces = Paces::default();
            paces.at_epoch(epoch(&["streams-1", "streams-2"], 2));
            paces.kept("0", now);
            paces.kept("1", now);
            paces.moved("0");
            assert_eq!(
                (paces.holds("0", now), paces.holds("1", now)),
                (false, true)
            );
            paces.at_epoch(epoch(&["streams-1", "streams-2"], 2));
            assert!(paces.holds("1", now), "the same view");
            paces.at_epoch(epoch(&["streams-1", "streams-2"], 3));
            assert!(!paces.holds("1", now), "a change of count");
            paces.kept("1", now);
            paces.at_epoch(epoch(&["streams-1"], 3));
            assert!(!paces.holds("1", now), "a change of members");
        }

        /// How a move ends decides its shard's back-off: a paced move whose commit ran out of its
        /// bound backs it off and an unpaced one does not; `Moved` clears it, whether or not paced;
        /// any other end, a bound's `Kept` included, leaves it as it was (the survivors of
        /// `paced`'s match on 99472c14d).
        #[test]
        fn a_moves_end_backs_its_shard_off_clears_it_or_leaves_it() {
            let now = Instant::now();
            let bound = (Outcome::Kept, Some(Committed::Bound));
            let mut paces = Paces::default();
            paces.ended("0", false, bound, now);
            assert!(!paces.holds("0", now), "an unpaced move is not backed off");
            paces.ended("0", true, bound, now);
            assert!(paces.holds("0", now), "a paced move past its bound");
            for other in [
                (Outcome::Kept, Some(Committed::Kept)),
                (Outcome::Kept, None),
                (Outcome::NotStarted, None),
                (Outcome::Evacuate, None),
            ] {
                paces.ended("0", true, other, now);
                assert!(paces.holds("0", now), "{other:?} leaves the back-off");
            }
            paces.ended("0", false, (Outcome::Moved, Some(Committed::Moved)), now);
            assert!(!paces.holds("0", now), "Moved clears it");
        }
    }
}

#[cfg(test)]
mod h1_tests {
    use super::{Change, gives_to_a_listed_other};
    use crate::fleet::{Member, Overrides};

    /// H1's table (the join ruling §6): a move is written only when the document gives the leaf to
    /// a listed instance other than the giver: `To` this instance, an unlisted name, or nobody
    /// listed writes nothing; `Return` writes only when the home over the members is another; an
    /// older cell's document (no members) is judged as before.
    #[test]
    fn a_move_is_written_only_to_a_listed_other_instance() {
        let member = |name: &str| Member {
            name: name.to_string(),
            boot_id: None,
        };
        let listed = |names: &[&str]| Overrides {
            members: Some(names.iter().copied().map(member).collect()),
            ..Overrides::default()
        };
        let to = |name: &str| Change::To(name.to_string());
        let two = listed(&["streams-1", "streams-2"]);
        let cases = [
            (&two, to("streams-1"), false),
            (&two, to("streams-2"), true),
            (&two, to("streams-9"), false),
            (&listed(&[]), to("streams-2"), false),
            (&listed(&["streams-1"]), Change::Return, false),
            (&Overrides::default(), to("streams-9"), true),
        ];
        for (doc, change, want) in cases {
            let got = gives_to_a_listed_other(doc, "00", &change, "streams-1");
            assert_eq!(got, want, "{change:?}");
        }
        let home = |doc: &Overrides| {
            let names: Vec<String> = doc
                .members
                .iter()
                .flatten()
                .map(|m| m.name.clone())
                .collect();
            names[crate::ownership::ring_pick("00", &names)].clone()
        };
        let returned = gives_to_a_listed_other(&two, "00", &Change::Return, "streams-1");
        assert_eq!(returned, home(&two) != "streams-1");
    }
}
