//! What this runtime's heartbeat states beyond its own liveness, written by
//! the task that knows it and read by the heartbeat task at every beat
//! (item 40). One per runtime, inside its fleet repository: two runtimes in
//! one process never share it.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Which beat of a candidate a view was computed from: its boot and that
/// boot's beat sequence (`Heartbeat::seq`). A drain compares these with its
/// own, never one host's clock with another's.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ViewedBeat {
    pub boot_id: String,
    pub seq: u64,
}

/// Each candidate's beat, by instance.
// mt-lint: allow(name-keyed-map): fleet instance -> the beat of it a view read
pub(crate) type Viewed = BTreeMap<String, ViewedBeat>;

#[derive(Debug)]
pub(crate) struct Standing {
    /// The monotonic origin of `progress`. Ages are measured on it, never
    /// on the wall clock, which a restore or a time sync can step.
    origin: Instant,
    /// Milliseconds from `origin` to the fleet tick's last completed pass,
    /// plus one; zero until its loop starts.
    progress: AtomicU64,
    /// Beats this runtime has built.
    beats: AtomicU64,
    /// Whether this runtime's planned drain has begun handing its shards off (the drain
    /// addendum G5: from then on this runtime starts no split, merge, co-location or move but the
    /// drain's own), announced or not.
    handing_off_all: AtomicBool,
    /// Whether this runtime's planned drain has been announced (`fleet::drain`).
    draining: AtomicBool,
    /// The first beat built after the drain began; zero before it.
    draining_from: AtomicU64,
    /// Asks the heartbeat for a beat now, not at its next period.
    beat_now: tokio::sync::Notify,
    /// The draining beats the tick's latest authority read found.
    read: Mutex<Viewed>,
    /// The draining beats the published ownership view was computed from.
    viewed: Mutex<Viewed>,
    /// How this runtime's planned drain ended, once it has.
    drained: Mutex<Option<super::drain::DrainOutcome>>,
    /// How far the tick's last desired count ran past the topology's
    /// shards (`planning::Demand`): the split demand its beat publishes.
    split_demand: AtomicU64,
    /// The cell's live topology intent, as this runtime's last beat read it: a rebalancer never
    /// moves one of its sources (S4-DESIGN §4.4), and return-home leaves its sources and a live
    /// split's children where they are (GATE-DECISION-f2-children-on-actor A4).
    topology_intent: Mutex<Option<crate::topology::intent::Live>>,
    /// This runtime's planned-move runner, once its fleet started (`fleet::handoff`).
    handoffs: std::sync::OnceLock<super::handoff::Queue>,
    /// Planned moves whose close did not end clean, so the move was a takeover by fence (F6 E5).
    handoffs_fenced: AtomicU64,
    /// The overrides document's version this instance holds (F6 step 8, `fleet::tags`).
    tags: super::tags::Tags,
    /// The last planned move's phases and the instant its hold ended.
    last_handoff: Mutex<Option<(super::handoff::Phases, tokio::time::Instant)>>,
    /// Every planned move's attempt as it ended, the moves that did not start apart
    /// (GATE-DECISION-s22-join-2026-11-20 Addendum 28). In process only, on no endpoint and no
    /// metric: the handoff DSTs' wait reads it.
    attempts: Mutex<Attempts>,
    /// The halves this instance's policy is co-locating or about to merge, each with the instant
    /// its shield lapses (`shield`).
    // mt-lint: allow(name-keyed-map): shard prefix (a merge candidate's half) -> its shield's lapse
    shielded: Mutex<BTreeMap<String, tokio::time::Instant>>,
    /// Tests: the fleet tick's next view publication pauses after its overrides read.
    #[cfg(test)]
    view_pause: ViewPause,
    /// Tests: an instance the topology policy's view shows not accepting (`skew_view`).
    #[cfg(test)]
    view_skew: Mutex<Option<String>>,
    /// Tests: the failpoint `hold_moves_of_open_leaves` (C3's control), armed with its hold.
    #[cfg(test)]
    moved_open_hold: Mutex<Option<std::time::Duration>>,
    /// Tests: how many more flushes under a planned move's park the failpoint
    /// `miss_parked_flushes` fails.
    #[cfg(test)]
    parked_flush_misses: AtomicU64,
    /// The beats the tick's latest heartbeat read returned, and when, in milliseconds from
    /// `origin` plus one: the topology policy reads the cell from here, not from the store.
    cached: Mutex<(u64, Arc<[super::Heartbeat]>)>,
    /// What the tick's latest reads of desired.json and the overrides found, for the
    /// topology policy's wait on the fleet (S4 §10.1 rule 1).
    docs: Mutex<FleetDocs>,
    /// The topology policy's latest pass: why its split waits (`None`: none waits), and the
    /// shards the ring gives this instance (S5, Q4).
    topology: Mutex<(Option<&'static str>, u32)>,
    /// The topology policy's latest decision as its segments (`Decision::reason`).
    decision: Mutex<String>,
    /// The fleet tick's last pass's candidates (§22 C2): what a publication between ticks makes
    /// its document's ring from. `None` before the first pass.
    candidates: Mutex<Option<super::record::Candidates>>,
    /// The join's state (§22 C5, C13): C13's verdict on the previous beat, the first pass that
    /// found something uncovered (the `JOIN_PHASE` clock), and the joins past it.
    join: Mutex<JoinState>,
    joins_unpinned: AtomicU64,
    /// The leave (§22 step 6, `fleet::leave`): whether this boot is leaving, and its phase.
    leave: super::leave::Leave,
    /// C14's evidence watch (`fleet::dead`).
    dead: super::dead::Dead,
    /// Addendum 46's watch of beats that do not qualify, by name: the boot and own time last read,
    /// and when this boot first read that beat (`settled`).
    beat_watch: Mutex<BeatWatch>,
    /// Tests: how far behind the wall clock this runtime stamps its beats (`skew_beats`).
    #[cfg(test)]
    beat_skew_ms: AtomicU64,
    /// Planned moves H1 kept: the document gave the leaf to no listed other instance.
    h1_kept: AtomicU64,
    /// Listed members logged unstamped while live (the join ruling §2).
    unstamped_listings: AtomicU64,
    /// The planned moves this runtime recorded (`record_handoff`), since boot.
    handoffs_recorded: AtomicU64,
    /// Whether this boot is listed and still undecided past its wait (`heartbeat`), and how many
    /// boots were (the join ruling, Addendum 3).
    undecided_boot: AtomicBool,
    undecided_boots: AtomicU64,
    /// Whether this boot may beat (the join ruling, Addendum 2): its name is not listed, or its
    /// C13 decision (the in-place stamp or its unlisting) has landed and been read back.
    beat_gate: tokio::sync::watch::Sender<bool>,
    /// Tests: the heartbeat's first beat pauses once allowed, before it is written.
    #[cfg(test)]
    first_beat_pause: ViewPause,
}

/// Addendum 46's watch, by name: the beat's boot and own time last read, and when it was first read.
type BeatWatch = Vec<(String, (u64, i64, tokio::time::Instant))>;

/// How long a stale beat must stand still before a joiner's coverage counts its member gone:
/// two beat periods, since reads one period apart can fall either side of a single PUT (Addendum 46).
const WATCH: std::time::Duration = super::heartbeat::PERIOD.saturating_mul(2);

/// When this boot first read the beat `member` holds now (`beat`: its boot and own time), as
/// `watch` keeps it; a newer time or another boot starts the watch at `now`.
fn watched_since(
    watch: &mut BeatWatch,
    member: &str,
    (boot, ts_ms): (u64, i64),
    now: tokio::time::Instant,
) -> tokio::time::Instant {
    match watch.iter_mut().find(|(held, _)| held == member) {
        Some((_, (held, at, since))) if *held == boot && *at >= ts_ms => *since,
        Some((_, entry)) => {
            *entry = (boot, ts_ms, now);
            now
        }
        None => {
            watch.push((member.to_string(), (boot, ts_ms, now)));
            now
        }
    }
}

/// The join's state this runtime keeps between passes (`fleet::join`).
#[derive(Debug, Default)]
struct JoinState {
    /// C13: whether the previous beat, read before the first beat, allows a restart in place;
    /// `None` until the heartbeat task has read it.
    young: Option<bool>,
    /// Since when a joiner within the count has found something uncovered, on its own clock.
    uncovered_since: Option<tokio::time::Instant>,
    /// Each listed peer seen unstamped with live beats: its passes so far (`note_unstamped`).
    // mt-lint: allow(name-keyed-map): fleet instance -> its passes seen unstamped with live beats
    unstamped: BTreeMap<String, u32>,
    /// This boot's membership passes so far (`next_join_pass`).
    passes: u32,
    /// The pass whose join CAS was written (`note_joined`); `None` before it.
    joined_at_pass: Option<u32>,
    /// C18: a pass of this boot read it listed with its own boot id (`note_member`), until a pass
    /// finds it unlisted (`forget_member`).
    member: bool,
}

/// desired.json's `computed_at_ms` and the youngest override's `ms`, as the latest read of
/// each found them, and when each was read (milliseconds from `origin` plus one; zero before
/// the first).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct FleetDocs {
    desired_computed_ms: Option<i64>,
    youngest_override_ms: Option<i64>,
    desired_read: u64,
    overrides_read: u64,
}

/// The fleet documents' times as the topology policy reads them: each `None` where the
/// document is absent or names none, and the age of the older of the two reads (`None`
/// before both have been read once).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DocTimes {
    pub desired_computed_ms: Option<i64>,
    pub youngest_override_ms: Option<i64>,
    pub age_ms: Option<u64>,
}

/// A runtime's planned-move attempts so far (`Standing::note_attempt`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Attempts {
    /// Every attempt that ended.
    pub(crate) ended: u64,
    /// Those of them that ended `NotStarted`.
    pub(crate) not_started: u64,
    /// Those of them that ended `BackedOff`: a pass that found the shard in its back-off and
    /// tried nothing (on neither of `MoveWait`'s lines; GATE-DECISION-s22-join-2026-11-20 A31).
    pub(crate) backed_off: u64,
    /// How the last one ended, and the step it ended at.
    pub(crate) last: Option<(super::handoff::Outcome, &'static str)>,
}

/// Tests: the slot of the tick's view pause.
#[cfg(test)]
struct ViewPause(Mutex<Option<crate::topology::gate::PausePoint>>);

#[cfg(test)]
impl std::fmt::Debug for ViewPause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ViewPause")
    }
}

impl Default for Standing {
    fn default() -> Self {
        Standing {
            origin: Instant::now(),
            progress: AtomicU64::new(0),
            beats: AtomicU64::new(0),
            handing_off_all: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            draining_from: AtomicU64::new(0),
            beat_now: tokio::sync::Notify::new(),
            read: Mutex::new(Viewed::new()),
            viewed: Mutex::new(Viewed::new()),
            drained: Mutex::new(None),
            split_demand: AtomicU64::new(0),
            topology_intent: Mutex::new(None),
            handoffs: std::sync::OnceLock::new(),
            handoffs_fenced: AtomicU64::new(0),
            tags: super::tags::Tags::default(),
            last_handoff: Mutex::new(None),
            attempts: Mutex::new(Attempts::default()),
            shielded: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            view_pause: ViewPause(Mutex::new(None)),
            #[cfg(test)]
            view_skew: Mutex::new(None),
            #[cfg(test)]
            moved_open_hold: Mutex::new(None),
            #[cfg(test)]
            parked_flush_misses: AtomicU64::new(0),
            cached: Mutex::new((0, Arc::from([]))),
            docs: Mutex::new(FleetDocs::default()),
            topology: Mutex::new((None, 0)),
            decision: Mutex::new(String::new()),
            candidates: Mutex::new(None),
            join: Mutex::new(JoinState::default()),
            joins_unpinned: AtomicU64::new(0),
            leave: super::leave::Leave::default(),
            dead: super::dead::Dead::default(),
            beat_watch: Mutex::new(Vec::new()),
            #[cfg(test)]
            beat_skew_ms: AtomicU64::new(0),
            h1_kept: AtomicU64::new(0),
            unstamped_listings: AtomicU64::new(0),
            handoffs_recorded: AtomicU64::new(0),
            undecided_boot: AtomicBool::new(false),
            undecided_boots: AtomicU64::new(0),
            beat_gate: tokio::sync::watch::channel(false).0,
            #[cfg(test)]
            first_beat_pause: ViewPause(Mutex::new(None)),
        }
    }
}

impl Standing {
    /// This runtime's planned-move runner, if its fleet started.
    pub(super) fn handoffs(&self) -> Option<super::handoff::Queue> {
        self.handoffs.get().cloned()
    }

    /// The fleet started its planned-move runner; a second start keeps the first.
    pub(super) fn set_handoffs(&self, queue: super::handoff::Queue) {
        if self.handoffs.set(queue).is_err() {
            tracing::debug!("handoff: a runner is already set");
        }
    }

    /// The overrides version this instance holds, and its header-triggered reads (F6 step 8).
    pub(crate) fn tags(&self) -> &super::tags::Tags {
        &self.tags
    }

    /// A planned move ended its hold at `released` after `phases`.
    pub(super) fn record_handoff(
        &self,
        phases: super::handoff::Phases,
        released: tokio::time::Instant,
    ) {
        if let Ok(mut last) = self.last_handoff.lock() {
            *last = Some((phases, released));
        }
        self.handoffs_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Tests: how many planned moves this runtime recorded, since boot.
    #[cfg(test)]
    pub(crate) fn handoffs_recorded(&self) -> u64 {
        self.handoffs_recorded.load(Ordering::Relaxed)
    }

    /// A planned move's attempt ended `end` at `step` (Addendum 28).
    pub(super) fn note_attempt(&self, end: super::handoff::Outcome, step: &'static str) {
        if let Ok(mut attempts) = self.attempts.lock() {
            attempts.ended += 1;
            attempts.not_started += u64::from(end == super::handoff::Outcome::NotStarted);
            attempts.backed_off += u64::from(end == super::handoff::Outcome::BackedOff);
            attempts.last = Some((end, step));
        }
    }

    /// Tests: the planned moves' attempts so far.
    #[cfg(test)]
    pub(crate) fn attempts(&self) -> Attempts {
        self.attempts
            .lock()
            .map(|attempts| *attempts)
            .unwrap_or_default()
    }

    /// Tests: the last planned move's phases and the instant its hold ended.
    #[cfg(test)]
    pub(crate) fn last_handoff(&self) -> Option<(super::handoff::Phases, tokio::time::Instant)> {
        self.last_handoff.lock().ok().and_then(|last| *last)
    }

    /// Tests: whether this runtime's fleet started its planned-move runner.
    #[cfg(test)]
    pub(crate) fn runs_handoffs(&self) -> bool {
        self.handoffs.get().is_some()
    }

    /// A planned move's close did not end clean (F6 E5).
    pub(super) fn note_handoff_fenced(&self) {
        self.handoffs_fenced.fetch_add(1, Ordering::Relaxed);
    }

    /// Planned moves whose own close was late and became takeovers by fence (F6 H8; the drain
    /// addendum G6 counts a fence ahead of a move apart, on the directory).
    pub(crate) fn handoffs_fenced(&self) -> u64 {
        self.handoffs_fenced.load(Ordering::Relaxed)
    }

    /// The fleet tick completed a pass now (or its loop started): the view
    /// it published was computed from the beats its latest read found.
    pub(crate) fn mark_progress(&self) {
        self.progress.store(self.elapsed_ms(), Ordering::Relaxed);
        let read = self
            .read
            .lock()
            .map(|read| read.clone())
            .unwrap_or_default();
        if let Ok(mut viewed) = self.viewed.lock() {
            *viewed = read;
        }
    }

    /// How long ago the fleet tick last completed a pass; `None` before its
    /// loop starts.
    pub(crate) fn progress_age_ms(&self) -> Option<u64> {
        let marked = self.progress.load(Ordering::Relaxed);
        (marked > 0).then(|| self.elapsed_ms().saturating_sub(marked))
    }

    /// The tick's authority read found these beats.
    pub(crate) fn read(&self, beats: Viewed) {
        if let Ok(mut read) = self.read.lock() {
            *read = beats;
        }
    }

    /// The beats the published ownership view was computed from.
    pub(crate) fn viewed(&self) -> Viewed {
        self.viewed
            .lock()
            .map(|viewed| viewed.clone())
            .unwrap_or_default()
    }

    /// The tick's latest desired count ran `demand` instances past the
    /// topology's shards.
    pub(crate) fn set_split_demand(&self, demand: u64) {
        self.split_demand.store(demand, Ordering::Relaxed);
    }

    /// The beat read the cell's live topology intent (`None`: no live one).
    pub(crate) fn set_topology_intent(&self, live: Option<crate::topology::intent::Live>) {
        if let Ok(mut kept) = self.topology_intent.lock() {
            *kept = live;
        }
    }

    /// The tick's pass read `candidates` (§22 C2).
    pub(super) fn set_candidates(&self, candidates: &super::record::Candidates) {
        if let Ok(mut kept) = self.candidates.lock() {
            *kept = Some(candidates.clone());
        }
    }

    /// The count the tick's last pass read (`None` before its first): with the view's members, the
    /// epoch of the planned moves' back-off (`handoff::backoff`).
    pub(crate) fn pass_count(&self) -> Option<u64> {
        let candidates = self.candidates.lock().ok()?;
        candidates.as_ref().map(super::record::Candidates::count)
    }

    /// Whether `instance`'s ordinal is within the count the tick's last pass read (C11), as the
    /// membership's passes read it (`record::within_count`); true before the first pass.
    pub(crate) fn within_count(&self, instance: &str) -> bool {
        self.pass_count()
            .is_none_or(|count| super::record::within_count(instance, count))
    }

    /// Addendum 46's guard: the members whose held beat this boot may count as gone for a joiner's
    /// coverage (`Candidates::evidence_ring`). A beat that states its end (draining or withdrawn)
    /// counts at once. One that is only stale counts once this boot has read that same beat (its
    /// boot, no newer time) at least `WATCH` apart on its own monotonic clock, so a reader's clock
    /// skew alone cannot count a live peer gone in its first pass. A newer beat, another boot, or a
    /// beat that qualifies again starts the watch over. No store read is added (C16). The watch
    /// guards the joiner's step only: once the reader is a member, R1 judges the same beat by its
    /// clock alone, so this is no protection against skew (Addendum 58).
    pub(super) fn settled(&self, candidates: &super::record::Candidates) -> Vec<String> {
        let now = tokio::time::Instant::now();
        let Ok(mut watch) = self.beat_watch.lock() else {
            return Vec::new();
        };
        let mut seen = Vec::new();
        let mut settled = Vec::new();
        for (name, boot, ts_ms, stated) in candidates.unqualified() {
            seen.push(name);
            let first = watched_since(&mut watch, name, (boot, ts_ms), now);
            if stated || now.duration_since(first) >= WATCH {
                settled.push(name.to_string());
            }
        }
        watch.retain(|(name, _)| seen.contains(&name.as_str()));
        settled
    }

    /// Tests: this runtime stamps its beats `skew_ms` behind the wall clock, as a peer reads a
    /// beat when its own clock is that far ahead (Addendum 46's skew DST).
    #[cfg(test)]
    pub(crate) fn skew_beats(&self, skew_ms: u64) {
        self.beat_skew_ms.store(skew_ms, Ordering::Relaxed);
    }

    /// Tests: how far behind the wall clock this runtime stamps its beats (`skew_beats`).
    #[cfg(test)]
    pub(crate) fn beat_skew(&self) -> u64 {
        self.beat_skew_ms.load(Ordering::Relaxed)
    }

    /// The time a beat is stamped with: the wall clock, less the tests' skew.
    pub(super) fn beat_stamp(&self) -> i64 {
        let now = crate::shard::now_ms();
        #[cfg(test)]
        let now = now - i64::try_from(self.beat_skew_ms.load(Ordering::Relaxed)).unwrap_or(0);
        now
    }

    /// Tests: the tick's last pass read `count` with `names` its candidates.
    #[cfg(test)]
    pub(crate) fn note_pass(&self, count: u64, names: &[&str]) {
        let names = names.iter().map(|name| (*name).to_string()).collect();
        self.set_candidates(&super::record::Candidates::new(count, names));
    }

    /// C13: the heartbeat task read the previous beat, which `young` allows a restart in place.
    pub(super) fn decide_boot(&self, young: bool) {
        if let Ok(mut join) = self.join.lock() {
            join.young = Some(young);
        }
    }

    /// Tests: this boot's membership passes so far (a pass begins once the tick read the document).
    #[cfg(test)]
    pub(crate) fn join_passes(&self) -> u32 {
        self.join.lock().map_or(0, |join| join.passes)
    }

    /// C13's verdict, once read.
    pub(super) fn boot_young(&self) -> Option<bool> {
        self.join.lock().ok()?.young
    }

    /// Tests: C13's verdict ([`Self::boot_young`]), which a DST holds against this boot's beat in
    /// the store (GATE-DECISION-s22-join-2026-11-20 Addendum 39).
    #[cfg(test)]
    pub(crate) fn boot_verdict(&self) -> Option<bool> {
        self.boot_young()
    }

    /// C18 (the join ruling, Addendum 33): a pass of this boot read it listed with its own boot
    /// id: it is a member, and C13's boot-time verdict is not consulted again while it stays one.
    pub(super) fn note_member(&self) {
        if let Ok(mut join) = self.join.lock() {
            join.member = true;
        }
    }

    /// Item 8's clearing (Addendum 41 B): a pass found this boot unlisted, so it is no member
    /// until a pass reads it listed again. C13's boot-time verdict is not touched.
    pub(super) fn forget_member(&self) {
        if let Ok(mut join) = self.join.lock() {
            join.member = false;
        }
    }

    /// C18: whether a pass of this boot has read it a member.
    pub(super) fn decided_member(&self) -> bool {
        self.join.lock().is_ok_and(|join| join.member)
    }

    /// Since when a joiner within the count has found something uncovered (`JOIN_PHASE`).
    pub(crate) fn join_phase_since(&self) -> Option<tokio::time::Instant> {
        self.join.lock().ok()?.uncovered_since
    }

    /// A membership pass begins: its number this boot, from 1.
    pub(super) fn next_join_pass(&self) -> u32 {
        let Ok(mut join) = self.join.lock() else {
            return 0;
        };
        join.passes = join.passes.saturating_add(1);
        join.passes
    }

    /// This boot's join CAS was written at the current pass: that pass's number.
    pub(super) fn note_joined(&self) -> u32 {
        let Ok(mut join) = self.join.lock() else {
            return 0;
        };
        join.joined_at_pass = Some(join.passes);
        join.passes
    }

    /// The pass whose join CAS this boot wrote, if it has (the cold-convergence DST).
    #[cfg(test)]
    pub(crate) fn joined_at_pass(&self) -> Option<u32> {
        self.join.lock().ok()?.joined_at_pass
    }

    /// A joiner's pass found something uncovered with its ordinal within the count (`waiting`), or
    /// not: the clock starts at the first such pass and restarts when the count leaves it out or
    /// nothing is uncovered (§3 of the ruling).
    pub(super) fn note_uncovered(&self, waiting: bool) {
        if let Ok(mut join) = self.join.lock() {
            join.uncovered_since = match (waiting, join.uncovered_since) {
                (true, Some(since)) => Some(since),
                (true, None) => Some(tokio::time::Instant::now()),
                (false, _) => None,
            };
        }
    }

    /// Tests: the `JOIN_PHASE` clock started `by` earlier (a DST cannot wait 139 s).
    #[cfg(test)]
    pub(crate) fn backdate_join_phase(&self, by: std::time::Duration) {
        if let Ok(mut join) = self.join.lock() {
            join.uncovered_since = join.uncovered_since.and_then(|since| since.checked_sub(by));
        }
    }

    /// This pass saw `live_unstamped` listed unstamped with live beats: the names that reach
    /// `join::UNSTAMPED_PASSES` such passes now (each once), counted `unstamped_listings`. A name
    /// stamped, unlisted or no longer live is forgotten.
    pub(super) fn note_unstamped(&self, live_unstamped: Vec<String>) -> Vec<String> {
        let Ok(mut join) = self.join.lock() else {
            return Vec::new();
        };
        join.unstamped
            .retain(|name, _| live_unstamped.contains(name));
        let mut reached = Vec::new();
        for name in live_unstamped {
            let passes = join.unstamped.entry(name.clone()).or_insert(0);
            *passes += 1;
            if *passes == super::join::UNSTAMPED_PASSES {
                reached.push(name);
            }
        }
        let reached_count = u64::try_from(reached.len()).unwrap_or(u64::MAX);
        self.unstamped_listings
            .fetch_add(reached_count, Ordering::Relaxed);
        reached
    }

    /// This boot may beat from now on (`fleet::join::pass`): one-way.
    pub(super) fn allow_beats(&self) {
        self.beat_gate.send_replace(true);
    }

    /// This boot is listed and undecided past its wait (`true`, counted), or no longer (`false`).
    pub(super) fn note_undecided_boot(&self, undecided: bool) {
        if undecided {
            self.undecided_boots.fetch_add(1, Ordering::Relaxed);
        }
        self.undecided_boot.store(undecided, Ordering::SeqCst);
    }

    /// Why this instance is not ready, if its boot is listed and undecided past its wait.
    pub(crate) fn undecided_boot(&self) -> Option<&'static str> {
        self.undecided_boot
            .load(Ordering::SeqCst)
            .then_some("fleet boot undecided: listed, and no decision of this boot in the document")
    }

    /// Wait until this boot may beat (see `allow_beats`).
    pub(super) async fn beats_allowed(&self) {
        let mut allowed = self.beat_gate.subscribe();
        if allowed.wait_for(|allowed| *allowed).await.is_err() {
            tracing::debug!("fleet: the beat gate closed");
        }
    }

    /// Tests: the heartbeat's first beat answers `point.reached` once allowed, and is written once
    /// `point.go` resolves (a process killed in that window never writes it).
    #[cfg(test)]
    pub(crate) fn pause_first_beat(&self, point: crate::topology::gate::PausePoint) {
        if let Ok(mut slot) = self.first_beat_pause.0.lock() {
            *slot = Some(point);
        }
    }

    /// Tests: the pause the first beat takes, if one is armed.
    #[cfg(test)]
    pub(super) fn take_first_beat_pause(&self) -> Option<crate::topology::gate::PausePoint> {
        self.first_beat_pause.0.lock().ok()?.take()
    }

    /// A join committed past `JOIN_PHASE` with a leaf uncovered (E5's `joins_unpinned`).
    pub(super) fn note_join_unpinned(&self) {
        self.joins_unpinned.fetch_add(1, Ordering::Relaxed);
    }

    /// A planned move H1 kept (its count, since boot: `h1_kept`).
    pub(super) fn note_h1_kept(&self) {
        self.h1_kept.fetch_add(1, Ordering::Relaxed);
    }

    /// Tests: the planned moves H1 kept, since boot.
    #[cfg(test)]
    pub(crate) fn h1_kept(&self) -> u64 {
        self.h1_kept.load(Ordering::Relaxed)
    }

    /// Tests: the joins committed past `JOIN_PHASE`, since boot.
    #[cfg(test)]
    pub(crate) fn joins_unpinned(&self) -> u64 {
        self.joins_unpinned.load(Ordering::Relaxed)
    }

    /// Tests: the listed peers that reached `join::UNSTAMPED_PASSES` unstamped passes, since boot.
    #[cfg(test)]
    pub(crate) fn unstamped_listings(&self) -> u64 {
        self.unstamped_listings.load(Ordering::Relaxed)
    }

    /// Tests: the times this boot was found listed and undecided past its wait, since boot.
    #[cfg(test)]
    pub(crate) fn undecided_boots(&self) -> u64 {
        self.undecided_boots.load(Ordering::Relaxed)
    }

    /// The ring of `document`, over the tick's last pass's candidates (§22 C1, C2): what every
    /// publication between ticks pairs with that document's entries. `None` before the first
    /// pass, when the caller keeps the view's ring (it creates no view an instance awaiting its
    /// first one lacks).
    pub(crate) fn ring_of(&self, document: &super::Overrides) -> Option<Vec<String>> {
        let candidates = self.candidates.lock().ok()?;
        Some(candidates.as_ref()?.ring(document.members.as_deref()))
    }

    /// The members this instance may hand a shard to over `document`, from its last pass's
    /// candidates (`drain::successors`, C11); none before its first pass.
    pub(crate) fn successors(&self, document: &super::Overrides) -> Vec<String> {
        let candidates = self.candidates.lock().ok().and_then(|kept| kept.clone());
        candidates.map_or_else(Vec::new, |kept| super::drain::successors(&kept, document))
    }

    /// Tests: the fleet tick's next view publication answers `point.reached` after its overrides
    /// read, under the revalidation lock, and publishes once `point.go` resolves.
    #[cfg(test)]
    pub(crate) fn pause_view_publication(&self, point: crate::topology::gate::PausePoint) {
        if let Ok(mut slot) = self.view_pause.0.lock() {
            *slot = Some(point);
        }
    }

    /// Tests, a failpoint (C3's control for a move that is not giver first,
    /// GATE-DECISION-s22-join-2026-11-20 Addendum 9): every view publication that gives elsewhere a
    /// leaf whose engine is still open here waits `hold` first, the revalidation lock held, so a
    /// target that learns of the move at its own tick opens the leaf while this instance still
    /// serves it. A planned move never meets it: its giver closes the engine before it writes.
    #[cfg(test)]
    pub(crate) fn hold_moves_of_open_leaves(&self, hold: std::time::Duration) {
        if let Ok(mut slot) = self.moved_open_hold.lock() {
            *slot = Some(hold);
        }
    }

    /// Tests: the failpoint's hold, if armed (`hold_moves_of_open_leaves`).
    #[cfg(test)]
    pub(crate) fn moved_open_hold(&self) -> Option<std::time::Duration> {
        self.moved_open_hold.lock().ok().and_then(|slot| *slot)
    }

    /// Tests, a failpoint (GATE-DECISION-s22-join-2026-11-20 Addendum 28): the next `misses`
    /// flushes under a planned move's park fail their bound once they end, as one past `FLUSH`
    /// does (`fleet::handoff`, step 3).
    #[cfg(test)]
    pub(crate) fn miss_parked_flushes(&self, misses: u64) {
        self.parked_flush_misses.store(misses, Ordering::Relaxed);
    }

    /// Tests: whether the failpoint `miss_parked_flushes` fails this flush under the park, and
    /// if so one fewer after it.
    #[cfg(test)]
    pub(crate) fn take_parked_flush_miss(&self) -> bool {
        let fewer = |misses: u64| misses.checked_sub(1);
        let misses = &self.parked_flush_misses;
        misses
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, fewer)
            .is_ok()
    }

    /// Tests: the pause the next view publication takes, if one is armed.
    #[cfg(test)]
    pub(crate) fn take_view_pause(&self) -> Option<crate::topology::gate::PausePoint> {
        self.view_pause.0.lock().ok()?.take()
    }

    /// Tests: the topology policy's view of the cell shows `instance` not accepting, as a view
    /// that lags its peers' would, until it is cleared (`None`).
    #[cfg(test)]
    pub(crate) fn skew_view(&self, instance: Option<&str>) {
        if let Ok(mut slot) = self.view_skew.lock() {
            *slot = instance.map(str::to_owned);
        }
    }

    /// Tests: the instance `skew_view` set.
    #[cfg(test)]
    pub(crate) fn view_skew(&self) -> Option<String> {
        self.view_skew.lock().ok()?.clone()
    }

    /// Tests: a live split intent naming `sources`, as a beat would have read it.
    #[cfg(test)]
    pub(crate) fn set_topology_sources(&self, sources: Vec<String>) {
        self.set_topology_intent(Some(crate::topology::intent::Live {
            op_id: "op-test".to_owned(),
            kind: "split".to_owned(),
            owner: String::new(),
            sources,
        }));
    }

    /// The prefixes the live topology intent names as its sources, as the last beat read them.
    pub(crate) fn topology_sources(&self) -> Vec<String> {
        self.topology_intent
            .lock()
            .ok()
            .and_then(|live| live.as_ref().map(|l| l.sources.clone()))
            .unwrap_or_default()
    }

    /// Shield `halves` until `lapse` from now, renewing any mark (GATE-DECISION-s22-join-2026-11-20
    /// Addendum 13 (2)): this instance's count balance and lag move ([`Standing::unmovable`]) and
    /// its return-home ([`Standing::shielded`]) leave them where they are meanwhile. It lives in
    /// this process only: nothing is written to the overrides document for it.
    pub(crate) fn shield(&self, halves: &[String], lapse: std::time::Duration) {
        let until = tokio::time::Instant::now() + lapse;
        if let Ok(mut marks) = self.shielded.lock() {
            for half in halves {
                marks.insert(half.clone(), until);
            }
        }
    }

    /// The live shields, each lapsed one dropped first: the one place a lapse is judged, so every
    /// reader sees what it leaves. `None` on a poisoned lock, which reads as no shield.
    fn shields(&self) -> Option<std::sync::MutexGuard<'_, BTreeMap<String, tokio::time::Instant>>> {
        let mut marks = self.shielded.lock().ok()?;
        let now = tokio::time::Instant::now();
        marks.retain(|_, until| *until > now);
        Some(marks)
    }

    /// Whether `prefix` is shielded now (`shield`).
    pub(crate) fn shielded(&self, prefix: &str) -> bool {
        self.shields()
            .is_some_and(|marks| marks.contains_key(prefix))
    }

    /// The shards no move of this instance's own may take: the live topology intent's sources
    /// (S4-DESIGN §4.4) and the halves it shields (`shield`). Count balance and the lag move read
    /// it.
    pub(crate) fn unmovable(&self) -> Vec<String> {
        let mut unmovable = self.topology_sources();
        if let Some(marks) = self.shields() {
            unmovable.extend(marks.keys().cloned());
        }
        unmovable
    }

    /// Whether return-home leaves the override on `prefix` in place this tick: the entry is
    /// younger (`age_ms`) than the return window of `return_secs`, or the live topology intent, as
    /// the last beat read it, names `prefix` as a source (S4-DESIGN §4.4) or keeps it as a split's
    /// child (`intent::Live::keeps`, A4). A poisoned lock reads as no intent, as
    /// `topology_sources` does.
    pub(crate) fn return_home_skips(&self, prefix: &str, age_ms: i64, return_secs: i64) -> bool {
        let named = |live: &crate::topology::intent::Live| {
            live.sources.iter().any(|source| source == prefix) || live.keeps(prefix)
        };
        age_ms < return_secs.saturating_mul(1000)
            || self
                .topology_intent
                .lock()
                .is_ok_and(|live| live.as_ref().is_some_and(named))
    }

    /// The split demand the next beat publishes.
    pub(crate) fn split_demand(&self) -> u64 {
        self.split_demand.load(Ordering::Relaxed)
    }

    /// The tick's heartbeat read returned `beats` now.
    pub(crate) fn cache_beats(&self, beats: &[super::Heartbeat]) {
        if let Ok(mut cached) = self.cached.lock() {
            *cached = (self.elapsed_ms(), Arc::from(beats));
        }
    }

    /// The beats the tick's latest heartbeat read returned, and how long ago it returned
    /// them; `None` before its first read.
    pub(crate) fn cached_beats(&self) -> Option<(u64, Arc<[super::Heartbeat]>)> {
        let (read, beats) = self.cached.lock().ok()?.clone();
        (read > 0).then(|| (self.elapsed_ms().saturating_sub(read), beats))
    }

    /// A read of desired.json found it computed at `computed_ms` (`None`: absent).
    pub(crate) fn note_desired(&self, computed_ms: Option<i64>) {
        let at = self.elapsed_ms();
        if let Ok(mut docs) = self.docs.lock() {
            (docs.desired_computed_ms, docs.desired_read) = (computed_ms, at);
        }
    }

    /// A read of the overrides found `youngest_ms` the newest entry's time (`None`: none).
    pub(crate) fn note_overrides(&self, youngest_ms: Option<i64>) {
        let at = self.elapsed_ms();
        if let Ok(mut docs) = self.docs.lock() {
            (docs.youngest_override_ms, docs.overrides_read) = (youngest_ms, at);
        }
    }

    /// The fleet documents' times the latest reads found.
    pub(crate) fn doc_times(&self) -> DocTimes {
        let docs = self.docs.lock().map(|docs| *docs).unwrap_or_default();
        let older = docs.desired_read.min(docs.overrides_read);
        DocTimes {
            desired_computed_ms: docs.desired_computed_ms,
            youngest_override_ms: docs.youngest_override_ms,
            age_ms: (older > 0).then(|| self.elapsed_ms().saturating_sub(older)),
        }
    }

    /// The topology policy's pass found its split waiting for `split_refused` (or none) with
    /// `shards_resident` shards on this instance.
    pub(crate) fn set_topology(&self, split_refused: Option<&'static str>, shards_resident: u32) {
        if let Ok(mut topology) = self.topology.lock() {
            *topology = (split_refused, shards_resident);
        }
    }

    /// The topology policy's pass decided `decision` (its segments).
    pub(crate) fn set_topology_decision(&self, decision: String) {
        if let Ok(mut held) = self.decision.lock() {
            *held = decision;
        }
    }

    /// The topology policy's latest decision, for the next beat; empty before its first pass.
    pub(crate) fn topology_decision(&self) -> String {
        self.decision
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }

    /// Why the topology policy's split waits, for the next beat.
    pub(crate) fn split_refused(&self) -> Option<&'static str> {
        self.topology.lock().map_or(None, |topology| topology.0)
    }

    /// The shards the topology policy counts on this instance, for the next beat.
    pub(crate) fn shards_resident(&self) -> u32 {
        self.topology.lock().map_or(0, |topology| topology.1)
    }

    /// The next beat's sequence number. A beat takes it before it reads
    /// whether the runtime drains.
    pub(crate) fn next_beat(&self) -> u64 {
        self.beats.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// This runtime's planned drain begins handing its shards off, before it announces (G1, G5).
    pub(crate) fn begin_handing_off_all(&self) {
        self.handing_off_all.store(true, Ordering::SeqCst);
    }

    /// Whether this runtime's planned drain has begun, its handoffs included: it starts no
    /// topology change and no move but the drain's own (G5).
    pub(crate) fn drain_begun(&self) -> bool {
        self.handing_off_all.load(Ordering::SeqCst) || self.draining()
    }

    /// C14's evidence watch (`fleet::dead`).
    pub(crate) fn dead(&self) -> &super::dead::Dead {
        &self.dead
    }

    /// This boot's leave (`fleet::leave`).
    pub(crate) fn leave(&self) -> &super::leave::Leave {
        &self.leave
    }

    /// Whether this runtime is departing, by a planned drain or a leave: it starts no topology
    /// change and no move but its departure's own (G5; Addendum 41 B, the draft §2).
    pub(crate) fn departing(&self) -> bool {
        self.drain_begun() || self.leave.leaving()
    }

    /// This runtime's planned drain is announced: its next beat, published
    /// at once, says so, and so does every beat after it.
    pub(crate) fn begin_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
        let first = self.beats.load(Ordering::SeqCst) + 1;
        self.draining_from.store(first, Ordering::SeqCst);
        self.beat_now.notify_one();
    }

    pub(crate) fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// The sequence from which every beat says the runtime drains; `None`
    /// before the drain begins.
    pub(crate) fn draining_from(&self) -> Option<u64> {
        let first = self.draining_from.load(Ordering::SeqCst);
        (first > 0).then_some(first)
    }

    /// The planned drain ended with `outcome`.
    pub(crate) fn record_drain(&self, outcome: super::drain::DrainOutcome) {
        if let Ok(mut drained) = self.drained.lock() {
            *drained = Some(outcome);
        }
    }

    /// How the planned drain ended; `None` before it has.
    #[cfg(test)]
    pub(crate) fn drain_outcome(&self) -> Option<super::drain::DrainOutcome> {
        self.drained.lock().ok().and_then(|drained| drained.clone())
    }

    /// Resolves when a beat is asked for ahead of its period.
    pub(crate) async fn beat_requested(&self) {
        self.beat_now.notified().await;
    }

    /// Milliseconds since `origin`, plus one, so a mark is never zero.
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }
}

#[cfg(test)]
mod tests;
