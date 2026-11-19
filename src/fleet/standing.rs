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
    /// Tests: the fleet tick's next view publication pauses after its overrides read.
    #[cfg(test)]
    view_pause: ViewPause,
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
            #[cfg(test)]
            view_pause: ViewPause(Mutex::new(None)),
            cached: Mutex::new((0, Arc::from([]))),
            docs: Mutex::new(FleetDocs::default()),
            topology: Mutex::new((None, 0)),
            decision: Mutex::new(String::new()),
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

    /// Tests: the fleet tick's next view publication answers `point.reached` after its overrides
    /// read, under the revalidation lock, and publishes once `point.go` resolves.
    #[cfg(test)]
    pub(crate) fn pause_view_publication(&self, point: crate::topology::gate::PausePoint) {
        if let Ok(mut slot) = self.view_pause.0.lock() {
            *slot = Some(point);
        }
    }

    /// Tests: the pause the next view publication takes, if one is armed.
    #[cfg(test)]
    pub(crate) fn take_view_pause(&self) -> Option<crate::topology::gate::PausePoint> {
        self.view_pause.0.lock().ok()?.take()
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
mod tests {
    use super::{DocTimes, Standing, Viewed, ViewedBeat};
    use std::sync::atomic::AtomicU64;
    use std::time::{Duration, Instant};

    /// A drain has begun from its first handoff and once it is announced, each alone; a planned
    /// move's close that did not end clean is counted, once each.
    #[test]
    fn a_drain_begins_with_its_handoffs_and_a_fenced_close_is_counted() {
        let fresh = Standing::default();
        assert!(!fresh.drain_begun());
        let handing_off = Standing::default();
        handing_off.begin_handing_off_all();
        let announced = Standing::default();
        announced.begin_draining();
        assert_eq!(
            (handing_off.drain_begun(), announced.drain_begun()),
            (true, true)
        );
        assert_eq!(fresh.handoffs_fenced(), 0);
        fresh.note_handoff_fenced();
        fresh.note_handoff_fenced();
        assert_eq!(fresh.handoffs_fenced(), 2);
    }

    /// The next beat publishes the split demand the latest tick set.
    #[test]
    fn the_latest_split_demand_is_what_the_next_beat_reads() {
        let standing = Standing::default();
        assert_eq!(standing.split_demand(), 0);
        standing.set_split_demand(7);
        assert_eq!(standing.split_demand(), 7);
        standing.set_split_demand(0);
        assert_eq!(standing.split_demand(), 0);
    }

    /// Return-home skips an entry younger than its window, to the millisecond, and any entry
    /// whose prefix the live intent names as a source, until the beat reads no intent.
    #[test]
    fn return_home_skips_a_young_entry_and_a_live_intents_source() {
        let standing = Standing::default();
        let window = 300_000;
        assert!(standing.return_home_skips("00", window - 1, 300));
        assert!(!standing.return_home_skips("00", window, 300));
        assert!(!standing.return_home_skips("00", 2_000, 0));
        standing.set_topology_intent(Some(crate::topology::intent::Live {
            op_id: "op".to_owned(),
            kind: "merge".to_owned(),
            owner: "streams-1".to_owned(),
            sources: vec!["00".to_owned(), "01".to_owned()],
        }));
        assert!(standing.return_home_skips("00", window, 300));
        assert!(standing.return_home_skips("01", i64::MAX, 300));
        assert!(!standing.return_home_skips("1", window, 300));
        standing.set_topology_intent(None);
        assert!(!standing.return_home_skips("00", window, 300));
    }

    /// **Return-home skips a live split's children** (GATE-DECISION-f2-children-on-actor A4), by
    /// the predicate the stale prune uses: while the split of `0` is live, `00` and `01` are
    /// skipped at any age, `1` and `000` are not; a live merge's sources keep their own entries and
    /// its product `0` is skipped too (§22 C19: the actor's entry for it); with no live intent none
    /// is skipped.
    #[test]
    fn return_home_skips_a_live_splits_children() {
        let standing = Standing::default();
        let live = |kind: &str, sources: &[&str]| crate::topology::intent::Live {
            op_id: "op".to_owned(),
            kind: kind.to_owned(),
            owner: "streams-1".to_owned(),
            sources: sources.iter().map(|s| (*s).to_owned()).collect(),
        };
        let old = i64::MAX;
        standing.set_topology_intent(Some(live("split", &["0"])));
        let skipped =
            ["0", "00", "01", "1", "000"].map(|p| standing.return_home_skips(p, old, 300));
        assert_eq!(skipped, [true, true, true, false, false]);
        standing.set_topology_intent(Some(live("merge", &["00", "01"])));
        let skipped = ["00", "01", "000", "0"].map(|p| standing.return_home_skips(p, old, 300));
        assert_eq!(skipped, [true, true, false, true]);
        standing.set_topology_intent(None);
        assert!(!standing.return_home_skips("00", old, 300));
    }

    fn beat(instance: &str) -> crate::fleet::Heartbeat {
        let document = format!(
            r#"{{"instance":"{instance}","ts_ms":1,"rps":0.0,"owned_shards":[],"draining":false,"seq":1}}"#
        );
        serde_json::from_str(&document).unwrap()
    }

    /// The topology policy reads the cell from the tick's latest heartbeat read, aged from
    /// that read on the monotonic clock.
    #[test]
    fn the_cached_beats_are_the_latest_reads_aged_from_it() {
        assert!(Standing::default().cached_beats().is_none());
        let standing = Standing {
            origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
            ..Standing::default()
        };
        standing.cache_beats(&[beat("streams-1"), beat("streams-2")]);
        let (age, beats) = standing.cached_beats().unwrap();
        assert!(age < 500, "{age}");
        let names: Vec<&str> = beats.iter().map(|beat| beat.instance.as_str()).collect();
        assert_eq!(names, ["streams-1", "streams-2"]);
        *standing.cached.lock().unwrap() = (1_001, beats);
        let (age, _) = standing.cached_beats().unwrap();
        assert!((4_000..4_500).contains(&age), "{age}");
        standing.cache_beats(&[]);
        assert!(standing.cached_beats().unwrap().1.is_empty());
    }

    /// The policy reads both documents' times, aged by the older read.
    #[test]
    fn the_fleet_documents_times_are_the_latest_reads_aged_by_the_older() {
        let none = DocTimes {
            desired_computed_ms: None,
            youngest_override_ms: None,
            age_ms: None,
        };
        let standing = Standing {
            origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
            ..Standing::default()
        };
        assert_eq!(standing.doc_times(), none);
        standing.note_desired(Some(7));
        assert_eq!(
            standing.doc_times().age_ms,
            None,
            "the overrides were not read yet"
        );
        standing.note_overrides(None);
        let times = standing.doc_times();
        assert_eq!(
            (times.desired_computed_ms, times.youngest_override_ms),
            (Some(7), None)
        );
        assert!(times.age_ms.unwrap() < 500, "{times:?}");
        standing.docs.lock().unwrap().desired_read = 1_001;
        standing.note_overrides(Some(9));
        let times = standing.doc_times();
        assert_eq!(times.youngest_override_ms, Some(9));
        assert!((4_000..4_500).contains(&times.age_ms.unwrap()), "{times:?}");
        standing.note_desired(None);
        assert_eq!(standing.doc_times().desired_computed_ms, None);
    }

    /// The next beat publishes the topology policy's latest pass.
    #[test]
    fn the_next_beat_reads_the_topology_policys_latest_pass() {
        let standing = Standing::default();
        assert_eq!(
            (standing.split_refused(), standing.shards_resident()),
            (None, 0)
        );
        standing.set_topology(Some("instance_full"), 7);
        assert_eq!(
            (standing.split_refused(), standing.shards_resident()),
            (Some("instance_full"), 7)
        );
        assert_eq!(standing.topology_decision(), "");
        standing.set_topology_decision("hold=cooldown".into());
        assert_eq!(standing.topology_decision(), "hold=cooldown");
        standing.set_topology(None, 3);
        assert_eq!(
            (standing.split_refused(), standing.shards_resident()),
            (None, 3)
        );
    }

    #[test]
    fn progress_ages_from_its_last_mark_and_is_absent_before_the_first() {
        assert_eq!(Standing::default().progress_age_ms(), None);
        // Marked one second after an origin five seconds ago.
        let standing = Standing {
            origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
            progress: AtomicU64::new(1_001),
            ..Standing::default()
        };
        let aged = standing.progress_age_ms().unwrap();
        assert!((4_000..4_500).contains(&aged), "{aged}");
        standing.mark_progress();
        assert!(standing.progress_age_ms().unwrap() < 500);
    }

    #[test]
    fn the_view_records_the_beats_of_the_read_it_was_computed_from() {
        let standing = Standing::default();
        let beat = |seq| {
            Viewed::from([(
                "streams-2".to_string(),
                ViewedBeat {
                    boot_id: "b".into(),
                    seq,
                },
            )])
        };
        standing.read(beat(7));
        assert!(standing.viewed().is_empty(), "no view published yet");
        standing.mark_progress();
        assert_eq!(standing.viewed(), beat(7));
        standing.read(beat(8));
        assert_eq!(standing.viewed(), beat(7), "a read alone publishes nothing");
    }

    #[tokio::test(crate = "crate::test_rt")]
    async fn a_drain_marks_every_later_beat_and_asks_for_one_at_once() {
        let standing = Standing::default();
        assert_eq!((standing.next_beat(), standing.next_beat()), (1, 2));
        assert!(!standing.draining());
        assert_eq!(standing.draining_from(), None);
        let unasked = tokio::time::timeout(Duration::from_millis(50), standing.beat_requested());
        assert!(
            unasked.await.is_err(),
            "no beat is asked for before a drain"
        );
        standing.begin_draining();
        assert!(standing.draining());
        assert_eq!(standing.draining_from(), Some(3));
        assert_eq!(standing.next_beat(), 3);
        tokio::time::timeout(Duration::from_secs(1), standing.beat_requested())
            .await
            .expect("the drain's beat is asked for at once");
    }
}
