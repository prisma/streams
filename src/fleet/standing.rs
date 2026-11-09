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
    /// Whether this runtime's planned drain has begun (`fleet::drain`).
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
    /// The prefixes the cell's live topology intent names as its sources, as this runtime's last
    /// beat read them: a rebalancer never moves one (S4-DESIGN §4.4).
    topology_sources: Mutex<Vec<String>>,
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

impl Default for Standing {
    fn default() -> Self {
        Standing {
            origin: Instant::now(),
            progress: AtomicU64::new(0),
            beats: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            draining_from: AtomicU64::new(0),
            beat_now: tokio::sync::Notify::new(),
            read: Mutex::new(Viewed::new()),
            viewed: Mutex::new(Viewed::new()),
            drained: Mutex::new(None),
            split_demand: AtomicU64::new(0),
            topology_sources: Mutex::new(Vec::new()),
            cached: Mutex::new((0, Arc::from([]))),
            docs: Mutex::new(FleetDocs::default()),
            topology: Mutex::new((None, 0)),
            decision: Mutex::new(String::new()),
        }
    }
}

impl Standing {
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

    /// The beat read the live topology intent's sources.
    pub(crate) fn set_topology_sources(&self, sources: Vec<String>) {
        if let Ok(mut kept) = self.topology_sources.lock() {
            *kept = sources;
        }
    }

    /// The prefixes the live topology intent names as its sources, as the last beat read them.
    pub(crate) fn topology_sources(&self) -> Vec<String> {
        self.topology_sources
            .lock()
            .map(|sources| sources.clone())
            .unwrap_or_default()
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

    /// This runtime's planned drain begins: its next beat, published at
    /// once, says so, and so does every beat after it.
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
