//! What this runtime's heartbeat states beyond its own liveness, written by
//! the task that knows it and read by the heartbeat task at every beat
//! (item 40). One per runtime, inside its fleet repository: two runtimes in
//! one process never share it.
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    use super::{Standing, Viewed, ViewedBeat};
    use std::sync::atomic::AtomicU64;
    use std::time::{Duration, Instant};

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

    #[tokio::test]
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
