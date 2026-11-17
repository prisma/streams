//! A split's timings by phase (GATE-DECISION-meas149-rerun-10000-2026-11-17, M): the pause from the
//! barrier's park, step by step (the flush, the checkpoint, the log reads, the plan, the lists, the
//! clones, `'T'`, the births, the topology CAS), then each child's open and the parent's drain
//! after the release. The published split carries them and its log line prints them, so a run's
//! log attributes its pause.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// One timed phase: its name and how many milliseconds it took.
pub(crate) type Phase = (String, u64);

/// When the current phase started, the phases recorded so far, in order, and the counts noted.
type Held = (Instant, Vec<Phase>, Vec<Phase>);

/// A split's phase recorder.
pub(crate) struct Phases(Mutex<Held>);

impl Phases {
    pub(crate) fn new() -> Self {
        Self(Mutex::new((Instant::now(), Vec::new(), Vec::new())))
    }

    /// The next phase starts now (the time before it is not recorded).
    pub(crate) fn start(&self) {
        if let Ok(mut held) = self.0.lock() {
            held.0 = Instant::now();
        }
    }

    /// The phase `phase` ended now: it took the time since the last mark or start.
    pub(crate) fn mark(&self, phase: &str) {
        if let Ok(mut held) = self.0.lock() {
            let now = Instant::now();
            let took = now.duration_since(held.0);
            held.0 = now;
            held.1.push((phase.to_string(), millis(took)));
        }
    }

    /// The phase `phase` took `took`, measured on its own (an open beside another).
    pub(crate) fn record(&self, phase: &str, took: Duration) {
        if let Ok(mut held) = self.0.lock() {
            held.1.push((phase.to_string(), millis(took)));
        }
    }

    /// A count the phases carry: `label` was `count`.
    pub(crate) fn note(&self, label: &str, count: usize) {
        if let Ok(mut held) = self.0.lock() {
            held.2
                .push((label.to_string(), u64::try_from(count).unwrap_or(u64::MAX)));
        }
    }

    /// The counts noted so far, in order.
    pub(crate) fn noted(&self) -> Vec<Phase> {
        self.0.lock().map(|held| held.2.clone()).unwrap_or_default()
    }

    /// The phases recorded so far, in order.
    pub(crate) fn taken(&self) -> Vec<Phase> {
        self.0.lock().map(|held| held.1.clone()).unwrap_or_default()
    }
}

fn millis(took: Duration) -> u64 {
    u64::try_from(took.as_millis()).unwrap_or(u64::MAX)
}

/// `phases` as `name=ms`, space-separated, in order.
pub(crate) fn shown(phases: &[Phase]) -> String {
    let parts: Vec<String> = phases
        .iter()
        .map(|(name, ms)| format!("{name}={ms}"))
        .collect();
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::{Phases, shown};
    use std::time::Duration;

    /// **Recorded phases are taken in order and print `name=ms`.** Two recorded phases are taken
    /// as `[("a", 3), ("open 0", 7)]` and print exactly `a=3 open 0=7`; none prints nothing.
    #[test]
    fn recorded_phases_are_taken_in_order_and_print_name_ms() {
        let phases = Phases::new();
        phases.record("a", Duration::from_millis(3));
        phases.record("open 0", Duration::from_millis(7));
        let taken = phases.taken();
        assert_eq!(taken, [("a".to_string(), 3), ("open 0".to_string(), 7)]);
        assert_eq!(shown(&taken), "a=3 open 0=7");
        assert_eq!(shown(&Phases::new().taken()), "", "none recorded");
        phases.note("rows", 20_000);
        phases.note("heads", 0);
        assert_eq!(shown(&phases.noted()), "rows=20000 heads=0");
        assert_eq!(phases.taken().len(), 2, "a note is no phase");
    }

    /// **A mark takes the time since the last mark or start, not since the recorder was made.**
    /// With the current phase begun 60 ms ago, a start then a mark takes under 50 ms; with it begun
    /// 60 ms ago again, a mark takes at least 60 ms.
    #[test]
    fn a_mark_takes_the_time_since_the_last_mark_or_start() {
        let phases = Phases::new();
        let back = |phases: &Phases| {
            let earlier = std::time::Instant::now().checked_sub(Duration::from_millis(60));
            phases.0.lock().unwrap().0 = earlier.unwrap();
        };
        back(&phases);
        phases.start();
        phases.mark("quick");
        back(&phases);
        phases.mark("slow");
        let took: Vec<u64> = phases.taken().iter().map(|(_, ms)| *ms).collect();
        assert!(took[0] < 50 && took[1] >= 60, "{took:?}");
    }
}
