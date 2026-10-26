//! Subscription units (layout 6A package 2, REVIEW-6A F15's memory line):
//! what the streams a multiplexed wait watches may hold on the instance.
//!
//! A multiplexed wait (`POST /v1/projects/{p}/live:wait`) takes ONE place
//! in the live-connection pool and its project's share of it however many
//! streams it watches, so the pool stays the socket and CPU boundary it
//! is. What grows with the streams it watches is memory: each watched
//! stream holds its readiness wait, cursor and handle while the request
//! waits (about 2 KiB measured). Each watched stream therefore takes one
//! unit for the request's lifetime, from two budgets:
//!
//! - the instance's: the RSS shed line divided by `SHARE_OF_SHED`, in
//!   `UNIT_BYTES` units (8,000 at the 500 MB line: 31.25 MiB, beside read
//!   memory's quarter of the line, so the certified 1 GiB baseline plus
//!   both stays under the line);
//! - its project's: the instance's divided by the cell's share k
//!   (`PROJECT_SHARE_K`), as every other shared bound (`auth::ceiling`);
//!   a dedicated cell (k = 1) gives its project the whole budget.
//!
//! A 0 shed line (off, as rigs run) leaves the units unbounded. The units
//! are counted, never estimated: a request releases what it took when it
//! is served or dropped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::tenant::ProjectId;

/// The instance's units are this share of the RSS shed line.
const SHARE_OF_SHED: u64 = 16;
/// What one unit stands for: one watched stream's wait state, with margin.
const UNIT_BYTES: u64 = 4 << 10;

/// The instance's subscription units and who holds them.
pub(crate) struct LiveUnits {
    /// 0 = unbounded.
    capacity: u64,
    held: Mutex<Held>,
}

#[derive(Default)]
struct Held {
    total: u64,
    /// Only projects holding units: an entry is removed at 0.
    projects: HashMap<ProjectId, u64>,
}

/// Why units were not taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnitRefusal {
    /// The project's share is full.
    Project,
    /// The instance's budget is full.
    Instance,
}

/// Units one request holds; released on drop.
pub(crate) struct UnitsHeld {
    units: Arc<LiveUnits>,
    project: Option<ProjectId>,
    count: u64,
}

impl super::AdmissionController {
    /// The instance's subscription units.
    pub(crate) fn live_units(&self) -> &Arc<LiveUnits> {
        &self.inner.live_units
    }
}

impl LiveUnits {
    pub(crate) fn new(rss_shed_mb: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity: rss_shed_mb.saturating_mul(1 << 20) / SHARE_OF_SHED / UNIT_BYTES,
            held: Mutex::new(Held::default()),
        })
    }

    /// The instance's budget (0 = unbounded) and the units held now.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> (u64, u64) {
        let held = self.held.lock().map_or(0, |held| held.total);
        (self.capacity, held)
    }

    /// Take `count` units for one request of `project` (none: the
    /// deployment's own) on a cell shared `share_k` ways, from the
    /// project's share first, then the instance's budget; neither moves
    /// when either refuses. A poisoned ledger refuses as the instance.
    pub(crate) fn take(
        self: &Arc<Self>,
        project: Option<&ProjectId>,
        count: u64,
        share_k: u64,
    ) -> Result<UnitsHeld, UnitRefusal> {
        let mut held = self.held.lock().map_err(|_| UnitRefusal::Instance)?;
        if self.capacity > 0 {
            let share = self.capacity / share_k.max(1);
            let mine = project
                .and_then(|p| held.projects.get(p))
                .copied()
                .unwrap_or(0);
            if project.is_some() && mine.saturating_add(count) > share {
                return Err(UnitRefusal::Project);
            }
            if held.total.saturating_add(count) > self.capacity {
                return Err(UnitRefusal::Instance);
            }
        }
        held.total = held.total.saturating_add(count);
        if let Some(p) = project {
            *held.projects.entry(p.clone()).or_insert(0) += count;
        }
        Ok(UnitsHeld {
            units: Arc::clone(self),
            project: project.cloned(),
            count,
        })
    }
}

impl Drop for UnitsHeld {
    fn drop(&mut self) {
        let Ok(mut held) = self.units.held.lock() else {
            return;
        };
        held.total = held.total.saturating_sub(self.count);
        if let Some(p) = &self.project
            && let Some(mine) = held.projects.get_mut(p)
        {
            *mine = mine.saturating_sub(self.count);
            if *mine == 0 {
                held.projects.remove(p);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LiveUnits, UnitRefusal};
    use crate::tenant::ProjectId;

    fn project(name: &str) -> ProjectId {
        ProjectId::new(name).unwrap()
    }

    #[test]
    fn the_budget_is_a_sixteenth_of_the_shed_line_in_four_kib_units() {
        assert_eq!(LiveUnits::new(500).snapshot(), (8_000, 0));
        assert_eq!(LiveUnits::new(0).snapshot(), (0, 0));
    }

    #[test]
    fn a_project_takes_at_most_its_share_and_the_cell_at_most_its_budget() {
        // 64 MiB shed line: 1,024 units; k = 4: 256 a project.
        let units = LiveUnits::new(64);
        let (a, b, c, d, e) = (
            project("pa"),
            project("pb"),
            project("pc"),
            project("pd"),
            project("pe"),
        );
        let held_a = units.take(Some(&a), 256, 4).unwrap();
        assert_eq!(units.take(Some(&a), 1, 4).err(), Some(UnitRefusal::Project));
        let _b = units.take(Some(&b), 256, 4).unwrap();
        let _c = units.take(Some(&c), 256, 4).unwrap();
        let _d = units.take(Some(&d), 200, 4).unwrap();
        assert_eq!(units.snapshot(), (1_024, 968));
        // e's share has room; the cell has 56 left.
        assert_eq!(
            units.take(Some(&e), 57, 4).err(),
            Some(UnitRefusal::Instance)
        );
        assert_eq!(units.snapshot(), (1_024, 968));
        drop(held_a);
        assert_eq!(units.snapshot(), (1_024, 712));
        let _a = units.take(Some(&a), 256, 4).unwrap();
        let _e = units.take(Some(&e), 56, 4).unwrap();
        assert_eq!(units.snapshot(), (1_024, 1_024));
    }

    #[test]
    fn a_dedicated_cell_gives_its_project_the_whole_budget_and_off_bounds_nothing() {
        let units = LiveUnits::new(64);
        let a = project("pa");
        let held = units.take(Some(&a), 1_024, 1).unwrap();
        assert_eq!(units.take(None, 1, 1).err(), Some(UnitRefusal::Instance));
        drop(held);
        assert_eq!(units.snapshot(), (1_024, 0));
        let off = LiveUnits::new(0);
        let _all = off.take(Some(&a), 1 << 40, 8).unwrap();
        assert_eq!(off.snapshot(), (0, 1 << 40));
    }
}
