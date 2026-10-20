//! Fair shedding (public-beta week 2026-10-17, the lead's decision of
//! 2026-10-20 20:00 under the owner's delegation; edge change
//! #184): over the RSS shed line, the projects that carry more
//! than their share of the cell's load are refused before any other.
//!
//! The blame signal is a project's share of the verified requests the cell
//! was offered recently, against 1/k on a cell shared k ways
//! (`PROJECT_SHARE_K`, the divisor of every project's ceilings). Each
//! verified request is counted at project admission, admitted or refused by
//! its project's own quotas (`QuotaRegistry::admit_counted`): a refusal
//! costs the cell its connection, its parse and its token check all the
//! same, so a flood its quotas refuse is still a flood, and a blamed
//! project's refused requests keep it blamed while it floods. The window is
//! the current 5 s epoch and the one before it.
//!
//! An admitted request's verdict (`park::judge_load`): `Blamed` when its
//! project's load times k is over the cell's; `Spared` when its project's is
//! not, and the heaviest project counted in the window is; `Even` otherwise
//! (nobody over its share, or a dedicated cell, k = 1). Over the line a
//! blamed project's requests are refused (`product::read_memory`), a spared
//! append passes the RSS write gate while the instance is at most a quarter
//! over the line (`AdmissionController::admit_write_memory`), and every
//! other append is shed as before.
//!
//! Bounded: one window in each project's tracker entry (the tracker's
//! 32,768 entries, idle-evicted) and one for the cell; a count or a verdict
//! takes the project's and the cell's lock once each, in that order, and
//! never scans.

use std::sync::{Mutex, PoisonError};

/// One counting epoch (ms): a load is the current epoch's count plus the
/// previous epoch's, a window of 5 to 10 s.
pub(crate) const EPOCH_MS: i64 = 5_000;

/// How far over `limit` (the RSS shed line in MiB, or the in-flight cap in
/// active requests) an append fair shedding spares still passes its gate:
/// a quarter over it. Past it every append is shed, the instance-wide
/// backstop.
pub(crate) fn hard_line(limit: u64) -> u64 {
    limit.saturating_add(limit / 4)
}

/// A count over the current epoch and the one before it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Window {
    epoch: i64,
    current: u64,
    previous: u64,
}

impl Window {
    /// Move to `epoch`: the current count becomes the previous one when
    /// `epoch` follows it, and is forgotten when an epoch passed between.
    /// True when the window moved.
    fn roll(&mut self, epoch: i64) -> bool {
        if epoch <= self.epoch {
            return false;
        }
        self.previous = if epoch == self.epoch + 1 {
            self.current
        } else {
            0
        };
        self.current = 0;
        self.epoch = epoch;
        true
    }

    /// Count one at `epoch`; the load afterwards.
    fn count(&mut self, epoch: i64) -> u64 {
        self.roll(epoch);
        self.current = self.current.saturating_add(1);
        self.load()
    }

    fn load(&self) -> u64 {
        self.current.saturating_add(self.previous)
    }
}

/// A project's recently offered requests: held by its tracker entry.
#[derive(Debug, Default)]
pub(crate) struct ProjectLoad(Mutex<Window>);

impl ProjectLoad {
    /// Count one at `epoch`; the load afterwards. A poisoned window holds
    /// only counts, every one of them whole: it is used as it stands.
    fn count(&self, epoch: i64) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .count(epoch)
    }

    /// The load as of `epoch`.
    fn load_at(&self, epoch: i64) -> u64 {
        let mut window = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        window.roll(epoch);
        window.load()
    }
}

/// The cell's offered requests, and the heaviest project load counted
/// in the current epoch and in the one before it.
#[derive(Debug, Default)]
struct Totals {
    requests: Window,
    heaviest: u64,
    heaviest_before: u64,
}

impl Totals {
    /// Move to `epoch`, the heaviest load with the window.
    fn roll(&mut self, epoch: i64) {
        let before = self.requests.epoch;
        if self.requests.roll(epoch) {
            self.heaviest_before = if epoch == before + 1 {
                self.heaviest
            } else {
                0
            };
            self.heaviest = 0;
        }
    }
}

/// The cell's recent load: held by the project tracker.
#[derive(Debug, Default)]
pub(crate) struct InstanceLoad(Mutex<Totals>);

/// What fair shedding makes of one request over the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Its project carries more than 1/k of the cell's load: refused.
    Blamed,
    /// Its project does not, and another does: its appends are admitted up
    /// to the hard line.
    Spared,
    /// Nobody is over its share: shed as every request was before.
    Even,
}

impl InstanceLoad {
    /// Count one request `project` offered the cell at `now_ms`.
    pub(crate) fn count(&self, project: &ProjectLoad, now_ms: i64) {
        let epoch = now_ms.div_euclid(EPOCH_MS);
        let own = project.count(epoch);
        let mut totals = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        totals.roll(epoch);
        totals.requests.count(epoch);
        totals.heaviest = totals.heaviest.max(own);
    }

    /// Judge a request of `project` at `now_ms` on a cell shared `share_k`
    /// ways, counting nothing.
    pub(crate) fn judge(&self, project: &ProjectLoad, share_k: u64, now_ms: i64) -> Verdict {
        let epoch = now_ms.div_euclid(EPOCH_MS);
        let own = project.load_at(epoch);
        let mut totals = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        totals.roll(epoch);
        let all = totals.requests.load();
        let heaviest = totals.heaviest.max(totals.heaviest_before);
        drop(totals);
        judge(own, heaviest, all, share_k)
    }
}

/// `load` of `all` is more than 1/`share_k` of it (never on a dedicated
/// cell).
fn over_share(load: u64, all: u64, share_k: u64) -> bool {
    share_k > 1 && load.saturating_mul(share_k) > all
}

fn judge(own: u64, heaviest: u64, all: u64, share_k: u64) -> Verdict {
    if over_share(own, all, share_k) {
        Verdict::Blamed
    } else if over_share(heaviest, all, share_k) {
        Verdict::Spared
    } else {
        Verdict::Even
    }
}

#[cfg(test)]
mod tests;
