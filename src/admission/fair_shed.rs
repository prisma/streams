//! Fair shedding (public-beta week 2026-10-17, the lead's decision of
//! 2026-10-20 20:00 under the owner's delegation; edge change
//! #184): over the RSS shed line, the projects that carry more
//! than their share of the cell's load are refused before any other.
//!
//! The blame signal is a project's share of the verified requests the cell
//! was offered recently, against its request weight's share w/k on a cell
//! shared k ways (`PROJECT_SHARE_K`, the divisor of every project's
//! ceilings; w is 1 unless its policy sets a `request_weight`, layout 6A
//! package 1). Each
//! verified request is counted at project admission, admitted or refused by
//! its project's own quotas (`QuotaRegistry::admit_counted`): a refusal
//! costs the cell its connection, its parse and its token check all the
//! same, so a flood its quotas refuse is still a flood, and a blamed
//! project's refused requests keep it blamed while it floods. The window is
//! the current 5 s epoch and the one before it.
//!
//! An admitted request's verdict (`park::judge_load`): `Blamed` when its
//! project's load times k is over the cell's times its weight; `Spared` when
//! its project's is not, and the heaviest project counted in the window, per
//! unit of its weight, is; `Even` otherwise (nobody over its share, or a
//! dedicated cell, k = 1). Over the line a
//! blamed project's requests are refused (`product::read_memory`), a spared
//! append passes the RSS write gate while the instance is at most an eighth
//! over the line (`AdmissionController::admit_write_memory`; at the
//! in-flight cap, a quarter over it), and every other append is shed as
//! before.
//!
//! Bounded: one window in each project's tracker entry (the tracker's
//! 32,768 entries, idle-evicted) and one for the cell; a count or a verdict
//! takes the project's and the cell's lock once each, in that order, and
//! never scans.

use std::sync::{Mutex, PoisonError};

/// One counting epoch (ms): a load is the current epoch's count plus the
/// previous epoch's, a window of 5 to 10 s.
pub(crate) const EPOCH_MS: i64 = 5_000;

/// How far over the RSS shed line (MiB) an append fair shedding spares
/// still passes the RSS write gate: an eighth over it (562 MiB on the
/// default 500), well inside the platform's kill line (~750 MiB, RUNBOOK).
/// Past it every append is shed, the instance-wide backstop.
pub(crate) fn rss_hard_line(line_mb: u64) -> u64 {
    line_mb.saturating_add(line_mb / 8)
}

/// How far over the in-flight write cap (active requests) an append fair
/// shedding spares still passes its gate: a quarter over it. Past it every
/// append is refused, the instance-wide backstop.
pub(crate) fn inflight_hard_line(cap: u64) -> u64 {
    cap.saturating_add(cap / 4)
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

/// A project's load and the request weight it is judged at: its share of
/// the cell is measured against weight / k (a weight of 0 is 1).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Weighted {
    load: u64,
    weight: u64,
}

impl Weighted {
    fn new(load: u64, weight: u64) -> Self {
        Self {
            load,
            weight: weight.max(1),
        }
    }

    /// The heavier of the two per unit of weight (`self` on a tie).
    fn heavier(self, other: Self) -> Self {
        let mine = u128::from(self.load) * u128::from(other.weight.max(1));
        let theirs = u128::from(other.load) * u128::from(self.weight.max(1));
        if theirs > mine { other } else { self }
    }
}

/// The cell's offered requests, and the heaviest project load per unit of
/// weight counted in the current epoch and in the one before it.
#[derive(Debug, Default)]
struct Totals {
    requests: Window,
    heaviest: Weighted,
    heaviest_before: Weighted,
}

impl Totals {
    /// Move to `epoch`, the heaviest load with the window.
    fn roll(&mut self, epoch: i64) {
        let before = self.requests.epoch;
        if self.requests.roll(epoch) {
            self.heaviest_before = if epoch == before + 1 {
                self.heaviest
            } else {
                Weighted::default()
            };
            self.heaviest = Weighted::default();
        }
    }
}

/// The cell's recent load: held by the project tracker.
#[derive(Debug, Default)]
pub(crate) struct InstanceLoad(Mutex<Totals>);

/// What fair shedding makes of one request over the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Its project carries more than its weight's w/k of the cell's load:
    /// refused.
    Blamed,
    /// Its project does not, and another does: its appends are admitted up
    /// to the hard line.
    Spared,
    /// Nobody is over its share: shed as every request was before.
    Even,
}

impl InstanceLoad {
    /// Count one request `project`, of request weight `weight`, offered
    /// the cell at `now_ms`.
    pub(crate) fn count(&self, project: &ProjectLoad, weight: u64, now_ms: i64) {
        let epoch = now_ms.div_euclid(EPOCH_MS);
        let own = Weighted::new(project.count(epoch), weight);
        let mut totals = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        totals.roll(epoch);
        totals.requests.count(epoch);
        totals.heaviest = totals.heaviest.heavier(own);
    }

    /// Judge a request of `project`, of request weight `weight`, at
    /// `now_ms` on a cell shared `share_k` ways, counting nothing.
    pub(crate) fn judge(
        &self,
        project: &ProjectLoad,
        weight: u64,
        share_k: u64,
        now_ms: i64,
    ) -> Verdict {
        let epoch = now_ms.div_euclid(EPOCH_MS);
        let own = Weighted::new(project.load_at(epoch), weight);
        let mut totals = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        totals.roll(epoch);
        let all = totals.requests.load();
        let heaviest = totals.heaviest.heavier(totals.heaviest_before);
        drop(totals);
        judge(own, heaviest, all, share_k)
    }
}

/// `load` of `all` is more than its weight's w/`share_k` of it (never on a
/// dedicated cell).
fn over_share(load: Weighted, all: u64, share_k: u64) -> bool {
    share_k > 1
        && u128::from(load.load) * u128::from(share_k)
            > u128::from(all) * u128::from(load.weight.max(1))
}

fn judge(own: Weighted, heaviest: Weighted, all: u64, share_k: u64) -> Verdict {
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
