//! An admission's hold on its tracker entry. `admit` looks the entry up
//! under the tracker lock but charges it (rate, then inflight) only after
//! that lock drops, since a poisoned bucket must not poison the map. The
//! entry is pinned at the lookup, under the lock, and the pin is released
//! at refusal or when the admitted request's guard drops, so a sweep never
//! evicts an entry an admission or its request still holds (external
//! review §9). One counter, pinned under the lock the sweep reads it
//! under: no other ordering is involved.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::ProjectAdmission;

/// The counters' atomic word: generic so the Loom model runs these same
/// transitions on instrumented atomics.
pub(crate) trait CounterWord {
    fn load(&self, order: Ordering) -> u64;
    fn fetch_add(&self, value: u64, order: Ordering) -> u64;
    fn fetch_sub(&self, value: u64, order: Ordering) -> u64;
}

impl CounterWord for std::sync::atomic::AtomicU64 {
    fn load(&self, order: Ordering) -> u64 {
        std::sync::atomic::AtomicU64::load(self, order)
    }
    fn fetch_add(&self, value: u64, order: Ordering) -> u64 {
        std::sync::atomic::AtomicU64::fetch_add(self, value, order)
    }
    fn fetch_sub(&self, value: u64, order: Ordering) -> u64 {
        std::sync::atomic::AtomicU64::fetch_sub(self, value, order)
    }
}

/// An entry's holds (admissions in progress and admitted requests, each
/// pinned from its tracker lookup) and its admitted requests in flight.
#[derive(Default)]
pub(crate) struct AdmissionCounters<W = std::sync::atomic::AtomicU64> {
    admitting: W,
    inflight: W,
}

impl<W: CounterWord> AdmissionCounters<W> {
    /// Under the tracker lock, at the lookup: the entry is in use from
    /// here, for every sweep that locks later.
    pub(crate) fn pin(&self) {
        self.admitting.fetch_add(1, Ordering::Relaxed);
    }

    /// At refusal, or when the admitted request's guard drops.
    pub(crate) fn unpin(&self) {
        self.admitting.fetch_sub(1, Ordering::Relaxed);
    }

    /// One more request in flight; the count before it.
    pub(crate) fn charge(&self) -> u64 {
        self.inflight.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn discharge(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Under the tracker lock: an admission or an admitted request holds
    /// the entry. Every pin taken before this lock is counted until its
    /// own unpin.
    pub(crate) fn active(&self) -> bool {
        self.admitting.load(Ordering::Relaxed) > 0
    }
}

/// The pin of one admission: dropped with `admit`'s refusal, or kept by
/// the admitted request's guard.
pub(super) struct AdmissionPin(Arc<ProjectAdmission>);

impl ProjectAdmission {
    /// Under the tracker lock only.
    pub(super) fn pin(self: &Arc<Self>) -> AdmissionPin {
        self.counters.pin();
        AdmissionPin(Arc::clone(self))
    }
}

impl Deref for AdmissionPin {
    type Target = ProjectAdmission;

    fn deref(&self) -> &ProjectAdmission {
        &self.0
    }
}

impl Drop for AdmissionPin {
    fn drop(&mut self) {
        self.0.counters.unpin();
    }
}

#[cfg(test)]
mod loom_tests;
