//! A project's read memory (shared cells H3): the page bytes its reads
//! reserve when they are admitted and its response bodies hold until each
//! body ends. One exact counter on the project's one entry
//! (`ProjectAdmission::read_held`), which the memory-pressure estimate
//! counts unweighted.
//!
//! A project with a memory line (`PROJECT_MEMORY_PRESSURE_BYTES`) may not
//! reserve past it: a reservation that would take the project's read
//! bytes over the line does not fit while the project already holds read
//! bytes, so one tenant's reads (64 concurrent 8 MiB pages, or bodies its
//! clients never drain) are bounded by its own line and wait, then are
//! refused, on its own requests (`admission::read_memory`). A read alone
//! always fits, so a page larger than the line is served one at a time. Writes keep the latch
//! (`ProjectAdmission::memory_gate`), which now sees these bytes too; a
//! project engaged by its write backlog still reads.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{ProjectAdmission, QuotaRegistry};
use crate::tenant::ProjectId;

/// A handle on one project's read bytes.
pub(crate) struct ProjectReadBytes {
    admission: Arc<ProjectAdmission>,
}

impl QuotaRegistry {
    /// `project`'s read bytes; `None` for a project the tracker does not
    /// hold (admission tracks every verified project first).
    pub(crate) fn read_bytes(&self, project: &ProjectId) -> Option<ProjectReadBytes> {
        self.tracked(project)
            .map(|admission| ProjectReadBytes { admission })
    }
}

impl ProjectReadBytes {
    /// Reserve `bytes`: `false` when the project already holds read bytes
    /// and these would take it over `line`. A `line` of 0 is no line.
    pub(crate) fn try_reserve(&self, bytes: u64, line: u64) -> bool {
        let held = &self.admission.read_held;
        let mut now = held.load(Ordering::Relaxed);
        loop {
            if line > 0 && now > 0 && now.saturating_add(bytes) > line {
                return false;
            }
            match held.compare_exchange_weak(
                now,
                now.saturating_add(bytes),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(moved) => now = moved,
            }
        }
    }

    /// A read was refused at the project's line: one memory shed.
    pub(crate) fn refused(&self) {
        self.admission
            .memory_shed_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Charge `bytes` that are already in memory (a rendered page larger
    /// than its reservation, or a page no admission reserved).
    pub(crate) fn charge(&self, bytes: u64) {
        self.admission.read_held.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Release `bytes` this handle's holder reserved or charged.
    pub(crate) fn release(&self, bytes: u64) {
        let held = &self.admission.read_held;
        let mut now = held.load(Ordering::Relaxed);
        while let Err(moved) = held.compare_exchange_weak(
            now,
            now.saturating_sub(bytes),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            now = moved;
        }
    }

    /// The project's read bytes now.
    #[cfg(test)]
    pub(crate) fn held(&self) -> u64 {
        self.admission.read_held.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;
