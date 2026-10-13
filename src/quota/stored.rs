//! Admission's copy of each project's stored bytes (stored-cap B3): the
//! figures the usage rollup's stored index (`rollup::stored_index`)
//! publishes at install and after every ledger page it applies. One table
//! per instance, behind `QuotaRegistry`; an instance that runs no rollup
//! holds an empty one.
//!
//! Admission reads one entry with one lock-free load; the rollup updates
//! an entry in place and copies the map only when a project appears for
//! the first time. No lock, so nothing poisons.
//!
//! Stored-cap B4: an append is refused while its project's figure is at
//! or over its `max_stored_bytes` quota (`StoredTable::admit`), and each
//! entry latches whether the cap is engaged, so admission emits one event
//! when it engages and one when it releases, never one per refusal.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use super::QuotaRefusal;
use crate::ops::{OpsEvent, OpsService};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// One project's figures as the rollup's stored index holds them: the
/// bytes its live segments store now, and the bytes it wrote (each increase
/// of its segments' gauges among the snapshots the rollup applied, which
/// deletes never lower; bytes stored and freed within one drain round are
/// not in it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredFigure {
    pub(crate) project: ProjectId,
    pub(crate) bytes: u64,
    pub(crate) written: u64,
}

/// The published figures. Clones share one table.
#[derive(Clone, Default)]
pub(crate) struct StoredTable(Arc<Inner>);

#[derive(Default)]
struct Inner {
    entries: arc_swap::ArcSwap<HashMap<ProjectId, Arc<StoredEntry>>>,
    /// When the rollup last published (wall-clock ms); 0: never.
    published_ms: AtomicI64,
}

/// A project's figures. A reader may see one figure of a publication and
/// the other of the one before it; each is a figure the index held.
/// `engaged`: admission last refused the project at its cap (a
/// publication leaves it as it was).
#[derive(Default)]
struct StoredEntry {
    bytes: AtomicU64,
    written: AtomicU64,
    engaged: AtomicBool,
}

impl StoredEntry {
    fn set(&self, figure: &StoredFigure) {
        self.bytes.store(figure.bytes, Ordering::Relaxed);
        self.written.store(figure.written, Ordering::Relaxed);
    }
}

impl StoredTable {
    /// Publishes `figures` (each a project's whole figures, replacing what
    /// the table held for it) as the index's state at `now_ms`.
    pub(crate) fn publish(&self, figures: &[StoredFigure], now_ms: i64) {
        let entries = self.0.entries.load();
        let mut new = Vec::new();
        for figure in figures {
            match entries.get(&figure.project) {
                Some(entry) => entry.set(figure),
                None => new.push(figure),
            }
        }
        if !new.is_empty() {
            self.0.entries.rcu(|entries| with_projects(entries, &new));
        }
        self.0.published_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Every project's figures, in no particular order.
    pub(crate) fn figures(&self) -> Vec<StoredFigure> {
        self.0
            .entries
            .load()
            .iter()
            .map(|(project, entry)| StoredFigure {
                project: project.clone(),
                bytes: entry.bytes.load(Ordering::Relaxed),
                written: entry.written.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// When the rollup last published into the table; 0 before it has.
    pub(crate) fn published_ms(&self) -> i64 {
        self.0.published_ms.load(Ordering::Relaxed)
    }

    /// The bytes `project` stores now; 0 before the rollup published a
    /// figure for it.
    pub(crate) fn bytes(&self, project: &ProjectId) -> u64 {
        let entries = self.0.entries.load();
        let entry = entries.get(project);
        entry.map_or(0, |entry| entry.bytes.load(Ordering::Relaxed))
    }

    /// Refuses an append of `project` while the bytes it stores are at or
    /// over its `max_stored_bytes` quota (0: no limit). A project with no
    /// published figure is admitted: the rollup has seen none of its bytes.
    /// Emits `project_stored_cap_reached` (warn) when the cap engages and
    /// `project_stored_cap_cleared` when an admitted append finds it
    /// released, by a lower figure or a larger quota.
    pub(crate) fn admit(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        ops: &OpsService,
    ) -> Result<(), QuotaRefusal> {
        let entries = self.0.entries.load();
        let Some(entry) = entries.get(project) else {
            return Ok(());
        };
        let limit = quotas.max_stored_bytes;
        let stored = entry.bytes.load(Ordering::Relaxed);
        if limit > 0 && stored >= limit {
            if !entry.engaged.swap(true, Ordering::Relaxed) {
                ops.emit(cap_event(CAP_REACHED, project, stored, limit).warn());
            }
            return Err(QuotaRefusal::StoredBytes { limit, stored });
        }
        if entry.engaged.load(Ordering::Relaxed) && entry.engaged.swap(false, Ordering::Relaxed) {
            ops.emit(cap_event(CAP_CLEARED, project, stored, limit));
        }
        Ok(())
    }
}

/// The ops event of a project's cap engaging (warn).
const CAP_REACHED: &str = "project_stored_cap_reached";
/// The ops event of a project's cap releasing (info).
const CAP_CLEARED: &str = "project_stored_cap_cleared";

/// The ops event of `project`'s cap engaging or releasing at `stored`
/// bytes under a quota of `limit`.
fn cap_event(kind: &str, project: &ProjectId, stored: u64, limit: u64) -> OpsEvent {
    let id = format!("{kind}/{}/{stored}", project.as_str());
    OpsEvent::new(kind, id).fields(serde_json::json!({
        "project": project.as_str(),
        "stored_bytes": stored,
        "max_stored_bytes": limit,
    }))
}

/// A copy of `entries` holding `figures`, whose projects it lacked; the
/// entries it had are shared with the original.
fn with_projects(
    entries: &HashMap<ProjectId, Arc<StoredEntry>>,
    figures: &[&StoredFigure],
) -> HashMap<ProjectId, Arc<StoredEntry>> {
    let mut entries = entries.clone();
    for figure in figures {
        entries
            .entry(figure.project.clone())
            .or_default()
            .set(figure);
    }
    entries
}

#[cfg(test)]
mod tests;
