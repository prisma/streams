//! The rollup's stored index (stored-cap B3; design `stored-cap.md` §4):
//! each project's stored bytes now, and the bytes it wrote as the drain
//! saw them, kept in the same `WriteBatch` as the ledger page that moved
//! them and published into admission's table (`quota::stored`).
//!
//! The segment rows are keyed by account, so a workspace change, which
//! re-stamps a segment's account, leaves the segment there twice. The
//! index is keyed by project, stream and segment alone, and counts each
//! segment once, by its latest snapshot:
//!
//! ```text
//! stored-seg/{project}/{stream_id}/{segment}  {"usage_version","through_ms","gauge"}
//! stored/{project}                            {"bytes","written"}
//! ```
//!
//! Project ids are unique per cell (the policy feed is keyed by them and
//! `cell-admin` never reuses one). A database that predates the index is
//! never backfilled: its rows are reported at install, and its fix is a
//! new rollup namespace, as `ROLLUP_PATH`'s own history did.

use super::{UsageRollup, decode_json, get_json};
use crate::billing::SegmentSnapshot;
use crate::quota::stored::{StoredFigure, StoredTable};
use crate::tenant::ProjectId;
use serde::{Deserialize, Serialize};
use slatedb::{Db, WriteBatch};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

fn k_stored_seg(project: &str, stream_id: &str, segment: u32) -> Vec<u8> {
    format!("stored-seg/{project}/{stream_id}/{segment}").into_bytes()
}

fn k_stored(project: &str) -> Vec<u8> {
    format!("stored/{project}").into_bytes()
}

/// The snapshot of one segment the index counts.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct SegmentRow {
    #[serde(default)]
    usage_version: u64,
    #[serde(default)]
    through_ms: i64,
    #[serde(default)]
    gauge: u64,
}

impl SegmentRow {
    /// The segment state's own rule (`advance_segment_state`): a greater
    /// version, or the same version accounted through a later instant.
    fn supersedes(&self, held: &Self) -> bool {
        (self.usage_version, self.through_ms) > (held.usage_version, held.through_ms)
    }
}

/// One project's figures: the bytes its live segments store now, and the
/// bytes it wrote: each increase of a segment's gauge among the snapshots
/// the rollup applied, in the segment state's order. A drain round emits a
/// segment as it then stands, so bytes stored and freed between two rounds
/// of its shard never reach `written`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRow {
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    written: u64,
}

/// The segments one page advanced, each with every snapshot the page
/// advanced it to, keyed by its `stored-seg/` row. A rollup behind its
/// ledger reads several drain rounds of a segment in one page, and each
/// increase among them is written.
#[derive(Default)]
pub(super) struct Pending {
    segments: HashMap<Vec<u8>, (ProjectId, Vec<SegmentRow>)>,
}

impl Pending {
    /// `advance_segment_state` moved `snap`'s segment to the gauge it
    /// carries, accounted through `through`. A snapshot whose project is
    /// no project id is reported and not indexed.
    pub(super) fn advanced(&mut self, snap: &SegmentSnapshot, through: i64) {
        let id = &snap.identity;
        let project = match ProjectId::new(&id.project_id) {
            Ok(project) => project,
            Err(error) => {
                tracing::error!(
                    %error,
                    project = %id.project_id,
                    stream_id = %id.stream_id,
                    segment = snap.segment_id,
                    "a segment snapshot whose project is no project id is not in the stored index"
                );
                return;
            }
        };
        let row = SegmentRow {
            usage_version: snap.usage_version,
            through_ms: through,
            gauge: snap.owned_frame_bytes_current,
        };
        let key = k_stored_seg(project.as_str(), &id.stream_id, snap.segment_id);
        let (_, rows) = self.segments.entry(key).or_insert((project, Vec::new()));
        rows.push(row);
    }

    /// Stages the page's index rows into `writes`: each segment row the
    /// page's snapshots supersede, at the latest, and each project row that
    /// moved, by each step of the segment's gauge from the one the index
    /// held through the page's snapshots in the segment state's order.
    /// Answers the projects whose figures moved. A read that fails, and a
    /// project row below what a segment gives back, fail the page.
    pub(super) async fn stage(
        self,
        db: &Db,
        writes: &mut WriteBatch,
    ) -> anyhow::Result<Vec<StoredFigure>> {
        let mut projects: HashMap<ProjectId, (StoredRow, StoredRow)> = HashMap::new();
        for (key, (project, rows)) in self.segments {
            let held: SegmentRow = get_json(db, &key).await?;
            let steps = advancing(held, rows);
            let Some(latest) = steps.last().copied() else {
                continue;
            };
            let figures = match projects.entry(project) {
                Entry::Occupied(slot) => &mut slot.into_mut().1,
                Entry::Vacant(slot) => {
                    let read: StoredRow = get_json(db, &k_stored(slot.key().as_str())).await?;
                    &mut slot.insert((read, read)).1
                }
            };
            let mut gauge = held.gauge;
            for step in steps {
                moved(figures, gauge, step.gauge, &key)?;
                gauge = step.gauge;
            }
            writes.put(&key, serde_json::to_vec(&latest)?);
        }
        let mut changed = Vec::new();
        for (project, (read, figures)) in projects {
            if figures == read {
                continue;
            }
            writes.put(k_stored(project.as_str()), serde_json::to_vec(&figures)?);
            changed.push(StoredFigure {
                project,
                bytes: figures.bytes,
                written: figures.written,
            });
        }
        Ok(changed)
    }
}

/// The snapshots of `rows` that advance a segment the index held at
/// `held`, in the segment state's order, each superseding the one before.
fn advancing(held: SegmentRow, mut rows: Vec<SegmentRow>) -> Vec<SegmentRow> {
    rows.sort_by_key(|row| (row.usage_version, row.through_ms));
    let mut last = held;
    rows.retain(|row| {
        let advances = row.supersedes(&last);
        if advances {
            last = *row;
        }
        advances
    });
    rows
}

/// Moves a project's figures by one segment's gauge going from `held` to
/// `now`: an increase is stored and written, a decrease given back.
fn moved(figures: &mut StoredRow, held: u64, now: u64, key: &[u8]) -> anyhow::Result<()> {
    if let Some(grown) = now.checked_sub(held) {
        figures.bytes = figures.bytes.saturating_add(grown);
        figures.written = figures.written.saturating_add(grown);
        return Ok(());
    }
    let freed = held - now;
    figures.bytes = figures.bytes.checked_sub(freed).ok_or_else(|| {
        anyhow::anyhow!(
            "{} gives back {freed} stored bytes; its project holds {}",
            String::from_utf8_lossy(key),
            figures.bytes
        )
    })?;
    Ok(())
}

impl UsageRollup {
    /// Publishes every project's durable figures into admission's table, at
    /// install, before the instance admits by them. A database with
    /// segment rows and no index rows predates the index: it is reported
    /// and publishes nothing, and it is never backfilled. The index exists
    /// once it holds a `stored-seg/` row, which a gauge-0 snapshot writes
    /// without a `stored/` one.
    pub(crate) async fn publish_stored_into(&self, table: &StoredTable) -> anyhow::Result<()> {
        let mut figures = Vec::new();
        let mut rows = self
            .db
            .scan_prefix_with_options(&b"stored/"[..], .., &crate::rollup::telemetry_scan())
            .await?;
        while let Some(kv) = rows.next().await? {
            let key = String::from_utf8_lossy(&kv.key);
            let project = key
                .strip_prefix("stored/")
                .and_then(|project| ProjectId::new(project).ok())
                .ok_or_else(|| anyhow::anyhow!("{key} names no project id"))?;
            let row: StoredRow = decode_json(&kv.value)?;
            figures.push(StoredFigure {
                project,
                bytes: row.bytes,
                written: row.written,
            });
        }
        if figures.is_empty()
            && !self.has_rows(b"stored-seg/").await?
            && self.has_rows(b"segment/").await?
        {
            tracing::error!(
                error = "segment rows and no stored index: this database predates the index",
                "the stored index publishes nothing; a new rollup namespace starts one"
            );
        }
        table.publish(&figures, crate::shard::now_ms());
        Ok(())
    }

    async fn has_rows(&self, prefix: &[u8]) -> anyhow::Result<bool> {
        let mut rows = self
            .db
            .scan_prefix_with_options(prefix, .., &crate::rollup::telemetry_scan())
            .await?;
        Ok(rows.next().await?.is_some())
    }
}

#[cfg(test)]
mod tests;
