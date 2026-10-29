//! The orphan sweep (DESIGN-pkg6 §5): objects no row names, left by a
//! seal, fold, merge or consolidation whose batch never landed (a crash
//! between its PUT and its commit, a fenced writer, a refused seal).
//!
//! Per prefix (`hseg/`, `ridx/`) one LIST continues from the `'W'` cursor,
//! at most `sweep_pages` pages. An object is an orphan iff no `'R'`, `'X'`
//! or `'G'` row names it, it is older than `orphan_age`, and either its
//! writer epoch is below this opening's (that writer is fenced, and every
//! commit it made was replayed when this opening began) or it is this
//! opening's and its sequence is not in flight. The in-flight set is read
//! after the LIST: an operation that commits between the two is seen by its
//! row, one that starts later PUTs nothing the LIST returned. Orphans are
//! DELETEd at once (no reader ever learned of them). The cursor moves past
//! every object decided, and stops before the first undecided one (too
//! young, or in flight). Once a sweep reaches the end of a prefix with
//! everything decided, and `wrap_every` passed since the last wrap, the
//! cursor starts over: a fenced writer's late PUT may sort below it.

use std::sync::atomic::Ordering;

use futures_util::StreamExt;
use object_store::ObjectStoreExt;
use slatedb::WriteBatch;

use super::{GcError, duration_ms};
use crate::shard::ShardEngine;
use crate::spill::gc::{self, ObjectKind};
use crate::spill::object::ObjectId;

/// What one sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SweepReport {
    pub(crate) listed: u64,
    pub(crate) orphans: u64,
    pub(crate) orphan_bytes: u64,
}

/// LIST results one page holds (the store's page size).
const PAGE: usize = 1000;

/// An object's id from its path's last two segments.
fn id_of(location: &object_store::path::Path) -> Option<ObjectId> {
    let mut parts = location.parts().collect::<Vec<_>>().into_iter().rev();
    let seq = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
    let epoch = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
    Some(ObjectId { epoch, seq })
}

impl ShardEngine {
    /// One sweep of both prefixes.
    pub(crate) async fn sweep_orphans(&self) -> Result<SweepReport, GcError> {
        let mut report = SweepReport::default();
        let gc = self.gc_state();
        let wrap = {
            let mut last = super::lock(&gc.last_wrap);
            let due = last.is_none_or(|t| t.elapsed() >= gc.settings().wrap_every);
            if due || last.is_none() {
                *last = Some(std::time::Instant::now());
            }
            due
        };
        for kind in ObjectKind::ALL {
            let r = self.sweep_kind(kind, wrap).await?;
            report.listed += r.listed;
            report.orphans += r.orphans;
            report.orphan_bytes += r.orphan_bytes;
        }
        let c = &self.gc_state().counters;
        c.sweeps.fetch_add(1, Ordering::Relaxed);
        c.listed.fetch_add(report.listed, Ordering::Relaxed);
        c.orphans.fetch_add(report.orphans, Ordering::Relaxed);
        c.orphan_bytes
            .fetch_add(report.orphan_bytes, Ordering::Relaxed);
        Ok(report)
    }

    async fn sweep_kind(&self, kind: ObjectKind, wrap: bool) -> Result<SweepReport, GcError> {
        let store = |e: object_store::Error| GcError::Store(e.to_string());
        let s = self.gc_state().settings();
        let shard = crate::sharddir::shard_db_path(&self.prefix);
        let prefix = object_store::path::Path::from(format!("{shard}/{}", kind.dir()));
        let cursor = self.sweep_cursor(kind).await?;
        let mut listing = match &cursor {
            Some(after) => self.data_store.list_with_offset(
                Some(&prefix),
                &object_store::path::Path::from(after.as_str()),
            ),
            None => self.data_store.list(Some(&prefix)),
        };
        let mut listed = Vec::new();
        let mut ended = false;
        while listed.len() < s.sweep_pages.max(1) * PAGE {
            let Some(meta) = listing.next().await else {
                ended = true;
                break;
            };
            listed.push(meta.map_err(store)?);
        }
        drop(listing);
        let in_flight = self.gc_state().in_flight();
        let young = chrono::Utc::now()
            - chrono::Duration::milliseconds(
                i64::try_from(duration_ms(s.orphan_age)).unwrap_or(i64::MAX),
            );
        let mut report = SweepReport {
            listed: listed.len() as u64,
            ..SweepReport::default()
        };
        let mut advance = None;
        let mut decided = 0;
        for meta in &listed {
            let Some(id) = id_of(&meta.location) else {
                advance = Some(meta.location.to_string());
                continue;
            };
            let undecided = meta.last_modified > young
                || (id.epoch == self.writer_epoch && in_flight.contains(&id.seq))
                || id.epoch > self.writer_epoch;
            if undecided {
                break;
            }
            if !self.named_by_row(kind, id).await? {
                self.delete_orphan(&meta.location).await?;
                report.orphans += 1;
                report.orphan_bytes += meta.size;
            }
            advance = Some(meta.location.to_string());
            decided += 1;
        }
        let mut batch = WriteBatch::new();
        let moved = if ended && wrap && decided == listed.len() && cursor.is_some() {
            batch.delete(gc::sweep_key(kind));
            true
        } else if let Some(after) = advance {
            batch.put(gc::sweep_key(kind), after.into_bytes());
            true
        } else {
            false
        };
        if moved {
            self.write_index_batch(batch)
                .await
                .map_err(|e| GcError::Store(e.to_string()))?;
        }
        Ok(report)
    }

    /// DELETE an orphan (a 404 is one already gone).
    async fn delete_orphan(&self, at: &object_store::path::Path) -> Result<(), GcError> {
        match self.data_store.delete(at).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(GcError::Store(e.to_string())),
        }
    }

    /// The sweep's cursor for `kind`: the last object it decided.
    async fn sweep_cursor(&self, kind: ObjectKind) -> Result<Option<String>, GcError> {
        let raw = self
            .db
            .get(gc::sweep_key(kind))
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        Ok(raw.and_then(|b| String::from_utf8(b.to_vec()).ok()))
    }

    /// Whether a row references `id` or awaits its delete.
    async fn named_by_row(&self, kind: ObjectKind, id: ObjectId) -> Result<bool, GcError> {
        let live = match kind {
            ObjectKind::Segment => gc::refcount_key(id),
            ObjectKind::RunIndex => crate::spill::runindex::pointer_key(id),
        };
        for key in [live, gc::garbage_key(id)] {
            let held = self
                .db
                .get(&key)
                .await
                .map_err(|e| GcError::Store(e.to_string()))?;
            if held.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
