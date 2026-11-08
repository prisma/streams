//! Logging releases (S6-DESIGN §3.2 step 3): on the collector's tick, the due
//! `'E'` rows (the `'G'` predicate: due, and no running read may hold the
//! object) are grouped by their object's home. Per home one release log is
//! PUT under that home's root (`spill::lineage`), and then one batch deletes
//! those rows. A home that this shard then references nothing of (no `'R'`,
//! `'I'` or `'E'` row of it left) gets its released-all marker.
//!
//! The crash windows equal the `'G'` path's. A crash before the log's PUT
//! leaves the rows, and the next tick logs them, the opening's pin clock
//! counting from open. A crash after the PUT and before the delete logs the
//! same ids again, which is harmless: the sweeper reads `list − ∪ logs`. A
//! release is never logged before its zeroing is durable, because the zeroing
//! batch wrote the row.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use object_store::{PutMode, PutOptions, PutPayload};
use slatedb::WriteBatch;

use super::{GcError, duration_ms};
use crate::shard::ShardEngine;
use crate::spill::gc::{self, ObjectKind, Release};
use crate::spill::lineage::{self, RELEASE_LOG_MAX};
use crate::spill::object::ObjectId;

/// The releases due, by their object's home.
type ByHome = BTreeMap<u32, Vec<(ObjectKind, ObjectId)>>;

impl ShardEngine {
    /// Log the due releases (see the module): (logged, `'E'` rows left).
    pub(crate) async fn log_releases(&self) -> Result<(u64, u64), GcError> {
        let rows = self.release_rows().await?;
        let total = rows.len() as u64;
        let due = self.due_releases(rows);
        let mut logged = 0;
        for (home, objects) in &due {
            for part in objects.chunks(RELEASE_LOG_MAX) {
                self.log_part(*home, part).await?;
                logged += part.len() as u64;
            }
            if !self.holds_any_of(*home).await? {
                self.put_released_all(*home).await?;
            }
        }
        Ok((logged, total - logged))
    }

    /// Log `part` (all of `home`) in one release log, then delete its rows.
    async fn log_part(&self, home: u32, part: &[(ObjectKind, ObjectId)]) -> Result<(), GcError> {
        self.put_release_log(home, part).await?;
        let mut batch = WriteBatch::new();
        for (kind, id) in part {
            batch.delete(gc::release_key(*kind, *id));
        }
        self.write_index_batch(batch)
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        for (_, id) in part {
            self.gc_state().forget_zeroed(*id);
        }
        Ok(())
    }

    /// Every `'E'` row; one that does not decode is counted and left.
    pub(crate) async fn release_rows(
        &self,
    ) -> Result<Vec<(ObjectKind, ObjectId, Release)>, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        let prefix = crate::spill::runindex::sentinel_prefix(gc::RELEASE_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        let mut out = Vec::new();
        while let Some(kv) = rows.next().await.map_err(store)? {
            let decoded = gc::parse_release_key(&kv.key)
                .and_then(|(kind, id)| Some((kind, id, gc::decode_release(&kv.value)?)));
            match decoded {
                Some(row) => out.push(row),
                None => {
                    self.gc_state()
                        .counters
                        .corrupt_rows
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(out)
    }

    /// The rows of `rows` due now that no running read may hold, at most one
    /// reclaim batch, by home.
    fn due_releases(&self, rows: Vec<(ObjectKind, ObjectId, Release)>) -> ByHome {
        let gc = self.gc_state();
        let now = crate::shard::now_ms().unsigned_abs();
        let reopened = gc
            .opened_ms
            .load(Ordering::Relaxed)
            .saturating_add(duration_ms(gc.settings().grace));
        let most = gc.settings().reclaim_batch.max(1);
        let mut due = ByHome::new();
        let ready = rows
            .into_iter()
            .filter(|(_, id, r)| r.due_ms.max(reopened) <= now && !gc.pinned(*id))
            .take(most);
        for (kind, id, _) in ready {
            due.entry(id.home).or_default().push((kind, id));
        }
        due
    }

    /// PUT one release log of `objects` (all of `home`) under `home`'s root.
    async fn put_release_log(
        &self,
        home: u32,
        objects: &[(ObjectKind, ObjectId)],
    ) -> Result<(), GcError> {
        let entries: Vec<lineage::Entry> = objects
            .iter()
            .map(|(kind, id)| (*kind, id.epoch, id.seq))
            .collect();
        let seq = self.gc_state().release_seq.fetch_add(1, Ordering::Relaxed);
        let root = self.object_root(home);
        let path = lineage::release_log_path(&root, self.home(), self.writer_epoch, seq);
        self.put_lineage(path, lineage::encode(&entries)).await
    }

    /// PUT `home`'s released-all marker for this shard.
    async fn put_released_all(&self, home: u32) -> Result<(), GcError> {
        let path = lineage::released_all_path(&self.object_root(home), self.home());
        self.put_lineage(path, lineage::encode(&[])).await
    }

    /// Create a lineage object; one already there was this shard's own
    /// (a name is unique per opening) and stands.
    async fn put_lineage(&self, path: String, body: Vec<u8>) -> Result<(), GcError> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let payload = PutPayload::from(body);
        let path = object_store::path::Path::from(path);
        match self.data_store.put_opts(&path, payload, options).await {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(GcError::Store(e.to_string())),
        }
    }

    /// Whether any `'R'`, `'I'` or `'E'` row of `home`'s objects is left.
    async fn holds_any_of(&self, home: u32) -> Result<bool, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        let tagged = |tag| {
            let mut k = crate::spill::runindex::sentinel_prefix(tag);
            k.extend_from_slice(&home.to_be_bytes());
            k
        };
        let prefixes = [
            tagged(crate::spill::directory::REFCOUNT_TAG),
            tagged(crate::spill::runindex::POINTER_TAG),
            gc::release_prefix(home),
        ];
        for prefix in prefixes {
            let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
            if rows.next().await.map_err(store)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
