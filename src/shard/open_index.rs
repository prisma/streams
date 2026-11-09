//! A shard log's open (S3a-DESIGN §2.6): the log's identity row `FF×16 ‖ 'Y'`, checked before the
//! engine exists, and the maintenance state the engine starts from.
//!
//! `'Y'` is `LAYOUT_VERSION u32 BE ‖ home u32 BE`, written once, when the log is created. Every
//! later open compares it with this binary's layout and the home the log's path names
//! (`keyspace::home_of`), and refuses a log of another layout or home: a layout 7 log has no `'Y'`
//! but has rows, so it is refused before anything reads or merges its rows. The Db open has fenced
//! the prior owner and the committer does not exist yet, so nothing races the first put.
//!
//! The open reads two indexes instead of every tail (S3a §4.1), each written by the committer in
//! the batch of the change it records:
//! - `'W' ‖ h` exists exactly when `h`'s durable tail holds unsealed pages (`absorbed < next`):
//!   the maintenance ledger is the sum of its members' unabsorbed bytes, and the sealer's open
//!   holds its members;
//! - `'M' ‖ h` (the staged usage version) is put on every billed group and deleted when the ledger
//!   acknowledges through the durable version: the sealer's open marks its members dirty, so the
//!   drain re-emits only them.
//!
//! The only full scan is the repair (`repair_indexes`), run by the next open, before the committer
//! exists, when the log carries the repair marker `'X'` (an operator's request, edge #245, or a
//! stream load that found an index fault) or when the open finds a `'W'` member whose tail is gone
//! or sealed. Run before the committer, it cannot race one: no index row can be written between
//! its read and its write.
use futures_util::{StreamExt, TryStreamExt};
use slatedb::Db;
use slatedb::config::WriteOptions;

use super::{ShardMaintenance, now_ms, stored_tail};

/// The identity row's key.
fn identity_key() -> Vec<u8> {
    let mut key = crate::keyspace::SENTINEL.to_vec();
    key.push(crate::keyspace::tag::IDENTITY);
    key
}

/// The unsealed index's row of `hash`.
pub(crate) fn unsealed_key(hash: &[u8; 16]) -> Vec<u8> {
    index_key(crate::keyspace::tag::UNSEALED, hash)
}

/// The usage-dirty index's row of `hash`.
pub(crate) fn dirty_key(hash: &[u8; 16]) -> Vec<u8> {
    index_key(crate::keyspace::tag::USAGE_DIRTY, hash)
}

fn index_key(tag: u8, hash: &[u8]) -> Vec<u8> {
    let mut key = crate::keyspace::SENTINEL.to_vec();
    key.push(tag);
    key.extend_from_slice(hash);
    key
}

/// What a group stages for a stream's index row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexOp {
    Put,
    Delete,
}

/// The `'W'` op a group's tail change needs, as a function of the group's base tail and its
/// staged tail (`(absorbed, next)` each), never of in-memory state: a put when the stream gains
/// unsealed pages, a delete when it loses its last.
pub(crate) fn unsealed_op(base: (u64, u64), staged: (u64, u64)) -> Option<IndexOp> {
    match (base.0 < base.1, staged.0 < staged.1) {
        (false, true) => Some(IndexOp::Put),
        (true, false) => Some(IndexOp::Delete),
        _ => None,
    }
}

/// Whether the ledger's acknowledgement through `through` clears the usage-dirty row of a stream
/// whose durable usage version is `durable`: only when it covers that version.
pub(crate) fn dirty_cleared(through: u64, durable: Option<u64>) -> bool {
    durable.is_none_or(|version| version <= through)
}

/// The members of the index `tag`, with their values.
async fn index_rows(db: &Db, tag: u8) -> Result<Vec<([u8; 16], Vec<u8>)>, slatedb::Error> {
    let prefix = index_key(tag, &[]);
    let mut rows = db.scan_prefix(&prefix, ..).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        if let Some(hash) = row.key.get(prefix.len()..).and_then(|h| h.try_into().ok()) {
            out.push((hash, row.value.to_vec()));
        }
    }
    Ok(out)
}

/// The streams the unsealed index names.
pub(crate) async fn unsealed_members(db: &Db) -> Result<Vec<[u8; 16]>, slatedb::Error> {
    let rows = index_rows(db, crate::keyspace::tag::UNSEALED).await?;
    Ok(rows.into_iter().map(|(hash, _)| hash).collect())
}

/// The streams the usage-dirty index names, with the version each was marked at.
pub(crate) async fn dirty_members(db: &Db) -> Result<Vec<([u8; 16], u64)>, slatedb::Error> {
    let rows = index_rows(db, crate::keyspace::tag::USAGE_DIRTY).await?;
    Ok(rows
        .into_iter()
        .filter_map(|(hash, v)| Some((hash, u64::from_be_bytes(v.try_into().ok()?))))
        .collect())
}

/// The repair marker's key.
pub(crate) fn repair_key() -> Vec<u8> {
    index_key(crate::keyspace::tag::REPAIR, &[])
}

/// Marks the log for an index repair at its next open.
pub(crate) async fn mark_for_repair(db: &Db) -> Result<(), slatedb::Error> {
    db.put(&repair_key(), b"").await.map(|_| ())
}

/// What a repair changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Repair {
    pub(crate) unsealed_added: u64,
    pub(crate) unsealed_removed: u64,
    pub(crate) dirty_set: u64,
    pub(crate) dirty_removed: u64,
}

/// One index write of a repair: the key, and its value (`None`: a delete).
type IndexWrite = (Vec<u8>, Option<Vec<u8>>);

/// The index writes a repair stages in one batch.
const REPAIR_BATCH: usize = 1024;

/// Recomputes both indexes from every stream tail (S3a §4.1) and writes the difference, in batches
/// of `REPAIR_BATCH`, deleting the repair marker in the last: `'W'` names exactly the tails with
/// unsealed pages, `'M'` every billed tail at its usage version (an acknowledged one is re-emitted
/// once, which the rollup deduplicates), and no row names a stream without a tail. A tail that does
/// not decode is left out (its stream fails alone when loaded).
pub(crate) async fn repair_indexes(db: &Db) -> Result<Repair, slatedb::Error> {
    let mut tails = std::collections::BTreeMap::new();
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let Some(hash) = crate::keyspace::tail_hash(&row.key) else {
            continue;
        };
        if let Ok(tail) = stored_tail(&row.value) {
            let version = tail.usage.as_ref().map(|u| u.usage_version);
            tails.insert(hash, (tail.absorbed < tail.next, version));
        }
    }
    let unsealed: std::collections::BTreeSet<_> = unsealed_members(db).await?.into_iter().collect();
    let dirty: std::collections::BTreeMap<_, _> = dirty_members(db).await?.into_iter().collect();
    let mut repair = Repair::default();
    let mut ops: Vec<IndexWrite> = Vec::new();
    for (hash, (held, version)) in &tails {
        if *held && !unsealed.contains(hash) {
            ops.push((unsealed_key(hash), Some(Vec::new())));
            repair.unsealed_added += 1;
        }
        if let Some(v) = version.filter(|v| dirty.get(hash) != Some(v)) {
            ops.push((dirty_key(hash), Some(v.to_be_bytes().to_vec())));
            repair.dirty_set += 1;
        }
    }
    for hash in &unsealed {
        if !tails.get(hash).is_some_and(|(held, _)| *held) {
            ops.push((unsealed_key(hash), None));
            repair.unsealed_removed += 1;
        }
    }
    for hash in dirty.keys().filter(|h| !tails.contains_key(*h)) {
        ops.push((dirty_key(hash), None));
        repair.dirty_removed += 1;
    }
    ops.push((repair_key(), None));
    for chunk in ops.chunks(REPAIR_BATCH) {
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in chunk {
            match value {
                Some(v) => batch.put(key, v),
                None => batch.delete(key),
            }
        }
        db.write(batch).await?;
    }
    Ok(repair)
}

/// The unabsorbed bytes over the `'W'` members' tails, and whether a member's tail is gone or
/// sealed (a stale index row, which only a repair fixes).
async fn unsealed_bytes(db: &Db) -> anyhow::Result<(u64, bool)> {
    let members = unsealed_members(db).await?;
    let bytes: Vec<Option<u64>> = futures_util::stream::iter(members)
        .map(|hash| async move {
            let raw = db.get(&super::tail_key(&hash)).await?;
            Ok::<_, slatedb::Error>(match raw.as_deref().map(stored_tail) {
                Some(Ok(tail)) if tail.absorbed < tail.next => Some(tail.unabsorbed_bytes),
                Some(Err(e)) => {
                    tracing::error!("maintenance rebuild skipped a tail it cannot decode: {e}");
                    Some(0)
                }
                Some(Ok(_)) | None => None,
            })
        })
        .buffer_unordered(TAIL_GETS)
        .try_collect()
        .await?;
    let stale = bytes.iter().any(Option::is_none);
    let total = bytes
        .iter()
        .flatten()
        .try_fold(0u64, |sum, b| sum.checked_add(*b));
    let total = total.ok_or_else(|| anyhow::anyhow!("maintenance rebuild overflow"))?;
    Ok((total, stale))
}

impl super::ShardEngine {
    /// A stream load's check that the unsealed index names a stream it found holding unsealed pages
    /// (read from the load's own snapshot, so the tail and the row are of one moment).
    pub(super) async fn check_unsealed_index(
        &self,
        snapshot: &slatedb::DbSnapshot,
        hash: &[u8; 16],
    ) -> Result<(), slatedb::Error> {
        if snapshot.get(unsealed_key(hash)).await?.is_none() {
            self.note_index_fault(hash).await;
        }
        Ok(())
    }

    /// A stream load found unsealed pages the unsealed index does not name: counted, logged, and
    /// the log marked for a repair at its next open. The pages stay durable in the stream's pending
    /// row; the committer holds the stream at its next append, and the repair restores the row.
    async fn note_index_fault(&self, hash: &[u8; 16]) {
        self.layout
            .seal
            .counters
            .index_faults
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            shard = %self.prefix,
            stream = %crate::crypto::hex(&hash[..4]),
            "a stream holds unsealed pages its unsealed index does not name; the next open repairs the indexes"
        );
        if let Err(error) = mark_for_repair(&self.db).await {
            tracing::warn!(shard = %self.prefix, %error, "the repair marker was not written");
        }
    }
}

/// The tail gets the open runs at once (S4-REVIEW #11): a busy shard's thousands of `'W'` members
/// at a cold store's 25-40 ms each would take minutes one by one.
const TAIL_GETS: usize = 64;

/// The identity row of a log of this binary's layout at `home` (a split's child is seeded with its
/// own, S4 step 11).
#[cfg(test)]
pub(crate) fn identity_row(home: u32) -> (Vec<u8>, Vec<u8>) {
    (identity_key(), identity_value(home).to_vec())
}

/// The identity row's value for a log of this binary's layout at `home`.
fn identity_value(home: u32) -> [u8; 8] {
    let mut value = [0u8; 8];
    value[..4].copy_from_slice(&crate::registry::LAYOUT_VERSION.to_be_bytes());
    value[4..].copy_from_slice(&home.to_be_bytes());
    value
}

/// Checks the identity of the shard log at `path`, writing it on an empty log, and refuses the
/// open of a log of another layout or home (a `data` error; a store error keeps its own kind).
pub(crate) async fn check_identity(db: &Db, path: &str) -> Result<(), slatedb::Error> {
    let Some(home) = crate::sharddir::home_of_db_path(path) else {
        return Err(slatedb::Error::data(format!(
            "shard log {path} names no shard prefix, so it has no home"
        )));
    };
    let key = identity_key();
    let want = identity_value(home);
    let found = db.get(&key).await?;
    if found.as_deref() == Some(&want[..]) {
        return Ok(());
    }
    let empty = found.is_none() && db.scan(..).await?.next().await?.is_none();
    if !empty {
        return Err(slatedb::Error::data(format!(
            "shard log {path} is of another layout or home (identity {}, this binary's {}); this cell \
             must be recreated (RUNBOOK)",
            found.map_or_else(|| "absent".to_string(), |v| crate::crypto::hex(&v)),
            crate::crypto::hex(&want)
        )));
    }
    db.put_with_options(&key, &want, &Default::default(), &WriteOptions::default())
        .await?;
    Ok(())
}

/// The home a log's `'Y'` row records; `None` for a log without one, which only a log opened
/// without `check_identity` can be (a test's `keyspace::db_builder` rig).
pub(super) async fn recorded_home(db: &Db) -> Result<Option<u32>, slatedb::Error> {
    let Some(value) = db.get(&identity_key()).await? else {
        return Ok(None);
    };
    let home = value
        .get(4..8)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes);
    home.map(Some)
        .ok_or_else(|| slatedb::Error::data("shard log identity row is short".to_string()))
}

/// This shard's maintenance ledger, rebuilt before the engine starts serving (layout 6: the
/// ledger lives in memory only): the unsealed bytes the durable tails of the unsealed index's
/// members hold, which by its invariant are every stream with unsealed bytes. A tail that does not
/// decode, or is gone, is left out and logged (its stream fails alone when loaded); a store error
/// fails the open.
pub(crate) async fn load_or_rebuild_maintenance(db: &Db) -> anyhow::Result<ShardMaintenance> {
    let marked = db.get(&repair_key()).await?.is_some();
    if marked {
        let repair = repair_indexes(db).await?;
        tracing::warn!(
            ?repair,
            "open repaired the shard's indexes: the log carried the repair marker"
        );
    }
    let (mut total, stale) = unsealed_bytes(db).await?;
    if stale && !marked {
        let repair = repair_indexes(db).await?;
        tracing::warn!(
            ?repair,
            "open repaired the shard's indexes: an unsealed-index member's tail is gone or sealed"
        );
        total = unsealed_bytes(db).await?.0;
    }
    let now = now_ms();
    Ok(ShardMaintenance {
        version: 1,
        unabsorbed_frame_bytes: total,
        backlog_started_ms: if total > 0 { now } else { 0 },
        last_progress_ms: if total > 0 { now } else { 0 },
    })
}

#[cfg(kani)]
mod proofs;
#[cfg(test)]
mod tests;
