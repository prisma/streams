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

/// The tail gets the open runs at once (S4-REVIEW #11): a busy shard's thousands of `'W'` members
/// at a cold store's 25-40 ms each would take minutes one by one.
const TAIL_GETS: usize = 64;

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
    let Some(home) = crate::keyspace::home_of(crate::sharddir::prefix_of_db_path(path)) else {
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
    let members = unsealed_members(db).await?;
    let bytes: Vec<u64> = futures_util::stream::iter(members)
        .map(|hash| async move {
            let raw = db.get(&super::tail_key(&hash)).await?;
            Ok::<_, slatedb::Error>(match raw.as_deref().map(stored_tail) {
                Some(Ok(tail)) => tail.unabsorbed_bytes,
                Some(Err(e)) => {
                    tracing::error!("maintenance rebuild skipped a tail it cannot decode: {e}");
                    0
                }
                None => 0,
            })
        })
        .buffer_unordered(TAIL_GETS)
        .try_collect()
        .await?;
    let total = bytes.iter().try_fold(0u64, |sum, b| sum.checked_add(*b));
    let total = total.ok_or_else(|| anyhow::anyhow!("maintenance rebuild overflow"))?;
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
