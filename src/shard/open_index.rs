//! A shard log's open (S3a-DESIGN §2.6): the log's identity row `FF×16 ‖ 'Y'`, checked before the
//! engine exists, and the maintenance state the engine starts from.
//!
//! `'Y'` is `LAYOUT_VERSION u32 BE ‖ home u32 BE`, written once, when the log is created. Every
//! later open compares it with this binary's layout and the home the log's path names
//! (`keyspace::home_of`), and refuses a log of another layout or home: a layout 7 log has no `'Y'`
//! but has rows, so it is refused before anything reads or merges its rows. The Db open has fenced
//! the prior owner and the committer does not exist yet, so nothing races the first put.
use slatedb::Db;
use slatedb::config::WriteOptions;

use super::{ShardMaintenance, now_ms, stored_tail};

/// The identity row's key.
fn identity_key() -> Vec<u8> {
    let mut key = crate::keyspace::SENTINEL.to_vec();
    key.push(crate::keyspace::tag::IDENTITY);
    key
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

/// This shard's maintenance ledger, rebuilt from its tails before the engine starts serving
/// (layout 6: the ledger lives in memory only): the unsealed bytes every stream's durable tail
/// holds. A tail that does not decode is left out and logged (its stream fails alone when loaded);
/// a store error fails the open.
pub(crate) async fn load_or_rebuild_maintenance(db: &Db) -> anyhow::Result<ShardMaintenance> {
    let mut total = 0u64;
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        if crate::keyspace::tail_hash(&row.key).is_none() {
            continue;
        }
        let unsealed = match stored_tail(&row.value) {
            Ok(tail) => tail.unabsorbed_bytes,
            Err(e) => {
                tracing::error!("maintenance rebuild skipped a tail it cannot decode: {e}");
                continue;
            }
        };
        total = total
            .checked_add(unsealed)
            .ok_or_else(|| anyhow::anyhow!("maintenance rebuild overflow"))?;
    }
    let now = now_ms();
    Ok(ShardMaintenance {
        version: 1,
        unabsorbed_frame_bytes: total,
        backlog_started_ms: if total > 0 { now } else { 0 },
        last_progress_ms: if total > 0 { now } else { 0 },
    })
}

#[cfg(test)]
mod tests;
