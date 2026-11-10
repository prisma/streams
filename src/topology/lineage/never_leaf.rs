//! The never-leaf child-path sweep (S6-DESIGN rev 6 §4 step 5; the path contract of S4-DESIGN
//! §15, Fable's S4 rev-2 finding N1). An operation that never published (a rolled-back split or
//! merge, or a stale actor's late clone after its claimant's `RolledBack`) may leave files at its
//! children's paths, and each clone a final checkpoint pinned on its source. Nothing else deletes
//! them.
//!
//! - **Which.** A home `h` is never-leaf when `h < next_home` of the stored topology, `h` is not in
//!   its home map (never published), and the cell's live intent does not name it as a child. Its
//!   path is `shards/<prefix>-<h>` (`born_home_path`), found by one delimiter LIST of `shards/`.
//! - **The race guard.** The intent is read before the LIST and again after it, and a home either
//!   read names is kept: an operation's intent precedes its clone's files, so a clone the LIST saw
//!   is named by the second read; a home reserved after the topology read is `≥ next_home`.
//! - **The delete.** `recover::delete_child`: SlateDB's `delete_db` strips each final checkpoint the
//!   path pinned on its sources (its manifest's `external_dbs`), then deletes its files. Idempotent,
//!   so two instances sweeping at once are safe. The path's reference lists under its ancestors are
//!   the lineage sweep's (step 2): a list whose home is no leaf is deleted there.

use std::sync::Arc;

use object_store::ObjectStore;

use crate::topology::model::born_home_path;

/// The never-leaf homes that still have files at their path: `(home, path)`, in home order.
pub(crate) async fn paths(store: &dyn ObjectStore) -> Result<Vec<(u32, String)>, String> {
    let named_before = super::live_intent(store).await?.children;
    let (stored, _) = crate::topology::store::read(store)
        .await
        .map_err(|e| format!("the stored topology: {e:?}"))?;
    let shards = object_store::path::Path::from("shards");
    let listed = store
        .list_with_delimiter(Some(&shards))
        .await
        .map_err(|e| format!("LIST shards/: {e}"))?;
    let named_after = super::live_intent(store).await?.children;
    let never = |h: u32| {
        h < stored.shards.next_home
            && !stored.shards.homes.contains_key(&h)
            && !named_before.contains(&h)
            && !named_after.contains(&h)
    };
    let mut found: Vec<(u32, String)> = listed
        .common_prefixes
        .iter()
        .filter_map(|dir| born_home_of(dir.as_ref()))
        .filter(|(h, _)| never(*h))
        .collect();
    found.sort_unstable();
    Ok(found)
}

/// The home and path a `shards/` directory names, when it is an operation-born home's
/// (`shards/<binary prefix>-<decimal home>`).
fn born_home_of(dir: &str) -> Option<(u32, String)> {
    let name = dir.strip_prefix("shards/")?;
    let (prefix, home) = name.rsplit_once('-')?;
    let binary = !prefix.is_empty() && prefix.bytes().all(|b| b == b'0' || b == b'1');
    let home: u32 = home.parse().ok()?;
    let path = born_home_path(prefix, home);
    (binary && path == dir).then_some((home, path))
}

/// One sweep: every never-leaf path deleted with what it pinned on its sources. Returns how many
/// paths it swept.
pub(crate) async fn sweep(store: &Arc<dyn ObjectStore>) -> Result<usize, String> {
    let found = paths(&**store).await?;
    for (_, path) in &found {
        crate::topology::recover::delete_child(store, path).await;
    }
    Ok(found.len())
}

#[cfg(test)]
mod tests {
    use super::born_home_of;

    #[test]
    fn a_born_homes_directory_names_its_home_and_nothing_else_does() {
        assert_eq!(
            born_home_of("shards/000-7"),
            Some((7, "shards/000-7".to_string()))
        );
        assert_eq!(
            born_home_of("shards/1-12"),
            Some((12, "shards/1-12".to_string()))
        );
        for not in [
            "shards/00",
            "shards/-7",
            "shards/0a0-7",
            "shards/000-x",
            "shards/000-07",
            "shards/000~7",
            "other/000-7",
        ] {
            assert_eq!(born_home_of(not), None, "{not}");
        }
    }
}
