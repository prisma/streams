//! The lineage sweep of a retired home (S6-DESIGN §4 steps 3-4, L6): under a
//! retired home's root, every segment and run-index object that no holder
//! still holds is deleted. A holder is a current leaf with a reference list
//! under the root. What it holds is its list less the releases it logged, or
//! nothing once its released-all marker exists.
//!
//! The caller decides that the home is retired, who the holders are and when
//! (S4's topology snapshot, held for `LINEAGE_GRACE`, and no live intent
//! naming the home: §4 steps 1-2). This sweep decides only on the evidence
//! under the root, and refuses to decide on any of it it cannot read. A
//! holder's list or a log that does not decode, or a declared holder without
//! a list, stops the sweep before any DELETE.

use std::collections::BTreeSet;

use futures_util::StreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;

use super::GcError;
use crate::shard::ShardEngine;
use crate::spill::gc::ObjectKind;
use crate::spill::lineage::{self, Entry};

/// What one lineage sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LineageSweep {
    /// Segment and run-index objects listed under the root.
    pub(crate) listed: u64,
    /// Of them, held by some holder and kept.
    pub(crate) held: u64,
    pub(crate) deleted: u64,
}

fn store(e: &object_store::Error) -> GcError {
    GcError::Store(e.to_string())
}

impl ShardEngine {
    /// One sweep of the retired home whose root is `root`, whose holders are
    /// `holders` (see the module).
    pub(crate) async fn sweep_lineage(
        &self,
        root: &str,
        holders: &[u32],
    ) -> Result<LineageSweep, GcError> {
        // A topology operation holds collection off on this engine (S4 §5.1): no
        // delete until it is over.
        if self.gc_state().gate.collection_off() {
            return Ok(LineageSweep::default());
        }
        let mut held = BTreeSet::new();
        for holder in holders {
            let Some(own) = self.held_by(root, *holder).await? else {
                return Err(GcError::Index(format!(
                    "holder {holder} has no reference list under {root}"
                )));
            };
            held.extend(own);
        }
        let mut report = LineageSweep::default();
        for kind in ObjectKind::ALL {
            let unheld = self.unheld(root, kind, &held, &mut report).await?;
            for location in unheld {
                match self.data_store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => report.deleted += 1,
                    Err(e) => return Err(store(&e)),
                }
            }
        }
        Ok(report)
    }

    /// The objects of `kind` under `root` that `held` lacks, counted into
    /// `report` as listed and held.
    async fn unheld(
        &self,
        root: &str,
        kind: ObjectKind,
        held: &BTreeSet<Entry>,
        report: &mut LineageSweep,
    ) -> Result<Vec<Path>, GcError> {
        let prefix = Path::from(format!("{root}/{}/", kind.dir()));
        let mut listing = self.data_store.list(Some(&prefix));
        let mut unheld = Vec::new();
        while let Some(meta) = listing.next().await {
            let location = meta.map_err(|e| store(&e))?.location;
            let Some(entry) = entry_of(kind, &location) else {
                continue;
            };
            report.listed += 1;
            if held.contains(&entry) {
                report.held += 1;
            } else {
                unheld.push(location);
            }
        }
        Ok(unheld)
    }

    /// What `holder` still holds under `root`: its reference list less its
    /// logged releases, or nothing once its released-all marker exists;
    /// `None` when it has no reference list there.
    pub(crate) async fn held_by(
        &self,
        root: &str,
        holder: u32,
    ) -> Result<Option<BTreeSet<Entry>>, GcError> {
        let Some(list) = self
            .lineage_body(&lineage::reference_list_path(root, holder))
            .await?
        else {
            return Ok(None);
        };
        let all = lineage::released_all_path(root, holder);
        let mut held: BTreeSet<Entry> = list.into_iter().collect();
        let released = Path::from(format!("{root}/lineage/rel/{holder}/"));
        let mut logs = self.data_store.list(Some(&released));
        let mut paths = Vec::new();
        while let Some(meta) = logs.next().await {
            paths.push(meta.map_err(|e| store(&e))?.location);
        }
        if paths.iter().any(|p| p.as_ref() == all.as_str()) {
            return Ok(Some(BTreeSet::new()));
        }
        for path in paths {
            let Some(log) = self.lineage_body(path.as_ref()).await? else {
                continue;
            };
            for entry in log {
                held.remove(&entry);
            }
        }
        Ok(Some(held))
    }

    /// The entries of the lineage body at `path`; `None` when there is none,
    /// an error when it does not decode.
    async fn lineage_body(&self, path: &str) -> Result<Option<Vec<Entry>>, GcError> {
        let body = match self.data_store.get(&Path::from(path)).await {
            Ok(got) => got.bytes().await.map_err(|e| store(&e))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(store(&e)),
        };
        let entries = lineage::decode(&body)
            .ok_or_else(|| GcError::Index(format!("lineage body {path} does not decode")))?;
        Ok(Some(entries))
    }
}

impl ShardEngine {
    /// The open-time detector (S6-DESIGN §3.3, L5): why this shard must not
    /// serve, if it holds a row (`'R'`, `'I'` or `'E'`) of an ancestor home
    /// without its reference list under that home's root. A list's absence
    /// cannot be healed by a late PUT (a sweep may have decided on it), so
    /// S4's open-path fence (`topology::fence::hold_until_served`) fails the
    /// open with it.
    pub(crate) async fn lineage_refusal(&self) -> Result<Option<String>, GcError> {
        let mut homes = BTreeSet::new();
        let tags = [
            crate::spill::directory::REFCOUNT_TAG,
            crate::spill::runindex::POINTER_TAG,
            crate::spill::gc::RELEASE_TAG,
        ];
        for tag in tags {
            homes.extend(self.homes_under(tag).await?);
        }
        homes.remove(&self.home());
        for home in homes {
            let path = lineage::reference_list_path(&self.object_root(home), self.home());
            match self.data_store.head(&Path::from(path.as_str())).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {
                    return Ok(Some(format!(
                        "shard {} holds rows of home {home} but has no reference list at {path}",
                        self.prefix
                    )));
                }
                Err(e) => return Err(store(&e)),
            }
        }
        Ok(None)
    }

    /// The distinct homes the sentinel rows of `tag` name (`tag ‖ home BE ‖
    /// …`): one seek per home, not one read per row.
    async fn homes_under(&self, tag: u8) -> Result<BTreeSet<u32>, GcError> {
        let db = |e: slatedb::Error| GcError::Store(e.to_string());
        let prefix = crate::spill::runindex::sentinel_prefix(tag);
        let mut end = prefix.clone();
        end.push(0xFF);
        let mut homes = BTreeSet::new();
        let mut from = prefix.clone();
        loop {
            let mut rows = self.db.scan(from..end.clone()).await.map_err(db)?;
            let Some(kv) = rows.next().await.map_err(db)? else {
                return Ok(homes);
            };
            let Some(home) = kv
                .key
                .strip_prefix(&prefix[..])
                .and_then(|rest| rest.first_chunk::<4>())
                .map(|h| u32::from_be_bytes(*h))
            else {
                return Ok(homes);
            };
            homes.insert(home);
            let Some(next) = home.checked_add(1) else {
                return Ok(homes);
            };
            from = [&prefix[..], &next.to_be_bytes()].concat();
        }
    }
}

/// The entry an object's path under `<root>/<kind dir>/<epoch>/<seq>` names.
fn entry_of(kind: ObjectKind, location: &Path) -> Option<Entry> {
    let mut parts = location.parts().collect::<Vec<_>>().into_iter().rev();
    let seq = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
    let epoch = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
    Some((kind, epoch, seq))
}
