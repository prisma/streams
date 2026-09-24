//! The fork-debt index (TLA-019-F4): one small object per stream incarnation
//! whose tombstone may owe its fork source a reference release. A tombstone
//! keeps `parent_ref_pending` when its request died after the tombstone
//! write, or when its release found the reference absent on a live source
//! while a creator could still install it. The descriptor alone is only
//! discoverable by a cell-wide catalog scan; this index is what the
//! background reconciler pages instead.
//!
//! The marker is written BEFORE the delete's tombstone write, which is where
//! the debt is created, so no crash leaves an unindexed leaf debt; a delete
//! whose release is conclusive drops it again in the same request. A debt a
//! release cascade leaves on an intermediate generation is reached from the
//! leaf's marker, by the same ancestor walk a repeated delete runs. It also records
//! the release itself, because a debt-bearing tombstone may be replaced by a
//! recreation of its name, and the marker is then the only record left.
use super::*;

/// Outside `PROJECTS_ROOT`: the catalog scans fail closed on any key under
/// that root that is not a descriptor.
const FORK_DEBT_ROOT: &str = "registry/v4/fork-debt/";
/// A marker holds two names and two epochs; anything larger was not minted
/// here.
const MAX_MARKER_BYTES: u64 = 16 * 1024;

/// The persisted body: the release the incarnation may owe.
#[derive(Serialize, Deserialize)]
struct OwedRelease {
    source: String,
    source_epoch: String,
    fork_id: String,
}

/// One indexed debt: the incarnation that may owe it and the release, by id
/// and fenced to the source incarnation, that pays it.
#[derive(Debug, Clone)]
pub(crate) struct ForkDebt {
    child: crate::tenant::TenantStreamRef,
    child_epoch: String,
    source: crate::tenant::TenantStreamRef,
    source_epoch: String,
    fork_id: String,
}

impl ForkDebt {
    /// The stream whose tombstone may owe the release.
    pub(crate) fn child(&self) -> &crate::tenant::TenantStreamRef {
        &self.child
    }
    /// The incarnation of `child` that may owe it.
    pub(crate) fn child_epoch(&self) -> &str {
        &self.child_epoch
    }
    /// The fork source, resolved in the child's own project.
    pub(crate) fn source(&self) -> &crate::tenant::TenantStreamRef {
        &self.source
    }
    /// The source incarnation the reference was installed on.
    pub(crate) fn source_epoch(&self) -> &str {
        &self.source_epoch
    }
    /// The reference id in the source's `fork_children`.
    pub(crate) fn fork_id(&self) -> &str {
        &self.fork_id
    }
}

/// One page of the index.
pub(crate) struct ForkDebtPage {
    pub(crate) debts: Vec<ForkDebt>,
    /// The last consumed key; a later page continues after it.
    pub(crate) next_after: Option<String>,
    /// The listing ran out: the next page starts a new circle.
    pub(crate) exhausted: bool,
}

fn debt_path(sref: &crate::tenant::TenantStreamRef, epoch: &str) -> ObjPath {
    ObjPath::from(format!(
        "{FORK_DEBT_ROOT}{}/{}/{epoch}.json",
        hex(sref.project_id().as_bytes()),
        hex(sref.name().as_str().as_bytes())
    ))
}

/// The incarnation a marker key names, for exactly the keys `debt_path`
/// mints.
fn debt_of_key(key: &ObjPath) -> Option<(crate::tenant::TenantStreamRef, String)> {
    let text = key.as_ref();
    if !text.is_ascii() {
        // `unhex` slices by byte index.
        return None;
    }
    let mut parts = text
        .strip_prefix(FORK_DEBT_ROOT)?
        .strip_suffix(".json")?
        .split('/');
    let (project_hex, name_hex, epoch) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let project = String::from_utf8(crate::crypto::unhex(project_hex)?).ok()?;
    let name = String::from_utf8(crate::crypto::unhex(name_hex)?).ok()?;
    let sref = crate::tenant::TenantStreamRef::new(
        crate::tenant::ProjectId::new(&project).ok()?,
        crate::tenant::CanonicalStreamName::new(&name).ok()?,
    );
    (debt_path(&sref, epoch) == *key).then(|| (sref, epoch.to_owned()))
}

impl Registry {
    /// Index, BEFORE the write that can create it, the release `desc`'s
    /// incarnation may come to owe its fork source. A stream that was not
    /// forked owes nothing. Rewriting the same marker is idempotent: its
    /// content is fixed for the incarnation, whose fork id is its own epoch
    /// (`ForkRef::same_identity`), so an unstamped child is indexed under
    /// the id its stamp will write.
    pub(crate) async fn record_fork_debt(
        &self,
        desc: &StreamDesc,
    ) -> Result<(), object_store::Error> {
        let Some(parent) = desc.forked_from.as_ref() else {
            return Ok(());
        };
        let fork_id = if parent.fork_id.is_empty() {
            desc.stream_epoch.clone()
        } else {
            parent.fork_id.clone()
        };
        let body = serde_json::to_vec(&OwedRelease {
            source: parent.source.clone(),
            source_epoch: parent.source_epoch.clone(),
            fork_id,
        })
        .map_err(|error| invalid_descriptor(&desc.name, &error.to_string()))?;
        self.store
            .put_opts(
                &debt_path(&desc.sref(), &desc.stream_epoch),
                PutPayload::from(body),
                PutOptions::default(),
            )
            .await?;
        Ok(())
    }

    /// At most `limit` markers after `after`, in key order, read with the
    /// catalog's deadline. A key this index did not mint, or a body it
    /// cannot read, is skipped with a warning and left in place: deleting
    /// it could drop a debt, and failing the page on it would stall every
    /// later marker.
    pub(crate) async fn fork_debt_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ForkDebtPage, object_store::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        if limit == 0 {
            return Err(catalog_error("fork-debt page limit must be positive"));
        }
        let prefix = ObjPath::from(FORK_DEBT_ROOT.trim_end_matches('/'));
        let listing = match after.map(ObjPath::from) {
            Some(offset) => self.store.list_with_offset(Some(&prefix), &offset),
            None => self.store.list(Some(&prefix)),
        };
        let pass = async {
            let metas: Vec<object_store::ObjectMeta> = listing.take(limit).try_collect().await?;
            let exhausted = metas.len() < limit;
            let next_after = metas.last().map(|meta| meta.location.to_string());
            let debts: Vec<Option<ForkDebt>> = futures_util::stream::iter(metas)
                .map(|meta| self.read_fork_debt(meta))
                .buffered(8)
                .try_collect()
                .await?;
            Ok(ForkDebtPage {
                debts: debts.into_iter().flatten().collect(),
                next_after,
                exhausted,
            })
        };
        tokio::time::timeout(Duration::from_secs(10), pass)
            .await
            .map_err(|_| catalog_error("fork-debt page deadline exceeded"))?
    }

    async fn read_fork_debt(
        &self,
        meta: object_store::ObjectMeta,
    ) -> Result<Option<ForkDebt>, object_store::Error> {
        let Some((child, child_epoch)) = debt_of_key(&meta.location) else {
            tracing::warn!(key = %meta.location, "fork-debt index: skipping a key it did not mint");
            return Ok(None);
        };
        if meta.size > MAX_MARKER_BYTES {
            tracing::warn!(key = %meta.location, "fork-debt index: skipping an oversized marker");
            return Ok(None);
        }
        let raw = match self.store.get(&meta.location).await {
            Ok(result) => result.bytes().await?,
            // Settled by another instance since the listing.
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some((owed, source)) =
            serde_json::from_slice::<OwedRelease>(&raw)
                .ok()
                .and_then(|owed| {
                    let name = crate::tenant::CanonicalStreamName::new(&owed.source).ok()?;
                    Some((owed, name))
                })
        else {
            tracing::warn!(key = %meta.location, "fork-debt index: skipping an unreadable marker");
            return Ok(None);
        };
        Ok(Some(ForkDebt {
            // A fork reference binds inside the referring stream's project
            // (`PersistedDescriptor::ref_in_project`).
            source: crate::tenant::TenantStreamRef::new(child.project_id().clone(), source),
            child,
            child_epoch,
            source_epoch: owed.source_epoch,
            fork_id: owed.fork_id,
        }))
    }

    /// Drop the marker of `child`'s incarnation `child_epoch` once the caller
    /// has seen its debt paid: a conclusive release, or a tombstone that owes
    /// nothing. Idempotent: an absent marker is not an error.
    pub(crate) async fn settle_fork_debt(
        &self,
        child: &crate::tenant::TenantStreamRef,
        child_epoch: &str,
    ) -> Result<(), object_store::Error> {
        match self.store.delete(&debt_path(child, child_epoch)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (Arc<object_store::memory::InMemory>, Registry) {
        let store = Arc::new(object_store::memory::InMemory::new());
        let registry = Registry::new(store.clone(), &crate::tenant::CellId::new("cell").unwrap());
        (store, registry)
    }

    #[test]
    fn a_marker_key_names_exactly_the_incarnation_that_minted_it() {
        let sref = crate::tenant::ProjectId::new("proj-a")
            .unwrap()
            .stream_ref("orders");
        let key = debt_path(&sref, "0123456789abcdef0123456789abcdef");
        let (back, epoch) = debt_of_key(&key).expect("minted key parses");
        assert_eq!(back, sref);
        assert_eq!(epoch, "0123456789abcdef0123456789abcdef");
        for foreign in [
            "registry/v4/fork-debt/zz/6f/e.json",
            "registry/v4/fork-debt/70726f6a2d61/6f7264657273.json",
            "registry/v4/fork-debt/70726f6a2d61/6f7264657273/e/x.json",
            "registry/v4/projects/70726f6a2d61/streams/6f7264657273.json",
        ] {
            assert!(debt_of_key(&ObjPath::from(foreign)).is_none(), "{foreign}");
        }
    }

    #[tokio::test]
    async fn the_index_pages_every_marker_once_and_skips_foreign_keys() {
        let (store, registry) = store();
        let project = crate::tenant::ProjectId::new("proj-a").unwrap();
        for i in 0..5 {
            let sref = project.stream_ref(&format!("kid-{i}"));
            let body = serde_json::to_vec(&OwedRelease {
                source: "src".into(),
                source_epoch: "se".into(),
                fork_id: format!("fid-{i}"),
            })
            .unwrap();
            store
                .put(&debt_path(&sref, &format!("e{i}")), PutPayload::from(body))
                .await
                .unwrap();
        }
        store
            .put(
                &ObjPath::from("registry/v4/fork-debt/not-minted.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
        assert!(registry.fork_debt_page(None, 0).await.is_err());
        let (mut after, mut seen, mut pages) = (None::<String>, Vec::new(), 0);
        let mut exhausted = false;
        while !exhausted {
            let page = registry.fork_debt_page(after.as_deref(), 2).await.unwrap();
            seen.extend(page.debts);
            (after, exhausted, pages) = (page.next_after, page.exhausted, pages + 1);
        }
        assert_eq!(
            pages, 4,
            "six keys in full pages of two, then the empty page that ends the circle"
        );
        assert!(
            seen.iter()
                .all(|d| d.source() == &project.stream_ref("src"))
        );
        assert!(seen.iter().all(|d| d.source_epoch() == "se"));
        let mut seen: Vec<_> = seen.iter().map(|d| d.fork_id().to_owned()).collect();
        seen.sort();
        assert_eq!(seen, ["fid-0", "fid-1", "fid-2", "fid-3", "fid-4"]);
        let first = registry.fork_debt_page(None, 10).await.unwrap();
        let (child, epoch) = (first.debts[0].child(), first.debts[0].child_epoch());
        registry.settle_fork_debt(child, epoch).await.unwrap();
        registry.settle_fork_debt(child, epoch).await.unwrap();
        assert_eq!(
            registry.fork_debt_page(None, 10).await.unwrap().debts.len(),
            4
        );
    }
}
