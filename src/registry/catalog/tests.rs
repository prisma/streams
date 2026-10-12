//! The cell-wide reconciliation page against the project catalog it shares one
//! scan with, and the key parser that binds a descriptor to the identity its
//! own object key names.
#![cfg(test)]
use super::sref_of_key;
use crate::registry::tests::desc;
use crate::registry::{PROJECTS_ROOT, PersistedDescriptor, Registry, desc_path};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use proptest::prelude::{ProptestConfig, Strategy};
use std::sync::Arc;

const EPOCH: &str = "00000000000000000000000000000001";

fn registry() -> (Arc<dyn ObjectStore>, Registry) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cell = crate::tenant::CellId::new("test-cell").unwrap();
    (store.clone(), Registry::new(store, &cell))
}

fn filed(project: &str, name: &str, deleted: bool) -> PersistedDescriptor {
    let mut d = desc(name, EPOCH, deleted);
    d.project_id = crate::tenant::ProjectId::new(project).unwrap();
    d
}

/// Bug 5: the reconciler's page is CELL-wide and key-ordered. Every project's
/// descriptors, terminals included, come back exactly once, each bound to the
/// project in its OWN key, across a cursor that lands mid-project; the
/// customer catalog still sees one project's live streams only.
#[tokio::test]
async fn reconciliation_page_reaches_terminals_of_every_project_in_key_order() {
    let (_store, reg) = registry();
    let mut expected = Vec::new();
    for project in ["proj-a", "proj-b", "proj-c"] {
        for (name, deleted) in [("gone", true), ("live", false)] {
            assert!(reg.create(filed(project, name, deleted)).await.unwrap().0);
            expected.push((project.to_string(), name.to_string(), deleted));
        }
    }
    assert!(reg.reconciliation_page(None, 0).await.is_err());
    let (mut after, mut seen) = (None, Vec::new());
    loop {
        let page = reg.reconciliation_page(after.as_deref(), 3).await.unwrap();
        for d in &page.streams {
            seen.push((d.project_id.to_string(), d.name.clone(), d.deleted));
        }
        if page.exhausted {
            break;
        }
        assert_ne!(page.next_after, after, "a page must make provider progress");
        after = page.next_after;
    }
    assert_eq!(
        seen, expected,
        "every project, terminals included, exactly once"
    );
    // §10.3 is untouched: a project's own catalog sees only its live streams.
    let b = crate::tenant::ProjectId::new("proj-b").unwrap();
    let visible = reg.list_page(&b, None, 10).await.unwrap();
    assert_eq!(visible.streams.len(), 1);
    assert_eq!(visible.streams[0].name, "live");
    assert_eq!(visible.streams[0].project_id, b);
}

#[tokio::test]
async fn reconciliation_page_fails_closed_on_a_key_it_cannot_bind() {
    // A descriptor filed under ANOTHER project's key is corruption, not a row.
    let (store, reg) = registry();
    assert!(reg.create(filed("proj-a", "s", true)).await.unwrap().0);
    let from = desc_path("test-cell", &filed("proj-a", "s", true).sref());
    let to = desc_path("test-cell", &filed("proj-b", "s", true).sref());
    let raw = store.get(&from).await.unwrap().bytes().await.unwrap();
    store.put(&to, raw.into()).await.unwrap();
    assert!(reg.reconciliation_page(None, 10).await.is_err());
    // An object under the projects root that no descriptor key can name
    // stops the page instead of being skipped...
    let (store, reg) = registry();
    assert!(reg.create(filed("proj-a", "s", true)).await.unwrap().0);
    let stray = ObjPath::from(format!("{PROJECTS_ROOT}zz/streams/00.json"));
    store.put(&stray, "{}".into()).await.unwrap();
    assert!(reg.reconciliation_page(None, 10).await.is_err());
    // ...and never reaches a project's own catalog.
    let a = crate::tenant::ProjectId::new("proj-a").unwrap();
    assert!(reg.list_page(&a, None, 10).await.is_ok());
}

/// Create `name`'s live descriptor in "proj-a" and store it again padded
/// with trailing JSON whitespace to exactly `len` bytes.
async fn stored_at(store: &Arc<dyn ObjectStore>, reg: &Registry, name: &str, len: usize) {
    let d = filed("proj-a", name, false);
    assert!(reg.create(d.clone()).await.unwrap().0);
    let path = desc_path("test-cell", &d.sref());
    let mut raw = store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    raw.resize(len, b' ');
    store.put(&path, raw.into()).await.unwrap();
}

/// C4 (c): a catalog page materializes at most 4 MiB of descriptors.
/// Forty descriptors of exactly 128 KiB list as a page of 32 (4 MiB to
/// the byte) continued after the 32nd, then a final page of 8.
#[tokio::test]
async fn a_catalog_page_holds_at_most_4_mib_of_descriptors() {
    let (store, reg) = registry();
    for n in 0..40 {
        stored_at(&store, &reg, &format!("s{n:02}"), 128 * 1024).await;
    }
    let a = crate::tenant::ProjectId::new("proj-a").unwrap();
    let first = reg.list_page(&a, None, 1000).await.unwrap();
    let shape = |page: &crate::registry::CatalogPage| {
        (page.streams.len(), page.next_after.clone(), page.exhausted)
    };
    assert_eq!(shape(&first), (32, Some("s31".to_string()), false));
    let rest = reg.list_page(&a, Some("s31"), 1000).await.unwrap();
    assert_eq!(rest.streams.first().map(|d| d.name.as_str()), Some("s32"));
    assert_eq!(shape(&rest), (8, Some("s39".to_string()), true));
}

/// C4 (c): a descriptor over 128 KiB fails its page closed, the project's
/// catalog and the cell's reconciliation alike; one of exactly 128 KiB is
/// served.
#[tokio::test]
async fn a_descriptor_over_128_kib_fails_its_catalog_page_closed() {
    let (store, reg) = registry();
    stored_at(&store, &reg, "at", 128 * 1024).await;
    let a = crate::tenant::ProjectId::new("proj-a").unwrap();
    assert_eq!(reg.list_page(&a, None, 10).await.unwrap().streams.len(), 1);
    stored_at(&store, &reg, "over", 128 * 1024 + 1).await;
    let listed = reg.list_page(&a, None, 10).await.map(|p| p.streams.len());
    let reconciled = reg
        .reconciliation_page(None, 10)
        .await
        .map(|p| p.streams.len());
    assert!(
        listed.is_err() && reconciled.is_err(),
        "pages over a descriptor of 131,073 bytes: catalog {listed:?}, reconciliation {reconciled:?}"
    );
}

fn identities() -> impl Strategy<Value = crate::tenant::TenantStreamRef> {
    (
        "[A-Za-z0-9_-]{1,24}",
        "[a-z0-9][a-z0-9-]{0,11}(/[a-z0-9][a-z0-9-]{0,11}){0,2}",
    )
        .prop_filter_map(
            "reserved ids and names are not customer identities",
            |(p, n)| {
                let project = crate::tenant::ProjectId::new(&p).ok()?;
                let name = crate::tenant::CanonicalStreamName::new(&n).ok()?;
                (!project.is_system()).then(|| crate::tenant::TenantStreamRef::new(project, name))
            },
        )
}

proptest::proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, ..ProptestConfig::default() })]

    /// Every key `desc_path` mints binds back to exactly its identity.
    #[test]
    fn a_minted_key_binds_exactly_its_own_identity(sref in identities()) {
        let key = desc_path("cell", &sref);
        proptest::prop_assert_eq!(sref_of_key("cell", &key), Some(sref));
    }

    /// Nothing else binds: non-canonical hex, a truncated or re-rooted key,
    /// odd-length hex and arbitrary (non-ASCII) text neither bind nor panic.
    #[test]
    fn no_damaged_key_binds(sref in identities(), damage in 0usize..6, noise in "\\PC{0,48}") {
        let key = desc_path("cell", &sref).to_string();
        let project_hex = crate::crypto::hex(sref.project_id().as_bytes());
        let damaged = match damage {
            0 => key.replacen(&project_hex, &project_hex.to_uppercase(), 1),
            1 => key.replacen(".json", "0.json", 1),
            2 => key.replacen("/streams/", "/stream/", 1),
            3 => key.replacen(".json", "", 1),
            4 => key.replacen(PROJECTS_ROOT, "registry/v3/projects/", 1),
            _ => noise,
        };
        proptest::prop_assume!(damaged != key);
        let damaged = ObjPath::from(damaged.as_str());
        let bound = sref_of_key("cell", &damaged);
        if damage < 5 {
            proptest::prop_assert_eq!(bound, None);
        } else if let Some(bound) = bound {
            proptest::prop_assert_eq!(desc_path("cell", &bound), damaged);
        }
    }
}
