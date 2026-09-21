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
