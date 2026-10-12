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

/// C4 (c): a catalog page holds at most 4 MiB of descriptors, each counted
/// at its stored bytes plus its decoded size (`held_bytes`). Forty
/// descriptors with no variable parts, stored at 128 KiB less a decoded
/// descriptor's size (so each counts exactly 128 KiB), list as a page of
/// 32 (4 MiB to the byte) continued after the 32nd, then a final page of 8.
#[tokio::test]
async fn a_catalog_page_holds_at_most_4_mib_of_descriptors() {
    let (store, reg) = registry();
    let stored = 128 * 1024 - std::mem::size_of::<crate::registry::StreamDesc>();
    for n in 0..40 {
        stored_at(&store, &reg, &format!("s{n:02}"), stored).await;
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

/// C8 (phase B item B6, the review of 2026-10-12): a page holds its
/// descriptors decoded, and a watch definition's pointers decode to far
/// more than they store (`"/",` is 4 bytes stored, a 24-byte header and an
/// 8-byte block decoded). A page counts each descriptor at its stored bytes
/// plus the decoded descriptor's own size plus what decoding its watch
/// definitions adds beyond their text, and stops at 4 MiB of that: 120
/// descriptors of 64 watches of 16 one-byte pointers (about 6 KB stored,
/// about 42 KB decoded each) list as a page of as many as fit, not 120.
#[tokio::test]
async fn a_catalog_page_counts_its_descriptors_as_decoded() {
    let (store, reg) = registry();
    let watches: Vec<crate::registry::WatchDefinition> = (0..64)
        .map(|w| crate::registry::WatchDefinition {
            name: format!("{w:02}"),
            fields: vec!["/".to_string(); 16],
        })
        .collect();
    for n in 0..120 {
        let mut d = filed("proj-a", &format!("s{n:03}"), false);
        d.watch_definitions.clone_from(&watches);
        assert!(reg.create(d).await.unwrap().0);
    }
    let path = desc_path("test-cell", &filed("proj-a", "s000", false).sref());
    let stored = usize::try_from(store.head(&path).await.unwrap().size).unwrap();
    // 3,072 of watch vector, 64 x (8 name + 384 field vector + 16 x 8
    // pointers) of heap, less the 64 x (2 + 16) bytes of text it stores.
    let extra = 3_072 + 64 * (8 + 384 + 16 * 8) - 64 * (2 + 16);
    let held = stored + std::mem::size_of::<crate::registry::StreamDesc>() + extra;
    let a = crate::tenant::ProjectId::new("proj-a").unwrap();
    let page = reg.list_page(&a, None, 1000).await.unwrap();
    let fit = 4 * 1024 * 1024 / held;
    assert_eq!(
        (page.streams.len(), page.exhausted),
        (fit, false),
        "a page of descriptors of {stored} bytes stored, {held} decoded"
    );
}

/// C8 (phase B item B6): what the catalog reserves before it reads a page
/// of `limit` streams, its descriptors decoded: the page (at most 4 MiB)
/// and the 8 descriptors it fetches ahead, each counted at most 128 KiB
/// stored plus what decoding adds (`MAX_DECODED_EXTRA`: a decoded
/// descriptor's size and 35,264 bytes for 64 watches of 16 pointers).
#[test]
fn a_catalog_page_reserves_its_page_and_what_it_fetches_ahead() {
    let extra = super::MAX_DECODED_EXTRA;
    let inline = std::mem::size_of::<crate::registry::StreamDesc>();
    assert_eq!(extra, inline + 35_264, "what decoding adds, at most");
    let each = u64::try_from(128 * 1024 + extra).unwrap();
    let (page, ahead) = (4 * 1024 * 1024, 8 * each);
    let reserved = [1, 10, 25, 26, 100, 1000, usize::MAX].map(Registry::catalog_page_bytes);
    assert_eq!(
        reserved,
        [
            each + ahead,
            10 * each + ahead,
            25 * each + ahead,
            page + ahead,
            page + ahead,
            page + ahead,
            page + ahead,
        ]
    );
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
