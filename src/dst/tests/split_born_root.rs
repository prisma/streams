#![cfg(test)]
//! A split child's objects live under its born home's root from the first instant
//! (GATE-DECISION-born-home-seal-root-2026-11-19, test 1). The topology job's red on slate
//! cd7e35b09 was a child whose sealer PUT its first object while the fenced open had not yet told
//! the engine where its log lives: the object went under the static `shards/<prefix>`, the
//! directory row named the born home, and every later read answered 500. Here the rig's opener is
//! wrapped so that an engine opened at a born path seals before the opener returns it: that
//! interleaving, made certain, with no timing.

use super::fixture_http::{OpenWrap, engine_shutdown};
use super::fixture_split::{Cell, leaf_cell, records};
use super::fixture_storage::mem;
use crate::shard::seal::SealReport;
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Each born path's early seal, as the wrapper recorded it.
type Reports = Arc<Mutex<BTreeMap<String, SealReport>>>;

/// `open`, wrapped: an engine opened at an operation-born path (a `-` in its last segment) seals
/// what its open found unsealed before the opener returns it, and the seal's report is kept in
/// `reports`, so a test can prove the early seal wrote an object.
fn sealing_before_return(
    open: crate::sharddir::OpenAt,
    reports: Reports,
) -> crate::sharddir::OpenAt {
    let open = Arc::new(open);
    Box::new(
        move |prefix: String, path: String, incarnation: crate::sharddir::EngineIncarnation| {
            let (open, reports) = (open.clone(), reports.clone());
            Box::pin(async move {
                let engine = (*open)(prefix, path.clone(), incarnation).await?;
                if path
                    .rsplit('/')
                    .next()
                    .is_some_and(|last| last.contains('-'))
                {
                    let report = engine.seal_requested().await;
                    let report = report.map_err(|e| anyhow::anyhow!("the early seal: {e}"))?;
                    reports.lock().unwrap().insert(path, report);
                }
                Ok(engine)
            })
        },
    )
}

/// Every object under `prefix` in `store`.
async fn object_paths(store: &dyn object_store::ObjectStore, prefix: &str) -> Vec<String> {
    use futures_util::TryStreamExt;
    let prefix = object_store::path::Path::from(prefix);
    let all: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    all.into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// The split of the two-bit leaf `leaf` on a cell whose children seal before their opener returns:
/// each child's early seal wrote one object under its born root, `<leaf>0` home 4 and `<leaf>1`
/// home 5; both streams read back byte-exact; nothing is under either child's static path; and each
/// served child answers its born path and its parent's root (`parent_root`).
async fn children_seal_under_their_born_roots(leaf: &str, parent_root: &str) {
    let store = mem();
    let reports = Reports::default();
    let recorded = reports.clone();
    let wrap: OpenWrap = Box::new(move |open| sealing_before_return(open, recorded));
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = leaf_cell(store.clone(), leaf, wrap).await;
    let want = (records(addr, &low).await, records(addr, &high).await);
    let clock = state.runtime.clock.clone();
    let op_id = format!("op-born-{leaf}");
    let published =
        crate::topology::split::split(&state.shards, &parent, &op_id, "inst-a", &*clock)
            .await
            .expect("the split publishes");
    assert_eq!(published.version, 2);
    let ended = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended.intent.state,
        crate::topology::intent::State::Reclaimed
    );
    let children = [(format!("{leaf}0"), 4), (format!("{leaf}1"), 5)];
    let early = reports.lock().unwrap().clone();
    let born: Vec<String> = children
        .iter()
        .map(|(prefix, home)| crate::topology::model::born_home_path(prefix, *home))
        .collect();
    assert_eq!(early.keys().cloned().collect::<Vec<_>>(), born, "{early:?}");
    for ((_, home), path) in children.iter().zip(&born) {
        let report = &early[path];
        let object = ObjectId {
            home: *home,
            epoch: 2,
            seq: 1,
        };
        assert_eq!(report.object, Some(object), "{path}: {report:?}");
        assert_eq!(report.streams, 1, "{path}: {report:?}");
        assert!(report.pages > 0, "{path}: {report:?}");
    }
    assert_eq!(records(addr, &high).await, want.1, "{high}");
    assert_eq!(records(addr, &low).await, want.0, "{low}");
    for ((prefix, _), path) in children.iter().zip(&born) {
        let at_static = object_paths(&*store, &format!("shards/{prefix}/")).await;
        assert_eq!(at_static, Vec::<String>::new(), "shards/{prefix}/");
        let sealed = object_paths(&*store, &format!("{path}/hseg/")).await;
        let named = format!("{path}/hseg/0000000000000002/0000000000000001");
        assert_eq!(sealed, vec![named], "{path}/hseg/");
    }
    let served: Vec<_> = published
        .children
        .iter()
        .map(|(prefix, _, child)| {
            (
                prefix.clone(),
                child.log_path(),
                child.object_root(parent.home()),
            )
        })
        .collect();
    let expected: Vec<_> = children
        .iter()
        .zip(&born)
        .map(|((prefix, _), path)| (prefix.clone(), path.clone(), parent_root.to_string()))
        .collect();
    assert_eq!(served, expected);
    engine_shutdown(&state).await;
}

/// **A split's children seal under their born roots from the first instant (the CI case).** The
/// split of `00` in a four-shard cell: `000` home 4 and `001` home 5, the parent's root `shards/00`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_childs_first_seal_lands_under_its_born_root() {
    children_seal_under_their_born_roots("00", "shards/00").await;
}

/// **The same where each child's home is the number its prefix reads as.** The split of `10`:
/// `100` home 4 and `101` home 5 (GATE-DECISION ruling 2's collision, which no fact inside the
/// engine tells from an initial home), the parent's root `shards/10`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_childs_first_seal_lands_under_its_born_root_when_its_home_is_its_prefixs_number() {
    children_seal_under_their_born_roots("10", "shards/10").await;
}
