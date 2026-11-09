#![cfg(test)]
//! A split of a split's child (S4-DESIGN §3.1, §6.2): a second-generation parent lives at an
//! operation-born home's path, and its children clone that path's own tables (its C4 seed table, and
//! what it flushed since the first split) as well as those it inherited from the first parent.

use super::fixture_http::engine_shutdown;
use super::split_k::{append, cell, records};

/// **A child of a split splits again and serves its stream from its own child.** Leaf `00` splits
/// into `000` and `001`; the lower stream is appended to on `000` and its log flushed, so `000`
/// holds tables of its own under its born home's path; then `000` splits into `0000` and `0001`.
/// The second split publishes version 3, `000` is retired, and the lower stream reads back its
/// three records from its new leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_childs_split_clones_its_own_tables_and_serves_from_its_child() {
    let cell = cell().await;
    cell.parent.seal_requested().await.expect("seal");
    let clock = cell.state.runtime.clock.clone();
    let first =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-g1", "inst-a", &*clock)
            .await
            .expect("the first split publishes");
    let (prefix, _, child) = first
        .children
        .into_iter()
        .find(|(p, _, _)| p == "000")
        .unwrap();
    assert_eq!(prefix, "000");
    assert_eq!(
        append(cell.addr, &cell.low, 5).await.0,
        204,
        "appended on the child"
    );
    child.db.flush().await.unwrap();
    let want = records(cell.addr, &cell.low).await;
    assert_eq!(want.as_array().map(Vec::len), Some(3));
    let second =
        crate::topology::split::split(&cell.state.shards, &child, "op-g2", "inst-a", &*clock)
            .await
            .expect("the second split publishes");
    assert_eq!(second.version, 3);
    let prefixes: Vec<&str> = second.children.iter().map(|(p, _, _)| p.as_str()).collect();
    assert_eq!(prefixes, ["0000", "0001"]);
    assert!(child.is_closed(), "the first child is retired");
    assert_eq!(records(cell.addr, &cell.low).await, want);
    engine_shutdown(&cell.state).await;
}
