//! The committer's touched-queue mark (S4-DESIGN §7.2): while a split's snapshot pass has set
//! it, every queue whose state row a commit group writes is recorded, once each, and nothing is
//! recorded before the mark or after its release.
#![cfg(test)]
use std::collections::HashSet;

use super::{HASH, claim, create, msg, op, send_op};
use crate::native_queue::{NqOp, QueueConfig};

/// **The mark records exactly the queues a group wrote while it was set.** A create before the
/// mark is not recorded; a send and a claim on one queue and a create of another, while it is
/// set, record both queues once; a send after its release records nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_mark_records_each_queue_a_group_wrote_while_it_was_set() {
    let (e, _db) = super::engine("touched").await;
    create(&e, QueueConfig::default()).await;
    let gate = &e.gc_state().gate;
    gate.start_marking();
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    assert_eq!(claim(&e, 1).await.len(), 1);
    let other = [7; 16];
    let cfg = QueueConfig::default();
    e.submit_native_queue(other, NqOp::Create { cfg })
        .await
        .unwrap();
    assert_eq!(gate.take_touched(), HashSet::from([HASH, other]));
    op(&e, send_op(vec![msg("b")])).await.unwrap();
    assert!(gate.take_touched().is_empty(), "nothing after the release");
    e.begin_close();
}
