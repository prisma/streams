//! Consumer atomicity.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, consumer_version, preq};
use super::fixture_storage::mem;

/// Queue consumer state joins the applied/durable discipline: a
/// receive whose group write FAILS must leave no phantom lease in
/// memory — the exact retry gets the same records, with fresh leases,
/// exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_queue_write_leaves_no_phantom_leases() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/q12",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/q12/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200, "append {i}");
    }
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/q12/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":5000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201, "consumer create: {}", String::from_utf8_lossy(&b));
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("q12"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("q12"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // The pull's lease writes ride a group we fail.
    engine.fail_next_group_for(identity);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/q12/consumers/c1:pull",
        &key,
        br#"{"max": 3}"#,
    )
    .await;
    // The selector covers queue operations (round 13), so the pull's
    // group deterministically trips the arm.
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(st >= 500, "a pull outlived its failed group: {st}");
    let first_failed = true;
    let _ = b;
    let (st2, _, b2) = preq(
        addr,
        "POST",
        "/v1/streams/q12/consumers/c1:pull",
        &key,
        br#"{"max": 3}"#,
    )
    .await;
    assert_eq!(st2, 200, "retry pull: {}", String::from_utf8_lossy(&b2));
    let v: serde_json::Value = serde_json::from_slice(&b2).unwrap();
    let leased = v["messages"].as_array().map(|a| a.len()).unwrap_or(0);
    if first_failed {
        // No phantom in-memory leases may block the retry: all three
        // records lease afresh.
        assert_eq!(
            leased, 3,
            "phantom leases from the failed write blocked the retry: {v}"
        );
    } else {
        // The first pull won its race and leased durably; the retry
        // sees them held (per-key FIFO) — equally coherent.
        assert!(leased <= 3);
    }
    engine_shutdown(&state).await;
}

/// QUE-004: a settlement whose group write fails must leave no
/// phantom acks — the lease tokens remain valid, and the exact settle
/// retry acknowledges every one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_settle_leaves_no_phantom_acks() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/que004",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/que004/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200, "append {i}");
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/que004/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/que004/consumers/c1:pull",
        &key,
        br#"{"max": 3}"#,
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let recs = v["messages"].as_array().cloned().unwrap_or_default();
    assert_eq!(recs.len(), 3, "{v}");
    let acks: Vec<serde_json::Value> = recs
        .iter()
        .map(|r| serde_json::json!({"leaseToken": r["leaseToken"]}))
        .collect();
    let settle_body = serde_json::json!({ "acks": acks }).to_string();

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("que004"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("que004"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();
    engine.fail_next_group_for(identity);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/que004/consumers/c1:settle",
        &key,
        settle_body.as_bytes(),
    )
    .await;
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(st >= 500, "a settlement outlived its failed group: {st}");

    // No phantom acks: the SAME tokens settle successfully on retry.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/que004/consumers/c1:settle",
        &key,
        settle_body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "settle retry: {}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v["acked"].as_u64().unwrap_or(0),
        3,
        "phantom acks consumed the lease tokens: {v}"
    );
    engine_shutdown(&state).await;
}

// ---------------------------------------------------------------
// Round 14: queue config joins the group-local model; fork releases
// are incarnation-fenced.
// ---------------------------------------------------------------

/// A failed ConfigDelete must leave the consumer, its leases and its
/// cursor untouched — it staged into the batch-local overlay, not the
/// shared handle, so a lost write publishes nothing. And a
/// Receive→ConfigDelete in one group must leave the consumer DELETED
/// on success (the state copy-back must not resurrect it).
#[expect(
    clippy::too_many_lines,
    reason = "group-local delete scenario; arranging two consumers, deleting one and checking the survivor's leases and rows is one atomicity argument; helpers would separate the rows from the group that wrote them"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_config_delete_is_group_local() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc14",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc14/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc14/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Lease both records so the consumer has real state to lose.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc14/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200);
    let held = serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(held, 2);

    // Fail the group carrying the ConfigDelete.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc14"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc14"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();
    engine.fail_next_group_for(identity);
    // Driven at the committer: the product DELETE endpoint is disabled
    // for the preview (round 16); the committer ConfigDelete remains
    // the internal machinery whose failed-group contract this pins.
    let del = engine
        .submit_queue(
            identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await;
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        del.is_err(),
        "a config delete outlived its failed group: {del:?}"
    );
    // The failed group published NOTHING durable: the config record is
    // untouched, and the durable lease rows survive (the fence is up —
    // conservative — but conservativeness never fakes a deletion).
    let rows = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert!(rows > 0, "a failed delete erased durable rows");
    let (st, _, _) = preq(addr, "GET", "/v1/streams/qc14/consumers/c1", &key, b"").await;
    assert_eq!(st, 200, "a failed delete removed the config record");
    // The SAGA retry finishes the job end to end.
    let ver = consumer_version(addr, "qc14", "c1").await;
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc14/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "the delete retry must complete the saga");
    let rows = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows, 0, "the completed saga left durable rows behind");
    // And recreation starts a NEW generation with a clean world.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc14/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation after the saga");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc14/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200);
    let got = serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(got, 2, "the recreated generation inherited state");
    engine_shutdown(&state).await;
}

/// Round 15 B, case 1: `Receive -> ConfigDelete` composed into ONE
/// commit group, and the group SUCCEEDS. The Receive stages a lease
/// put into the group's WriteBatch; the durable scan inside
/// ConfigDelete cannot see it. Deletion must also delete the
/// group-LOCAL rows, or the lease outlives the consumer and a
/// recreated consumer inherits it after handle eviction. Composed by
/// DIRECT committer submits — the HTTP handlers interleave their own
/// preliminary ops, which is a different scenario.
#[expect(
    clippy::disallowed_methods,
    reason = "single-group receive/delete fixture; both queue operations are joined after the held commit is released; they must be staged concurrently to land in the same write group"
)]
#[expect(
    clippy::too_many_lines,
    reason = "same-group receive/delete scenario; staging both operations under one held commit and counting durable rows afterwards is one atomicity argument; splitting it would hide which group buried the lease"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_then_delete_in_one_group_leaves_no_stale_lease() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15a",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc15a/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc15a"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc15a"))
        .await
        .unwrap()
        .unwrap();
    let ro = desc.resolve_segment("");
    let engine = state.engine_for(&ro.shard_route).await.unwrap();
    let identity = ro.identity;

    let mut keys_map: std::collections::HashMap<u64, [u8; 16]> = Default::default();
    keys_map.insert(0, crate::crypto::stream_hash("k0"));
    keys_map.insert(1, crate::crypto::stream_hash("k1"));

    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e1 = engine.clone();
    let km = keys_map.clone();
    let recv = tokio::spawn(async move {
        e1.submit_queue(
            identity,
            crate::queue::QueueOp::Receive {
                consumer: "c1".into(),
                cgen: 1,
                max: 2,
                visibility_ms: 30_000,
                max_deliveries: 3,
                keys: km,
                covered_to: 2,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let e2 = engine.clone();
    let del = tokio::spawn(async move {
        e2.submit_queue(
            identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    drop(hold);
    let r = recv.await.unwrap().expect("receive");
    match r {
        crate::queue::QueueOut::Received { leased, .. } => {
            assert_eq!(leased.len(), 2, "the Receive leased both records in-group");
        }
        other => panic!("expected Received, got {other:?}"),
    }
    del.await.unwrap().expect("delete");

    // Ground truth: with generations, leaked residue is INVISIBLE to a
    // recreated consumer by design — so burial is proven by counting
    // durable rows, not by pulling. Zero rows, all generations.
    let rows = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "a lease staged in the SAME group as the delete survived it durably"
    );
    engine_shutdown(&state).await;
}

/// Round 15 B, case 2: `Settle -> ConfigDelete` in ONE group. The
/// settle stages ack/cursor mutations into the WriteBatch; the delete
/// must bury those too. A recreated consumer starts from scratch —
/// no inherited cursor, no inherited acks.
#[expect(
    clippy::disallowed_methods,
    reason = "single-group settle/delete fixture; both queue operations are joined after the held commit is released; they must be staged concurrently to land in the same write group"
)]
#[expect(
    clippy::too_many_lines,
    reason = "same-group settle/delete scenario; leasing, settling and deleting under one held commit before counting rows is one atomicity argument; splitting it would hide which group buried the acknowledgement"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_then_delete_in_one_group_leaves_no_stale_rows() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc15b/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15b/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc15b"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc15b"))
        .await
        .unwrap()
        .unwrap();
    let ro = desc.resolve_segment("");
    let engine = state.engine_for(&ro.shard_route).await.unwrap();
    let identity = ro.identity;

    // Lease both records through a NORMAL committed group, keeping the
    // lease generations for the settle.
    let mut keys_map: std::collections::HashMap<u64, [u8; 16]> = Default::default();
    keys_map.insert(0, crate::crypto::stream_hash("k0"));
    keys_map.insert(1, crate::crypto::stream_hash("k1"));
    let leased = match engine
        .submit_queue(
            identity,
            crate::queue::QueueOp::Receive {
                consumer: "c1".into(),
                cgen: 1,
                max: 2,
                visibility_ms: 30_000,
                max_deliveries: 3,
                keys: keys_map,
                covered_to: 2,
            },
        )
        .await
        .expect("seed receive")
    {
        crate::queue::QueueOut::Received { leased, .. } => leased,
        other => panic!("expected Received, got {other:?}"),
    };
    assert_eq!(leased.len(), 2);
    // Ack the SECOND record while the first stays leased: an
    // OUT-OF-ORDER ack stages a persistent ack ROW (an in-order ack
    // only advances the cursor, which the delete already buries
    // unconditionally) — the row the group-local enumeration exists
    // to find.
    let (off0, gen0) = (leased[1].0, leased[1].1);

    // ONE group: [Settle(ack off1), ConfigDelete].
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e1 = engine.clone();
    let settle = tokio::spawn(async move {
        e1.submit_queue(
            identity,
            crate::queue::QueueOp::Settle {
                consumer: "c1".into(),
                cgen: 1,
                acks: vec![(off0, gen0)],
                retries: Vec::new(),
                extends: Vec::new(),
                max_deliveries: 3,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let e2 = engine.clone();
    let del = tokio::spawn(async move {
        e2.submit_queue(
            identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    drop(hold);
    settle.await.unwrap().expect("settle");
    del.await.unwrap().expect("delete");

    let rows = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert_eq!(
        rows, 0,
        "ack/cursor rows staged in the SAME group as the delete survived it durably"
    );
    engine_shutdown(&state).await;
}

/// Round 15, the REVERSE order: a Receive that lands in the same group
/// AFTER the ConfigDelete must be refused — the consumer no longer
/// exists for later ops in the group. Without the overlay check it
/// silently re-staged lease rows for the dead consumer (this exact
/// composition found the hole).
#[expect(
    clippy::disallowed_methods,
    reason = "single-group delete/receive fixture; both queue operations are joined after the held commit is released; the refusal only exists when the receive is staged behind the delete in one group"
)]
#[expect(
    clippy::too_many_lines,
    reason = "same-group delete/receive scenario; the ordering, the refusal and the absence of re-staged rows are one argument; splitting it would hide the overlay check being proved"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receive_after_delete_in_the_same_group_is_refused() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15d",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc15d/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15d/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc15d"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc15d"))
        .await
        .unwrap()
        .unwrap();
    let ro = desc.resolve_segment("");
    let engine = state.engine_for(&ro.shard_route).await.unwrap();
    let identity = ro.identity;
    let mut keys_map: std::collections::HashMap<u64, [u8; 16]> = Default::default();
    keys_map.insert(0, crate::crypto::stream_hash("k0"));
    keys_map.insert(1, crate::crypto::stream_hash("k1"));

    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let e1 = engine.clone();
    let del = tokio::spawn(async move {
        e1.submit_queue(
            identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let e2 = engine.clone();
    let km = keys_map.clone();
    let recv = tokio::spawn(async move {
        e2.submit_queue(
            identity,
            crate::queue::QueueOp::Receive {
                consumer: "c1".into(),
                cgen: 1,
                max: 2,
                visibility_ms: 30_000,
                max_deliveries: 3,
                keys: km,
                covered_to: 2,
            },
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    drop(hold);
    del.await.unwrap().expect("delete");
    let r = recv.await.unwrap();
    match r {
        Err(m) => assert!(
            m.starts_with("consumer_generation_fenced") || m.starts_with("consumer_not_found"),
            "wrong refusal: {m}"
        ),
        Ok(o) => panic!("a Receive after an in-group delete succeeded: {o:?}"),
    }

    // And nothing of the refused Receive survives durably.
    let rows = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows, 0, "the refused Receive leaked durable rows");
    // Finish the saga over the wire, recreate at generation 2, and the
    // new consumer's world is clean.
    let ver = consumer_version(addr, "qc15d", "c1").await;
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc15d/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "saga completes");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15d/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation allocates a new generation");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc15d/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200);
    let got = serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        got,
        2,
        "the refused Receive leaked state: {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}
