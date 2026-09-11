//! Consumer generations.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, consumer_version, preq};
use super::fixture_storage::mem;

// ---- round 17: deletion names an incarnation, never a name ------------

/// **A stale segment cleanup can no longer erase a recreated
/// generation.** D1 deletes generation 1 fully; the consumer is
/// recreated at generation 2 with live leases; then D2's stale
/// cleanup (fence_below = 2) replays against the segment. Generation-2
/// rows must remain intact — the cleanup deletes generations the fence
/// declared dead, never every generation sharing the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_cleanup_replay_never_touches_a_recreated_generation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17a",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc17a/records",
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
        "/v1/streams/qc17a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc17a/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200, "generation-1 leases exist");

    // D1 completes: generation 1 -> Deleted.
    let ver1 = consumer_version(addr, "qc17a", "c1").await;
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc17a/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver1.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));

    // Recreation: generation 2, with its own live leases.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation after the tombstone");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc17a/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let leased: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        leased["messages"].as_array().unwrap().len(),
        2,
        "generation 2 leased both records"
    );
    let ver2 = consumer_version(addr, "qc17a", "c1").await;

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc17a"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc17a"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state.engine_for(&seg.shard_route).await.unwrap();
    let rows_before = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert!(rows_before > 0, "generation-2 rows exist before the replay");

    // D2's STALE cleanup resumes: fence_below = 2 (the generation-1
    // deletion's fence). It must find nothing left to delete and must
    // not touch generation 2.
    let out = engine
        .submit_queue(
            seg.identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await
        .expect("stale replay submits cleanly");
    match out {
        crate::queue::QueueOut::DeleteStep {
            complete,
            deleted_rows,
        } => {
            assert!(complete, "nothing below the fence remains");
            assert_eq!(deleted_rows, 0, "the stale replay deleted rows");
        }
        other => panic!("expected DeleteStep, got {other:?}"),
    }
    let rows_after = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert_eq!(
        rows_after, rows_before,
        "the stale replay changed generation-2 rows"
    );
    // The live generation still works end to end: settle its leases.
    let acks: Vec<String> = leased["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["leaseToken"].as_str().unwrap().to_string())
        .collect();
    let settle_body = serde_json::json!({
        "acks": acks.iter().map(|t| serde_json::json!({"leaseToken": t})).collect::<Vec<_>>()
    })
    .to_string();
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc17a/consumers/c1:settle",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        settle_body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 2, "generation-2 settle survived the replay");
    let ver2_after = consumer_version(addr, "qc17a", "c1").await;
    assert_eq!(ver2, ver2_after, "the incarnation token changed");
    engine_shutdown(&state).await;
}

/// **A stale DELETE retry cannot delete the replacement consumer.**
/// The version token pins the incarnation: retrying generation 1's
/// DELETE after a recreation is an idempotent 204 that leaves
/// generation 2 Active and its rows untouched; a forged newer-than-
/// record version is a 409; a missing version is a 400.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_delete_retry_cannot_delete_the_replacement_consumer() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17b/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201);
    let ver1 = h
        .get("prisma-consumer-version")
        .expect("create returns the version")
        .clone();

    // Missing version: refused before anything happens.
    let (st, _, b) = preq(addr, "DELETE", "/v1/streams/qc17b/consumers/c1", &key, b"").await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "missing_consumer_version");

    // Generation 1 deleted; its 204 "lost"; the name recreated.
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc17b/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver1.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204);
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17b/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "replacement consumer");
    let ver2 = h.get("prisma-consumer-version").unwrap().clone();
    assert_ne!(ver1, ver2, "recreation minted a new incarnation");
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc17b/records",
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
        "POST",
        "/v1/streams/qc17b/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200, "replacement leases records");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc17b"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc17b"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state.engine_for(&seg.shard_route).await.unwrap();
    let rows_before = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert!(rows_before > 0);

    // The original client's retry arrives with the OLD version.
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc17b/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver1.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(
        st,
        204,
        "a stale retry is idempotent success: {}",
        String::from_utf8_lossy(&b)
    );
    let (st, h, _) = preq(addr, "GET", "/v1/streams/qc17b/consumers/c1", &key, b"").await;
    assert_eq!(st, 200, "the replacement survived the stale retry");
    assert_eq!(
        h.get("prisma-consumer-version").unwrap(),
        &ver2,
        "the replacement's incarnation changed"
    );
    let rows_after = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows_after, rows_before, "the stale retry deleted rows");

    // A version NEWER than the record is impossible from an honest
    // client: conflict, no mutation.
    let epoch = {
        match crate::http::check_key(Some(PRISMA_KEY), &desc) {
            crate::http::KeyCheck::Ok(_, e) => e,
            _ => panic!("key check failed"),
        }
    };
    let forged = crate::product::consumer_version_token(&epoch, 99);
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc17b/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", forged.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "consumer_version_conflict");
    let (st, _, _) = preq(addr, "GET", "/v1/streams/qc17b/consumers/c1", &key, b"").await;
    assert_eq!(st, 200, "the conflict mutated nothing");
    engine_shutdown(&state).await;
}

/// **A parked deletion saga never rebinds to a recreated stream.** The
/// saga parks before its descriptor refresh; the collection is deleted
/// and recreated under the same name and key (new epoch) with its own
/// consumer and leases; the resumed saga must observe the epoch change
/// and answer 204 without touching the replacement.
#[expect(
    clippy::disallowed_methods,
    reason = "parked saga fixture; the deletion saga is released and joined before the replacement is examined; it must park before its descriptor refresh while the stream is recreated"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_saga_never_touches_a_recreated_stream() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17c",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17c/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201);
    let ver1 = consumer_version(addr, "qc17c", "c1").await;

    crate::failpoints::park_consumer_saga_before_refresh("qc17c");
    let before =
        crate::failpoints::parked(crate::failpoints::Fp::ConsumerSagaBeforeRefresh, "qc17c");
    let a1 = addr;
    let v1 = ver1.clone();
    let del = tokio::spawn(async move {
        preq(
            a1,
            "DELETE",
            "/v1/streams/qc17c/consumers/c1",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-consumer-version", v1.as_str()),
            ],
            b"",
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::ConsumerSagaBeforeRefresh, "qc17c")
        == before
    {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // While the saga is parked: the stream dies and is reborn under
    // the same name and key — a NEW incarnation, with its own consumer
    // and leases.
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/qc17c", &key, b"").await;
    assert!(st == 200 || st == 202 || st == 204, "stream delete: {st}");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17c",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation under the same name");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc17c/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "k0"),
        ],
        br#"{"n":0}"#,
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17c/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "replacement stream's consumer");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc17c/consumers/c1:pull",
        &key,
        br#"{"max": 1}"#,
    )
    .await;
    assert_eq!(st, 200, "replacement consumer leases");
    let ver_b = consumer_version(addr, "qc17c", "c1").await;
    assert_ne!(ver1, ver_b, "the replacement is a different incarnation");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc17c"));
    let desc_b = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc17c"))
        .await
        .unwrap()
        .unwrap();
    let seg_b = desc_b.resolve_segment("");
    let engine_b = state.engine_for(&seg_b.shard_route).await.unwrap();
    let rows_before = engine_b
        .count_consumer_state_rows(seg_b.identity, "c1")
        .await
        .unwrap();
    assert!(rows_before > 0);

    // Resume the old saga: it must observe the epoch change and stop.
    crate::failpoints::release_consumer_saga_before_refresh("qc17c");
    let (st, _, b) = del.await.unwrap();
    assert_eq!(
        st,
        204,
        "the resumed saga's old target is gone — idempotent success: {}",
        String::from_utf8_lossy(&b)
    );
    let rows_after = engine_b
        .count_consumer_state_rows(seg_b.identity, "c1")
        .await
        .unwrap();
    assert_eq!(
        rows_after, rows_before,
        "the resumed saga touched the replacement stream"
    );
    let (st, h, _) = preq(addr, "GET", "/v1/streams/qc17c/consumers/c1", &key, b"").await;
    assert_eq!(st, 200, "the replacement consumer survived");
    assert_eq!(h.get("prisma-consumer-version").unwrap(), &ver_b);
    engine_shutdown(&state).await;
}

/// **Cleanup is bounded and resumable under residue at scale.** 500
/// dead-generation rows plus a live generation's leases and ack
/// markers: every step's batch stays within its row budget, every step
/// makes progress, the dead rows drain to zero across many steps, and
/// the LIVE generation's rows survive byte-count-identically. Then the
/// real DELETE completes and recreation starts clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bounded_cleanup_drains_residue_without_touching_the_live_generation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17d",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..8 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc17d/records",
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
        "/v1/streams/qc17d/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Live generation (1): leases on all eight keys, half acked (ack
    // markers accumulate for offsets settled out of order).
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc17d/consumers/c1:pull",
        &key,
        br#"{"max": 8}"#,
    )
    .await;
    assert_eq!(st, 200);
    let leased: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let toks: Vec<String> = leased["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["leaseToken"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(toks.len(), 8);
    let settle_body = serde_json::json!({
        "acks": toks[4..].iter().map(|t| serde_json::json!({"leaseToken": t})).collect::<Vec<_>>()
    })
    .to_string();
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc17d/consumers/c1:settle",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        settle_body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc17d"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc17d"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state.engine_for(&seg.shard_route).await.unwrap();
    let live_rows = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert!(live_rows > 0, "live generation has rows");

    // Dead residue: 500 generation-0 lease rows (a prior incarnation's
    // wreckage).
    engine
        .seed_consumer_residue_rows(seg.identity, "c1", 0, 500)
        .await
        .unwrap();
    let total = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert_eq!(total, live_rows + 500);

    // Drain the residue with TINY steps (fence_below = 1: only
    // generation 0 is dead). Every step: batch <= budget, progress > 0,
    // and the live generation untouched throughout.
    let budget = 64usize;
    let mut steps = 0u32;
    let mut remaining = total;
    loop {
        let out = engine
            .submit_queue(
                seg.identity,
                crate::queue::QueueOp::ConfigDeleteStep {
                    consumer: "c1".into(),
                    fence_below: 1,
                    max_rows: budget,
                    max_bytes: 1 << 20,
                },
            )
            .await
            .expect("step");
        steps += 1;
        assert!(steps <= 64, "the bounded drain never converged");
        let (complete, deleted) = match out {
            crate::queue::QueueOut::DeleteStep {
                complete,
                deleted_rows,
            } => (complete, deleted_rows),
            other => panic!("expected DeleteStep, got {other:?}"),
        };
        assert!(
            deleted as usize <= budget,
            "a step staged {deleted} rows over its {budget} budget"
        );
        let now = engine
            .count_consumer_state_rows(seg.identity, "c1")
            .await
            .unwrap();
        assert_eq!(
            now,
            remaining - deleted as usize,
            "durable rows and reported deletions disagree"
        );
        remaining = now;
        if complete {
            break;
        }
        assert!(deleted > 0, "an incomplete step made no progress");
    }
    assert!(
        steps >= (500 / budget) as u32,
        "500 rows cannot drain in {steps} steps of {budget}"
    );
    assert_eq!(
        remaining, live_rows,
        "the drain touched the live generation"
    );

    // The real DELETE (fence 2) then completes collection-wide, and
    // recreation starts clean.
    let ver = consumer_version(addr, "qc17d", "c1").await;
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc17d/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));
    let rows = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows, 0, "the completed deletion left rows");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc17d/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation after the drain");
    engine_shutdown(&state).await;
}

/// #108: the failpoint registry is ENUMERABLE — every variant carries
/// its site contract, and the count is pinned so adding a failpoint
/// without registering it (or registering without describing it) is a
/// red test, not a silent drift.
#[test]
fn failpoint_registry_is_enumerable_and_described() {
    use crate::failpoints::Fp;
    assert_eq!(Fp::ALL.len(), 25);
    for fp in Fp::ALL {
        assert!(!fp.site().is_empty(), "{fp:?} has no site contract");
    }
    // Per-name isolation: arrivals for one name are invisible to
    // another — the property whose absence was the parallel-flake
    // family.
    assert_eq!(
        crate::failpoints::parked(Fp::PullBeforeReceive, "fp-enum-a"),
        0
    );
    crate::failpoints::arm(Fp::PullBeforeReceive, "fp-enum-a");
    assert_eq!(
        crate::failpoints::parked(Fp::PullBeforeReceive, "fp-enum-b"),
        0
    );
    crate::failpoints::release(Fp::PullBeforeReceive, "fp-enum-a");
}
