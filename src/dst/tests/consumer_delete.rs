//! Consumer delete.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, consumer_version, preq};
use super::fixture_storage::mem;

/// Round 16, the reviewer's first required scenario: a SPLIT
/// collection with leases on BOTH physical segments; one segment's
/// cleanup FAILS mid-saga. The DELETE must not return 204; the retry
/// finishes the cleanup; recreation inherits nothing — proven at the
/// row level on both segments.
#[expect(
    clippy::too_many_lines,
    reason = "split-consumer deletion scenario; leasing on both segments, failing one segment's cleanup and retrying to a clean state form one causal sequence; helper phases would hide which segment retained rows"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_consumers_deletion_fails_one_segment_then_retries_clean() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16s",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Find two routing keys that land on OPPOSITE halves of the
    // keyspace, then split at the midpoint so each key has its own
    // physical segment.
    let mid = 0x8000_0000_0000_0000u64;
    let mut lo_key = None;
    let mut hi_key = None;
    for i in 0..64 {
        let k = format!("rk{i}");
        let point = crate::registry::StreamDesc::key_point(&k);
        if point < mid && lo_key.is_none() {
            lo_key = Some(k);
        } else if point >= mid && hi_key.is_none() {
            hi_key = Some(k);
        }
        if lo_key.is_some() && hi_key.is_some() {
            break;
        }
    }
    let (lo_key, hi_key) = (lo_key.unwrap(), hi_key.unwrap());
    // Split FIRST, append after: post-split records land in the child
    // segments, so the consumer's leases live one per child (a
    // pre-split append would put both leases in the sealed parent).
    assert!(
        crate::scaler3::execute_split(&state, &state.deployment.raw_adapter_sref("qc16s"), 0, mid)
            .await,
        "split"
    );
    for k in [&lo_key, &hi_key] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc16s/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            br#"{"n":1}"#,
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16s/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Lease on BOTH segments. One pull serves one segment (the same
    // per-segment pagination discipline as reads), so pull twice.
    let mut total = 0usize;
    for _ in 0..2 {
        let (st, _, b) = preq(
            addr,
            "POST",
            "/v1/streams/qc16s/consumers/c1:pull",
            &key,
            br#"{"max": 4}"#,
        )
        .await;
        assert_eq!(st, 200);
        total += serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
    }
    assert_eq!(total, 2, "one lease per segment across two pulls");

    // The two CHILD segments' engines + identities.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc16s"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc16s"))
        .await
        .unwrap()
        .unwrap();
    let children: Vec<_> = desc
        .segments
        .as_ref()
        .unwrap()
        .segments
        .iter()
        .filter(|sg| sg.is_live())
        .map(|sg| {
            (
                desc.dynamic_segment_identity(sg.seg_id),
                desc.segment_route(sg),
            )
        })
        .collect();
    assert_eq!(children.len(), 2);
    let mut engines = Vec::new();
    for (id, route) in &children {
        let e = state.engine_for(route).await.unwrap();
        let rows = e.count_consumer_state_rows(*id, "c1").await.unwrap();
        assert!(
            rows > 0,
            "each child holds this consumer's rows before deletion"
        );
        engines.push((e, *id));
    }

    // Fail ONE child's cleanup scan: the saga must NOT answer 204.
    let ver = consumer_version(addr, "qc16s", "c1").await;
    engines[1].0.fail_next_config_scan();
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc16s/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert!(
        st >= 500,
        "a partial deletion answered {st}: {}",
        String::from_utf8_lossy(&b)
    );
    // The consumer is now Deleting: pulls refuse, recreation refuses.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc16s/consumers/c1:pull",
        &key,
        br#"{"max": 1}"#,
    )
    .await;
    assert_eq!(st, 409, "a Deleting consumer must refuse pulls");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16s/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 409, "recreation must wait for the saga to settle");

    // The retry resumes from Deleting and completes — same token.
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc16s/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));
    for (e, id) in &engines {
        let rows = e.count_consumer_state_rows(*id, "c1").await.unwrap();
        assert_eq!(rows, 0, "a segment kept rows after the completed saga");
    }
    // Recreation allocates a new generation and starts from scratch.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16s/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut got = 0usize;
    for _ in 0..2 {
        let (st, _, b) = preq(
            addr,
            "POST",
            "/v1/streams/qc16s/consumers/c1:pull",
            &key,
            br#"{"max": 4}"#,
        )
        .await;
        assert_eq!(st, 200);
        got += serde_json::from_slice::<serde_json::Value>(&b).unwrap()["messages"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
    }
    assert_eq!(got, 2, "the recreated generation inherited state");
    engine_shutdown(&state).await;
}

/// Round 16, the reviewer's second required scenario: a pull loads the
/// config (generation N), parks BEFORE its segment Receive enqueues;
/// the DELETE completes collection-wide meanwhile. The released pull's
/// old-generation Receive must be REJECTED — no lease row of a deleted
/// generation may land after its deletion finished — and a recreated
/// consumer starts clean.
#[expect(
    clippy::too_many_lines,
    reason = "parked-pull deletion scenario; loading the generation, parking before settlement, deleting underneath and checking the lease verdict form one causal sequence; helper phases would hide which generation the lease was refused against"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_pull_cannot_lease_after_its_generation_was_deleted() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16p",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc16p/records",
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
        "/v1/streams/qc16p/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Park the pull between its config load and its Receive enqueue.
    crate::failpoints::park_pull_before_receive("qc16p");
    let before = crate::failpoints::parked(crate::failpoints::Fp::PullBeforeReceive, "qc16p");
    let a1 = addr;
    let pull = tokio::spawn(async move {
        preq(
            a1,
            "POST",
            "/v1/streams/qc16p/consumers/c1:pull",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"max": 2}"#,
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::PullBeforeReceive, "qc16p") == before {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // The deletion completes collection-wide while the pull is parked.
    let ver = consumer_version(addr, "qc16p", "c1").await;
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc16p/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));

    // Release: the old-generation Receive must refuse.
    crate::failpoints::release_pull_before_receive("qc16p");
    let (st, _, b) = pull.await.unwrap();
    assert_eq!(
        st,
        409,
        "an old-generation pull leased after deletion: {}",
        String::from_utf8_lossy(&b)
    );
    // No row of the dead generation landed.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc16p"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc16p"))
        .await
        .unwrap()
        .unwrap();
    let ro = desc.resolve_segment("");
    let engine = state.engine_for(&ro.shard_route).await.unwrap();
    let rows = engine
        .count_consumer_state_rows(ro.identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows, 0, "the parked pull's lease landed after deletion");

    // Recreation starts a clean generation.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc16p/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qc16p/consumers/c1:pull",
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

/// Round 15 A: a ConfigDelete whose state-row scan FAILS must change
/// nothing — no error may be swallowed into a partial deletion that
/// reports success. The failure is injected at the scan boundary (the
/// deterministic stand-in for a store error mid-enumeration).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_config_scan_aborts_the_delete_untouched() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc15c",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc15c/records",
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
        "/v1/streams/qc15c/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc15c/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc15c"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc15c"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();
    engine.fail_next_config_scan();

    // Driven at the committer: the product DELETE endpoint is disabled
    // for the preview (round 16); the committer ConfigDelete remains
    // the internal deletion machinery whose abort contract this pins.
    let del = engine
        .submit_queue(
            seg.identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await;
    match del {
        Err(m) => assert!(m.contains("state scan failed"), "wrong abort reason: {m}"),
        Ok(o) => panic!("a delete over a failed scan must FAIL, got {o:?}"),
    }
    // The aborted cleanup staged NOTHING: config record intact, every
    // durable row intact. (The generation fence IS up — that is the
    // conservative direction; the saga's retry finishes the job.)
    let (st, _, _) = preq(addr, "GET", "/v1/streams/qc15c/consumers/c1", &key, b"").await;
    assert_eq!(st, 200, "the aborted delete removed the consumer config");
    let rows = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert!(rows > 0, "the aborted delete erased durable rows");
    // Retry (failpoint is one-shot): the cleanup completes.
    engine
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
        .expect("retry cleanup");
    let rows = engine
        .count_consumer_state_rows(seg.identity, "c1")
        .await
        .unwrap();
    assert_eq!(rows, 0, "the retried cleanup left rows behind");
    engine_shutdown(&state).await;
}

/// Two ConfigPut for the same consumer in ONE group: the second must
/// see the first's staged config (overlay), so exactly one reports
/// created and an equal repeat is idempotent — the DB behind an
/// unwritten batch would show both "missing" and mint two creations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_group_config_puts_see_each_other() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc14b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc14b"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc14b"))
        .await
        .unwrap()
        .unwrap();
    let route = desc
        .segment_route_by_id(desc.resolve_segment("").seg_id)
        .unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    let cfg = br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#;
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let a = tokio::spawn(async move {
        preq(addr, "PUT", "/v1/streams/qc14b/consumers/dup", &key, cfg).await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let b = tokio::spawn(async move {
        preq(
            addr,
            "PUT",
            "/v1/streams/qc14b/consumers/dup",
            &[("prisma-encryption-key", PRISMA_KEY)],
            cfg,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    drop(hold);
    let (s1, _, b1) = a.await.unwrap();
    let (s2, _, b2) = b.await.unwrap();
    // Createdness is carried by the STATUS: 201 created, 200
    // idempotent echo. Exactly one creation — never two, which is what
    // the DB behind an unwritten batch would have minted; and never a
    // spurious 409 conflict between two IDENTICAL configs.
    let _ = (&b1, &b2);
    let codes = [s1, s2];
    assert_eq!(
        codes.iter().filter(|&&c| c == 201).count(),
        1,
        "expected exactly one creation, got {s1} and {s2}"
    );
    assert_eq!(
        codes.iter().filter(|&&c| c == 200).count(),
        1,
        "expected exactly one idempotent echo, got {s1} and {s2}"
    );
    engine_shutdown(&state).await;
}
