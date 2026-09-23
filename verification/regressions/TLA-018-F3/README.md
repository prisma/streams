# TLA-018-F3: a stale applied cursor is accepted once the new owner's tail passes it

**Classification:** production defect (model counterexample, reproduced on the
real code). **Open:** the fix needs an owner decision on the cursor format.
**Model check:** `TLA-018/known-defect-stale-applied-cursor`
(`verification/tla/history/MC_ReadCompose_kd_stale_applied_cursor.cfg`), which
must keep violating `ExactDurablePrefix` until the defect is fixed.
**Code:** `ReadService::execute_read`, `src/application/read_request.rs:291-293`:

```rust
if command.visibility == Deliver::Applied && start > end {
    return Err(ReadFailure::CursorBeyondTail);
}
```

`end` is the current owner's end. A product cursor (`KIND_KEY_V2`,
`src/product_cursor.rs`) carries no owner incarnation and no durable frontier,
so the guard is the only check. It fires only while the new owner's tail is
still below the stale cursor.

## Minimized schedule

The model's counterexample (`deliver=applied`, unfiltered, one ownership move;
`verification/tla/history/evidence/TLA-018_applied_stale_cursor.trace.txt`):

1. The old owner has record 0 durable and appends record 1, applied only.
2. A page delivers record 0. The session cursor and the durable cursor are 1.
3. The next page starts on the old owner.
4. Ownership moves, and the applied record 1 is lost with the old memtable.
5. The new owner appends a different record 1 and a record 2, and the new
   record 1 becomes durable.
6. The page on the old owner completes from its frozen view and delivers the
   lost record 1 as pending. The session cursor is 2; the durable cursor
   stays 1.
7. The client continues from the session cursor 2. The new owner's end is 3,
   not below it, so the cursor is accepted.
8. The page delivers record 2, and the durable cursor becomes 2. Offset 1 is
   now inside the promised prefix, but the client holds the lost record, not
   the durable one.

The real-code test below reaches the same state with one page that delivers
records 0 and 1 before the crash.

## Real-code reproduction

The test below extends `a_stale_applied_cursor_is_refused_after_crash_restart`
(`src/dst/tests/reads_applied.rs`). That test presents the stale cursor before
any new append, so it never reaches the race. The new test appends two records
on the restarted server first. It is recorded here and is not in `src/`: it
fails until the defect is fixed. It was run on a revision before the TLA-018-F1
fix. The guard it exercises is unchanged at `ab73296`.

```rust
/// Phases 1 and 2 of the stale-cursor scenario: one durable record, a
/// crash-consistent snapshot of the store, then an applied page over a
/// provisional second record on the live store. Returns the snapshot,
/// which never saw that record, with the page's session and durable
/// cursors.
async fn applied_cursor_over_a_lost_suffix(
    store: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, String, String) {
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    let (state1, addr1) = http_rig(store.clone()).await;
    let format = br#"{"format":{"kind":"json"}}"#;
    let (st, _, _) = preq(addr1, "PUT", "/v1/streams/sp", &credentials, format).await;
    assert_eq!(st, 201);
    let r0 = br#"{"n":0}"#;
    let (st, _, _) = preq(addr1, "POST", "/v1/streams/sp/records", &credentials, r0).await;
    assert!(st == 200 || st == 202, "append r0: {st}");
    engine_shutdown(&state1).await;
    let snapshot = mem();
    copy_store(&store, &snapshot).await;

    let (state2, addr2) = http_rig_at(store, RigRuntime::incarnation(1)).await;
    let sref = state2.deployment.raw_adapter_sref("sp");
    state2.registry.invalidate(&sref);
    let desc = state2.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state2.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    let guard = engine.test_hold_dispatch().await;
    // The provisional append and the applied read overlap in one task:
    // the read runs while dispatch is held, then releases it.
    let r1 = br#"{"n":1}"#;
    let append = preq(addr2, "POST", "/v1/streams/sp/records", &credentials, r1);
    let read = async move {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while handle.state.lock().unwrap().applied.next < 2 {
            assert!(std::time::Instant::now() < deadline, "r1 never applied");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let path = "/v1/streams/sp/records?deliver=applied";
        let page = preq(addr2, "GET", path, &credentials, b"").await;
        drop(guard);
        page
    };
    let ((acked, _, _), (st, h, b)) = futures_util::future::join(append, read).await;
    assert!(
        acked == 200 || acked == 202,
        "r1 acked after release: {acked}"
    );
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v, serde_json::json!([{"n":0},{"n":1}]), "applied page");
    assert_eq!(h.get("prisma-pending-from").map(String::as_str), Some("1"));
    engine_shutdown(&state2).await;
    (
        snapshot,
        h.get("prisma-next-cursor").expect("next").clone(),
        h.get("prisma-durable-cursor").expect("durable").clone(),
    )
}

/// **TLA-018-F3: a stale applied cursor stays refused after the new
/// owner's tail passes it.** The session cursor (2) was minted over
/// record 1, which the crash lost. The restarted server appends a
/// different record 1 and a record 2, then sees the stale cursor. It
/// must refuse it with 409 `cursor_beyond_tail`: accepting it would
/// serve only record 2 and advance the durable cursor over offset 1,
/// whose durable record the client never received. The durable cursor
/// from the same page resumes and delivers both replacements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_applied_cursor_is_refused_after_the_new_tail_passes_it() {
    let (snapshot, next_cur, durable_cur) = applied_cursor_over_a_lost_suffix(mem()).await;
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    let (state3, addr3) = http_rig_at(snapshot, RigRuntime::incarnation(2)).await;
    for body in [br#"{"n":10}"#, br#"{"n":20}"#] {
        let (st, _, _) = preq(addr3, "POST", "/v1/streams/sp/records", &credentials, body).await;
        assert!(st == 200 || st == 202, "replacement append: {st}");
    }
    let desc = state3
        .registry
        .get(&state3.deployment.raw_adapter_sref("sp"))
        .await
        .unwrap()
        .unwrap();
    let offset = |cursor: Option<&String>| {
        cursor.map(|cursor| {
            crate::product_cursor::KeyCursor::decode(
                cursor,
                &desc.project_id,
                &skey(),
                &desc.epoch(),
                &crate::crypto::RoutingKeyHash::of("").0,
            )
            .map(|cursor| cursor.offset)
        })
    };
    let path = format!("/v1/streams/sp/records?cursor={next_cur}&deliver=applied");
    let (st, h, b) = preq(addr3, "GET", &path, &credentials, b"").await;
    assert_eq!(
        st,
        409,
        "stale cursor at {:?} accepted: body {}, durable cursor {:?}, next cursor {:?}",
        offset(Some(&next_cur)),
        String::from_utf8_lossy(&b),
        offset(h.get("prisma-durable-cursor")),
        offset(h.get("prisma-next-cursor")),
    );
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "cursor_beyond_tail");
    let path = format!("/v1/streams/sp/records?cursor={durable_cur}&deliver=applied");
    let (st, _, b) = preq(addr3, "GET", &path, &credentials, b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v, serde_json::json!([{"n":10},{"n":20}]), "durable resume");
    engine_shutdown(&state3).await;
}
```

Observed output (`cargo test --lib`, test profile, unfixed code; excerpt of a
run that selected four `reads_applied` tests):

```text
test dst::dst_tests::reads_applied::a_stale_applied_cursor_is_refused_after_crash_restart ... ok
test dst::dst_tests::reads_applied::a_stale_applied_cursor_is_refused_after_the_new_tail_passes_it ... FAILED

thread 'dst::dst_tests::reads_applied::a_stale_applied_cursor_is_refused_after_the_new_tail_passes_it' panicked at src/dst/tests/reads_applied.rs:656:5:
assertion `left == right` failed: stale cursor at Some(Ok(2)) accepted: body [{"n":20}], durable cursor Some(Ok(3)), next cursor Some(Ok(3))
  left: 200
 right: 409
```

The restarted server served only `{"n":20}` and moved the durable cursor to 3.
The client never received the durable record `{"n":10}` at offset 1, and it
still holds the lost `{"n":1}` for that offset.

## Decision needed

The proposed design is a new product cursor kind, `KIND_KEY_V3`, that binds an
applied session cursor to the owner incarnation that minted it or to the
durable frontier at minting. A different incarnation would then refuse the
cursor, or rewind it to the durable frontier, even after its tail has passed
the cursor. This changes a persisted, client-visible format, so it needs an
owner decision on:

- what the cursor carries (owner incarnation, durable frontier, or both);
- whether a stale cursor is refused (`409 cursor_beyond_tail`) or rewound to
  the durable frontier;
- how `KIND_KEY_V2` cursors already held by clients are treated after the
  upgrade (roadmap TLA-021 and TLA-044).

When the fix lands, the test above goes into `src/dst/tests/reads_applied.rs`,
the model's cursor check changes to match, the known-defect check becomes part
of the `baseline-applied-unfiltered-expanded` baseline, and the pre-fix
acceptance becomes a negative control.
