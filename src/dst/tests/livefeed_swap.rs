//! Livefeed swap.

use super::fixture_failpoints::{FailpointGuard, gap_lock, sweep_lock};
use super::fixture_http::{await_published, http_rig, http_rig_cold_absorb};
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, last_next_cursor, lf_connect, lf_record_and_status, seal_ok,
    split_and_await, wait_parked,
};
use super::fixture_requests::RIG_KEY_B64;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, skey};

/// 6.5: a split landing DURING initial catch-up — every record exactly
/// once, no disconnect (failpoint-parked between attach and start).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_during_initial_catchup_delivers_everything() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfc2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..3u64 {
        hub_append_lf(addr, "lfc2", &format!(r#"{{"i":{i}}}"#)).await;
    }

    crate::failpoints::arm(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfc2");
    let mut sck = lf_connect(addr, "lfc2", "?cursor=beginning").await;
    wait_parked(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfc2", 1).await;

    // The split lands while the subscriber is parked between attach
    // and session start; successor records land after it.
    split_and_await(&state, "lfc2", 0).await;
    for i in 3..5u64 {
        hub_append_lf(addr, "lfc2", &format!(r#"{{"i":{i}}}"#)).await;
    }
    crate::failpoints::release(crate::failpoints::Fp::SseFeedAfterSubscribe, "lfc2");

    let (acc, eof) = hub_sse_collect(&mut sck, 12, |t| {
        t.matches("event: data").count() >= 5 && t.contains("upToDate")
    })
    .await;
    assert!(!eof, "no disconnect across the catch-up split:\n{acc}");
    for i in 0..5u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} exactly once across the split:\n{acc}"
        );
    }
    assert!(
        !acc.contains("\"sealed\":true"),
        "no false terminal control:\n{acc}"
    );
}

/// 6.5: a KEYED lane rides a split in place with its isolation intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_keyed_lane_survives_split() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfk2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sck = lf_connect(addr, "lfk2", "?routingKey=ka").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "parks:\n{acc0}");

    split_and_await(&state, "lfk2", 0).await;

    // One record on the lane's key, one on a foreign key.
    for (k, v) in [("ka", 1), ("kb", 2)] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfk2/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!(r#"{{"k":"{k}","v":{v}}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "append {k}: {st}");
    }
    let (acc, eof) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"v\":1")).await;
    assert!(!eof, "the keyed lane survives the split:\n{acc}");
    assert!(acc.contains("\"v\":1"), "lane record delivered:\n{acc}");
    assert!(
        !acc.contains("\"v\":2"),
        "foreign-key record must not cross the lane:\n{acc}"
    );
    assert!(
        !acc.contains("\"sealed\":true"),
        "no false terminal:\n{acc}"
    );
}

/// 6.4/6.5 fallback: the RAW surface takes the typed disconnect on a
/// split (scalar cursors cannot name segments) — EOF, never a
/// streamClosed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_raw_disconnects_without_terminal_on_split() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfr2", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201);

    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    raw.write_all(
        format!(
            "GET /v1/stream/lfr2?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let (acc0, _) = hub_sse_collect(&mut raw, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "raw parks:\n{acc0}");

    split_and_await(&state, "lfr2", 0).await;
    let (tail, eof) = hub_sse_collect(&mut raw, 12, |_| false).await;
    assert!(
        eof,
        "the raw session takes the typed disconnect; feeds={} tail:\n{tail}",
        state.livefeed.registry().len()
    );
    assert!(
        !tail.contains("\"streamClosed\":true"),
        "a topology fallback is NEVER a terminal control:\n{tail}"
    );
}

/// 6.5: two SEQUENTIAL splits continue in place — the source swaps
/// twice and every record lands exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_two_sequential_splits_continue_in_place() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfs3",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfs3", r#"{"i":0}"#).await;
    let mut sck = lf_connect(addr, "lfs3", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"i\":0"), "backlog:\n{acc0}");

    split_and_await(&state, "lfs3", 0).await;
    hub_append_lf(addr, "lfs3", r#"{"i":1}"#).await;
    let (acc1, eof1) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"i\":1")).await;
    assert!(
        !eof1 && acc1.contains("\"i\":1"),
        "first swap delivers:\n{acc1}"
    );

    // The SECOND split, of the lane's current live segment.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfs3"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfs3"))
        .await
        .unwrap()
        .unwrap();
    let live_seg = desc.resolve_segment("").seg_id;
    split_and_await(&state, "lfs3", live_seg).await;
    hub_append_lf(addr, "lfs3", r#"{"i":2}"#).await;
    let (acc2, eof2) = hub_sse_collect(&mut sck, 12, |t| {
        t.contains("\"i\":2") && t.matches("\"upToDate\":true").count() >= 3
    })
    .await;
    assert!(!eof2, "the second swap must not disconnect either");
    let full = format!("{acc0}{acc1}{acc2}");
    for i in 0..3u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            full.matches(&needle).count(),
            1,
            "record {i} exactly once across two splits:\n{full}"
        );
    }
    assert!(
        !full.contains("\"sealed\":true"),
        "no false terminal across two splits:\n{full}"
    );
}

/// 6.5 lifecycle: delete + recreate is a DISTINCT incarnation — the
/// old feed never leaks new-incarnation records to the old session,
/// no terminal control is forged, and the recreated stream lands on a
/// FRESH feed (storage-hash feed identity). The old session itself
/// lingers until its lease/client closes, exactly like the legacy
/// path: nothing about a delete closes the old segment's handle, and
/// a key-leased session has no deadline pressure — what MUST hold is
/// that the incarnations share nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_delete_recreate_isolates_incarnations() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfd",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut sck = lf_connect(addr, "lfd", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "parks:\n{acc0}");
    assert_eq!(
        state.livefeed.registry().len(),
        1,
        "one feed on the first incarnation"
    );

    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/lfd",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert!(st == 200 || st == 202 || st == 204, "delete {st}");

    // Recreate may briefly race deletion cleanup; the new incarnation
    // serves a fresh feed independently.
    let mut created = false;
    for _ in 0..50 {
        let (st, _, _) = preq(
            addr,
            "PUT",
            "/v1/streams/lfd",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        if st == 201 {
            created = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(created, "recreate");
    hub_append_lf(addr, "lfd", r#"{"n":1}"#).await;

    // The OLD session must NOT see the new incarnation's records (nor
    // any forged terminal): a short window with nothing new arriving.
    let (acc1, _) = hub_sse_collect(&mut sck, 4, |t| t.contains("\"n\":1")).await;
    assert!(
        !acc1.contains("\"n\":1"),
        "no cross-incarnation delivery to the old session:\n{acc1}"
    );
    assert!(
        !acc1.contains("\"sealed\":true"),
        "delete is never a terminal control:\n{acc1}"
    );

    // The recreated stream is a DISTINCT feed (storage-hash identity).
    let mut sck2 = lf_connect(addr, "lfd", "").await;
    let (acc2, _) = hub_sse_collect(&mut sck2, 8, |t| t.contains("\"n\":1")).await;
    assert!(
        acc2.contains("\"n\":1"),
        "the new incarnation serves normally:\n{acc2}"
    );
    assert_eq!(
        state.livefeed.registry().len(),
        2,
        "old and new incarnations are distinct feeds"
    );

    // Old session closes: its feed is evicted; the new one remains.
    drop(sck);
    for _ in 0..100 {
        if state.livefeed.registry().len() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        state.livefeed.registry().len(),
        1,
        "the old feed is evicted"
    );
}

/// Round-4 blocker 1 (red): the cursor emitted after a split must
/// name the successor segment AND the SEGMENT-LOCAL offset — and it
/// must be usable to resume with no gap and no duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_cursor_decodes_to_segment_local_and_resumes() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfcur",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..5u64 {
        hub_append_lf(addr, "lfcur", &format!(r#"{{"i":{i}}}"#)).await;
    }
    let mut sck = lf_connect(addr, "lfcur", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"i\":4"), "backlog:\n{acc0}");

    split_and_await(&state, "lfcur", 0).await;
    // The FIRST child record (child-local offset 0).
    hub_append_lf(addr, "lfcur", r#"{"i":5}"#).await;
    let (acc1, _) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"i\":5")).await;
    assert!(acc1.contains("\"i\":5"), "successor record:\n{acc1}");

    // Decode the cursor at the head: it must be (child, 1) — the
    // segment-local position after the first child record — never the
    // linearized stream offset (which would be 6).
    let tok = last_next_cursor(&acc1);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfcur"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfcur"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    let expected = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: child_seg,
        offset: 1,
    }
    .encode(&desc.project_id, &skey());
    assert_eq!(
        tok, expected,
        "the emitted cursor IS (successor segment, segment-local 1) —          never the linearized offset (6)"
    );

    // RESUME from the emitted cursor: append one more child record,
    // reconnect with the cursor, and require exactly the tail — no
    // gap, no duplicate. (The reconnect lands on the legacy lineage
    // path; the cursor contract is engine-independent.)
    hub_append_lf(addr, "lfcur", r#"{"i":6}"#).await;
    let mut sub2 = lf_connect(addr, "lfcur", &format!("?cursor={tok}")).await;
    let (acc2, _) = hub_sse_collect(&mut sub2, 10, |t| t.contains("\"i\":6")).await;
    assert!(
        acc2.contains("\"i\":6"),
        "resume must deliver the record AFTER the cursor:\n{acc2}"
    );
    assert!(
        !acc2.contains("\"i\":5"),
        "resume must not redeliver the cursor's own record:\n{acc2}"
    );
}

/// Round-4 blocker 2 (red): TWO shared subscribers, one swap — both
/// receive the successor record exactly once, and both head cursors
/// decode to the successor's segment-local position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_shared_subscribers_swap_once_deliver_twice() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfsh",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfsh", r#"{"i":0}"#).await;
    let mut sub1 = lf_connect(addr, "lfsh", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;
    let mut sub2 = lf_connect(addr, "lfsh", "").await;
    let (_, _) = hub_sse_collect(&mut sub2, 8, |t| t.contains("upToDate")).await;

    split_and_await(&state, "lfsh", 0).await;
    hub_append_lf(addr, "lfsh", r#"{"i":1}"#).await;

    let mut curs = Vec::new();
    for (n, s) in [(1, &mut sub1), (2, &mut sub2)] {
        let (acc, eof) = hub_sse_collect(s, 30, |t| lf_record_and_status(t, "\"i\":1")).await;
        if eof {
            let topo = crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                .load(std::sync::atomic::Ordering::Relaxed);
            let lag = crate::sse::auth::sse_stats::FEED_LAG_DISCONNECTS
                .load(std::sync::atomic::Ordering::Relaxed);
            let srcf = crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
                .load(std::sync::atomic::Ordering::Relaxed);
            let retries = crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES
                .load(std::sync::atomic::Ordering::Relaxed);
            let feed_state = state
                .livefeed
                .registry()
                .feed_for_test(&crate::sse::session::feed_key_of(
                    &state
                        .registry
                        .get(&state.deployment.raw_adapter_sref("lfsh"))
                        .await
                        .unwrap()
                        .unwrap(),
                    &Some(String::new()),
                ))
                .map(|f| {
                    format!(
                        "subs={} head={} floor={} gen={} retained={}",
                        f.subscriber_count(),
                        f.head(),
                        f.floor(),
                        f.source_snapshot().generation,
                        f.retained()
                    )
                })
                .unwrap_or_else(|| "feed-gone".to_string());
            panic!(
                "sub{n} disconnected across the split: topo={topo} lag={lag} srcfailed={srcf} retries={retries} feed[{feed_state}]"
            );
        }
        assert_eq!(
            acc.matches("\"i\":1").count(),
            1,
            "sub{n}: successor record exactly once:\n{acc}"
        );
        assert!(
            !acc.contains("\"sealed\":true"),
            "sub{n}: no false terminal:\n{acc}"
        );
        curs.push(last_next_cursor(&acc));
    }

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfsh"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfsh"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    let expected = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: child_seg,
        offset: 1,
    }
    .encode(&desc.project_id, &skey());
    for (n, tok) in curs.iter().enumerate() {
        assert_eq!(
            tok,
            &expected,
            "sub{}: cursor IS (successor segment, segment-local 1)",
            n + 1
        );
    }
}

/// Round-4 blocker 4 (red): the seal-to-publication handoff does NOT
/// depend on the heartbeat — with the successor publication HELD at a
/// failpoint, no false terminal appears; on release, the parked
/// session continues promptly (deadline well below the 15-s
/// heartbeat).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_held_publication_handoff_is_prompt() {
    let _gap = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfhp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfhp", r#"{"i":0}"#).await;
    let mut sck = lf_connect(addr, "lfhp", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "parks:\n{acc0}");

    // Hold the successor publication AFTER the parent seal.
    crate::failpoints::arm_scaler_before_publish("lfhp");
    let guard = FailpointGuard("lfhp".to_string());
    let split = {
        let state = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &state,
                &state.deployment.raw_adapter_sref("lfhp"),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    // Enter the gap: parent sealed, descriptor still pending.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref("lfhp"))
            .await
            .unwrap()
            .unwrap();
        if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "seal gap never entered"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // While the publication is held: no terminal control may appear.
    let (acc1, _) = hub_sse_collect(&mut sck, 3, |_| false).await;
    assert!(
        !acc1.contains("\"sealed\":true"),
        "no terminal while the transition is held:\n{acc1}"
    );

    // Release: the parked session must continue PROMPTLY (well below
    // the 15-s heartbeat that a detached-resume design would need).
    drop(guard);
    await_published(&state, "lfhp").await;
    hub_append_lf(addr, "lfhp", r#"{"i":1}"#).await;
    let t0 = std::time::Instant::now();
    let (acc2, _) = hub_sse_collect(&mut sck, 5, |t| t.contains("\"i\":1")).await;
    assert!(
        acc2.contains("\"i\":1"),
        "successor record arrives promptly after publication:\n{acc2}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(10),
        "delivery must not wait for the heartbeat"
    );
    assert!(
        !acc2.contains("\"sealed\":true"),
        "no false terminal after the handoff:\n{acc2}"
    );
    let _ = split.await;
}

// ==================================================================
// LIVE-FEED Stage 6 round-5 legs: seal-before-refresh drain,
// raw late-attach compatibility, external adoption custody.
// ==================================================================

/// Round-5 blocker 1 (red): split + successor records + seal ALL
/// before the session refreshes — the session must drain the full
/// successor lineage FIRST, then emit exactly one terminal control.
/// With TWO shared subscribers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_split_seal_before_refresh_drains_then_terminates() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfseal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2u64 {
        hub_append_lf(addr, "lfseal", &format!(r#"{{"i":{i}}}"#)).await;
    }
    let mut sub1 = lf_connect(addr, "lfseal", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;
    let mut sub2 = lf_connect(addr, "lfseal", "").await;
    let (_, _) = hub_sse_collect(&mut sub2, 8, |t| t.contains("upToDate")).await;

    // Hold BOTH subscribers' drives, then: split, append successor
    // records, seal the collection — ALL before any refresh. Wait for
    // both sessions to actually PARK at the drive failpoint first
    // (round 6: the intended interleaving must be reached every run).
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "lfseal");
    split_and_await(&state, "lfseal", 0).await;
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "lfseal", 2).await;
    for i in 2..4u64 {
        hub_append_lf(addr, "lfseal", &format!(r#"{{"i":{i}}}"#)).await;
    }
    seal_ok(addr, "lfseal").await;
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "lfseal");

    // Both subscribers: all four records exactly once, then ONE
    // terminal control, then EOF — and the TERMINAL cursor names the
    // final segment-local position.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfseal"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfseal"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    let expected_terminal = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: child_seg,
        offset: 2,
    }
    .encode(&desc.project_id, &skey());
    for (n, s) in [(1, &mut sub1), (2, &mut sub2)] {
        let (acc, eof) = hub_sse_collect(s, 15, |_| false).await;
        assert!(eof, "sub{n}: EOF after the terminal control");
        // The subscribers joined at Now: the SUCCESSOR records are
        // what the drain must not lose.
        for i in 2..4u64 {
            let needle = format!("\"i\":{i}}}");
            assert_eq!(
                acc.matches(&needle).count(),
                1,
                "sub{n}: successor record {i} exactly once (no drain loss):\n{acc}"
            );
        }
        assert_eq!(
            acc.matches("\"sealed\":true").count(),
            1,
            "sub{n}: exactly one terminal control:\n{acc}"
        );
        assert_eq!(
            last_next_cursor(&acc),
            expected_terminal,
            "sub{n}: the terminal cursor IS (successor segment, segment-local 2)"
        );
    }
}

/// Round-5 blocker 3 (red): a RAW request that attaches AFTER the
/// lane's feed swapped takes the typed fallback — immediate EOF,
/// never a scalar control over lineage data, never a terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_raw_late_attach_after_swap_gets_no_lineage_scalars() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfr3", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A PRODUCT subscriber holds the default lane's feed.
    let mut prod = lf_connect(addr, "lfr3", "").await;
    let (_, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("upToDate")).await;

    // The raw request parks BEFORE the lease gate (thus before attach).
    crate::failpoints::arm(crate::failpoints::Fp::SseBeforeLeaseGate, "lfr3");
    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    raw.write_all(
        format!(
            "GET /v1/stream/lfr3?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    wait_parked(crate::failpoints::Fp::SseBeforeLeaseGate, "lfr3", 1).await;

    // Split + successor append: the PRODUCT session swaps the feed to
    // a lineage source (generation 1).
    split_and_await(&state, "lfr3", 0).await;
    hub_append_lf(addr, "lfr3", r#"{"r":1}"#).await;
    let (p1, _) = hub_sse_collect(&mut prod, 10, |t| t.contains("\"r\":1")).await;
    assert!(p1.contains("\"r\":1"), "product rode the swap:\n{p1}");

    // Resume the raw request: it must NOT emit the successor record
    // with scalar controls — typed immediate disconnect only.
    crate::failpoints::release(crate::failpoints::Fp::SseBeforeLeaseGate, "lfr3");
    let (acc, eof) = hub_sse_collect(&mut raw, 10, |_| false).await;
    assert!(
        eof,
        "the late-attaching raw request is cut off, not served lineage"
    );
    assert!(
        !acc.contains("\"streamClosed\":true"),
        "the fallback is never a terminal control:\n{acc}"
    );
    assert!(
        !acc.contains("\"r\":1"),
        "no successor data with scalar cursors:\n{acc}"
    );
}

/// Round-5/6 blocker 2 (red): the child engine LiveFeed opens for a
/// swapped lineage carries the EXTERNAL adoption stamp FROM THE
/// LIVEFEED BUILD ITSELF — no customer request ever touches the empty
/// successor (round-6 correction: the old version appended first,
/// which itself stamped the engine and masked the implementation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_swap_externally_adopts_child_engines() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfad",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfad", r#"{"i":0}"#).await;
    let mut sck = lf_connect(addr, "lfad", "").await;
    let (_, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;

    split_and_await(&state, "lfad", 0).await;

    // Wait for LiveFeed to observe the sealed parent and build/install
    // the EMPTY child lineage (feed generation 1). No customer request
    // has touched the child.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lfad"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfad"))
        .await
        .unwrap()
        .unwrap();
    let fkey = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let g = state
            .livefeed
            .registry()
            .feed_for_test(&fkey)
            .map(|f| f.source_snapshot().generation);
        if g == Some(1) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the feed never swapped to the lineage source (gen={g:?})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // The child engine must carry the external adoption stamp from the
    // LiveFeed build (last_external_seq > 0), and sweeps must neither
    // close it nor install custody.
    let child_route = desc
        .segment_route_by_id(desc.resolve_segment("").seg_id)
        .unwrap();
    let prefix = state.shards.prefix_for(&child_route);
    let engine = state
        .shards
        .open(&prefix)
        .expect("the child engine is resident after the swap");
    assert!(
        engine
            .last_external_seq
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "the LiveFeed build itself must stamp external adoption — no customer request touched the child"
    );
    for _ in 0..6 {
        crate::billing::sweep_owned_outboxes(&state).await;
    }
    assert!(
        state.shards.is_open(&prefix),
        "an engine serving a customer LiveFeed must never be sweep-closed"
    );
    assert_eq!(
        engine
            .sweep_custody
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "custody must never be installed over a customer LiveFeed engine"
    );

    // And delivery through the swapped source still works.
    hub_append_lf(addr, "lfad", r#"{"i":1}"#).await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"i\":1")).await;
    assert!(acc.contains("\"i\":1"), "the swap delivered:\n{acc}");
}

/// Round-6 raw gate second level (red): the swap lands BETWEEN the
/// compatibility peek and the atomic attach — the atomically captured
/// join generation (not the peek) is what must refuse the raw session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_raw_swap_between_peek_and_attach_is_refused() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lfr5", &[ct], br#"[{"r":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A PRODUCT subscriber holds the default lane's feed (generation 0,
    // so the peek would PASS for a raw request).
    let mut prod = lf_connect(addr, "lfr5", "").await;
    let (_, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("upToDate")).await;

    // The raw request parks AFTER the peek, BEFORE the attach.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeSubscribe, "lfr5");
    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start_tok = crate::offsets::encode_ep(0, crate::offsets::Offset::START);
    raw.write_all(
        format!(
            "GET /v1/stream/lfr5?live=sse&offset={start_tok} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    wait_parked(crate::failpoints::Fp::SseFeedBeforeSubscribe, "lfr5", 1).await;

    // Split + successor append: the PRODUCT session swaps the feed to
    // generation 1 in the window between peek and attach.
    split_and_await(&state, "lfr5", 0).await;
    hub_append_lf(addr, "lfr5", r#"{"r":1}"#).await;
    let (p1, _) = hub_sse_collect(&mut prod, 10, |t| t.contains("\"r\":1")).await;
    assert!(p1.contains("\"r\":1"), "product rode the swap:\n{p1}");

    // Resume the raw request: the ATOMIC attach captures generation 1
    // and the raw session is refused — immediate EOF, never scalar
    // lineage data, never a terminal control.
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeSubscribe, "lfr5");
    let (acc, eof) = hub_sse_collect(&mut raw, 10, |_| false).await;
    assert!(
        eof,
        "the raw request attaching to a swapped feed is refused"
    );
    assert!(
        !acc.contains("\"streamClosed\":true"),
        "the refusal is never a terminal control:\n{acc}"
    );
    assert!(
        !acc.contains("\"r\":1"),
        "no successor data with scalar cursors:\n{acc}"
    );
}

/// Round-6 refresh regression (red): a transition completed EXTERNALLY
/// must be picked up by a later refresh with a stale descriptor and
/// signature — the unconditional re-read installs the lineage (the
/// resume boolean is never treated as evidence of no change).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_refresh_installs_after_external_completion() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfx5",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfx5", r#"{"i":0}"#).await;

    // Capture the PRE-SPLIT descriptor, then complete the split
    // EXTERNALLY (no LiveFeed involvement).
    let old_desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lfx5"))
        .await
        .unwrap()
        .unwrap();
    split_and_await(&state, "lfx5", 0).await;

    // Refresh with the stale descriptor + old span signature: the
    // re-read must install the longer compatible lineage.
    let epoch = old_desc.epoch_bytes().unwrap();
    let outcome = crate::sse::source::refresh_transition(
        &state.read_service(),
        &old_desc,
        &skey(),
        &epoch,
        &Some(String::new()),
        &[(0, 0, None)],
    )
    .await
    .unwrap();
    match outcome {
        crate::sse::feed::SourceTransition::NewSource(src) => {
            let sig = src.span_sig();
            assert_eq!(sig.len(), 2, "the refreshed lineage has both spans");
            assert_eq!(sig[0], (0, 0, Some(1)), "the parent gained its cap");
            assert_eq!(sig[1].1, 1, "the successor starts at the cap");
        }
        _ => panic!("refresh after external completion must install the lineage"),
    }
}
