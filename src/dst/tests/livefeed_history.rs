//! Livefeed history.

use super::fixture_http::{http_rig, http_rig_owner, http_rig_owner_at};
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, last_next_cursor, lf_connect, lf_record_and_status,
    read_billing_sum, seal_ok, split_and_await,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use std::sync::Arc;

/// Round-10e review (red): a FORK's materialized partial record is a
/// customer record the child persists — it must satisfy the
/// per-record ceiling BEFORE any fork lifecycle work becomes durable.
/// The source record predates the ceiling ("created under a different
/// profile"); the oversized partial must refuse the fork with NO
/// externally visible child, and a compliant partial must still work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_materialized_partial_respects_the_record_ceiling() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "text/plain")];
    // The source record is created BEFORE the ceiling exists.
    let big = "x".repeat(8 * 1024);
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fbig", &ct, big.as_bytes()).await;
    assert!(st == 200 || st == 201, "source create {st}");
    state.admission.set_record_ceiling(4096);
    // An oversized materialized partial (6000 > 4096): refused, and
    // the child never becomes externally visible.
    let (st, _, body) = hreq(
        addr,
        "PUT",
        "/v1/stream/fbig-child",
        &[
            ("content-type", "text/plain"),
            ("stream-forked-from", "/v1/stream/fbig"),
            ("stream-fork-offset", "0000000000000000_0000000000000000"),
            ("stream-fork-sub-offset", "6000"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 413, "{}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("record_too_large"),
        "typed refusal:\n{}",
        String::from_utf8_lossy(&body)
    );
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fbig-child", &[], b"").await;
    assert_eq!(st, 404, "no child may survive the refused fork");
    // A compliant partial still forks.
    let (st, _, body) = hreq(
        addr,
        "PUT",
        "/v1/stream/fbig-child2",
        &[
            ("content-type", "text/plain"),
            ("stream-forked-from", "/v1/stream/fbig"),
            ("stream-fork-offset", "0000000000000000_0000000000000000"),
            ("stream-fork-sub-offset", "1000"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&body));
}

/// Round-10 two-instance groundwork (red): a subscriber landing on
/// the CHILD's owner streams the WHOLE lineage — the sealed
/// predecessor is read from ITS owner over the bounded internal
/// segment-scan surface, the live child locally. Before the
/// SpanReader split, LineageSource::build refused ANY lineage with a
/// foreign span (WrongOwner), so the child's owner could not serve a
/// split stream's history at all. The live tail must still be local:
/// remote ownership of the TAIL stays a typed WrongOwner cutoff.
#[expect(
    clippy::too_many_lines,
    reason = "remote lineage scenario; two instances, the child's owner and the whole-lineage stream form one causal sequence; helper phases would hide which owner served the sealed predecessor"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_remote_sealed_predecessor_streams_through_owner() {
    let store = mem();
    let (state_a, addr_a) = http_rig_owner(store.clone(), "inst-a").await;
    let (state_b, addr_b) = http_rig_owner_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    // History on A, then a split (parent seg0 seals, the child opens).
    let (st, _, _) = preq(
        addr_a,
        "PUT",
        "/v1/streams/xown",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr_a, "xown", r#"{"h":0}"#).await;
    hub_append_lf(addr_a, "xown", r#"{"h":1}"#).await;
    let sref = state_a.deployment.raw_adapter_sref("xown");
    let _ = crate::scaler3::execute_split(&state_a, &sref, 0, 0x8000_0000_0000_0000).await;
    for _ in 0..200 {
        state_a.registry.invalidate(&sref);
        let d = state_a.registry.get(&sref).await.unwrap().unwrap();
        if d.segments
            .as_ref()
            .is_some_and(|m| m.pending.is_none() && m.segments.len() > 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    state_b.registry.invalidate(&sref);
    let desc = state_b.registry.get(&sref).await.unwrap().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    // The parent's shard belongs to A (an override on B + A's peer
    // URL); the child's shard differs, so B serves it itself. The
    // fixed stream name makes both routes deterministic.
    let p_parent = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(0).unwrap());
    let p_child = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(child_seg).unwrap());
    assert_ne!(
        p_parent, p_child,
        "pick a stream name whose parent/child routes differ"
    );
    // B's view of the fleet: both instances active; the parent's
    // shard is pinned to A, every other shard to B (explicit
    // overrides beat the rendezvous pick for determinism).
    state_b
        .ownership
        .set_ring_active(vec!["inst-a".to_string(), "inst-b".to_string()]);
    {
        for p in state_b.shards.prefixes().to_vec() {
            let owner = if p == p_parent { "inst-a" } else { "inst-b" };
            state_b.ownership.set_override(&p, owner);
        }
    }
    state_b.peer.set_peer("inst-a", &format!("http://{addr_a}"));
    // The successor record lands through B (B owns the child's shard;
    // its open fences A's copy of that shard).
    hub_append_lf(addr_b, "xown", r#"{"h":2}"#).await;

    // THE contract: a subscriber on B streams the whole lineage —
    // remote sealed parent, local live child — and parks live ON THE
    // LIVEFEED ENGINE. (The legacy lineage streamer also relays
    // remote reads, so delivery alone cannot distinguish the engines;
    // legacy dies at Stage 7 deletion, so the feed itself must carry
    // this session.)
    let mut sck = lf_connect(addr_b, "xown", "?cursor=beginning").await;
    let (acc, eof) = hub_sse_collect(&mut sck, 15, |t| lf_record_and_status(t, "\"h\":2")).await;
    assert!(
        !eof,
        "the child's owner must serve the split stream in place:\n{acc}"
    );
    assert!(
        state_b.livefeed.registry().len() >= 1,
        "the session must ride the LIVEFEED engine, not the legacy lineage fallback"
    );
    for i in 0..3u64 {
        let needle = format!("\"h\":{i}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "{needle} exactly once across the ownership boundary:\n{acc}"
        );
    }
    let last_data = acc.rfind("\"h\":2").expect("frontier record");
    let utd = acc.find("\"upToDate\":true").expect("status");
    assert!(
        utd > last_data,
        "honest upToDate after the full lineage:\n{acc}"
    );
    // Liveness on the local child after the remote catch-up.
    hub_append_lf(addr_b, "xown", r#"{"h":3}"#).await;
    let (tail, eof2) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"h\":3")).await;
    assert!(!eof2, "still live:\n{tail}");
    assert_eq!(tail.matches("\"h\":3").count(), 1);
    // Internal pages are UNBILLED on the serving owner: A relayed the
    // sealed parent's records but its read accumulator must not have
    // metered them (customer metering happens once, at B's external
    // body yield).
    assert_eq!(
        read_billing_sum(&state_a),
        (0, 0),
        "internal segment-scan pages must never meter customer reads"
    );

    // PEER OUTAGE: a fresh subscriber whose remote predecessor pages
    // cannot be fetched STALLS with bounded retries — no data, no
    // false terminal, no EOF — and RECOVERS in place the moment the
    // peer returns. The first subscriber must be gone first: its
    // feed's retained ring covers the whole lineage and would serve a
    // late attacher without touching the peer.
    drop(sck);
    for _ in 0..200 {
        if state_b.livefeed.registry().len() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        state_b.livefeed.registry().len(),
        0,
        "the feed tears down at zero"
    );
    state_b
        .peer
        .set_peer("inst-a", &"http://127.0.0.1:9".to_string());
    let mut down = lf_connect(addr_b, "xown", "?cursor=beginning").await;
    let (held, eof3) = hub_sse_collect(&mut down, 2, |_| false).await;
    assert!(!eof3, "a peer outage is a stall, not a disconnect:\n{held}");
    assert!(
        !held.contains("event: data") && !held.contains("\"sealed\":true"),
        "no data and no false terminal while the peer is down:\n{held}"
    );
    state_b.peer.set_peer("inst-a", &format!("http://{addr_a}"));
    let (rec, eof4) = hub_sse_collect(&mut down, 15, |t| lf_record_and_status(t, "\"h\":3")).await;
    assert!(!eof4, "recovery completes in place:\n{rec}");
    for i in 0..4u64 {
        let needle = format!("\"h\":{i}");
        assert_eq!(
            rec.matches(&needle).count(),
            1,
            "{needle} exactly once after peer recovery:\n{rec}"
        );
    }
}

/// Round-11.3: KEYED live reads (raw-key records:sse with routingKey)
/// ride LiveFeed across a split IN PLACE — product cursor vocabulary,
/// exactly-once delivery, no false terminal. (The singular raw SSE
/// surface is keyless by design; keyed live subscriptions live here.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_rawkey_records_sse_survives_split() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfrk",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let append_ka = |i: u64| async move {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfrk/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "ka"),
            ],
            format!(r#"{{"r":{i}}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "append {i}: {st}");
    };
    append_ka(0).await;
    let mut sck = lf_connect(addr, "lfrk", "?routingKey=ka").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"r\":0"), "keyed backlog:\n{acc0}");

    split_and_await(&state, "lfrk", 0).await;
    append_ka(1).await;
    let (acc1, eof1) = hub_sse_collect(&mut sck, 10, |t| lf_record_and_status(t, "\"r\":1")).await;
    assert!(!eof1, "keyed live rides the split in place:\n{acc1}");
    assert!(
        !acc1.contains("\"sealed\":true"),
        "no false terminal:\n{acc1}"
    );
    assert_eq!(acc1.matches("\"r\":1").count(), 1, "exactly once:\n{acc1}");
    // Resume from the emitted cursor: exactly the tail.
    let tok = last_next_cursor(&acc1);
    append_ka(2).await;
    let mut r2 = lf_connect(addr, "lfrk", &format!("?routingKey=ka&cursor={tok}")).await;
    let (a2, eof2) = hub_sse_collect(&mut r2, 10, |t| lf_record_and_status(t, "\"r\":2")).await;
    assert!(!eof2);
    assert!(
        a2.contains("\"r\":2") && !a2.contains("\"r\":1") && !a2.contains("\"r\":0"),
        "resume from the split cursor: exactly the tail:\n{a2}"
    );
}

// ==================================================================
// LIVE-FEED Stage 7A legs: connect-time product lineage. A stream
// ALREADY split when the request arrives builds a LineageSource at
// connect and rides the same engine — beginning, now, signed cursors
// in every span position, invalid cursors, and sealed streams.
// ==================================================================

/// Create a stream, append records, split, append more — return the
/// post-split descriptor.
async fn lf7_split_stream(
    addr: std::net::SocketAddr,
    state: &Arc<crate::http::AppState>,
    name: &str,
    pre: u64,
    post: u64,
) -> crate::registry::StreamDesc {
    let (st, _, _) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..pre {
        hub_append_lf(addr, name, &format!(r#"{{"i":{i}}}"#)).await;
    }
    split_and_await(state, name, 0).await;
    for i in pre..(pre + post) {
        hub_append_lf(addr, name, &format!(r#"{{"i":{i}}}"#)).await;
    }
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref(name));
    state
        .registry
        .get(&state.deployment.raw_adapter_sref(name))
        .await
        .unwrap()
        .unwrap()
}

/// Connect at `cursor=beginning` on an ALREADY-split stream: the full
/// lineage (predecessor + successor) drains through LiveFeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_already_split_beginning_drains_lineage() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let _ = lf7_split_stream(addr, &state, "lf7b", 3, 2).await;

    let mut sck = lf_connect(addr, "lf7b", "?cursor=beginning").await;
    let (acc, _) = hub_sse_collect(&mut sck, 12, |t| {
        t.matches("event: data").count() >= 5 && t.contains("upToDate")
    })
    .await;
    for i in 0..5u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} exactly once through connect-time lineage:\n{acc}"
        );
    }
    assert!(acc.contains("upToDate"), "reached the head:\n{acc}");

    // And live records keep flowing on the same connection.
    hub_append_lf(addr, "lf7b", r#"{"i":5}"#).await;
    let (acc2, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"i\":5")).await;
    assert!(acc2.contains("\"i\":5"), "live records flow:\n{acc2}");
}

/// Connect at `cursor=now` on an already-split stream: only records
/// written after the connect arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_already_split_now() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let _ = lf7_split_stream(addr, &state, "lf7n", 3, 2).await;

    let mut sck = lf_connect(addr, "lf7n", "?cursor=now").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(
        !acc0.contains("event: data"),
        "now starts at the tail:\n{acc0}"
    );
    hub_append_lf(addr, "lf7n", r#"{"i":5}"#).await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"i\":5")).await;
    assert!(acc.contains("\"i\":5"), "only post-connect records:\n{acc}");
    assert!(!acc.contains("\"i\":4"), "no history:\n{acc}");
}

/// Connect with a SIGNED cursor inside a sealed predecessor: the
/// predecessor remainder plus the successor tail arrive exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_already_split_cursor_in_sealed_predecessor() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let desc = lf7_split_stream(addr, &state, "lf7p", 3, 2).await;
    let epoch = desc.epoch_bytes().unwrap();

    // Cursor: consumed through predecessor record 1 (local offset 2).
    let tok = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: 0,
        offset: 2,
    }
    .encode(&desc.project_id, &skey());
    let mut sck = lf_connect(addr, "lf7p", &format!("?cursor={tok}")).await;
    let (acc, _) = hub_sse_collect(&mut sck, 12, |t| {
        t.matches("event: data").count() >= 3 && t.contains("upToDate")
    })
    .await;
    for i in [2u64, 3, 4] {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} exactly once from the predecessor cursor:\n{acc}"
        );
    }
    assert!(
        !acc.contains("\"i\":0") && !acc.contains("\"i\":1"),
        "records at/below the cursor are not redelivered:\n{acc}"
    );
}

/// Connect with a signed cursor in the live tail and exactly at a span
/// boundary; a cursor beyond a sealed cap is a 400; an unknown segment
/// is a 400.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_already_split_cursor_positions() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let desc = lf7_split_stream(addr, &state, "lf7c", 3, 2).await;
    let epoch = desc.epoch_bytes().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    let mk = |seg_id: u32, offset: u64| {
        crate::product_cursor::KeyCursor {
            epoch,
            key_hash: crate::crypto::stream_hash(""),
            seg_id,
            offset,
        }
        .encode(&desc.project_id, &skey())
    };

    // In the live tail (child, 0): only successor records.
    let mut s1 = lf_connect(addr, "lf7c", &format!("?cursor={}", mk(child_seg, 0))).await;
    let (a1, _) = hub_sse_collect(&mut s1, 10, |t| t.matches("event: data").count() >= 2).await;
    assert!(
        a1.contains("\"i\":3") && a1.contains("\"i\":4"),
        "live tail:\n{a1}"
    );
    assert!(
        !a1.contains("\"i\":2"),
        "no predecessor records from a tail cursor:\n{a1}"
    );

    // Exactly at the span boundary (seg 0, cap): continues into the
    // successor with no gap and no duplicate.
    let mut s2 = lf_connect(addr, "lf7c", &format!("?cursor={}", mk(0, 3))).await;
    let (a2, _) = hub_sse_collect(&mut s2, 10, |t| t.matches("event: data").count() >= 2).await;
    assert!(
        a2.contains("\"i\":3") && a2.contains("\"i\":4"),
        "boundary resumes into the successor:\n{a2}"
    );
    assert!(
        !a2.contains("\"i\":2"),
        "no duplicate at the boundary:\n{a2}"
    );

    // Beyond the sealed cap: 400.
    let (st, _, _) = preq(
        addr,
        "GET",
        &format!("/v1/streams/lf7c/records:sse?cursor={}", mk(0, 99)),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400, "beyond a sealed cap is invalid_cursor");

    // Unknown segment: 400.
    let (st, _, _) = preq(
        addr,
        "GET",
        &format!("/v1/streams/lf7c/records:sse?cursor={}", mk(77, 0)),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 400, "an unknown segment is invalid_cursor");
}

/// Connect at beginning on a SEALED split stream: full lineage, then
/// exactly one terminal control, then EOF.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_already_split_sealed_stream() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let _ = lf7_split_stream(addr, &state, "lf7s", 3, 2).await;
    seal_ok(addr, "lf7s").await;

    let mut sck = lf_connect(addr, "lf7s", "?cursor=beginning").await;
    let (acc, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert!(eof, "EOF after the terminal");
    for i in 0..5u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} exactly once on the sealed stream:\n{acc}"
        );
    }
    assert_eq!(
        acc.matches("\"sealed\":true").count(),
        1,
        "exactly one terminal control:\n{acc}"
    );
}

/// Two sequential splits before connect: the three-span lineage
/// drains in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_connect_after_two_sequential_splits() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lf7t",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lf7t", r#"{"i":0}"#).await;
    split_and_await(&state, "lf7t", 0).await;
    hub_append_lf(addr, "lf7t", r#"{"i":1}"#).await;
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lf7t"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lf7t"))
        .await
        .unwrap()
        .unwrap();
    let live_seg = desc.resolve_segment("").seg_id;
    split_and_await(&state, "lf7t", live_seg).await;
    hub_append_lf(addr, "lf7t", r#"{"i":2}"#).await;

    let mut sck = lf_connect(addr, "lf7t", "?cursor=beginning").await;
    let (acc, _) = hub_sse_collect(&mut sck, 12, |t| {
        t.matches("event: data").count() >= 3 && t.contains("upToDate")
    })
    .await;
    for i in 0..3u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            acc.matches(&needle).count(),
            1,
            "record {i} exactly once across two splits:\n{acc}"
        );
    }
}

/// Merge continuation: split, append on the lane's child, MERGE the
/// children, append on the merged successor — the same connection
/// continues in place, cursors decode, no false terminal.
#[expect(
    clippy::too_many_lines,
    reason = "merge continuation scenario; splitting, appending on a child, merging and appending on the successor while one subscriber follows form one causal sequence; helper phases would hide which topology step broke continuity"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_merge_continuation_in_place() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lf7m",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lf7m", r#"{"i":0}"#).await;
    let mut sck = lf_connect(addr, "lf7m", "").await;
    let (acc0, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("\"i\":0"), "backlog:\n{acc0}");

    // Split, then one record on the lane's child.
    split_and_await(&state, "lf7m", 0).await;
    hub_append_lf(addr, "lf7m", r#"{"i":1}"#).await;
    let (acc1, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"i\":1")).await;
    assert!(acc1.contains("\"i\":1"), "split record:\n{acc1}");

    // MERGE the two children; the lane's chain becomes
    // [seg0 sealed, child sealed, merged live].
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lf7m"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lf7m"))
        .await
        .unwrap()
        .unwrap();
    let mut children: Vec<u32> = desc
        .segments
        .as_ref()
        .unwrap()
        .segments
        .iter()
        .filter(|s| s.is_live())
        .map(|s| s.seg_id)
        .collect();
    children.sort_unstable();
    assert_eq!(children.len(), 2, "a split leaves two live children");
    // The return value is deliberately not asserted (same race as
    // execute_split: a livefeed session observing the pending merge
    // spawns the same resumable resume and may win the completion).
    let _ = crate::scaler3::execute_merge(
        &state,
        &state.deployment.raw_adapter_sref("lf7m"),
        children[0],
        children[1],
    )
    .await;
    // Await the merged topology.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref("lf7m"));
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref("lf7m"))
            .await
            .unwrap()
            .unwrap();
        let done = d.segments.as_ref().is_some_and(|m| {
            m.pending.is_none() && m.segments.iter().filter(|s| s.is_live()).count() == 1
        });
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "merge did not complete"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // The same connection continues: the record on the merged segment
    // arrives, no disconnect, no false terminal.
    hub_append_lf(addr, "lf7m", r#"{"i":2}"#).await;
    let (acc2, eof) = hub_sse_collect(&mut sck, 12, |t| {
        t.contains("\"i\":2") && t.matches("\"upToDate\":true").count() >= 3
    })
    .await;
    assert!(!eof, "a merge must not disconnect the session");
    let full = format!("{acc0}{acc1}{acc2}");
    for i in 0..3u64 {
        let needle = format!("\"i\":{i}}}");
        assert_eq!(
            full.matches(&needle).count(),
            1,
            "record {i} exactly once across split+merge:\n{full}"
        );
    }
    assert!(
        !full.contains("\"sealed\":true"),
        "no false terminal across split+merge:\n{full}"
    );

    // The head cursor decodes to the merged segment at local 1.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lf7m"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lf7m"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let merged_seg = desc.resolve_segment("").seg_id;
    let expected = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: merged_seg,
        offset: 1,
    }
    .encode(&desc.project_id, &skey());
    assert_eq!(
        last_next_cursor(&acc2),
        expected,
        "the head cursor IS (merged segment, segment-local 1)"
    );
}

/// Stage 7A static-concern red leg: a feed that EXISTS from before a
/// split must not hand a new subscriber a stale frontier. `cursor=now`
/// binds to the RECONCILED source — records durable before the
/// subscribe are never delivered under "now".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_cursor_now_uses_the_reconciled_source() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfnow",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfnow", r#"{"i":0}"#).await;
    let mut sub1 = lf_connect(addr, "lfnow", "").await;
    let (_, _) = hub_sse_collect(&mut sub1, 8, |t| t.contains("upToDate")).await;

    // Freeze the existing subscriber's drive so the feed stays at the
    // PRE-SPLIT source (generation 0, frontier 1) across the split.
    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "lfnow");
    split_and_await(&state, "lfnow", 0).await;
    // A successor record becomes durable while the feed is stale.
    hub_append_lf(addr, "lfnow", r#"{"i":1}"#).await;

    // A second client connects with cursor=now against the CURRENT
    // descriptor: reconcile must install the lineage BEFORE its join
    // state is captured, so "now" = the successor tail (2), never the
    // stale parent frontier (1).
    let mut sub2 = lf_connect(addr, "lfnow", "?cursor=now").await;
    let (acc0, _) = hub_sse_collect(&mut sub2, 6, |t| t.contains("upToDate")).await;
    assert!(
        !acc0.contains("\"i\":1"),
        "cursor=now must not deliver the pre-subscribe successor record:\n{acc0}"
    );

    // Unfreeze the drive failpoint (the first subscriber now swaps
    // and drains; its drain batch ends AT the second subscriber's
    // cursor and is correctly skipped).
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "lfnow");

    // Only records written AFTER the subscribe arrive.
    hub_append_lf(addr, "lfnow", r#"{"i":2}"#).await;
    let (acc2, _) = hub_sse_collect(&mut sub2, 10, |t| t.contains("\"i\":2")).await;
    assert!(acc2.contains("\"i\":2"), "the new record arrives:\n{acc2}");
    assert!(
        !acc2.contains("\"i\":1"),
        "still no pre-subscribe record:\n{acc2}"
    );

    // The first subscriber drains the whole lineage.
    let (acc1, _) = hub_sse_collect(&mut sub1, 10, |t| t.contains("\"i\":2")).await;
    assert!(
        acc1.contains("\"i\":1") && acc1.contains("\"i\":2"),
        "the parked subscriber drains the whole lineage:\n{acc1}"
    );
}
