//! Livefeed ownership.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{http_rig, http_rig_owner, http_rig_owner_at};
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, lf_connect, lf_record_and_status, seal_ok, split_and_await,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;

/// Round-11.2 (red): the typed remote-span protocol across THREE
/// instances. Phase 1: a sealed predecessor whose owner MOVED follows
/// exactly ONE verified redirect (A answers 409 replay-to inst-c; C
/// serves). Phase 2: a redirect loop (C points back at A) is refused
/// with the typed cutoff — nonterminal EOF, no endless retry. Phase
/// 3: a predecessor that moves TO the reading instance is adopted
/// locally. Phase 4: a MOVED LIVE TAIL is never served from stale
/// state — typed WrongOwner cutoff at the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_owner_movement_one_redirect_and_typed_cutoffs() {
    let store = mem();
    let (state_a, addr_a) = http_rig_owner(store.clone(), "inst-a").await;
    let (state_b, addr_b) =
        http_rig_owner_at(store.clone(), "inst-b", RigRuntime::incarnation(1)).await;
    let (_state_c, addr_c) = http_rig_owner_at(store, "inst-c", RigRuntime::incarnation(2)).await;
    let (st, _, _) = preq(
        addr_a,
        "PUT",
        "/v1/streams/xmv",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr_a, "xmv", r#"{"h":0}"#).await;
    hub_append_lf(addr_a, "xmv", r#"{"h":1}"#).await;
    let sref = state_a.deployment.raw_adapter_sref("xmv");
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
    let p_parent = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(0).unwrap());
    let p_child = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(child_seg).unwrap());
    assert_ne!(p_parent, p_child);
    let all = ["inst-a", "inst-b", "inst-c"].map(str::to_string).to_vec();
    state_b.ownership.set_ring_active(all.clone());
    {
        for p in state_b.shards.prefixes().to_vec() {
            let owner = if p == p_parent { "inst-a" } else { "inst-b" };
            state_b.ownership.set_override(&p, owner);
        }
    }
    {
        state_b.peer.set_peer("inst-a", &format!("http://{addr_a}"));
        state_b.peer.set_peer("inst-c", &format!("http://{addr_c}"));
    }
    hub_append_lf(addr_b, "xmv", r#"{"h":2}"#).await;
    let teardown = |state_b: Arc<crate::http::AppState>| async move {
        for _ in 0..300 {
            if state_b.livefeed.registry().len() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("feed teardown stalled");
    };

    // PHASE 1 — one verified redirect: A no longer owns the parent
    // and names inst-c; C serves the pages.
    state_a.ownership.set_ring_active(all.clone());
    state_a
        .ownership
        .set_override(&p_parent.clone(), &"inst-c".to_string());
    {
        let mut sck = lf_connect(addr_b, "xmv", "?cursor=beginning").await;
        let (acc, eof) =
            hub_sse_collect(&mut sck, 15, |t| lf_record_and_status(t, "\"h\":2")).await;
        assert!(!eof, "redirected reads serve in place:\n{acc}");
        for i in 0..3u64 {
            let needle = format!("\"h\":{i}");
            assert_eq!(
                acc.matches(&needle).count(),
                1,
                "{needle} exactly once through the redirect:\n{acc}"
            );
        }
        drop(sck);
    }
    teardown(state_b.clone()).await;

    // PHASE 2 — redirect LOOP refused: C points the parent back at A.
    _state_c.ownership.set_ring_active(all.clone());
    _state_c
        .ownership
        .set_override(&p_parent.clone(), &"inst-a".to_string());
    let loops_before = crate::sse::auth::sse_stats::FEED_CUTOFF_REDIRECT_LOOP
        .load(std::sync::atomic::Ordering::Relaxed);
    {
        let mut sck = lf_connect(addr_b, "xmv", "?cursor=beginning").await;
        let (acc, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
        assert!(
            eof,
            "a redirect loop is a typed cutoff, not a stall:\n{acc}"
        );
        assert!(
            !acc.contains("event: data") && !acc.contains("\"sealed\":true"),
            "no data, no false terminal on the loop cutoff:\n{acc}"
        );
    }
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_REDIRECT_LOOP
            .load(std::sync::atomic::Ordering::Relaxed)
            > loops_before,
        "the loop must be counted under its typed reason"
    );
    teardown(state_b.clone()).await;

    // PHASE 3 — the predecessor moves TO the reader: B now owns it
    // and adopts it locally.
    state_b
        .ownership
        .set_override(&p_parent.clone(), &"inst-b".to_string());
    {
        let mut sck = lf_connect(addr_b, "xmv", "?cursor=beginning").await;
        let (acc, eof) =
            hub_sse_collect(&mut sck, 15, |t| lf_record_and_status(t, "\"h\":2")).await;
        assert!(
            !eof,
            "a locally-adopted predecessor serves in place:\n{acc}"
        );
        assert_eq!(acc.matches("\"h\":0").count(), 1, "local adoption:\n{acc}");
        drop(sck);
    }
    teardown(state_b.clone()).await;

    // PHASE 4 — the LIVE TAIL moves away UNDER an established feed:
    // the next read takes the typed WrongOwner cutoff (nonterminal
    // EOF), never stale local serving. Sub1 establishes the feed
    // while B still owns the child; the move lands; sub2 joins the
    // SAME feed and its catch-up read hits the moved tail.
    let wrong_before = crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut sub1 = lf_connect(addr_b, "xmv", "?cursor=beginning").await;
    let (a1, _) = hub_sse_collect(&mut sub1, 15, |t| lf_record_and_status(t, "\"h\":2")).await;
    assert!(a1.contains("\"h\":2"), "sub1 established:\n{a1}");
    state_b
        .ownership
        .set_override(&p_child.clone(), &"inst-a".to_string());
    {
        let mut sub2 = lf_connect(addr_b, "xmv", "?cursor=beginning").await;
        let (acc, _) = hub_sse_collect(&mut sub2, 15, |t| t.contains("HTTP/1.1 409")).await;
        // Round-11.3: the connect-time answer is the TYPED routing
        // refusal (the gateway reroutes the retry; the product
        // translator renders the inner not_ring_owner as its 409
        // conflict) — never a legacy fallback, never data from stale
        // state, never a terminal.
        assert!(
            acc.contains("HTTP/1.1 409"),
            "a moved live tail at connect is the typed routing refusal:\n{acc}"
        );
        // Round-11.4 field finding: without Streams-Replay-To the
        // product translator renders an ownership bounce as the
        // non-retryable cursor_beyond_tail — SDKs rewind healthy
        // cursors and routers never learn the new owner. The refusal
        // must carry the routing signal and keep its ownership code.
        let lower = acc.to_ascii_lowercase();
        assert!(
            lower.contains("streams-replay-to: inst-a"),
            "the ownership refusal must name the owner for the router:\n{acc}"
        );
        assert!(
            !acc.contains("cursor_beyond_tail"),
            "an ownership bounce is never a cursor condition:\n{acc}"
        );
        assert!(
            !acc.contains("event: data") && !acc.contains("\"sealed\":true"),
            "no stale data, no terminal:\n{acc}"
        );
    }
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER
            .load(std::sync::atomic::Ordering::Relaxed)
            > wrong_before,
        "the moved tail must be counted under WrongOwner"
    );
    drop(sub1);
}

/// Round-11.6 (red): the certification-mode seal-publication delay.
/// STREAMS_CERT_SEALED_PUBLISH_DELAY_MS widens the close→publication
/// gap so the field seal-herd campaign can observe the two-step
/// window on a REAL release binary. The knob must actually hold the
/// publication open for the configured window — and the seal must
/// still complete (the delay is a wider gap, not a broken seal).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn certification_seal_publish_delay_widens_the_gap() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xdelay",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xdelay", r#"{"d":0}"#).await;
    state
        .cert_sealed_publish_delay_ms
        .store(600, std::sync::atomic::Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    seal_ok(addr, "xdelay").await;
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(600),
        "the armed delay must hold the publication window open: {elapsed:?}"
    );
    // The seal COMPLETED (seal_ok asserted success); the delay is a
    // window, never a wedge. And it is one-shot per seal drive, not a
    // tax on every later read: a replay serves promptly.
    state
        .cert_sealed_publish_delay_ms
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let t1 = std::time::Instant::now();
    let mut sck = lf_connect(addr, "xdelay", "?cursor=beginning").await;
    let (acc, _) = hub_sse_collect(&mut sck, 10, |t| t.contains("\"sealed\":true")).await;
    assert!(
        acc.contains("\"d\":0") && acc.contains("\"sealed\":true"),
        "sealed replay serves records + terminal:\n{acc}"
    );
    assert!(
        t1.elapsed() < std::time::Duration::from_secs(5),
        "reads after the seal are undelayed"
    );
}

/// Round-11.4 fleet finding (red): an ORPHANED pending transition —
/// the executor died between the durable intent and the publication
/// (a crash, a fence, an ownership move mid-merge) — starved the
/// WHOLE stream: every livefeed connect answered 503
/// segment_transition, and resume() was driven only by sessions,
/// which could no longer exist. The certification fleet wedged in
/// pending=merge with every instance refusing every connect. The
/// refusal must HELP the transition along (spawn the idempotent
/// resume) so retrying clients converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_pending_transition_connect_drives_resume() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xpend",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..3 {
        hub_append_lf(addr, "xpend", &format!(r#"{{"q":{i}}}"#)).await;
    }
    // Enter the seal-to-publication gap, then ORPHAN it: the executor
    // dies with the intent durable and the publication never issued.
    crate::failpoints::arm_scaler_before_publish("xpend");
    let sref = state.deployment.raw_adapter_sref("xpend");
    let split = {
        let state = state.clone();
        let sref = sref.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000).await
        })
    };
    let mut pending_seen = false;
    for _ in 0..400 {
        state.registry.invalidate(&sref);
        let d = state.registry.get(&sref).await.unwrap().unwrap();
        if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
            pending_seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(pending_seen, "the split must reach the durable-intent gap");
    split.abort();
    let _ = split.await;
    crate::failpoints::release_scaler_before_publish("xpend");
    // A retrying client must converge: the typed 503 is retryable
    // BECAUSE the refusal drives the resume. Without that, nothing
    // ever completes the transition and this loops 503 forever.
    let mut served = false;
    for _ in 0..80 {
        let mut sck = lf_connect(addr, "xpend", "?cursor=beginning").await;
        let (acc, _) = hub_sse_collect(&mut sck, 5, |t| {
            t.contains("HTTP/1.1 503") || t.contains("\"upToDate\":true")
        })
        .await;
        if acc.contains("\"upToDate\":true") {
            assert!(
                acc.contains("\"q\":0"),
                "records replay after resume:\n{acc}"
            );
            served = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        served,
        "an orphaned pending transition must converge under client retries"
    );
}

/// Round-11.4 fleet finding (red): a PARKED live session at a shard
/// that loses ownership must be WOKEN by the engine close and take
/// the typed WrongOwner cutoff — resumable EOF, no terminal, no
/// stale serving. On the real fleet the loser's engine closes when
/// slatedb fencing surfaces (or a straggler request forces the
/// possession-yield), but begin_close woke no parked readers: the
/// certification harness held a moved-away session on keep-alives
/// for 40 s with the new owner already serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_parked_live_session_is_cut_off_by_engine_close() {
    let store = mem();
    let (state_b, addr_b) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr_b,
        "PUT",
        "/v1/streams/xpark",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr_b, "xpark", r#"{"p":0}"#).await;
    let wrong_before = crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut sub = lf_connect(addr_b, "xpark", "?cursor=now").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(
        a0.contains("\"upToDate\":true") && !eof0,
        "session parked at the live tail:\n{a0}"
    );
    // Ownership moves away; the loser's engine closes (fence). The
    // parked session must observe it WITHOUT any traffic.
    state_b
        .ownership
        .set_ring_active(["inst-a", "inst-b"].map(str::to_string).to_vec());
    {
        let ov = &state_b.ownership;
        for p in state_b.shards.prefixes().to_vec() {
            ov.set_override(&p, &"inst-a".to_string());
        }
    }
    let engines: Vec<_> = state_b.shards.engines();
    drop(engines); // retirement already closed them
    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "the parked session must take the cutoff, not keep-alive forever:\n{a1}"
    );
    assert!(
        !a1.contains("event: data") && !a1.contains("\"sealed\":true"),
        "no stale data, no terminal:\n{a1}"
    );
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER
            .load(std::sync::atomic::Ordering::Relaxed)
            > wrong_before,
        "the cutoff must be classified WrongOwner"
    );
    drop(sub);
    for _ in 0..300 {
        if state_b.livefeed.registry().len() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(state_b.livefeed.registry().len(), 0, "feed teardown");
}

/// Round-11.1 (red): a BLACKHOLED peer — TCP accepted, request read,
/// no response ever — must not suppress SSE keep-alives (the body
/// owns them), must not cancel other subscribers, must cancel its
/// in-flight page on final client drop (feed teardown), and must
/// recover in place when the peer returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_blackholed_peer_never_suppresses_heartbeats() {
    let store = mem();
    let (state_a, addr_a) = http_rig_owner(store.clone(), "inst-a").await;
    let (state_b, addr_b) = http_rig_owner_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    // Fast keep-alives so the leg proves cadence in seconds.
    state_b.livefeed.set_heartbeat_ms(300);
    let (st, _, _) = preq(
        addr_a,
        "PUT",
        "/v1/streams/xbh",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr_a, "xbh", r#"{"h":0}"#).await;
    let sref = state_a.deployment.raw_adapter_sref("xbh");
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
    let p_parent = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(0).unwrap());
    let p_child = state_b
        .shards
        .prefix_for(&desc.segment_route_by_id(child_seg).unwrap());
    assert_ne!(p_parent, p_child);
    state_b
        .ownership
        .set_ring_active(vec!["inst-a".to_string(), "inst-b".to_string()]);
    {
        for p in state_b.shards.prefixes().to_vec() {
            let owner = if p == p_parent { "inst-a" } else { "inst-b" };
            state_b.ownership.set_override(&p, owner);
        }
    }
    hub_append_lf(addr_b, "xbh", r#"{"h":1}"#).await;

    // The BLACKHOLE: accepts, reads the request, never answers.
    let hole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hole_addr = hole.local_addr().unwrap();
    let held: Arc<std::sync::Mutex<Vec<tokio::net::TcpStream>>> = Arc::new(Default::default());
    let held2 = held.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sck, _)) = hole.accept().await else {
                return;
            };
            let held = held2.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 4096];
                let _ = sck.read(&mut buf).await; // consume the request
                held.lock().unwrap().push(sck); // hold forever
            });
        }
    });
    state_b
        .peer
        .set_peer("inst-a", &format!("http://{hole_addr}"));

    // Two subscribers whose remote catch-up is blackholed.
    let mut s1 = lf_connect(addr_b, "xbh", "?cursor=beginning").await;
    let s2 = lf_connect(addr_b, "xbh", "?cursor=beginning").await;
    let (held_acc, eof) = hub_sse_collect(&mut s1, 2, |_| false).await;
    assert!(!eof, "a blackholed peer is a stall, not a disconnect");
    assert!(
        !held_acc.contains("event: data"),
        "no data while blackholed:\n{held_acc}"
    );
    assert!(
        held_acc.matches(": keep-alive").count() >= 3,
        "the BODY owns keep-alives — a blocked remote read must not \
         suppress them (got {}):\n{held_acc}",
        held_acc.matches(": keep-alive").count()
    );
    // One client's drop must not cancel the shared feed's work.
    drop(s2);
    let (still, eof2) = hub_sse_collect(&mut s1, 1, |_| false).await;
    assert!(!eof2, "the survivor stays open");
    assert!(
        still.matches(": keep-alive").count() >= 1,
        "still alive:\n{still}"
    );

    // Peer restored: recovery completes in place.
    state_b.peer.set_peer("inst-a", &format!("http://{addr_a}"));
    let (rec, eof3) = hub_sse_collect(&mut s1, 30, |t| lf_record_and_status(t, "\"h\":1")).await;
    assert!(!eof3, "recovery in place:\n{rec}");
    for i in 0..2u64 {
        let needle = format!("\"h\":{i}");
        assert_eq!(rec.matches(&needle).count(), 1, "{needle} once:\n{rec}");
    }
    // Final drop tears the feed down and CANCELS in-flight work.
    drop(s1);
    for _ in 0..200 {
        if state_b.livefeed.registry().len() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        state_b.livefeed.registry().len(),
        0,
        "teardown reaches zero feeds"
    );
}

/// Round-11.1 (red): a delayed seal publication over a parked FAN-OUT
/// creates ONE feed retry task, never a per-session timer herd — and
/// still converges promptly on release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_seal_retry_is_one_task_per_feed_at_fanout() {
    let _serial = gap_lock().lock().await; // global failpoint registry
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfherd",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfherd", r#"{"s":0}"#).await;
    // The RetryLater window needs a SPLIT topology: a single-segment
    // close resolves as genuine closure without any retry.
    split_and_await(&state, "lfherd", 0).await;
    hub_append_lf(addr, "lfherd", r#"{"s":1}"#).await;
    let mut subs = Vec::new();
    for _ in 0..24 {
        let mut sck = lf_connect(addr, "lfherd", "").await;
        let (a, _) = hub_sse_collect(&mut sck, 8, |t| lf_record_and_status(t, "\"s\":1")).await;
        assert!(a.contains("upToDate"), "parked:\n{a}");
        subs.push(sck);
    }
    crate::failpoints::stop_before_sealed_publish("lfherd");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/lfherd:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert!(st == 500 || st == 503, "withheld publication: {st}");
    // Let the withheld window breathe: every parked session wakes on
    // the close notify and observes the unresolved transition.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let key = crate::sse::session::feed_key_of(
        &state
            .registry
            .get(&state.deployment.raw_adapter_sref("lfherd"))
            .await
            .unwrap()
            .unwrap(),
        &Some(String::new()),
    );
    let feed = state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("shared feed");
    let spawns = feed.retry_spawns.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        (1..=3).contains(&spawns),
        "ONE retry scheduler per feed (a few re-arms tolerated), got {spawns} for 24 sessions"
    );
    crate::failpoints::stop_before_sealed_publish_off("lfherd");
    seal_ok(addr, "lfherd").await;
    let mut stuck = Vec::new();
    for (n, mut sck) in subs.into_iter().enumerate() {
        let (tail, eof) = hub_sse_collect(&mut sck, 5, |_| false).await;
        if !(eof && tail.matches("\"sealed\":true").count() == 1) {
            stuck.push((n, eof, tail.matches("\"sealed\":true").count()));
        }
    }
    assert!(
        stuck.is_empty(),
        "stuck subs {stuck:?}; feed={:?}",
        state.livefeed.registry().feed_for_test(&key).map(|f| (
            f.subscriber_count(),
            f.lifecycle_for_test(),
            f.current_source().closed(),
        )),
    );
}

/// Round-10 review: the per-RECORD payload ceiling is independent of
/// the request-body ceiling — a batch of many small records passes
/// while ONE record over the ceiling is refused 413, on both the
/// plain-append and the create-with-content paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_ceiling_refuses_one_oversized_record_not_the_batch() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    state.admission.set_record_ceiling(4096);
    let ct = [("content-type", "application/json")];
    // A JSON stream on the raw surface: appended ARRAYS split into
    // individual records, so the per-record ceiling is observable.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/ceil", &ct, b"").await;
    assert!(st == 200 || st == 201, "create {st}");
    // A batch of MANY small records whose TOTAL far exceeds the
    // per-record ceiling: admitted (the ceiling is per record, never
    // per body).
    let batch: Vec<String> = (0..8)
        .map(|i| format!(r#"{{"i":{i},"pad":"{}"}}"#, "x".repeat(1024)))
        .collect();
    let (st, _, dbg) = hreq(
        addr,
        "POST",
        "/v1/stream/ceil",
        &ct,
        format!("[{}]", batch.join(",")).as_bytes(),
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "a small-record batch passes: {st} {}",
        String::from_utf8_lossy(&dbg)
    );
    // ONE record over the ceiling: typed 413.
    let big = format!(r#"{{"big":"{}"}}"#, "y".repeat(8 * 1024));
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/ceil", &ct, big.as_bytes()).await;
    assert_eq!(st, 413, "one oversized record is refused");
    assert!(
        String::from_utf8_lossy(&body).contains("record_too_large"),
        "typed refusal:\n{}",
        String::from_utf8_lossy(&body)
    );
    // A small record alongside one oversized record: the whole append
    // is refused (no partial admission).
    let mixed = format!(r#"[{{"ok":1}},{big}]"#);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/ceil", &ct, mixed.as_bytes()).await;
    assert_eq!(st, 413, "no partial admission of a mixed batch");
    // The PRODUCT surface treats the append body as ONE record: the
    // same oversized body is refused there too (the product error
    // translator renders the 413 as its generic body_too_large).
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/ceil/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        big.as_bytes(),
    )
    .await;
    assert_eq!(
        st, 413,
        "the product surface enforces the ceiling per body-record"
    );
    // Create-with-content enforces the same ceiling.
    let (st, _, body) = hreq(addr, "PUT", "/v1/stream/ceil2", &ct, big.as_bytes()).await;
    assert_eq!(st, 413, "create-with-content enforces the ceiling");
    assert!(
        String::from_utf8_lossy(&body).contains("record_too_large"),
        "typed refusal on create-with-content:\n{}",
        String::from_utf8_lossy(&body)
    );
}
