//! A live tail's pinned engine retires under the SAME owner (review rank
//! 14): the parked session takes the typed EngineRetired cutoff, a read
//! on the retired engine is the same cutoff, and a reconnect through the
//! route lands on the replacement incarnation.

use super::fixture_http::http_rig_owner;
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, lf_connect, lf_record_and_status, split_and_await, sse_head,
    wait_parked,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// The same bounded teardown assertion the ownership scenarios use.
async fn wait_for_feed_teardown(state: &crate::http::AppState, attempts: usize) {
    for _ in 0..attempts {
        if state.livefeed.registry().len() == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(state.livefeed.registry().len(), 0, "feed teardown stalled");
}

/// Connect until the route answers 200 (a bounded open on the replacement
/// engine may first answer the retryable 503); the head is consumed so the
/// caller collects the body only.
async fn connect_ready(
    addr: std::net::SocketAddr,
    name: &str,
    query: &str,
) -> tokio::net::TcpStream {
    for attempt in 0..20u32 {
        let mut sck = lf_connect(addr, name, query).await;
        let (status, head) = sse_head(&mut sck).await;
        if status == 200 {
            return sck;
        }
        assert_eq!(
            status, 503,
            "only the open-wait refusal may precede the resume:\n{head}"
        );
        drop(sck);
        tokio::time::sleep(std::time::Duration::from_millis(
            50 * (u64::from(attempt) + 1),
        ))
        .await;
    }
    panic!("{name}: the replacement engine never served the resume");
}

fn engine_retired_cutoffs() -> u64 {
    crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Review rank 14 (red): a live-tail source pins ONE engine incarnation.
/// When that engine closes under the SAME owner (fatal store, worker
/// exit, sub-tick flap) the parked session was woken once by begin_close,
/// re-checked ownership only, and re-parked on a dead handle's notify:
/// keep-alives forever, and every reconnect rejoined the dead feed
/// (install_source refuses an equal-length signature). The session must
/// take the typed EngineRetired cutoff (nonterminal EOF), the feed must
/// tear down, and a reconnect through the same route must land on a NEW
/// engine incarnation and see new data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_under_the_same_owner_cuts_a_parked_session() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xret",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xret", r#"{"p":0}"#).await;
    let sref = state.deployment.raw_adapter_sref("xret");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state
        .shards
        .prefix_for(&desc.resolve_segment("").shard_route);
    let load = |body: &[u8]| serde_json::from_slice::<serde_json::Value>(body).unwrap();
    let (st, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert_eq!(st, 200);
    let exposed_before = load(&body)["sse_livefeed"]["cutoff_engine_retired"]
        .as_u64()
        .expect("the engine-retired cutoff is exposed beside the other cutoff reasons");
    let retired_before = engine_retired_cutoffs();

    let mut sub = lf_connect(addr, "xret", "?cursor=now").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(
        a0.contains("\"upToDate\":true") && !eof0,
        "parked at the live tail:\n{a0}"
    );

    // The acker's own close path: ownership does NOT move.
    let inc_before = state.shards.resident_incarnation(&prefix);
    let engine = state
        .shards
        .open(&prefix)
        .expect("the live tail's engine is resident");
    engine.begin_close();
    assert!(
        state.shards.open(&prefix).is_none(),
        "the close evicted the resident"
    );
    assert!(state.ownership.is_mine(&prefix), "same owner throughout");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "the parked session must take the typed EngineRetired cutoff, not keep-alive forever:\n{a1}"
    );
    assert!(
        !a1.contains("event: data") && !a1.contains("\"sealed\":true"),
        "no stale data, no terminal:\n{a1}"
    );
    assert!(
        engine_retired_cutoffs() > retired_before,
        "the cutoff must be classified EngineRetired"
    );
    let (_, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert!(
        load(&body)["sse_livefeed"]["cutoff_engine_retired"]
            .as_u64()
            .unwrap()
            > exposed_before,
        "the typed reason must be visible on /v1/debug/load"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;

    // The replacement can only open once the retired incarnation has
    // TERMINATED (the gate awaits its shutdown handle before reopening);
    // bound that wait here so the reconnect's head arrives inside the
    // fixture's read deadline.
    let retired = engine.shutdown_handle();
    drop(engine);
    for _ in 0..400 {
        if retired.terminated() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(retired.terminated(), "the retired engine must terminate");

    // The route reopens (holdoff skipped, test-only) and a resume lands
    // on the REPLACEMENT engine: new appends reach the new session.
    state.shards.clear_holdoff(&prefix);
    let mut again = connect_ready(addr, "xret", "?cursor=now").await;
    let (b0, eof) = hub_sse_collect(&mut again, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(
        b0.contains("\"upToDate\":true") && !eof,
        "resume on the replacement:\n{b0}"
    );
    let inc_after = state.shards.resident_incarnation(&prefix);
    assert!(
        inc_before.is_some() && inc_after.is_some() && inc_before != inc_after,
        "the resume must be served by a NEW engine incarnation"
    );
    hub_append_lf(addr, "xret", r#"{"p":9}"#).await;
    let (b1, eof) = hub_sse_collect(&mut again, 15, |t| t.contains("\"p\":9")).await;
    assert!(
        b1.contains("\"p\":9") && !eof,
        "the replacement engine serves the resume:\n{b1}"
    );
    drop(again);
    wait_for_feed_teardown(&state, 300).await;
}

/// Review rank 14 (red), lineage shape: after a split the feed's live
/// tail is a LiveLocal span pinned to the child's engine. Closing that
/// engine under the same owner must cut the parked session with
/// EngineRetired; the sealed predecessor's engine is not the tail and
/// re-resolves per page, so only the tail decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_under_the_same_owner_cuts_a_parked_lineage_session() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xlret",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xlret", r#"{"h":0}"#).await;
    split_and_await(&state, "xlret", 0).await;
    hub_append_lf(addr, "xlret", r#"{"h":1}"#).await;
    let sref = state.deployment.raw_adapter_sref("xlret");
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let child = desc.resolve_segment("").seg_id;
    let p_child = state
        .shards
        .prefix_for(&desc.segment_route_by_id(child).unwrap());
    let p_parent = state
        .shards
        .prefix_for(&desc.segment_route_by_id(0).unwrap());
    assert_ne!(p_parent, p_child);
    let retired_before = engine_retired_cutoffs();

    let mut sub = lf_connect(addr, "xlret", "?cursor=beginning").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| lf_record_and_status(t, "\"h\":1")).await;
    assert!(
        a0.contains("\"h\":1") && !eof0,
        "parked at the lineage's live tail:\n{a0}"
    );

    let engine = state
        .shards
        .open(&p_child)
        .expect("the child's engine is resident");
    engine.begin_close();
    assert!(state.ownership.is_mine(&p_child), "same owner throughout");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "a retired LiveLocal tail must cut the parked lineage session, not keep-alive forever:\n{a1}"
    );
    assert!(
        !a1.contains("event: data") && !a1.contains("\"sealed\":true"),
        "no stale data, no terminal:\n{a1}"
    );
    assert!(
        engine_retired_cutoffs() > retired_before,
        "the lineage cutoff must be classified EngineRetired"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;
}

/// Review rank 14 (red), the read side: a session woken by an append
/// drives a read on its pinned engine. If that engine retired between the
/// wake and the read, the read must be the typed EngineRetired cutoff,
/// never a page served from a dead engine's ring (the record resumes
/// from the replacement). Forced with the drive failpoint so the
/// interleaving is exact, not raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_before_a_drive_is_a_typed_read_cutoff() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xrread",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let sref = state.deployment.raw_adapter_sref("xrread");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state
        .shards
        .prefix_for(&desc.resolve_segment("").shard_route);
    let retired_before = engine_retired_cutoffs();

    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread");
    let mut sub = lf_connect(addr, "xrread", "?cursor=now").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(
        a0.contains("\"upToDate\":true") && !eof0,
        "parked at the live tail:\n{a0}"
    );
    // The append wakes the session; it stops just before its drive.
    hub_append_lf(addr, "xrread", r#"{"r":1}"#).await;
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread", 1).await;
    let engine = state
        .shards
        .open(&prefix)
        .expect("the live tail's engine is resident");
    engine.begin_close();
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "the drive on a retired engine must take the typed cutoff, not park:\n{a1}"
    );
    assert!(
        !a1.contains("\"r\":1"),
        "a retired engine must never serve a page; the record resumes from the replacement:\n{a1}"
    );
    assert!(
        engine_retired_cutoffs() > retired_before,
        "the read cutoff must be classified EngineRetired"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;
}
