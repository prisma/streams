#![cfg(test)]
//! F6's peer read hop (GATE-DECISION-f6-prep-rows-rev2-2026-11-17, addendum 2026-11-18 A1-A8):
//! reads of a planned move's shard at a target whose mirror is still stale. A read the target
//! relays to the giver (`/v1/internal/segment-read`) meets the giver's `409` naming the target
//! under the giver's overrides version; the target refreshes from it and serves the read itself
//! (A2's second case), and a burst of such reads refreshes once (R1).

use super::fixture_requests::{PRISMA_KEY, hreq};
use super::fleet_birth::Pair;
use super::fleet_handoff::moved_with_a_stale_target;
use crate::application::read::ReadRange;
use crate::application::read_remote::{InternalTarget, RemoteSpanError, remote_span_page};

/// **A read at a stale target is served there once the giver's `409` names it** (A2, its second
/// case). After a returned child's move, `streams-2` still publishes its view from before the
/// move, so a client read of the child's stream at `streams-2` is relayed to `streams-1`. The
/// giver answers `409` naming `streams-2` with its overrides version; `streams-2` refreshes from
/// it (one overrides read), resolves the read again from the top and serves it: `200`, the child
/// now open on `streams-2`, and no `Streams-Overrides-Version` on the client's answer (A5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_at_a_stale_target_is_served_there_once_the_givers_409_names_it() {
    let pair = Pair::loaded().await;
    let (gone, stream) = moved_with_a_stale_target(&pair).await;
    let (b, b_addr) = (&pair.b.state, pair.b.addr);
    let reads = b.fleet.standing().tags().reads();
    let path = format!("/v1/stream/{stream}");
    let (status, headers, body) = hreq(b_addr, "GET", &path, &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert!(
        !headers.contains_key("streams-overrides-version"),
        "A5: {headers:?}"
    );
    let read = b.fleet.standing().tags().reads() - reads;
    let child = ["0", "1"][gone];
    assert_eq!(
        (read, b.shards.is_open(child)),
        (1, true),
        "one overrides read, served here"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **A burst of reads at a stale target refreshes it once** (R1 on the peer read hop, A8). 32
/// concurrent reads of the moved child's stream at `streams-2`, its mirror stale: every one is
/// served (`200`), and `streams-2` reads the overrides once for the burst.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_burst_of_reads_at_a_stale_target_makes_one_overrides_read() {
    let pair = Pair::loaded().await;
    let (_, stream) = moved_with_a_stale_target(&pair).await;
    let (b, b_addr) = (&pair.b.state, pair.b.addr);
    let reads = b.fleet.standing().tags().reads();
    let path = format!("/v1/stream/{stream}");
    let read = |_| async { hreq(b_addr, "GET", &path, &[], b"").await.0 };
    let answered = futures_util::future::join_all((0..32).map(read)).await;
    assert!(answered.iter().all(|s| *s == 200), "{answered:?}");
    let read = b.fleet.standing().tags().reads() - reads;
    assert_eq!(read, 1, "one overrides read for the burst");
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// The moved child's stream as `peer` would ask an owner for its live segment's span page.
async fn span_page(
    pair: &Pair,
    asker: &crate::http::AppState,
    owner: &str,
    stream: &str,
) -> Result<crate::application::read::ReadPage, RemoteSpanError> {
    let sref = pair.b.state.deployment.raw_adapter_sref(stream);
    let desc = pair.b.state.registry.get(&sref).await.unwrap().unwrap();
    let target = InternalTarget::of(&desc, desc.resolve_segment("").seg_id).unwrap();
    let range = ReadRange::open(0);
    let page = remote_span_page(
        &asker.peer,
        owner,
        &desc,
        &target,
        range,
        64 << 10,
        PRISMA_KEY,
    );
    page.await.map(|page| page.out)
}

/// **The giver's span page of the moved child is served by the stale target** (A1 on the span
/// route, A8's span-route case). `/v1/internal/segment-scan` carries the giver's overrides
/// version; the target refreshes from it before it resolves and serves the page, where it
/// answered `409` naming the giver before (a live session's `RedirectLoop` cutoff).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_givers_span_page_of_the_moved_child_is_served_by_the_stale_target() {
    let pair = Pair::loaded().await;
    let (_, stream) = moved_with_a_stale_target(&pair).await;
    let b = &pair.b.state;
    let reads = b.fleet.standing().tags().reads();
    let page = span_page(&pair, &pair.a.state, "streams-2", &stream).await;
    assert!(page.is_ok(), "{:?}", page.err());
    assert_eq!(
        b.fleet.standing().tags().reads() - reads,
        1,
        "one overrides read"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **A stale target's span page of the moved child never ends in a redirect loop** (A2 on the
/// span route). The target asks the giver, its view naming it; the giver's `409` names the
/// target under the giver's version, and the target refreshes from it before its next step.
/// The page is served, or answered as a live session retries it, never `RedirectLoop` (its
/// fatal cutoff); afterwards the target's view gives it the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_stale_targets_span_page_never_ends_in_a_redirect_loop() {
    let pair = Pair::loaded().await;
    let (gone, stream) = moved_with_a_stale_target(&pair).await;
    let b = &pair.b.state;
    let reads = b.fleet.standing().tags().reads();
    let page = span_page(&pair, b, "streams-1", &stream).await;
    let looped = matches!(page, Err(RemoteSpanError::RedirectLoop { .. }));
    assert!(!looped, "{:?}", page.err());
    let owner = b.ownership.effective_owner(["0", "1"][gone]);
    let read = b.fleet.standing().tags().reads() - reads;
    assert_eq!(
        (owner.as_deref(), read),
        (Some("streams-2"), 1),
        "refreshed once"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **A `409` naming this instance that its refreshed view does not confirm is answered as
/// before** (A2, its third case: no third hop, no second re-resolve). `streams-1`'s view and the
/// stored overrides give `00` to `peer-x`, a peer that answers every read page `409` naming
/// `streams-1` under another version. A client read of a stream in `00` at `streams-1` asks
/// `peer-x` once, refreshes once (the document still names `peer-x`) and is answered as before
/// F6's peer read hop: `503` with `Retry-After: 1`, the redirect loop named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_409_naming_this_instance_that_its_refreshed_view_denies_answers_as_before() {
    use object_store::{ObjectStoreExt, PutPayload, path::Path};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fleet = super::fixture_storage::mem();
    let data = super::fixture_storage::mem();
    let first = super::fixture_runtime::RigRuntime::first();
    let rig = super::fleet_handoff_cases::member(&data, fleet.clone(), "streams-1", first, true);
    let rig = rig.await;
    let name = super::fleet_handoff_cases::stream_in_00(&rig).await;
    let doc = br#"{"entries":{"00":{"to":"peer-x","ms":0}}}"#;
    let path = Path::from("fleet/overrides.json");
    fleet
        .put(&path, PutPayload::from_static(doc))
        .await
        .unwrap();
    let ring = vec!["streams-1".to_string(), "peer-x".to_string()];
    let entries = [("00".to_string(), "peer-x".to_string())];
    rig.state.ownership.set_view(ring, entries.into());
    let asked = std::sync::Arc::new(AtomicUsize::new(0));
    let count = asked.clone();
    let refuse = move || {
        count.fetch_add(1, Ordering::SeqCst);
        let headers = [
            ("streams-replay-to", "streams-1"),
            ("streams-overrides-version", "v-peer"),
        ];
        let body = r#"{"error":{"code":"not_ring_owner","message":"peer-x names streams-1"}}"#;
        std::future::ready((axum::http::StatusCode::CONFLICT, headers, body))
    };
    let route = axum::routing::get(refuse);
    let app = axum::Router::new().route("/v1/internal/segment-read/{*name}", route);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    rig.state.peer.set_peer("peer-x", &url);
    let reads = rig.state.fleet.standing().tags().reads();
    let path = format!("/v1/stream/{name}");
    let read = hreq(rig.addr, "GET", &path, &[], b"");
    let serve = axum::serve(listener, app).into_future();
    let ended = futures_util::future::select(Box::pin(read), Box::pin(serve)).await;
    let futures_util::future::Either::Left(((status, headers, body), _)) = ended else {
        panic!("the fake peer ended first");
    };
    let body = String::from_utf8_lossy(&body);
    let retry = headers.get("retry-after").map(String::as_str);
    assert_eq!((status, retry), (503, Some("1")), "{body}");
    assert!(body.contains("RedirectLoop"), "{body}");
    let read = rig.state.fleet.standing().tags().reads() - reads;
    assert_eq!(
        (asked.load(Ordering::SeqCst), read),
        (1, 1),
        "one hop, one refresh"
    );
    super::fleet_handoff_cases::stop(rig).await;
}

/// A one-member rig whose view gives `00` to `peer-x`, a fake peer that answers every peer read
/// page `409` naming `streams-1` under its own overrides version, and the stored overrides naming
/// `streams-1` for `00`: the rig, a stream in `00` an earlier incarnation wrote (nothing of `00` is
/// resident or held off here, as at a stale target), the peer's URL listener, and its ask count.
async fn stale_rig_and_fake_peer() -> (
    super::fixture_http::HttpRig,
    String,
    tokio::net::TcpListener,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use object_store::{ObjectStoreExt, PutPayload, path::Path};
    let fleet = super::fixture_storage::mem();
    let data = super::fixture_storage::mem();
    let name = written_by_an_earlier_incarnation(&data, &fleet, |rig| {
        Box::pin(super::fleet_handoff_cases::stream_in_00(rig))
    })
    .await;
    let incarnation = super::fixture_runtime::RigRuntime::incarnation(1);
    let member =
        super::fleet_handoff_cases::member(&data, fleet.clone(), "streams-1", incarnation, false);
    let rig = member.await;
    let doc = br#"{"entries":{"00":{"to":"streams-1","ms":0}}}"#;
    let path = Path::from("fleet/overrides.json");
    fleet
        .put(&path, PutPayload::from_static(doc))
        .await
        .unwrap();
    let ring = vec!["streams-1".to_string(), "peer-x".to_string()];
    let entries = [("00".to_string(), "peer-x".to_string())];
    rig.state.ownership.set_view(ring, entries.into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    rig.state.peer.set_peer("peer-x", &url);
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    (rig, name, listener, asked)
}

/// What `write` returns, written through a first incarnation of `streams-1` on `data` and `fleet`
/// with its view to itself alone, which then stops: a later incarnation finds the data and nothing
/// of it resident.
async fn written_by_an_earlier_incarnation<T>(
    data: &std::sync::Arc<dyn object_store::ObjectStore>,
    fleet: &std::sync::Arc<dyn object_store::ObjectStore>,
    write: impl for<'a> FnOnce(
        &'a super::fixture_http::HttpRig,
    )
        -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>,
) -> T {
    let first = super::fixture_runtime::RigRuntime::first();
    let rig = super::fleet_handoff_cases::member(data, fleet.clone(), "streams-1", first, true);
    let rig = rig.await;
    let alone = vec!["streams-1".to_string()];
    rig.state.ownership.set_view(alone, Default::default());
    let written = write(&rig).await;
    super::fleet_handoff_cases::stop(rig).await;
    written
}

/// `GET path` through `rig` while `listener` serves the fake peer; the answer, the asks counted.
async fn read_beside_the_fake_peer(
    rig: &super::fixture_http::HttpRig,
    path: &str,
    listener: tokio::net::TcpListener,
    asked: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> (u16, String) {
    use std::sync::atomic::Ordering;
    let count = asked.clone();
    let refuse = move || {
        count.fetch_add(1, Ordering::SeqCst);
        let headers = [
            ("streams-replay-to", "streams-1"),
            ("streams-overrides-version", "v-peer"),
        ];
        let body = r#"{"error":{"code":"not_ring_owner","message":"peer-x names streams-1"}}"#;
        std::future::ready((axum::http::StatusCode::CONFLICT, headers, body))
    };
    let route = axum::routing::get(refuse);
    let app = axum::Router::new().route("/v1/internal/segment-read/{*name}", route);
    let read = hreq(rig.addr, "GET", path, &[], b"");
    let serve = axum::serve(listener, app).into_future();
    let ended = futures_util::future::select(Box::pin(read), Box::pin(serve)).await;
    let futures_util::future::Either::Left(((status, _, body), _)) = ended else {
        panic!("the fake peer ended first");
    };
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// **A `409` naming this instance that its refreshed view confirms is served here** (A2, its
/// second case, `ReadService::remote_page`). The rig's view gives `00` to `peer-x`; the stored
/// overrides name `streams-1`. A read is relayed to `peer-x`, whose `409` names `streams-1`; the
/// rig refreshes once, finds `00` its own, resolves the read again and serves it: `200`, one ask.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_409_naming_this_instance_that_its_refreshed_view_confirms_is_served_here() {
    use std::sync::atomic::Ordering;
    let (rig, name, listener, asked) = stale_rig_and_fake_peer().await;
    let path = format!("/v1/stream/{name}?offset=-1");
    let (status, body) = read_beside_the_fake_peer(&rig, &path, listener, &asked).await;
    assert_eq!((status, asked.load(Ordering::SeqCst)), (200, 1), "{body}");
    super::fleet_handoff_cases::stop(rig).await;
}

/// A stream on `rig` split once, whose sealed segment 0 and the segment the default key resolves
/// to after the split live in two prefixes: its name, segment 0's sealed end (`n` records), the
/// live segment's id, and the two prefixes.
async fn split_across_two_prefixes(
    rig: &super::fixture_http::HttpRig,
    n: u32,
) -> (String, String, u32, String, String) {
    use super::billing_closure_debts::{JSON, stored};
    let state = &rig.state;
    for attempt in 0..8 {
        let name = format!("t1-{attempt}");
        let path = format!("/v1/stream/{name}");
        let (status, _, body) = hreq(rig.addr, "PUT", &path, &[JSON], b"").await;
        assert_eq!(status, 201, "{name}: {}", String::from_utf8_lossy(&body));
        let mut sealed_end = None;
        for i in 0..n {
            let record = format!(r#"[{{"n":{i}}}]"#);
            let (status, headers, _) =
                hreq(rig.addr, "POST", &path, &[JSON], record.as_bytes()).await;
            assert_eq!(status, 204, "{name}: append {i}");
            sealed_end = headers.get("stream-next-offset").cloned();
        }
        super::fixture_livefeed::split_and_await(state, &name, 0).await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        let parent = desc.segment_route_by_id(0).expect("segment 0 routes");
        let live = desc.resolve_segment("");
        let prefixes = (
            state.shards.prefix_for(&parent),
            state.shards.prefix_for(&live.shard_route),
        );
        if prefixes.0 != prefixes.1 {
            let sealed_end = sealed_end.expect("segment 0 holds records");
            return (name, sealed_end, live.seg_id, prefixes.0, prefixes.1);
        }
    }
    panic!("no split put its segments in two prefixes");
}

/// **A re-resolved read ends its page at a segment another instance owns** (the re-resolve's
/// bound, GATE-DECISION-f6-reresolve-bound-2026-11-18 C2, S1). One member, `streams-1`, and a fake
/// `peer-x` that answers every peer read page `409` naming `streams-1` under another overrides
/// version. A stream's segment 0 is sealed in one prefix, which `streams-1`'s stale mirror gives
/// to `peer-x` and the stored overrides give to `streams-1`; the segment after it lives in another
/// prefix, `peer-x`'s in both. A replay read from segment 0's sealed end asks `peer-x` once,
/// refreshes once, re-resolves here with no further peer hop, finds segment 0 exhausted and ends
/// its page at the next segment: `200`, empty, the next cursor at that segment's offset 0, not up
/// to date, and segment 0's shard open here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_re_resolved_read_ends_its_page_at_a_segment_another_instance_owns() {
    use object_store::{ObjectStoreExt, PutPayload, path::Path};
    use std::sync::atomic::Ordering;
    let fleet = super::fixture_storage::mem();
    let data = super::fixture_storage::mem();
    let split = written_by_an_earlier_incarnation(&data, &fleet, |rig| {
        Box::pin(split_across_two_prefixes(rig, 3))
    })
    .await;
    let (name, sealed_end, next_seg, sealed, next) = split;
    let incarnation = super::fixture_runtime::RigRuntime::incarnation(1);
    let member =
        super::fleet_handoff_cases::member(&data, fleet.clone(), "streams-1", incarnation, false);
    let rig = member.await;
    assert!(
        !rig.state.shards.is_open(&sealed),
        "nothing of segment 0 is resident"
    );
    let doc = format!(
        r#"{{"entries":{{"{sealed}":{{"to":"streams-1","ms":0}},"{next}":{{"to":"peer-x","ms":0}}}}}}"#
    );
    let path = Path::from("fleet/overrides.json");
    fleet.put(&path, PutPayload::from(doc)).await.unwrap();
    let ring = vec!["streams-1".to_string(), "peer-x".to_string()];
    let stale = [
        (sealed.clone(), "peer-x".to_string()),
        (next, "peer-x".to_string()),
    ];
    rig.state.ownership.set_view(ring, stale.into());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    rig.state.peer.set_peer("peer-x", &url);
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reads = rig.state.fleet.standing().tags().reads();
    let count = asked.clone();
    let refuse = move || {
        count.fetch_add(1, Ordering::SeqCst);
        let headers = [
            ("streams-replay-to", "streams-1"),
            ("streams-overrides-version", "v-peer"),
        ];
        let body = r#"{"error":{"code":"not_ring_owner","message":"peer-x names streams-1"}}"#;
        std::future::ready((axum::http::StatusCode::CONFLICT, headers, body))
    };
    let route = axum::routing::get(refuse);
    let app = axum::Router::new().route("/v1/internal/segment-read/{*name}", route);
    let read_path = format!("/v1/stream/{name}?offset={sealed_end}");
    let read = hreq(rig.addr, "GET", &read_path, &[], b"");
    let serve = axum::serve(listener, app).into_future();
    let ended = futures_util::future::select(Box::pin(read), Box::pin(serve)).await;
    let futures_util::future::Either::Left(((status, headers, body), _)) = ended else {
        panic!("the fake peer ended first");
    };
    let body = String::from_utf8_lossy(&body);
    let cursor = crate::offsets::encode(next_seg, 0);
    let page = (status, body.as_ref(), headers.get("stream-next-offset"));
    assert_eq!(page, (200, "[]", Some(&cursor)), "{headers:?}");
    let absent = [
        "stream-up-to-date",
        "stream-closed",
        "streams-overrides-version",
    ];
    let present: Vec<&str> = absent
        .into_iter()
        .filter(|h| headers.contains_key(*h))
        .collect();
    assert_eq!(present, Vec::<&str>::new(), "{headers:?}");
    let read = rig.state.fleet.standing().tags().reads() - reads;
    let served = (
        asked.load(Ordering::SeqCst),
        read,
        rig.state.shards.is_open(&sealed),
    );
    assert_eq!(
        served,
        (1, 1, true),
        "one ask, one refresh, segment 0 open here"
    );
    super::fleet_handoff_cases::stop(rig).await;
}
