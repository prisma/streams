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
