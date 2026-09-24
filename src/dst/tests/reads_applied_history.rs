//! TLA-018-F3: an applied session cursor is bound to the history that served
//! its provisional suffix. The shard WAL PUT is held so the suffix is provably
//! not durable, a second instance fences the owner, and the replacement tail
//! grows past the stale cursor before the client continues.

use super::fixture_http::{engine_shutdown, http_rig_at};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::application::read::{
    Continuation, ReadCommand, ReadFailure, ReadMode, ReadPosition, ReadStart, ScanStart,
};
use crate::application::read_remote::{InternalTarget, remote_read_page};
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::offsets::Offset;
use crate::product_cursor::{KeyCursor, ReadCursor};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

type Headers = std::collections::HashMap<String, String>;

/// The records route of stream `tp` for one routing key and query.
fn records(rk: &str, query: &str) -> String {
    let key = if rk.is_empty() {
        String::new()
    } else {
        format!("&routingKey={rk}")
    };
    format!("/v1/streams/tp/records?deliver=applied{key}{query}")
}

async fn append(addr: std::net::SocketAddr, rk: &str, body: &[u8]) -> u16 {
    let mut headers = vec![("prisma-encryption-key", PRISMA_KEY)];
    if !rk.is_empty() {
        headers.push(("prisma-routing-key", rk));
    }
    preq(addr, "POST", "/v1/streams/tp/records", &headers, body)
        .await
        .0
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, Headers, serde_json::Value) {
    let (status, headers, body) = preq(
        addr,
        "GET",
        path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    let value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&body).into()));
    (status, headers, value)
}

async fn descriptor(state: &Arc<crate::http::AppState>) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref("tp");
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}

/// The segment offset a product cursor for `rk` names.
fn offset_of(desc: &crate::registry::StreamDesc, rk: &str, cursor: &str) -> u64 {
    let decoded = ReadCursor::decode(
        cursor,
        &desc.project_id,
        &skey(),
        &desc.epoch(),
        &crate::crypto::stream_hash(rk),
    )
    .expect("a cursor this stream minted");
    match decoded {
        ReadCursor::Durable(cursor) => cursor.offset,
        ReadCursor::Session(cursor) => cursor.position.offset,
    }
}

/// A fleet-internal raw read of `tp` in applied mode from `offset`, with an
/// optional relayed continuation, on the page route or the raw rendering.
async fn raw_read(
    addr: std::net::SocketAddr,
    desc: &crate::registry::StreamDesc,
    offset: &str,
    continuation: Option<&str>,
    page: bool,
) -> (u16, Headers, serde_json::Value) {
    let target = InternalTarget::of(desc, desc.resolve_segment("").seg_id)
        .unwrap()
        .headers();
    let mut headers = vec![
        ("authorization", "Bearer dst-internal-token"),
        ("stream-encryption-key", PRISMA_KEY),
        ("streams-internal-deliver", "applied"),
    ];
    headers.extend(target.iter().map(|(k, v)| (*k, v.as_str())));
    if let Some(continuation) = continuation {
        headers.push(("streams-internal-continuation", continuation));
    }
    if page {
        headers.push(("streams-internal-read-page", "1"));
    }
    let path = format!("/v1/internal/segment-read/tp?offset={offset}");
    let (status, headers, body) = preq(addr, "GET", &path, &headers, b"").await;
    let value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&body).into()));
    (status, headers, value)
}

/// The stream's one segment handle on `state`'s engine.
async fn handle_of(
    state: &Arc<crate::http::AppState>,
) -> (
    Arc<crate::shard::ShardEngine>,
    Arc<crate::shard::StreamHandle>,
) {
    let sref = state.deployment.raw_adapter_sref("tp");
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    (engine, handle)
}

/// Parks the next WAL PUT of the stream's shard DB, and only that one: the
/// new owner's open writes a WAL object too, and the shard's history
/// partition keeps writing its own WAL. The rig opener keeps a shard's DB at
/// `{prefix}/shard` (`fixture_http::rig_opener`).
async fn hold_shard_wal(
    store: &Arc<FaultStore>,
    state: &Arc<crate::http::AppState>,
) -> Arc<std::sync::atomic::AtomicU64> {
    let (engine, _) = handle_of(state).await;
    let wal = format!("{}/shard/wal/", engine.prefix);
    store.hold_class_under(StoreOp::Put, ObjClass::Wal, &wal, 1)
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// One stream with record 0 durable on a fresh instance over `store`.
async fn stream_with_one_durable_record(
    store: &Arc<dyn ObjectStore>,
    rk: &str,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    let (state, addr) = http_rig_at(store.clone(), RigRuntime::incarnation(1)).await;
    let (status, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/tp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201);
    assert_eq!(append(addr, rk, br#"{"n":0}"#).await, 200, "record 0");
    (state, addr)
}

/// What the clients hold after the applied pages over the provisional
/// record: the product session and durable cursors, and the raw next offset
/// with its continuation.
struct HeldPage {
    next: String,
    durable: String,
    raw_next: String,
    raw_continuation: String,
}

/// Holds the shard's next WAL PUT, applies record 1 behind it and reads the
/// applied page that delivers it as pending.
async fn applied_page_over_a_held_record(
    state: &Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
    engaged: &Arc<std::sync::atomic::AtomicU64>,
    rk: &str,
) -> HeldPage {
    let (_, handle) = handle_of(state).await;
    until("record 1 never applied behind the held WAL PUT", || {
        handle.state.lock().unwrap().applied.next >= 2 && engaged.load(Ordering::SeqCst) > 0
    })
    .await;
    assert_eq!(
        handle.state.lock().unwrap().durable.next,
        1,
        "the held WAL PUT keeps record 1 provisional"
    );
    let (status, headers, page) = get(addr, &records(rk, "")).await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(page, serde_json::json!([{"n":0},{"n":1}]), "applied page");
    assert_eq!(
        headers.get("prisma-pending-from").map(String::as_str),
        Some("1")
    );
    let next = headers["prisma-next-cursor"].clone();
    let durable = headers["prisma-durable-cursor"].clone();
    let desc = descriptor(state).await;
    assert_eq!(
        (offset_of(&desc, rk, &next), offset_of(&desc, rk, &durable)),
        (2, 1)
    );
    // The same page on the raw surface (the coordinator's relay vocabulary).
    let (status, headers, page) = raw_read(addr, &desc, "-1", None, false).await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(
        page,
        serde_json::json!([{"n":0},{"n":1}]),
        "raw applied page"
    );
    assert_eq!(headers["stream-next-offset"], Offset::before(2).encode());
    assert_eq!(headers["stream-durable-offset"], Offset::before(1).encode());
    let (status, _, wire) = raw_read(addr, &desc, "-1", None, true).await;
    assert_eq!(status, 200, "{wire}");
    assert_eq!(wire["continuation"]["continues"]["recover"], 1, "{wire}");
    // v2 treatment: a durable-position token carries no history, so past
    // the durable frontier it is refused although the applied tail covers it.
    let v2 = KeyCursor {
        epoch: desc.epoch(),
        key_hash: crate::crypto::stream_hash(rk),
        seg_id: desc.resolve_segment("").seg_id,
        offset: 2,
    }
    .encode(&desc.project_id, &skey());
    let (status, _, body) = get(addr, &records(rk, &format!("&cursor={v2}"))).await;
    assert_eq!(
        status, 409,
        "a v2 position past the durable frontier: {body}"
    );
    assert_eq!(body["error"]["code"], "cursor_beyond_tail");
    HeldPage {
        next,
        durable,
        raw_next: headers["stream-next-offset"].clone(),
        raw_continuation: headers
            .get("stream-continuation")
            .expect("a raw page past the durable frontier carries its continuation")
            .clone(),
    }
}

/// **TLA-018-F3.** Record 1 is applied behind a held WAL PUT and delivered
/// as pending. A second instance fences the owner, so record 1 is lost, and
/// appends different records at offsets 1 and 2. The session cursor (2) is
/// not a continuation of that history: it must be refused with the durable
/// recovery cursor (1), which then delivers both replacements. Accepting it
/// would serve only offset 2 and move the durable cursor past offset 1,
/// whose durable record the client never received.
async fn stale_continuation_after_replacement(rk: &str) {
    let (state1, held, state2, addr2) = replace_a_lost_provisional_record(rk).await;
    let desc = descriptor(&state2).await;
    product_continuation_resynchronises(addr2, &desc, rk, &held).await;
    raw_continuation_resynchronises(&state2, addr2, &desc, &held).await;

    // v2 treatment: a durable-position token at or below the durable
    // frontier is replay. It carries no history, so a v2 token minted by an
    // older server over a lost suffix cannot be detected once the frontier
    // passes it (docs/GUIDE-COMPOSER.md, "Cursor versions").
    let v2 = KeyCursor {
        epoch: desc.epoch(),
        key_hash: crate::crypto::stream_hash(rk),
        seg_id: desc.resolve_segment("").seg_id,
        offset: 2,
    }
    .encode(&desc.project_id, &skey());
    let (status, _, body) = get(addr2, &records(rk, &format!("&cursor={v2}"))).await;
    assert_eq!((status, body), (200, serde_json::json!([{"n":20}])));
    engine_shutdown(&state2).await;
    engine_shutdown(&state1).await;
}

/// Record 0 durable, record 1 applied behind the held WAL PUT and delivered
/// as pending, then a replacement owner that recovered without record 1 and
/// appended records 10 and 20 at offsets 1 and 2.
async fn replace_a_lost_provisional_record(
    rk: &str,
) -> (
    Arc<crate::http::AppState>,
    HeldPage,
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
) {
    let store = FaultStore::uniform(mem(), 0xF3, FaultPlan::CLEAN);
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    let (state1, addr1) = stream_with_one_durable_record(&dyn_store, rk).await;
    let engaged = hold_shard_wal(&store, &state1).await;
    let provisional = async {
        tokio::time::timeout(Duration::from_secs(30), append(addr1, rk, br#"{"n":1}"#)).await
    };
    let scenario = async {
        let held = applied_page_over_a_held_record(&state1, addr1, &engaged, rk).await;
        // Ownership replacement: the second instance opens the shard while
        // the owner's WAL PUT is still parked, so it recovers without
        // record 1 and fences the owner.
        let (state2, addr2) = http_rig_at(dyn_store.clone(), RigRuntime::incarnation(2)).await;
        let (_, handle2) = handle_of(&state2).await;
        {
            let st = handle2.state.lock().unwrap();
            assert_eq!(
                (st.durable.next, st.applied.next),
                (1, 1),
                "the replacement owner recovered without the provisional record"
            );
        }
        for body in [br#"{"n":10}"#, br#"{"n":20}"#] {
            assert_eq!(append(addr2, rk, body).await, 200, "replacement append");
        }
        store.release_hold();
        (held, state2, addr2)
    };
    let (acked, (held, state2, addr2)) = futures_util::future::join(provisional, scenario).await;
    assert!(
        !matches!(acked, Ok(200)),
        "the lost provisional record was acknowledged: {acked:?}"
    );
    let (status, _, durable) = get(addr2, &records(rk, "")).await;
    assert_eq!(status, 200);
    assert_eq!(
        durable,
        serde_json::json!([{"n":0},{"n":10},{"n":20}]),
        "offset 1 now holds the replacement record"
    );
    (state1, held, state2, addr2)
}

/// The product read refuses the stale session cursor on every mode with the
/// durable recovery cursor, which then delivers every replacement record.
async fn product_continuation_resynchronises(
    addr2: std::net::SocketAddr,
    desc: &crate::registry::StreamDesc,
    rk: &str,
    held: &HeldPage,
) {
    let (status, headers, body) = get(addr2, &records(rk, &format!("&cursor={}", held.next))).await;
    assert_eq!(
        status,
        409,
        "stale continuation accepted: body {body}, durable cursor {:?}",
        headers.get("prisma-durable-cursor")
    );
    assert_eq!(body["error"]["code"], "cursor_beyond_tail", "{body}");
    assert_eq!(body["error"]["details"]["reason"], "history_replaced");
    let recovery = headers["prisma-durable-cursor"].clone();
    assert_eq!(body["error"]["details"]["durableCursor"], recovery.as_str());
    assert_eq!(
        offset_of(desc, rk, &recovery),
        1,
        "the durable recovery position"
    );
    // Durable mode does not bypass the check.
    let durable_mode = format!("/v1/streams/tp/records?cursor={}", held.next);
    let durable_mode = if rk.is_empty() {
        durable_mode
    } else {
        format!("{durable_mode}&routingKey={rk}")
    };
    assert_eq!(get(addr2, &durable_mode).await.0, 409);
    // SSE serves durable records only, but it must not adopt the stale
    // position either.
    let sse = durable_mode.replace("/records?", "/records:sse?");
    let (status, _, body) = get(addr2, &sse).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["details"]["reason"], "history_replaced");

    // Resynchronising delivers every replacement record, and the durable
    // cursor never passes one the client has not received.
    let (status, headers, resumed) = get(addr2, &records(rk, &format!("&cursor={recovery}"))).await;
    assert_eq!(status, 200, "{resumed}");
    assert_eq!(
        resumed,
        serde_json::json!([{"n":10},{"n":20}]),
        "the recovery cursor resumes over both replacements"
    );
    assert_eq!(offset_of(desc, rk, &headers["prisma-durable-cursor"]), 3);
    let (status, _, resumed) = get(addr2, &records(rk, &format!("&cursor={}", held.durable))).await;
    assert_eq!(status, 200, "{resumed}");
    assert_eq!(resumed, serde_json::json!([{"n":10},{"n":20}]));
}

/// The raw surface refuses the same stale continuation with the same
/// recovery position, on both renderings and through a relay.
async fn raw_continuation_resynchronises(
    state2: &Arc<crate::http::AppState>,
    addr2: std::net::SocketAddr,
    desc: &crate::registry::StreamDesc,
    held: &HeldPage,
) {
    let seg = desc.resolve_segment("").seg_id;
    let continuation = Some(held.raw_continuation.as_str());
    let (status, headers, body) = raw_read(addr2, desc, &held.raw_next, continuation, false).await;
    assert_eq!(status, 409, "stale raw continuation accepted: {body}");
    assert_eq!(body["error"]["code"], "cursor_beyond_tail", "{body}");
    assert_eq!(headers["stream-durable-offset"], Offset::before(1).encode());
    let (status, _, body) = raw_read(addr2, desc, &held.raw_next, continuation, true).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(
        body,
        serde_json::json!({"refused": "history_replaced", "recover": {"segment": seg, "after": 1}})
    );
    state2
        .peer
        .set_peer("replacement-owner", &format!("http://{addr2}"));
    let command = ReadCommand {
        descriptor: desc.clone(),
        key: Some(skey()),
        start: ReadStart::Continue(
            ReadPosition {
                segment: seg,
                after: 2,
            },
            Continuation::from_header(&held.raw_continuation).unwrap(),
        ),
        selector: None,
        mode: ReadMode::Replay,
        visibility: crate::shard::Deliver::Applied,
        max_bytes: 4096,
        tail_max_bytes: 4096,
        allow_remote: true,
        refresh: false,
    };
    let relayed = remote_read_page(
        &state2.peer,
        "replacement-owner",
        &command,
        seg,
        ScanStart::At(2),
    )
    .await;
    assert!(
        matches!(
            relayed,
            Err(ReadFailure::HistoryReplaced(ReadPosition { after: 1, .. }))
        ),
        "the relay carries the owner's verdict: {:?}",
        relayed.map(|out| out.next)
    );
    let (status, headers, body) =
        raw_read(addr2, desc, &Offset::before(1).encode(), None, false).await;
    assert_eq!(
        (status, body),
        (200, serde_json::json!([{"n":10},{"n":20}]))
    );
    assert_eq!(headers["stream-durable-offset"], Offset::before(3).encode());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_unfiltered_continuation_is_refused_after_the_replacement_tail_passes_it() {
    stale_continuation_after_replacement("").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_keyed_continuation_is_refused_after_the_replacement_tail_passes_it() {
    stale_continuation_after_replacement("k").await;
}

/// **An owner change that loses nothing keeps the continuation.** Record 1
/// is delivered as pending, then becomes durable; the owner shuts down and a
/// new instance, a different writer history, takes over. The session cursor
/// is verified against what the client observed and continues without a
/// resynchronisation, and the durable cursor replays across the change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_change_that_loses_nothing_keeps_the_continuation() {
    let store = FaultStore::uniform(mem(), 0xF4, FaultPlan::CLEAN);
    let dyn_store: Arc<dyn ObjectStore> = store.clone();
    let (state1, addr1) = stream_with_one_durable_record(&dyn_store, "").await;
    let engaged = hold_shard_wal(&store, &state1).await;
    let scenario = async {
        let held = applied_page_over_a_held_record(&state1, addr1, &engaged, "").await;
        store.release_hold();
        let (_, handle) = handle_of(&state1).await;
        until("record 1 never became durable after the release", || {
            handle.state.lock().unwrap().durable.next >= 2
        })
        .await;
        held
    };
    let (acked, held) =
        futures_util::future::join(append(addr1, "", br#"{"n":1}"#), scenario).await;
    assert_eq!(acked, 200, "record 1 is acknowledged once durable");
    let (engine1, _) = handle_of(&state1).await;
    let first_writer = engine1.writer_epoch;
    drop(engine1);
    engine_shutdown(&state1).await;

    let (state2, addr2) = http_rig_at(dyn_store.clone(), RigRuntime::incarnation(2)).await;
    let (engine2, _) = handle_of(&state2).await;
    assert_ne!(
        engine2.writer_epoch, first_writer,
        "the new owner is a different writer history"
    );
    drop(engine2);
    assert_eq!(append(addr2, "", br#"{"n":2}"#).await, 200);
    let desc = descriptor(&state2).await;
    let (status, headers, page) = get(addr2, &records("", &format!("&cursor={}", held.next))).await;
    assert_eq!(status, 200, "a continuation over a durable suffix: {page}");
    assert_eq!(page, serde_json::json!([{"n":2}]));
    assert_eq!(offset_of(&desc, "", &headers["prisma-durable-cursor"]), 3);
    let (status, _, page) = get(addr2, &records("", &format!("&cursor={}", held.durable))).await;
    assert_eq!((status, page), (200, serde_json::json!([{"n":1},{"n":2}])));
    let (status, _, page) = get(
        addr2,
        &format!("/v1/streams/tp/records?cursor={}", held.durable),
    )
    .await;
    assert_eq!((status, page), (200, serde_json::json!([{"n":1},{"n":2}])));
    let (status, _, page) = raw_read(
        addr2,
        &desc,
        &held.raw_next,
        Some(&held.raw_continuation),
        false,
    )
    .await;
    assert_eq!((status, page), (200, serde_json::json!([{"n":2}])));
    engine_shutdown(&state2).await;
}
