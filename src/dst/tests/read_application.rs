//! R06 application positions and protocol adapters use the same consumed range.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::application::read::{ReadCommand, ReadFailure, ReadMode, ReadPosition, ReadStart};
use crate::application::read_remote::{InternalTarget, remote_read_page};
use std::time::Duration;

fn command(desc: &crate::registry::StreamDesc) -> ReadCommand {
    ReadCommand {
        descriptor: desc.clone(),
        key: Some(skey()),
        start: ReadStart::Beginning,
        selector: Some(String::new()),
        mode: ReadMode::Replay,
        visibility: crate::shard::Deliver::Durable,
        max_bytes: 4096,
        tail_max_bytes: 4096,
        allow_remote: true,
        refresh: true,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06_empty_filtered_page_has_one_position_across_application_and_protocols() {
    let (state, addr) = http_rig(mem()).await;
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            addr,
            "PUT",
            "/v1/streams/read-position",
            &credentials,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    for n in 0..3 {
        let body = format!("{{\"n\":{n}}}");
        assert_eq!(
            preq(
                addr,
                "POST",
                "/v1/streams/read-position/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", "other")
                ],
                body.as_bytes()
            )
            .await
            .0,
            200
        );
    }
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("read-position"))
        .await
        .unwrap()
        .unwrap();
    let out = state
        .read_service()
        .execute_read(command(&desc))
        .await
        .unwrap();
    assert!(out.records.is_empty());
    assert_eq!(out.next.after, 3);
    assert!(out.up_to_date);
    let (status, headers, body) = preq(
        addr,
        "GET",
        "/v1/stream/read-position",
        &[("stream-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(&body[..], b"[]");
    assert_eq!(
        crate::offsets::parse(headers.get("stream-next-offset").unwrap()),
        Ok((0, out.next.after))
    );
    let (status, headers, body) = preq(
        addr,
        "GET",
        "/v1/streams/read-position/records",
        &credentials,
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(&body[..], b"[]");
    let cursor = crate::product_cursor::KeyCursor::decode(
        headers.get("prisma-next-cursor").unwrap(),
        &desc.project_id,
        &skey(),
        &desc.epoch(),
        &crate::crypto::RoutingKeyHash::of("").0,
    )
    .unwrap();
    assert_eq!(
        (cursor.seg_id, cursor.offset),
        (out.next.segment, out.next.after)
    );
    let mut wire = serde_json::to_value(
        crate::application::read_remote::WireReadPage::from_outcome(&out),
    )
    .unwrap();
    wire.as_object_mut().unwrap().remove("next");
    assert!(
        serde_json::from_value::<crate::application::read_remote::WireReadPage>(wire).is_err(),
        "a missing peer cursor must never become offset zero"
    );
    engine_shutdown(&state).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06_refresh_rejects_same_name_recreation() {
    let (state, addr) = http_rig(mem()).await;
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    let path = "/v1/streams/read-refresh";
    assert_eq!(
        preq(
            addr,
            "PUT",
            path,
            &credentials,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("read-refresh"))
        .await
        .unwrap()
        .unwrap();
    let mut request = command(&desc);
    request.start = ReadStart::Position(ReadPosition {
        segment: 99,
        after: 0,
    });
    assert_eq!(preq(addr, "DELETE", path, &credentials, b"").await.0, 204);
    assert_eq!(
        preq(
            addr,
            "PUT",
            path,
            &credentials,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    let result = state.read_service().execute_read(request).await;
    assert!(matches!(result, Err(ReadFailure::ChangedIncarnation)));
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r06_cross_owner_replay_and_scan_share_typed_pages_and_bill_once() {
    use super::fixture_http::{http_rig_owner, http_rig_owner_at};
    use super::fixture_livefeed::{hub_append_lf, read_billing_sum, split_and_await};
    use super::fixture_runtime::RigRuntime;
    let store = mem();
    let (a, addr_a) = http_rig_owner(store.clone(), "inst-a").await;
    let (b, addr_b) = http_rig_owner_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            addr_a,
            "PUT",
            "/v1/streams/xown",
            &credentials,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    hub_append_lf(addr_a, "xown", r#"{"h":0}"#).await;
    hub_append_lf(addr_a, "xown", r#"{"h":1}"#).await;
    split_and_await(&a, "xown", 0).await;
    let sref = b.deployment.raw_adapter_sref("xown");
    b.registry.invalidate(&sref);
    let desc = b.registry.get(&sref).await.unwrap().unwrap();
    let parent = b.shards.prefix_for(&desc.segment_route_by_id(0).unwrap());
    let child = b.shards.prefix_for(&desc.resolve_segment("").shard_route);
    assert_ne!(parent, child, "the fixture must cross physical owners");
    b.ownership
        .set_ring_active(vec!["inst-a".into(), "inst-b".into()]);
    for prefix in b.shards.prefixes().to_vec() {
        b.ownership
            .set_override(&prefix, if prefix == parent { "inst-a" } else { "inst-b" });
    }
    b.peer.set_peer("inst-a", &format!("http://{addr_a}"));
    hub_append_lf(addr_b, "xown", r#"{"h":2}"#).await;
    let mut cursor = "beginning".to_string();
    let mut values = Vec::new();
    for _ in 0..8 {
        let (status, headers, body) = preq(
            addr_b,
            "GET",
            &format!("/v1/streams/xown/records?cursor={cursor}"),
            &credentials,
            b"",
        )
        .await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        values.extend(
            serde_json::from_slice::<Vec<serde_json::Value>>(&body)
                .unwrap()
                .into_iter()
                .map(|value| value["h"].as_u64().unwrap()),
        );
        cursor = headers.get("prisma-next-cursor").unwrap().clone();
        if headers.contains_key("prisma-up-to-date") {
            break;
        }
    }
    assert_eq!(values, vec![0, 1, 2]);
    let (status, headers, body) =
        preq(addr_b, "GET", "/v1/streams/xown:scan", &credentials, b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert!(headers.contains_key("prisma-scan-complete"));
    let scan: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        scan.iter()
            .map(|row| row["value"]["h"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        read_billing_sum(&a),
        (0, 0),
        "internal pages are never separately billed"
    );
    assert_eq!(
        read_billing_sum(&b).1,
        6,
        "three replay and three scan records billed once at delivery"
    );
    engine_shutdown(&a).await;
    engine_shutdown(&b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06_peer_target_is_bound_before_peer_resolution() {
    use crate::application::read_remote::{InternalTarget, RemoteSpanError, remote_span_page};
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(
        preq(
            addr,
            "PUT",
            "/v1/streams/peer-binding",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("peer-binding"))
        .await
        .unwrap()
        .unwrap();
    for component in 0..4 {
        let mut target = InternalTarget::of(&desc, 0).unwrap();
        match component {
            0 => target.project_id = crate::tenant::ProjectId::new("other-project").unwrap(),
            1 => target.stream_epoch[0] ^= 1,
            2 => target.seg_id = u32::MAX,
            _ => target.identity[0] ^= 1,
        }
        assert!(matches!(
            remote_span_page(
                &state.peer,
                "no-such-peer",
                &desc,
                &target,
                crate::application::read::ReadRange::open(0),
                4096,
                PRISMA_KEY
            )
            .await,
            Err(RemoteSpanError::TargetMismatch)
        ));
    }
    let target = InternalTarget::of(&desc, 0).unwrap();
    assert!(matches!(
        remote_span_page(
            &state.peer,
            "no-such-peer",
            &desc,
            &target,
            crate::application::read::ReadRange::open(0),
            4096,
            PRISMA_KEY
        )
        .await,
        Err(RemoteSpanError::Transport(_))
    ));
    engine_shutdown(&state).await;
}

/// A loopback rig that is its own relay target, with `relay-tail` holding
/// two records: the smallest fixture on which a relayed verdict can be
/// compared with the local one.
async fn relay_tail_rig() -> (super::fixture_http::HttpRig, crate::registry::StreamDesc) {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let credentials = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/relay-tail",
            &credentials,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    for n in 0..2 {
        let body = format!("{{\"n\":{n}}}");
        assert_eq!(
            preq(
                rig.addr,
                "POST",
                "/v1/streams/relay-tail/records",
                &credentials,
                body.as_bytes()
            )
            .await
            .0,
            200
        );
    }
    rig.state
        .peer
        .set_peer("relay-owner", &format!("http://{}", rig.addr));
    let desc = rig
        .state
        .registry
        .get(&rig.state.deployment.raw_adapter_sref("relay-tail"))
        .await
        .unwrap()
        .unwrap();
    (rig, desc)
}

/// Review item 22: the owner's read verdict crosses the relay AS ITSELF.
/// An applied read beyond the tail is `CursorBeyondTail` on the owner (the
/// SDK rewinds to its durable cursor); the coordinator used to rebuild a
/// `ChangedIncarnation` from the bare 409, breaking the rewind contract on
/// every cross-owner applied replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relayed_applied_read_beyond_the_tail_keeps_the_owner_verdict() {
    let (rig, desc) = relay_tail_rig().await;
    let state = &rig.state;
    let mut command = command(&desc);
    command.start = ReadStart::Position(ReadPosition {
        segment: 0,
        after: 100,
    });
    command.visibility = crate::shard::Deliver::Applied;
    command.refresh = false;
    let local = state
        .read_service()
        .execute_read(command.clone())
        .await
        .err()
        .expect("an applied read beyond the tail is refused locally");
    assert!(
        matches!(local, ReadFailure::CursorBeyondTail),
        "local verdict: {local:?}"
    );
    let relayed = remote_read_page(&state.peer, "relay-owner", &command, 0, 100)
        .await
        .err()
        .expect("an applied read beyond the tail is refused through the relay");
    assert!(
        matches!(relayed, ReadFailure::CursorBeyondTail),
        "the relayed verdict must be the owner's cursor_beyond_tail: {relayed:?}"
    );
    engine_shutdown(state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}

/// The page route (`streams-internal-read-page: 1`) answers a decided
/// verdict as its typed body; the public route never speaks that
/// vocabulary, header or not, and keeps the public error envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope() {
    let (rig, desc) = relay_tail_rig().await;
    let addr = rig.addr;
    let target = InternalTarget::of(&desc, 0).unwrap();
    let target_headers = target.headers();
    let mut headers = vec![
        ("authorization", "Bearer dst-internal-token"),
        ("stream-encryption-key", PRISMA_KEY),
        ("streams-internal-read-page", "1"),
        ("streams-internal-deliver", "applied"),
        ("streams-internal-max-bytes", "4096"),
    ];
    headers.extend(target_headers.iter().map(|(k, v)| (*k, v.as_str())));
    // next == 100, two records: beyond the tail.
    let offset = crate::offsets::encode(0, 100);
    let (status, _, body) = preq(
        addr,
        "GET",
        &format!("/v1/internal/segment-read/relay-tail?offset={offset}"),
        &headers,
        b"",
    )
    .await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    let reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        reply["refused"], "cursor_beyond_tail",
        "the page route answers the owner's typed verdict, got {reply}"
    );
    // Without the page header the same fleet request keeps the public
    // envelope: the typed body belongs to the page route alone.
    let envelope_headers: Vec<(&str, &str)> = headers
        .iter()
        .copied()
        .filter(|(k, _)| *k != "streams-internal-read-page")
        .collect();
    let (status, _, body) = preq(
        addr,
        "GET",
        &format!("/v1/internal/segment-read/relay-tail?offset={offset}"),
        &envelope_headers,
        b"",
    )
    .await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        envelope["error"]["code"], "cursor_beyond_tail",
        "no page header, no typed body: {envelope}"
    );
    // The public route never speaks the fleet vocabulary, header or not.
    let (status, _, body) = preq(
        addr,
        "GET",
        "/v1/stream/relay-tail",
        &[
            ("stream-encryption-key", PRISMA_KEY),
            ("streams-internal-read-page", "1"),
        ],
        b"",
    )
    .await;
    assert_eq!(status, 200);
    let public: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        public.is_array(),
        "the public route renders records, never the page DTO: {public}"
    );
    // ...and a public refusal keeps the public envelope.
    let (status, _, body) = preq(
        addr,
        "GET",
        "/v1/stream/relay-absent",
        &[("stream-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(status, 404);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["error"]["code"], "not_found", "{envelope}");
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}
