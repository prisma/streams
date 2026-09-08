//! R06 application positions and protocol adapters use the same consumed range.
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, skey};
use crate::application::read::{ReadCommand, ReadFailure, ReadMode, ReadPosition, ReadStart};

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
        crate::offsets::Offset::parse(headers.get("stream-next-offset").unwrap())
            .unwrap()
            .scan_from(),
        out.next.after
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
