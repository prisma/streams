//! O2-A exact retained allocation oracle through real frozen scan/fork readers.
use super::fixture_http::{HttpRigOptions, cold_absorber, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::application::read_budget::MAX_PAGE_PLAINTEXT;
use crate::application::read_retention_probe::Probe;

async fn rig() -> super::fixture_http::HttpRig {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            shard: crate::shard::ShardConfig {
                frame_compression: crate::crypto::FrameCompression::Disabled,
                tail_ring_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    rig.state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    rig
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o2a_frozen_scan_compacts_retained_owners_across_segments_and_reads() {
    let rig = rig().await;
    let state = &rig.state;
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/subsets",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    super::fixture_livefeed::split_and_await(state, "subsets", 0).await;
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("subsets"))
        .await
        .unwrap()
        .unwrap();
    let segments: Vec<_> = desc
        .segments
        .as_ref()
        .unwrap()
        .live()
        .map(|s| s.seg_id)
        .collect();
    assert_eq!(segments.len(), 2);
    for &seg in &segments {
        seed(state, &desc, seg, b"x", 1).await;
    }
    let cursor = crate::product_cursor::ScanCursor {
        epoch: desc.epoch(),
        map_version: desc.segments.as_ref().unwrap().version,
        segments: segments.iter().map(|s| (*s, 1)).collect(),
        current_index: 0,
        current_offset: 0,
        expires_at_ms: i64::MAX,
    };
    for &seg in &segments {
        seed(state, &desc, seg, &vec![b'y'; 512 << 10], 14).await;
    }
    let command = || crate::application::read_scan::ScanCommand {
        descriptor: desc.clone(),
        key: skey(),
        cursor: Some(cursor.clone()),
        max_bytes: MAX_PAGE_PLAINTEXT,
        now_ms: 0,
        lifetime_ms: 1000,
    };
    let probe = Probe::default();
    let out = probe
        .scope(state.read_service().execute_scan(command()))
        .await
        .unwrap();
    assert!(out.continuation.is_none());
    assert_eq!(
        out.records
            .iter()
            .map(|r| (r.off, r.payload.to_vec()))
            .collect::<Vec<_>>(),
        vec![(0, b"x".to_vec()); 2]
    );
    let live = probe.live();
    let second = probe
        .scope(state.read_service().execute_scan(command()))
        .await
        .unwrap();
    let doubled = probe.live();
    drop(out);
    drop(second);
    assert_eq!(probe.live(), 0, "last owner release must return the charge");
    state
        .peer
        .set_peer("subset-owner", &format!("http://{}", rig.addr));
    for &segment in &segments {
        let target = crate::application::read_remote::InternalTarget::of(&desc, segment).unwrap();
        let remote = probe
            .scope(crate::application::read_remote::remote_span_page(
                &state.peer,
                "subset-owner",
                &desc,
                &target,
                crate::application::read::ReadRange::bounded(0, 1),
                MAX_PAGE_PLAINTEXT,
                PRISMA_KEY,
            ))
            .await
            .unwrap()
            .out;
        assert_eq!(remote.recs.len(), 1);
        assert_eq!(remote.recs[0].payload.as_ref(), b"x");
        assert_eq!(
            remote.end, 15,
            "bounded execution preserves the actual physical frontier"
        );
        assert_eq!(remote.scanned_through(0), 1);
        assert!(remote.completed);
        assert_eq!(probe.live(), 1);
        drop(remote);
        assert_eq!(probe.live(), 0);
    }
    engine_shutdown(state).await;
    rig.tasks.shutdown(std::time::Duration::from_secs(5)).await;
    println!("frozen scan: retained={live}, two held reads={doubled}, budget={MAX_PAGE_PLAINTEXT}");
    assert_eq!(
        live, 2,
        "snapshot bytes must not retain post-snapshot suffixes"
    );
    assert_eq!(doubled, 4);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o2a_clipped_fork_body_holds_only_selected_storage_until_cancel() {
    let rig = rig().await;
    let state = &rig.state;
    assert_eq!(
        hreq(
            rig.addr,
            "PUT",
            "/v1/stream/subset-parent",
            &[("content-type", "application/octet-stream")],
            b"x"
        )
        .await
        .0,
        201
    );
    let (_, headers, _) = hreq(rig.addr, "GET", "/v1/stream/subset-parent", &[], b"").await;
    let boundary = headers.get("stream-next-offset").unwrap();
    assert_eq!(
        hreq(
            rig.addr,
            "PUT",
            "/v1/stream/subset-child",
            &[
                ("content-type", "application/octet-stream"),
                ("stream-forked-from", "subset-parent"),
                ("stream-fork-offset", boundary)
            ],
            b""
        )
        .await
        .0,
        201
    );
    let parent = state
        .registry
        .get(&state.deployment.raw_adapter_sref("subset-parent"))
        .await
        .unwrap()
        .unwrap();
    seed(state, &parent, 0, &vec![b'y'; 512 << 10], 14).await;
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("subset-child"))
        .await
        .unwrap()
        .unwrap();
    let probe = Probe::default();
    let out = probe
        .scope(
            state
                .read_service()
                .execute_read(crate::application::read::ReadCommand {
                    descriptor: child,
                    key: Some(skey()),
                    start: crate::application::read::ReadStart::Beginning,
                    selector: Some(String::new()),
                    mode: crate::application::read::ReadMode::Replay,
                    visibility: crate::shard::Deliver::Durable,
                    max_bytes: MAX_PAGE_PLAINTEXT,
                    tail_max_bytes: MAX_PAGE_PLAINTEXT,
                    allow_remote: true,
                    refresh: false,
                }),
        )
        .await
        .unwrap();
    assert_eq!(out.next.after, 1);
    assert!(out.up_to_date);
    let payload = crate::http::read_payload(&out, false, Some(&skey()), Some(""), false);
    assert_eq!(payload.as_ref(), b"x");
    drop(out);
    let live = probe.live();
    let body = axum::body::Body::from(payload);
    tokio::task::yield_now().await;
    assert_eq!(
        probe.live(),
        live,
        "an unpolled slow body retains its owner"
    );
    drop(body);
    assert_eq!(probe.live(), 0, "body cancellation releases its last owner");
    engine_shutdown(state).await;
    rig.tasks.shutdown(std::time::Duration::from_secs(5)).await;
    println!("clipped fork slow body retained={live}");
    assert_eq!(
        live, 1,
        "the singleton body must not retain the clipped ancestor suffix"
    );
}

async fn seed(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &crate::registry::StreamDesc,
    segment: u32,
    payload: &[u8],
    count: usize,
) {
    let hash = desc.dynamic_segment_identity(segment);
    let route = desc.segment_route_by_id(segment).unwrap();
    let engine = state.engine_for(&route).await.unwrap();
    let (resp, result) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route,
        entries: vec![bytes::Bytes::copy_from_slice(payload); count],
        usage: state.runtime.usage.counters(&route),
        routing_key: String::new(),
        key_hash: crate::crypto::RoutingKeyHash::of("").0,
        producer_lineage: vec![],
        key_version: 0,
        subkey: crate::crypto::derive_subkey(&skey(), &desc.epoch(), "", 0),
        ts_hint_ms: None,
        seq: None,
        bytes: payload.len() * count,
        finish: crate::shard::AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    };
    assert!(engine.try_enqueue(req).is_ok());
    result.await.unwrap().unwrap();
}
