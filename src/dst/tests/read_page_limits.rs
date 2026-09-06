//! R06-A real append -> compressed ring/DB -> local page -> HTTP peer receiver.
use super::fixture_http::{HttpRigOptions, cold_absorber, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::application::read::{ReadCommand, ReadMode, ReadPosition, ReadStart};
use crate::application::read_remote::{InternalTarget, remote_read_page, remote_span_page};

fn command(desc: &crate::registry::StreamDesc, from: u64) -> ReadCommand {
    ReadCommand {
        descriptor: desc.clone(),
        key: Some(skey()),
        start: ReadStart::Position(ReadPosition {
            segment: 0,
            after: from,
        }),
        selector: Some(String::new()),
        mode: ReadMode::Replay,
        visibility: crate::shard::Deliver::Durable,
        max_bytes: 64 << 10,
        tail_max_bytes: 64 << 10,
        allow_remote: true,
        refresh: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r06a_compressed_local_and_peer_pages_have_identical_complete_sequences() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            max_request_body_bytes: Some(32 << 20),
            shard: crate::shard::ShardConfig {
                frame_compression: crate::crypto::FrameCompression::ZstdLevel1,
                tail_ring_bytes: 4 << 20,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    let state = &rig.state;
    // Recovery backdates discovered debt, so age thresholds alone cannot
    // keep this tail-page fixture cold under a delayed startup schedule.
    state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/compressed-peer",
            &headers,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("compressed-peer"))
        .await
        .unwrap()
        .unwrap();
    let payload = format!("\"{}\"", "x".repeat((16 << 10) - 2));
    seed_records(state, &desc, payload.as_bytes(), 1600).await;
    state
        .peer
        .set_peer("r06a-owner", &format!("http://{}", rig.addr));
    let mut from = 0;
    let mut seen = Vec::new();
    while from < 1600 {
        let command = command(&desc, from);
        let local = state
            .read_service()
            .execute_read(command.clone())
            .await
            .unwrap();
        let remote = remote_read_page(&state.peer, "r06a-owner", &command, 0, from)
            .await
            .unwrap();
        assert_eq!(local.records.len(), 4);
        assert_eq!(remote.records.len(), local.records.len());
        assert_eq!(local.next, remote.next);
        assert_eq!(remote.next.after, from + 4);
        assert_eq!(remote.up_to_date, from + 4 == 1600);
        for (left, right) in local.records.iter().zip(&remote.records) {
            assert_eq!((left.off, &left.payload), (right.off, &right.payload));
            assert_eq!(right.payload.as_ref(), payload.as_bytes());
            seen.push(right.off);
        }
        from = remote.next.after;
    }
    assert_eq!(seen, (0..1600).collect::<Vec<_>>());
    // The first-record exception also works over both actual peer formats.
    // Its real base64 body crosses the previous 24 MiB receiver ceiling.
    let large = format!("\"{}\"", "z".repeat(20 << 20));
    seed_records(state, &desc, large.as_bytes(), 1).await;
    let command = command(&desc, 1600);
    let remote = remote_read_page(&state.peer, "r06a-owner", &command, 0, 1600)
        .await
        .unwrap();
    assert_eq!(remote.records.len(), 1);
    assert_eq!(remote.records[0].payload.as_ref(), large.as_bytes());
    assert_eq!(remote.next.after, 1601);
    let scan = remote_span_page(
        &state.peer,
        "r06a-owner",
        &desc,
        &InternalTarget::of(&desc, 0).unwrap(),
        1600,
        64 << 10,
        PRISMA_KEY,
    )
    .await
    .unwrap();
    assert_eq!(scan.out.recs[0].payload, remote.records[0].payload);
    assert_eq!(scan.out.scanned_through(1600), 1601);
    engine_shutdown(state).await;
    rig.tasks.shutdown(std::time::Duration::from_secs(5)).await;
}

// Seed through the real committer/encryption/WAL path. The read contract also
// covers retained records larger than the default per-stream ingest bucket;
// that unrelated bucket must not decide which paging cases this test reaches.
async fn seed_records(
    state: &std::sync::Arc<crate::http::AppState>,
    desc: &crate::registry::StreamDesc,
    payload: &[u8],
    count: usize,
) {
    let hash = desc.dynamic_segment_identity(0);
    let route = desc.segment_route_by_id(0).unwrap();
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
