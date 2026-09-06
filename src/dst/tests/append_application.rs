//! Typed append ownership and incarnation/duplicate boundaries (R02).
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, skey};
use crate::application::append::{AppendCode, AppendCommand};
use bytes::Bytes;

fn command(
    desc: &crate::registry::StreamDesc,
    producer: &str,
    value: &'static [u8],
) -> AppendCommand {
    AppendCommand {
        sref: desc.sref(),
        expected_epoch: Some(desc.epoch()),
        key: skey(),
        body: Bytes::from_static(value),
        producer: Some(crate::shard::ProducerReq {
            id: producer.into(),
            epoch: 1,
            seq: 0,
            request_hash: None,
        }),
        content_type: Some(desc.content_type.clone()),
        routing_key: String::new(),
        close: false,
        seal_auth: None,
        request_hash: Some(crate::application::append::product_request_hash(
            false,
            "",
            &desc.content_type,
            value,
            false,
        )),
        sequence: None,
        ts_hint_ms: None,
        key_version: 0,
        close_identity: None,
        body_charge: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_typed_append_preserves_original_duplicate_offsets() {
    let (state, addr) = http_rig(mem()).await;
    let (status, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typed-append",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("typed-append"))
        .await
        .unwrap()
        .unwrap();
    let app = state.append_service();
    let first = app
        .execute(command(&desc, "first", br#"{"n":1}"#))
        .await
        .unwrap();
    assert_eq!(
        (
            first.seg_id,
            first.next_offset,
            first.last_offset,
            first.duplicate,
            first.closed
        ),
        (0, 1, 0, false, false)
    );
    let second = app
        .execute(command(&desc, "second", br#"{"n":2}"#))
        .await
        .unwrap();
    assert_eq!(second.next_offset, 2);
    let retry = app
        .execute(command(&desc, "first", br#"{"n":1}"#))
        .await
        .unwrap();
    assert!(retry.duplicate);
    assert_eq!(
        retry.last_offset, 0,
        "the original durable result survives newer appends"
    );
    let (engine, handle) = state.read_service().handle_of(&desc).await.unwrap();
    let page = crate::application::read::read_merged(
        &skey(),
        &desc.epoch(),
        &handle,
        &engine,
        0,
        None,
        4096,
        crate::shard::Deliver::Durable,
    )
    .await
    .unwrap();
    assert_eq!(
        page.recs.len(),
        2,
        "the duplicate publishes no additional record"
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_append_target_fence_rejects_same_name_recreation() {
    let (state, addr) = http_rig(mem()).await;
    let path = "/v1/streams/typed-target";
    let create = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(
        preq(
            addr,
            "PUT",
            path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            create
        )
        .await
        .0,
        201
    );
    let sref = state.deployment.raw_adapter_sref("typed-target");
    let old = state.registry.get(&sref).await.unwrap().unwrap();
    let stale = command(&old, "dlq-delivery", br#"{"old":true}"#);
    assert_eq!(
        preq(
            addr,
            "DELETE",
            path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            b""
        )
        .await
        .0,
        204
    );
    assert_eq!(
        preq(
            addr,
            "PUT",
            path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            create
        )
        .await
        .0,
        201
    );
    let fresh = state.registry.get(&sref).await.unwrap().unwrap();
    assert_ne!(fresh.epoch(), old.epoch());
    let error = state.append_service().execute(stale).await.unwrap_err();
    assert_eq!(error.code, AppendCode::TargetIncarnationChanged);
    let (_, handle) = state.read_service().handle_of(&fresh).await.unwrap();
    assert_eq!(
        handle.state.lock().unwrap().durable.next,
        0,
        "the replacement received no stale write"
    );
    engine_shutdown(&state).await;
}
