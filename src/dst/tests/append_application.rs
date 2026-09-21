//! Typed append ownership and incarnation/duplicate boundaries (R02).
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, skey};
use crate::application::append::{AppendCode, AppendCommand, AppendKey};
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

/// A refresh that cannot be read proves nothing. The closure a
/// split-away parent's engine reports must then reach the writer as
/// retryable, never as the collection's own, and the retry lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final() {
    let (state, addr) = http_rig(mem()).await;
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(addr, "PUT", "/v1/streams/typed-unproven", &headers, create).await;
    assert_eq!(status, 201);
    let sref = state.deployment.raw_adapter_sref("typed-unproven");
    let pre_split = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000).await);

    let app = state.append_service();
    state.registry.test_poison_cache(&sref, pre_split.clone());
    let stale = command(&pre_split, "unproven", br#"{"n":1}"#);
    let prepared = app
        .prepare(&sref, AppendKey::Provided(stale.key.clone()))
        .await
        .unwrap();
    state.registry.fail_next_get("typed-unproven");
    let error = app.execute_prepared(prepared, stale).await.unwrap_err();
    assert_eq!(error.code, AppendCode::SegmentTransition, "{error:?}");
    assert!(error.retry_after.is_some(), "unproven closure is retryable");

    state.registry.test_poison_cache(&sref, pre_split.clone());
    let landed = app
        .execute(command(&pre_split, "unproven", br#"{"n":1}"#))
        .await
        .unwrap();
    assert_ne!(landed.seg_id, 0, "the retry lands on the child");
    engine_shutdown(&state).await;
}

/// A sealed descriptor's closure is final by itself: `sealed` never
/// resets within an incarnation and freezes the map, so its route cannot
/// be stale. The engine's refusal must cost no descriptor refresh (a
/// refresh here would meet the armed store failure and answer 503).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_a_sealed_descriptors_closure_costs_no_refresh() {
    let (state, addr) = http_rig(mem()).await;
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(addr, "PUT", "/v1/streams/typed-sealed", &headers, create).await;
    assert_eq!(status, 201);
    super::fixture_livefeed::seal_ok(addr, "typed-sealed").await;
    let sref = state.deployment.raw_adapter_sref("typed-sealed");
    state.registry.invalidate(&sref);
    let sealed = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(sealed.sealed);

    let app = state.append_service();
    let late = command(&sealed, "late", br#"{"n":1}"#);
    let prepared = app
        .prepare(&sref, AppendKey::Provided(late.key.clone()))
        .await
        .unwrap();
    state.registry.fail_next_get("typed-sealed");
    let error = app.execute_prepared(prepared, late).await.unwrap_err();
    assert_eq!(error.code, AppendCode::StreamClosed, "{error:?}");
    engine_shutdown(&state).await;
}

/// The refresh is fenced on the incarnation, as the reads' is: a closure
/// met through a descriptor of the PREVIOUS incarnation says nothing
/// about the stream that now holds the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_a_closure_from_a_replaced_incarnation_is_not_the_new_streams() {
    let (state, addr) = http_rig(mem()).await;
    let path = "/v1/streams/typed-replaced";
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(preq(addr, "PUT", path, &headers, create).await.0, 201);
    let sref = state.deployment.raw_adapter_sref("typed-replaced");
    let old = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000).await);
    assert_eq!(preq(addr, "DELETE", path, &headers, b"").await.0, 204);
    assert_eq!(preq(addr, "PUT", path, &headers, create).await.0, 201);

    state.registry.test_poison_cache(&sref, old.clone());
    let stale = command(&old, "replaced", br#"{"n":1}"#);
    let error = state.append_service().execute(stale).await.unwrap_err();
    assert_eq!(
        error.code,
        AppendCode::TargetIncarnationChanged,
        "{error:?}"
    );
    engine_shutdown(&state).await;
}
