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

/// F3: the re-preparation after a waited-out transition is the retry
/// loop's second refresh. A registry it cannot read proves nothing, as
/// the closure check's cannot: the append, refused as closed by every
/// attempt and so uncommitted, answers the same retryable 503, never a
/// 500, and its retry lands once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal() {
    use super::fixture_livefeed::wait_parked;
    use crate::application::append::FailureClass;
    use crate::failpoints::Fp::ScalerBeforePublish;
    let (state, addr) = http_rig(mem()).await;
    let name = "typed-reprepare";
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(addr, "PUT", "/v1/streams/typed-reprepare", &headers, create).await;
    assert_eq!(status, 201);
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let app = state.append_service();
    crate::failpoints::arm_scaler_before_publish(name);
    let held = super::fixture_failpoints::FailpointGuard(name.to_string());
    let split = crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000);
    let append = async {
        wait_parked(ScalerBeforePublish, name, 1).await;
        app.execute(command(&desc, "reprepare", br#"{"n":1}"#))
            .await
    };
    let release = async {
        wait_parked(ScalerBeforePublish, name, 2).await;
        state.registry.fail_next_get(name);
        drop(held);
    };
    let (_, answer, ()) = futures_util::future::join3(split, append, release).await;
    let error = answer.unwrap_err();
    assert!(
        error.message.contains("injected registry get failure"),
        "{error:?}"
    );
    assert_eq!(
        (error.class, error.code, error.retry_after),
        (
            FailureClass::Unavailable,
            AppendCode::SegmentTransition,
            Some(1)
        ),
        "{error:?}"
    );
    state.registry.invalidate(&sref);
    let published = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(
        published
            .segments
            .as_ref()
            .is_some_and(|m| m.pending.is_none())
    );
    let landed = app
        .execute(command(&desc, "reprepare", br#"{"n":1}"#))
        .await
        .unwrap();
    assert!(!landed.duplicate, "the refused append committed nothing");
    assert_ne!(landed.seg_id, 0, "the retry lands on a child");
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

/// A raw close whose seal intent could not be installed has not sealed
/// anything. Another seal's live claim is a conflict and stays the 409
/// `sealed`. A registry the intent could not read (here, the lapsed
/// final's takeover re-reading the descriptor to fence it) decided
/// nothing: the close answers the retryable 503 `seal_incomplete` its
/// completion answers, and its retry closes the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r02_a_close_whose_intent_cannot_read_the_registry_is_retryable_not_sealed() {
    use crate::application::append::FailureClass;
    let (state, addr) = http_rig(mem()).await;
    let name = "typed-close-read";
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typed-close-read",
        &headers,
        create,
    )
    .await;
    assert_eq!(status, 201);
    let sref = state.deployment.raw_adapter_sref(name);
    let owed = |claimed_ms| {
        move |d: &mut crate::registry::PersistedDescriptor| {
            d.seal_gen_counter += 1;
            d.sealing = Some(crate::registry::SealState {
                operation_id: "owed-final".into(),
                intent: crate::registry::SealIntent::Final {
                    routing_key: String::new(),
                    request_hash: "owed-final".into(),
                    final_committed: false,
                },
                claimed_ms,
                claim_generation: d.seal_gen_counter,
            });
            true
        }
    };
    let close = |desc: &crate::registry::StreamDesc| AppendCommand {
        body: Bytes::new(),
        producer: None,
        close: true,
        request_hash: None,
        ..command(desc, "close", b"")
    };
    let app = state.append_service();
    let now = crate::shard::now_ms();
    assert!(state.registry.cas_update(&sref, owed(now)).await.unwrap());
    state.registry.invalidate(&sref);
    let live = state.registry.get(&sref).await.unwrap().unwrap();
    let conflict = app.execute(close(&live)).await.unwrap_err();
    assert_eq!(
        (conflict.class, conflict.code),
        (FailureClass::Conflict, AppendCode::Sealed),
        "{conflict:?}"
    );

    let lapsed = now - crate::registry::SEAL_CLAIM_MS - 1_000;
    assert!(
        state
            .registry
            .cas_update(&sref, owed(lapsed))
            .await
            .unwrap()
    );
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prepared = app
        .prepare(&sref, AppendKey::Provided(skey()))
        .await
        .unwrap();
    state.registry.fail_next_get(name);
    let error = app
        .execute_prepared(prepared, close(&desc))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("injected registry get failure"),
        "{error:?}"
    );
    assert_eq!(
        (error.class, error.code),
        (FailureClass::Unavailable, AppendCode::SealIncomplete),
        "{error:?}"
    );
    state.registry.invalidate(&sref);
    assert!(!state.registry.get(&sref).await.unwrap().unwrap().sealed);
    let closed = app.execute(close(&desc)).await.unwrap();
    assert!(closed.closed, "the retry closes the collection");
    state.registry.invalidate(&sref);
    assert!(state.registry.get(&sref).await.unwrap().unwrap().sealed);
    engine_shutdown(&state).await;
}

/// NEXT-WORK item 3 (the owner's typed classification, ratifying #54): an
/// append's FIRST registry read that fails on the store has written
/// nothing, so it answers the retryable 503 (raw `internal`, product
/// `temporarily_unavailable` retryable) with `Retry-After: 1`; a descriptor
/// that was read but is corrupt stays a fail-closed 500, since a retry
/// reads the same bytes. Red before: the store failure answered
/// `(Internal, Internal, None)` like corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r02_a_first_read_the_store_fails_is_retryable_and_a_corrupt_descriptor_is_not() {
    use crate::application::append::FailureClass;
    use object_store::ObjectStoreExt;
    let (state, addr) = http_rig(mem()).await;
    let name = "typed-first-read";
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typed-first-read",
        &headers,
        create,
    )
    .await;
    assert_eq!(status, 201);
    let sref = state.deployment.raw_adapter_sref(name);
    let app = state.append_service();

    state.registry.fail_next_get(name);
    let transient = app
        .prepare(&sref, AppendKey::Provided(skey()))
        .await
        .err()
        .expect("the store failure refuses the append");
    assert!(
        transient.message.contains("injected registry get failure"),
        "{transient:?}"
    );
    assert_eq!(
        (transient.class, transient.code, transient.retry_after),
        (FailureClass::Unavailable, AppendCode::Internal, Some(1)),
        "{transient:?}"
    );

    // Corrupt the stored descriptor itself: the same read is final.
    let path = format!(
        "registry/v4/projects/{}/streams/{}.json",
        crate::crypto::hex(sref.project_id().as_bytes()),
        crate::crypto::hex(name.as_bytes())
    );
    let path = object_store::path::Path::from(path);
    assert!(
        state.data_store.head(&path).await.is_ok(),
        "the descriptor's path"
    );
    state
        .data_store
        .put(&path, bytes::Bytes::from_static(b"{ not json").into())
        .await
        .unwrap();
    state.registry.invalidate(&sref);
    let corrupt = app
        .prepare(&sref, AppendKey::Provided(skey()))
        .await
        .err()
        .expect("a corrupt descriptor refuses the append");
    assert_eq!(
        (corrupt.class, corrupt.code, corrupt.retry_after),
        (FailureClass::Internal, AppendCode::Internal, None),
        "{corrupt:?}"
    );
    engine_shutdown(&state).await;
}
