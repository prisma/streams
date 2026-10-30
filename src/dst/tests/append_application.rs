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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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

/// A key in the low quarter: it stays in the low child of a split at the
/// middle and of that child's split at the quarter, on the root's route.
fn low_key() -> String {
    (0..)
        .map(|n| format!("k{n}"))
        .find(|k| crate::registry::StreamDesc::key_point(k) < 1 << 61)
        .unwrap()
}

/// Stamp the root 10 s after its newest descendant, as when the instance
/// that recorded the first split ran ahead of those that published it.
async fn skew_the_root(
    state: &crate::http::AppState,
    sref: &crate::tenant::TenantStreamRef,
) -> crate::registry::StreamDesc {
    let skewed = state.registry.update(sref, |desc| {
        let map = desc.segments.as_mut().expect("a split map");
        let newest = map.segments.iter().map(|s| s.created_ms).max().unwrap_or(0);
        let root = map
            .segments
            .iter_mut()
            .find(|s| s.seg_id == 0)
            .expect("the root");
        root.created_ms = newest + 10_000;
    });
    skewed.await.unwrap().expect("the stream")
}

/// An append of `seq` under `key`, as producer "skewed" or as Stream-Seq.
fn keyed(
    desc: &crate::registry::StreamDesc,
    key: &str,
    seq: u64,
    stream_seq: bool,
) -> AppendCommand {
    let body: &'static [u8] = br#"{"n":1}"#;
    let hash = crate::application::append::product_request_hash(
        false,
        key,
        &desc.content_type,
        body,
        false,
    );
    let mut cmd = command(desc, "skewed", body);
    cmd.routing_key = key.into();
    cmd.request_hash = Some(hash);
    if stream_seq {
        cmd.producer = None;
        cmd.sequence = Some(format!("{seq:03}"));
    } else if let Some(producer) = cmd.producer.as_mut() {
        producer.seq = seq;
    }
    cmd
}

/// Producer and Stream-Seq dedupe read the nearest sealed predecessor's row
/// first, and "nearest" is lineage, not the wall clock that stamped
/// `created_ms`. With the root stamped after its grandchildren, a retry
/// across two splits must meet the row its first attempt wrote in the
/// child: a producer retry answers as the duplicate it is, and a Stream-Seq
/// retry is refused as a conflict, instead of either being written twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_retry_across_two_splits_meets_its_row_when_the_clocks_disagree() {
    let (state, addr) = http_rig(mem()).await;
    let app = state.append_service();
    let key = low_key();
    for (name, stream_seq) in [("skew-producer", false), ("skew-sequence", true)] {
        let create = br#"{"format":{"kind":"json"}}"#;
        let headers = [("prisma-encryption-key", PRISMA_KEY)];
        let path = format!("/v1/streams/{name}");
        assert_eq!(preq(addr, "PUT", &path, &headers, create).await.0, 201);
        let sref = state.deployment.raw_adapter_sref(name);
        let fresh = || async {
            state.registry.invalidate(&sref);
            state.registry.get(&sref).await.unwrap().unwrap()
        };
        let first = app
            .execute(keyed(&fresh().await, &key, 0, stream_seq))
            .await;
        assert!(!first.unwrap().duplicate, "{name}");
        assert!(crate::scaler3::execute_split(&state, &sref, 0, 1 << 63).await);
        let child = app
            .execute(keyed(&fresh().await, &key, 1, stream_seq))
            .await
            .unwrap();
        assert!(!child.duplicate && child.seg_id != 0, "{name}");
        assert!(crate::scaler3::execute_split(&state, &sref, child.seg_id, 1 << 62).await);
        let desc = skew_the_root(&state, &sref).await;
        let retry = app.execute(keyed(&desc, &key, 1, stream_seq)).await;
        let answer = match retry {
            Ok(out) => Ok(out.duplicate),
            Err(error) => Err(error.code),
        };
        let expected = if stream_seq {
            Err(AppendCode::SeqConflict)
        } else {
            Ok(true)
        };
        assert_eq!(
            answer, expected,
            "{name}: the child's row answers the retry"
        );
    }
    engine_shutdown(&state).await;
}

/// A reply as the wire gave it: status, lowercased headers, body.
type Reply = (u16, std::collections::HashMap<String, String>, Vec<u8>);

/// W3: runs `request`, an append to `name`, parks it after its shard engine
/// resolved, and retires that engine as the sweep's eviction does (remove,
/// arm the holdoff, close) before letting it reach its enqueue. Then waits
/// for the retired engine to terminate and lifts the holdoff, so the caller
/// can retry at once and read what the refused append left.
async fn closed_under(
    state: &std::sync::Arc<crate::http::AppState>,
    name: &str,
    request: impl std::future::Future<Output = Reply>,
) -> Reply {
    use super::fixture_livefeed::wait_parked;
    use crate::shard_directory::{RetireOutcome, RetirementReason};
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state
        .shards
        .prefix_for(&desc.resolve_segment("").shard_route);
    crate::failpoints::park_append_after_resolve(name);
    let retire = async {
        wait_parked(crate::failpoints::Fp::AppendAfterResolve, name, 1).await;
        let retired = state
            .shards
            .retire(&prefix, RetirementReason::SweepEviction, |_, _| true);
        let RetireOutcome::Retired(engine) = retired else {
            panic!("the engine serving {name} was not retired");
        };
        assert!(engine.is_closed(), "the retirement closed {prefix:?}");
        crate::failpoints::release_append_after_resolve(name);
        engine.shutdown_handle()
    };
    let (reply, shutdown) = futures_util::future::join(request, retire).await;
    for _ in 0..1000 {
        if shutdown.terminated() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(shutdown.terminated(), "the retired engine terminates");
    state.shards.clear_holdoff(&prefix);
    reply
}

/// The answer's status, `Retry-After` and JSON body, in one comparable tuple.
fn answer(reply: &Reply) -> (u16, Option<&str>, serde_json::Value) {
    let (status, headers, body) = reply;
    let body = serde_json::from_slice(body)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned().into());
    (
        *status,
        headers.get("retry-after").map(String::as_str),
        body,
    )
}

/// W3 over the raw wire: an append whose shard engine is retired between
/// its resolve and its enqueue is answered as the moved shard it met, 503
/// `shard_moving` with `Retry-After: 1` (as the committer-stranded group
/// and the holdoff after it are), never as a rate limit. Nothing was
/// written: the retry is the stream's first record. Red before: 429
/// `overloaded` "append queue full" with no `Retry-After`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_append_whose_engine_closes_between_resolve_and_enqueue_is_answered_as_a_moved_shard() {
    use super::fixture_requests::hreq;
    let (state, addr) = http_rig(mem()).await;
    let name = "w3-closed";
    let path = format!("/v1/stream/{name}");
    let json = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr, "PUT", &path, &json, b"").await;
    assert!(status == 200 || status == 201, "create {name}: {status}");
    let request = hreq(addr, "POST", &path, &json, br#"[{"n":1}]"#);
    let reply = closed_under(&state, name, request).await;
    let message = "shard closed under the request; nothing was written; retry";
    assert_eq!(
        answer(&reply),
        (
            503,
            Some("1"),
            serde_json::json!({"error": {"code": "shard_moving", "message": message}})
        )
    );
    let (status, headers, _) = hreq(addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
    let first = crate::http::tail_token(1);
    assert_eq!(
        (status, headers.get("stream-next-offset")),
        (204, Some(&first)),
        "the retry is the stream's first record: {headers:?}"
    );
    let (status, _, body) = hreq(addr, "GET", &path, &[], b"").await;
    let records: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!((status, records), (200, serde_json::json!([{"n": 1}])));
    engine_shutdown(&state).await;
}

/// W3 over the product surface: the same closed engine is the product's
/// 503 `temporarily_unavailable`, retryable, `Retry-After: 1`, as every
/// other raw 503; the retry lands once. Red before: 429 `rate_limited`
/// "admission or rate limit" with no `Retry-After`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_product_append_whose_engine_closes_between_resolve_and_enqueue_is_temporarily_unavailable()
 {
    let (state, addr) = http_rig(mem()).await;
    let name = "w3-closed-p";
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let path = format!("/v1/streams/{name}");
    let format = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(preq(addr, "PUT", &path, &key, format).await.0, 201);
    let records = format!("{path}/records");
    let request = preq(addr, "POST", &records, &key, br#"{"n":1}"#);
    let reply = closed_under(&state, name, request).await;
    assert_eq!(
        answer(&reply),
        (
            503,
            Some("1"),
            serde_json::json!({"error": {
                "code": "temporarily_unavailable",
                "message": "retry shortly",
                "retryable": true,
            }})
        )
    );
    let (status, _, body) = preq(addr, "POST", &records, &key, br#"{"n":1}"#).await;
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (status, &body["count"], &body["duplicate"]),
        (200, &serde_json::json!(1), &serde_json::json!(false)),
        "the retry lands once: {body}"
    );
    let (status, _, body) = preq(addr, "GET", &records, &key, b"").await;
    let stored: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!((status, stored), (200, serde_json::json!([{"n": 1}])));
    engine_shutdown(&state).await;
}

/// The append path keeps its own RSS gate beside the product entry's
/// (edge change #183): a raw append over the shed line is refused
/// 429 with `Retry-After: 2` and writes nothing, and under the line the
/// same append is the stream's first record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_raw_append_over_the_shed_line_is_refused_by_the_append_paths_gate() {
    use super::fixture_requests::hreq;
    let (state, addr) = http_rig(mem()).await;
    let path = "/v1/stream/shed";
    let json = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr, "PUT", path, &json, b"").await;
    assert!(status == 200 || status == 201, "create: {status}");
    state.admission.set_rss_shed_mb(500);
    state.admission.record_rss_mb(501);
    let (status, headers, _) = hreq(addr, "POST", path, &json, br#"[{"n":1}]"#).await;
    let retry = headers.get("retry-after").map(String::as_str);
    assert_eq!((status, retry), (429, Some("2")));
    state.admission.record_rss_mb(0);
    let (status, headers, _) = hreq(addr, "POST", path, &json, br#"[{"n":1}]"#).await;
    let first = crate::http::tail_token(1);
    assert_eq!(
        (status, headers.get("stream-next-offset")),
        (204, Some(&first))
    );
    engine_shutdown(&state).await;
}
