//! Cancellation and typed final-record verdicts at the seal coordinator.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_livefeed::wait_parked;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::application::lifecycle::{
    FinalDisposition, FinalRecordFailure, FinalSealRequest, SealFinalError, seal_final,
};

#[expect(
    clippy::disallowed_methods,
    reason = "seal cancellation fixture; the test aborts and joins the entered final operation before retrying its retained claim; a detached request would make the retry race with unknown work"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_final_preserves_claim_and_only_definitive_retry_releases_it() {
    let (state, addr) = http_rig(mem()).await;
    let (status, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/cancel-final",
        &[("content-type", "application/json")],
        b"[]",
    )
    .await;
    assert!(status == 200 || status == 201);
    let stream = state.deployment.raw_adapter_sref("cancel-final");
    let epoch = state
        .registry
        .get(&stream)
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();
    let service = state.lifecycle_service();
    let (entered, observed) = tokio::sync::oneshot::channel();
    let task_service = service.clone();
    let task_stream = stream.clone();
    let task_epoch = epoch.clone();
    let task = tokio::spawn(async move {
        seal_final(
            &task_service,
            FinalSealRequest {
                stream: &task_stream,
                epoch: &task_epoch,
                operation: "cancelled-op",
                routing_key: "",
            },
            |_authority| async move {
                entered
                    .send(())
                    .expect("claim observer must still be waiting");
                std::future::pending::<Result<_, FinalRecordFailure<()>>>().await
            },
        )
        .await
    });
    observed.await.unwrap(); // The claim CAS is durable before append is called.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    state.registry.invalidate(&stream);
    let original = state.registry.get(&stream).await.unwrap().unwrap();
    let generation = original.sealing.as_ref().unwrap().claim_generation;
    assert!(original.sealing.as_ref().unwrap().owes_final());

    for disposition in [
        FinalDisposition::AmbiguousOrTransient,
        FinalDisposition::DefinitivelyRejected,
    ] {
        let result = seal_final(
            &service,
            FinalSealRequest {
                stream: &stream,
                epoch: &epoch,
                operation: "cancelled-op",
                routing_key: "",
            },
            |authority| async move {
                assert!(authority.generation > generation);
                Err(FinalRecordFailure {
                    error: (),
                    disposition,
                })
            },
        )
        .await;
        assert!(matches!(result, Err(SealFinalError::Append(()))));
        state.registry.invalidate(&stream);
        let descriptor = state.registry.get(&stream).await.unwrap().unwrap();
        assert_eq!(
            descriptor.sealing.is_some(),
            disposition == FinalDisposition::AmbiguousOrTransient
        );
        assert!(!descriptor.sealed);
    }
    engine_shutdown(&state).await;
}

/// A product seal whose final record exceeds the per-record ceiling is
/// refused before it publishes its seal intent (TLA-003-F3). The claim
/// would put the collection in Sealing for a record the append must
/// refuse, so the request may never reach the claim-to-append gap. The
/// ceiling is measured on the record the append stores: a JSON collection
/// stores the value re-encoded, and serde_json's default float parse
/// lengthens `ceilfloat`'s value by a digit, so its own text fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_over_ceiling_product_final_is_refused_before_its_seal_intent() {
    let _serial = gap_lock().lock().await;
    let (state, addr) = http_rig(mem()).await;
    for (name, value, ceiling) in [
        ("ceilfin", format!(r#"{{"pad":"{}"}}"#, "x".repeat(90)), 64),
        ("ceilfloat", r#"{"f":1.7802719962921167e-19}"#.into(), 27),
    ] {
        let text = serde_json::from_str::<serde_json::Value>(&value)
            .unwrap()
            .to_string();
        let stored = serde_json::from_str::<serde_json::Value>(&text)
            .unwrap()
            .to_string();
        assert!(stored.len() > ceiling, "{name}: {stored} fits {ceiling}");
        assert_eq!(text.len() <= ceiling, name == "ceilfloat", "{name}: {text}");
        seal_is_refused_before_its_intent(&state, addr, name, &value, ceiling).await;
    }
    engine_shutdown(&state).await;
}

/// Seals a new JSON collection `name` with `value` as its final record
/// under `ceiling`, the final append parked, and asserts a 4xx answer with
/// no arrival at the claim-to-append gap and no claim ever observed.
async fn seal_is_refused_before_its_intent(
    state: &crate::http::AppState,
    addr: std::net::SocketAddr,
    name: &str,
    value: &str,
    ceiling: usize,
) {
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let point = crate::failpoints::Fp::ProductFinalBeforeAppend;
    let path = format!("/v1/streams/{name}");
    let format = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(preq(addr, "PUT", &path, &key, format).await.0, 201);
    state.admission.set_record_ceiling(ceiling);
    let stream = state.deployment.raw_adapter_sref(name);
    let before = crate::failpoints::parked(point, name);
    crate::failpoints::park_product_final_before_append(name);
    let body = format!(r#"{{"final":{value}}}"#);
    let seal = format!("{path}:seal");
    let mut request = std::pin::pin!(preq(addr, "POST", &seal, &key, body.as_bytes()));
    let mut claimed = None;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let (status, _, answer) = loop {
        tokio::select! {
            answer = &mut request => break answer,
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
        assert!(tokio::time::Instant::now() < deadline, "{name}: no answer");
        if claimed.is_none() && crate::failpoints::parked(point, name) > before {
            state.registry.invalidate(&stream);
            let descriptor = state.registry.get(&stream).await.unwrap().unwrap();
            claimed = Some(descriptor.sealing.clone());
            crate::failpoints::release_product_final_before_append(name);
        }
    };
    crate::failpoints::release_product_final_before_append(name);
    let arrivals = crate::failpoints::parked(point, name) - before;
    let answer = String::from_utf8_lossy(&answer);
    assert!(
        (400..500).contains(&status),
        "{name} answered {status}: {answer}"
    );
    assert_eq!(
        (arrivals, claimed),
        (0, None),
        "{name}: the over-ceiling final reached its append holding a published seal intent (answer {status}: {answer})"
    );
    state.registry.invalidate(&stream);
    let descriptor = state.registry.get(&stream).await.unwrap().unwrap();
    assert!(descriptor.sealing.is_none() && !descriptor.sealed, "{name}");
}

/// TLA-003-F4: a raw close-with-content claims its final and parks before
/// its enqueue on instance A; its exact retry lands on instance B, whose
/// ingest capacity cannot hold the request. B's 413 must leave A's claim
/// exactly as A installed it, so A's final commits, marks and seals under
/// its own generation. The retry used to renew the claim before its content
/// was validated: A's durable final then failed its mark (503) and the
/// collection stayed Sealing under a lease the refused retry had refreshed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ingest_refused_exact_retry_leaves_the_claim_to_its_original() {
    let _serial = gap_lock().lock().await;
    let admission = crate::config::AdmissionConfig {
        limit_recs_per_sec: 1.0,
        limit_burst_secs: 1.0,
        ..Default::default()
    };
    let refused = original_survives_a_refused_retry("ingestskew", Some(admission), None).await;
    assert_eq!(refused, 413, "B's ingest capacity did not refuse the retry");
}

/// TLA-003-F5: as above, but B's per-record ceiling is below the final's
/// record, and B owns the shard when the retry arrives. The retry's record
/// is a deferred refusal the committer answers definitively; that refusal
/// proves nothing about A's attempt, which A's own ceiling admitted. It must
/// neither renew nor release A's claim. It used to do both: the release left
/// A's final to close the segment with no claim standing, answered 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ceiling_refused_exact_retry_leaves_the_claim_to_its_original() {
    let _serial = gap_lock().lock().await;
    let refused = original_survives_a_refused_retry("ceilskew", None, Some(8)).await;
    assert!(
        (400..500).contains(&refused),
        "B's ceiling did not refuse the retry"
    );
}

/// The replay half of TLA-003-F4/F5: a close-with-content whose final is
/// durable but unmarked on instance A is retried on instance B, whose
/// per-record ceiling is below the record. The retry must not be refused
/// ahead of the committer's duplicate decision: it is acknowledged as the
/// committed final, marks it under the claim it observed, and seals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ceiling_limited_exact_retry_still_completes_a_committed_final() {
    let _serial = gap_lock().lock().await;
    let (name, path) = ("ceilreplay", "/v1/stream/ceilreplay");
    let store = mem();
    let (a, addr_a) = http_rig(store.clone()).await;
    let (b, addr_b) = http_rig_build(store, RigRuntime::incarnation(1), HttpRigOptions::default())
        .await
        .parts();
    b.admission.set_record_ceiling(8);
    let ct = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr_a, "PUT", path, &ct, br#"[{"n":0}]"#).await;
    assert!(status == 200 || status == 201);
    crate::failpoints::stop_before_mark_committed(name);
    let (status, _, _) = two_record_close(addr_a, path.to_string()).await;
    crate::failpoints::stop_before_mark_committed_off(name);
    assert_eq!(status, 503, "the failpoint did not interrupt the close");
    engine_shutdown(&a).await;

    let (status, _, body) = two_record_close(addr_b, path.to_string()).await;
    let sref = b.deployment.raw_adapter_sref(name);
    b.registry.invalidate(&sref);
    let d = b.registry.get(&sref).await.unwrap().unwrap();
    let (_, _, records) = hreq(addr_b, "GET", path, &[], b"").await;
    let records: Vec<serde_json::Value> = serde_json::from_slice(&records).unwrap();
    assert!(
        (status == 200 || status == 204) && d.sealed && d.sealing.is_none(),
        "the retry did not replay the committed final: {status} {}; sealed={} sealing={:?}",
        String::from_utf8_lossy(&body),
        d.sealed,
        d.sealing,
    );
    assert_eq!(records.len(), 3, "records duplicated or lost: {records:?}");
    engine_shutdown(&b).await;
}

/// Two instances over one store. A's close-with-content parks before its
/// enqueue holding its claim; A retires its engine; the exact retry runs on B
/// (with `admission` limits or a record `ceiling`); B retires; A's close
/// proceeds on a reopened engine. Asserts the retry left the claim untouched
/// and A's close sealed with its records, and returns the retry's status.
#[expect(
    clippy::disallowed_methods,
    reason = "refused-retry fixture; A's parked close is spawned and joined after B's retry answers; running it inline cannot hold it at its failpoint while B runs"
)]
async fn original_survives_a_refused_retry(
    name: &str,
    admission: Option<crate::config::AdmissionConfig>,
    ceiling: Option<usize>,
) -> u16 {
    let store = mem();
    let (a, addr_a) = http_rig(store.clone()).await;
    let options = HttpRigOptions {
        admission,
        ..Default::default()
    };
    let (b, addr_b) = http_rig_build(store, RigRuntime::incarnation(1), options)
        .await
        .parts();
    if let Some(ceiling) = ceiling {
        b.admission.set_record_ceiling(ceiling);
    }
    let path = format!("/v1/stream/{name}");
    let ct = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr_a, "PUT", &path, &ct, br#"[{"n":0}]"#).await;
    assert!(status == 200 || status == 201);
    let sref = a.deployment.raw_adapter_sref(name);
    let claim = async |state: &crate::http::AppState| {
        state.registry.invalidate(&sref);
        let d = state.registry.get(&sref).await.unwrap().unwrap();
        d.sealing
            .as_ref()
            .map(|c| (c.operation_id.clone(), c.claim_generation, c.claimed_ms))
    };
    crate::failpoints::park_close_before_enqueue(name);
    let original = tokio::spawn(two_record_close(addr_a, path.clone()));
    wait_parked(crate::failpoints::Fp::CloseBeforeEnqueue, name, 1).await;
    let installed = claim(&a).await;
    assert!(installed.is_some(), "A's close published no claim");
    engine_shutdown(&a).await;

    let (retry_status, _, retry_body) = two_record_close(addr_b, path.clone()).await;
    let after_retry = claim(&b).await;
    engine_shutdown(&b).await;

    crate::failpoints::release_close_before_enqueue(name);
    let (original_status, _, original_body) = original.await.unwrap();
    let d = {
        a.registry.invalidate(&sref);
        a.registry.get(&sref).await.unwrap().unwrap()
    };
    let seg = d.resolve_segment("");
    let engine = a.engine_for(&seg.shard_route).await.unwrap();
    let closed = engine
        .tail_fields(&seg.identity)
        .await
        .unwrap()
        .is_some_and(|tail| tail.closed);
    let (_, _, records) = hreq(addr_a, "GET", &path, &[], b"").await;
    let records: Vec<serde_json::Value> = serde_json::from_slice(&records).unwrap();
    let observed = format!(
        "{name}: retry on B answered {retry_status} {}; claim installed {installed:?}, \
         after the retry {after_retry:?}; A's close answered {original_status} {}; \
         segment closed={closed}, sealed={}, sealing={:?}, records={records:?}",
        String::from_utf8_lossy(&retry_body),
        String::from_utf8_lossy(&original_body),
        d.sealed,
        d.sealing,
    );
    assert_eq!(
        after_retry, installed,
        "the refused retry moved the claim: {observed}"
    );
    assert!(
        original_status == 200 || original_status == 204,
        "A's durable final was not acknowledged: {observed}"
    );
    assert!(
        closed && d.sealed && d.sealing.is_none(),
        "A's close did not seal: {observed}"
    );
    assert_eq!(
        records,
        [
            serde_json::json!({"n": 0}),
            serde_json::json!({"fin": "a"}),
            serde_json::json!({"fin": "b"}),
        ],
        "{observed}"
    );
    engine_shutdown(&a).await;
    retry_status
}

/// The raw close-with-content both attempts of one operation send.
async fn two_record_close(
    addr: std::net::SocketAddr,
    path: String,
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    let headers = [
        ("content-type", "application/json"),
        ("stream-closed", "true"),
    ];
    hreq(
        addr,
        "POST",
        &path,
        &headers,
        br#"[{"fin":"a"},{"fin":"b"}]"#,
    )
    .await
}

/// A plain append is never its stream's owed final. Its body, content type
/// and coordination headers equal those of a raw close-with-content parked
/// before its enqueue, and the close flag is not part of the operation
/// identity they share. It must be refused as every append during Sealing
/// is, and neither renew the claim nor land a record; the parked close then
/// seals with its final written once.
#[expect(
    clippy::disallowed_methods,
    reason = "plain-append identity fixture; the parked close is spawned and joined after the plain append answers; running it inline cannot hold it at its failpoint while the append runs"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_append_with_the_owed_finals_body_is_refused_during_sealing() {
    let _serial = gap_lock().lock().await;
    let name = "plainfinal";
    let path = format!("/v1/stream/{name}");
    let (state, addr) = http_rig(mem()).await;
    let ct = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr, "PUT", &path, &ct, br#"[{"n":0}]"#).await;
    assert!(status == 200 || status == 201);
    let sref = state.deployment.raw_adapter_sref(name);
    let claim = async || {
        state.registry.invalidate(&sref);
        let d = state.registry.get(&sref).await.unwrap().unwrap();
        d.sealing
            .as_ref()
            .map(|c| (c.operation_id.clone(), c.claim_generation, c.claimed_ms))
    };
    crate::failpoints::park_close_before_enqueue(name);
    let close = tokio::spawn(two_record_close(addr, path.clone()));
    wait_parked(crate::failpoints::Fp::CloseBeforeEnqueue, name, 1).await;
    let installed = claim().await;
    assert!(installed.is_some(), "the close published no claim");

    let (other, _, _) = hreq(addr, "POST", &path, &ct, br#"[{"n":1}]"#).await;
    let (plain, _, plain_body) =
        hreq(addr, "POST", &path, &ct, br#"[{"fin":"a"},{"fin":"b"}]"#).await;
    let after_plain = claim().await;
    crate::failpoints::release_close_before_enqueue(name);
    let (close_status, _, close_body) = close.await.unwrap();
    state.registry.invalidate(&sref);
    let d = state.registry.get(&sref).await.unwrap().unwrap();
    let (_, _, records) = hreq(addr, "GET", &path, &[], b"").await;
    let records: Vec<serde_json::Value> = serde_json::from_slice(&records).unwrap();
    let observed = format!(
        "another append answered {other}; the plain append answered {plain} {}; claim \
         installed {installed:?}, after the plain append {after_plain:?}; the close \
         answered {close_status} {}; sealed={}, sealing={:?}, records={records:?}",
        String::from_utf8_lossy(&plain_body),
        String::from_utf8_lossy(&close_body),
        d.sealed,
        d.sealing,
    );
    assert_eq!(other, 409, "a plain append during Sealing: {observed}");
    assert_eq!(plain, other, "the plain append was not refused: {observed}");
    assert_eq!(
        after_plain, installed,
        "the plain append moved the claim: {observed}"
    );
    assert!(
        (close_status == 200 || close_status == 204) && d.sealed && d.sealing.is_none(),
        "the close did not seal: {observed}"
    );
    assert_eq!(
        records,
        [
            serde_json::json!({"n": 0}),
            serde_json::json!({"fin": "a"}),
            serde_json::json!({"fin": "b"}),
        ],
        "{observed}"
    );
    engine_shutdown(&state).await;
}
