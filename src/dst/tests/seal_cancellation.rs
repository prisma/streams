//! Cancellation and typed final-record verdicts at the seal coordinator.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
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
