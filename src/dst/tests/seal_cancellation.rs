//! Cancellation and typed final-record verdicts at the seal coordinator.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::application::lifecycle::{
    FinalDisposition, FinalRecordFailure, FinalSealRequest, SealFinalError, seal_final,
};

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
    let task = {
        let (service, stream, epoch) = (service.clone(), stream.clone(), epoch.clone());
        tokio::spawn(async move {
            seal_final(
                &service,
                FinalSealRequest {
                    stream: &stream,
                    epoch: &epoch,
                    operation: "cancelled-op",
                    routing_key: "",
                },
                |_authority| async move {
                    let _ = entered.send(());
                    std::future::pending::<Result<_, FinalRecordFailure<()>>>().await
                },
            )
            .await
        })
    };
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
