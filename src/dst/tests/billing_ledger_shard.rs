//! The billing sweep and the rig's ledger shard. On the default rig (`[""]`) the system ledgers
//! (`_ops_metrics`, `_usage`) live on the one leaf the billing sweep walks. Its discovery opens a
//! cold leaf, finds no debt and closes it, which arms the leaf's base anti-flap holdoff (3 s): a
//! system read of an existing ledger then answers `503 shard_moving`. A production ledger shard is
//! kept resident by the customer traffic on it; a test that reads the ledgers right after starting
//! telemetry adopts their shard the same way first ([`adopt_the_ledger_shard`]).

use std::sync::Arc;
use std::time::Duration;

use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::shard_directory::{RetireOutcome, RetirementReason};
use crate::sharddir::OpenOutcome;

/// How long one step of these tests may take.
const BOUND: Duration = Duration::from_secs(20);
/// Past the base holdoff a retirement arms (`sharddir::HOLDOFF_BASE`, 3 s).
const PAST_HOLDOFF: Duration = Duration::from_millis(3_500);

/// Resolve the shard the system ledgers live on as a customer request does (an external adoption,
/// R29), so the billing sweep's discovery treats it as customer-resident and never closes it.
pub(super) async fn adopt_the_ledger_shard(state: &Arc<crate::http::AppState>) {
    let sref = crate::tenant::system_project().stream_ref(crate::billing::OPS_METRICS_STREAM);
    let route = crate::crypto::RouteHash::for_stream(&sref).0;
    match tokio::time::timeout(BOUND, state.engine_for(&route)).await {
        Ok(Ok(_)) => {}
        Ok(Err(refused)) => panic!("the ledger shard did not open: {}", refused.status()),
        Err(_) => panic!("the ledger shard did not open within {BOUND:?}"),
    }
}

/// **An adopted ledger shard stays resident across the sweep.** `_ops_metrics` holds one point,
/// sealed until its leaf `""` has no debt, and the leaf is cold (retired, its holdoff run out), as
/// on a rig whose telemetry just started. Adopted, the sweep leaves it resident and a system read
/// answers the point. Without the adoption the sweep opens the cold leaf, closes it, and the read
/// meets its holdoff: `503 shard_moving` (`billing_controller`'s push-4 CI failure).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_adopted_ledger_shard_stays_resident_across_the_sweep() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    let key = state.billing.usage_key().unwrap();
    let point = br#"[{"point":1}]"#;
    let stream = crate::billing::OPS_METRICS_STREAM;
    let appended = crate::billing::system_append(&state, stream, &key, point.to_vec()).await;
    assert_eq!(appended, Ok(()));
    let engine = state
        .shards
        .open("")
        .expect("the append opened the ledger leaf");
    let absorbed = tokio::time::timeout(BOUND, async {
        while engine.maintenance_snapshot().unabsorbed_frame_bytes > 0
            || engine.has_billing_debt().await.unwrap_or(true)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    assert!(
        absorbed.await.is_ok(),
        "the leaf has no debt within {BOUND:?}"
    );
    drop(engine);
    let retired = state
        .shards
        .retire("", RetirementReason::Shutdown, |_, _| true);
    assert!(matches!(retired, RetireOutcome::Retired(_)));
    tokio::time::sleep(PAST_HOLDOFF).await;
    adopt_the_ledger_shard(&state).await;
    let sweep = tokio::time::timeout(BOUND, crate::billing::sweep_owned_outboxes(&state));
    assert!(sweep.await.is_ok(), "one sweep within {BOUND:?}");
    let read = crate::billing::system_read(&state, stream, &key, None).await;
    if read.is_err() {
        let leaf = match state.shards.open_or_wait("", Duration::ZERO).await {
            OpenOutcome::Ready(_) => "ready".to_string(),
            OpenOutcome::Wait { code, .. } => code.to_string(),
            OpenOutcome::Failed(error) => error,
        };
        panic!("the ledger read after the sweep: {read:?}; the leaf answers {leaf}");
    }
    let (body, _) = read.unwrap().expect("_ops_metrics exists");
    assert_eq!(&body[..], point, "the read answers the point");
    assert!(
        state.shards.is_open(""),
        "the sweep left the adopted leaf resident"
    );
    rig.shutdown().await;
}
