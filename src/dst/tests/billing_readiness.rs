//! Billing readiness follows the clap-resolved billing selectors (item 32).

use super::fixture_http::{HttpRigOptions, http_rig_build, install_rollup};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use serde_json::Value;

/// `/health`'s status and body text.
async fn health(addr: std::net::SocketAddr) -> (u16, String) {
    let (status, _, body) = hreq(addr, "GET", "/health", &[], b"").await;
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// `/operator/billing.json`'s `mode` and `ready`.
async fn billing_report(addr: std::net::SocketAddr) -> (Value, Value) {
    let (status, _, body) = hreq(addr, "GET", "/operator/billing.json", &[], b"").await;
    assert_eq!(status, 200);
    let report: Value = serde_json::from_slice(&body).unwrap();
    (report["mode"].clone(), report["ready"].clone())
}

/// Item 32: `--billing-mode required` on argv alone. Boot already
/// enforced it; /health, the readiness report and the drain read a copy
/// of the environment and so reported `off`. With one reader, all three
/// stay closed until the read spool opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn argv_billing_mode_required_holds_readiness_until_the_spool_opens() {
    let options = HttpRigOptions {
        cli: |cli| cli.billing_mode = "required".into(),
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    assert_eq!(
        health(rig.addr).await,
        (
            503,
            "billing not ready (spool=false, rollup=true)".to_string()
        ),
        "argv --billing-mode required must gate /health until the spool opens"
    );
    let closed = (Value::from("required"), Value::from(false));
    assert_eq!(billing_report(rig.addr).await, closed);
    let refused = "read spool not open (BILLING_MODE=required refuses the memory-only path)";
    assert_eq!(
        crate::billing::drain_once(&rig.state).await,
        Err(refused.to_string())
    );
    crate::billing::open_read_spool(&rig.state).await.unwrap();
    assert_eq!(health(rig.addr).await, (200, "ok".to_string()));
    let ready = (Value::from("required"), Value::from(true));
    assert_eq!(billing_report(rig.addr).await, ready);
    rig.shutdown().await;
}

/// Item 32: `--rollup 1` on argv alone. A required-mode rollup owner is
/// not ready until its rollup database is installed; the environment
/// copy saw no ROLLUP and waived the check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn argv_rollup_owner_is_unready_until_its_rollup_installs() {
    let options = HttpRigOptions {
        cli: |cli| {
            cli.billing_mode = "required".into();
            cli.rollup = "1".into();
        },
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    crate::billing::open_read_spool(&rig.state).await.unwrap();
    assert_eq!(
        health(rig.addr).await,
        (
            503,
            "billing not ready (spool=true, rollup=false)".to_string()
        ),
        "argv --rollup 1 must gate /health until the rollup installs"
    );
    let closed = (Value::from("required"), Value::from(false));
    assert_eq!(billing_report(rig.addr).await, closed);
    let store = rig.state.data_store.clone();
    let rollup = crate::rollup::UsageRollup::open(store, "", &rig.state.config)
        .await
        .unwrap();
    install_rollup(&rig.state, rollup);
    assert_eq!(health(rig.addr).await, (200, "ok".to_string()));
    let ready = (Value::from("required"), Value::from(true));
    assert_eq!(billing_report(rig.addr).await, ready);
    rig.shutdown().await;
}

/// Pin: with no --billing-mode anywhere the report shows clap's `off`
/// default and the instance is ready without a spool, as it always was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_billing_mode_reports_off_and_ready_without_a_spool() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    assert_eq!(health(rig.addr).await, (200, "ok".to_string()));
    let off = (Value::from("off"), Value::from(true));
    assert_eq!(billing_report(rig.addr).await, off);
    rig.shutdown().await;
}
