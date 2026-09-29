//! A closure debt that settles after its month AND the next were invoiced
//! (NEXT-WORK §2, B3; edge change #66): the late close corrects January to
//! the expiry and reverses February's carry under the identity the real
//! carry credited, end to end through the production sweep, drain, rollup
//! and month close. One contract with `billing_closure_debts.rs`, whose
//! helpers it shares.

use super::billing_closure_debts::{
    ClockReset, assert_billed, carried_gauge, closed_months, expected_close, idle_january,
    invoiced, recreate_and_settle,
};
use super::fixture_http::engine_shutdown;
use crate::billing::month_start_ms;

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// Owner requirement "month crossing", decided for B3 on 2026-09-29: the
/// replaced row closes at its January expiry only after January AND
/// February were invoiced. January nets to the expiry, February's carry of
/// the open gauge is reversed by its own correction, and March carries
/// nothing; every corrected row keeps frozen + corrections == its floors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_that_reaches_the_rollup_after_february_closed_reverses_february() {
    let _clock = crate::billing::billing_clock_lock().write().await;
    let _reset = ClockReset;
    let case = idle_january().await;
    for (month, due) in [("2026-01", 2), ("2026-02", 3)] {
        let at = month_start_ms(2026, due) + DAY + HOUR;
        assert_eq!(closed_months(&case.state, at).await, [month]);
    }
    let got = recreate_and_settle(&case).await;
    let want = expected_close(&case.before, case.expired_at);
    assert_billed(&got, &want, "the replaced row closes at its January expiry");
    let mar_close = month_start_ms(2026, 4) + DAY + HOUR;
    assert_eq!(closed_months(&case.state, mar_close).await, ["2026-03"]);
    let epoch = case.old.stream_epoch.as_str();
    assert_eq!(
        (
            invoiced(&case.state, "2026-01", epoch).await,
            invoiced(&case.state, "2026-02", epoch).await,
            invoiced(&case.state, "2026-03", epoch).await,
            carried_gauge(&case.state, epoch).await,
        ),
        (case.owed, 0, 0, 0),
        "a close that reached the rollup after February closed must correct \
         January, reverse February's carry, and carry nothing into March"
    );
    let rollup = case.state.rollup.get().expect("the rig's rollup");
    for month in ["2026-01", "2026-02"] {
        let row = rollup.month_row(month, "acct_test", "proj-test", epoch);
        let row = row.await.unwrap().expect("a corrected month");
        assert_eq!(row.corrections.len(), 1, "{month}");
        row.assert_storage_telescopes();
    }
    engine_shutdown(&case.state).await;
}
