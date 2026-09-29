//! B3 (NEXT-WORK §2, edge change #66): a storage close that reaches the
//! rollup after its month was finalized. The month nets to what the shard
//! recorded up to the close instant through one signed correction, every
//! month already carried from the superseded gauge is reversed in the same
//! page, and a month not yet closed carries nothing. The figures mirror the
//! closure-debt DST: 81 B from Jan 15, closed at Jan 31 12:00.
#![cfg(test)]

use crate::billing::{BillingIdentity, SegmentSnapshot, UsageEnvelope, UsagePayload};
use crate::rollup::{
    MonthRow, SegmentState, UsageRollup, eff_u128, k_month, k_segment, month_start_ms, read_faults,
    read_json,
};
use slatedb::Db;
use std::sync::Arc;

const DAY: i64 = 86_400_000;
/// What January owes: 81 B from Jan 15 to the close at Jan 31 12:00.
const OWED: u128 = 115_473_600_000;
/// January as its carry billed it: 81 B from Jan 15 to Feb 1.
const JAN_CARRIED: u128 = 118_972_800_000;
/// February carried at 81 B: 28 days.
const FEB_CARRIED: u128 = 195_955_200_000;
/// The bystander segment's carries: 10 B from Jan 15, then whole months.
const BYSTANDER: [u128; 3] = [14_688_000_000, 24_192_000_000, 26_784_000_000];

fn jan15() -> i64 {
    month_start_ms(2026, 1) + 14 * DAY
}

fn expiry() -> i64 {
    month_start_ms(2026, 2) - DAY / 2
}

/// Segment 0's snapshot `version` of `month`: its byte-time so far, its
/// gauge and its storage clock. Live; `month_final` marks a final.
fn live(version: u64, month: &str, byte_ms: u128, gauge: u64, through: i64) -> SegmentSnapshot {
    SegmentSnapshot {
        identity: BillingIdentity {
            account_id: "a".into(),
            project_id: "p".into(),
            stream_id: "s".into(),
            stream_name: "orders".into(),
        },
        segment_id: 0,
        usage_version: version,
        month: month.into(),
        month_final: false,
        ingest_payload_bytes_month: 0,
        ingest_records_month: 0,
        owned_frame_bytes_current: gauge,
        storage_byte_ms_month: byte_ms.to_string(),
        storage_accounted_through_ms: through,
        retained_by_forks: false,
    }
}

fn month_final(snapshot: SegmentSnapshot) -> SegmentSnapshot {
    SegmentSnapshot {
        month_final: true,
        ..snapshot
    }
}

/// The last snapshot before any close: 81 B, accounted through Jan 15.
fn opened() -> SegmentSnapshot {
    live(1, "2026-01", 0, 81, jan15())
}

/// The shard's close at the January expiry, reported at `version`.
fn closed(version: u64) -> SegmentSnapshot {
    live(version, "2026-01", OWED, 0, expiry())
}

/// Segment 1 of the same stream: 10 B from Jan 15, never closed.
fn bystander() -> SegmentSnapshot {
    SegmentSnapshot {
        segment_id: 1,
        ..live(1, "2026-01", 0, 10, jan15())
    }
}

async fn rollup(name: &str) -> UsageRollup {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Db::builder(name, store).build().await.unwrap();
    UsageRollup {
        db: Arc::new(db),
        close_rows_visited: Default::default(),
    }
}

async fn apply(
    r: &UsageRollup,
    snapshots: impl IntoIterator<Item = SegmentSnapshot>,
    cursor: &str,
) -> anyhow::Result<()> {
    let envelopes: Vec<UsageEnvelope> = snapshots
        .into_iter()
        .map(|s| UsageEnvelope {
            v: 1,
            event_id: s.deterministic_event_id(),
            event_time_ms: 0,
            emitted_ms: 0,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s),
        })
        .collect();
    r.apply_page(&envelopes, cursor).await
}

/// Closes 2026's `months` in order, each finalizing the stream's row.
async fn close(r: &UsageRollup, months: &[u32]) {
    for &month in months {
        assert_eq!(r.close_month(2026, month, 0).await.unwrap(), 1, "{month}");
    }
}

/// `opened` applied, then January and February closed.
async fn closed_through_february(name: &str) -> UsageRollup {
    let r = rollup(name).await;
    apply(&r, [opened()], "opened").await.unwrap();
    close(&r, &[1, 2]).await;
    r
}

async fn row(r: &UsageRollup, month: &str) -> MonthRow {
    let row = r.month_row(month, "a", "p", "s").await.unwrap();
    row.expect("a month row")
}

/// Segment `seg`'s storage floor in `month` (a carry of gauge 0 leaves the
/// historical empty representation of zero).
async fn floor(r: &UsageRollup, month: &str, seg: u32) -> u128 {
    let row = row(r, month).await;
    match row.segments[&seg].storage_byte_ms.as_str() {
        "" => 0,
        figure => figure.parse().unwrap(),
    }
}

/// What `month` invoices: frozen storage plus storage corrections.
async fn invoiced(r: &UsageRollup, month: &str) -> u128 {
    let row = row(r, month).await;
    let frozen = row.frozen.expect("a finalized month").storage_byte_ms;
    eff_u128(frozen.parse().unwrap(), &row.corr.storage_byte_ms_delta)
}

/// The (name, project) aggregates' storage, each base plus corrections.
async fn aggregates(r: &UsageRollup, month: &str) -> (u128, u128) {
    let name = r.name_row(month, "a", "p", "orders").await.unwrap();
    let project = r.project_row(month, "a", "p").await.unwrap();
    let effective = |row: Option<crate::rollup::AggRow>| {
        let row = row.unwrap_or_default();
        eff_u128(
            row.storage_byte_ms.parse().unwrap_or(0),
            &row.corr.storage_byte_ms_delta,
        )
    };
    (effective(name), effective(project))
}

/// Segment 0's state as every later carry reads it: (version, gauge, through).
async fn state(r: &UsageRollup) -> (u64, u64, i64) {
    let key = k_segment("a", "p", "s", 0);
    let st: SegmentState = read_json(&r.db, &key).await.unwrap().unwrap();
    (
        st.usage_version,
        st.owned_frame_bytes_current,
        st.storage_accounted_through_ms,
    )
}

/// `month`'s corrections as "id storage-delta".
async fn corrections(r: &UsageRollup, month: &str) -> Vec<String> {
    let row = row(r, month).await;
    let listed = row.corrections.iter();
    listed
        .map(|c| format!("{} {}", c.correction_id, c.storage_byte_ms_delta))
        .collect()
}

/// One closed month's books: the stream's invoice, and the name and
/// project aggregates, all equal; the storage identity; and reconciliation
/// wherever the month has an aggregate. A month whose only row is a carry
/// of gauge 0 has none (the carry credits no aggregate), which
/// reconciliation reports on its own, as it does for an on-time close.
async fn assert_books(r: &UsageRollup, month: &str, owed: u128) {
    let stream = row(r, month).await;
    stream.assert_storage_telescopes();
    let (name, project) = aggregates(r, month).await;
    assert_eq!(
        (invoiced(r, month).await, name, project),
        (owed, owed, owed)
    );
    if r.project_row(month, "a", "p").await.unwrap().is_some() {
        let report = r.reconcile_month(month).await.unwrap();
        assert!(report.ok, "{month}: {report:?}");
    }
}

async fn database_rows(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut scan = db.scan(..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key.to_vec(), row.value.to_vec()));
    }
    rows
}

/// January, February and March of segment 0 after a late close at v2: the
/// close corrects January down to the expiry, February's carry is reversed
/// by its own correction, March carries nothing, and the bystander segment
/// in the same rows keeps every floor byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_after_two_closed_months_corrects_the_first_and_reverses_the_carried_one() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("late-two").await;
    apply(&r, [opened(), bystander()], "opened").await.unwrap();
    close(&r, &[1, 2]).await;
    let untouched = |row: MonthRow| serde_json::to_vec(&row.segments[&1]).unwrap();
    let before = [
        untouched(row(&r, "2026-01").await),
        untouched(row(&r, "2026-02").await),
    ];
    apply(&r, [closed(2)], "closed").await.unwrap();
    close(&r, &[3]).await;
    assert_eq!(
        (
            floor(&r, "2026-01", 0).await,
            floor(&r, "2026-02", 0).await,
            floor(&r, "2026-03", 0).await,
            state(&r).await.1,
        ),
        (OWED, 0, 0, 0),
        "a late close corrects its month and no later month bills the segment"
    );
    let after = [
        untouched(row(&r, "2026-01").await),
        untouched(row(&r, "2026-02").await),
    ];
    assert_eq!(after, before, "the bystander segment is untouched");
    assert_eq!(
        corrections(&r, "2026-01").await,
        ["corr/snap/snap/s/0/2026-01/2/2026-01 -3499200000"]
    );
    assert_eq!(
        corrections(&r, "2026-02").await,
        ["corr/snap/snap/s/0/2026-01/2/2026-02 -195955200000"]
    );
    assert!(corrections(&r, "2026-03").await.is_empty());
    for month in ["2026-01", "2026-02"] {
        for c in row(&r, month).await.corrections {
            assert_eq!((c.identity, c.month.as_str()), (closed(2).identity, month));
        }
    }
    assert_books(&r, "2026-01", OWED + BYSTANDER[0]).await;
    assert_books(&r, "2026-02", BYSTANDER[1]).await;
    assert_books(&r, "2026-03", BYSTANDER[2]).await;
    assert_eq!(r.pending_correction_artifacts(64).await.unwrap().len(), 2);
    assert_eq!(
        state(&r).await,
        (2, 0, month_start_ms(2026, 4)),
        "accounted-through never moves back from what the carries accounted"
    );
}

/// Re-delivery in the same page or a later one, an older snapshot after the
/// close, and a later zero-gauge version (a retention toggle) after March
/// was carried: January and February are corrected exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_close_corrects_once_whatever_its_delivery() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = closed_through_february("late-once").await;
    apply(&r, [closed(2), closed(2)], "closed").await.unwrap();
    let (jan, feb) = (
        corrections(&r, "2026-01").await,
        corrections(&r, "2026-02").await,
    );
    assert_eq!((jan.len(), feb.len()), (1, 1), "one correction per month");
    let settled = database_rows(&r.db).await;
    apply(&r, [closed(2)], "closed").await.unwrap();
    apply(&r, [opened()], "closed").await.unwrap();
    assert_eq!(
        database_rows(&r.db).await,
        settled,
        "a replay writes nothing"
    );
    close(&r, &[3]).await;
    let toggled = SegmentSnapshot {
        retained_by_forks: true,
        ..closed(3)
    };
    apply(&r, [toggled], "toggled").await.unwrap();
    assert_eq!(
        (
            corrections(&r, "2026-01").await,
            corrections(&r, "2026-02").await,
            corrections(&r, "2026-03").await.len(),
        ),
        (jan, feb, 0),
        "a later zero-gauge version corrects nothing more"
    );
    assert_eq!(state(&r).await, (3, 0, month_start_ms(2026, 4)));
    assert_books(&r, "2026-01", OWED).await;
    assert_books(&r, "2026-02", 0).await;
    assert_books(&r, "2026-03", 0).await;
}

/// A later zero-gauge version that reaches the rollup before the close
/// settles the same books, and the close then changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_zero_gauge_version_first_settles_the_same_books() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = closed_through_february("late-toggle-first").await;
    let toggled = SegmentSnapshot {
        retained_by_forks: true,
        ..closed(3)
    };
    apply(&r, [toggled], "toggled").await.unwrap();
    let settled = database_rows(&r.db).await;
    apply(&r, [closed(2)], "closed").await.unwrap();
    let rows = database_rows(&r.db).await;
    assert_eq!(rows.len(), settled.len());
    assert!(
        rows.iter()
            .zip(&settled)
            .all(|(now, then)| now == then || now.0 == b"meta/usage-cursor"),
        "the older close changes nothing but the cursor"
    );
    assert_eq!(
        (
            corrections(&r, "2026-01").await.len(),
            corrections(&r, "2026-02").await.len(),
        ),
        (1, 1)
    );
    assert_books(&r, "2026-01", OWED).await;
    assert_books(&r, "2026-02", 0).await;
    assert_eq!(state(&r).await.1, 0);
}

/// February's close stopped after its carry committed and before its
/// freeze: the late close lowers February's floor and both aggregates in
/// place with no correction, so the resumed freeze bills 0 once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_between_a_months_carry_and_its_freeze_lowers_that_month_in_place() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("late-window").await;
    apply(&r, [opened()], "opened").await.unwrap();
    close(&r, &[1]).await;
    let stop = (
        Arc::as_ptr(&r.db) as usize,
        b"stop-after-close-chunk".to_vec(),
    );
    read_faults().lock().unwrap().insert(stop);
    assert!(r.close_month(2026, 2, 0).await.is_err());
    apply(&r, [closed(2)], "closed").await.unwrap();
    let feb = row(&r, "2026-02").await;
    assert!(feb.finalized_at_ms.is_none(), "the freeze is still pending");
    assert_eq!(
        (
            feb.segments[&0].storage_byte_ms.as_str(),
            aggregates(&r, "2026-02").await
        ),
        ("0", (0, 0)),
        "the carry is reversed in place"
    );
    assert_eq!(
        (feb.segments[&0].gauge_bytes, feb.corrections.len()),
        (0, 0)
    );
    let staged = r.pending_correction_artifacts(64).await.unwrap();
    assert_eq!(staged.len(), 1, "only January's correction is staged");
    assert_eq!(r.close_month(2026, 2, 0).await.unwrap(), 1);
    assert_eq!(
        row(&r, "2026-02").await.frozen.unwrap().storage_byte_ms,
        "0"
    );
    assert!(corrections(&r, "2026-02").await.is_empty());
    assert_books(&r, "2026-01", OWED).await;
    assert_books(&r, "2026-02", 0).await;
}

/// January's own close stopped after its carry: the close lands on the open
/// row, whose floor already takes the figure; the aggregates lose the
/// carry's excess with it, so the freeze and the project agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_inside_its_own_months_carry_window_lowers_the_aggregates_too() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("late-own-window").await;
    apply(&r, [opened()], "opened").await.unwrap();
    let stop = (
        Arc::as_ptr(&r.db) as usize,
        b"stop-after-close-chunk".to_vec(),
    );
    read_faults().lock().unwrap().insert(stop);
    assert!(r.close_month(2026, 1, 0).await.is_err());
    apply(&r, [closed(2)], "closed").await.unwrap();
    assert_eq!(r.close_month(2026, 1, 0).await.unwrap(), 1);
    let jan = row(&r, "2026-01").await;
    assert_eq!(
        (
            jan.frozen.unwrap().storage_byte_ms.parse::<u128>().unwrap(),
            aggregates(&r, "2026-01").await,
        ),
        (OWED, (OWED, OWED)),
        "the carry's excess stays in the name and project aggregates"
    );
    assert!(corrections(&r, "2026-01").await.is_empty());
    close(&r, &[2]).await;
    assert_books(&r, "2026-01", OWED).await;
    assert_books(&r, "2026-02", 0).await;
}

/// The close reaches the rollup only as the shard's month-finals of a
/// second close (and the live snapshot after them), in either order of the
/// finals: each month is corrected once, never by both its own final and
/// the reversal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_carried_only_by_month_finals_corrects_every_carried_month() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let jan = month_final(live(2, "2026-01", OWED, 0, month_start_ms(2026, 2)));
    let feb = month_final(live(2, "2026-02", 0, 0, month_start_ms(2026, 3)));
    let mar = live(3, "2026-03", 0, 0, month_start_ms(2026, 3) + 4 * DAY);
    for (name, finals) in [
        ("late-finals", [jan.clone(), feb.clone()]),
        ("late-finals-reordered", [feb.clone(), jan.clone()]),
    ] {
        let r = closed_through_february(name).await;
        apply(&r, finals.into_iter().chain([mar.clone()]), "second-close")
            .await
            .unwrap();
        assert_eq!(
            (
                invoiced(&r, "2026-01").await,
                invoiced(&r, "2026-02").await,
                state(&r).await.1,
            ),
            (OWED, 0, 0),
            "{name}: the finals of a second close settle every carried month"
        );
        let counts = (
            corrections(&r, "2026-01").await.len(),
            corrections(&r, "2026-02").await.len(),
        );
        assert_eq!(counts, (1, 1), "{name}");
        assert_books(&r, "2026-01", OWED).await;
        assert_books(&r, "2026-02", 0).await;
    }
}

/// A carried month the page cannot read fails the late close whole; the
/// fault is one-shot, and the retried page settles the same books.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_month_the_page_cannot_read_fails_the_late_close_whole() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = closed_through_february("late-read-fault").await;
    let before = database_rows(&r.db).await;
    let fault = (
        Arc::as_ptr(&r.db) as usize,
        k_month("2026-02", "a", "p", "s"),
    );
    read_faults().lock().unwrap().insert(fault.clone());
    let failed = apply(&r, [closed(2)], "closed").await;
    let unread = read_faults().lock().unwrap().remove(&fault);
    assert!(
        failed.is_err() && !unread,
        "the late close reads February and fails with it"
    );
    assert_eq!(database_rows(&r.db).await, before, "nothing is written");
    apply(&r, [closed(2)], "closed").await.unwrap();
    assert_eq!(
        (invoiced(&r, "2026-01").await, invoiced(&r, "2026-02").await),
        (OWED, 0)
    );
}

/// The gauge rose after the carry's knowledge (120 B from Jan 20), then the
/// segment closed: January's correction is positive, and both delivery
/// orders settle the same books. The rise alone still keeps the upward-only
/// rule and the carried gauge (owner follow-up (c) is not in this change).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gauge_rise_and_a_close_settle_the_same_books_in_either_order() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let jan20 = month_start_ms(2026, 1) + 19 * DAY;
    let rose = live(2, "2026-01", 34_992_000_000, 120, jan20);
    let owed = 154_224_000_000;
    let close_v3 = live(3, "2026-01", owed, 0, expiry());
    let first = closed_through_february("late-rise").await;
    apply(&first, [rose.clone()], "rose").await.unwrap();
    assert_eq!(
        (
            invoiced(&first, "2026-01").await,
            invoiced(&first, "2026-02").await,
            state(&first).await,
        ),
        (JAN_CARRIED, FEB_CARRIED, (1, 81, month_start_ms(2026, 3))),
        "a late rise alone keeps the carried books"
    );
    apply(&first, [close_v3.clone()], "closed").await.unwrap();
    let second = closed_through_february("late-rise-reordered").await;
    apply(&second, [close_v3], "closed").await.unwrap();
    apply(&second, [rose], "rose").await.unwrap();
    for r in [&first, &second] {
        assert_eq!(
            (
                invoiced(r, "2026-01").await,
                invoiced(r, "2026-02").await,
                state(r).await.1,
            ),
            (owed, 0, 0)
        );
        assert_eq!(
            corrections(r, "2026-01").await,
            ["corr/snap/snap/s/0/2026-01/3/2026-01 35251200000"]
        );
        assert_books(r, "2026-01", owed).await;
        assert_books(r, "2026-02", 0).await;
    }
}

/// A late snapshot that still owns bytes is not a close: a lower figure
/// corrects no storage, keeps the carried floor and leaves the state the
/// carries read, so February is carried as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_snapshot_that_does_not_close_keeps_the_upward_only_rule() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("late-open").await;
    apply(&r, [opened()], "opened").await.unwrap();
    close(&r, &[1]).await;
    let lower = live(2, "2026-01", 40_000, 40, jan15() + DAY);
    apply(&r, [lower], "lower").await.unwrap();
    assert_eq!(
        (
            floor(&r, "2026-01", 0).await,
            corrections(&r, "2026-01").await,
            state(&r).await,
        ),
        (
            JAN_CARRIED,
            Vec::<String>::new(),
            (1, 81, month_start_ms(2026, 2))
        )
    );
    close(&r, &[2]).await;
    assert_eq!(invoiced(&r, "2026-02").await, FEB_CARRIED);
    assert!(corrections(&r, "2026-02").await.is_empty());
}

/// A state accounted more than 600 months past the close's month is not a
/// carry the month close could have made: the late close fails its page
/// before reading any month, and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_span_beyond_the_month_close_cap_fails_the_late_close() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("late-cap").await;
    apply(&r, [opened()], "opened").await.unwrap();
    close(&r, &[1]).await;
    let far = SegmentState {
        usage_version: 1,
        owned_frame_bytes_current: 81,
        storage_accounted_through_ms: month_start_ms(2076, 3),
        stream_name: "orders".into(),
        account_id: "a".into(),
    };
    let key = k_segment("a", "p", "s", 0);
    r.db.put(&key, serde_json::to_vec(&far).unwrap())
        .await
        .unwrap();
    let before = database_rows(&r.db).await;
    let failed = apply(&r, [closed(2)], "closed").await;
    let error = failed.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(error.contains("600 months"), "{error:?}");
    assert_eq!(database_rows(&r.db).await, before);
}

/// A carried month whose floor is not what the carry billed from the
/// superseded state is refused rather than reversed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_month_whose_floor_the_carry_did_not_bill_fails_the_late_close() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = closed_through_february("late-foreign-floor").await;
    let mut feb = row(&r, "2026-02").await;
    let segment = feb.segments.get_mut(&0).unwrap();
    segment.storage_byte_ms = (FEB_CARRIED + 1).to_string();
    let key = k_month("2026-02", "a", "p", "s");
    r.db.put(&key, serde_json::to_vec(&feb).unwrap())
        .await
        .unwrap();
    let before = database_rows(&r.db).await;
    let failed = apply(&r, [closed(2)], "closed").await;
    let error = failed.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(error.contains("did not bill"), "{error:?}");
    assert_eq!(database_rows(&r.db).await, before);
}
