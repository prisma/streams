#![cfg(test)]

use super::MonthClose;
use crate::billing::month_start_ms;
use crate::rollup::tests::ClockGuard;
use crate::rollup::{
    AggRow, FrozenTotals, K_OLDEST_UNCLOSED, MonthRow, SegMonth, SegmentState, UsageRollup,
    k_segment,
};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;
use std::time::Duration;

const DAY_MS: i64 = 86_400_000;
const GRACE_MS: i64 = 2 * 3_600_000;
const CARRY_CURSOR: &[u8] = b"meta/close-seg-cursor/2026-07";

#[tokio::test]
async fn byte_bound_pages_resume_exclusively_and_always_make_progress() {
    let db = Arc::new(
        Db::builder(
            "close-byte-cap",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let mut batch = WriteBatch::new();
    let state = SegmentState {
        stream_name: "x".repeat(600_000),
        ..Default::default()
    };
    let value = serde_json::to_vec(&state).unwrap();
    for key in [b"segment/a", b"segment/b", b"segment/c"] {
        batch.put(key, value.clone());
    }
    db.write(batch).await.unwrap();
    let rollup = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    let close = MonthClose {
        rollup: &rollup,
        month: "2026-07".into(),
        start: 0,
        boundary: 1,
        now: 2,
    };
    let first = close
        .read_page::<SegmentState>(b"segment/", None)
        .await
        .unwrap();
    assert_eq!(
        first.len(),
        2,
        "the byte cap must stop before the third row"
    );
    assert_eq!(first[1].0, b"segment/b");
    let last = close
        .read_page::<SegmentState>(b"segment/", Some(&first[1].0))
        .await
        .unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].0, b"segment/c");
    assert!(
        close
            .read_page::<SegmentState>(b"segment/", Some(&last[0].0))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(rollup.close_rows_visited(), 3);
    let oversized = SegmentState {
        stream_name: "x".repeat(1_100_000),
        ..Default::default()
    };
    db.put(b"segment/z", serde_json::to_vec(&oversized).unwrap())
        .await
        .unwrap();
    let oversized_page = close
        .read_page::<SegmentState>(b"segment/", Some(&last[0].0))
        .await
        .unwrap();
    assert_eq!(
        oversized_page.len(),
        1,
        "a single oversized row must still advance"
    );
    assert_eq!(oversized_page[0].0, b"segment/z");
    db.close().await.unwrap();
}

/// A rollup on its own in-memory store whose writes turn durable within about
/// a millisecond: a month walk costs its work, not a flush tick per write, so
/// a walk that strays back through the centuries fails its assertion quickly.
async fn quick_rollup(path: &str) -> UsageRollup {
    let settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        ..Default::default()
    };
    let db = Db::builder(path, Arc::new(object_store::memory::InMemory::new()))
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    UsageRollup {
        db: Arc::new(db),
        close_rows_visited: Default::default(),
    }
}

/// July 2026's close as `close_month` builds it, read at `now`.
fn july_close(rollup: &UsageRollup, now: i64) -> MonthClose<'_> {
    MonthClose {
        rollup,
        month: "2026-07".into(),
        start: month_start_ms(2026, 7),
        boundary: month_start_ms(2026, 8),
        now,
    }
}

fn json(value: &impl serde::Serialize) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

/// Each month segment's (id, recorded byte-ms, accounted through, final) in
/// id order: the storage a failed assertion prints legibly, before the whole
/// row is compared.
fn storage_view(row: &MonthRow) -> Vec<(u32, String, i64, bool)> {
    let mut view: Vec<_> = row
        .segments
        .iter()
        .map(|(id, s)| {
            (
                *id,
                s.storage_byte_ms.clone(),
                s.accounted_through_ms,
                s.final_seen,
            )
        })
        .collect();
    view.sort_unstable();
    view
}

/// The month row `month_row` reads back, which the test requires to exist.
async fn stored_month(rollup: &UsageRollup, stream: &str) -> MonthRow {
    let row = rollup.month_row("2026-07", "acct", "proj", stream).await;
    row.unwrap().expect("the close wrote the month row")
}

/// Every live key in the rollup, in key order.
async fn keys(rollup: &UsageRollup) -> Vec<String> {
    let mut iter = rollup.db.scan(..).await.unwrap();
    let mut keys = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        keys.push(String::from_utf8(row.key.to_vec()).unwrap());
    }
    keys
}

async fn oldest_unclosed(rollup: &UsageRollup) -> Option<String> {
    let raw = rollup.db.get(K_OLDEST_UNCLOSED).await.unwrap()?;
    Some(String::from_utf8(raw.to_vec()).unwrap())
}

/// An open invoice row of stream `s1` in `month`, with no segments.
async fn put_open_row(rollup: &UsageRollup, month: &str) {
    let row = MonthRow {
        account_id: "acct".into(),
        stream_name: "orders".into(),
        ..Default::default()
    };
    rollup
        .db
        .put(
            format!("month/{month}/acct/proj/s1"),
            serde_json::to_vec(&row).unwrap(),
        )
        .await
        .unwrap();
}

async fn finalized_at(rollup: &UsageRollup, month: &str) -> Option<i64> {
    let row = rollup.month_row(month, "acct", "proj", "s1").await.unwrap();
    row.unwrap().finalized_at_ms
}

/// A month segment as page apply leaves it: storage accounted `through`, with
/// `stored` byte-ms recorded so far; its ingest scales with the gauge.
fn month_segment(gauge: u64, through: i64, stored: &str, final_seen: bool) -> SegMonth {
    SegMonth {
        usage_version: 1,
        ingest_bytes: gauge * 10,
        ingest_records: gauge,
        storage_byte_ms: stored.into(),
        gauge_bytes: gauge,
        accounted_through_ms: through,
        final_seen,
    }
}

/// The month segment carry synthesizes: closed at the boundary.
fn carried(gauge: u64, stored: &str) -> SegMonth {
    SegMonth {
        storage_byte_ms: stored.into(),
        gauge_bytes: gauge,
        accounted_through_ms: month_start_ms(2026, 8),
        final_seen: true,
        ..Default::default()
    }
}

/// The durable state of a segment of account `acct` retaining `gauge` bytes,
/// its storage accounted `through`.
fn retained(name: &str, gauge: u64, through: i64) -> SegmentState {
    SegmentState {
        usage_version: 1,
        owned_frame_bytes_current: gauge,
        storage_accounted_through_ms: through,
        stream_name: name.into(),
        account_id: "acct".into(),
    }
}

async fn put_segment(rollup: &UsageRollup, stream: &str, segment: u32, state: &SegmentState) {
    rollup
        .db
        .put(
            k_segment("acct", "proj", stream, segment),
            serde_json::to_vec(state).unwrap(),
        )
        .await
        .unwrap();
}

/// A first walk over an empty rollup has neither a marker nor a month index
/// to start from, so it starts at the month before the clock's own. In
/// mid-year that is the previous month of the same year, closed alone.
#[tokio::test]
async fn a_first_walk_without_data_closes_only_the_previous_month_in_mid_year() {
    let _clock = ClockGuard::at(month_start_ms(2026, 7) + 3 * DAY_MS).await;
    let rollup = quick_rollup("walk-mid-year").await;
    assert_eq!(
        rollup.close_months_due(0).await.unwrap(),
        [("2026-06".to_owned(), 0)]
    );
    assert_eq!(oldest_unclosed(&rollup).await.as_deref(), Some("2026-07"));
}

/// In January the month before the clock's is December of the year before.
#[tokio::test]
async fn a_first_walk_without_data_in_january_closes_december_of_the_year_before() {
    let _clock = ClockGuard::at(month_start_ms(2027, 1) + 3 * DAY_MS).await;
    let rollup = quick_rollup("walk-january").await;
    assert_eq!(
        rollup.close_months_due(0).await.unwrap(),
        [("2026-12".to_owned(), 0)]
    );
    assert_eq!(oldest_unclosed(&rollup).await.as_deref(), Some("2027-01"));
}

/// The walk closes a month only once the grace after its boundary has run
/// out. A millisecond short it closes nothing and writes nothing, not even
/// the marker; at the grace exactly it freezes the row, stages its artifact
/// and moves the marker to the next month.
#[tokio::test]
async fn the_walk_closes_a_month_only_after_the_grace_past_its_boundary() {
    let boundary = month_start_ms(2026, 7);
    let clock = ClockGuard::at(boundary + GRACE_MS - 1).await;
    let rollup = quick_rollup("walk-grace").await;
    put_open_row(&rollup, "2026-06").await;
    assert_eq!(
        rollup.close_months_due(GRACE_MS).await.unwrap(),
        Vec::<(String, usize)>::new()
    );
    assert_eq!(keys(&rollup).await, ["month/2026-06/acct/proj/s1"]);
    assert_eq!(finalized_at(&rollup, "2026-06").await, None);
    clock.set(boundary + GRACE_MS);
    assert_eq!(
        rollup.close_months_due(GRACE_MS).await.unwrap(),
        [("2026-06".to_owned(), 1)]
    );
    assert_eq!(
        keys(&rollup).await,
        [
            "artifact-pending/2026-06/acct/proj/s1",
            "meta/oldest-unclosed-month",
            "month/2026-06/acct/proj/s1",
        ]
    );
    assert_eq!(oldest_unclosed(&rollup).await.as_deref(), Some("2026-07"));
    assert_eq!(
        finalized_at(&rollup, "2026-06").await,
        Some(boundary + GRACE_MS)
    );
}

/// `close_month` itself refuses a month inside its grace, whoever calls it: a
/// millisecond short it reports nothing closed and writes nothing; at the
/// grace exactly it freezes the row and stages its artifact.
#[tokio::test]
async fn close_month_refuses_a_month_still_inside_its_grace() {
    let boundary = month_start_ms(2026, 7);
    let clock = ClockGuard::at(boundary + GRACE_MS - 1).await;
    let rollup = quick_rollup("close-grace").await;
    put_open_row(&rollup, "2026-06").await;
    assert_eq!(rollup.close_month(2026, 6, GRACE_MS).await.unwrap(), 0);
    assert_eq!(keys(&rollup).await, ["month/2026-06/acct/proj/s1"]);
    assert_eq!(finalized_at(&rollup, "2026-06").await, None);
    clock.set(boundary + GRACE_MS);
    assert_eq!(rollup.close_month(2026, 6, GRACE_MS).await.unwrap(), 1);
    assert_eq!(
        keys(&rollup).await,
        [
            "artifact-pending/2026-06/acct/proj/s1",
            "month/2026-06/acct/proj/s1",
        ]
    );
    assert_eq!(
        finalized_at(&rollup, "2026-06").await,
        Some(boundary + GRACE_MS)
    );
}

/// Finalizing extends only the segments still open short of the boundary: a
/// final segment, and one already accounted through the boundary, keep what
/// they recorded. An open one adds its gauge times the time from where it was
/// accounted (or the month's start, if earlier) to the boundary onto what it
/// had. The frozen totals sum every segment; the outbox holds the same bytes.
#[tokio::test]
async fn finalize_extends_only_open_segments_to_the_boundary_and_freezes_the_sums() {
    let rollup = quick_rollup("finalize-row").await;
    let (start, boundary) = (month_start_ms(2026, 7), month_start_ms(2026, 8));
    let now = boundary + 5;
    let row = MonthRow {
        account_id: "acct".into(),
        stream_name: "orders".into(),
        segments: [
            (0, month_segment(7, boundary - 1_000, "11", true)),
            (1, month_segment(9, boundary, "13", false)),
            (2, month_segment(3, start + 1_000, "1000000000000", false)),
            (3, month_segment(2, start - 1_000, "", false)),
        ]
        .into(),
        read_payload_bytes: 21,
        read_records: 22,
        read_operations: 23,
        queue_operations: 24,
        append_requests: 25,
        ..Default::default()
    };
    let mut expected = row.clone();
    let key = b"month/2026-07/acct/proj/s1";
    let mut batch = WriteBatch::new();
    let close = july_close(&rollup, now);
    assert_eq!(close.finalize_row(&mut batch, key, row).unwrap(), 1);
    rollup.db.write(batch).await.unwrap();

    // July is 2,678,400,000 ms. Segment 2: 10^12 + 3 B x (July less 1 s);
    // segment 3: 2 B x all of July, from the month's start.
    for (segment, stored) in [(2, "1008035197000"), (3, "5356800000")] {
        let segment = expected.segments.get_mut(&segment).unwrap();
        segment.storage_byte_ms = stored.into();
        segment.accounted_through_ms = boundary;
        segment.final_seen = true;
    }
    expected.finalized_at_ms = Some(now);
    expected.frozen = Some(FrozenTotals {
        ingest_bytes: 210,
        ingest_records: 21,
        // 11 + 13 + 1,008,035,197,000 + 5,356,800,000
        storage_byte_ms: "1013391997024".into(),
        read_payload_bytes: 21,
        read_records: 22,
        read_operations: 23,
        queue_operations: 24,
        append_requests: 25,
    });
    let stored = rollup.db.get(key).await.unwrap().unwrap();
    let row: MonthRow = serde_json::from_slice(&stored).unwrap();
    assert_eq!(storage_view(&row), storage_view(&expected));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored).unwrap(),
        json(&expected)
    );
    let outbox = rollup.db.get(b"artifact-pending/2026-07/acct/proj/s1");
    assert_eq!(outbox.await.unwrap(), Some(stored));
}

/// Carry bills each retained segment's gauge from where its storage was
/// accounted (or the month's start, if earlier) to the boundary, closes the
/// month segment there, and adds the same byte-ms to the stream's name and
/// project aggregates. The name aggregate lists the incarnation once, however
/// many of its segments the page carries.
#[tokio::test]
async fn carry_bills_retained_segments_to_the_boundary_and_names_the_incarnation_once() {
    let rollup = quick_rollup("carry-retained").await;
    let (start, boundary) = (month_start_ms(2026, 7), month_start_ms(2026, 8));
    let now = boundary + 5;
    let states = [
        retained("orders", 100, start + 1_000),
        retained("orders", 50, start - 1_000),
    ];
    put_segment(&rollup, "s1", 0, &states[0]).await;
    put_segment(&rollup, "s1", 1, &states[1]).await;
    july_close(&rollup, now).carry(CARRY_CURSOR).await.unwrap();

    // 100 B x (July less its first second) + 50 B x all of July.
    let (first, second, total) = ("267839900000", "133920000000", "401759900000");
    let month = MonthRow {
        account_id: "acct".into(),
        stream_name: "orders".into(),
        segments: [(0, carried(100, first)), (1, carried(50, second))].into(),
        updated_ms: now,
        ..Default::default()
    };
    let stored = stored_month(&rollup, "s1").await;
    assert_eq!(storage_view(&stored), storage_view(&month));
    assert_eq!(json(&stored), json(&month));
    let name = AggRow {
        storage_byte_ms: total.into(),
        incarnations: vec!["s1".into()],
        ..Default::default()
    };
    let stored = rollup.name_row("2026-07", "acct", "proj", "orders").await;
    let stored = stored.unwrap().expect("carry wrote the name aggregate");
    assert_eq!(stored.incarnations, ["s1"], "the incarnation, named once");
    assert_eq!(json(&stored), json(&name));
    let project = AggRow {
        storage_byte_ms: total.into(),
        ..Default::default()
    };
    let stored = rollup.project_row("2026-07", "acct", "proj").await;
    assert_eq!(json(&stored.unwrap()), json(&Some(project)));
    let carried_states = states.map(|state| SegmentState {
        storage_accounted_through_ms: boundary,
        ..state
    });
    let stored = rollup.stream_segment_states("acct", "proj", "s1").await;
    assert_eq!(json(&stored.unwrap()), json(&carried_states));
}

/// A segment that retains no bytes still closes its month segment at the
/// boundary (gauge zero, final), but bills nothing: no byte-ms is recorded,
/// not even zero, and no name or project aggregate is written for it.
#[tokio::test]
async fn carry_closes_an_empty_segment_without_billing_or_naming_it() {
    let rollup = quick_rollup("carry-empty").await;
    let (start, boundary) = (month_start_ms(2026, 7), month_start_ms(2026, 8));
    let now = boundary + 5;
    put_segment(&rollup, "s2", 0, &retained("idle", 0, start + 1_000)).await;
    july_close(&rollup, now).carry(CARRY_CURSOR).await.unwrap();

    let month = MonthRow {
        account_id: "acct".into(),
        stream_name: "idle".into(),
        segments: [(0, carried(0, ""))].into(),
        updated_ms: now,
        ..Default::default()
    };
    let stored = stored_month(&rollup, "s2").await;
    assert_eq!(storage_view(&stored), storage_view(&month));
    assert_eq!(json(&stored), json(&month));
    assert_eq!(
        keys(&rollup).await,
        [
            "meta/close-seg-cursor/2026-07",
            "month/2026-07/acct/proj/s2",
            "segment/acct/proj/s2/0",
        ]
    );
}

/// A segment whose storage is already accounted through the boundary is left
/// alone: carry writes no month row and no aggregate for it and keeps its
/// state's bytes; only the phase's resume cursor is written.
#[tokio::test]
async fn carry_leaves_a_segment_accounted_through_the_boundary_alone() {
    let rollup = quick_rollup("carry-accounted").await;
    let boundary = month_start_ms(2026, 8);
    put_segment(&rollup, "s1", 0, &retained("orders", 100, boundary)).await;
    let key = k_segment("acct", "proj", "s1", 0);
    let before = rollup.db.get(&key).await.unwrap();
    july_close(&rollup, boundary + 5)
        .carry(CARRY_CURSOR)
        .await
        .unwrap();
    assert_eq!(
        keys(&rollup).await,
        ["meta/close-seg-cursor/2026-07", "segment/acct/proj/s1/0"]
    );
    assert_eq!(rollup.db.get(&key).await.unwrap(), before);
}
