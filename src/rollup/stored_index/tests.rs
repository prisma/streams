//! T3 (stored-cap B3): the rollup's account-free stored index. Each test
//! applies real ledger pages of segment snapshots; the figures a page
//! answers are what the rollup step publishes to admission's table.
#![cfg(test)]

use super::{StoredRow, k_stored, k_stored_seg};
use crate::billing::{
    BillingIdentity, SegmentSnapshot, UsageEnvelope, UsagePayload, month_start_ms,
};
use crate::quota::stored::{StoredFigure, StoredTable};
use crate::rollup::{SegmentState, UsageRollup, decode_json, k_segment, read_faults, read_json};
use crate::tenant::ProjectId;
use rand::{Rng, SeedableRng};
use slatedb::Db;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One segment of one stream of one project, as an account stamps it.
#[derive(Clone, Copy)]
struct Seg {
    account: &'static str,
    project: &'static str,
    stream: &'static str,
    segment: u32,
}

/// Segment 0 of project p's stream s, under account a.
const P0: Seg = Seg {
    account: "a",
    project: "p",
    stream: "s",
    segment: 0,
};

impl Seg {
    fn under(self, account: &'static str) -> Self {
        Self { account, ..self }
    }

    /// The live July snapshot `version`: its storage clock `through` ms
    /// into July and its gauge.
    fn at(self, version: u64, through: i64, gauge: u64) -> SegmentSnapshot {
        SegmentSnapshot {
            identity: BillingIdentity {
                account_id: self.account.into(),
                project_id: self.project.into(),
                stream_id: self.stream.into(),
                stream_name: format!("name-{}", self.stream),
            },
            segment_id: self.segment,
            usage_version: version,
            month: "2026-07".into(),
            month_final: false,
            ingest_payload_bytes_month: 0,
            ingest_records_month: 0,
            owned_frame_bytes_current: gauge,
            storage_byte_ms_month: "0".into(),
            storage_accounted_through_ms: month_start_ms(2026, 7) + through,
            retained_by_forks: false,
        }
    }
}

/// July's final snapshot, accounted through August's start.
fn july_final(snapshot: SegmentSnapshot) -> SegmentSnapshot {
    SegmentSnapshot {
        month_final: true,
        storage_accounted_through_ms: month_start_ms(2026, 8),
        ..snapshot
    }
}

/// An August live snapshot, accounted `ms` past August's start.
fn august(snapshot: SegmentSnapshot, ms: i64) -> SegmentSnapshot {
    SegmentSnapshot {
        month: "2026-08".into(),
        storage_accounted_through_ms: month_start_ms(2026, 8) + ms,
        ..snapshot
    }
}

fn figure(project: &str, bytes: u64, written: u64) -> StoredFigure {
    StoredFigure {
        project: ProjectId::new(project).unwrap(),
        bytes,
        written,
    }
}

fn by_project(mut figures: Vec<StoredFigure>) -> Vec<StoredFigure> {
    figures.sort_by(|a, b| a.project.as_str().cmp(b.project.as_str()));
    figures
}

async fn rollup_on(store: Arc<object_store::memory::InMemory>, name: &str) -> UsageRollup {
    UsageRollup {
        db: Arc::new(Db::builder(name, store).build().await.unwrap()),
        close_rows_visited: Default::default(),
    }
}

async fn rollup(name: &str) -> UsageRollup {
    rollup_on(Arc::new(object_store::memory::InMemory::new()), name).await
}

/// Applies one page of `snapshots` and answers the figures it changed.
async fn apply(
    r: &UsageRollup,
    snapshots: impl IntoIterator<Item = SegmentSnapshot>,
    cursor: &str,
) -> anyhow::Result<Vec<StoredFigure>> {
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
    Ok(by_project(r.apply_page(&envelopes, cursor).await?))
}

/// A project's durable index row: (bytes, written).
async fn row(r: &UsageRollup, project: &str) -> (u64, u64) {
    let row: Option<StoredRow> = read_json(&r.db, &k_stored(project)).await.unwrap();
    let row = row.unwrap_or_default();
    (row.bytes, row.written)
}

async fn rows(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut scan = db.scan(..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key.to_vec(), row.value.to_vec()));
    }
    rows
}

/// Every `segment/` row of the database: (project, stream, segment, state).
async fn segment_rows(db: &Db) -> Vec<(String, String, String, SegmentState)> {
    let mut out = Vec::new();
    let mut scan = db.scan_prefix(&b"segment/"[..], ..).await.unwrap();
    while let Some(kv) = scan.next().await.unwrap() {
        let key = String::from_utf8(kv.key.to_vec()).unwrap();
        let [_, _, project, stream, segment] = key.splitn(5, '/').collect::<Vec<_>>()[..] else {
            panic!("{key}");
        };
        let state: SegmentState = decode_json(&kv.value).unwrap();
        out.push((project.into(), stream.into(), segment.into(), state));
    }
    out
}

/// (project, stream, segment).
type SegmentKey = (String, String, String);

/// The test oracle (the design's derivation, never production code): the
/// `segment/` rows deduplicated by (project, stream, segment), keeping the
/// greatest (version, storage clock), their gauges summed per project.
async fn derive_from_segments(db: &Db) -> BTreeMap<String, u64> {
    let mut latest: BTreeMap<SegmentKey, (u64, i64, u64)> = BTreeMap::new();
    for (project, stream, segment, state) in segment_rows(db).await {
        let seen = (
            state.usage_version,
            state.storage_accounted_through_ms,
            state.owned_frame_bytes_current,
        );
        let held = latest.entry((project, stream, segment)).or_insert(seen);
        if (seen.0, seen.1) > (held.0, held.1) {
            *held = seen;
        }
    }
    let mut sums = BTreeMap::new();
    for ((project, ..), (.., gauge)) in latest {
        *sums.entry(project).or_default() += gauge;
    }
    sums
}

/// T3(a): a project's figure is the latest gauge of each of its live
/// segments. A replayed snapshot and an older version change nothing and
/// publish nothing, nor does a newer one that moves no gauge; on a version
/// tie the later storage clock wins, in either order of a rollover's
/// month-final and live snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_figure_is_the_latest_gauge_of_each_live_segment() {
    let r = rollup("stored-latest").await;
    let p1 = Seg { segment: 1, ..P0 };
    let first = apply(&r, [P0.at(1, 10, 100), p1.at(1, 10, 50)], "c1");
    assert_eq!(first.await.unwrap(), [figure("p", 150, 150)]);
    let newer = apply(&r, [P0.at(3, 30, 300)], "c2").await.unwrap();
    assert_eq!(newer, [figure("p", 350, 350)]);
    let stale = apply(&r, [P0.at(3, 30, 300), P0.at(2, 40, 999)], "c3");
    let nothing = Vec::<StoredFigure>::new();
    assert_eq!(
        stale.await.unwrap(),
        nothing,
        "a replay and an older version"
    );
    assert_eq!(row(&r, "p").await, (350, 350));
    let unmoved = apply(&r, [P0.at(3, 35, 300)], "c3b").await.unwrap();
    assert_eq!(unmoved, nothing, "a later clock that moves no gauge");
    let rollover = [
        july_final(P0.at(4, 0, 310)),
        august(P0.at(4, 0, 320), 5),
        august(p1.at(4, 0, 70), 5),
        july_final(p1.at(4, 0, 60)),
    ];
    let tied = apply(&r, rollover, "c4").await.unwrap();
    assert_eq!(tied, [figure("p", 390, 390)], "320 + 70");
    assert_eq!(row(&r, "p").await, (390, 390));
    r.db.close().await.unwrap();
}

/// T3(b): a workspace change re-stamps a segment's account. The segment
/// rows, keyed by account, then hold the segment twice; the index counts it
/// once, by its latest snapshot, in order and in one page out of order. A
/// snapshot under another account that is not later than the one the index
/// holds (the same version and clock) moves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_workspace_change_counts_a_segment_once() {
    let r = rollup("stored-workspace").await;
    let under_a = apply(&r, [P0.at(3, 30, 300)], "c1").await.unwrap();
    assert_eq!(under_a, [figure("p", 300, 300)]);
    let under_b = apply(&r, [P0.under("b").at(4, 40, 400)], "c2").await;
    assert_eq!(under_b.unwrap(), [figure("p", 400, 400)], "400, not 700");
    let summed: u64 = segment_rows(&r.db)
        .await
        .iter()
        .map(|(.., state)| state.owned_frame_bytes_current)
        .sum();
    assert_eq!(summed, 700, "the account-keyed rows hold it twice");
    let q0 = Seg { project: "q", ..P0 };
    let reordered = [q0.under("b").at(4, 40, 400), q0.at(3, 30, 300)];
    let page = apply(&r, reordered, "c3").await.unwrap();
    assert_eq!(
        page,
        [figure("q", 400, 400)],
        "the older account's arrives last"
    );
    let repeated = apply(&r, [P0.under("c").at(4, 40, 999)], "c4").await;
    let nothing = Vec::<StoredFigure>::new();
    assert_eq!(
        repeated.unwrap(),
        nothing,
        "not later than the held snapshot"
    );
    assert_eq!(row(&r, "p").await, (400, 400));
    r.db.close().await.unwrap();
}

/// T3(c): a close gives its segment's bytes back and `written` keeps them:
/// on the live path, and as a late close of a month already closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_close_gives_its_bytes_back_and_written_keeps_them() {
    // July 2026 closes only on the real clock: a scenario that injects the
    // billing clock holds the write lock while it does.
    let _clock = crate::billing::billing_clock_lock().read().await;
    let r = rollup("stored-close").await;
    let p1 = Seg { segment: 1, ..P0 };
    let opened = apply(&r, [P0.at(1, 10, 100), p1.at(1, 10, 40)], "c1");
    assert_eq!(opened.await.unwrap(), [figure("p", 140, 140)]);
    let closed = apply(&r, [P0.at(2, 20, 0)], "c2").await.unwrap();
    assert_eq!(closed, [figure("p", 40, 140)]);
    assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 1);
    let late = apply(&r, [p1.at(2, 25, 0)], "c3").await.unwrap();
    assert_eq!(
        late,
        [figure("p", 0, 140)],
        "a late close of a closed month"
    );
    assert_eq!(row(&r, "p").await, (0, 140));
    r.db.close().await.unwrap();
}

/// T3(g): `written` adds each gauge increase among the snapshots one page
/// holds of a segment (a rollup behind its ledger reads several drain
/// rounds at once), in the segment state's order: a growth and its close,
/// one arriving after the other across a workspace change, and a fall
/// between two growths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn written_adds_each_increase_one_page_holds() {
    let r = rollup("stored-in-page").await;
    let grown_and_closed = apply(&r, [P0.at(1, 10, 100), P0.at(2, 20, 0)], "c1");
    assert_eq!(grown_and_closed.await.unwrap(), [figure("p", 0, 100)]);
    assert_eq!(row(&r, "p").await, (0, 100));
    let q0 = Seg { project: "q", ..P0 };
    let reordered = [q0.under("b").at(2, 20, 0), q0.at(1, 10, 40)];
    let page = apply(&r, reordered, "c2").await.unwrap();
    assert_eq!(page, [figure("q", 0, 40)], "the close arrives first");
    let r0 = Seg { project: "r", ..P0 };
    let page = [r0.at(1, 10, 50), r0.at(2, 20, 30), r0.at(3, 30, 80)];
    let fell = apply(&r, page, "c3").await.unwrap();
    assert_eq!(fell, [figure("r", 80, 100)], "50 + (80 - 30)");
    r.db.close().await.unwrap();
}

/// One segment's ledger as its drain rounds emit it: versions 1 to n in
/// order, each with its gauge (0 a close), its account moving from a to b
/// at a random version. Answers it, the last gauge and the sum of the
/// gauge's increases.
fn random_history(rng: &mut rand::rngs::StdRng, seg: Seg) -> (Vec<SegmentSnapshot>, u64, u64) {
    let versions = rng.random_range(1..=6u64);
    let moves_at = rng.random_range(1..=versions + 1);
    let (mut gauge, mut written) = (0, 0);
    let mut history = Vec::new();
    for version in 1..=versions {
        let account = if version < moves_at { "a" } else { "b" };
        let now = rng.random_range(0..50u64);
        written += now.saturating_sub(gauge);
        gauge = now;
        let through = i64::try_from(version).unwrap() * 1000;
        history.push(seg.under(account).at(version, through, now));
    }
    (history, gauge, written)
}

/// T3(h): over seeded random ledgers of eight segments, their histories
/// interleaved with replays and cut into pages of one to six snapshots,
/// each project's durable and last published figures are the sum of its
/// segments' last gauges and of their gauge increases, wherever the pages
/// cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn written_does_not_depend_on_where_pages_cut_the_ledger() {
    for seed in 0..16u64 {
        let r = rollup(&format!("stored-cut-{seed}")).await;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let (ledger, mut expected) = random_ledger(&mut rng);
        let published = apply_cut(&r, &mut rng, &ledger).await;
        let mut durable = BTreeMap::new();
        for project in expected.keys() {
            durable.insert(project.clone(), row(&r, project).await);
        }
        assert_eq!(durable, expected, "seed {seed}: the durable index");
        expected.retain(|_, figures| *figures != (0, 0));
        assert_eq!(published, expected, "seed {seed}: the published figures");
        r.db.close().await.unwrap();
    }
}

/// Each project's (bytes, written).
type Figures = BTreeMap<String, (u64, u64)>;

/// A ledger of every segment's random history, interleaved, one snapshot in
/// five followed by a replay of an earlier one, and each project's expected
/// (bytes, written).
fn random_ledger(rng: &mut rand::rngs::StdRng) -> (Vec<SegmentSnapshot>, Figures) {
    let mut expected = Figures::new();
    let mut histories = Vec::new();
    for (project, stream, segment) in every_segment() {
        let seg = Seg {
            project,
            stream,
            segment,
            ..P0
        };
        let (history, last, written) = random_history(rng, seg);
        let figures = expected.entry(project.to_owned()).or_default();
        *figures = (figures.0 + last, figures.1 + written);
        histories.push(std::collections::VecDeque::from(history));
    }
    let mut ledger: Vec<SegmentSnapshot> = Vec::new();
    while let Some(next) = next_of(rng, &mut histories) {
        ledger.push(next);
        if rng.random_range(0..5) == 0 {
            ledger.push(ledger[rng.random_range(0..ledger.len())].clone());
        }
    }
    (ledger, expected)
}

/// Applies `ledger` in pages of one to six snapshots and answers each
/// project's last published (bytes, written).
async fn apply_cut(
    r: &UsageRollup,
    rng: &mut rand::rngs::StdRng,
    ledger: &[SegmentSnapshot],
) -> Figures {
    let mut published = BTreeMap::new();
    let mut rest = ledger;
    let mut page = 0;
    while !rest.is_empty() {
        let (cut, after) = rest.split_at(rest.len().min(rng.random_range(1..=6)));
        let figures = apply(r, cut.to_vec(), &format!("c{page}")).await.unwrap();
        let moved = figures
            .into_iter()
            .map(|f| (f.project.as_str().to_owned(), (f.bytes, f.written)));
        published.extend(moved);
        (rest, page) = (after, page + 1);
    }
    published
}

/// Every (project, stream, segment) of projects p and q, streams s and t,
/// segments 0 and 1.
fn every_segment() -> Vec<(&'static str, &'static str, u32)> {
    let mut all = Vec::new();
    for project in ["p", "q"] {
        for stream in ["s", "t"] {
            all.extend([(project, stream, 0), (project, stream, 1)]);
        }
    }
    all
}

/// The next snapshot of a random history that has one left.
fn next_of(
    rng: &mut rand::rngs::StdRng,
    histories: &mut [std::collections::VecDeque<SegmentSnapshot>],
) -> Option<SegmentSnapshot> {
    let left: Vec<usize> = (0..histories.len())
        .filter(|&i| !histories[i].is_empty())
        .collect();
    let pick = *left.get(rng.random_range(0..left.len().max(1)))?;
    histories[pick].pop_front()
}

/// T3(d): a page whose index read fails commits nothing (no row, no cursor)
/// and answers no figure to publish; its replay applies it whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_failed_page_stages_nothing_and_publishes_nothing() {
    let r = rollup("stored-fault").await;
    apply(&r, [P0.at(1, 10, 100)], "c1").await.unwrap();
    let before = rows(&r.db).await;
    for key in [k_stored_seg("p", "s", 0), k_stored("p")] {
        let fault = (Arc::as_ptr(&r.db) as usize, key.clone());
        read_faults().lock().unwrap().insert(fault);
        let failed = apply(&r, [P0.at(2, 20, 300)], "c2").await;
        assert!(failed.is_err(), "{key:?}");
        assert_eq!(rows(&r.db).await, before, "{key:?}");
    }
    let replayed = apply(&r, [P0.at(2, 20, 300)], "c2").await.unwrap();
    assert_eq!(replayed, [figure("p", 300, 300)]);
    r.db.close().await.unwrap();
}

/// A project row holding less than a closing segment gives back is corrupt:
/// the page fails whole and its rows stay as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_project_row_below_a_closing_segment_fails_the_page() {
    let r = rollup("stored-corrupt").await;
    apply(&r, [P0.at(1, 10, 100)], "c1").await.unwrap();
    let corrupt = br#"{"bytes":10,"written":100}"#;
    r.db.put(k_stored("p"), corrupt).await.unwrap();
    let before = rows(&r.db).await;
    let refused = apply(&r, [P0.at(2, 20, 0)], "c2").await.unwrap_err();
    let expected = "stored-seg/p/s/0 gives back 100 stored bytes; its project holds 10";
    assert_eq!(refused.to_string(), expected);
    assert_eq!(rows(&r.db).await, before);
    r.db.close().await.unwrap();
}

/// A snapshot whose project is no project id is not indexed, and says so;
/// the rest of its page applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_snapshot_of_no_project_id_is_reported_and_not_indexed() {
    let r = rollup("stored-bad-id").await;
    let bad = Seg {
        project: "no project",
        ..P0
    };
    let log = crate::sse::test_log::ErrorLog::capture();
    let page = apply(&r, [bad.at(1, 10, 100), P0.at(1, 10, 7)], "c1").await;
    let causes = log.causes();
    drop(log);
    assert_eq!(page.unwrap(), [figure("p", 7, 7)]);
    let not_indexed = "identifier contains a byte outside [A-Za-z0-9_-] at offset 2";
    assert_eq!(causes, [not_indexed]);
    let keys: Vec<Vec<u8>> = rows(&r.db).await.into_iter().map(|(k, _)| k).collect();
    assert!(!keys.contains(&k_stored_seg("no project", "s", 0)));
    assert!(!keys.contains(&k_stored("no project")));
    r.db.close().await.unwrap();
}

/// A version determines its snapshot: one gauge (0 is a close) and one
/// storage clock per segment and version, whichever account stamps it.
fn random_snapshot(rng: &mut rand::rngs::StdRng) -> SegmentSnapshot {
    let project = ["p", "q"][rng.random_range(0..2)];
    let stream = ["s", "t"][rng.random_range(0..2)];
    let segment = rng.random_range(0..2u32);
    let account = ["a", "b"][rng.random_range(0..2)];
    let version = rng.random_range(1..=8u64);
    let salt = u64::from(segment) * 11 + u64::from(project == "q") * 5 + u64::from(stream == "t");
    let gauge = (version * 37 + salt) % 50;
    let seg = Seg {
        account,
        project,
        stream,
        segment,
    };
    seg.at(version, i64::try_from(version).unwrap() * 1000, gauge)
}

/// T3(e): over seeded random snapshot sequences across two accounts
/// (replays, older versions, closes, pages of one to five snapshots), each
/// project's durable and last published figure equals the derivation from
/// the segment rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_index_equals_the_segment_rows_derivation() {
    for seed in 0..16u64 {
        let r = rollup(&format!("stored-random-{seed}")).await;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut published = BTreeMap::new();
        for page in 0..12 {
            let size = rng.random_range(1..=5);
            let snapshots: Vec<_> = (0..size).map(|_| random_snapshot(&mut rng)).collect();
            let page = apply(&r, snapshots, &format!("c{page}")).await.unwrap();
            published.extend(
                page.into_iter()
                    .map(|f| (f.project.as_str().to_owned(), f.bytes)),
            );
        }
        let derived = derive_from_segments(&r.db).await;
        let mut durable = BTreeMap::new();
        for project in derived.keys() {
            durable.insert(project.clone(), row(&r, project).await.0);
        }
        assert_eq!(durable, derived, "seed {seed}: the durable index");
        assert_eq!(published, derived, "seed {seed}: the published figures");
        r.db.close().await.unwrap();
    }
}

/// T3(f): a reopened rollup publishes its durable figures, and the time,
/// into admission's table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_reopened_rollup_publishes_its_durable_figures() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let first = rollup_on(store.clone(), "stored-reopen").await;
    let q0 = Seg { project: "q", ..P0 };
    apply(&first, [P0.at(1, 10, 100), q0.at(1, 10, 30)], "c1")
        .await
        .unwrap();
    apply(&first, [P0.at(2, 20, 0)], "c2").await.unwrap();
    first.db.close().await.unwrap();
    let reopened = rollup_on(store, "stored-reopen").await;
    let table = StoredTable::default();
    reopened.publish_stored_into(&table).await.unwrap();
    let published = by_project(table.figures());
    assert_eq!(published, [figure("p", 0, 100), figure("q", 30, 30)]);
    assert!(table.published_ms() > 0);
    reopened.db.close().await.unwrap();
}

/// D3: a database with segment rows and no index predates the index. Its
/// install publishes nothing and says so; it is never backfilled. A
/// database with the index, or with no rows, says nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_database_that_predates_the_index_is_reported_never_backfilled() {
    let r = rollup("stored-predates").await;
    let table = StoredTable::default();
    let log = crate::sse::test_log::ErrorLog::capture();
    r.publish_stored_into(&table).await.unwrap();
    assert_eq!(log.causes(), Vec::<String>::new(), "an empty database");
    let state = SegmentState {
        owned_frame_bytes_current: 100,
        ..Default::default()
    };
    let key = k_segment("a", "p", "s", 0);
    r.db.put(key, serde_json::to_vec(&state).unwrap())
        .await
        .unwrap();
    let before = rows(&r.db).await;
    r.publish_stored_into(&table).await.unwrap();
    let predates = "\"segment rows and no stored index: this database predates the index\"";
    assert_eq!(log.causes(), [predates]);
    assert_eq!(table.figures(), Vec::new(), "nothing is published");
    assert_eq!(rows(&r.db).await, before, "nothing is backfilled");
    let q0 = Seg { project: "q", ..P0 };
    apply(&r, [q0.at(1, 10, 5)], "c1").await.unwrap();
    r.publish_stored_into(&table).await.unwrap();
    let causes = log.causes();
    drop(log);
    assert_eq!(causes, [predates], "a database with the index");
    assert_eq!(table.figures(), [figure("q", 5, 5)]);
    r.db.close().await.unwrap();
}

/// D3's guard asks whether the index exists, not whether a project moved:
/// a database whose index holds only gauge-0 snapshots (a stream created
/// and deleted before any append, the close its segment's first snapshot)
/// has `segment/` and `stored-seg/` rows and no `stored/` row, and its
/// reopened install publishes nothing and says nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_index_of_closes_alone_is_not_reported_as_predating_it() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let first = rollup_on(store.clone(), "stored-closes-only").await;
    let closed = apply(&first, [P0.at(1, 10, 0)], "c1").await.unwrap();
    assert_eq!(closed, Vec::<StoredFigure>::new());
    let keys: Vec<Vec<u8>> = rows(&first.db).await.into_iter().map(|(k, _)| k).collect();
    assert!(keys.contains(&k_stored_seg("p", "s", 0)));
    assert!(keys.contains(&k_segment("a", "p", "s", 0)));
    assert!(!keys.contains(&k_stored("p")));
    first.db.close().await.unwrap();
    let reopened = rollup_on(store, "stored-closes-only").await;
    let table = StoredTable::default();
    let log = crate::sse::test_log::ErrorLog::capture();
    reopened.publish_stored_into(&table).await.unwrap();
    let causes = log.causes();
    drop(log);
    assert_eq!(causes, Vec::<String>::new());
    assert_eq!(table.figures(), Vec::new());
    assert!(table.published_ms() > 0, "it still dates the table");
    reopened.db.close().await.unwrap();
}
