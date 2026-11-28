#![cfg(test)]
//! A split of a compacting parent (F5 part 1b, GATE-DECISION-f5-checkpoint-compaction-2026-11-17 and
//! its addenda): Preparing flushes the parent's memtable and waits until its L0 is below its L0
//! watermark's `low` mark before the barrier parks, so the pause's checkpoint finds room for its
//! upload. The parent is `split_pause`'s 400-queue parent, reopened cold, with a compaction of its
//! log held: the compactor's write of its `compactions` file when it schedules one.

use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::split_pause::{
    BOUND, COMPACTION_SOURCES, Engine, QUEUES, flush, queues_in_00, reopened_cold, trimmed, until,
};
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::dst::trace_store::{TraceEventKind, TraceStore};
use crate::dst::{ObjClass, StoreOp};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// How long a 1b test waits for the split, or for appends sent while the compaction is held: past
/// Preparing's bound (12 s under test) with room for a loaded host.
const HELD_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// A fault store's hold, released when the test lets go of it: on success, on a failed assert and
/// on a timeout alike, so a held PUT never outlives its test.
pub(super) struct Held(pub(super) Arc<FaultStore>);

impl Held {
    /// Releases the held PUT now.
    pub(super) fn release(&self) {
        self.0.release_hold();
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.release_hold();
    }
}

/// How long test 1 and the re-settle test hold the compaction once Preparing waits for it, so its
/// wait is at least this long (`settle_ms`).
const HELD_FOR: std::time::Duration = std::time::Duration::from_secs(1);

/// `HELD_FOR` as the phase line's milliseconds, less what Preparing may start after the test saw
/// the split's intent.
const HELD_MS: u64 = 900;

/// A published split's phase-line count `name`.
fn note(published: &crate::topology::split::Published, name: &str) -> u64 {
    published
        .notes
        .iter()
        .find_map(|(note, value)| (note == name).then_some(*value))
        .unwrap_or_else(|| panic!("the phase line's {name}"))
}

/// SlateDB's `l0_max_ssts`, its default (the rig does not set it): a memtable upload waits while
/// the log holds this many L0 tables, until a compaction frees one.
const L0_MAX_SSTS: usize = 8;

/// The parent's L0 line in the two tests of a parent without room: its marks are then low 8 and
/// high 12, so an L0 of `L0_MAX_SSTS` is not settled (8 is not below 8) and the shed stays off
/// (8 is below 12): every append is acked while the compaction is held.
const UNSETTLED_LINE: usize = 16;

/// The parent's L0 in the test of a parent with room: a compaction in flight (at least
/// `COMPACTION_SOURCES` tables), and room under `L0_MAX_SSTS` for Preparing's flush and the
/// pause's upload.
const ROOM_L0: usize = 5;

/// Appends a record to `stream` at `addr` and flushes `leaf`'s memtable, one L0 table each,
/// until `leaf`'s L0 holds `tables`; `k` numbers the records.
pub(super) async fn topped_up(
    (addr, stream): (std::net::SocketAddr, &str),
    leaf: &Engine,
    tables: usize,
    k: &mut u32,
) {
    while leaf.db.manifest().l0().len() < tables {
        let (status, _) = super::fixture_split::append(addr, stream, *k).await;
        assert_eq!(status, 204, "the parent takes a record");
        flush(leaf).await;
        *k += 1;
    }
}

/// A cell over a fault store under a trace store, whose leaf `00` holds `QUEUES` trimmed queues,
/// reopened cold, its L0 line set to `line` (none: the process's), with a compaction of its log
/// held and its L0 filled to `fill` tables: the compactor's write of its `compactions` file when it
/// schedules a compaction is held (L0 tables are themselves PUTs under `compacted/`, so a table PUT
/// cannot name the compactor's), so the compaction's sources stay in L0 for as long as the test
/// holds it. At `L0_MAX_SSTS` a memtable upload waits for that compaction (the 7.7 s Linux case had
/// L0 at 8).
async fn compacting(
    line: Option<usize>,
    fill: usize,
) -> (super::fixture_split::Cell, Held, Arc<TraceStore>, Engine) {
    let fault = FaultStore::uniform(super::fixture_storage::mem(), 0x5413, FaultPlan::CLEAN);
    let trace = TraceStore::verbatim(fault.clone());
    let cell = super::fixture_split::cell_over(trace.clone()).await;
    cell.parent.gc_state().configure(|s| s.auto = false);
    let names = queues_in_00(&cell, QUEUES).await;
    trimmed(&cell, &names).await;
    flush(&cell.parent).await;
    let body = super::fixture_split::body(9);
    let parent = reopened_cold(&cell.state, cell.addr, ("00", &cell.low), &body, true).await;
    if let Some(line) = line {
        parent.set_l0_line(line);
    }
    let compactions = format!("{}/compactions", parent.log_path());
    let engaged = fault.hold_class_under(StoreOp::Put, ObjClass::Other, &compactions, 1);
    let held = Held(fault);
    let mut k = 400;
    topped_up((cell.addr, &cell.low), &parent, COMPACTION_SOURCES, &mut k).await;
    until(
        || engaged.load(Ordering::Relaxed) >= 1,
        || {
            let l0 = parent.db.manifest().l0().len();
            format!("the compactor's compactions write is held (l0 {l0})")
        },
    )
    .await;
    topped_up((cell.addr, &cell.low), &parent, fill, &mut k).await;
    let marks = parent.l0_marks();
    println!(
        "COMPACTING_SETUP held, l0={} line={} low={} high={}",
        parent.db.manifest().l0().len(),
        marks.line,
        marks.low,
        marks.high
    );
    (cell, held, trace, parent)
}

/// One append's answer: its status and, for a refusal, its code (`shard_splitting` is the
/// barrier's, `shard_l0_backpressure` the L0 shed's).
type Answer = (u16, Option<String>);

/// Appends `n` records to `cell`'s low stream: each answer.
async fn appended(cell: &super::fixture_split::Cell, from: u32, n: u32) -> Vec<Answer> {
    let path = format!("/v1/stream/{}", cell.low);
    let json = [("content-type", "application/json")];
    let mut answers = Vec::new();
    for k in from..from + n {
        let body = super::fixture_split::body(k);
        let (status, _, raw) = hreq(cell.addr, "POST", &path, &json, body.as_bytes()).await;
        answers.push((status, super::fixture_cell::error_code(&raw)));
    }
    answers
}

/// Whether every answer in `answers` is a `204`.
fn all_acked(answers: &[Answer]) -> bool {
    answers.iter().all(|(status, _)| *status == 204)
}

/// Where a split's window starts and ends in a trace: the park and the pause's checkpoint.
pub(super) struct At {
    /// The trace's last event about two seconds before the park (the park itself when the test
    /// did not sample the trace while it waited).
    pub(super) before: u64,
    pub(super) parked: u64,
    pub(super) checkpointed: u64,
}

/// What `trace` saw from the park to the publish's topology CAS.
pub(super) struct Window {
    /// Store requests from the park to the pause's checkpoint: the flush and checkpoint phases.
    pause: usize,
    /// Those requests less GETs under `compacted/`: in these two phases the split reads no table
    /// (a WAL flush, a memtable upload, the manifest), so a compactor read landing in them does not
    /// count, and a stall's manifest polls do.
    pause_untabled: usize,
    /// GETs from the park to the CAS, by path class (`tables` is `compacted/`, the split's and a
    /// compactor's).
    gets: std::collections::BTreeMap<&'static str, usize>,
    /// PUTs under `compacted/` (tables: the pause's upload and a compactor's output).
    table_puts: usize,
    /// The compactor's writes of its `compactions` file from the park to the CAS, as trace offsets
    /// from the park: a compaction running through the pause (the addendum "overlap guard"). The
    /// trace cannot tell one compaction's writes from another's (a held compaction makes several
    /// after its release, before the park), so only writes inside the window count: a compaction
    /// that started before the park and reads inside the window without writing in it is not seen,
    /// and fails the 64-GET bound with its window printed.
    compactor_writes: Vec<u64>,
    /// The compactor's writes of its `compactions` file from `At::before` to the park, as
    /// (negative) trace offsets from the park: a compaction submitted before the park, which the
    /// guard does not see.
    writes_before: Vec<i64>,
    /// The window's table GETs by table (its file name): how many and the bytes they asked for.
    tables: std::collections::BTreeMap<String, (usize, u64)>,
}

impl Window {
    /// GETs from the park to the CAS.
    fn gets(&self) -> usize {
        self.gets.values().sum()
    }

    /// Of those, GETs outside `compacted/`: the split's alone, whatever a compactor does.
    fn untabled_gets(&self) -> usize {
        self.gets() - self.gets.get("tables").copied().unwrap_or(0)
    }

    /// The window's counts, for a test's log line and its failures.
    pub(super) fn shown(&self) -> String {
        format!(
            "flush+checkpoint={} (untabled {}) gets={} {:?} table_puts={} compactor_writes={:?} \
             writes_before_park={:?} tables={:?}",
            self.pause,
            self.pause_untabled,
            self.gets(),
            self.gets,
            self.table_puts,
            self.compactor_writes,
            self.writes_before,
            self.tables
        )
    }
}

/// A store path's class, for a window's GETs.
fn path_class(path: &str) -> &'static str {
    if path.contains("/compacted/") {
        "tables"
    } else if path.contains("manifest") {
        "manifest"
    } else if path.contains("/wal/") {
        "wal"
    } else {
        "other"
    }
}

/// The `Window` of a split at `at` in `trace`.
pub(super) fn window(trace: &TraceStore, at: &At) -> Window {
    let events = trace.events();
    let operations = |from: u64, to: u64| {
        events
            .iter()
            .filter(move |e| e.seq > from && e.seq <= to && e.kind == TraceEventKind::Operation)
    };
    let cas = operations(at.parked, u64::MAX)
        .find(|e| e.op == StoreOp::Put && e.path.ends_with("topology.json"))
        .map(|e| e.seq)
        .expect("the publish's topology CAS");
    let mut gets = std::collections::BTreeMap::new();
    let mut tables = std::collections::BTreeMap::new();
    for e in operations(at.parked, cas).filter(|e| e.op == StoreOp::Get) {
        let class = path_class(&e.path);
        *gets.entry(class).or_insert(0) += 1;
        if class == "tables" {
            let name = e.path.rsplit('/').next().unwrap_or_default().to_string();
            let table: &mut (usize, u64) = tables.entry(name).or_default();
            table.0 += 1;
            table.1 += e.bytes.unwrap_or(0);
        }
    }
    let offset =
        |seq: u64| i64::try_from(seq).unwrap_or(i64::MAX) - i64::try_from(at.parked).unwrap_or(0);
    let writes_before = operations(at.before, at.parked)
        .filter(|e| e.op == StoreOp::Put && e.path.contains("/compactions/"))
        .map(|e| offset(e.seq))
        .collect();
    let compactor_writes = operations(at.parked, cas)
        .filter(|e| e.op == StoreOp::Put && e.path.contains("/compactions/"))
        .map(|e| e.seq - at.parked)
        .collect();
    let table_get = |e: &&crate::dst::trace_store::StoreTraceEvent| {
        e.op == StoreOp::Get && e.path.contains("/compacted/")
    };
    Window {
        pause: operations(at.parked, at.checkpointed).count(),
        pause_untabled: operations(at.parked, at.checkpointed)
            .filter(|e| !table_get(e))
            .count(),
        gets,
        table_puts: operations(at.parked, cas)
            .filter(|e| e.op == StoreOp::Put && e.path.contains("/compacted/"))
            .count(),
        compactor_writes,
        writes_before,
        tables,
    }
}

/// Test 1's bounds on its window (the addendum "condition B"): flush and checkpoint at most 16
/// requests, table GETs aside; GETs outside `compacted/` at most 64; and, unless the compactor
/// wrote its `compactions` file inside the window (`COMPACTION_OVERLAP`, the addendum "overlap
/// guard"), every GET at most 64.
fn assert_compacting_window(seen: &Window, l0_at_park: usize, phases: &str) {
    let window = format!("{} l0_at_park={l0_at_park} [{phases}]", seen.shown());
    assert!(
        seen.pause_untabled <= 16,
        "requests in flush and checkpoint, table GETs aside: {window}"
    );
    assert!(
        seen.untabled_gets() <= BOUND,
        "GETs outside compacted/ from park to CAS: {window}"
    );
    if seen.compactor_writes.is_empty() {
        assert!(seen.gets() <= BOUND, "GETs from park to CAS: {window}");
    } else {
        println!(
            "COMPACTION_OVERLAP {} compactor writes in the window: {window}",
            seen.compactor_writes.len()
        );
    }
}

/// Waits, at most 30 s, until a split of `parent` has started: its intent is stored, so its
/// Preparing runs.
pub(super) async fn started(parent: &Engine) {
    let started = std::time::Instant::now();
    loop {
        let intent = crate::topology::intent::read(&**parent.data_store()).await;
        if matches!(intent, Ok(Some(_))) {
            return;
        }
        assert!(started.elapsed() < HELD_WAIT, "the split stores its intent");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Waits, at most `HELD_WAIT`, for the park `park_rx` reports, sampling `trace` every 50 ms: the
/// trace's last event about two seconds before the park (the first sample's, when it came
/// sooner); none if the split did not park.
async fn parked_after(
    trace: &TraceStore,
    mut park_rx: tokio::sync::oneshot::Receiver<()>,
) -> Option<u64> {
    let started = std::time::Instant::now();
    let mut samples = vec![(started, last_seq(trace))];
    loop {
        let tick = std::time::Duration::from_millis(50);
        match tokio::time::timeout(tick, &mut park_rx).await {
            Ok(reached) => {
                reached.ok()?;
                let cut = std::time::Instant::now().checked_sub(std::time::Duration::from_secs(2));
                let before = cut.and_then(|cut| samples.iter().rev().find(|(at, _)| *at <= cut));
                return before.or(samples.first()).map(|(_, seq)| *seq);
            }
            Err(_) if started.elapsed() >= HELD_WAIT => return None,
            Err(_) => samples.push((std::time::Instant::now(), last_seq(trace))),
        }
    }
}

/// `split` and the test's `control`, both run to their end; a split that ends while the control
/// still waits (refused before the point the control waits for) fails the test at once, with the
/// split's outcome, instead of the control waiting out its bounds.
async fn joined<C: std::future::Future>(
    split: impl std::future::Future<
        Output = Result<crate::topology::split::Published, crate::topology::split::Refused>,
    >,
    control: C,
) -> (
    Result<crate::topology::split::Published, crate::topology::split::Refused>,
    C::Output,
) {
    let (split, control) = (Box::pin(split), Box::pin(control));
    match futures_util::future::select(split, control).await {
        futures_util::future::Either::Left((ended, _)) => {
            let ended = ended.map(|published| published.version);
            let ended = ended.map_err(|refused| refused.to_string());
            panic!("the split ended while the test still waited: {ended:?}")
        }
        futures_util::future::Either::Right((done, split)) => (split.await, done),
    }
}

/// How long the bound test waits for its refusal: Preparing's bound under test (`settle::SETTLE`,
/// 12 s) and 8 s for the rollback; it moves with that bound.
const REFUSED_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// A compaction worker of the test's own on `parent`'s log, over `store` (the fault store under
/// the trace, so its polls and reads are in no traced window), polling every 50 ms
/// (GATE-DECISION-f5-resettle-margin-2026-11-18): the log's embedded worker claims a released job on
/// a poll jittered over 2.5 to 7.5 s, which a test landing a compaction inside Preparing's bound
/// must not wait for. Claims are exclusive; the coordinator, its commit tick and the writer's
/// manifest poll stay the product's.
pub(super) async fn fast_worker(
    parent: &Engine,
    store: Arc<dyn object_store::ObjectStore>,
) -> slatedb::CompactionWorker {
    let options = slatedb::config::CompactionWorkerOptions {
        compactions_poll_interval: std::time::Duration::from_millis(50),
        ..Default::default()
    };
    slatedb::CompactionWorkerBuilder::new(parent.log_path(), store)
        .with_options(options)
        .with_merge_operator(crate::spill::pending::merge_operator())
        .build()
        .await
        .expect("a compaction worker")
}

/// `wait`, with `worker` running beside it (driven here, not spawned); once `wait` ends the worker
/// is stopped, within `HELD_WAIT`.
pub(super) async fn while_running<T>(
    worker: &slatedb::CompactionWorker,
    wait: impl std::future::Future<Output = T>,
) -> T {
    let (run, wait) = (Box::pin(worker.run()), Box::pin(wait));
    match futures_util::future::select(run, wait).await {
        futures_util::future::Either::Left((ended, _)) => {
            panic!("the test's compaction worker ended while the test waited: {ended:?}")
        }
        futures_util::future::Either::Right((out, run)) => {
            let stopped = futures_util::future::join(worker.stop(), run);
            let (stop, ran) = tokio::time::timeout(HELD_WAIT, stopped)
                .await
                .expect("the worker stops within 30 s");
            stop.expect("the worker stops");
            ran.expect("the worker's run");
            out
        }
    }
}

/// The trace's last event id now.
pub(super) fn last_seq(trace: &TraceStore) -> u64 {
    trace.events().last().map_or(0, |event| event.seq)
}

/// **A split of a compacting parent parks only after the compaction** (F5 part 1b,
/// GATE-DECISION-f5-checkpoint-compaction-2026-11-17). The 400-queue parent, reopened cold with a
/// compaction in flight (its compactor's compactions write held) and its L0 at `L0_MAX_SSTS`, its
/// line `UNSETTLED_LINE`. The split's Preparing starts by waiting for the log to settle; while the
/// write is held the barrier does not park and appends to the parent are acked. Released, the split parks and publishes: its
/// flush and checkpoint together make at most 16 store requests, and from the park to the topology
/// CAS it makes at most 64 GETs. Red before part 1b: the split parks at once, and the appends are
/// answered the barrier's `503 shard_splitting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_compacting_parent_parks_only_after_the_compaction() {
    use crate::topology::gate::PausePoint;
    let ready = compacting(Some(UNSETTLED_LINE), L0_MAX_SSTS);
    let (cell, held, trace, parent) = tokio::time::timeout(6 * HELD_WAIT, ready)
        .await
        .expect("the compacting parent is ready within 180 s");
    let point = || {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go) = tokio::sync::oneshot::channel();
        (PausePoint { reached, go }, reached_rx, go_tx)
    };
    let gate = &parent.gc_state().gate;
    let (park, park_rx, park_go) = point();
    let (checkpoint, checkpoint_rx, checkpoint_go) = point();
    gate.pause_at_park(park);
    gate.pause_after_checkpoint(checkpoint);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &parent, "op-held", "inst-a", &*clock);
    let control = async {
        let mut park_rx = park_rx;
        started(&parent).await;
        let Ok(answers) = tokio::time::timeout(HELD_WAIT, appended(&cell, 200, 5)).await else {
            let parked = park_rx.try_recv().is_ok();
            panic!(
                "appends not acked within {HELD_WAIT:?} while the compaction is held (parked: {parked})"
            );
        };
        assert!(
            all_acked(&answers),
            "appends acked while the compaction is held: {answers:?}"
        );
        tokio::time::sleep(HELD_FOR).await;
        assert!(
            park_rx.try_recv().is_err(),
            "parked while the compaction is held"
        );
        let worker = fast_worker(&parent, held.0.clone()).await;
        let released = std::time::Instant::now();
        held.release();
        let parked = while_running(&worker, parked_after(&trace, park_rx)).await;
        let Some(before) = parked else {
            let intent = crate::topology::intent::read(&**parent.data_store()).await;
            let state = intent.map(|read| read.map(|ended| ended.intent.state));
            panic!("the split did not park within 30 s of the release: its intent {state:?}");
        };
        println!("COMPACTING_AT release to park {:?}", released.elapsed());
        let (parked, l0_at_park) = (last_seq(&trace), parent.l0_now());
        assert!(park_go.send(()).is_ok(), "the split waits for go");
        checkpoint_rx.await.expect("the pause checkpoints");
        let checkpointed = last_seq(&trace);
        assert!(checkpoint_go.send(()).is_ok(), "the split waits for go");
        let at = At {
            before,
            parked,
            checkpointed,
        };
        (at, l0_at_park)
    };
    let both = joined(split, control);
    let (published, (at, l0_at_park)) = tokio::time::timeout(3 * HELD_WAIT, both)
        .await
        .expect("the split publishes within 90 s of the start");
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let phases = crate::topology::split::shown_phases(&published);
    let seen = window(&trace, &at);
    println!(
        "COMPACTING_PAUSE {} ms {} l0_at_park={l0_at_park} [{phases}]",
        published.pause.as_millis(),
        seen.shown()
    );
    assert_compacting_window(&seen, l0_at_park, &phases);
    let settle_ms = note(&published, "settle_ms");
    assert!(
        settle_ms >= HELD_MS,
        "Preparing waited {settle_ms} ms, the compaction held {HELD_MS} ms ({phases})"
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// **A split of a parent compacting past the wait's bound is refused before the park** (F5 part
/// 1b). The same parent, its compactor's compactions write held past Preparing's bound: the split is
/// refused, rolled back, without parking the shard; every append sent meanwhile is acked (no
/// `503`), collection runs again and the split's intent ends rolled back. Red before part 1b: the
/// split parks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_parent_compacting_past_the_bound_is_refused_before_the_park() {
    let ready = compacting(Some(UNSETTLED_LINE), L0_MAX_SSTS);
    let (cell, held, _trace, parent) = tokio::time::timeout(6 * HELD_WAIT, ready)
        .await
        .expect("the compacting parent is ready within 180 s");
    let (reached, mut reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let gate = &parent.gc_state().gate;
    gate.pause_at_park(crate::topology::gate::PausePoint { reached, go });
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &parent, "op-bound", "inst-a", &*clock);
    let appending = appended(&cell, 300, 20);
    let both = futures_util::future::join(split, appending);
    let ended = tokio::time::timeout(REFUSED_WITHIN, both).await;
    let parked = reached_rx.try_recv().is_ok();
    drop(go_tx);
    held.release();
    assert!(!parked, "the split parked the shard");
    let (refused, answers) =
        ended.unwrap_or_else(|_| panic!("the split ends within {REFUSED_WITHIN:?}"));
    match refused {
        Err(crate::topology::split::Refused::RolledBack(why)) => {
            println!("REFUSED {why}");
            assert!(why.contains("did not settle"), "{why}");
        }
        other => panic!("the split is refused: {:?}", other.map(|p| p.version)),
    }
    assert!(all_acked(&answers), "every append acked: {answers:?}");
    assert!(!gate.collection_off(), "collection runs again");
    let ended = crate::topology::intent::read(&**parent.data_store())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended.intent.state,
        crate::topology::intent::State::RolledBack
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// **A split of a compacting parent with L0 room parks at once** (F5 part 1b, the addendum
/// "harness line"). The 400-queue parent at the process's L0 line (32: low 16), a compaction in
/// flight (its compactor's compactions write held) and its L0 at `ROOM_L0`, below the settle line
/// and under SlateDB's `L0_MAX_SSTS`: Preparing waits under a second, the barrier parks while the
/// write is still held, the pause's flush and checkpoint make at most 16 store requests and the
/// pause at most 64 GETs from the park to the topology CAS, and no append is refused. A running
/// compaction with free slots does not delay the checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_compacting_parent_with_l0_room_parks_at_once() {
    use crate::topology::gate::PausePoint;
    let (cell, held, trace, parent) =
        tokio::time::timeout(6 * HELD_WAIT, compacting(None, ROOM_L0))
            .await
            .expect("the compacting parent is ready within 180 s");
    let (park, park_rx) = {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go) = tokio::sync::oneshot::channel();
        ((PausePoint { reached, go }, go_tx), reached_rx)
    };
    let (checkpoint, checkpoint_rx) = {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go) = tokio::sync::oneshot::channel();
        ((PausePoint { reached, go }, go_tx), reached_rx)
    };
    until(
        || parent.l0_max_seen() >= ROOM_L0,
        || {
            format!(
                "the watermark's tick sees L0 {ROOM_L0}: {}",
                parent.l0_max_seen()
            )
        },
    )
    .await;
    let gate = &parent.gc_state().gate;
    gate.pause_at_park(park.0);
    gate.pause_after_checkpoint(checkpoint.0);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &parent, "op-room", "inst-a", &*clock);
    let control = async {
        if tokio::time::timeout(HELD_WAIT, park_rx).await.is_err() {
            panic!("the split did not park while the compactions write is held");
        }
        let at_park = parent.l0_now();
        let parked_at = last_seq(&trace);
        assert!(park.1.send(()).is_ok(), "the split waits for go");
        checkpoint_rx.await.expect("the pause checkpoints");
        let checkpointed = last_seq(&trace);
        assert!(checkpoint.1.send(()).is_ok(), "the split waits for go");
        (at_park, parked_at, checkpointed)
    };
    let appending = appended(&cell, 500, 5);
    let all = futures_util::future::join(joined(split, control), appending);
    let ((published, (at_park, parked_at, checkpointed)), answers) =
        tokio::time::timeout(3 * HELD_WAIT, all)
            .await
            .expect("the split publishes within 90 s of the start");
    held.release();
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let phases = crate::topology::split::shown_phases(&published);
    assert!(
        at_park >= COMPACTION_SOURCES,
        "a compaction's sources in L0 at the park: {at_park} ({phases})"
    );
    let settle_ms = published
        .notes
        .iter()
        .find_map(|(name, value)| (*name == "settle_ms").then_some(*value))
        .expect("the phase line's settle_ms");
    assert!(
        settle_ms < 1_000,
        "Preparing's wait: {settle_ms} ms ({phases})"
    );
    let l0_max = published
        .notes
        .iter()
        .find_map(|(name, value)| (*name == "l0_max").then_some(*value))
        .expect("the phase line's l0_max");
    let l0_max = usize::try_from(l0_max).expect("an L0 count");
    assert!(
        l0_max >= ROOM_L0,
        "the parent's highest L0: {l0_max} ({phases})"
    );
    let at = At {
        before: parked_at,
        parked: parked_at,
        checkpointed,
    };
    let seen = window(&trace, &at);
    println!("ROOM_PAUSE at_park={at_park} {} [{phases}]", seen.shown());
    let (pause, gets) = (seen.pause, seen.gets());
    assert!(
        pause <= 16,
        "requests in flush and checkpoint: {pause} ({phases})"
    );
    assert!(gets <= BOUND, "GETs from park to CAS: {gets} ({phases})");
    assert!(all_acked(&answers), "every append acked: {answers:?}");
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// **A split of a compacting parent without L0 room does not park** (F5 part 1b, the control of
/// `a_split_of_a_compacting_parent_with_l0_room_parks_at_once`): the same parent, its line 8 (low
/// 4), so its L0 at `ROOM_L0` is not settled: while the compactions write is held the barrier does
/// not park. Only that is asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_compacting_parent_without_l0_room_does_not_park() {
    let ready = compacting(Some(L0_MAX_SSTS), ROOM_L0);
    let (cell, held, _trace, parent) = tokio::time::timeout(6 * HELD_WAIT, ready)
        .await
        .expect("the compacting parent is ready within 180 s");
    let (reached, mut reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let gate = &parent.gc_state().gate;
    gate.pause_at_park(crate::topology::gate::PausePoint { reached, go });
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &parent, "op-noroom", "inst-a", &*clock);
    let control = async {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let parked = reached_rx.try_recv().is_ok();
        held.release();
        drop(go_tx);
        parked
    };
    let both = futures_util::future::join(split, control);
    let (_, parked) = tokio::time::timeout(HELD_WAIT, both)
        .await
        .expect("the split ends within 30 s");
    assert!(
        !parked,
        "the split parked while the compactions write was held"
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// The parent's log as the store holds it, read by the test's own admin (never through the parent,
/// whose cache or manifest view the read would warm or refresh: the warm check would be handed
/// its answer): its sorted runs' tables by view with their sizes, the runs' ids, and every table
/// (L0 and sorted run) by id and size.
struct Stored {
    tables: std::collections::BTreeMap<u128, u64>,
    runs: Vec<u32>,
    shown: Vec<String>,
}

/// The parent's log at `admin`'s latest manifest.
async fn stored(admin: &slatedb::admin::Admin) -> Stored {
    let manifest = admin
        .read_manifest(None)
        .await
        .expect("a manifest read")
        .expect("the log's manifest");
    let views = manifest.compacted().iter().flat_map(|run| {
        run.sst_views().iter().map(move |view| {
            (
                run.id,
                view.id.0,
                view.estimate_size(),
                format!("{:?}", view.sst.id),
            )
        })
    });
    let mut out = Stored {
        tables: std::collections::BTreeMap::new(),
        runs: manifest.compacted().iter().map(|run| run.id).collect(),
        shown: manifest
            .l0()
            .iter()
            .map(|view| format!("L0 {:?} {} B", view.sst.id, view.estimate_size()))
            .collect(),
    };
    for (run, view, size, id) in views {
        out.tables.insert(view, size);
        out.shown.push(format!("SR{run} {id} {size} B"));
    }
    out
}

/// Waits, at most `HELD_WAIT`, until the stored log's sorted runs differ from `read`: the released
/// compaction landed. The log as it then stands.
async fn changed(
    admin: &slatedb::admin::Admin,
    read: &std::collections::BTreeMap<u128, u64>,
) -> Stored {
    let started = std::time::Instant::now();
    loop {
        let now = stored(admin).await;
        if now.tables != *read {
            return now;
        }
        assert!(
            started.elapsed() < HELD_WAIT,
            "the released compaction lands"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Submits through `admin` a compaction of all the sorted runs `runs` into one: an output that
/// carries every interval row.
async fn runs_merged(admin: &slatedb::admin::Admin, runs: Vec<u32>) {
    use slatedb::compactor::{CompactionSpec, SourceId};
    let destination = runs.iter().copied().min().expect("a sorted run");
    let sources = runs.into_iter().map(SourceId::SortedRun).collect();
    let spec = CompactionSpec::new(sources, destination);
    let started = std::time::Instant::now();
    while admin.submit_compaction(spec.clone()).await.is_err() {
        assert!(
            started.elapsed() < HELD_WAIT,
            "the compactor accepts the merge"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The smallest merge output the landing test waits for: the queues' interval rows merged into
/// one sorted run (about 490 KB, which SlateDB writes as tables of about 225 KB), not a merge of a
/// few small L0s.
const LARGE_OUTPUT: u64 = 256 * 1024;

/// Waits, at most `HELD_WAIT`, until the stored log holds one sorted run, every table of it new
/// since `read`, of at least `LARGE_OUTPUT` together: the merge landed. Its bytes.
async fn landed(
    admin: &slatedb::admin::Admin,
    read: &std::collections::BTreeMap<u128, u64>,
) -> u64 {
    let started = std::time::Instant::now();
    loop {
        let now = stored(admin).await;
        let fresh = now.tables.keys().all(|view| !read.contains_key(view));
        let bytes: u64 = now.tables.values().sum();
        if now.runs.len() == 1 && fresh && bytes >= LARGE_OUTPUT {
            return bytes;
        }
        let shown = &now.shown;
        assert!(
            started.elapsed() < HELD_WAIT,
            "no merged sorted run of {LARGE_OUTPUT} B landed: {shown:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// **A compaction landing after Preparing's read is read again before the park** (F5 part 1b,
/// GATE-DECISION-f5-checkpoint-compaction-2026-11-17 addenda "cold output tables", "1b green and
/// the cold-table counts", "Q1 variant"). The 400-queue parent with L0 room (line 32, L0 at
/// `ROOM_L0`) and a compaction held; once Preparing has read the log the held compaction is
/// released and lands, then a merge of all the parent's sorted runs is submitted, and Preparing
/// goes on only once its output, of at least `LARGE_OUTPUT`, has landed: tables no read has
/// warmed, observed on the store by the test's own reader. The large output is a submitted merge
/// because the compactor's own merges of sorted runs come on its 5 s poll, not on a test's cue;
/// the same compactor runs and writes it. Preparing's warm check refreshes the
/// manifest, finds them and reads the log once more, so the pause reads warm blocks: `warm_reads`
/// is 1, and at most 8 table GETs and 64 GETs from the park to the topology CAS unless the
/// compactor writes inside that window (`COMPACTION_OVERLAP`). Red before the warm check: the
/// pause reads the output cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_compaction_landing_after_preparings_read_is_read_again_before_the_park() {
    use crate::topology::gate::PausePoint;
    let ready = compacting(None, ROOM_L0);
    let (cell, held, trace, parent) = tokio::time::timeout(6 * HELD_WAIT, ready)
        .await
        .expect("the compacting parent is ready within 180 s");
    let point = || {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go) = tokio::sync::oneshot::channel();
        (PausePoint { reached, go }, reached_rx, go_tx)
    };
    let gate = &parent.gc_state().gate;
    let (reads, reads_rx, reads_go) = point();
    let (park, park_rx, park_go) = point();
    let (checkpoint, checkpoint_rx, checkpoint_go) = point();
    gate.pause_after_reads(reads);
    gate.pause_at_park(park);
    gate.pause_after_checkpoint(checkpoint);
    // The test's own reader of the parent's log, on the store under the trace: its reads are not
    // the split's.
    let admin = slatedb::admin::AdminBuilder::new(parent.log_path(), held.0.clone()).build();
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &parent, "op-landed", "inst-a", &*clock);
    let control = async {
        reads_rx.await.expect("Preparing reads the log");
        let read = stored(&admin).await.tables;
        held.release();
        let runs = changed(&admin, &read).await.runs;
        runs_merged(&admin, runs).await;
        let landed = landed(&admin, &read).await;
        assert!(reads_go.send(()).is_ok(), "Preparing waits for go");
        let Ok(Ok(())) = tokio::time::timeout(HELD_WAIT, park_rx).await else {
            panic!("the split did not park within 30 s of the landing");
        };
        let parked = last_seq(&trace);
        let at_park = stored(&admin).await.shown;
        assert!(park_go.send(()).is_ok(), "the split waits for go");
        checkpoint_rx.await.expect("the pause checkpoints");
        let checkpointed = last_seq(&trace);
        assert!(checkpoint_go.send(()).is_ok(), "the split waits for go");
        let at = At {
            before: parked,
            parked,
            checkpointed,
        };
        (at, at_park, landed)
    };
    let both = joined(split, control);
    let (published, (at, at_park, landed)) = tokio::time::timeout(3 * HELD_WAIT, both)
        .await
        .expect("the split publishes within 90 s of the start");
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let phases = crate::topology::split::shown_phases(&published);
    let seen = window(&trace, &at);
    let window = format!("{} tables at the park {at_park:?} [{phases}]", seen.shown());
    println!("LANDED_PAUSE {window}");
    let warm_reads = published
        .notes
        .iter()
        .find_map(|(name, value)| (*name == "warm_reads").then_some(*value))
        .expect("the phase line's warm_reads");
    assert_eq!(warm_reads, 1, "Preparing's warm reads: {window}");
    assert!(
        landed >= LARGE_OUTPUT,
        "the landed merge: {landed} B: {window}"
    );
    let table_gets = seen.gets() - seen.untabled_gets();
    if seen.compactor_writes.is_empty() {
        assert!(table_gets <= 8, "table GETs from park to CAS: {window}");
        assert!(seen.gets() <= BOUND, "GETs from park to CAS: {window}");
    } else {
        let writes = seen.compactor_writes.len();
        println!("COMPACTION_OVERLAP {writes} compactor writes in the window: {window}");
    }
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// The parent's line in the re-settle test: low 6 and high 9, so `ROOM_L0` (5) settles at once
/// and a flush to 7 (under SlateDB's 8, the shed off) brings it back to its line.
const RESETTLE_LINE: usize = 12;

/// **A flush that brings the parent to its line after Preparing's read is settled again before
/// the park** (F5 part 1b, the warm check's L0 re-check). The 400-queue parent at line
/// `RESETTLE_LINE` with L0 at `ROOM_L0` and a compaction held: Preparing settles at once and reads
/// the log; then records flushed to L0 bring it to 7, at its line. The warm check waits again: the
/// barrier does not park while the compaction is held (`HELD_FOR`), parks once it lands, and the
/// phase line's `settle_ms` covers the hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_flush_that_brings_the_parent_to_its_line_after_preparings_read_is_settled_again_before_the_park()
 {
    use crate::topology::gate::PausePoint;
    let ready = compacting(Some(RESETTLE_LINE), ROOM_L0);
    let (cell, held, _trace, parent) = tokio::time::timeout(6 * HELD_WAIT, ready)
        .await
        .expect("the compacting parent is ready within 180 s");
    let (reads, reads_rx) = tokio::sync::oneshot::channel();
    let (reads_go, reads_wait) = tokio::sync::oneshot::channel();
    let (park, mut park_rx) = tokio::sync::oneshot::channel();
    let (park_go, park_wait) = tokio::sync::oneshot::channel();
    let gate = &parent.gc_state().gate;
    gate.pause_after_reads(PausePoint {
        reached: reads,
        go: reads_wait,
    });
    gate.pause_at_park(PausePoint {
        reached: park,
        go: park_wait,
    });
    let clock = cell.state.runtime.clock.clone();
    let a74_before = a74_rows(&parent).await;
    let a74_mark = _trace.events().last().map_or(0, |event| event.seq);
    println!("A74_SPLIT_START l0={} rows={}", parent.l0_now(), a74_before.len());
    let split = async {
        let ended = crate::topology::split::split(
            &cell.state.shards,
            &parent,
            "op-resettle",
            "inst-a",
            &*clock,
        )
        .await;
        if ended.is_err() {
            a74_report(&parent, &a74_before, &_trace, a74_mark).await;
        }
        ended
    };
    let begun = std::time::Instant::now();
    let control = async {
        let at = |step: &str| println!("RESETTLE_AT {step} {:?}", begun.elapsed());
        reads_rx.await.expect("Preparing reads the log");
        at("read");
        topped_up((cell.addr, &cell.low), &parent, 7, &mut 600).await;
        at("topped up");
        assert!(reads_go.send(()).is_ok(), "Preparing waits for go");
        tokio::time::sleep(HELD_FOR).await;
        let parked = park_rx.try_recv().is_ok();
        let worker = fast_worker(&parent, held.0.clone()).await;
        at("released");
        held.release();
        assert!(!parked, "parked at its line while the compaction is held");
        let landed = while_running(&worker, tokio::time::timeout(HELD_WAIT, park_rx)).await;
        let Ok(Ok(())) = landed else {
            let l0 = parent.l0_now();
            panic!("the split did not park within 30 s of the release (l0 {l0})");
        };
        at("parked");
        assert!(park_go.send(()).is_ok(), "the split waits for go");
    };
    let both = joined(split, control);
    let (published, ()) = tokio::time::timeout(3 * HELD_WAIT, both)
        .await
        .expect("the split publishes within 90 s of the start");
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let phases = crate::topology::split::shown_phases(&published);
    println!("RESETTLE [{phases}]");
    let settle_ms = note(&published, "settle_ms");
    assert!(
        settle_ms >= HELD_MS,
        "Preparing waited {settle_ms} ms, the compaction held {HELD_MS} ms ({phases})"
    );
    let l0 = note(&published, "l0");
    assert!(
        l0 < 6,
        "L0 at the park, under the line's low 6: {l0} ({phases})"
    );
    tokio::time::timeout(HELD_WAIT, engine_shutdown(&cell.state))
        .await
        .expect("the cell shuts down within 30 s");
}

/// Measure only (A74, never lands): every row of `parent`'s log, by key, with its value's length and hash.
async fn a74_rows(parent: &Engine) -> std::collections::BTreeMap<Vec<u8>, (usize, u64)> {
    use std::hash::{Hash, Hasher};
    let mut rows = std::collections::BTreeMap::new();
    let mut scan = parent.db.scan(..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        row.value.hash(&mut hasher);
        rows.insert(row.key.to_vec(), (row.value.len(), hasher.finish()));
    }
    rows
}

/// Measure only (A74): after a refused split, the rows that changed since its start and the store's
/// operations since then, so the write that made the sixth L0 table is named.
async fn a74_report(
    parent: &Engine,
    before: &std::collections::BTreeMap<Vec<u8>, (usize, u64)>,
    trace: &TraceStore,
    mark: u64,
) {
    let after = a74_rows(parent).await;
    let hex = |key: &[u8]| crate::crypto::hex(&key[..key.len().min(24)]);
    println!("A74_REFUSED l0={} rows={}", parent.l0_now(), after.len());
    for (key, value) in &after {
        match before.get(key) {
            None => println!("A74_ROW added {} len={} key_len={}", hex(key), value.0, key.len()),
            Some(old) if old != value => {
                println!("A74_ROW changed {} len={}->{} key_len={}", hex(key), old.0, value.0, key.len());
            }
            Some(_) => {}
        }
    }
    for key in before.keys().filter(|key| !after.contains_key(*key)) {
        println!("A74_ROW removed {} key_len={}", hex(key), key.len());
    }
    let log = parent.log_path();
    for event in trace.events().into_iter().filter(|event| event.seq > mark).take(300) {
        let ours = if event.path.contains(log.as_str()) { "parent" } else { "other" };
        println!("A74_OP {} {ours} {:?} {:?} {:?} {}", event.seq, event.kind, event.op, event.class, event.path);
    }
}
