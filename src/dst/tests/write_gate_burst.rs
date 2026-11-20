//! The write gate sheds a burst's excess, exactly (Addendum 27 of the forward-wedge ruling,
//! 2026-11-20; edge change #272). The gate judged every request counted at its arrival, the
//! writes that had not yet met it and the requests it had already shed included, so a burst that
//! arrived at once shed about 2.4 times its excess, by the order the checks fell in (0 or 459 for
//! one pile of 600 appends). It now judges today's count less those two: a write enters it at its
//! gate in one step with the check, and a shed request never.
//!
//! The hook holds the whole burst in flight before any check runs: three stores in a stack, the
//! outer holding the readers' descriptor reads until the end, the middle the appends' until every
//! request of the burst is in flight, the inner every WAL PUT so that an admitted append stays in
//! flight. Each case asserts its project's fair-shed verdict, then that the sheds are the excess
//! over the verdict's line (`cap`, or `cap x 5/4` for a spared project), exactly.

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell, project};
use crate::admission::fair_shed::Verdict;
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use crate::project_policy::ProjectQuotas;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The production in-flight cap.
const CAP: i64 = 512;

/// Project 0 at a feed weight of 8, which a cell shared eight ways holds as 7 (a weight is
/// clamped to `1..=k-1`, `auth::ceiling`): under its share beside a heavier project 1 at a weight
/// of one (`Spared`), and over it alone (a lone project's load x 8 is over the cell's x 7). On a
/// dedicated cell nobody is over a share (`Even`).
fn weighted(i: usize) -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec: 100_000,
        request_weight: if i == 0 { 8 } else { 1 },
        ..ProjectQuotas::default()
    }
}

/// Every project at a weight of one: project 0 alone is over its share (one eighth of the cell's).
fn unweighted(_: usize) -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec: 100_000,
        ..ProjectQuotas::default()
    }
}

/// Hex of `text`, as the registry keys a project and a stream name (`registry::desc_path`).
fn hex(text: &str) -> String {
    text.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The prefix of project 0's descriptors of the streams named from `first` (a single character).
fn descriptors_from(first: char) -> String {
    let streams = format!("registry/v5/containers/{}/streams/", hex(&project(0)));
    format!("{streams}{}", hex(&first.to_string()))
}

/// The write gate's sheds and fair shedding's in-flight refusals of a blamed project at its
/// entry: both are `429 rate_limited`, and a blamed write is refused at whichever it meets first.
fn sheds_now(admission: &crate::admission::AdmissionController) -> u64 {
    let shed = admission.snapshot().shed;
    shed.inflight + shed.blamed_inflight
}

/// Wait until `done`, at most 30 s.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The three stores of the hook over memory: (outer, middle, inner), the cell's store the outer.
fn stack() -> (Arc<FaultStore>, Arc<FaultStore>, Arc<FaultStore>) {
    let inner = FaultStore::uniform(super::fixture_storage::mem(), 0x272, FaultPlan::CLEAN);
    let middle = FaultStore::uniform(inner.clone(), 0x273, FaultPlan::CLEAN);
    let outer = FaultStore::uniform(middle.clone(), 0x274, FaultPlan::CLEAN);
    (outer, middle, inner)
}

/// A cell of `projects` (project 0 weighted whole) whose project 0 holds `appends` streams `s…`
/// and `reads` streams `r…`, each appended once, with every descriptor past its cache's TTL.
async fn loaded(store: Arc<FaultStore>, quotas: Quotas, appends: usize, reads: usize) -> Cell {
    let (projects, quotas, cli) = quotas;
    let cell = open_cell(CellSpec {
        store,
        quotas,
        cli,
        ..CellSpec::open(projects)
    })
    .await;
    let names: Vec<String> = (0..appends)
        .map(|n| format!("s{n}"))
        .chain((0..reads).map(|n| format!("r{n}")))
        .collect();
    for chunk in names.chunks(64) {
        let made = chunk.iter().map(|name| {
            let path = format!("/v1/streams/{name}");
            let cell = &cell;
            async move { cell.call(0, "PUT", &path, CREATE).await.0 }
        });
        assert!(
            futures_util::future::join_all(made)
                .await
                .iter()
                .all(|s| *s == 201)
        );
        let touched = chunk.iter().map(|name| {
            let path = format!("/v1/streams/{name}/records");
            let cell = &cell;
            async move { cell.call(0, "POST", &path, br#"{"n":0}"#).await.0 }
        });
        assert!(
            futures_util::future::join_all(touched)
                .await
                .iter()
                .all(|s| *s < 300)
        );
    }
    // Past the descriptor cache's 5 s TTL, so every request's next descriptor read is at the
    // store, and past two of fair shedding's 5 s epochs, so the setup's load is out of its window.
    tokio::time::sleep(Duration::from_secs(11)).await;
    cell
}

/// The `appends` of a burst met the gate judged `verdict`, every one but those fair shedding
/// refused at their entry, since `before` (`AdmissionController::gate_verdicts`: blamed, spared,
/// even, none; and the blamed refusals at entry).
fn assert_judged(cell: &Cell, before: ([u64; 4], u64), verdict: Verdict, appends: usize) {
    let (before, blamed_before) = before;
    // A blamed write at the cap is refused at its entry by fair shedding (`shed_blamed`) and
    // never meets the gate: the appends that met it are the rest.
    let at_entry = cell.state.admission.snapshot().shed.blamed_inflight - blamed_before;
    let at = match verdict {
        Verdict::Blamed => 0,
        Verdict::Spared => 1,
        Verdict::Even => 2,
    };
    let met = cell.state.admission.gate_verdicts();
    let mut want = [0; 4];
    want[at] = appends as u64 - at_entry;
    assert_eq!(
        [0, 1, 2, 3].map(|i| met[i] - before[i]),
        want,
        "the appends met the gate judged {verdict:?} (blamed, spared, even, none)"
    );
}

/// A cell's projects and their quotas.
type Quotas = (
    usize,
    fn(usize) -> ProjectQuotas,
    fn(&mut crate::config::CliArgs),
);

/// A dedicated cell (k = 1): nobody is over a share, so every request is judged `Even`.
fn dedicated(_: &mut crate::config::CliArgs) {}

/// A cell shared eight ways, as production's (`CONTAINER_SHARE_K`): fair shedding blames and
/// spares.
fn shared(cli: &mut crate::config::CliArgs) {
    cli.project_share_k = 8;
}

/// One case: project 0 appends to its `appends` streams at once while `reads` reads of its other
/// streams are held in flight beside them, project 1 (when there are two) having sent 800
/// requests first, over its share of the window's (one eighth), so project 0 is spared. Asserts the verdict, then the sheds; returns them.
async fn burst(quotas: Quotas, appends: usize, reads: usize, verdict: Verdict) -> usize {
    let (outer, middle, inner) = stack();
    let projects = quotas.0;
    let cell = loaded(outer.clone(), quotas, appends, reads).await;
    cell.state.admission.set_max_inflight(CAP);
    if projects > 1 {
        let heavier = (0..800).map(|_| cell.call(1, "GET", "/v1/streams/none", b""));
        drop(futures_util::future::join_all(heavier).await);
    }
    let verdicts_before = (
        cell.state.admission.gate_verdicts(),
        cell.state.admission.snapshot().shed.blamed_inflight,
    );
    let line = match verdict {
        Verdict::Spared => CAP + CAP / 4,
        _ => CAP,
    };
    let readers = outer.hold_class_under(
        StoreOp::Get,
        ObjClass::Other,
        &descriptors_from('r'),
        1 << 20,
    );
    let writers = middle.hold_class_under(
        StoreOp::Get,
        ObjClass::Other,
        &descriptors_from('s'),
        1 << 20,
    );
    let _commits = inner.hold_class(StoreOp::Put, ObjClass::Wal, 1 << 20);
    let calls = (0..reads)
        .map(|n| (format!("/v1/streams/r{n}/records"), "GET", &b""[..]))
        .chain((0..appends).map(|n| {
            (
                format!("/v1/streams/s{n}/records"),
                "POST",
                &br#"{"n":1}"#[..],
            )
        }));
    let requests = calls.map(|(path, method, body)| {
        let cell = &cell;
        async move {
            let (status, _, answer) = cell.call(0, method, &path, body).await;
            (method, status, error_code(&answer))
        }
    });
    let admission = cell.state.admission.clone();
    let excess = usize::try_from(i64::try_from(appends + reads).unwrap() - line).unwrap_or(0);
    let judged = async {
        let held = |n: &std::sync::atomic::AtomicU64| n.load(std::sync::atomic::Ordering::Relaxed);
        until("the whole burst is in flight", || {
            held(&writers) >= appends as u64 && held(&readers) >= reads as u64
        })
        .await;
        middle.release_hold();
        // Every append has been judged once the shed count has held for half a second and the
        // judged count holds the reads and the admitted appends (the WAL holds their commits).
        let mut last = (u64::MAX, 0);
        until("every append is judged", || {
            let sheds = sheds_now(&admission);
            last = if last.0 == sheds {
                (sheds, last.1 + 1)
            } else {
                (sheds, 0)
            };
            last.1 >= 100
        })
        .await;
        let sheds = usize::try_from(sheds_now(&admission)).unwrap();
        let held = admission.judged() - i64::try_from(reads + appends - sheds).unwrap_or(0);
        inner.release_hold();
        outer.release_hold();
        (sheds, held)
    };
    let (answers, (sheds, held)) =
        futures_util::future::join(futures_util::future::join_all(requests), judged).await;
    assert_judged(&cell, verdicts_before, verdict, appends);
    let shed_answers = answers
        .iter()
        .filter(|(m, s, code)| *m == "POST" && *s == 429 && code.as_deref() == Some("rate_limited"))
        .count();
    assert_eq!(
        (sheds, shed_answers),
        (excess, excess),
        "{appends} appends beside {reads} reads at a line of {line}: the sheds are the excess"
    );
    assert_eq!(
        held, 0,
        "the judged count held every read and every admitted append"
    );
    let others_ok = answers
        .iter()
        .all(|(m, s, _)| *s < 300 || (*m == "POST" && *s == 429));
    assert!(others_ok, "every other request is answered: {answers:?}");
    super::fixture_http::engine_shutdown(&cell.state).await;
    sheds
}

/// **A burst's sheds are its excess over the line, exactly** (Addendum 27): project 0 alone on
/// a dedicated cell (`Even`, line 512) sheds 88 of 600 appends and 488 of 1,000, and 188 of 600
/// beside 100 reads held in flight; on a cell shared eight ways, beside a heavier project 1
/// (`Spared`, line 640) it sheds none of 600 and 360 of 1,000, and alone (`Blamed`, line 512) 88
/// of 600. Each case first asserts the verdict the gate judged every append with. Before the fix
/// every check saw the whole burst in flight, so each case shed every append over the line's
/// worth of arrivals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_bursts_sheds_are_its_excess_over_the_line() {
    let alone: Quotas = (1, weighted, dedicated);
    let beside: Quotas = (2, weighted, shared);
    let cases = [
        (alone, 600, 0, Verdict::Even),
        (alone, 1_000, 0, Verdict::Even),
        (beside, 600, 0, Verdict::Spared),
        (beside, 1_000, 0, Verdict::Spared),
        (alone, 600, 100, Verdict::Even),
        ((1, unweighted, shared), 600, 0, Verdict::Blamed),
    ];
    let mut seen = Vec::new();
    for (quotas, appends, reads, verdict) in cases {
        seen.push(burst(quotas, appends, reads, verdict).await);
    }
    assert_eq!(seen, [88, 488, 0, 360, 188, 88]);
}

/// One request's effect on the classifier's pins: (met entered at 0, met counted, unmet).
type Pins = ([u64; 2], u64);

/// The pins after one request on its ordinary path, against `before`: a route the classifier
/// names meets the gate entered at 0, once; any other leaves the pins as they were.
fn pinned(before: Pins, after: Pins, gated: bool, what: &str) {
    let want = if gated {
        ([before.0[0] + 1, before.0[1]], before.1)
    } else {
        before
    };
    assert_eq!(after, want, "{what}: (met at 0, met counted), unmet");
}

/// A raw request: its method, headers, body, and whether it meets the gate.
type RawStep<'a> = (&'a str, Vec<(&'a str, &'a str)>, &'a [u8], bool);

/// The product surface's routes, pinned: a create, an append, a batch, a read, metadata, and a
/// seal, which meets the gate only with a final record and so counts from its arrival.
async fn product_routes_pinned() {
    let cell = open_cell(CellSpec {
        quotas: weighted,
        ..CellSpec::open(1)
    })
    .await;
    cell.state.admission.set_max_inflight(CAP);
    let product = [
        ("PUT", "/v1/streams/orders", CREATE, false),
        (
            "POST",
            "/v1/streams/orders/records",
            &br#"{"n":1}"#[..],
            true,
        ),
        (
            "POST",
            "/v1/streams/orders/records:batch",
            &br#"[{"n":2},{"n":3}]"#[..],
            true,
        ),
        ("GET", "/v1/streams/orders/records", &b""[..], false),
        ("GET", "/v1/streams/orders", &b""[..], false),
        ("POST", "/v1/streams/orders:seal", &b"{}"[..], false),
    ];
    for (method, path, body, gated) in product {
        let before = cell.state.admission.gate_pins();
        let (status, _, answer) = cell.call(0, method, path, body).await;
        let what = format!(
            "{method} {path}: {status} {}",
            String::from_utf8_lossy(&answer)
        );
        assert!(status < 300, "{what}");
        pinned(before, cell.state.admission.gate_pins(), gated, &what);
    }
    super::fixture_http::engine_shutdown(&cell.state).await;
}

/// The queue surface's routes, pinned: a create, a send, a batch, a claim, a settle.
async fn queue_routes_pinned() {
    let queues = open_cell(CellSpec {
        quotas: weighted,
        ..CellSpec::queues(1)
    })
    .await;
    queues.state.admission.set_max_inflight(CAP);
    let claim = br#"{"max":1,"leaseMs":600000,"waitMs":0}"#;
    let mut token = String::new();
    let steps: [(&str, &str, Vec<u8>, bool); 5] = [
        ("PUT", "/v1/queues/jobs", b"{}".to_vec(), false),
        (
            "POST",
            "/v1/queues/jobs/messages",
            br#"{"body":{"n":1}}"#.to_vec(),
            true,
        ),
        (
            "POST",
            "/v1/queues/jobs/messages:batch",
            br#"{"messages":[{"body":{"n":2}}]}"#.to_vec(),
            true,
        ),
        (
            "POST",
            "/v1/queues/jobs/groups/default:claim",
            claim.to_vec(),
            false,
        ),
        (
            "POST",
            "/v1/queues/jobs/groups/default:settle",
            Vec::new(),
            true,
        ),
    ];
    for (method, path, body, gated) in steps {
        let body = if body.is_empty() {
            format!(r#"{{"ops":[{{"token":"{token}","op":"ack"}}]}}"#).into_bytes()
        } else {
            body
        };
        let before = queues.state.admission.gate_pins();
        let (status, _, answer) = queues.call(0, method, path, &body).await;
        let what = format!(
            "{method} {path}: {status} {}",
            String::from_utf8_lossy(&answer)
        );
        assert!(status < 300, "{what}");
        if path.ends_with(":claim") {
            let claimed: serde_json::Value = serde_json::from_slice(&answer).unwrap();
            token = claimed["messages"][0]["token"].as_str().unwrap().to_owned();
        }
        pinned(before, queues.state.admission.gate_pins(), gated, &what);
    }
    super::fixture_http::engine_shutdown(&queues.state).await;
}

/// The raw surface's routes, pinned: a create, an append, a close.
async fn raw_routes_pinned() {
    use super::fixture_requests::hreq;
    let (state, addr) = super::fixture_http::http_rig(super::fixture_storage::mem()).await;
    state.admission.set_max_inflight(CAP);
    let json = ("content-type", "application/json");
    let raw: [RawStep<'_>; 3] = [
        ("PUT", vec![json], b"", false),
        ("POST", vec![json], br#"[{"n":1}]"#, true),
        ("POST", vec![json, ("stream-closed", "true")], b"", true),
    ];
    for (method, headers, body, gated) in raw {
        let before = state.admission.gate_pins();
        let (status, _, answer) = hreq(addr, method, "/v1/stream/raw-pins", &headers, body).await;
        let what = format!(
            "raw {method} {headers:?}: {status} {}",
            String::from_utf8_lossy(&answer)
        );
        assert!(status < 300, "{what}");
        pinned(before, state.admission.gate_pins(), gated, &what);
    }
    super::fixture_http::engine_shutdown(&state).await;
}

/// **The classifier is pinned both ways, route by route** (Addendum 27's first condition): each
/// route that meets the write gate on its ordinary path (a product append and batch, a raw append
/// and close, a queue's send, batch and settle) enters at 0 and meets the gate once, and
/// no request of a route the classifier names is answered without meeting it; the routes beside
/// them that do not meet the gate (a read, a create, metadata, a claim) leave the pins as they
/// were. A route the classifier names and that did not meet the gate would be counted nowhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_route_that_enters_at_zero_meets_the_gate_and_only_they() {
    product_routes_pinned().await;
    queue_routes_pinned().await;
    raw_routes_pinned().await;
}
