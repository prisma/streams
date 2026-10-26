//! Native queues under the cell's overload gates (layout 6A package 1,
//! DESIGN-6A §4.1, docs/QUEUES-DESIGN.md §6.1): on a cell that streams and
//! queues share, a queue request meets the gates an append meets. Every
//! assertion reads the wire answers and the queue's stored state (its
//! `queuedMessages`, counted by the committer).

use super::fixture_cell::{Cell, CellSpec, error_code, open_cell};
use super::fixture_http::engine_shutdown;

/// The rig's shed line (MiB), as `fair_shed.rs` sets it: an eighth over it
/// leaves room for the absorber's reservation inside the hard line.
const LINE_MB: u64 = 1000;
/// The flooder's sends under the line: far more than 8 times the compliant
/// project's whole leg.
const FLOOD: usize = 120;
/// Rounds over the line: one compliant send and one flooder send each.
const ROUNDS: usize = 5;
/// The in-flight cap the in-flight leg sets.
const CAP: i64 = 8;

type Answer = (u16, Option<String>, Option<String>);

/// One request of project `i` with a JSON body: its status, error code and
/// Retry-After.
async fn ask(cell: &Cell, i: usize, method: &str, path: &str, body: &str) -> Answer {
    let (status, headers, body) = cell.call(i, method, path, body.as_bytes()).await;
    (
        status,
        error_code(&body),
        headers.get("retry-after").cloned(),
    )
}

/// One send of `{"n": n}` to project `i`'s queue `jobs`.
async fn send(cell: &Cell, i: usize, n: usize) -> Answer {
    let body = format!("{{\"body\":{{\"n\":{n}}}}}");
    ask(cell, i, "POST", "/v1/queues/jobs/messages", &body).await
}

/// The messages project `i`'s queue `jobs` holds, as its committer counts
/// them.
async fn queued(cell: &Cell, i: usize) -> u64 {
    let stats = cell.json(i, "GET", "/v1/queues/jobs").await;
    stats["stats"]["queuedMessages"].as_u64().unwrap()
}

fn refused(code: &str, retry: &str) -> Answer {
    (429, Some(code.to_string()), Some(retry.to_string()))
}

const SENT: Answer = (201, None, None);

/// A shared cell (k = 8) of two projects, each with the queue `jobs`, and
/// the first project's flood of sends under every line.
async fn flooded_cell() -> Cell {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    let cell = open_cell(spec).await;
    for i in 0..2 {
        let created = ask(&cell, i, "PUT", "/v1/queues/jobs", "").await;
        assert_eq!(created.0, 201, "project {i}");
    }
    for n in 0..FLOOD {
        assert_eq!(send(&cell, 0, n).await, SENT, "under the line");
    }
    cell
}

/// The flooder's other queue requests, each refused as its sends are.
async fn every_route_refused(cell: &Cell, code: &str, retry: &str) {
    let routes = [
        ("POST", "/v1/queues/jobs/groups/default:claim", "{}"),
        ("GET", "/v1/queues/jobs", ""),
        ("GET", "/v1/queues", ""),
    ];
    for (method, path, body) in routes {
        let answer = ask(cell, 0, method, path, body).await;
        assert_eq!(answer, refused(code, retry), "{method} {path}");
    }
}

/// Over the shed line on a cell shared 8 ways, the queue project that sent
/// more than 1/8 of the cell's recent requests (project 0, 120 sends) is
/// refused `429 project_memory_pressure`, Retry-After 2, on its sends,
/// claims, stats and listing, while project 1's sends are served and
/// stored; under the line both are served again. Before: queue requests
/// were never judged, so the flooder's sends were served over the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_the_queue_project_over_its_share_is_refused_before_its_neighbour() {
    let cell = flooded_cell().await;
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(LINE_MB);
    admission.record_rss_mb(LINE_MB + 1);
    for n in 0..ROUNDS {
        assert_eq!(send(&cell, 1, n).await, SENT, "compliant, round {n}");
        let flood = send(&cell, 0, FLOOD + n).await;
        assert_eq!(flood, refused("project_memory_pressure", "2"), "round {n}");
    }
    every_route_refused(&cell, "project_memory_pressure", "2").await;
    assert_eq!(
        admission.snapshot().shed.blamed,
        u64::try_from(ROUNDS + 3).unwrap(),
        "the flooder's refusals"
    );
    admission.record_rss_mb(0);
    assert_eq!(send(&cell, 0, FLOOD + ROUNDS).await, SENT, "under the line");
    assert_eq!(send(&cell, 1, ROUNDS).await, SENT, "under the line");
    assert_eq!(
        (queued(&cell, 0).await, queued(&cell, 1).await),
        (
            u64::try_from(FLOOD + 1).unwrap(),
            u64::try_from(ROUNDS + 1).unwrap()
        ),
        "the refused sends stored nothing, every served one once"
    );
    engine_shutdown(&cell.state).await;
}

/// At the in-flight write cap (8, the cell's other slots held) on a cell
/// shared 8 ways, the flooding queue project is refused `429 rate_limited`,
/// Retry-After 1, on every queue route, while its neighbour's sends are
/// served and stored. Before: the flooder's queue requests were served at
/// the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_the_inflight_cap_the_queue_project_over_its_share_is_refused_before_its_neighbour() {
    let cell = flooded_cell().await;
    let admission = &cell.state.admission;
    admission.set_max_inflight(CAP);
    admission.add_inflight_for_test(CAP);
    for n in 0..ROUNDS {
        assert_eq!(send(&cell, 1, n).await, SENT, "compliant, round {n}");
        let flood = send(&cell, 0, FLOOD + n).await;
        assert_eq!(flood, refused("rate_limited", "1"), "round {n}");
    }
    every_route_refused(&cell, "rate_limited", "1").await;
    let shed = admission.snapshot().shed;
    assert_eq!(
        (shed.blamed_inflight, shed.blamed),
        (u64::try_from(ROUNDS + 3).unwrap(), 0),
        "the flooder's refusals at entry"
    );
    admission.add_inflight_for_test(-CAP);
    assert_eq!(send(&cell, 0, FLOOD + ROUNDS).await, SENT, "under the cap");
    assert_eq!(
        (queued(&cell, 0).await, queued(&cell, 1).await),
        (
            u64::try_from(FLOOD + 1).unwrap(),
            u64::try_from(ROUNDS).unwrap()
        )
    );
    engine_shutdown(&cell.state).await;
}
