//! Native queues under the cell's overload gates (layout 6A package 1,
//! DESIGN-6A §4.1, docs/QUEUES-DESIGN.md §6.1): on a cell that streams and
//! queues share, a queue request meets the gates an append meets. Every
//! assertion reads the wire answers and the queue's stored state (its
//! `queuedMessages`, counted by the committer).

use super::fixture_cell::{Cell, CellSpec, error_code, open_cell, project};
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
/// stored, inside line x 9/8 too, and past it shed `429 rate_limited` as an
/// append is; under the line both are served again. Before: queue requests
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
    // Spared, the neighbour's sends pass the RSS gate inside line x 9/8
    // (65 MiB clear of the absorber's reservation, as `fair_shed.rs`), and
    // past it every send is shed as an append is.
    admission.record_rss_mb(LINE_MB * 9 / 8 - 65);
    assert_eq!(send(&cell, 1, ROUNDS).await, SENT, "inside line x 9/8");
    admission.record_rss_mb(LINE_MB * 9 / 8 + 1);
    assert_eq!(
        send(&cell, 1, 99).await,
        refused("rate_limited", "2"),
        "past it"
    );
    admission.record_rss_mb(0);
    assert_eq!(send(&cell, 0, FLOOD + ROUNDS).await, SENT, "under the line");
    assert_eq!(send(&cell, 1, ROUNDS + 1).await, SENT, "under the line");
    assert_eq!(
        (queued(&cell, 0).await, queued(&cell, 1).await),
        (
            u64::try_from(FLOOD + 1).unwrap(),
            u64::try_from(ROUNDS + 2).unwrap()
        ),
        "the refused sends stored nothing, every served one once"
    );
    engine_shutdown(&cell.state).await;
}

/// At the in-flight write cap (8, the cell's other slots held) on a cell
/// shared 8 ways, the flooding queue project is refused `429 rate_limited`,
/// Retry-After 1, on every queue route, while its neighbour's sends are
/// served and stored up to cap x 5/4 and refused as appends are past it.
/// Before: the flooder's queue requests were served at the cap.
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
    // Spared, the neighbour's sends pass the in-flight gate up to cap x 5/4
    // (10 active), and past it every send is refused as an append is.
    admission.add_inflight_for_test(CAP / 4 - 1);
    assert_eq!(send(&cell, 1, ROUNDS).await, SENT, "at cap x 5/4");
    admission.add_inflight_for_test(1);
    assert_eq!(
        send(&cell, 1, 99).await,
        refused("rate_limited", "1"),
        "past it"
    );
    admission.add_inflight_for_test(-CAP - CAP / 4);
    assert_eq!(send(&cell, 0, FLOOD + ROUNDS).await, SENT, "under the cap");
    assert_eq!(
        (queued(&cell, 0).await, queued(&cell, 1).await),
        (
            u64::try_from(FLOOD + 1).unwrap(),
            u64::try_from(ROUNDS + 1).unwrap()
        )
    );
    engine_shutdown(&cell.state).await;
}

/// The project memory line the memory leg sets (256 KiB).
const PROJECT_LINE: u64 = 256 * 1024;

/// A queue write of a project over its memory line is refused `429
/// project_memory_pressure`, Retry-After 1, as an append is: a send, a
/// claim and a settle, while its stats are read; below the release point
/// it writes again. And a send's body is charged to the project while it
/// is buffered: with 200 KiB held, a 60 KiB message would take it past the
/// line and is refused, the same message with nothing held is sent.
/// Before: queue writes passed the latch and buffered their bodies
/// uncharged, so both were sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_write_is_held_to_its_projects_memory_line() {
    let cell = open_cell(CellSpec::open(1)).await;
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PROJECT_LINE);
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/jobs", "").await.0, 201);
    assert_eq!(send(&cell, 0, 0).await, SENT, "under the line");
    let pid = crate::tenant::ProjectId::new(&project(0)).unwrap();
    let adm = cell.state.quotas.pressure_handle(&pid).unwrap();
    adm.retained_sse_add(300 * 1024);
    let writes = [
        ("POST", "/v1/queues/jobs/messages", r#"{"body":1}"#),
        ("POST", "/v1/queues/jobs/groups/default:claim", "{}"),
        (
            "POST",
            "/v1/queues/jobs/groups/default:settle",
            r#"{"ops":[]}"#,
        ),
    ];
    for (method, path, body) in writes {
        let answer = ask(&cell, 0, method, path, body).await;
        assert_eq!(answer, refused("project_memory_pressure", "1"), "{path}");
    }
    assert_eq!(queued(&cell, 0).await, 1, "its reads are served");
    adm.retained_sse_sub(300 * 1024);
    assert_eq!(send(&cell, 0, 1).await, SENT, "below the release point");
    let large = format!("{{\"body\":\"{}\"}}", "x".repeat(60 * 1024));
    adm.retained_sse_add(200 * 1024);
    let held = ask(&cell, 0, "POST", "/v1/queues/jobs/messages", &large).await;
    assert_eq!(held, refused("project_memory_pressure", "1"), "the body");
    adm.retained_sse_sub(200 * 1024);
    let alone = ask(&cell, 0, "POST", "/v1/queues/jobs/messages", &large).await;
    assert_eq!(alone, SENT, "a body alone is buffered whole");
    assert_eq!(
        queued(&cell, 0).await,
        3,
        "every refused write stored nothing"
    );
    engine_shutdown(&cell.state).await;
}

/// Over the RSS shed line (a dedicated cell, so nobody is spared) a queue
/// send and a settle are refused `429 rate_limited`, Retry-After 2, as an
/// append is, before the body is read: a head declaring 4 MiB with no body
/// sent reads the refusal. A claim and the queue's stats are served, and
/// under the line the send is served. Before: queue writes never met the
/// RSS shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_queue_sends_and_settles_are_shed_before_their_bodies() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cell = open_cell(CellSpec::open(1)).await;
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/jobs", "").await.0, 201);
    assert_eq!(send(&cell, 0, 0).await, SENT, "under the line");
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(500);
    admission.record_rss_mb(501);
    let batch = r#"{"messages":[{"body":1}]}"#;
    let shed = [
        ("/v1/queues/jobs/messages", r#"{"body":1}"#),
        ("/v1/queues/jobs/messages:batch", batch),
        ("/v1/queues/jobs/groups/default:settle", r#"{"ops":[]}"#),
    ];
    for (path, body) in shed {
        let answer = ask(&cell, 0, "POST", path, body).await;
        assert_eq!(answer, refused("rate_limited", "2"), "{path}");
    }
    let head = format!(
        "POST /v1/queues/jobs/messages HTTP/1.1\r\nhost: rig\r\nauthorization: {}\r\nprisma-encryption-key: {}\r\ncontent-length: {}\r\n\r\n",
        cell.bearers[0],
        super::fixture_requests::PRISMA_KEY,
        4 << 20
    );
    let mut silent = tokio::net::TcpStream::connect(cell.addr).await.unwrap();
    silent.write_all(head.as_bytes()).await.unwrap();
    let (mut text, mut chunk) = (Vec::new(), vec![0_u8; 4096]);
    while !text.ends_with(b"}") {
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), silent.read(&mut chunk));
        let n = read.await.unwrap().unwrap();
        assert!(n > 0, "closed after {}", String::from_utf8_lossy(&text));
        text.extend_from_slice(&chunk[..n]);
    }
    let text = String::from_utf8_lossy(&text).to_lowercase();
    assert!(
        text.starts_with("http/1.1 429") && text.contains("rate_limited"),
        "a body never sent is not waited for: {text}"
    );
    let claim = ask(
        &cell,
        0,
        "POST",
        "/v1/queues/jobs/groups/default:claim",
        "{}",
    )
    .await;
    assert_eq!(claim.0, 200, "a claim is served over the line");
    assert_eq!(queued(&cell, 0).await, 1, "and the stats");
    admission.record_rss_mb(0);
    assert_eq!(send(&cell, 0, 1).await, SENT, "under the line");
    assert_eq!(queued(&cell, 0).await, 2, "every shed write stored nothing");
    engine_shutdown(&cell.state).await;
}

/// At the in-flight write cap (a dedicated cell, so nobody is spared) a
/// queue send and a settle are refused `429 rate_limited`, Retry-After 1,
/// as an append is, while a claim and the queue's stats are served; under
/// the cap the send is served. Before: queue writes never met the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_the_inflight_cap_queue_sends_and_settles_are_refused_as_appends_are() {
    let cell = open_cell(CellSpec::open(1)).await;
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/jobs", "").await.0, 201);
    assert_eq!(send(&cell, 0, 0).await, SENT, "under the cap");
    let admission = &cell.state.admission;
    admission.set_max_inflight(CAP);
    admission.add_inflight_for_test(CAP);
    let writes = [
        ("/v1/queues/jobs/messages", r#"{"body":1}"#),
        (
            "/v1/queues/jobs/messages:batch",
            r#"{"messages":[{"body":1}]}"#,
        ),
        ("/v1/queues/jobs/groups/default:settle", r#"{"ops":[]}"#),
    ];
    for (path, body) in writes {
        let answer = ask(&cell, 0, "POST", path, body).await;
        assert_eq!(answer, refused("rate_limited", "1"), "{path}");
    }
    let shed = admission.snapshot().shed;
    assert_eq!((shed.inflight, shed.total), (3, 3), "counted as the gate's");
    let claim = ask(
        &cell,
        0,
        "POST",
        "/v1/queues/jobs/groups/default:claim",
        "{}",
    )
    .await;
    assert_eq!(claim.0, 200, "a claim is served at the cap");
    assert_eq!(queued(&cell, 0).await, 1, "and the stats");
    admission.add_inflight_for_test(-CAP);
    assert_eq!(send(&cell, 0, 1).await, SENT, "under the cap");
    assert_eq!(
        queued(&cell, 0).await,
        2,
        "every refused write stored nothing"
    );
    engine_shutdown(&cell.state).await;
}
