//! Native queues under the cell's overload gates (layout 6A package 1,
//! DESIGN-6A §4.1, docs/QUEUES-DESIGN.md §6.1): on a cell that streams and
//! queues share, a queue request meets the gates an append meets. Every
//! assertion reads the wire answers and the queue's stored state (its
//! `queuedMessages`, counted by the committer).

use super::fixture_cell::{Cell, CellSpec, error_code, journal, open_cell, project};
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
    // The latch holds until the pressure is below its release point (75%
    // of the line): at 220 KiB a send would fit under the line, so only
    // the latch refuses it, and a claim with no body, which charges
    // nothing, is the latch's alone too.
    adm.retained_sse_sub(80 * 1024);
    let latched = [
        ("/v1/queues/jobs/messages", r#"{"body":1}"#),
        ("/v1/queues/jobs/groups/default:claim", ""),
    ];
    for (path, body) in latched {
        let answer = ask(&cell, 0, "POST", path, body).await;
        assert_eq!(
            answer,
            refused("project_memory_pressure", "1"),
            "latched: {path}"
        );
    }
    adm.retained_sse_sub(220 * 1024);
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
    let journaled: Vec<_> = journal(&cell.state)
        .await
        .into_iter()
        .filter(|e| e["status"] == 429)
        .map(|e| {
            let field = |name: &str| e[name].as_str().unwrap_or_default().to_string();
            (
                field("code"),
                field("project_id"),
                field("route"),
                field("method"),
            )
        })
        .collect();
    let denial = |route: &str| {
        let code = "project_memory_pressure".to_string();
        (code, project(0), route.to_string(), "POST".to_string())
    };
    assert_eq!(
        journaled,
        [
            "jobs/messages",
            "jobs/groups/default:claim",
            "jobs/groups/default:settle",
            "jobs/messages",
            "jobs/groups/default:claim",
            "jobs/messages",
        ]
        .map(denial),
        "each refusal journaled to its project, in order"
    );
    engine_shutdown(&cell.state).await;
}

/// Over the RSS shed line (a dedicated cell, so nobody is spared) a queue
/// send and a batch are refused `429 rate_limited`, Retry-After 2, as an
/// append is, before the body is read: a head declaring 4 MiB with no body
/// sent reads the refusal. A claim and the queue's stats are served, and
/// under the line the send is served. Before: queue writes never met the
/// RSS shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_queue_sends_are_shed_before_their_bodies() {
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

/// The committer queue the full-committer leg gives the cell's shard.
const COMMIT_QUEUE: usize = 2;

/// A queue operation on a shard whose committer queue is full is answered
/// at once, `503 shard_busy`, Retry-After 1, as an append meets a full
/// queue at once, instead of waiting for room while holding its request:
/// with the committer held after taking one of three reads of the queue's
/// state, submitted to the engine directly so they fill its queue whatever
/// the runner's pace, a send answers within 5 s (it would wait without
/// bound before); once the committer is released the reads are answered
/// and the refused send stored nothing. Before: the send waited for room
/// until the committer moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_operation_on_a_full_committer_is_answered_at_once() {
    let mut spec = CellSpec::open(1);
    spec.shard.queue_reqs = COMMIT_QUEUE;
    let cell = open_cell(spec).await;
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/jobs", "").await.0, 201);
    let sref = crate::tenant::ProjectId::new(&project(0))
        .unwrap()
        .stream_ref("jobs");
    let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let engine = cell
        .state
        .shards
        .resolve(&seg.shard_route, crate::shard_directory::Adoption::Internal)
        .await
        .unwrap();
    let held = engine.test_hold_commit().await;
    let reads = (0..=COMMIT_QUEUE)
        .map(|_| engine.submit_native_queue(seg.identity, crate::native_queue::NqOp::Get));
    let pending = futures_util::future::join_all(reads);
    let probe = async {
        tokio::time::timeout(std::time::Duration::from_secs(30), full(&engine))
            .await
            .expect("the three reads fill the committer's queue");
        let busy = send(&cell, 0, 99);
        let busy = tokio::time::timeout(std::time::Duration::from_secs(5), busy).await;
        drop(held);
        busy.map_err(|_| "no answer within 5 s")
    };
    let (read, busy) = futures_util::future::join(pending, probe).await;
    assert_eq!(
        busy,
        Ok((503, Some("shard_busy".to_string()), Some("1".to_string())))
    );
    let answered = read
        .iter()
        .filter(|r| matches!(r, Ok(crate::native_queue::NqOut::Stats(_))))
        .count();
    assert_eq!(answered, COMMIT_QUEUE + 1, "the queued reads are answered");
    assert_eq!(queued(&cell, 0).await, 0, "the refused send stored nothing");
    assert_eq!(send(&cell, 0, 1).await, SENT, "with room, a send is served");
    engine_shutdown(&cell.state).await;
}

/// Until `engine`'s committer queue has no free place.
async fn full(engine: &crate::shard::ShardEngine) {
    while engine.committer_slots_free() > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Until `admission` counts `n` parked waits, at most 10 s.
async fn parked(admission: &crate::admission::AdmissionController, n: i64) -> bool {
    let wait = async {
        while admission.parked() != n {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), wait)
        .await
        .is_ok()
}

/// A claim waiting for work parks in the live pool, as a pull's wait does
/// (its queue named for this test alone, so no other cell's send wakes it):
/// it leaves the active requests, so at the in-flight cap (8, seven held)
/// a send is still admitted beside it, and the send wakes it with the
/// message. Before: the claim waited as an active request, so the send
/// was the ninth active and refused `429 rate_limited`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_claim_parks_and_leaves_the_write_gate_to_sends() {
    let cell = open_cell(CellSpec::open(1)).await;
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/parking", "").await.0, 201);
    let admission = &cell.state.admission;
    admission.set_max_inflight(CAP);
    admission.add_inflight_for_test(CAP - 1);
    let claim = r#"{"max":1,"waitMs":20000}"#;
    let waiting = cell.call(
        0,
        "POST",
        "/v1/queues/parking/groups/default:claim",
        claim.as_bytes(),
    );
    let probe = async {
        assert!(parked(admission, 1).await, "the waiting claim parks");
        let sent = ask(
            &cell,
            0,
            "POST",
            "/v1/queues/parking/messages",
            r#"{"body":{"n":7}}"#,
        )
        .await;
        assert!(
            parked(admission, 0).await,
            "the woken claim is active again"
        );
        sent
    };
    let ((status, _, body), sent) = futures_util::future::join(waiting, probe).await;
    assert_eq!(sent, SENT, "the send beside the parked claim");
    let claimed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (status, &claimed["messages"][0]["body"]["n"]),
        (200, &serde_json::json!(7)),
        "the claim takes the message that woke it"
    );
    admission.add_inflight_for_test(1 - CAP);
    engine_shutdown(&cell.state).await;
}

/// The instance read memory the claim-memory leg gives the cell.
const READ_MEMORY: u64 = 64 * 1024;

/// A claim reserves read memory for exactly the messages it returns, after
/// it leases them and before it reads their pages, as a page read
/// reserves before it materializes: with 60 KiB of the cell's 64 KiB read
/// memory held elsewhere, a claim that leases a 10 KiB message waits for
/// room and is refused `503 read_memory_busy`, Retry-After 1, its lease
/// given back with its attempt (due again a second later, the next claim
/// takes it as attempt 1); with room
/// for the message but not for the claim's 1 MiB budget it is served, so
/// it reserves what it returns, never its budget; and a claim waiting for
/// work holds nothing. Before: claims reserved nothing, so the first claim
/// was served over the full read memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_reserves_read_memory_only_for_what_it_returns() {
    use crate::admission::read_memory::ReadHold;
    let cell = open_cell(CellSpec::open(1)).await;
    let admission = &cell.state.admission;
    admission.set_read_memory_capacity(READ_MEMORY);
    assert_eq!(
        ask(&cell, 0, "PUT", "/v1/queues/reserving", "").await.0,
        201
    );
    let message = format!("{{\"body\":\"{}\"}}", "x".repeat(10 * 1024));
    let sent = ask(&cell, 0, "POST", "/v1/queues/reserving/messages", &message).await;
    assert_eq!(sent, SENT);
    let other = ReadHold::reserve(admission, None, 0, READ_MEMORY - 4 * 1024).await;
    let other = other.expect("the other read fits");
    let claim = r#"{"max":1,"leaseMs":60000}"#;
    let path = "/v1/queues/reserving/groups/default:claim";
    let (status, headers, body) = cell.call(0, "POST", path, claim.as_bytes()).await;
    assert_eq!(
        (
            status,
            error_code(&body),
            headers.get("retry-after").map(String::as_str)
        ),
        (503, Some("read_memory_busy".into()), Some("1")),
        "no room for the leased message"
    );
    drop(other);
    let waiting = r#"{"max":1,"waitMs":200}"#;
    let held = ReadHold::reserve(admission, None, 0, READ_MEMORY - 20 * 1024).await;
    let held = held.expect("room for the message, not for a 1 MiB budget");
    let after = r#"{"max":1,"leaseMs":60000,"waitMs":3000}"#;
    let (status, _, body) = cell.call(0, "POST", path, after.as_bytes()).await;
    let claimed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (status, &claimed["messages"][0]["attempt"]),
        (200, &serde_json::json!(1)),
        "served, the refused claim's attempt given back"
    );
    drop(held);
    let empty = cell.call(0, "POST", path, waiting.as_bytes());
    let probe = async {
        assert!(parked(admission, 1).await, "the empty claim waits parked");
        admission.read_memory().0
    };
    let ((status, _, _), during) = futures_util::future::join(empty, probe).await;
    assert_eq!((status, during), (200, 0), "a waiting claim holds nothing");
    assert_eq!(admission.read_memory().0, 0, "every hold released");
    engine_shutdown(&cell.state).await;
}

/// A claim of one message from project 0's queue `jobs` for a minute: its
/// lease token.
async fn lease_one(cell: &Cell) -> String {
    let path = "/v1/queues/jobs/groups/default:claim";
    let claim = r#"{"max":1,"leaseMs":60000}"#;
    let (status, _, b) = cell.call(0, "POST", path, claim.as_bytes()).await;
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(status, 200, "{v}");
    v["messages"][0]["token"].as_str().unwrap().to_string()
}

/// One settle of `token` with `op`: its status, code and Retry-After, and
/// the op's outcome when it was served.
async fn settle_one(cell: &Cell, token: &str, op: &str) -> (Answer, Option<String>) {
    let body = format!(r#"{{"ops":[{{"token":"{token}","op":"{op}"}}]}}"#);
    let path = "/v1/queues/jobs/groups/default:settle";
    let (status, headers, b) = cell.call(0, "POST", path, body.as_bytes()).await;
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap_or_default();
    let answer = (status, error_code(&b), headers.get("retry-after").cloned());
    (
        answer,
        v["results"][0]["outcome"].as_str().map(str::to_string),
    )
}

/// Over the RSS shed line a settle whose every operation ends its lease (an
/// ack, a release, a retry, a fail or a discard: no message bytes) is
/// served until the instance is an eighth over the line, the lead's ruling
/// on #191 under the owner's delegation of 2026-10-27, because refusing it
/// turns every lease into a redelivery that feeds the overload; a settle
/// that extends a lease is refused `429 rate_limited` as an append is, and
/// past the hard line every settle is. Under the line the last lease is
/// acked. Before: every settle was refused over the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_a_settle_that_ends_leases_passes_until_the_hard_line() {
    let cell = open_cell(CellSpec::open(1)).await;
    assert_eq!(ask(&cell, 0, "PUT", "/v1/queues/jobs", "").await.0, 201);
    for n in 0..3 {
        assert_eq!(send(&cell, 0, n).await, SENT);
    }
    let tokens = [
        lease_one(&cell).await,
        lease_one(&cell).await,
        lease_one(&cell).await,
    ];
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(500);
    admission.record_rss_mb(501);
    let served = |outcome: &str| ((200, None, None), Some(outcome.to_string()));
    assert_eq!(
        settle_one(&cell, &tokens[0], "ack").await,
        served("ok"),
        "an ack"
    );
    assert_eq!(
        settle_one(&cell, &tokens[1], "release").await,
        served("ok"),
        "a release"
    );
    let shed = (refused("rate_limited", "2"), None);
    assert_eq!(
        settle_one(&cell, &tokens[2], "extend").await,
        shed,
        "an extend"
    );
    admission.record_rss_mb(500 * 9 / 8 + 1);
    assert_eq!(
        settle_one(&cell, &tokens[2], "ack").await,
        shed,
        "past the hard line"
    );
    admission.record_rss_mb(0);
    assert_eq!(
        settle_one(&cell, &tokens[2], "ack").await,
        served("ok"),
        "under the line"
    );
    let stats = cell.json(0, "GET", "/v1/queues/jobs").await;
    let s = &stats["stats"];
    assert_eq!(
        (&s["counters"]["acked"], &s["queuedMessages"]),
        (&serde_json::json!(2), &serde_json::json!(1)),
        "two acked, the released message queued again"
    );
    engine_shutdown(&cell.state).await;
}
