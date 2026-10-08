//! Read memory (shared-cells PLAN step 6, finding H3). A page read reserves
//! the bytes its page may hold before it runs and its response body holds
//! the page's exact bytes until the body ends, in its project's read bytes
//! (counted in its memory pressure, bounded by its memory line) and in the
//! instance's read memory (waited for, then refused). A read waiting for
//! data holds nothing, and a parked wait weighs in its project's pressure.
//!
//! Every client here that "never reads" sends its request over a socket
//! with a 4 KiB receive buffer and leaves the response unread, so the
//! server cannot finish writing a page of megabytes: its body is held
//! until the client leaves.

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::failpoints::{Fp, parked};
use crate::tenant::ProjectId;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const MIB: u64 = 1 << 20;
/// The default page budget a read without `maxBytes` reserves.
const PAGE_BUDGET: u64 = 8 * MIB;
const RECORDS: &str = "/v1/streams/big/records";
/// Room under a line for the small bodies of pulls, which are buffered
/// under it as a write's are (`admission::body`).
const BODY_ROOM: u64 = 64 << 10;

/// Record `n`: a JSON object of 64 KiB and a few bytes.
fn padded(n: usize) -> String {
    format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(64 * 1024))
}

/// Project `i`'s JSON stream `big`: 48 records of 64 KiB appended in one
/// batch. Returns the length of the page a whole read of it renders.
async fn big_stream(cell: &Cell, i: usize) -> u64 {
    let created = cell
        .call(
            i,
            "PUT",
            "/v1/streams/big",
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
    assert_eq!(created.0, 201);
    let records: Vec<String> = (0..48).map(padded).collect();
    let batch = format!("[{}]", records.join(","));
    let appended = cell
        .call(i, "POST", "/v1/streams/big/records:batch", batch.as_bytes())
        .await;
    assert_eq!(appended.0, 200, "{}", String::from_utf8_lossy(&appended.2));
    let (status, _, page) = cell.call(i, "GET", RECORDS, b"").await;
    assert_eq!(status, 200);
    let page: serde_json::Value = serde_json::from_slice(&page).unwrap();
    assert_eq!(
        page.as_array().map(Vec::len),
        Some(48),
        "one page holds all"
    );
    u64::try_from(page.to_string().len()).unwrap()
}

/// Project `i`'s JSON stream `q` with consumer `g`: 48 records of 64 KiB,
/// each under its own routing key, so one pull leases all of them. Returns
/// the length of the batch a pull of all 48 renders (taken by a pull whose
/// leases a second consumer's are not).
async fn queue_of_keys(cell: &Cell, i: usize) -> u64 {
    let created = cell
        .call(i, "PUT", "/v1/streams/q", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    for n in 0..48 {
        let record = padded(n);
        let key = format!("k{n}");
        let headers = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", cell.bearers[i].as_str()),
            ("prisma-routing-key", key.as_str()),
        ];
        let (status, _, _) = preq(
            cell.addr,
            "POST",
            "/v1/streams/q/records",
            &headers,
            record.as_bytes(),
        )
        .await;
        assert_eq!(status, 200);
    }
    for consumer in ["probe", "g"] {
        let path = format!("/v1/streams/q/consumers/{consumer}");
        let created = cell
            .call(i, "PUT", &path, br#"{"maxBatchRecords":48}"#)
            .await;
        assert_eq!(created.0, 201, "{}", String::from_utf8_lossy(&created.2));
    }
    let (status, _, pulled) = cell
        .call(
            i,
            "POST",
            "/v1/streams/q/consumers/probe:pull",
            br#"{"max":48}"#,
        )
        .await;
    let messages: serde_json::Value = serde_json::from_slice(&pulled).unwrap();
    assert_eq!(
        (status, messages["messages"].as_array().map(Vec::len)),
        (200, Some(48))
    );
    u64::try_from(pulled.len()).unwrap()
}

/// Project `i`'s JSON stream `name`: 48 records of 64 KiB, each under its
/// own routing key, and consumers `g0`.. that each lease all 48 in a pull.
async fn keyed_queue(cell: &Cell, i: usize, name: &str, consumers: usize) {
    let path = format!("/v1/streams/{name}");
    let created = cell
        .call(i, "PUT", &path, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let records = format!("{path}/records");
    for n in 0..48 {
        let record = padded(n);
        let key = format!("k{n}");
        let headers = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", cell.bearers[i].as_str()),
            ("prisma-routing-key", key.as_str()),
        ];
        let (status, _, _) = preq(cell.addr, "POST", &records, &headers, record.as_bytes()).await;
        assert_eq!(status, 200);
    }
    for g in 0..consumers {
        let consumer = format!("{path}/consumers/g{g}");
        let created = cell
            .call(i, "PUT", &consumer, br#"{"maxBatchRecords":48}"#)
            .await;
        assert_eq!(created.0, 201, "{}", String::from_utf8_lossy(&created.2));
    }
}

/// Send `method path body` as project `i` and never read the response.
async fn never_read(cell: &Cell, i: usize, method: &str, path: &str, body: &[u8]) -> TcpStream {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stream = socket.connect(cell.addr).await.unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: rig\r\nauthorization: {}\r\nprisma-encryption-key: {PRISMA_KEY}\r\ncontent-length: {}\r\n\r\n",
        cell.bearers[i],
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream
}

/// (project `i`'s read bytes, the instance's read memory).
fn ledgers(cell: &Cell, i: usize) -> (u64, u64) {
    let id = ProjectId::new(&project(i)).unwrap();
    let held = cell.state.quotas.read_bytes(&id).map_or(0, |b| b.held());
    (held, cell.state.admission.read_memory().0)
}

/// The ledgers once they read `want`, or after 10 s.
async fn settle_at(cell: &Cell, i: usize, want: (u64, u64)) -> (u64, u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while ledgers(cell, i) != want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ledgers(cell, i)
}

/// Project `i` at rest: (its whole estimated pressure, the instance's read
/// memory) once both are 0, or after 10 s. A leg waits for this before it
/// lowers the project's line or pulls under it: the frames its appends
/// committed weigh in the estimate, 64 KiB more for each stream holding
/// them, until the absorber takes them, and a write's latch and a body
/// buffered under the line (a pull's beside its coverage reservation) see
/// them, as they see a page still held.
async fn at_baseline(cell: &Cell, i: usize) -> (u64, u64) {
    let id = ProjectId::new(&project(i)).unwrap();
    let now = || {
        let estimate = cell.state.quotas.pressure_handle(&id);
        (
            estimate.map_or(0, |e| e.estimated_pressure_bytes()),
            cell.state.admission.read_memory().0,
        )
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while now() != (0, 0) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    now()
}

/// Project `i`'s memory pressure, for a failure message: its estimate,
/// its `/v1/debug/load` row (each dimension but its read bytes and parked
/// waits, and its latch's engage and shed counts; no row while the
/// estimate is 0 and the latch is clear), its read bytes and the
/// instance's parked waits. A 429 `project_memory_pressure` names its
/// cause here: the latch counts an engage, a read reservation refused at
/// the line counts a shed, a body refused while it is buffered counts
/// neither.
fn pressure(cell: &Cell, i: usize) -> String {
    let id = ProjectId::new(&project(i)).unwrap();
    let line = cell.state.admission.project_memory_pressure_bytes();
    let load = cell.state.quotas.memory_pressure_json(line, usize::MAX);
    let row = load["rows"].as_array().and_then(|rows| {
        rows.iter()
            .find(|row| row["project"].as_str() == Some(id.as_str()))
    });
    let estimate = cell.state.quotas.pressure_handle(&id);
    format!(
        "estimate {:?} B under line {line} B, read bytes {}, parked waits {}, row {}",
        estimate.map(|e| e.estimated_pressure_bytes()),
        ledgers(cell, i).0,
        cell.state.admission.parked(),
        row.map_or_else(|| "none".to_string(), ToString::to_string)
    )
}

/// A page's body holds the page's exact bytes in its project's read bytes
/// and in the instance's read memory while its client has not read it,
/// and releases them when the client leaves; a scan's page and a consumer
/// pull's batch are held the same way, and a client that reads its page
/// leaves nothing held. Before: nothing was held once the handler returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_is_held_until_its_client_reads_it_or_leaves() {
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    let first = never_read(&cell, 0, "GET", RECORDS, b"").await;
    assert_eq!(settle_at(&cell, 0, (page, page)).await, (page, page), "one");
    let second = never_read(&cell, 0, "GET", RECORDS, b"").await;
    let two = (2 * page, 2 * page);
    assert_eq!(settle_at(&cell, 0, two).await, two, "two");
    drop(first);
    assert_eq!(
        settle_at(&cell, 0, (page, page)).await,
        (page, page),
        "one left"
    );
    drop(second);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0), "both left");

    let (status, _, scanned) = cell.call(0, "GET", "/v1/streams/big:scan", b"").await;
    assert_eq!(status, 200);
    let scan = u64::try_from(scanned.len()).unwrap();
    let scanning = never_read(&cell, 0, "GET", "/v1/streams/big:scan", b"").await;
    assert_eq!(
        settle_at(&cell, 0, (scan, scan)).await,
        (scan, scan),
        "scan"
    );
    drop(scanning);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0), "scan left");

    let batch = queue_of_keys(&cell, 0).await;
    let pull = br#"{"max":48}"#;
    let pulling = never_read(&cell, 0, "POST", "/v1/streams/q/consumers/g:pull", pull).await;
    assert_eq!(
        settle_at(&cell, 0, (batch, batch)).await,
        (batch, batch),
        "pull"
    );
    drop(pulling);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0), "pull left");

    let (status, _, read) = cell.call(0, "GET", RECORDS, b"").await;
    assert_eq!((status, u64::try_from(read.len()).unwrap()), (200, page));
    assert_eq!(
        settle_at(&cell, 0, (0, 0)).await,
        (0, 0),
        "a page read whole"
    );
    engine_shutdown(&cell.state).await;
}

/// A project whose bodies hold its memory line waits for its own memory
/// and is refused its next read after 2 s (429 `project_memory_pressure`,
/// retryable), while its neighbour reads at once; a read waiting when one
/// of its unread pages is released is admitted then. With the line at two
/// pages plus one page budget, a project's third unread page still fits
/// and its fourth read does not. Before: every read was admitted at once,
/// whatever its project's bodies held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_at_its_memory_line_waits_and_is_refused_and_its_neighbour_is_not() {
    let cell = open_cell(CellSpec::open(2)).await;
    let page = big_stream(&cell, 0).await;
    assert_eq!(big_stream(&cell, 1).await, page);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(2 * page + PAGE_BUDGET);
    let mut unread = Vec::new();
    for held in 1..=3 {
        unread.push(never_read(&cell, 0, "GET", RECORDS, b"").await);
        let want = (held * page, held * page);
        assert_eq!(settle_at(&cell, 0, want).await, want, "{held} unread");
    }
    let started = Instant::now();
    let (status, headers, body) = cell.call(0, "GET", RECORDS, b"").await;
    let waited = started.elapsed();
    let refusal: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (
            status,
            refusal["error"]["code"].as_str(),
            headers.get("retry-after").map(String::as_str)
        ),
        (429, Some("project_memory_pressure"), Some("1")),
        "the fourth read"
    );
    assert!(waited >= Duration::from_secs(2), "refused after {waited:?}");
    let started = Instant::now();
    let (status, _, read) = cell.call(1, "GET", RECORDS, b"").await;
    assert_eq!(
        (status, u64::try_from(read.len()).unwrap()),
        (200, page),
        "the neighbour"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the neighbour waits for nothing"
    );
    assert_eq!(ledgers(&cell, 0), (3 * page, 3 * page), "nothing more held");

    let started = Instant::now();
    let waiting = cell.call(0, "GET", RECORDS, b"");
    let leave = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        unread.pop();
    };
    let ((status, _, read), ()) = futures_util::future::join(waiting, leave).await;
    let waited = started.elapsed();
    assert_eq!((status, u64::try_from(read.len()).unwrap()), (200, page));
    assert!(
        waited >= Duration::from_millis(500) && waited < Duration::from_secs(2),
        "admitted after {waited:?}"
    );
    unread.clear();
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// The instance's read memory is waited for, then refused: with room for
/// one page budget beside one unread page, a read beside two unread pages
/// waits 2 s and is refused (503 `read_memory_busy`, retryable), a read
/// asking for 4 KiB fits beside them at once, and a read that is waiting
/// when a page is released is admitted then. Before: every read was
/// admitted at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_waits_for_the_instance_read_memory_and_is_refused_after_the_wait() {
    let cell = open_cell(CellSpec::open(2)).await;
    let page = big_stream(&cell, 0).await;
    big_stream(&cell, 1).await;
    cell.state
        .admission
        .set_read_memory_capacity(page + PAGE_BUDGET);
    let first = never_read(&cell, 0, "GET", RECORDS, b"").await;
    assert_eq!(settle_at(&cell, 0, (page, page)).await, (page, page));
    let second = never_read(&cell, 0, "GET", RECORDS, b"").await;
    let two = (2 * page, 2 * page);
    assert_eq!(settle_at(&cell, 0, two).await, two);

    let started = Instant::now();
    let (status, headers, body) = cell.call(1, "GET", RECORDS, b"").await;
    let waited = started.elapsed();
    let refusal: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (
            status,
            refusal["error"]["code"].as_str(),
            headers.get("retry-after").map(String::as_str)
        ),
        (503, Some("read_memory_busy"), Some("1")),
    );
    assert!(waited >= Duration::from_secs(2), "refused after {waited:?}");

    let small = "/v1/streams/big/records?maxBytes=4096";
    let (status, _, _) = cell.call(1, "GET", small, b"").await;
    assert_eq!(status, 200, "a 4 KiB page fits");

    let started = Instant::now();
    let waiting = cell.call(1, "GET", RECORDS, b"");
    let release = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(second);
    };
    let ((status, _, read), ()) = futures_util::future::join(waiting, release).await;
    let waited = started.elapsed();
    assert_eq!((status, u64::try_from(read.len()).unwrap()), (200, page));
    assert!(
        waited >= Duration::from_millis(500) && waited < Duration::from_secs(2),
        "admitted after {waited:?}"
    );
    drop(first);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// Long-polls waiting for data hold no read memory: with room for one
/// page budget, four waiting long-polls leave both ledgers at zero and a
/// read beside them is admitted at once; the append that wakes them is
/// served to each and released when each body ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_long_polls_hold_no_read_memory() {
    let cell = open_cell(CellSpec::open(1)).await;
    let created = cell
        .call(0, "PUT", "/v1/streams/lp", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    cell.state.admission.set_read_memory_capacity(PAGE_BUDGET);
    let poll = "/v1/streams/lp/records:long-poll?cursor=now&waitMs=5000";
    let polls = (0..4).map(|_| cell.call(0, "GET", poll, b""));
    let observe = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cell.state.admission.parked() < 4 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let parked = cell.state.admission.parked();
        let held = ledgers(&cell, 0);
        let started = Instant::now();
        let (status, _, _) = cell.call(0, "GET", "/v1/streams/lp/records", b"").await;
        let read = (status, started.elapsed() < Duration::from_secs(1));
        let appended = cell
            .call(0, "POST", "/v1/streams/lp/records", br#"{"n":1}"#)
            .await;
        (parked, held, read, appended.0)
    };
    let (answers, observed) =
        futures_util::future::join(futures_util::future::join_all(polls), observe).await;
    assert_eq!(observed, (4, (0, 0), (200, true), 200));
    for (status, _, body) in answers {
        assert_eq!((status, body.as_slice()), (200, br#"[{"n":1}]"#.as_slice()));
    }
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// A parked wait weighs 48 KiB in its project's memory pressure (model
/// version 2): three parked long-polls on an idle stream are exactly
/// 3 x 48 KiB, and nothing once they end. Before: parked waits weighed
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_waits_weigh_in_their_projects_pressure() {
    let cell = open_cell(CellSpec::open(1)).await;
    let created = cell
        .call(0, "PUT", "/v1/streams/lp", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let entry = cell
        .state
        .quotas
        .pressure_handle(&ProjectId::new(&project(0)).unwrap())
        .unwrap();
    assert_eq!(entry.estimated_pressure_bytes(), 0);
    let poll = "/v1/streams/lp/records:long-poll?cursor=now&waitMs=1500";
    let polls = (0..3).map(|_| cell.call(0, "GET", poll, b""));
    let observe = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cell.state.admission.parked() < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        entry.estimated_pressure_bytes()
    };
    let (answers, parked) =
        futures_util::future::join(futures_util::future::join_all(polls), observe).await;
    assert_eq!(parked, 3 * 48 * 1024);
    assert!(answers.iter().all(|(status, _, _)| *status == 204));
    assert_eq!(entry.estimated_pressure_bytes(), 0);
    assert_eq!(crate::quota::pressure_model_json()["version"], 2);
    engine_shutdown(&cell.state).await;
}

/// Project `i`'s default read of its stream `big`: (status, error code,
/// answered within 1 s).
async fn prompt_read(cell: &Cell, i: usize) -> (u16, Option<String>, bool) {
    let started = Instant::now();
    let (status, _, body) = cell.call(i, "GET", RECORDS, b"").await;
    let code = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
    (status, code, started.elapsed() < Duration::from_secs(1))
}

/// The ledgers once they have not moved for 300 ms, or after 10 s.
async fn at_rest(cell: &Cell, i: usize) -> (u64, u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let (mut last, mut since) = (ledgers(cell, i), Instant::now());
    while Instant::now() < deadline && since.elapsed() < Duration::from_millis(300) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = ledgers(cell, i);
        if now != last {
            (last, since) = (now, Instant::now());
        }
    }
    last
}

/// `polls` long-polls of project 0 on its empty stream `lp`, each from a
/// client that never reads, parked holding nothing.
async fn parked_polls(cell: &Cell, polls: usize, query: &str) -> Vec<TcpStream> {
    let created = cell
        .call(0, "PUT", "/v1/streams/lp", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let poll = format!("/v1/streams/lp/records:long-poll?cursor=now&{query}");
    let mut sockets = Vec::new();
    for _ in 0..polls {
        sockets.push(never_read(cell, 0, "GET", &poll, b"").await);
    }
    let parked = i64::try_from(polls).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while cell.state.admission.parked() < parked && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        (cell.state.admission.parked(), ledgers(cell, 0)),
        (parked, (0, 0)),
        "parked, holding nothing while they wait"
    );
    sockets
}

/// `n` records of 64 KiB appended to project 0's stream `lp` in one batch.
async fn append_to_lp(cell: &Cell, n: usize) {
    let records: Vec<String> = (0..n).map(padded).collect();
    let batch = format!("[{}]", records.join(","));
    let path = "/v1/streams/lp/records:batch";
    let appended = cell.call(0, "POST", path, batch.as_bytes()).await;
    assert_eq!(appended.0, 200, "{}", String::from_utf8_lossy(&appended.2));
}

/// A woken long-poll takes its read reservation again before it renders
/// (isolation review F1): twelve of project 0's long-polls of 1 MiB each,
/// woken together by one append of 15 x 64 KiB to clients that never read,
/// hold at most project 0's line of one page budget, so the instance's
/// read memory of two page budgets keeps a page budget for project 1,
/// whose read is served at once. Before: each woken page was charged
/// unreserved, project 0 held 11,799,612 B past its line of 8,388,608 B
/// and project 1 was refused 503 `read_memory_busy` after 2 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projects_woken_long_polls_stay_inside_its_line_and_its_neighbour_reads() {
    let cell = open_cell(CellSpec::open(2)).await;
    big_stream(&cell, 1).await;
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    cell.state
        .admission
        .set_read_memory_capacity(2 * PAGE_BUDGET);
    let polls = parked_polls(&cell, 12, "waitMs=20000&maxBytes=1048576").await;
    append_to_lp(&cell, 15).await;
    let (held, instance) = at_rest(&cell, 0).await;
    let neighbour = prompt_read(&cell, 1).await;
    assert_eq!(
        (held <= PAGE_BUDGET, held == instance, neighbour.clone()),
        (true, true, (200, None, true)),
        "project 0 holds {held} B against its line of {PAGE_BUDGET} B (instance {instance} B); \
         its neighbour's read: {neighbour:?}"
    );
    drop(polls);
    engine_shutdown(&cell.state).await;
}

/// The same with long-polls that leave `maxBytes` to its default (capacity
/// review C2): 32 of them, woken by one append of 48 x 64 KiB, take the
/// default page budget again each before reading their 1 MiB tail page, so
/// project 0 holds at most its line and project 1 reads at once beside
/// it. Before: project 0 held 31,465,632 B under a line of 8,388,608 B,
/// past the instance's read memory of 25,165,824 B, and project 1 was
/// refused 503 `read_memory_busy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn woken_default_long_polls_stay_inside_their_projects_line_and_its_neighbour_reads() {
    let cell = open_cell(CellSpec::open(2)).await;
    big_stream(&cell, 1).await;
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    cell.state
        .admission
        .set_read_memory_capacity(3 * PAGE_BUDGET);
    let polls = parked_polls(&cell, 32, "waitMs=20000").await;
    append_to_lp(&cell, 48).await;
    let (held, instance) = at_rest(&cell, 0).await;
    let neighbour = prompt_read(&cell, 1).await;
    assert_eq!(
        (held <= PAGE_BUDGET, held == instance, neighbour.clone()),
        (true, true, (200, None, true)),
        "project 0 holds {held} B against its line of {PAGE_BUDGET} B (instance {instance} B); \
         its neighbour's read: {neighbour:?}"
    );
    drop(polls);
    engine_shutdown(&cell.state).await;
}

/// A woken long-poll whose page finds no room under its project's line
/// before its wait ends answers as a timeout (204) holding its cursor and
/// charges nothing; a read from that cursor serves the record once the
/// line has room. Project 0's line is one page budget and an unread page
/// of `big` holds part of it, so the long-poll's default budget cannot be
/// taken again. Before: the woken page was rendered and served (200) past
/// the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_without_room_ends_its_wait_as_a_timeout_holding_its_cursor() {
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    let created = cell
        .call(0, "PUT", "/v1/streams/lp", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let started = Instant::now();
    let poll = "/v1/streams/lp/records:long-poll?cursor=now&waitMs=1500";
    let waiting = cell.call(0, "GET", poll, b"");
    let wake = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cell.state.admission.parked() < 1 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let unread = never_read(&cell, 0, "GET", RECORDS, b"").await;
        let held = settle_at(&cell, 0, (page, page)).await;
        let appended = cell
            .call(0, "POST", "/v1/streams/lp/records", br#"{"n":1}"#)
            .await;
        (unread, held, appended.0)
    };
    let ((status, headers, body), (unread, held, appended)) =
        futures_util::future::join(waiting, wake).await;
    let waited = started.elapsed();
    assert_eq!(
        (held, appended, status, body.len(), ledgers(&cell, 0)),
        ((page, page), 200, 204, 0, (page, page)),
        "the woken long-poll, after {waited:?}"
    );
    assert!(
        waited >= Duration::from_millis(1400),
        "answered after {waited:?}"
    );
    drop(unread);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    let cursor = &headers["prisma-next-cursor"];
    let from = format!("/v1/streams/lp/records?cursor={cursor}");
    let (status, _, read) = cell.call(0, "GET", &from, b"").await;
    assert_eq!((status, read.as_slice()), (200, br#"[{"n":1}]"#.as_slice()));
    engine_shutdown(&cell.state).await;
}

/// A consumer pull reserves what its walk reads before it leases (capacity
/// review C1): with project 0's line at two coverages and room for the
/// pulls' bodies, two never-read pulls pass admission and are held after
/// their coverage read (`PullBeforeReceive`), a third waits 2 s and is
/// refused 429 `project_memory_pressure`, and once the two render their
/// batches project 0 holds them inside its line and project 1 reads at
/// once.
/// Before: every pull passed admission at pressure 0 (eight together held
/// 25,271,096 B against the line of 8,388,608 B, past the instance's read
/// memory of 25,165,824 B, and project 1 was refused 503
/// `read_memory_busy`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projects_pulls_reserve_their_coverage_inside_its_line_and_its_neighbour_reads() {
    const Q: &str = "cq";
    let cell = open_cell(CellSpec::open(2)).await;
    keyed_queue(&cell, 0, Q, 3).await;
    big_stream(&cell, 1).await;
    let rest = at_baseline(&cell, 0).await;
    assert_eq!(rest, (0, 0), "project 0 at rest: {}", pressure(&cell, 0));
    let coverage = u64::try_from(crate::application::consumer::PULL_COVERAGE_BYTES).unwrap();
    let line = 2 * coverage + BODY_ROOM;
    cell.state.admission.set_project_memory_pressure_bytes(line);
    cell.state
        .admission
        .set_read_memory_capacity(3 * PAGE_BUDGET);
    let before = parked(Fp::PullBeforeReceive, Q);
    crate::failpoints::park_pull_before_receive(Q);
    let pull = |g: usize| format!("/v1/streams/{Q}/consumers/g{g}:pull");
    let mut held = Vec::new();
    for g in 0..2 {
        held.push(never_read(&cell, 0, "POST", &pull(g), br#"{"max":48}"#).await);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while parked(Fp::PullBeforeReceive, Q) - before < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let reserved = ledgers(&cell, 0);
    // A third pull that passed admission would wait at the failpoint: it is
    // given 5 s to be refused.
    let started = Instant::now();
    let path = pull(2);
    let third = cell.call(0, "POST", &path, br#"{"max":48}"#);
    let third = tokio::time::timeout(Duration::from_secs(5), third).await;
    let waited = started.elapsed();
    let at_third = pressure(&cell, 0);
    let third = third.map(|(status, headers, body)| {
        let refusal: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let code = refusal["error"]["code"].as_str().map(str::to_string);
        (status, code, headers.get("retry-after").cloned())
    });
    let past_admission = parked(Fp::PullBeforeReceive, Q) - before;
    crate::failpoints::release_pull_before_receive(Q);
    let refused = (
        429,
        Some("project_memory_pressure".into()),
        Some("1".into()),
    );
    assert_eq!(
        (past_admission, reserved, third.ok()),
        (2, (2 * coverage, 2 * coverage), Some(refused)),
        "the third pull, after {waited:?}, with project 0 at {at_third}"
    );
    assert!(waited >= Duration::from_secs(2), "refused after {waited:?}");
    let (hostile, instance) = at_rest(&cell, 0).await;
    let neighbour = prompt_read(&cell, 1).await;
    assert_eq!(
        (hostile <= line, hostile == instance, neighbour.clone()),
        (true, true, (200, None, true)),
        "project 0 holds {hostile} B of batches against its line of {line} B; its \
         neighbour's read: {neighbour:?}"
    );
    drop(held);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// A pull waiting for messages holds nothing, and takes its coverage
/// reservation again before it walks: woken while an unread page of its
/// project fills its line of one coverage and room for the pull's body,
/// it finds no room before its wait ends, answers empty and leases
/// nothing, and the message is pulled once the line has room. Before: a
/// woken pull walked unreserved and leased the message past the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_pull_without_room_for_its_coverage_answers_empty_and_leases_nothing() {
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    let created = cell
        .call(0, "PUT", "/v1/streams/wq", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let consumer = cell
        .call(0, "PUT", "/v1/streams/wq/consumers/g", b"{}")
        .await;
    assert_eq!(consumer.0, 201, "{}", String::from_utf8_lossy(&consumer.2));
    let rest = at_baseline(&cell, 0).await;
    assert_eq!(rest, (0, 0), "project 0 at rest: {}", pressure(&cell, 0));
    let coverage = u64::try_from(crate::application::consumer::PULL_COVERAGE_BYTES).unwrap();
    cell.state
        .admission
        .set_project_memory_pressure_bytes(coverage + BODY_ROOM);
    let path = "/v1/streams/wq/consumers/g:pull";
    let started = Instant::now();
    let waiting = async {
        let answer = cell
            .call(0, "POST", path, br#"{"max":1,"waitMs":1500}"#)
            .await;
        (answer, started.elapsed(), pressure(&cell, 0))
    };
    let wake = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cell.state.admission.parked() < 1 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let parked = ledgers(&cell, 0);
        let unread = never_read(&cell, 0, "GET", RECORDS, b"").await;
        let held = settle_at(&cell, 0, (page, page)).await;
        let appended = cell
            .call(0, "POST", "/v1/streams/wq/records", br#"{"n":1}"#)
            .await;
        (unread, parked, held, appended.0)
    };
    let (((status, _, body), answered, at_answer), (unread, parked, held, appended)) =
        futures_util::future::join(waiting, wake).await;
    let waited = started.elapsed();
    let code = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
    let messages = |body: &[u8]| {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v["messages"].as_array().map(Vec::len))
    };
    assert_eq!(
        (parked, held, appended, status, messages(&body)),
        ((0, 0), (page, page), 200, 200, Some(0)),
        "the woken pull, after {waited:?}: answered {code:?} after {answered:?} with project 0 \
         at {at_answer}; project 0 now {}",
        pressure(&cell, 0)
    );
    assert!(
        waited >= Duration::from_millis(1400),
        "answered after {waited:?}"
    );
    assert_eq!(settle_at(&cell, 0, (page, page)).await, (page, page));
    drop(unread);
    let rest = at_baseline(&cell, 0).await;
    assert_eq!(
        rest,
        (0, 0),
        "project 0 at rest again: {}",
        pressure(&cell, 0)
    );
    let (status, _, body) = cell.call(0, "POST", path, br#"{"max":1}"#).await;
    assert_eq!(
        (status, messages(&body)),
        (200, Some(1)),
        "the message waited: {}",
        String::from_utf8_lossy(&body)
    );
    engine_shutdown(&cell.state).await;
}

/// The answer to a long-poll on project 0's `lp` (created here) that
/// parks, finds an unread page of `big` holding its project's line of one
/// page budget when it wakes, and is woken by `then` (a request on `lp`):
/// (its status, headers, body, the wait in ms, `then`'s status). The
/// unread page is released before it returns.
async fn woken_at_the_line(cell: &Cell, poll: &str, then: (&str, &str, &[u8])) -> WokenAnswer {
    let page = big_stream(cell, 0).await;
    assert_eq!(settle_at(cell, 0, (0, 0)).await, (0, 0), "big's read left");
    let created = cell
        .call(0, "PUT", "/v1/streams/lp", br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let started = Instant::now();
    let wake = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cell.state.admission.parked() < 1 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let unread = never_read(cell, 0, "GET", RECORDS, b"").await;
        assert_eq!(settle_at(cell, 0, (page, page)).await, (page, page));
        let (method, path, body) = then;
        (unread, cell.call(0, method, path, body).await.0)
    };
    let ((status, headers, body), (unread, woke)) =
        futures_util::future::join(cell.call(0, "GET", poll, b""), wake).await;
    let waited = started.elapsed().as_millis();
    drop(unread);
    assert_eq!(settle_at(cell, 0, (0, 0)).await, (0, 0));
    (status, headers, body, waited, woke)
}

type WokenAnswer = (
    u16,
    std::collections::HashMap<String, String>,
    Vec<u8>,
    u128,
    u16,
);

/// Waits, up to 10 s, until `ready` holds.
async fn until(ready: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(ready(), "never ready");
}

/// An applied long-poll woken by records its project's line has no room
/// for answers its timeout with its durable cursor at the durable frontier
/// it waited with, never past it (`resumed` caps the durable position at
/// the start). With the shard's dispatch held, r1 is applied but not
/// durable when the poll starts from the applied tail, and r2 wakes it;
/// once dispatch resumes, a read from that durable cursor serves r1 and r2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_on_applied_records_without_room_keeps_its_durable_cursor_at_the_frontier()
 {
    const LP: &str = "/v1/streams/lp/records";
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    let json = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(cell.call(0, "PUT", "/v1/streams/lp", json).await.0, 201);
    assert_eq!(cell.call(0, "POST", LP, br#"{"n":0}"#).await.0, 200);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let sref = ProjectId::new(&project(0)).unwrap().stream_ref("lp");
    let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = cell.state.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    let applied = || handle.state.lock().unwrap().applied.next;
    let guard = engine.test_hold_dispatch().await;
    let polled = async {
        until(|| applied() >= 2).await;
        let poll = "/v1/streams/lp/records:long-poll?cursor=now&deliver=applied&waitMs=1500";
        let answer = cell.call(0, "GET", poll, b"").await;
        drop(guard);
        answer
    };
    let woken = async {
        until(|| applied() >= 2 && cell.state.admission.parked() >= 1).await;
        let unread = never_read(&cell, 0, "GET", RECORDS, b"").await;
        let held = settle_at(&cell, 0, (page, page)).await;
        (
            unread,
            held,
            cell.call(0, "POST", LP, br#"{"n":2}"#).await.0,
        )
    };
    let r1 = cell.call(0, "POST", LP, br#"{"n":1}"#);
    let ((status, headers, body), r1, (unread, held, r2)) =
        futures_util::future::join3(polled, r1, woken).await;
    drop(unread);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    assert_eq!(
        (held, r1.0, r2, status, body.len()),
        ((page, page), 200, 200, 204, 0)
    );
    let durable = &headers["prisma-durable-cursor"];
    let from = format!("{LP}?cursor={durable}");
    let (status, _, read) = cell.call(0, "GET", &from, b"").await;
    assert_eq!(
        (status, String::from_utf8_lossy(&read).as_ref()),
        (200, r#"[{"n":1},{"n":2}]"#)
    );
    engine_shutdown(&cell.state).await;
}

/// A long-poll whose stream is sealed while it waits at its project's line
/// answers the seal at once: a seal brings no records past its cursor, so
/// it takes no reservation again and never waits for room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_sealed_while_it_waits_at_the_line_answers_the_seal_at_once() {
    let cell = open_cell(CellSpec::open(1)).await;
    let poll = "/v1/streams/lp/records:long-poll?cursor=now&waitMs=1500";
    let seal = ("POST", "/v1/streams/lp:seal", b"{}".as_slice());
    let (status, headers, _, waited, sealed) = woken_at_the_line(&cell, poll, seal).await;
    assert_eq!(
        (
            sealed,
            status,
            headers.get("prisma-sealed").map(String::as_str)
        ),
        (200, 204, Some("true")),
        "after {waited} ms"
    );
    assert!(waited < 1_000, "answered after {waited} ms");
    engine_shutdown(&cell.state).await;
}
