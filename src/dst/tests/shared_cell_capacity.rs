//! Shared-cell capacity (the capacity review of 2026-10-07): what one
//! project inside its quotas can hold of what a shared cell's projects
//! share, besides read memory (`read_memory`). Each test pins one bound.
//!
//! Green: a write's buffered bodies share their project's memory line
//! (C3), and a deleted or expired watched stream's touch journal ends with
//! it (C9).
//! Ignored, red until the owner decides them (`impl/fixes.md`): the bytes
//! a descriptor may hold (C4), the coverage a pull reads charged to
//! nobody (C7), and a catalog page reserving nothing (C8).
//!
//! The hostile project is project 0, its neighbour project 1. A client
//! that "never reads" sends its request over a socket with a 4 KiB receive
//! buffer and leaves the response unread.

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MIB: u64 = 1 << 20;
/// The page budget a read without `maxBytes` reserves.
const PAGE_BUDGET: u64 = 8 * MIB;
const CREATE: &[u8] = br#"{"format":{"kind":"json"}}"#;

/// Record `n`: a JSON object of 64 KiB and a few bytes.
fn padded(n: usize) -> String {
    format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(64 * 1024))
}

/// Project `i`'s JSON stream `name` holding 48 records of 64 KiB.
async fn big_stream(cell: &Cell, i: usize, name: &str) {
    let path = format!("/v1/streams/{name}");
    assert_eq!(cell.call(i, "PUT", &path, CREATE).await.0, 201);
    let records: Vec<String> = (0..48).map(padded).collect();
    let batch = format!("[{}]", records.join(","));
    let target = format!("{path}/records:batch");
    let appended = cell.call(i, "POST", &target, batch.as_bytes()).await;
    assert_eq!(appended.0, 200, "{}", String::from_utf8_lossy(&appended.2));
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

/// Start a POST of `declared` body bytes as project `i`: the head only.
async fn upload_head(cell: &Cell, i: usize, path: &str, declared: u64) -> TcpStream {
    let mut stream = TcpStream::connect(cell.addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: rig\r\nauthorization: {}\r\nprisma-encryption-key: {PRISMA_KEY}\r\ncontent-length: {declared}\r\n\r\n",
        cell.bearers[i]
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
}

/// Send `sent` body bytes on an upload and stall. A refused upload (the
/// server no longer reading) ends the sending early.
async fn upload_some(stream: &mut TcpStream, sent: usize) {
    let body = vec![b' '; sent];
    let _sent = tokio::time::timeout(Duration::from_secs(5), stream.write_all(&body)).await;
}

/// The status and error code an upload was answered with, if it was
/// answered within 1 s.
async fn answer(stream: &mut TcpStream) -> Option<(u16, Option<String>)> {
    let mut buf = vec![0u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await;
    let n = read.ok()?.ok().filter(|n| *n > 0)?;
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    let status = text.split(' ').nth(1)?.parse().ok()?;
    let code = text
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
    Some((status, code))
}

/// A create body just under `MAX_CONFIG_BODY` (256 KiB): 64 watches of 16
/// field pointers of about 240 bytes, all stored in the descriptor.
fn fat_create() -> String {
    let watches: Vec<String> = (0..64)
        .map(|w| {
            let fields: Vec<String> = (0..16)
                .map(|f| format!("\"/{w}-{f}-{}\"", "p".repeat(228)))
                .collect();
            format!(r#"{{"name":"w{w}","fields":[{}]}}"#, fields.join(","))
        })
        .collect();
    format!(
        r#"{{"format":{{"kind":"json"}},"watches":[{}]}}"#,
        watches.join(",")
    )
}

/// Project `i`'s read bytes.
fn read_held(cell: &Cell, i: usize) -> u64 {
    let id = ProjectId::new(&project(i)).unwrap();
    cell.state.quotas.read_bytes(&id).map_or(0, |b| b.held())
}

/// `probe` once it has not moved for 300 ms, or after 10 s.
async fn at_rest<T: PartialEq + Copy>(probe: impl Fn() -> T) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    let (mut last, mut since) = (probe(), Instant::now());
    while Instant::now() < deadline && since.elapsed() < Duration::from_millis(300) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = probe();
        if now != last {
            (last, since) = (now, Instant::now());
        }
    }
    last
}

/// C3. A write's body is buffered under its project's memory line: four
/// uploads of project 0 whose heads arrive together all pass the write
/// gate at pressure 0, then send 6 MiB each of 8 MiB declared under a
/// line of 8 MiB. Each chunk that would take the project past the line
/// while it holds the others' bytes refuses its upload (429
/// `project_memory_pressure`), so the project's buffered bodies stay at
/// the line, plus the chunks in flight. Before: nothing refused a body
/// past the line, and the project held 25,165,824 body bytes (in
/// production, 64 uploads of 32 MiB: 2 GiB on a 1 GiB instance).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_uploads_share_their_projects_memory_line() {
    const UPLOADS: usize = 4;
    const DECLARED: u64 = 8 * MIB;
    const SENT: usize = 6 << 20;
    let cell = open_cell(CellSpec::open(1)).await;
    assert_eq!(cell.call(0, "PUT", "/v1/streams/up", CREATE).await.0, 201);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let entry = cell
        .state
        .quotas
        .pressure_handle(&ProjectId::new(&project(0)).unwrap())
        .unwrap();
    let mut uploads = Vec::new();
    for _ in 0..UPLOADS {
        uploads.push(upload_head(&cell, 0, "/v1/streams/up/records:batch", DECLARED).await);
    }
    // The heads arrive together, as a client opening its uploads at once
    // sends them: each passes admission while nothing is buffered yet.
    tokio::time::sleep(Duration::from_millis(300)).await;
    futures_util::future::join_all(uploads.iter_mut().map(|u| upload_some(u, SENT))).await;
    let buffered = at_rest(|| entry.estimated_pressure_bytes()).await;
    let mut answers = Vec::new();
    for upload in &mut uploads {
        answers.push(answer(upload).await);
    }
    drop(uploads);
    let refused = answers.iter().flatten().count();
    let refusal = Some((429, Some("project_memory_pressure".to_string())));
    assert!(
        buffered <= PAGE_BUDGET + MIB
            && refused >= 2
            && answers.iter().flatten().all(|a| Some(a.clone()) == refusal),
        "{UPLOADS} uploads of {SENT} bytes each under a line of {PAGE_BUDGET}: the project's \
         pressure holds {buffered} body bytes; answers {answers:?}"
    );
    assert_eq!(at_rest(|| entry.estimated_pressure_bytes()).await, 0);
    engine_shutdown(&cell.state).await;
}

/// C9. Deleting a watched stream retires its touch journal: 32 watched
/// streams created, appended to (which opens each incarnation's journal
/// and its 25 ms flusher task) and deleted leave the runtime's alive task
/// count where it was. Before: each deleted incarnation kept its journal
/// and flusher for the life of the process (35 tasks before, 67 after),
/// attributed to no project, and only a shard fence or move closed them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_watched_streams_leave_no_touch_journal_running() {
    const STREAMS: usize = 32;
    let cell = open_cell(CellSpec::open(1)).await;
    let before = steady_tasks(&cell).await;
    let watched = br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/k"]}]}"#;
    for n in 0..STREAMS {
        let path = format!("/v1/streams/touch-{n}");
        assert_eq!(cell.call(0, "PUT", &path, watched).await.0, 201);
        let record = format!(r#"{{"k":{n}}}"#);
        let appended = cell
            .call(0, "POST", &format!("{path}/records"), record.as_bytes())
            .await;
        assert_eq!(appended.0, 200);
        assert_eq!(cell.call(0, "DELETE", &path, b"").await.0, 204);
    }
    let after = at_rest(alive_tasks).await;
    assert!(
        after <= before,
        "{} more tasks alive after {STREAMS} watched streams were created, appended to and \
         deleted ({before} before, {after} after)",
        after.saturating_sub(before)
    );
    engine_shutdown(&cell.state).await;
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// The runtime's alive tasks once the engine has its steady task set:
/// plain streams created, appended to and deleted leave no task.
async fn steady_tasks(cell: &Cell) -> usize {
    for n in 0..8 {
        let warm = format!("/v1/streams/warm-{n}");
        assert_eq!(cell.call(0, "PUT", &warm, CREATE).await.0, 201);
        let record = format!("{warm}/records");
        assert_eq!(cell.call(0, "POST", &record, br#"{"k":0}"#).await.0, 200);
        assert_eq!(cell.call(0, "DELETE", &warm, b"").await.0, 204);
    }
    at_rest(alive_tasks).await
}

/// Puts the billing clock back however the test ends.
struct BillingClockReset;

impl Drop for BillingClockReset {
    fn drop(&mut self) {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// C9's expiry half (owner-approved 2026-10-08, README "Shared cells Q10"
/// (a)): an expired watched stream is closed by the tombstone walk, not
/// deleted, and the walk's close retires its touch journal, so the
/// journal's flusher ends there, not at a fence (which never comes on a
/// one-shard cell) nor ten idle minutes later. 32 watched streams that
/// expire, appended to once each, leave the alive task count where it was
/// once one walk pass has closed them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_watched_streams_leave_no_touch_journal_after_the_walk() {
    const STREAMS: usize = 32;
    let _clock = crate::billing::billing_clock_lock().write().await;
    let _reset = BillingClockReset;
    let cell = open_cell(CellSpec::open(1)).await;
    let before = steady_tasks(&cell).await;
    let at = chrono::DateTime::from_timestamp_millis(crate::shard::now_ms() + 60_000)
        .unwrap()
        .to_rfc3339();
    let watched = format!(
        r#"{{"format":{{"kind":"json"}},"expiry":{{"at":"{at}"}},"watches":[{{"name":"w","fields":["/k"]}}]}}"#
    );
    for n in 0..STREAMS {
        let path = format!("/v1/streams/expiring-{n}");
        assert_eq!(cell.call(0, "PUT", &path, watched.as_bytes()).await.0, 201);
        let record = format!(r#"{{"k":{n}}}"#);
        let appended = cell
            .call(0, "POST", &format!("{path}/records"), record.as_bytes())
            .await;
        assert_eq!(appended.0, 200);
    }
    let opened = at_rest(alive_tasks).await;
    assert!(
        opened >= before + STREAMS,
        "{opened} tasks with {STREAMS} journals open"
    );
    let expired = crate::shard::now_ms() + 120_000;
    crate::billing::BILLING_CLOCK_OVERRIDE.store(expired, std::sync::atomic::Ordering::Relaxed);
    crate::billing::tombstone_walk(&cell.state).await;
    let after = at_rest(alive_tasks).await;
    assert!(
        after <= before,
        "{} more tasks alive after {STREAMS} watched streams expired and the walk closed them \
         ({before} before, {opened} with them open, {after} after)",
        after.saturating_sub(before)
    );
    engine_shutdown(&cell.state).await;
}

/// C4. A descriptor's bytes are bounded by nothing but the request rate:
/// a create body (and so its stored descriptor: 64 watches x 16 field
/// pointers of any length) may be 256 KiB, creates are charged to no byte
/// quota, and the descriptor cache counts entries (65,536), not bytes, and
/// deep-clones a descriptor under its one lock on every lookup. Under the
/// shared cell's append ceiling (257,500 B/s at k = 8) project 0 creates
/// 16 descriptors of about 245 KB in well under a second. Pass: the
/// project is refused (by a byte quota or a descriptor bound) before 16.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the owner decides capacity review C4: a byte bound on watch definitions and on the descriptor cache"]
async fn descriptor_bytes_are_held_to_a_byte_bound() {
    const CREATES: usize = 16;
    let cell = open_cell(CellSpec {
        quotas: |_| ProjectQuotas {
            append_bytes_per_sec: 257_500,
            max_streams: 8_192,
            ..ProjectQuotas::default()
        },
        ..CellSpec::open(1)
    })
    .await;
    let body = fat_create();
    let started = Instant::now();
    let mut created = 0;
    for n in 0..CREATES {
        let path = format!("/v1/streams/fat-{n}");
        if cell.call(0, "PUT", &path, body.as_bytes()).await.0 == 201 {
            created += 1;
        }
    }
    let took = started.elapsed();
    assert!(
        created < CREATES,
        "{created} descriptors of {} bytes each created in {took:?} under an append ceiling of \
         257,500 B/s",
        body.len()
    );
    engine_shutdown(&cell.state).await;
}

/// C7. A consumer pull's coverage read is charged to nobody: a pull reads
/// up to 4 MiB of the stream before it leases, and only the batch it
/// renders is debited from its project's read quota. With every record
/// leased, each further pull reads about 3 MiB and delivers nothing, so a
/// project under a 4 MiB/s read quota runs every one of nine such pulls
/// (and a parked pull repeats that walk on every append to the stream).
/// Pass: the project is refused (429) before eight empty pulls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the owner decides capacity review C7: who pays for a pull's coverage read"]
async fn empty_pulls_are_held_to_their_projects_read_quota_by_what_they_read() {
    let cell = open_cell(CellSpec {
        quotas: |_| ProjectQuotas {
            read_bytes_per_sec: 4 << 20,
            ..ProjectQuotas::default()
        },
        ..CellSpec::open(1)
    })
    .await;
    let path = "/v1/streams/cover";
    assert_eq!(cell.call(0, "PUT", path, CREATE).await.0, 201);
    for n in 0..48 {
        let record = padded(n);
        let key = format!("k{n}");
        let headers = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", cell.bearers[0].as_str()),
            ("prisma-routing-key", key.as_str()),
        ];
        let records = format!("{path}/records");
        let (status, _, _) = preq(cell.addr, "POST", &records, &headers, record.as_bytes()).await;
        assert_eq!(status, 200);
    }
    let consumer = format!("{path}/consumers/g0");
    let created = cell
        .call(0, "PUT", &consumer, br#"{"maxBatchRecords":48}"#)
        .await;
    assert_eq!(created.0, 201);
    let pull = format!("{consumer}:pull");
    let messages = |body: &[u8]| {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v["messages"].as_array().map(Vec::len))
    };
    let (status, _, body) = cell
        .call(0, "POST", &pull, br#"{"max":48,"visibilityMs":600000}"#)
        .await;
    assert_eq!(
        (status, messages(&body)),
        (200, Some(48)),
        "the leasing pull"
    );
    let mut admitted = 0;
    for _ in 0..9 {
        let (status, _, body) = cell.call(0, "POST", &pull, br#"{"max":48}"#).await;
        if status != 200 {
            break;
        }
        assert_eq!(messages(&body), Some(0), "every record is leased");
        admitted += 1;
    }
    assert!(
        admitted < 8,
        "{admitted} empty pulls admitted, each reading about 3 MiB of coverage, under a read \
         quota of 4 MiB/s"
    );
    engine_shutdown(&cell.state).await;
}

/// C8. A catalog page materializes up to 16 MiB of descriptors
/// (`MAX_PAGE_BYTES`, read with up to 8,064 store GETs) and reserves
/// nothing, so it is served while its project's held pages fill its
/// memory line, where a read is refused: 64 concurrent listings of one
/// project can materialize up to 1 GiB. Here 64 descriptors of about
/// 245 KB make one page of about 15.6 MB. Pass: the catalog page is held
/// to the line as a read is (both answered alike).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until the owner decides capacity review C8: product_list reserves its page (a frozen scope)"]
async fn a_catalog_page_is_held_to_its_projects_memory_line_as_a_read_is() {
    let cell = open_cell(CellSpec::open(1)).await;
    let body = fat_create();
    for n in 0..64 {
        let path = format!("/v1/streams/cat-{n}");
        assert_eq!(cell.call(0, "PUT", &path, body.as_bytes()).await.0, 201);
    }
    big_stream(&cell, 0, "page").await;
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let page = "/v1/streams/page/records";
    let unread = never_read(&cell, 0, "GET", page, b"").await;
    // The page's reservation, then the rendered page itself.
    let deadline = Instant::now() + Duration::from_secs(5);
    while matches!(read_held(&cell, 0), 0 | PAGE_BUDGET) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let held = read_held(&cell, 0);
    let read = cell.call(0, "GET", page, b"").await.0;
    let started = Instant::now();
    let (listed, _, names) = cell.call(0, "GET", "/v1/streams?limit=1000", b"").await;
    let took = started.elapsed();
    drop(unread);
    assert_eq!(
        listed,
        read,
        "with {held} bytes of unread page held under a line of {PAGE_BUDGET}, a read answered \
         {read} and a catalog page of 64 descriptors answered {listed} ({} bytes) in {took:?}",
        names.len()
    );
    engine_shutdown(&cell.state).await;
}
