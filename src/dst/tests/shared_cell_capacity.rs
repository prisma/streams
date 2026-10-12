//! Shared-cell capacity (the capacity review of 2026-10-07): what one
//! project inside its quotas can hold of what a shared cell's projects
//! share, besides read memory (`read_memory`). Each test pins one bound.
//!
//! Green: a write's buffered bodies share their project's memory line
//! (C3), a creation document holds at most 64 KiB (C4), a catalog page
//! reserves what it may hold as a read does (C8), a settle's dead-letter
//! handoff reads only what its project can hold (C1's residual), and a
//! deleted or expired watched stream's touch journal ends with it (C9).
//! Ignored, red until the owner decides it (`impl/fixes.md`): the coverage
//! a pull reads charged to nobody (C7).
//!
//! The hostile project is project 0, its neighbour project 1. A client
//! that "never reads" sends its request over a socket with a 4 KiB receive
//! buffer and leaves the response unread.

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use crate::failpoints::Fp;
use crate::project_policy::ProjectQuotas;
use crate::registry::Registry;
use crate::tenant::ProjectId;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
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

/// Project `i`'s JSON stream `name` holding `records` records of 64 KiB.
async fn big_stream(cell: &Cell, i: usize, name: &str, records: usize) {
    let path = format!("/v1/streams/{name}");
    assert_eq!(cell.call(i, "PUT", &path, CREATE).await.0, 201);
    let records: Vec<String> = (0..records).map(padded).collect();
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

/// The largest creation document a create accepts (`MAX_CONFIG_BODY`).
const CREATE_BOUND: usize = 64 * 1024;

/// A create body of exactly `len` bytes (at least about 12 KiB): 64
/// watches of 16 field pointers padded alike, the last pointer taking the
/// remainder, all stored in the descriptor.
fn fat_create(len: usize) -> String {
    let doc = |pad: usize, rest: usize| {
        let watches: Vec<String> = (0..64)
            .map(|w| {
                let fields: Vec<String> = (0..16)
                    .map(|f| {
                        let last = usize::from(w == 63 && f == 15);
                        format!("\"/{w}-{f}-{}\"", "p".repeat(pad + last * rest))
                    })
                    .collect();
                format!(r#"{{"name":"w{w}","fields":[{}]}}"#, fields.join(","))
            })
            .collect();
        format!(
            r#"{{"format":{{"kind":"json"}},"watches":[{}]}}"#,
            watches.join(",")
        )
    };
    let spare = len - doc(0, 0).len();
    let body = doc(spare / 1024, spare % 1024);
    assert_eq!(body.len(), len);
    body
}

/// The error code of a product error body.
fn error_code(body: &[u8]) -> Option<String> {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    value["error"]["code"].as_str().map(str::to_string)
}

/// An answer's status, error code and `Retry-After`.
fn refusal(
    (status, headers, body): (u16, HashMap<String, String>, Vec<u8>),
) -> (u16, Option<String>, Option<String>) {
    (
        status,
        error_code(&body),
        headers.get("retry-after").cloned(),
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

/// C4. A creation document holds at most 64 KiB, and so a descriptor at
/// most about 64 KiB of watch definitions. It could be 256 KiB (64 watches
/// x 16 field pointers of any length), creates are charged to no byte
/// quota, and the descriptor cache counted entries (65,536), not bytes (its
/// estimate of what it holds, counting what each descriptor allocates, is
/// held to 64 MiB now, C4 (b), pinned by
/// `registry::cache::tests::the_cache_holds_its_byte_budget`), and
/// deep-clones a descriptor under its one lock on every lookup: under the
/// shared cell's append ceiling (257,500 B/s at k = 8) project 0 created
/// 16 descriptors of about 245 KB in well under a second. Pass: 16 creates
/// of 64 KiB and one byte are each refused 400 `invalid_config` and store
/// nothing; one of exactly 64 KiB is created and listed (its descriptor,
/// about 66 KB, is within the catalog's per-descriptor guard).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let over = fat_create(CREATE_BOUND + 1);
    let mut answers = Vec::new();
    for n in 0..CREATES {
        let path = format!("/v1/streams/fat-{n}");
        let (status, _, body) = cell.call(0, "PUT", &path, over.as_bytes()).await;
        answers.push((status, error_code(&body)));
    }
    assert_eq!(
        answers,
        vec![(400, Some("invalid_config".to_string())); CREATES],
        "creates of {} bytes under an append ceiling of 257,500 B/s",
        over.len()
    );
    let at = fat_create(CREATE_BOUND);
    let (status, _, body) = cell
        .call(0, "PUT", "/v1/streams/fat-at", at.as_bytes())
        .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let listing = cell.json(0, "GET", "/v1/streams").await;
    let listed: Vec<&str> = listing["streams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert_eq!(listed, ["fat-at"], "only the create at the bound stored");
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

/// C8 (phase B item B6). A catalog page materializes up to 4 MiB of
/// stored descriptors (`MAX_PAGE_BYTES`, read with up to 8,064 store GETs)
/// and up to 8 more of at most 128 KiB fetched ahead. It reserved nothing,
/// so it was served while its project's held pages filled its memory line,
/// where a read is refused: 64 concurrent listings of one project could
/// materialize up to 320 MiB. It now reserves what it may hold decoded
/// (`Registry::catalog_page_bytes`, about 5.3 MiB for a page of 26 or more) as a
/// read reserves its page, and releases it once it has answered (it
/// renders names only). Here 60 descriptors of about 65 KB (creation
/// documents of 63 KiB) make one page of about 3.9 MB, and project 0 holds
/// an unread page of 96 records (about 6.3 MB) under a line of 8 MiB.
/// Pass: the listing is refused as the read is (429
/// `project_memory_pressure`, `Retry-After: 1`), and once the unread page
/// is released it lists all 61 streams, in two pages since a page counts
/// its descriptors decoded (about 98 KB each here), and leaves nothing
/// held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catalog_page_is_held_to_its_projects_memory_line_as_a_read_is() {
    const STREAMS: usize = 60;
    let cell = open_cell(CellSpec::open(1)).await;
    let body = fat_create(CREATE_BOUND - 1024);
    for n in 0..STREAMS {
        let path = format!("/v1/streams/cat-{n:02}");
        assert_eq!(cell.call(0, "PUT", &path, body.as_bytes()).await.0, 201);
    }
    big_stream(&cell, 0, "page", 96).await;
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
    let read = refusal(cell.call(0, "GET", page, b"").await);
    let started = Instant::now();
    let listed = refusal(cell.call(0, "GET", "/v1/streams?limit=1000", b"").await);
    let took = started.elapsed();
    let pressure = Some("project_memory_pressure".to_string());
    assert_eq!(
        read,
        (429, pressure, Some("1".to_string())),
        "a read beside {held} held bytes"
    );
    assert_eq!(
        listed, read,
        "with {held} bytes of unread page held under a line of {PAGE_BUDGET}, a catalog page of \
         {STREAMS} descriptors answered as a read, in {took:?}"
    );
    drop(unread);
    let deadline = Instant::now() + Duration::from_secs(5);
    while read_held(&cell, 0) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (mut pages, mut listed, mut cursor) = (0, 0, None);
    loop {
        let path = cursor.map_or("/v1/streams?limit=1000".to_string(), |c: String| {
            format!("/v1/streams?limit=1000&cursor={c}")
        });
        let listing = cell.json(0, "GET", &path).await;
        pages += 1;
        listed += listing["streams"].as_array().map_or(0, Vec::len);
        cursor = listing["cursor"].as_str().map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        (pages, listed),
        (2, STREAMS + 1),
        "the listing of the 60 and the read stream once nothing is held, about 98 KB each decoded"
    );
    assert_eq!(
        at_rest(|| read_held(&cell, 0)).await,
        0,
        "held after the listing"
    );
    engine_shutdown(&cell.state).await;
}

/// C8 (phase B item B6): a listing holds its reservation while it reads
/// its page and releases it once it has answered. With the first
/// descriptor GET of project 0's page held at the store, the project's
/// read bytes are exactly what a page of up to 1,000 streams reserves
/// (`Registry::catalog_page_bytes`, 5 MiB); released, the listing answers
/// its four streams and the project holds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listing_holds_its_reservation_until_it_has_answered() {
    let store = FaultStore::new(mem(), 808, FaultProfile::clean());
    let cell = open_cell(CellSpec {
        store: store.clone(),
        ..CellSpec::open(1)
    })
    .await;
    for n in 0..4 {
        let path = format!("/v1/streams/s{n}");
        assert_eq!(cell.call(0, "PUT", &path, CREATE).await.0, 201);
    }
    let hex = crate::crypto::hex(project(0).as_bytes());
    let streams = format!("registry/v4/projects/{hex}/streams/");
    let engaged = store.hold_class_under(StoreOp::Get, ObjClass::Other, &streams, 1);
    let listing = cell.call(0, "GET", "/v1/streams?limit=1000", b"");
    let reading = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while engaged.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let held = read_held(&cell, 0);
        store.release_hold();
        held
    };
    let ((status, _, body), held) = futures_util::future::join(listing, reading).await;
    let listed = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["streams"].as_array().map(Vec::len));
    let reserved = Registry::catalog_page_bytes(1000);
    assert_eq!(
        (held, status, listed),
        (reserved, 200, Some(4)),
        "held while the page is read, then the answer"
    );
    assert_eq!(read_held(&cell, 0), 0, "held once the listing answered");
    engine_shutdown(&cell.state).await;
}

/// One POST as project `i`: its status and its JSON answer (`Null` if none).
async fn posted(cell: &Cell, i: usize, path: &str, body: &[u8]) -> (u16, serde_json::Value) {
    let (status, _, answer) = cell.call(i, "POST", path, body).await;
    let value = serde_json::from_slice(&answer).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// `work`'s output and the most project `i` held in read bytes while it
/// ran, sampled every millisecond.
async fn peak_while<T>(cell: &Cell, i: usize, work: impl Future<Output = T>) -> (T, u64) {
    let done = std::sync::atomic::AtomicBool::new(false);
    let working = async {
        let output = work.await;
        done.store(true, Ordering::SeqCst);
        output
    };
    let sampling = async {
        let mut peak = 0;
        while !done.load(Ordering::SeqCst) {
            peak = peak.max(read_held(cell, i));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        peak
    };
    futures_util::future::join(working, sampling).await
}

/// `work`'s output, run with appends to `stream` parked before their
/// enqueue, and what project `i` held in read bytes once one was parked
/// there (`None` if none came within 5 s), released after.
async fn held_at_append<T>(
    cell: &Cell,
    i: usize,
    stream: &str,
    work: impl Future<Output = T>,
) -> (T, Option<u64>) {
    let before = crate::failpoints::parked(Fp::AppendBeforeEnqueue, stream);
    crate::failpoints::park_append_before_enqueue(stream);
    let probe = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut held = None;
        while held.is_none() && Instant::now() < deadline {
            if crate::failpoints::parked(Fp::AppendBeforeEnqueue, stream) > before {
                held = Some(read_held(cell, i));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        crate::failpoints::release_append_before_enqueue(stream);
        held
    };
    futures_util::future::join(work, probe).await
}

/// C1's residual (phase B item B7). A settle that poisons records (a
/// retry at `maxAttempts`) hands them to the dead-letter stream from a read
/// of up to 4 MiB from the lowest poisoned offset, and that read was
/// reserved nowhere: a project settling poison at its 64-inflight ceiling
/// held about 256 MiB beside its memory line. The read now takes its
/// reservation (`PULL_COVERAGE_BYTES`) without waiting, in its project's
/// read bytes and the instance's read memory, held until the handoff
/// ends; without room it reads nothing, the poisoned lease stays held, and
/// the first pull after it expires hands the record off under the pull's
/// own coverage. Here project 0's stream holds 72 records of 64 KiB, its
/// consumer dead-letters at the first retry, and one pull leases two of
/// them for 1 s. Pass: with nothing else held, a settle poisoning record 0
/// holds exactly the coverage while its dead-letter copy is appended, then
/// nothing; beside an unread page of 96 records under a line of 8 MiB, a
/// settle poisoning record 1 dead-letters nothing and the project's read
/// bytes never pass the line; the page released, the first pull after the
/// lease expired dead-letters record 1, and nothing stays held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poisoning_settle_reads_only_what_its_project_can_hold() {
    let cell = open_cell(CellSpec::open(1)).await;
    big_stream(&cell, 0, "page", 96).await;
    big_stream(&cell, 0, "poison", 72).await;
    assert_eq!(
        cell.call(0, "PUT", "/v1/streams/b7-dead", CREATE).await.0,
        201
    );
    let consumer = "/v1/streams/poison/consumers/g0";
    let config = br#"{"maxAttempts":1,"deadLetterStream":"b7-dead"}"#;
    assert_eq!(cell.call(0, "PUT", consumer, config).await.0, 201);
    cell.state
        .admission
        .set_project_memory_pressure_bytes(PAGE_BUDGET);
    let (pull, settle) = (format!("{consumer}:pull"), format!("{consumer}:settle"));
    let (status, leased) = posted(&cell, 0, &pull, br#"{"max":2,"visibilityMs":1000}"#).await;
    let leased_at = Instant::now();
    let tokens: Vec<String> = leased["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["leaseToken"].as_str().map(str::to_string))
        .collect();
    assert_eq!((status, tokens.len()), (200, 2), "the leasing pull");
    let retry = |n: usize| {
        let token = &tokens[n];
        format!(r#"{{"retries":[{{"leaseToken":"{token}","delayMs":0}}]}}"#)
    };
    assert_eq!(at_rest(|| read_held(&cell, 0)).await, 0, "after the pull");
    let body = retry(0);
    let at_once = posted(&cell, 0, &settle, body.as_bytes());
    let ((status, at_once), held) = held_at_append(&cell, 0, "b7-dead", at_once).await;
    let coverage = crate::application::consumer::PULL_COVERAGE_BYTES as u64;
    assert_eq!(
        (status, at_once["dlq"].as_u64(), held, read_held(&cell, 0)),
        (200, Some(1), Some(coverage), 0),
        "a settle with room: its answer {at_once}, held while its dead-letter copy was \
         appended, held after"
    );

    let unread = never_read(&cell, 0, "GET", "/v1/streams/page/records", b"").await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while matches!(read_held(&cell, 0), 0 | PAGE_BUDGET) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let held = read_held(&cell, 0);
    let body = retry(1);
    let deferred = posted(&cell, 0, &settle, body.as_bytes());
    let ((status, deferred), peak) = peak_while(&cell, 0, deferred).await;
    assert!(
        peak <= PAGE_BUDGET,
        "the project held {peak} read bytes under a line of {PAGE_BUDGET} while the settle ran"
    );
    let counts = (deferred["dlq"].as_u64(), deferred["dlqBlocked"].as_u64());
    assert_eq!(
        (status, counts),
        (200, (Some(0), Some(0))),
        "beside {held} bytes of unread page held under a line of {PAGE_BUDGET}, a settle \
         poisoning a record dead-lettered it from an unreserved read: {deferred}"
    );

    drop(unread);
    let released = at_rest(|| read_held(&cell, 0)).await;
    assert_eq!(released, 0, "the page released");
    tokio::time::sleep(Duration::from_millis(1100).saturating_sub(leased_at.elapsed())).await;
    let (status, _) = posted(&cell, 0, &pull, br#"{"max":1}"#).await;
    assert_eq!(status, 200, "the first pull after the lease expired");
    let dead = cell
        .call(0, "GET", "/v1/streams/b7-dead/records", b"")
        .await;
    let copies: Vec<serde_json::Value> = serde_json::from_slice(&dead.2).unwrap_or_default();
    let numbers: Vec<Option<u64>> = copies.iter().map(|c| c["value"]["n"].as_u64()).collect();
    assert_eq!(
        (dead.0, numbers),
        (200, vec![Some(0), Some(1)]),
        "the dead-letter stream once the first pull after the lease's expiry has run"
    );
    assert_eq!(at_rest(|| read_held(&cell, 0)).await, 0, "held at the end");
    engine_shutdown(&cell.state).await;
}
