//! A woken long-poll's read reservation (public-beta phase B item B8, D′
//! in place of the owner's Q1 (D)). A long-poll parks only at its stream's
//! tail, so the page a wake brings is what was appended while it waited:
//! it takes that payload again, plus 64 B a record and 4 KiB a page, at
//! most its admission budget, measured each time it tries for room, so
//! what is appended while it waits for room is taken too. A read from a
//! cursor behind the tail still reserves its whole budget at admission.
//! A fork's woken long-poll takes its whole budget again (the owner's
//! round-3 call 3): its page is read from the tail once its fork chain is
//! resolved, after the wait, so what was appended while it waited cannot
//! bound it.
//!
//! The first leg is bound by its project's memory line. The others are
//! bound by the instance's read memory: a project whose held pages leave
//! it a few KiB under its line also latches its own writes (its pressure
//! counts its held pages and its parked waits), so the appends that wake
//! a waiter there would be refused, and writes never consult the
//! instance's read memory. `woken_at_a_full_line` (F1 and C2, run from
//! `read_memory`) is bound by its project's line all the same: it sets
//! the line once the append that wakes its waiters is applied and before
//! its shard's held dispatch makes it durable. A reservation takes one
//! amount in both ledgers at once (`ReadHold::take`).
//!
//! A client that "never reads" sends its request over a socket with a
//! 4 KiB receive buffer and leaves the response unread, so its page is
//! held until it leaves if it is larger than what the kernel takes from
//! the server ahead of the client: about 1.7 MB on Linux (a loopback send
//! buffer autotuned to 2,626,560 B) and 0.65 MB on macOS. A page the
//! kernel takes whole ends and releases its hold at once.

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::PRISMA_KEY;
use super::fixture_storage::mem;
use super::read_memory::{
    MIB, PAGE_BUDGET, RECORDS, append_to_lp, at_rest, big_stream, ledgers, never_read, padded,
    parked_polls, prompt_read, settle_at, until,
};
use crate::application::creation::{CreateCommand, ForkCommand};
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use crate::tenant::{ProjectId, TenantStreamRef};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// The shared-cell profile's project memory line.
const LINE: u64 = 32 * MIB;
const LP: &str = "/v1/streams/lp";
/// Project 0's fork of its stream `src` (`fork_of_src`).
const FK: &str = "/v1/streams/fk";
/// What a woken page takes beside the payload appended while it waited
/// (`application::read_request`).
const RECORD_FRAMING: u64 = 64;
const PAGE_FRAMING: u64 = 4096;

/// A JSON record of about 1 KiB.
fn kib_record() -> String {
    format!(r#"{{"n":0,"pad":"{}"}}"#, "x".repeat(1000))
}

/// Project 0's JSON stream `lp`, empty.
async fn create_lp(cell: &Cell) {
    let created = cell
        .call(0, "PUT", LP, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
}

/// A project's fan-out wake takes what was appended, not its page budget
/// again: 16 long-polls of project 0 parked at the tail of `lp`, beside
/// unread pages that leave it less than one page budget under its 32 MiB
/// line, are woken by one append of a 1 KiB record and all answer 200
/// with it within 1 s. Before: each took its whole 8 MiB budget again,
/// none fit, and all 16 answered 204 at their deadline with the record
/// waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_reserves_what_was_appended_while_it_waited() {
    let cell = open_cell(CellSpec::open(1)).await;
    big_stream(&cell, 0).await;
    create_lp(&cell).await;
    let poll = format!("{LP}/records:long-poll?cursor=now&waitMs=10000");
    let polls = futures_util::future::join_all((0..16).map(|_| cell.call(0, "GET", &poll, b"")));
    let wake = async {
        until(|| cell.state.admission.parked() >= 16).await;
        let mut unread = Vec::new();
        for _ in 0..8 {
            unread.push(never_read(&cell, 0, "GET", RECORDS, b"").await);
        }
        let partial = format!("{RECORDS}?maxBytes={}", 2 * MIB);
        unread.push(never_read(&cell, 0, "GET", &partial, b"").await);
        let held = at_rest(&cell, 0).await;
        cell.state.admission.set_project_memory_pressure_bytes(LINE);
        let woke = Instant::now();
        let records = format!("{LP}/records");
        let appended = cell
            .call(0, "POST", &records, kib_record().as_bytes())
            .await;
        (unread, held, appended.0, woke)
    };
    let (answers, (unread, held, appended, woke)) = futures_util::future::join(polls, wake).await;
    let answered = woke.elapsed();
    let room = LINE.saturating_sub(held.0);
    assert!(
        held.0 == held.1 && (2 * MIB..PAGE_BUDGET).contains(&room),
        "the unread pages hold {held:?}, leaving {room} B under the line"
    );
    let page = format!("[{}]", kib_record());
    let statuses: Vec<u16> = answers.iter().map(|(status, _, _)| *status).collect();
    let served = answers
        .iter()
        .filter(|(status, _, body)| *status == 200 && body == page.as_bytes())
        .count();
    assert_eq!(
        (appended, served),
        (200, 16),
        "the woken long-polls answered {statuses:?} after {answered:?}"
    );
    assert!(
        answered < Duration::from_secs(1),
        "answered after {answered:?}"
    );
    drop(unread);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// A woken long-poll held out of room measures what it takes when room
/// comes, not when it woke, so what is appended while it waits never takes
/// the instance's read memory past its capacity (the phase B skeptic's
/// S3). Project 0's waiter on `lp` is woken by a 1 KiB record beside
/// unread pages that leave the read memory 1 KiB of room, and waits; 1 MiB
/// more is appended; the capacity is then raised by 63 KiB, room for the
/// 1 KiB record but not for the 1 MiB, and a 1 KiB read's release wakes
/// the waiter, which keeps waiting, holding and answering nothing; a
/// page's release with room for both lets it take them, and it answers a
/// page no larger than what it took. The read memory never holds more
/// than its capacity, and its project's read bytes move with it. Measured
/// at the wake, the waiter takes 5 KiB at the first release and answers a
/// 1 MiB page past the capacity. Before: it took its whole 8 MiB budget
/// again, which neither release leaves, so it never rendered. The
/// waiter's page is read from its socket, not from the ledgers: a woken
/// page is at most 1 MiB (`tail_max_bytes`), which Linux takes whole from
/// the server, ending its body and its hold, though its client never reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_held_out_of_room_never_takes_the_read_memory_past_its_capacity() {
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    create_lp(&cell).await;
    cell.state.admission.set_project_memory_pressure_bytes(LINE);
    let poll = format!("{LP}/records:long-poll?cursor=now&waitMs=20000");
    let mut waiter = never_read(&cell, 0, "GET", &poll, b"").await;
    until(|| cell.state.admission.parked() >= 1).await;
    let mut pages = Vec::new();
    for _ in 0..3 {
        pages.push(never_read(&cell, 0, "GET", RECORDS, b"").await);
    }
    let held = at_rest(&cell, 0).await;
    let capacity = held.1 + 64 * 1024;
    cell.state.admission.set_read_memory_capacity(held.1 + 1024);
    let done = AtomicBool::new(false);
    let scenario = async {
        let records = format!("{LP}/records");
        let woke = cell
            .call(0, "POST", &records, kib_record().as_bytes())
            .await;
        // The waiter wakes at the record's durable dispatch and waits.
        tokio::time::sleep(Duration::from_millis(300)).await;
        append_to_lp(&cell, 16).await;
        cell.state.admission.set_read_memory_capacity(capacity);
        let first = format!("{records}?maxBytes=1024");
        let read = cell.call(0, "GET", &first, b"").await;
        let partial = (at_rest(&cell, 0).await, received(&waiter).await);
        drop(pages.pop());
        let answered = answer(&mut waiter).await;
        let room = at_rest(&cell, 0).await;
        done.store(true, Ordering::Relaxed);
        ((woke.0, read.0, read.2.len()), partial, answered, room)
    };
    let sampled = async {
        let mut peak = (0, 0);
        while !done.load(Ordering::Relaxed) {
            let now = ledgers(&cell, 0);
            peak = (peak.0.max(now.0), peak.1.max(now.1));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        peak
    };
    let ((answers, partial, (status, body), room), peak) =
        futures_util::future::join(scenario, sampled).await;
    let payload = kib_record().len() + (0..16).map(|n| padded(n).len()).sum::<usize>();
    let measure = u64::try_from(payload).unwrap() + 17 * RECORD_FRAMING + PAGE_FRAMING;
    // The 1 KiB record and the first 15 appended: `tail_max_bytes` cuts the 16th.
    let woken: Vec<String> = std::iter::once(kib_record())
        .chain((0..15).map(padded))
        .collect();
    let woken = format!("[{}]", woken.join(","));
    assert_eq!(
        (answers, partial),
        (
            (200, 200, kib_record().len() + 2),
            ((3 * page, 3 * page), 0)
        ),
        "with room for the 1 KiB record but not for the 1 MiB appended while it waited, \
         the waiter holds nothing and has answered no byte (capacity {capacity} B)"
    );
    assert_eq!(
        (status, body.len(), body == woken.as_bytes()),
        (200, woken.len(), true),
        "with room for both, the waiter answers the 1 KiB record and 15 appended, no more \
         than the {measure} B it took"
    );
    assert_eq!(
        room,
        (2 * page, 2 * page),
        "once its client has read its page, the waiter holds nothing"
    );
    assert!(
        peak.0 <= capacity && peak.1 <= capacity,
        "the ledgers peaked at {peak:?} against the read memory's capacity of {capacity} B"
    );
    drop((waiter, pages));
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// The bytes `stream` has received and not read, waiting up to 100 ms for
/// the first.
async fn received(stream: &TcpStream) -> usize {
    let mut buf = vec![0; 64 * 1024];
    tokio::time::timeout(Duration::from_millis(100), stream.peek(&mut buf))
        .await
        .map_or(0, |peeked| peeked.unwrap())
}

/// The response `stream` receives within 5 s: (its status, its body), or
/// (0, nothing) if none arrives whole. 5 s is well inside the 10 s drain
/// window after which the server cuts the unread pages beside it, whose
/// release would otherwise make room for any take.
async fn answer(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let read = async {
        let (mut bytes, mut chunk) = (Vec::new(), vec![0; 64 * 1024]);
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            bytes.extend_from_slice(&chunk[..n]);
            match response(&bytes) {
                Some(whole) => return whole,
                None if n == 0 => return (0, Vec::new()),
                None => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap_or((0, Vec::new()))
}

/// The response `bytes` hold once it is whole, read by its
/// `content-length` (a 204 has none and no body): (its status, its body).
fn response(bytes: &[u8]) -> Option<(u16, Vec<u8>)> {
    let head = bytes.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let text = String::from_utf8_lossy(&bytes[..head]).to_ascii_lowercase();
    let status = text.get(9..12)?.parse().ok()?;
    let length = match text
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
    {
        Some(length) => length.trim().parse::<usize>().ok()?,
        None if status == 204 => 0,
        None => return None,
    };
    let body = bytes.get(head..head + length)?;
    Some((status, body.to_vec()))
}

/// A project's fan-out wake at its full line (isolation review F1 and
/// capacity review C2, in `read_memory`): project 0's `polls` long-polls
/// (`query`) on `lp`, from clients that never read, are woken by one
/// append of `appended` x 64 KiB while two unread pages of its `big` hold
/// its line but for 64 KiB, less than any woken take. They take nothing,
/// answer no byte and stay parked, and project 1 reads at once beside
/// them. Once the pages leave, project 0 holds at most its line, project
/// 1 still reads at once, and each waiter answers the first 15 appended
/// (`tail_max_bytes` cuts the 16th). The waiters are observed on their
/// sockets and in the parked count, which Linux keeps: it takes a woken
/// page whole from the server, ending its hold, and leaves at rest only
/// what a page larger than that holds.
pub(super) async fn woken_at_a_full_line(polls: usize, query: &str, appended: usize) {
    let cell = open_cell(CellSpec::open(2)).await;
    let page = big_stream(&cell, 0).await;
    big_stream(&cell, 1).await;
    let line = 2 * page + 64 * 1024;
    cell.state
        .admission
        .set_read_memory_capacity(line + PAGE_BUDGET);
    let mut waiters = parked_polls(&cell, polls, query).await;
    let pages = fill_the_line_and_wake(&cell, appended, page, line).await;
    let rest = at_rest(&cell, 0).await;
    let parked = cell.state.admission.parked();
    let received = futures_util::future::join_all(waiters.iter().map(received)).await;
    let answered = received.iter().filter(|bytes| **bytes > 0).count();
    assert_eq!(
        (rest, parked, answered, prompt_read(&cell, 1).await),
        (
            (2 * page, 2 * page),
            i64::try_from(polls).unwrap(),
            0,
            (200, None, true)
        ),
        "woken beside unread pages that hold project 0's line of {line} B but for 64 KiB, \
         the long-polls hold nothing, stay parked and answer no byte, and project 1 reads \
         at once (the ledgers at rest, the parked waits, the waiters that answered, \
         project 1's read)"
    );
    drop(pages);
    let (held, instance) = at_rest(&cell, 0).await;
    let neighbour = prompt_read(&cell, 1).await;
    assert_eq!(
        (held <= line, held == instance, neighbour.clone()),
        (true, true, (200, None, true)),
        "project 0 holds {held} B against its line of {line} B (instance {instance} B); \
         its neighbour's read: {neighbour:?}"
    );
    let answers = futures_util::future::join_all(waiters.iter_mut().map(answer)).await;
    let woken: Vec<String> = (0..15).map(padded).collect();
    let woken = format!("[{}]", woken.join(","));
    let statuses: Vec<(u16, usize)> = answers.iter().map(|(s, b)| (*s, b.len())).collect();
    let served = answers
        .iter()
        .filter(|(status, body)| *status == 200 && *body == woken.as_bytes())
        .count();
    assert_eq!(
        served, polls,
        "each waiter answers the first 15 appended: {statuses:?}"
    );
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// Append `n` x 64 KiB to project 0's `lp` with its shard's dispatch held
/// and, once they are applied, read two pages of `big` from clients that
/// never read, then set project 0's memory line to `line`: the release
/// makes the append durable and wakes `lp`'s waits. Set before, the line
/// would refuse the append, whose body and the parked waits weigh in the
/// pressure its writes are latched at. Returns the pages' sockets.
async fn fill_the_line_and_wake(cell: &Cell, n: usize, page: u64, line: u64) -> Vec<TcpStream> {
    let sref = ProjectId::new(&project(0)).unwrap().stream_ref("lp");
    let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = cell.state.engine_for(&route).await.unwrap();
    let handle = engine.stream_handle(seg.identity).await.unwrap();
    let applied = || handle.state.lock().unwrap().applied.next;
    let guard = engine.test_hold_dispatch().await;
    let fill = async {
        until(|| applied() >= u64::try_from(n).unwrap()).await;
        let mut pages = Vec::new();
        for _ in 0..2 {
            pages.push(never_read(cell, 0, "GET", RECORDS, b"").await);
        }
        let held = settle_at(cell, 0, (2 * page, 2 * page)).await;
        assert_eq!(held, (2 * page, 2 * page), "two unread pages of big");
        cell.state.admission.set_project_memory_pressure_bytes(line);
        drop(guard);
        pages
    };
    let ((), pages) = futures_util::future::join(append_to_lp(cell, n), fill).await;
    pages
}

/// A woken long-poll takes exactly what was appended while it waited, 64 B
/// a record and 4 KiB a page: woken by two records beside an unread page
/// that leaves the read memory exactly that much room, it answers them at
/// once; with one byte less, it answers 204 at its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_takes_what_was_appended_with_its_framing_and_no_more() {
    const BATCH: &str = r#"[{"n":1},{"n":22}]"#;
    let cell = open_cell(CellSpec::open(1)).await;
    big_stream(&cell, 0).await;
    create_lp(&cell).await;
    let unread = never_read(&cell, 0, "GET", RECORDS, b"").await;
    let held = at_rest(&cell, 0).await;
    let measure = 15 + 2 * RECORD_FRAMING + PAGE_FRAMING;
    let fits = woken_with_room(&cell, held.1 + measure, BATCH).await;
    let short = woken_with_room(&cell, held.1 + measure - 1, BATCH).await;
    assert_eq!(
        (fits.0, fits.1.as_str()),
        (200, BATCH),
        "with room for {measure} B, after {} ms",
        fits.2
    );
    assert!(fits.2 < 1_000, "answered after {} ms", fits.2);
    assert_eq!(
        (short.0, short.1.len()),
        (204, 0),
        "with room for {} B, after {} ms",
        measure - 1,
        short.2
    );
    drop(unread);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// A long-poll of project 0 on `lp` that parks while the read memory has
/// no capacity and is woken by `batch` with its capacity at `capacity`:
/// (its status, its body, ms from the append to its answer).
async fn woken_with_room(cell: &Cell, capacity: u64, batch: &str) -> (u16, String, u128) {
    cell.state.admission.set_read_memory_capacity(0);
    let parked = cell.state.admission.parked();
    let poll = format!("{LP}/records:long-poll?cursor=now&waitMs=1500");
    let wake = async {
        until(|| cell.state.admission.parked() > parked).await;
        cell.state.admission.set_read_memory_capacity(capacity);
        let woke = Instant::now();
        let path = format!("{LP}/records:batch");
        assert_eq!(cell.call(0, "POST", &path, batch.as_bytes()).await.0, 200);
        woke
    };
    let ((status, _, body), woke) =
        futures_util::future::join(cell.call(0, "GET", &poll, b""), wake).await;
    let body = String::from_utf8_lossy(&body).into_owned();
    (status, body, woke.elapsed().as_millis())
}

/// Project 0's JSON stream `src` and `fk`, a fork of it at its tail, made
/// as the raw surface's fork makes one (`CreationService::create`): a
/// shared cell's customer forks on neither surface. Returns `src`.
async fn fork_of_src(cell: &Cell) -> TenantStreamRef {
    let created = cell
        .call(
            0,
            "PUT",
            "/v1/streams/src",
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
    assert_eq!(created.0, 201);
    let id = ProjectId::new(&project(0)).unwrap();
    let forked = cell
        .state
        .creation_service()
        .create(CreateCommand {
            sref: id.stream_ref("fk"),
            key: crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap(),
            content_type: Some("application/json".to_string()),
            ttl_secs: None,
            expires_at_ms: None,
            close: false,
            body: bytes::Bytes::new(),
            fork: Some(ForkCommand {
                source: id.stream_ref("src"),
                offset: None,
                sub_offset: None,
            }),
        })
        .await
        .map(|out| out.created);
    assert!(matches!(forked, Ok(true)), "the fork: {forked:?}");
    id.stream_ref("src")
}

/// `n` records of 64 KiB appended to project 0's stream at `path` in one
/// batch: the append's status.
async fn append_to(cell: &Cell, path: &str, n: usize) -> u16 {
    let records: Vec<String> = (0..n).map(padded).collect();
    let batch = format!("[{}]", records.join(","));
    let path = format!("{path}/records:batch");
    cell.call(0, "POST", &path, batch.as_bytes()).await.0
}

/// `work`'s output and the most project 0 held in read bytes and the
/// instance in read memory while it ran, sampled every millisecond.
async fn peak_while<T>(cell: &Cell, work: impl Future<Output = T>) -> (T, (u64, u64)) {
    let done = AtomicBool::new(false);
    let working = async {
        let output = work.await;
        done.store(true, Ordering::Relaxed);
        output
    };
    let sampling = async {
        let mut peak = (0, 0);
        while !done.load(Ordering::Relaxed) {
            let now = ledgers(cell, 0);
            peak = (peak.0.max(now.0), peak.1.max(now.1));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        peak
    };
    futures_util::future::join(working, sampling).await
}

/// A woken fork long-poll takes its whole page budget again (the owner's
/// round-3 call 3): its page is read from the tail once its fork chain is
/// resolved (`read_stitched`, a registry read for each ancestor), after
/// its wait, so what was appended while it waited cannot bound it. Eight
/// never-read long-polls of project 0 on its fork `fk` are woken by a
/// 1 KiB record while the read of `fk`'s source descriptor is held at the
/// store (its cache slot expired), and 96 records of 64 KiB are appended
/// before the read is released. With the read memory at a page budget and
/// 1 MiB, one waiter takes its 8 MiB and answers the record and the 96, a
/// page of 6 MiB its client leaves unread (more than either kernel takes,
/// so it stays held, exactly, in both ledgers), and the seven others, with
/// no room beside it, answer 204 at their deadline. Neither the project's
/// read bytes nor the read memory ever passes its line or its capacity.
/// Before (D′ on forks): each waiter took 5 KiB, the record and its
/// framing, and all eight rendered the 6 MiB: the ledgers held 50,353,536 B
/// against the 32 MiB line and the 9 MiB read memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_fork_long_poll_takes_its_whole_page_budget_again() {
    let store = FaultStore::new(mem(), 808, FaultProfile::clean());
    let cell = open_cell(CellSpec {
        store: store.clone(),
        ..CellSpec::open(1)
    })
    .await;
    let src = fork_of_src(&cell).await;
    let started = Instant::now();
    let poll = format!("{FK}/records:long-poll?cursor=now&waitMs=5000");
    let mut waiters = Vec::new();
    for _ in 0..8 {
        waiters.push(never_read(&cell, 0, "GET", &poll, b"").await);
    }
    until(|| cell.state.admission.parked() >= 8).await;
    let capacity = PAGE_BUDGET + MIB;
    cell.state.admission.set_project_memory_pressure_bytes(LINE);
    cell.state.admission.set_read_memory_capacity(capacity);
    cell.state.registry.expire_cache_for_tests(&src);
    let hex = |raw: &str| crate::crypto::hex(raw.as_bytes());
    let descriptor = format!(
        "registry/v4/projects/{}/streams/{}.json",
        hex(&project(0)),
        hex("src")
    );
    let engaged = store.hold_class_under(StoreOp::Get, ObjClass::Other, &descriptor, 8);
    let page: Vec<String> = std::iter::once(kib_record())
        .chain((0..96).map(padded))
        .collect();
    let page = format!("[{}]", page.join(","));
    let held = u64::try_from(page.len()).unwrap();
    let scenario = async {
        let records = format!("{FK}/records");
        let woke = cell
            .call(0, "POST", &records, kib_record().as_bytes())
            .await;
        until(|| engaged.load(Ordering::SeqCst) >= 1).await;
        let appended = append_to(&cell, FK, 96).await;
        store.release_hold();
        until(|| cell.state.admission.parked() == 0).await;
        let waited = started.elapsed();
        let rest = settle_at(&cell, 0, (held, held)).await;
        let mut answers = Vec::new();
        for waiter in &mut waiters {
            answers.push(answer(waiter).await);
        }
        (woke.0, appended, waited, rest, answers)
    };
    let ((woke, appended, waited, rest, answers), peak) = peak_while(&cell, scenario).await;
    let statuses: Vec<(u16, usize)> = answers.iter().map(|(s, b)| (*s, b.len())).collect();
    assert!(
        peak.0 <= LINE && peak.1 <= capacity,
        "the ledgers peaked at {peak:?} against the {LINE} B line and the {capacity} B read \
         memory; the woken fork long-polls answered {statuses:?}"
    );
    let served = answers
        .iter()
        .filter(|(status, body)| *status == 200 && *body == page.as_bytes())
        .count();
    let timeouts = answers
        .iter()
        .filter(|(status, body)| *status == 204 && body.is_empty())
        .count();
    assert_eq!(
        (woke, appended, rest, served, timeouts),
        (200, 200, (held, held), 1, 7),
        "the woken fork long-polls answered {statuses:?}, the ledgers then at rest"
    );
    assert!(
        waited >= Duration::from_millis(4900),
        "the waits ended after {waited:?}"
    );
    drop(waiters);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// Woken fork long-polls beside no held page all answer their record
/// (round-3 call 3's companion): under the same 32 MiB line and 9 MiB read
/// memory, eight of project 0's long-polls on its fork `fk` each take
/// their whole 8 MiB again in turn, render the 1 KiB record and, their
/// clients reading, release it, so all eight answer it within 1 s of its
/// append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn woken_fork_long_polls_beside_no_held_page_all_answer_their_record() {
    let cell = open_cell(CellSpec::open(1)).await;
    fork_of_src(&cell).await;
    let poll = format!("{FK}/records:long-poll?cursor=now&waitMs=10000");
    let polls = futures_util::future::join_all((0..8).map(|_| cell.call(0, "GET", &poll, b"")));
    let wake = async {
        until(|| cell.state.admission.parked() >= 8).await;
        cell.state.admission.set_project_memory_pressure_bytes(LINE);
        cell.state
            .admission
            .set_read_memory_capacity(PAGE_BUDGET + MIB);
        let woke = Instant::now();
        let records = format!("{FK}/records");
        let appended = cell
            .call(0, "POST", &records, kib_record().as_bytes())
            .await;
        (appended.0, woke)
    };
    let (answers, (appended, woke)) = futures_util::future::join(polls, wake).await;
    let answered = woke.elapsed();
    let page = format!("[{}]", kib_record());
    let statuses: Vec<u16> = answers.iter().map(|(status, _, _)| *status).collect();
    let served = answers
        .iter()
        .filter(|(status, _, body)| *status == 200 && body == page.as_bytes())
        .count();
    assert_eq!(
        (appended, served),
        (200, 8),
        "the woken fork long-polls answered {statuses:?} after {answered:?}"
    );
    assert!(
        answered < Duration::from_secs(1),
        "answered after {answered:?}"
    );
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// On a shared cell (k > 1) a read without `maxBytes` pages 1 MiB (owner
/// call Shared cells Q1 (B), the Layer B fallback): beside a held 3 MiB
/// page it fits a read memory with 1 MiB 4 KiB of room left and renders
/// 14-47 records, at most 1 MiB. Before: it reserved 8 MiB and was refused
/// 503. The held page is larger than either kernel takes from a client that
/// never reads (Linux about 1.7 MB, macOS 0.65 MB), so it stays held on both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_cells_default_page_reserves_one_mib_and_renders_at_most_that() {
    let mut spec = CellSpec::open(1);
    spec.cli = |cli| cli.project_share_k = 8;
    let cell = open_cell(spec).await;
    let put = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(cell.call(0, "PUT", "/v1/streams/big", put).await.0, 201);
    let batch = format!("[{}]", (0..48).map(padded).collect::<Vec<_>>().join(","));
    let batched = cell.call(0, "POST", "/v1/streams/big/records:batch", batch.as_bytes());
    assert_eq!(batched.await.0, 200);
    let whole = "/v1/streams/big/records?maxBytes=8388608";
    let full = u64::try_from(cell.call(0, "GET", whole, b"").await.2.len()).unwrap();
    let held = never_read(&cell, 0, "GET", whole, b"").await;
    assert_eq!(
        settle_at(&cell, 0, (full, full)).await,
        (full, full),
        "held"
    );
    cell.state
        .admission
        .set_read_memory_capacity(full + MIB + 4096);
    let (status, _, page) = cell.call(0, "GET", RECORDS, b"").await;
    let records = serde_json::from_slice::<Vec<serde_json::Value>>(&page).map(|v| v.len());
    let len = u64::try_from(page.len()).unwrap();
    let paged = records.as_ref().is_ok_and(|n| (14..48).contains(n)) && len <= MIB;
    assert_eq!((status, paged), (200, true), "{records:?} records, {len} B");
    assert_eq!(
        settle_at(&cell, 0, (full, full)).await,
        (full, full),
        "the page left"
    );
    drop(held);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}
