//! A woken long-poll's read reservation (public-beta phase B item B8, D′
//! in place of the owner's Q1 (D)). A long-poll parks only at its stream's
//! tail, so the page a wake brings is what was appended while it waited:
//! it takes that payload again, plus 64 B a record and 4 KiB a page, at
//! most its admission budget, measured each time it tries for room, so
//! what is appended while it waits for room is taken too. A read from a
//! cursor behind the tail still reserves its whole budget at admission.
//!
//! The first leg is bound by its project's memory line. The others are
//! bound by the instance's read memory: a project whose held pages leave
//! it a few KiB under its line also latches its own writes (its pressure
//! counts its held pages and its parked waits), so the appends that wake
//! a waiter there would be refused, and writes never consult the
//! instance's read memory. A reservation takes one amount in both ledgers
//! at once (`ReadHold::take`).
//!
//! A client that "never reads" sends its request over a socket with a
//! 4 KiB receive buffer and leaves the response unread, so its page is
//! held until it leaves.

use super::fixture_cell::{Cell, CellSpec, open_cell};
use super::fixture_http::engine_shutdown;
use super::read_memory::{
    MIB, PAGE_BUDGET, RECORDS, append_to_lp, at_rest, big_stream, ledgers, never_read, padded,
    settle_at, until,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The shared-cell profile's project memory line.
const LINE: u64 = 32 * MIB;
const LP: &str = "/v1/streams/lp";
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
/// the waiter, which keeps waiting, holding nothing; a page's release
/// with room for both lets it take them, and its page is no larger than
/// what it took. The read memory never holds more than its capacity, and
/// its project's read bytes move with it. Measured at the wake, the
/// waiter takes 5 KiB at the first release and renders a 1 MiB page past
/// the capacity. Before: it took its whole 8 MiB budget again, which
/// neither release leaves, so it never rendered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_long_poll_held_out_of_room_never_takes_the_read_memory_past_its_capacity() {
    let cell = open_cell(CellSpec::open(1)).await;
    let page = big_stream(&cell, 0).await;
    create_lp(&cell).await;
    cell.state.admission.set_project_memory_pressure_bytes(LINE);
    let poll = format!("{LP}/records:long-poll?cursor=now&waitMs=20000");
    let waiter = never_read(&cell, 0, "GET", &poll, b"").await;
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
        let partial = at_rest(&cell, 0).await;
        drop(pages.pop());
        let room = at_rest(&cell, 0).await;
        done.store(true, Ordering::Relaxed);
        ((woke.0, read.0, read.2.len()), partial, room)
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
    let ((answers, partial, room), peak) = futures_util::future::join(scenario, sampled).await;
    let payload = kib_record().len() + (0..16).map(|n| padded(n).len()).sum::<usize>();
    let measure = u64::try_from(payload).unwrap() + 17 * RECORD_FRAMING + PAGE_FRAMING;
    let rendered = room.0.saturating_sub(2 * page);
    assert_eq!(
        (answers, partial),
        ((200, 200, kib_record().len() + 2), (3 * page, 3 * page)),
        "with room for the 1 KiB record but not for the 1 MiB appended while it waited, \
         the waiter holds nothing (capacity {capacity} B)"
    );
    assert!(
        room.0 == room.1 && rendered > 0 && rendered <= measure,
        "with room for both, the waiter holds its page of {rendered} B (it took {measure} B): \
         {room:?}"
    );
    assert!(
        peak.0 <= capacity && peak.1 <= capacity,
        "the ledgers peaked at {peak:?} against the read memory's capacity of {capacity} B"
    );
    drop((waiter, pages));
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
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
