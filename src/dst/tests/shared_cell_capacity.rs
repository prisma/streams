//! Shared-cell capacity (the capacity review of 2026-10-07): what one
//! project inside its quotas can hold of what a shared cell's projects
//! share, besides read memory (`read_memory`). Each test pins one bound:
//! a write's buffered bodies share their project's memory line (C3).

use super::fixture_cell::{Cell, CellSpec, open_cell, project};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::PRISMA_KEY;
use crate::tenant::ProjectId;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MIB: u64 = 1 << 20;
/// The page budget a read without `maxBytes` reserves.
const PAGE_BUDGET: u64 = 8 * MIB;
const CREATE: &[u8] = br#"{"format":{"kind":"json"}}"#;

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
