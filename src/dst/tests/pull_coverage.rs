//! A consumer pull's coverage on a shared cell (public-beta pullcov lane,
//! the lead's decision of 2026-10-21, edge change #NEW-pullcov-1). On a
//! cell shared k > 1 ways a pull reserves and walks the shared cell's page,
//! 1 MiB, not a dedicated cell's 4 MiB, and once its walk has read, its
//! hold shrinks to the bytes the walk read until its batch renders. A
//! walk that stops at 1 MiB leases only what it covered: the pull answers
//! what it leased, and once those leases are settled the next pull
//! continues at the next record, so no message is skipped or lost.

use super::fixture_cell::{Cell, CellSpec, open_cell};
use super::fixture_http::engine_shutdown;
use super::read_memory::{MIB, never_read, padded, settle_at};
use crate::failpoints::{Fp, parked};
use std::time::{Duration, Instant};

/// The stream the held pulls walk: failpoints are keyed by its name, so
/// no other test here uses it.
const Q: &str = "pcq";
/// The stream the consumer drains.
const DRAINED: &str = "pcd";

/// A cell of `projects` projects shared eight ways.
fn shared(projects: usize) -> CellSpec {
    let mut spec = CellSpec::open(projects);
    spec.cli = |cli| cli.project_share_k = 8;
    spec
}

/// Project `i`'s JSON stream `stream` holding `records`, appended in one
/// batch without routing keys, and consumers `g0`.. of up to 48 records a
/// pull: the payload bytes the stream holds.
async fn queue(cell: &Cell, i: usize, stream: &str, records: &[String], consumers: usize) -> u64 {
    let path = format!("/v1/streams/{stream}");
    let created = cell
        .call(i, "PUT", &path, br#"{"format":{"kind":"json"}}"#)
        .await;
    assert_eq!(created.0, 201);
    let batch = format!("[{}]", records.join(","));
    let batch_path = format!("{path}/records:batch");
    let appended = cell.call(i, "POST", &batch_path, batch.as_bytes()).await;
    assert_eq!(appended.0, 200, "{}", String::from_utf8_lossy(&appended.2));
    for g in 0..consumers {
        let consumer = format!("{path}/consumers/g{g}");
        let created = cell
            .call(i, "PUT", &consumer, br#"{"maxBatchRecords":48}"#)
            .await;
        assert_eq!(created.0, 201, "{}", String::from_utf8_lossy(&created.2));
    }
    records
        .iter()
        .map(|r| u64::try_from(r.len()).unwrap())
        .sum()
}

/// A pull's answer: (status, the error code, its messages' `n`s, their
/// lease tokens).
type Pulled = (u16, Option<String>, Vec<u64>, Vec<String>);

/// Consumer `g` of project `i`'s `stream` pulls up to 48 messages, without
/// waiting.
async fn pull(cell: &Cell, i: usize, stream: &str, g: usize) -> Pulled {
    let path = format!("/v1/streams/{stream}/consumers/g{g}:pull");
    let (status, _, body) = cell.call(i, "POST", &path, br#"{"max":48}"#).await;
    let answer: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = answer["error"]["code"].as_str().map(str::to_string);
    let messages = answer["messages"].as_array().cloned().unwrap_or_default();
    let ns = messages
        .iter()
        .filter_map(|m| m["value"]["n"].as_u64())
        .collect();
    let tokens = messages
        .iter()
        .filter_map(|m| m["leaseToken"].as_str().map(str::to_string))
        .collect();
    (status, code, ns, tokens)
}

/// Eight compliant pulls, two consumers in each of four projects, of
/// streams of 32 records of about 1 KiB, while a never-read page of 3 MiB
/// holds the instance's read memory to 3 MiB of room: each reserves 1 MiB,
/// shrinks to the 32,534 B its walk read, and all eight reach their Receive
/// together (held there by `PullBeforeReceive`) and lease all 32 records.
/// Before: each reserved 4 MiB, which never fit beside the page; the eight
/// waited 2 s and were refused 503 `read_memory_busy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_cells_pulls_reserve_one_mib_and_hold_what_their_walks_read() {
    let cell = open_cell(shared(4)).await;
    cell.state
        .admission
        .set_project_memory_pressure_bytes(1 << 30);
    let records: Vec<String> = (0..32)
        .map(|n| format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(1000)))
        .collect();
    let mut read = 0;
    for i in 0..4 {
        read = queue(&cell, i, Q, &records, 2).await;
    }
    let put = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(cell.call(0, "PUT", "/v1/streams/big", put).await.0, 201);
    let batch = format!("[{}]", (0..48).map(padded).collect::<Vec<_>>().join(","));
    let batched = cell.call(0, "POST", "/v1/streams/big/records:batch", batch.as_bytes());
    assert_eq!(batched.await.0, 200);
    let whole = "/v1/streams/big/records?maxBytes=8388608";
    let full = u64::try_from(cell.call(0, "GET", whole, b"").await.2.len()).unwrap();
    let held = never_read(&cell, 0, "GET", whole, b"").await;
    assert_eq!(settle_at(&cell, 0, (full, full)).await, (full, full));
    cell.state
        .admission
        .set_read_memory_capacity(full + 3 * MIB);
    let before = parked(Fp::PullBeforeReceive, Q);
    crate::failpoints::park_pull_before_receive(Q);
    let pulls = futures_util::future::join_all((0..8).map(|n| pull(&cell, n / 2, Q, n % 2)));
    let watch = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while parked(Fp::PullBeforeReceive, Q) - before < 8 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let at_receive = (
            parked(Fp::PullBeforeReceive, Q) - before,
            cell.state.admission.read_memory().0,
        );
        crate::failpoints::release_pull_before_receive(Q);
        at_receive
    };
    let (answers, at_receive) = futures_util::future::join(pulls, watch).await;
    let answers: Vec<(u16, Option<String>, usize)> = answers
        .into_iter()
        .map(|(status, code, ns, _)| (status, code, ns.len()))
        .collect();
    assert_eq!(
        (at_receive, answers),
        ((8, full + 8 * read), vec![(200, None, 32); 8]),
        "(pulls at their Receive, the instance's read memory), answers"
    );
    drop(held);
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}

/// On a shared cell a pull's walk covers 1 MiB from the consumer's cursor:
/// of 48 records of 64 KiB, a pull of 48 leases the first 15, a second
/// pull before they are settled finds the rest uncovered and leases
/// nothing, and a pull after each acknowledgement continues at the next
/// record, so the consumer receives every record once, in order, in four
/// batches, and a last pull finds nothing left. Before: the first pull
/// leased all 48.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_cells_pull_leases_what_one_mib_covers_and_the_next_continues_after_it() {
    let cell = open_cell(shared(1)).await;
    let records: Vec<String> = (0..48).map(padded).collect();
    queue(&cell, 0, DRAINED, &records, 1).await;
    let settle = format!("/v1/streams/{DRAINED}/consumers/g0:settle");
    let mut batches = Vec::new();
    let mut second = None;
    for round in 0..5 {
        let (status, code, ns, tokens) = pull(&cell, 0, DRAINED, 0).await;
        assert_eq!((status, code), (200, None), "round {round}");
        if round == 0 {
            second = Some(pull(&cell, 0, DRAINED, 0).await);
        }
        if tokens.is_empty() {
            break;
        }
        let acks: Vec<String> = tokens
            .iter()
            .map(|t| format!(r#"{{"leaseToken":"{t}"}}"#))
            .collect();
        let body = format!(r#"{{"acks":[{}]}}"#, acks.join(","));
        let (status, _, settled) = cell.call(0, "POST", &settle, body.as_bytes()).await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&settled));
        batches.push(ns);
    }
    let lengths: Vec<usize> = batches.iter().map(Vec::len).collect();
    let received: Vec<u64> = batches.concat();
    assert_eq!(
        (second, lengths, received),
        (
            Some((200, None, Vec::new(), Vec::new())),
            vec![15, 15, 15, 3],
            (0..48).collect::<Vec<u64>>()
        ),
        "(the pull before the first batch was settled, the batches, every record received)"
    );
    assert_eq!(settle_at(&cell, 0, (0, 0)).await, (0, 0));
    engine_shutdown(&cell.state).await;
}
