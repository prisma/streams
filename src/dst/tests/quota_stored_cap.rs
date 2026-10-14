//! Stored-cap B4 (edge change #158): a project whose stored
//! bytes reach its `max_stored_bytes` quota is refused new appends with
//! `429 stored_bytes_limit`, by the figure the usage rollup publishes into
//! admission's table. Its neighbour is not; a restarted server refuses it
//! from its first append; a delete frees its quota once the drain and the
//! rollup step.

use super::fixture_cell::{Cell, CellSpec, NAMES, open_cell, project, roll_up};
use super::fixture_http::engine_shutdown;
use crate::project_policy::ProjectQuotas;
use std::collections::HashMap;

/// The cell's `PROJECT_STORED_BYTES_DEFAULT`: the quota of a project whose
/// feed sets none.
const DEFAULT: u64 = 262_144;
/// Q's quota, which its feed sets.
const Q_QUOTA: u64 = 1 << 20;
/// P appends incompressible records of 32 KiB, Q of 100 KiB.
const P_RECORD: usize = 32 * 1024;
const Q_RECORD: usize = 100 * 1024;
/// P's appends before the test gives up on a refusal: 768 KiB, three times
/// its quota. Each append's drain round spools one read batch, and the
/// spool's writer waits out its 300 s manifest poll once it holds 32 level-0
/// tables (`billing::telemetry_settings`), so the test stays under 32 rounds.
const P_APPENDS: u64 = 24;
const BYTES: &[u8] = br#"{"format":{"kind":"bytes"}}"#;
const P: usize = 0;
const Q: usize = 1;

/// Two projects of one cell shared 8 ways under production billing
/// (`BILLING_MODE=required`, `ROLLUP=1`): P's feed sets no stored quota,
/// so it takes the cell's default; Q's sets 1 MiB. The rig runs no
/// telemetry loop: the test steps the drain and the rollup itself.
fn spec() -> CellSpec {
    CellSpec {
        quotas: |i| ProjectQuotas {
            max_stored_bytes: if i == Q { Q_QUOTA } else { 0 },
            ..ProjectQuotas::default()
        },
        cli: |c| {
            c.project_stored_bytes_default = DEFAULT;
            c.billing_mode = "required".into();
            c.rollup = "1".into();
        },
        ..CellSpec::open(2)
    }
}

/// The cell with its read spool open, as required billing drains it.
async fn open(spec: CellSpec) -> Cell {
    let cell = open_cell(spec).await;
    crate::billing::open_read_spool(&cell.state).await.unwrap();
    cell
}

/// `len` bytes no compressor shrinks (xorshift64), one sequence per seed.
fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x.to_le_bytes()[3]
    };
    (0..len).map(|_| next()).collect()
}

type Answer = (u16, HashMap<String, String>, Vec<u8>);

/// Project `i` appends its record `n`, `len` incompressible bytes, to `name`.
async fn append(cell: &Cell, i: usize, name: &str, n: u64, len: usize) -> Answer {
    let seed = n * 2 + u64::from(i == Q);
    let path = format!("/v1/streams/{name}/records");
    let body = incompressible(seed, len);
    cell.call(i, "POST", &path, &body).await
}

/// Project `i`'s stored bytes in admission's table (0 before a figure).
fn stored(cell: &Cell, i: usize) -> u64 {
    let figures = cell.state.quotas.stored_table().figures();
    let ours = figures.iter().find(|f| f.project.as_str() == project(i));
    ours.map_or(0, |f| f.bytes)
}

/// What project `i`'s usage says each of its streams stores now, by name,
/// and what its project usage says the project stores.
async fn owned(cell: &Cell, i: usize, names: &[&str]) -> (Vec<u64>, u64) {
    let mut streams = Vec::new();
    for name in names {
        let usage = cell
            .json(i, "GET", &format!("/v1/streams/{name}/usage/current"))
            .await;
        streams.push(usage["ownedStoredBytesNow"].as_u64().unwrap());
    }
    let path = format!("/v1/projects/{}/usage", project(i));
    let usage = cell.json(i, "GET", &path).await;
    (streams, usage["ownedStoredBytesNow"].as_u64().unwrap())
}

/// P appends to its streams in turn, the drain and the rollup stepping
/// after each answer, until an append is refused: the refusal and the
/// figure P's table held after each accepted append.
async fn fill(cell: &Cell) -> (Answer, Vec<u64>) {
    let mut figures = Vec::new();
    for n in 0..P_APPENDS {
        let name = NAMES[usize::try_from(n).unwrap() % NAMES.len()];
        let answer = append(cell, P, name, n, P_RECORD).await;
        if answer.0 != 200 {
            return (answer, figures);
        }
        roll_up(&cell.state).await;
        figures.push(stored(cell, P));
    }
    panic!("P was never refused: {P_APPENDS} appends of {P_RECORD} bytes all answered 200");
}

/// The refusal's wire shape: `429 stored_bytes_limit`, not retryable, no
/// `retry-after`, its details the quota and the figure it was refused at.
fn assert_refused(answer: &Answer, at: u64, what: &str) {
    let (status, headers, body) = answer;
    let text = String::from_utf8_lossy(body);
    assert_eq!(*status, 429, "{what}: {text}");
    assert_eq!(headers.get("retry-after"), None, "{what}");
    let body: serde_json::Value = serde_json::from_slice(body).unwrap();
    let error = &body["error"];
    assert_eq!(error["code"], "stored_bytes_limit", "{what}");
    assert_eq!(error["retryable"], false, "{what}");
    let want = serde_json::Value::from_iter([
        ("dimension", serde_json::Value::from("stored_bytes")),
        ("limit", DEFAULT.into()),
        ("stored", at.into()),
    ]);
    assert_eq!(error["details"], want, "{what}");
}

/// The events admission emitted for project `i` reaching its cap.
fn reached(cell: &Cell, i: usize) -> usize {
    let events = cell.state.runtime.ops.recent(256);
    let ours = |e: &&crate::ops::OpsEvent| e.fields["project"] == project(i).as_str();
    let reached = events.iter().filter(ours);
    reached
        .filter(|e| e.event_type == "project_stored_cap_reached")
        .count()
}

/// Stored-cap B4 (T6): P, on the cell's 256 KiB default, appends 32 KiB
/// records until it is refused; it is refused exactly when the published
/// figure reaches the quota, which its streams' usage and its project
/// usage both state, and stays refused. Q, on 1 MiB, stores past P's quota
/// and is refused nothing. A restarted server refuses P on its first
/// append, from the index its rollup publishes at install; once P deletes
/// its largest stream and the drain and the rollup step, it appends again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_at_its_stored_cap_is_refused_and_its_neighbour_is_not() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let cell = open(spec()).await;
    for (i, name) in [P, Q].into_iter().flat_map(|i| NAMES.map(|n| (i, n))) {
        let path = format!("/v1/streams/{name}");
        assert_eq!(cell.call(i, "PUT", &path, BYTES).await.0, 201);
    }
    let (refusal, figures) = fill(&cell).await;
    let at = stored(&cell, P);
    assert_refused(&refusal, at, "P's first refusal");
    let before = figures[figures.len() - 2];
    let last = at - before;
    assert!(
        before < DEFAULT && (DEFAULT..DEFAULT + last).contains(&at),
        "refused at {at}, accepted at {before}, one append {last}"
    );
    let (streams, project_figure) = owned(&cell, P, &NAMES).await;
    assert_eq!((streams.iter().sum::<u64>(), project_figure), (at, at));
    let again = append(&cell, P, NAMES[0], P_APPENDS, P_RECORD).await;
    assert_refused(&again, at, "P's next append");
    let engaged = reached(&cell, P);
    assert_eq!(engaged, 1, "one event for the engage, none per refusal");
    for n in 0..4 {
        let (st, _, b) = append(&cell, Q, "orders", n, Q_RECORD).await;
        assert_eq!(st, 200, "Q's append {n}: {}", String::from_utf8_lossy(&b));
        roll_up(&cell.state).await;
    }
    let q = stored(&cell, Q);
    assert!(q > DEFAULT && q < Q_QUOTA, "Q stores {q}");
    let store = cell.state.data_store.clone();
    engine_shutdown(&cell.state).await;
    cell.state.rollup.get().unwrap().db.close().await.unwrap();
    let cell = open(CellSpec {
        store,
        incarnation: 1,
        ..spec()
    })
    .await;
    let first = append(&cell, P, NAMES[1], P_APPENDS + 1, P_RECORD).await;
    assert_refused(&first, at, "P's first append after the restart");
    let (streams, _) = owned(&cell, P, &NAMES).await;
    let largest = (0..NAMES.len()).max_by_key(|&s| streams[s]).unwrap();
    let path = format!("/v1/streams/{}", NAMES[largest]);
    assert_eq!(cell.call(P, "DELETE", &path, b"").await.0, 204);
    freed(&cell, at - streams[largest]).await;
    let kept = NAMES[(largest + 1) % NAMES.len()];
    let (st, _, b) = append(&cell, P, kept, P_APPENDS + 2, P_RECORD).await;
    assert_eq!(st, 200, "after the delete: {}", String::from_utf8_lossy(&b));
    engine_shutdown(&cell.state).await;
}

/// Step the drain and the rollup until P's table figure is `want`: a hard
/// delete closes its segments' gauges in the background.
async fn freed(cell: &Cell, want: u64) {
    for _ in 0..200 {
        roll_up(&cell.state).await;
        if stored(cell, P) == want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let figure = stored(cell, P);
    panic!("P's figure is {figure}, not {want}, after its delete");
}
