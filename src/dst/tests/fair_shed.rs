//! Fair shedding over the RSS shed line (public-beta week 2026-10-17, the
//! lead's decision of 2026-10-20 20:00, edge change #184): on a
//! cell shared k ways, the project that sends more than 1/k of the verified
//! requests the cell was recently offered is refused first, and a project within its
//! share keeps its appends while the instance is at most an eighth over the
//! line (`admission::fair_shed`).

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell};
use super::fixture_http::engine_shutdown;

/// The rig's shed line (MiB): twice the production default, so that an
/// eighth over it (125 MiB) leaves the 65 MiB the leg keeps for the
/// absorber's reservation inside the hard line and still over the line.
const LINE_MB: u64 = 1000;
/// The flooder's appends under the line: far more than 8 times the
/// compliant project's whole leg (fewer than 12 requests).
const FLOOD: usize = 120;
/// Rounds over the line: one compliant append and one flooder append each.
const ROUNDS: usize = 5;

/// One append of `{"n": n}` by project `i`: its status, error code and
/// Retry-After.
async fn append(cell: &Cell, i: usize, n: usize) -> (u16, Option<String>, Option<String>) {
    let body = format!("{{\"n\":{n}}}");
    let path = "/v1/streams/orders/records";
    let (status, headers, body) = cell.call(i, "POST", path, body.as_bytes()).await;
    (
        status,
        error_code(&body),
        headers.get("retry-after").cloned(),
    )
}

/// Project `i`'s stored records' `n`s, in order.
async fn stored(cell: &Cell, i: usize) -> Vec<u64> {
    let read = cell.json(i, "GET", "/v1/streams/orders/records").await;
    let records = read.as_array().expect("a JSON stream reads as an array");
    records.iter().filter_map(|r| r["n"].as_u64()).collect()
}

fn refused(code: &str, retry: &str) -> (u16, Option<String>, Option<String>) {
    (429, Some(code.to_string()), Some(retry.to_string()))
}

/// On a cell shared 8 ways, over the shed line: the flooder (project 0,
/// 120 appends of the cell's last ones) is refused `429
/// project_memory_pressure`, Retry-After 2, on its appends, reads,
/// creates and listings, while the compliant project 1 is served each
/// append, exactly stored; inside line x 9/8 the compliant project is still
/// served, past it every append is shed `429 rate_limited` as before; under
/// the line both are served again. Before: the compliant project's first append over the
/// line was refused `429 rate_limited` with the flooder's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_the_project_over_its_share_is_refused_before_its_neighbour() {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    let cell = open_cell(spec).await;
    for i in 0..2 {
        assert_eq!(
            cell.call(i, "PUT", "/v1/streams/orders", CREATE).await.0,
            201
        );
    }
    for n in 0..FLOOD {
        assert_eq!(append(&cell, 0, n).await.0, 200, "under the line");
    }
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(LINE_MB);
    admission.record_rss_mb(LINE_MB + 1);
    for n in 0..ROUNDS {
        let served = append(&cell, 1, n).await;
        assert_eq!(
            served,
            (200, None, None),
            "the compliant project, round {n}"
        );
        let flood = append(&cell, 0, FLOOD + n).await;
        assert_eq!(flood, refused("project_memory_pressure", "2"), "round {n}");
    }
    let read = cell.call(0, "GET", "/v1/streams/orders/records", b"").await;
    assert_eq!(
        (read.0, error_code(&read.2)),
        (429, Some("project_memory_pressure".into()))
    );
    let create = cell.call(0, "PUT", "/v1/streams/more", CREATE).await;
    assert_eq!(
        (create.0, error_code(&create.2)),
        (429, Some("project_memory_pressure".into()))
    );
    let listing = cell.call(0, "GET", "/v1/streams", b"").await;
    assert_eq!(
        (listing.0, error_code(&listing.2)),
        (429, Some("project_memory_pressure".into()))
    );
    // The absorber's reservation adds to the pressure (tens of MiB while a
    // gather runs), so the leg stays 65 MiB inside the hard line; the exact
    // boundary is `fair_shed::rss_hard_line`'s unit test.
    admission.record_rss_mb(LINE_MB * 9 / 8 - 65);
    assert_eq!(
        append(&cell, 1, ROUNDS).await,
        (200, None, None),
        "inside line x 9/8"
    );
    admission.record_rss_mb(LINE_MB * 9 / 8 + 1);
    assert_eq!(
        append(&cell, 1, 99).await,
        refused("rate_limited", "2"),
        "past it"
    );
    assert_eq!(
        append(&cell, 0, 99).await,
        refused("project_memory_pressure", "2")
    );
    assert_eq!(
        admission.snapshot().shed.blamed,
        9,
        "the flooder's refusals"
    );
    admission.record_rss_mb(0);
    assert_eq!(
        append(&cell, 0, FLOOD + ROUNDS).await.0,
        200,
        "under the line"
    );
    assert_eq!(append(&cell, 1, ROUNDS + 1).await.0, 200, "under the line");
    let flooder: Vec<u64> = (0..=FLOOD + ROUNDS)
        .filter(|n| *n < FLOOD || *n == FLOOD + ROUNDS)
        .map(|n| u64::try_from(n).unwrap())
        .collect();
    assert_eq!(
        stored(&cell, 0).await,
        flooder,
        "the flooder's refused appends stored nothing"
    );
    let compliant: Vec<u64> = (0..=u64::try_from(ROUNDS).unwrap() + 1).collect();
    assert_eq!(
        stored(&cell, 1).await,
        compliant,
        "every compliant append stored once"
    );
    engine_shutdown(&cell.state).await;
}

/// The instance in-flight cap the in-flight leg sets (`ADMIT_MAX_INFLIGHT`).
const CAP: i64 = 8;

/// On a cell shared 8 ways, at the in-flight write cap (8; the cell's other
/// slots held, so each request is the ninth): the flooder (project 0, 120
/// appends of the cell's last ones) is refused `429 rate_limited`,
/// Retry-After 1, on its appends, reads and listings, while the compliant
/// project 1 is served each append, exactly stored, and its reads; up to cap
/// x 5/4 (10 active) the compliant project is still served, past it every
/// append is refused `429 rate_limited` as before; under the cap both are
/// served again. Before: the compliant project's first append at the cap was
/// refused with the flooder's, and the flooder's reads were served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_the_inflight_cap_the_project_over_its_share_is_refused_before_its_neighbour() {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    let cell = open_cell(spec).await;
    for i in 0..2 {
        let created = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201);
    }
    for n in 0..FLOOD {
        assert_eq!(append(&cell, 0, n).await.0, 200, "under the cap");
    }
    let admission = &cell.state.admission;
    admission.set_max_inflight(CAP);
    admission.add_inflight_for_test(CAP);
    for n in 0..ROUNDS {
        let served = append(&cell, 1, n).await;
        assert_eq!(
            served,
            (200, None, None),
            "the compliant project, round {n}"
        );
        let flood = append(&cell, 0, FLOOD + n).await;
        assert_eq!(flood, refused("rate_limited", "1"), "round {n}");
    }
    assert_eq!(stored(&cell, 1).await.len(), ROUNDS, "its reads are served");
    for path in ["/v1/streams/orders/records", "/v1/streams"] {
        let (status, headers, body) = cell.call(0, "GET", path, b"").await;
        let answer = (
            status,
            error_code(&body),
            headers.get("retry-after").cloned(),
        );
        assert_eq!(answer, refused("rate_limited", "1"), "{path}");
    }
    admission.add_inflight_for_test(CAP / 4 - 1);
    assert_eq!(
        append(&cell, 1, ROUNDS).await,
        (200, None, None),
        "at cap x 5/4"
    );
    admission.add_inflight_for_test(1);
    assert_eq!(
        append(&cell, 1, 99).await,
        refused("rate_limited", "1"),
        "past it"
    );
    assert_eq!(append(&cell, 0, 99).await, refused("rate_limited", "1"));
    let shed = admission.snapshot().shed;
    assert_eq!(
        (shed.blamed_inflight, shed.blamed),
        (8, 0),
        "the flooder's refusals at entry"
    );
    admission.add_inflight_for_test(-CAP - CAP / 4);
    assert_eq!(
        append(&cell, 0, FLOOD + ROUNDS).await.0,
        200,
        "under the cap"
    );
    assert_eq!(append(&cell, 1, ROUNDS + 1).await.0, 200, "under the cap");
    let flooder: Vec<u64> = (0..=FLOOD + ROUNDS)
        .filter(|n| *n < FLOOD || *n == FLOOD + ROUNDS)
        .map(|n| u64::try_from(n).unwrap())
        .collect();
    assert_eq!(
        stored(&cell, 0).await,
        flooder,
        "the flooder's refused appends stored nothing"
    );
    let compliant: Vec<u64> = (0..=u64::try_from(ROUNDS).unwrap() + 1).collect();
    assert_eq!(
        stored(&cell, 1).await,
        compliant,
        "every compliant append stored once"
    );
    engine_shutdown(&cell.state).await;
}
