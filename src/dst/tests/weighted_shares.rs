//! Weighted per-project shares (layout 6A package 1, DESIGN-6A §4.2 and
//! F15; the model of `~/.streams-k2/analysis/layout6a/pkg1/
//! FABLE-WEIGHTS.md`): on a cell shared k ways a project's ceilings are
//! its weights' share of each bound (`bound x weight / k`), its request
//! weight on the request axes and its byte weight on the byte-rate axes,
//! so a medium or large tenant is admitted beside unit tenants. Every
//! assertion reads the wire answers and the stored records.

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell, project};
use super::fixture_http::engine_shutdown;
use crate::auth::ceiling::SharedBounds;
use crate::project_policy::ProjectQuotas;

/// The shared-cell profile's measured append envelope (B/s): at k = 8 a
/// unit project's append ceiling is 257,500 B/s, a byte weight of 5 gives
/// 1,287,500.
const APPEND_ENVELOPE: u64 = 2_060_000;

/// One 120,000-byte record: ten of them are 1.2 MB, profile M's second.
fn record() -> String {
    format!("{{\"pad\":\"{}\"}}", "x".repeat(120_000))
}

/// Project `i` appends `n` records to `orders` as fast as it can: the
/// answers' statuses and error codes.
async fn burst(cell: &Cell, i: usize, n: usize) -> Vec<(u16, Option<String>)> {
    let body = record();
    let mut answers = Vec::with_capacity(n);
    for _ in 0..n {
        let path = "/v1/streams/orders/records";
        let (status, _, b) = cell.call(i, "POST", path, body.as_bytes()).await;
        answers.push((status, error_code(&b)));
    }
    answers
}

/// The records project `i` stored in `orders`.
async fn stored(cell: &Cell, i: usize) -> usize {
    let read = cell
        .json(i, "GET", "/v1/streams/orders/records?maxBytes=8388608")
        .await;
    read.as_array().map_or(0, Vec::len)
}

/// Profile M (request weight 2, byte weight 5) beside a unit project on
/// a cell shared 8 ways over the 2,060,000 B/s append envelope: M's second
/// of ten 120,000-byte appends (1.2 MB) is admitted whole, inside its
/// 1,287,500 B/s ceiling, while the unit project meets its unchanged
/// 257,500 B/s ceiling at its third append, `429 project_rate_limit`.
/// Before: every project's ceiling was bound / 8, so M's third append was
/// refused (79% of profile M's writes, docs/STREAMS-COST-LEVERS-2026-10.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_weighted_project_appends_at_its_weighted_ceiling_beside_a_unit_project() {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    spec.bounds = SharedBounds {
        append_bytes_per_sec: APPEND_ENVELOPE,
        ..SharedBounds::default()
    };
    spec.quotas = |i| ProjectQuotas {
        request_weight: if i == 0 { 2 } else { 0 },
        byte_weight: if i == 0 { 5 } else { 0 },
        ..ProjectQuotas::default()
    };
    let cell = open_cell(spec).await;
    for i in 0..2 {
        let created = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201, "project {i}");
    }
    let weighted = burst(&cell, 0, 10).await;
    assert_eq!(weighted, vec![(200, None); 10], "M's 1.2 MB second");
    let unit = burst(&cell, 1, 3).await;
    let refused = (429, Some("project_rate_limit".to_string()));
    assert_eq!(
        unit,
        vec![(200, None), (200, None), refused],
        "the unit project at 257,500 B/s"
    );
    assert_eq!(
        (stored(&cell, 0).await, stored(&cell, 1).await),
        (10, 2),
        "every admitted append stored once, the refused one not"
    );
    engine_shutdown(&cell.state).await;
}

/// The rig's shed line (MiB), as `fair_shed.rs` sets it.
const LINE_MB: u64 = 1000;

/// One small append of project `i` to `orders`: its status and code.
async fn append(cell: &Cell, i: usize) -> (u16, Option<String>) {
    let path = "/v1/streams/orders/records";
    let (status, _, b) = cell.call(i, "POST", path, br#"{"n":1}"#).await;
    (status, error_code(&b))
}

/// Over the shed line on a cell shared 8 ways, blame follows the weighted
/// share: project 0 at request weight 4 sent 100 of the cell's 250 recent
/// appends (40%, within its four eighths) and is spared, its appends
/// served; project 1 at weight 1 sent 150 (60%, over its one eighth) and
/// is refused `429 project_memory_pressure`. Before: blame compared every
/// project with 1/8, so project 0 was blamed and refused too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_the_shed_line_blame_follows_the_weighted_share() {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    spec.quotas = |i| ProjectQuotas {
        request_weight: if i == 0 { 4 } else { 0 },
        ..ProjectQuotas::default()
    };
    let cell = open_cell(spec).await;
    for i in 0..2 {
        let created = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201, "project {i}");
    }
    for (i, n) in [(0, 100), (1, 150)] {
        for _ in 0..n {
            assert_eq!(append(&cell, i).await, (200, None), "under the line");
        }
    }
    let admission = &cell.state.admission;
    admission.set_rss_shed_mb(LINE_MB);
    admission.record_rss_mb(LINE_MB + 1);
    let blamed = (429, Some("project_memory_pressure".to_string()));
    for round in 0..5 {
        assert_eq!(
            append(&cell, 0).await,
            (200, None),
            "weighted, round {round}"
        );
        assert_eq!(append(&cell, 1).await, blamed, "unit, round {round}");
    }
    assert_eq!(admission.snapshot().shed.blamed, 5, "the unit project's");
    admission.record_rss_mb(0);
    assert_eq!(
        (stored(&cell, 0).await, stored(&cell, 1).await),
        (105, 150),
        "every served append stored once, the refused ones not"
    );
    engine_shutdown(&cell.state).await;
}

const MIB: u64 = 1 << 20;
/// The shared-cell profile's project memory line (32 MiB).
const PROJECT_LINE: u64 = 32 * MIB;
/// The instance read memory of the 1 GiB profile: a quarter of its 500 MiB
/// shed line. A weighted line is held so two unit lines fit beside it:
/// 125 - 64 MiB, less a byte.
const READ_MEMORY: u64 = 125 * MIB;

/// Hold `bytes` of project `i`'s read bytes, as its unsent pages would:
/// its estimated pressure and the read ledger its line bounds both count
/// them.
async fn hold(cell: &Cell, i: usize, bytes: u64) -> crate::admission::read_memory::ReadHold {
    let pid = crate::tenant::ProjectId::new(&project(i)).unwrap();
    let project = cell.state.quotas.read_bytes(&pid);
    let held =
        crate::admission::read_memory::ReadHold::reserve(&cell.state.admission, project, 0, bytes);
    held.await.expect("room in the instance's read memory")
}

/// A project's memory line scales with its byte weight, held so two unit
/// lines fit beside it in the instance's read memory: at byte weight 5
/// project 0's line is min(5 x 32 MiB, 61 MiB less a byte), so with 40 MiB of its read bytes held its
/// append is served and its 8 MiB page read reserved, where the unit
/// project 1 with the same 40 MiB is refused `429
/// project_memory_pressure` on both (the read after its 2 s wait), and the
/// same for each one's catalog page (5.3 MiB on the same ledger); with
/// 70 MiB held project 0 is refused too, the cap. Before: every project's line was 32 MiB, so project 0 was
/// refused at 40 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_weighted_projects_memory_line_scales_leaving_two_unit_lines_beside_it() {
    let mut spec = CellSpec::open(2);
    spec.cli = |cli| cli.project_share_k = 8;
    spec.quotas = |i| ProjectQuotas {
        byte_weight: if i == 0 { 5 } else { 0 },
        ..ProjectQuotas::default()
    };
    let cell = open_cell(spec).await;
    let admission = &cell.state.admission;
    admission.set_project_memory_pressure_bytes(PROJECT_LINE);
    admission.set_read_memory_capacity(READ_MEMORY);
    for i in 0..2 {
        let created = cell.call(i, "PUT", "/v1/streams/orders", CREATE).await;
        assert_eq!(created.0, 201, "project {i}");
        assert_eq!(append(&cell, i).await, (200, None), "under the line");
    }
    let held = [
        hold(&cell, 0, 40 * MIB).await,
        hold(&cell, 1, 40 * MIB).await,
    ];
    let pressure = (429, Some("project_memory_pressure".to_string()));
    let page = "/v1/streams/orders/records?maxBytes=8388608";
    let read = async |i| {
        let (status, _, b) = cell.call(i, "GET", page, b"").await;
        (status, error_code(&b))
    };
    assert_eq!(append(&cell, 0).await, (200, None), "weighted at 40 MiB");
    assert_eq!(read(0).await, (200, None), "and its page reserved");
    assert_eq!(append(&cell, 1).await, pressure, "unit at 40 MiB");
    assert_eq!(read(1).await, pressure, "and its page refused");
    let listing = async |i| ask_listing(&cell, i).await;
    assert_eq!(listing(0).await, (200, None), "the weighted catalog page");
    assert_eq!(listing(1).await, pressure, "the unit project's refused");
    let more = hold(&cell, 0, 30 * MIB).await;
    assert_eq!(
        append(&cell, 0).await,
        pressure,
        "weighted at 70 MiB: the cap"
    );
    drop((held, more));
    assert_eq!(
        (stored(&cell, 0).await, stored(&cell, 1).await),
        (2, 1),
        "every refused append stored nothing"
    );
    engine_shutdown(&cell.state).await;
}

/// Project `i`'s catalog listing: its status and code.
async fn ask_listing(cell: &Cell, i: usize) -> (u16, Option<String>) {
    let (status, _, b) = cell.call(i, "GET", "/v1/streams", b"").await;
    (status, error_code(&b))
}
