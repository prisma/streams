//! Weighted per-project shares (layout 6A package 1, DESIGN-6A §4.2 and
//! F15; the model of `~/.streams-k2/analysis/layout6a/pkg1/
//! FABLE-WEIGHTS.md`): on a cell shared k ways a project's ceilings are
//! its weights' share of each bound (`bound x weight / k`), its request
//! weight on the request axes and its byte weight on the byte-rate axes,
//! so a medium or large tenant is admitted beside unit tenants. Every
//! assertion reads the wire answers and the stored records.

use super::fixture_cell::{CREATE, Cell, CellSpec, error_code, open_cell};
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
