//! The shipped retention geometry: the cell ceiling and the project
//! allowance a feed budget takes from its configuration.
#![cfg(test)]
use crate::config::SseConfig;
use crate::sse::feed::{FeedMemoryBudget, configured_project_cap};
use std::sync::atomic::Ordering;

const CELL: u64 = 64 * 1024 * 1024;

fn caps(cfg: &SseConfig) -> (u64, u64) {
    let budget = FeedMemoryBudget::from_config(cfg);
    (
        budget.max.load(Ordering::Relaxed),
        budget.project_cap.load(Ordering::Relaxed),
    )
}

/// The shipped ring is 1 MiB per feed and the shipped cell 64 MiB.
#[test]
fn the_shipped_ring_and_cell_are_the_certified_ones() {
    let cfg = SseConfig::default();
    assert_eq!(crate::sse::budget::feed_ring_bytes(&cfg), 1024 * 1024);
    assert_eq!(crate::sse::budget::feed_total_cap(&cfg), CELL);
}

/// Unset, one project may hold half of the cell: 32 MiB of the shipped 64.
#[test]
fn an_unset_project_allowance_is_half_of_the_cell() {
    let cfg = SseConfig::default();
    assert_eq!(configured_project_cap(&cfg, CELL), Ok(32 * 1024 * 1024));
    assert_eq!(configured_project_cap(&cfg, 10), Ok(5));
    assert_eq!(caps(&cfg), (CELL, 32 * 1024 * 1024));
}

/// A configured allowance is taken as written, whitespace trimmed.
#[test]
fn a_configured_project_allowance_is_taken_as_written() {
    let cfg = SseConfig {
        feed_project_bytes_raw: Some(" 12345 ".into()),
        ..SseConfig::default()
    };
    assert_eq!(configured_project_cap(&cfg, CELL), Ok(12_345));
    assert_eq!(caps(&cfg), (CELL, 12_345));
}

/// Outside the release posture an allowance that does not parse falls
/// back to half of the cell; the strict form names the value.
#[test]
fn an_unparseable_project_allowance_falls_back_to_half_of_the_cell() {
    let cfg = SseConfig {
        feed_total_bytes: 1_000,
        feed_project_bytes_raw: Some("32 MiB".into()),
        ..SseConfig::default()
    };
    assert_eq!(
        configured_project_cap(&cfg, 1_000),
        Err(r#"SSE_FEED_PROJECT_BYTES="32 MiB" does not parse as a byte count"#.to_string())
    );
    assert_eq!(caps(&cfg), (1_000, 500));
}
