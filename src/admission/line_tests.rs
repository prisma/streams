#![cfg(test)]
//! The RSS and memory lines a project's writes meet (layout 6A package 1):
//! the per-project memory line at a byte weight and the lease-ending
//! write's hard line.
use super::{AdmissionController, AdmissionKnobs, SubscriptionCapacity, WriteRefusal};

fn ctl(rss_shed_mb: u64) -> AdmissionController {
    AdmissionController::new(AdmissionKnobs {
        max_inflight: 0,
        per_stream_cap: 0,
        rss_shed_mb,
        project_memory_pressure_bytes: 0,
        project_memory_release_pct: 0,
        subscriptions: SubscriptionCapacity {
            effective: 0,
            configured: 0,
        },
        record_ceiling_bytes: 0,
    })
}

/// A project's memory line is the cell's at weight 1 (or 0), the line
/// times its byte weight above it, held so two unit lines fit beside it
/// in the instance's read memory (125 MiB at a 500 MiB shed line: 125 -
/// 64 MiB less a byte), never below the cell's line, unbounded without
/// a read budget, and 0 when off.
#[test]
fn a_projects_memory_line_scales_with_its_byte_weight_leaving_two_lines_beside_it() {
    const MIB: u64 = 1 << 20;
    let c = ctl(500);
    c.set_project_memory_pressure_bytes(32 * MIB);
    let lines = [0, 1, 2, 5].map(|w| c.project_memory_line(w));
    let cap = 125 * MIB - 64 * MIB - 1;
    assert_eq!(lines, [32 * MIB, 32 * MIB, cap, cap]);
    assert!(
        cap + 2 * 32 * MIB < 125 * MIB,
        "three projects never fill it"
    );
    c.set_read_memory_capacity(160 * MIB);
    assert_eq!(c.project_memory_line(2), 64 * MIB, "under the cap");
    c.set_read_memory_capacity(80 * MIB);
    assert_eq!(c.project_memory_line(5), 32 * MIB, "never below the line");
    c.set_read_memory_capacity(0);
    assert_eq!(c.project_memory_line(5), 160 * MIB, "no read budget");
    c.set_project_memory_pressure_bytes(0);
    assert_eq!(c.project_memory_line(5), 0, "off");
}

/// A write that only ends leases passes the RSS gate up to the hard line
/// (an eighth over the shed line) and is refused, counted as the RSS
/// shed, past it; with no line set it always passes.
#[test]
fn a_lease_ending_write_passes_the_rss_gate_until_the_hard_line() {
    let c = ctl(1000);
    c.record_rss_mb(1125);
    assert_eq!(c.admit_lease_ending_write(), Ok(()), "at the hard line");
    assert_eq!(
        c.admit_write_memory(),
        Err(WriteRefusal::MemoryPressure),
        "an append"
    );
    c.record_rss_mb(1126);
    assert_eq!(
        c.admit_lease_ending_write(),
        Err(WriteRefusal::MemoryPressure),
        "past it"
    );
    let s = c.snapshot().shed;
    assert_eq!((s.rss, s.total), (2, 2), "the append's and this one");
    let off = ctl(0);
    off.record_rss_mb(10_000);
    assert_eq!(off.admit_lease_ending_write(), Ok(()), "no line");
}
