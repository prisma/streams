#![cfg(test)]
//! The L0 watermark's marks and hysteresis: derived from the shard log's
//! `l0_max_ssts`, never hard-coded.

use super::{L0Watermark, Marks, next_shed};

/// At the line the shard log runs with (32), the collector yields at 16 and
/// the shed engages at 24 and releases below 16.
#[test]
fn the_marks_of_the_production_line_are_16_24_and_16() {
    let line = crate::config::CliArgs::deterministic().l0_max_ssts;
    assert_eq!(line, 32, "the CLI default the shard log runs with");
    assert_eq!(
        Marks::of(line),
        Marks {
            line: 32,
            yield_at: 16,
            high: 24,
            low: 16,
        }
    );
}

/// Every line's marks follow it: the yield and the release at half the
/// line, the shed at three quarters, each strictly below the line, so the
/// shed engages before SlateDB's stall at any `l0_max_ssts`.
#[test]
fn the_marks_follow_the_line() {
    for (line, expected) in [
        (8, (4, 6, 4)),
        (12, (6, 9, 6)),
        (16, (8, 12, 8)),
        (48, (24, 36, 24)),
        (64, (32, 48, 32)),
    ] {
        let m = Marks::of(line);
        assert_eq!(
            (m.line, (m.yield_at, m.high, m.low)),
            (line, expected),
            "the marks of a line of {line}"
        );
        assert!(m.low < m.high && m.high < m.line, "{m:?}");
    }
    // A line below four is read as four: the marks stay distinct.
    assert_eq!(Marks::of(1), Marks::of(4));
    assert_eq!(
        Marks::of(4),
        Marks {
            line: 4,
            yield_at: 2,
            high: 3,
            low: 2,
        }
    );
}

/// The shed engages at the high mark, holds between the marks, and
/// releases only below the low mark.
#[test]
fn the_shed_engages_at_high_and_releases_below_low() {
    let m = Marks::of(32);
    let cases = [
        (false, 0, false),
        (false, 23, false),
        (false, 24, true),
        (false, 40, true),
        (true, 24, true),
        (true, 17, true),
        (true, 16, true),
        (true, 15, false),
        (true, 0, false),
    ];
    for (shed, count, expected) in cases {
        assert_eq!(
            next_shed(shed, count, m),
            expected,
            "shed {shed} at {count} L0s"
        );
    }
}

/// An engine's watermark: no decision before its first reading; the
/// collector yields from the yield mark; the shed engages once per
/// crossing, counts it, and releases below the low mark.
#[test]
fn an_engines_watermark_decides_from_its_readings() {
    let w = L0Watermark::default();
    w.set_line(8);
    assert_eq!(w.marks(), Marks::of(8));
    assert!(!w.collector_yields(), "no reading yet");
    assert!(!w.shedding());
    w.observe(3);
    assert!(!w.collector_yields(), "3 is below the yield mark (4)");
    w.observe(4);
    assert!(w.collector_yields(), "the yield mark");
    assert!(!w.shedding(), "4 is below the high mark (6)");
    w.observe(6);
    assert!(w.shedding(), "the high mark");
    w.observe(7);
    w.observe(4);
    assert!(w.shedding(), "held down to the low mark");
    w.observe(3);
    assert!(!w.shedding(), "released below the low mark");
    assert!(!w.collector_yields());
    w.observe(6);
    let v = w.json();
    assert_eq!(
        (
            &v["l0"],
            &v["max_seen"],
            &v["shedding"],
            &v["engages"],
            &v["collector_yields"],
            &v["high"],
            &v["line"],
        ),
        (
            &serde_json::json!(6),
            &serde_json::json!(7),
            &serde_json::json!(true),
            &serde_json::json!(2),
            &serde_json::json!(1),
            &serde_json::json!(6),
            &serde_json::json!(8),
        )
    );
}

/// An engine without a line of its own runs at the process's line, which
/// boot sets from `l0_max_ssts`: the CLI default, or the 48 the bootstrap
/// test holds for a moment.
#[test]
fn an_engine_without_its_own_line_reads_the_process_line() {
    let marks = L0Watermark::default().marks();
    assert!(
        marks == Marks::of(32) || marks == Marks::of(48),
        "{marks:?}"
    );
}
