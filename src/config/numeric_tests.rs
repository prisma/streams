//! Boundary contracts for environment coercion and diagnostic durations.
use super::{CliArgs, MapEnvironment, ServerConfig};
use std::time::Duration;

#[test]
fn scaler_coercions_preserve_the_existing_float_contract() {
    // Environment parsing intentionally accepts floating-point inputs before
    // the integer owners truncate and saturate. Defaults apply on parse errors.
    for (input, unsigned, signed) in [
        ("1.9", Some(1), Some(1)),
        ("-1.9", Some(0), Some(-1)),
        ("NaN", Some(0), Some(0)),
        ("inf", None, None),
        ("-inf", Some(0), Some(i64::MIN)),
    ] {
        let cfg = ServerConfig::load(
            CliArgs::deterministic(),
            &MapEnvironment::from([
                ("SCALE_EVAL_SECS", input),
                ("SCALE_HOT_EVALS", input),
                ("SCALE_COLD_EVALS", input),
                ("SCALE_COOLDOWN_SECS", input),
                ("MAX_SEGMENTS_PER_STREAM", input),
            ]),
        );
        assert_eq!(cfg.scaler.eval_secs, unsigned.unwrap_or(u64::MAX));
        assert_eq!(
            u64::from(cfg.scaler.hot_evals),
            unsigned.unwrap_or(u64::from(u32::MAX))
        );
        assert_eq!(cfg.scaler.cold_evals, cfg.scaler.hot_evals);
        assert_eq!(cfg.scaler.cooldown_secs, signed.unwrap_or(i64::MAX));
        assert_eq!(
            cfg.scaler.max_segments,
            unsigned
                .map(|v| usize::try_from(v).unwrap())
                .unwrap_or(usize::MAX)
        );
    }
    let cfg = ServerConfig::load(
        CliArgs::deterministic(),
        &MapEnvironment::from([
            ("SCALE_EVAL_SECS", "bad"),
            ("SCALE_HOT_EVALS", "bad"),
            ("SCALE_COLD_EVALS", "bad"),
            ("SCALE_COOLDOWN_SECS", "bad"),
            ("MAX_SEGMENTS_PER_STREAM", "bad"),
        ]),
    );
    assert_eq!(cfg.scaler.eval_secs, 10);
    assert_eq!(cfg.scaler.hot_evals, 2);
    assert_eq!(cfg.scaler.cold_evals, 180);
    assert_eq!(cfg.scaler.cooldown_secs, 600);
    assert_eq!(cfg.scaler.max_segments, 64);
}

#[test]
fn summary_durations_saturate_instead_of_wrapping_to_zero() {
    let mut cfg = ServerConfig::load(CliArgs::deterministic(), &MapEnvironment::empty());
    for (duration, expected) in [
        (Duration::ZERO, 0),
        (Duration::from_nanos(1_999_999), 1),
        (Duration::from_millis(u64::MAX), u64::MAX),
        (
            Duration::from_millis(u64::MAX) + Duration::from_millis(1),
            u64::MAX,
        ),
        (Duration::MAX, u64::MAX),
    ] {
        cfg.shard.open_deadline = duration;
        cfg.history.gc_interval = Some(duration);
        let summary = cfg.redacted_summary();
        assert_eq!(summary["shard"]["open_deadline_ms"], expected);
        assert_eq!(summary["history"]["gc_interval_ms"], expected);
    }
    cfg.history.gc_interval = None;
    assert!(cfg.redacted_summary()["history"]["gc_interval_ms"].is_null());
}
