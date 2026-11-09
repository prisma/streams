#![cfg(test)]
//! The topology policy's flags in the pinned CLI surface (S5, `cli_surface_is_pinned`).

pub(super) const EXPECTED: &[(&str, &str, &str)] = &[
    ("shard-split-line-pct", "SHARD_SPLIT_LINE_PCT", "70"),
    (
        "shard-split-sustain-secs",
        "SHARD_SPLIT_SUSTAIN_SECS",
        "600",
    ),
    ("shard-emergency-line-pct", "SHARD_EMERGENCY_LINE_PCT", "90"),
    (
        "shard-emergency-sustain-secs",
        "SHARD_EMERGENCY_SUSTAIN_SECS",
        "45",
    ),
    ("shard-merge-line-pct", "SHARD_MERGE_LINE_PCT", "25"),
    (
        "shard-merge-sustain-secs",
        "SHARD_MERGE_SUSTAIN_SECS",
        "21600",
    ),
    (
        "shard-split-reversal-min-secs",
        "SHARD_SPLIT_REVERSAL_MIN_SECS",
        "43200",
    ),
    (
        "shard-topology-cooldown-secs",
        "SHARD_TOPOLOGY_COOLDOWN_SECS",
        "60",
    ),
    ("shard-beat-stale-secs", "SHARD_BEAT_STALE_SECS", "30"),
    ("shard-appends-ceiling", "SHARD_APPENDS_CEILING", "5000"),
];
