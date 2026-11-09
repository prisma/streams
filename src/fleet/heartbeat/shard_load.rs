//! One owned shard's load in the heartbeat: the topology policy's input (S5, SCALING-DESIGN
//! rev 4 §4.3). Rates span the ack latency's window of the shard's commit-group timings.
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::shard::{GroupTiming, ShardEngine};

/// The groups a shard's timing ring keeps (`ShardEngine::timings`): a full ring may cover less
/// than the window, and then its rates span what it covers.
const TIMING_RING: usize = 128;

/// One owned shard's load. Every field defaults, so a beat from a version without it reads as
/// an idle shard.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardLoad {
    pub prefix: String,
    /// Records committed per second.
    #[serde(default)]
    pub appends_per_s: u64,
    #[serde(default)]
    pub append_bytes_per_s: u64,
    /// The committer's serial work (encode and write) as a percent of the span: its occupancy.
    #[serde(default)]
    pub committer_busy_pct: u64,
    /// Stream handles resident, of the shard's `HANDLE_MAX_RESIDENT` (0: eviction off).
    #[serde(default)]
    pub resident_streams: u64,
    #[serde(default)]
    pub handle_max_resident: u64,
}

/// What `groups` committed in the `window_ms` before `now_ms`: records and bytes per second,
/// and the committer's busy percent.
fn rates(groups: &[GroupTiming], now_ms: i64, window_ms: i64) -> (u64, u64, u64) {
    let cutoff = now_ms - window_ms;
    let oldest = groups
        .iter()
        .map(|group| group.ts_ms)
        .min()
        .unwrap_or(cutoff);
    let start = if groups.len() >= TIMING_RING {
        oldest.max(cutoff)
    } else {
        cutoff
    };
    let span_ms = u64::try_from(now_ms - start).unwrap_or(0).max(1);
    let (mut records, mut bytes, mut busy_us) = (0_u64, 0_u64, 0_u64);
    for group in groups.iter().filter(|group| group.ts_ms >= cutoff) {
        records += u64::from(group.records);
        bytes += group.bytes;
        busy_us += u64::from(group.encode_us) + u64::from(group.write_us);
    }
    (
        records * 1_000 / span_ms,
        bytes * 1_000 / span_ms,
        busy_us / (span_ms * 10),
    )
}

/// The load of each of `engines`, at `now_ms`, against the handle cap `handle_max_resident`.
pub(super) fn of_engines(
    engines: &[(String, Arc<ShardEngine>)],
    handle_max_resident: usize,
    now_ms: i64,
    window_ms: i64,
) -> Vec<ShardLoad> {
    engines
        .iter()
        .map(|(prefix, engine)| {
            // A poisoned ring may hold a half-recorded group: the shard reads as idle.
            let groups: Vec<GroupTiming> = engine
                .timings
                .lock()
                .map(|ring| ring.iter().copied().collect())
                .unwrap_or_default();
            let (appends_per_s, append_bytes_per_s, committer_busy_pct) =
                rates(&groups, now_ms, window_ms);
            ShardLoad {
                prefix: prefix.clone(),
                appends_per_s,
                append_bytes_per_s,
                committer_busy_pct,
                resident_streams: engine.resident_streams() as u64,
                handle_max_resident: handle_max_resident as u64,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{GroupTiming, TIMING_RING, rates};

    const NOW: i64 = 1_000_000;

    /// A group committed at `ts_ms` of `records` records and `bytes` bytes, whose committer
    /// spent `busy_us` encoding and writing it.
    fn group(ts_ms: i64, records: u32, bytes: u64, busy_us: u32) -> GroupTiming {
        GroupTiming {
            ts_ms,
            queue_wait_us: 0,
            encode_us: busy_us / 2,
            write_us: busy_us - busy_us / 2,
            durable_wait_us: 0,
            reqs: records,
            records,
            bytes,
        }
    }

    #[test]
    fn a_shards_rates_span_the_window_and_leave_older_groups_out() {
        let groups = [
            group(NOW - 15_001, 9_000, 9_000, 9_000_000),
            group(NOW - 15_000, 1_500, 30_000, 1_500_000),
            group(NOW - 1, 1_500, 30_000, 1_500_000),
        ];
        assert_eq!(rates(&groups, NOW, 15_000), (200, 4_000, 20));
    }

    #[test]
    fn a_full_ring_newer_than_the_window_spans_what_it_covers() {
        let groups: Vec<GroupTiming> = (0..TIMING_RING as i64)
            .map(|i| group(NOW - 5_000 + i * 39, 100, 1_000, 10_000))
            .collect();
        // 128 groups over the 5 s the ring covers: 12,800 records, 128,000 bytes, 1.28 s busy.
        assert_eq!(rates(&groups, NOW, 15_000), (2_560, 25_600, 25));
        let one_short = &groups[1..];
        assert_eq!(
            rates(one_short, NOW, 15_000),
            (846, 8_466, 8),
            "a ring not full spans the window"
        );
    }

    #[test]
    fn an_idle_shard_has_no_load() {
        assert_eq!(rates(&[], NOW, 15_000), (0, 0, 0));
    }
}
