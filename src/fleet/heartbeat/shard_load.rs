//! One owned shard's load in the heartbeat: the topology policy's input (S5, SCALING-DESIGN
//! rev 4 §4.3). Rates span the ack latency's window of the shard's commit-group timings.
use std::collections::{BTreeMap, HashMap};
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
    /// The part of `appends_per_s` the shard's busiest stream carried over the last beat: one
    /// entity's share of the appends axis, which a split cannot relieve (`hot_entity`).
    #[serde(default)]
    pub top_stream_appends_per_s: u64,
}

/// Each owned shard's resident streams' applied tails at the last beat, and so how much of the
/// shard's appends its busiest stream carried since the beat before (rev 4 §4.3's hot entity).
#[derive(Debug, Default)]
pub(super) struct TopStreams {
    tails: BTreeMap<String, HashMap<[u8; 16], u64>>,
    /// By shard: the busiest stream's growth over the last beat, and every stream's.
    shares: BTreeMap<String, (u64, u64)>,
}

impl TopStreams {
    /// Read each of `engines`' streams' tails now, and measure each shard's share against the
    /// last reading.
    pub(super) fn observe(&mut self, engines: &[(String, Arc<ShardEngine>)]) {
        let mut tails = BTreeMap::new();
        let mut shares = BTreeMap::new();
        for (prefix, engine) in engines {
            let now: HashMap<[u8; 16], u64> = engine.applied_tails().into_iter().collect();
            shares.insert(prefix.clone(), share(self.tails.get(prefix), &now));
            tails.insert(prefix.clone(), now);
        }
        (self.tails, self.shares) = (tails, shares);
    }

    /// The part of `appends_per_s` the busiest stream of `prefix` carried over the last beat.
    fn top_of(&self, prefix: &str, appends_per_s: u64) -> u64 {
        match self.shares.get(prefix) {
            Some(&(top, all)) if all > 0 => {
                let part = u128::from(appends_per_s) * u128::from(top) / u128::from(all);
                u64::try_from(part).unwrap_or(u64::MAX)
            }
            _ => 0,
        }
    }
}

/// The busiest stream's growth from `before` to `now`, and every stream's. A stream not in
/// `before` has no measured growth; with no `before` nothing is measured.
fn share(before: Option<&HashMap<[u8; 16], u64>>, now: &HashMap<[u8; 16], u64>) -> (u64, u64) {
    let Some(before) = before else {
        return (0, 0);
    };
    let (mut top, mut all) = (0_u64, 0_u64);
    for (hash, next) in now {
        if let Some(previous) = before.get(hash) {
            let grew = next.saturating_sub(*previous);
            top = top.max(grew);
            all = all.saturating_add(grew);
        }
    }
    (top, all)
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
    top: &TopStreams,
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
                top_stream_appends_per_s: top.top_of(prefix, appends_per_s),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{GroupTiming, TIMING_RING, TopStreams, rates, share};
    use std::collections::{BTreeMap, HashMap};

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

    /// The busiest stream's growth over the beat against every stream's: a stream new since
    /// the last beat, and a first beat, measure nothing.
    #[test]
    fn a_shards_busiest_stream_is_measured_by_its_tails_growth() {
        let tails = |pairs: &[(u8, u64)]| -> HashMap<[u8; 16], u64> {
            pairs.iter().map(|&(id, next)| ([id; 16], next)).collect()
        };
        let before = tails(&[(1, 100), (2, 50), (3, 10)]);
        let now = tails(&[(1, 160), (2, 70), (3, 10), (4, 900)]);
        assert_eq!(share(Some(&before), &now), (60, 80));
        assert_eq!(share(None, &now), (0, 0));
        let top = TopStreams {
            tails: BTreeMap::new(),
            shares: BTreeMap::from([("0".to_owned(), (60, 80)), ("1".to_owned(), (0, 0))]),
        };
        assert_eq!(top.top_of("0", 200), 150, "three quarters of 200");
        assert_eq!(top.top_of("1", 200), 0, "nothing measured");
        assert_eq!(top.top_of("10", 200), 0, "a shard not observed");
    }

    #[test]
    fn an_idle_shard_has_no_load() {
        assert_eq!(rates(&[], NOW, 15_000), (0, 0, 0));
    }
}
