//! The shard's queue-state gauges and its categorised bound (S3b-DESIGN
//! §8.2-§8.4, SCALING-DESIGN rev 4 §6.4; edge change #246). The shard's
//! queue state is bounded by `BOUND_BYTES` in five slices, one per
//! category; each slice counts only its own category, so no category can
//! take another's room:
//!
//! | category | rows | slice |
//! |---|---|---|
//! | active | open deliveries (leased, retrying) and their lease or retry entries | 30% |
//! | failed | failed deliveries and their failure entries | 20% |
//! | delayed | scheduled entries, their by-offset rows and inline pages | 20% |
//! | dedup | dedup entries and their expiry entries | 20% |
//! | cleanup | range rows | 10% |
//!
//! At a category's line the refusal is per container first, then per queue:
//! a container at or over its share of the slice (the slice ÷ the containers
//! with state in it), and within it a queue at or over its share of the
//! container's bytes, has its claims lease nothing new (active, failed) or
//! its scheduled sends (delayed) or sends with fresh dedup keys (dedup)
//! refused. Cleanup refuses nothing: what creates it is never refused.
//!
//! A queue's bytes per category follow from its state row's exact gauges
//! with one fixed weight per message (each row's key, value and a 32-byte
//! entry overhead, rounded up), and its range rows' stored bytes. The
//! committer, the only writer, keeps the shard's and every container's sums
//! and puts them as `'N'` rows in the batch that changes them: `'N' ‖
//! 16+category` holds the shard's bytes, `'N' ‖ 21` the rows' generation,
//! `'N' ‖ 32+category ‖ container` a container's bytes and its queues with
//! state. They are read once, by the engine's first queue operation.
//!
//! A group's moves are staged apart from the written sums and put in its
//! batch at the next generation; the next group's first queue operation
//! takes them as written if the generation row holds that generation, and
//! drops them otherwise (a group that was rejected wrote nothing), so the
//! sums never hold a move the shard log does not.
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// The shard's queue-state bound (provisional; S3b's collector-pass
/// measurement sets it, SCALING-DECISIONS Q11).
pub(crate) const BOUND_BYTES: u64 = 2 << 30;
/// Each category's slice of the bound, in percent.
pub(crate) const SLICE_PERCENT: [u64; CATEGORIES] = [30, 20, 20, 20, 10];
pub(crate) const CATEGORIES: usize = 5;
pub(crate) const ACTIVE: usize = 0;
pub(crate) const FAILED: usize = 1;
pub(crate) const DELAYED: usize = 2;
pub(crate) const DEDUP: usize = 3;
pub(crate) const CLEANUP: usize = 4;

/// Bytes one message holds per category: an open delivery's row (30-byte
/// key, 87-byte value) and its lease or retry entry (38-byte key); a
/// failed one's row and its two failure entries; a scheduled entry (38-byte
/// key, 12-byte value) and its by-offset row (30 + 8); a dedup entry (38 +
/// 17) and its expiry entry (46). Each row adds 32 bytes; rounded up to 8.
pub(crate) const W_ACTIVE: u64 = 224;
pub(crate) const W_FAILED: u64 = 296;
pub(crate) const W_DELAYED: u64 = 160;
pub(crate) const W_DEDUP: u64 = 168;
/// A range row's stored bytes beyond its key and value.
pub(crate) const ROW_OVERHEAD: u64 = 32;

const TAG: u8 = crate::keyspace::tag::QUEUE_GAUGE;
const SHARD_ID: u8 = 16;
/// After the shard's five categories.
const GENERATION_ID: u8 = 21;
const CONTAINER_ID: u8 = 32;
/// After the containers' five categories.
const CONTAINER_STOP: u8 = 37;
/// The end of the ids gauges own.
const CONTAINER_END: u8 = CONTAINER_ID + 16;

/// A queue's bytes per category, from its gauges.
pub(crate) fn categories(s: &crate::native_queue::rows::StateRow) -> [u64; CATEGORIES] {
    [
        s.leased.saturating_add(s.retrying).saturating_mul(W_ACTIVE),
        s.failed.saturating_mul(W_FAILED),
        s.scheduled
            .saturating_mul(W_DELAYED)
            .saturating_add(s.inline_bytes),
        s.dedup_entries.saturating_mul(W_DEDUP),
        s.range_bytes,
    ]
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ContainerTotals {
    bytes: [u64; CATEGORIES],
    /// Its queues with state in each category.
    queues: [u64; CATEGORIES],
}

/// The shard's sums at one generation.
#[derive(Debug, Default)]
struct Totals {
    generation: u64,
    shard: [u64; CATEGORIES],
    /// The containers with state in each category.
    with_state: [u64; CATEGORIES],
    containers: BTreeMap<[u8; 16], ContainerTotals>,
}

/// One group's moves over the written sums: the shard's sums and every
/// container it touched, whole.
#[derive(Debug)]
struct Staged {
    generation: u64,
    shard: [u64; CATEGORIES],
    with_state: [u64; CATEGORIES],
    containers: BTreeMap<[u8; 16], ContainerTotals>,
}

#[derive(Debug, Default)]
struct Sums {
    /// As the shard log holds them.
    written: Totals,
    /// A group's moves not yet known to be written.
    staged: Option<Staged>,
}

/// The shard's queue gauges (part of the engine's layout state).
#[derive(Default)]
pub(crate) struct QueueGauges {
    sums: Mutex<Option<Sums>>,
    /// Tests: a bound in place of `BOUND_BYTES` (0: the constant).
    bound_for_tests: AtomicU64,
}

impl QueueGauges {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Sums>> {
        // Every update replaces whole values: a poisoned value is whole.
        self.sums
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the persisted gauges were read.
    pub(crate) fn loaded(&self) -> bool {
        self.lock().is_some()
    }

    /// Tests: as a new engine finds them, unread.
    #[cfg(test)]
    pub(crate) fn unload_for_tests(&self) {
        *self.lock() = None;
    }

    /// Take the persisted `'N'` rows (`rows`: key and value, in key
    /// order); a row that does not decode is left out (a later change
    /// rewrites it).
    pub(crate) fn load(&self, rows: &[(Vec<u8>, Vec<u8>)]) {
        let mut t = Totals::default();
        let prefix = prefix().len();
        for (k, v) in rows {
            let (Some(&id), Some((first, second))) = (k.get(prefix), read_u64s(v)) else {
                continue;
            };
            let container = k
                .get(prefix + 1..)
                .and_then(|p| <[u8; 16]>::try_from(p).ok());
            match (id, container) {
                (GENERATION_ID, None) if k.len() == prefix + 1 => t.generation = first,
                (SHARD_ID..GENERATION_ID, None) if k.len() == prefix + 1 => {
                    t.shard[usize::from(id - SHARD_ID)] = first;
                }
                (CONTAINER_ID..CONTAINER_STOP, Some(container)) => {
                    let p = t.containers.entry(container).or_default();
                    p.bytes[usize::from(id - CONTAINER_ID)] = first;
                    p.queues[usize::from(id - CONTAINER_ID)] = second;
                }
                _ => {}
            }
        }
        for p in t.containers.values() {
            for cat in 0..CATEGORIES {
                t.with_state[cat] += u64::from(p.bytes[cat] > 0);
            }
        }
        *self.lock() = Some(Sums {
            written: t,
            staged: None,
        });
    }

    pub(crate) fn bound(&self) -> u64 {
        match self.bound_for_tests.load(Ordering::Relaxed) {
            0 => BOUND_BYTES,
            n => n,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_bound_for_tests(&self, bytes: u64) {
        self.bound_for_tests.store(bytes, Ordering::Relaxed);
    }

    /// The generation of the staged moves, if a group staged any.
    pub(crate) fn staged_generation(&self) -> Option<u64> {
        self.lock()
            .as_ref()
            .and_then(|s| s.staged.as_ref())
            .map(|s| s.generation)
    }

    /// The staged moves' group is over: they become the written sums when
    /// its batch was written, and are dropped when it was not.
    pub(crate) fn settle(&self, written: bool) {
        let mut guard = self.lock();
        let Some(sums) = guard.as_mut() else {
            return;
        };
        let Some(staged) = sums.staged.take() else {
            return;
        };
        if !written {
            return;
        }
        let w = &mut sums.written;
        w.generation = staged.generation;
        w.shard = staged.shard;
        w.with_state = staged.with_state;
        for (container, p) in staged.containers {
            if p == ContainerTotals::default() {
                w.containers.remove(&container);
            } else {
                w.containers.insert(container, p);
            }
        }
    }

    /// A queue of `container` went from `before` to `after` bytes per
    /// category (`moved`): the group's staged sums move, and the `'N'` rows
    /// that changed are returned to be put in its batch, with the
    /// generation. The cleanup slice refuses nothing (S3b-REVIEW D3): past
    /// 80% of it, `shard` logs an alarm.
    pub(crate) fn apply(
        &self,
        shard: &str,
        container: [u8; 16],
        moved: ([u64; CATEGORIES], [u64; CATEGORIES]),
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (before, after) = moved;
        let alarm = self.bound() / 100 * SLICE_PERCENT[CLEANUP] / 10 * 8;
        let mut guard = self.lock();
        let Sums { written, staged } = guard.get_or_insert_with(Sums::default);
        let s = staged.get_or_insert_with(|| Staged {
            generation: written.generation.wrapping_add(1),
            shard: written.shard,
            with_state: written.with_state,
            containers: BTreeMap::new(),
        });
        let cleanup = s.shard[CLEANUP];
        let p = s.containers.entry(container).or_insert_with(|| {
            written
                .containers
                .get(&container)
                .cloned()
                .unwrap_or_default()
        });
        let mut out = Vec::new();
        for cat in (0..CATEGORIES).filter(|&cat| before[cat] != after[cat]) {
            let had = p.bytes[cat] > 0;
            s.shard[cat] = s.shard[cat]
                .saturating_sub(before[cat])
                .saturating_add(after[cat]);
            p.bytes[cat] = p.bytes[cat]
                .saturating_sub(before[cat])
                .saturating_add(after[cat]);
            p.queues[cat] = (p.queues[cat] + u64::from(after[cat] > 0))
                .saturating_sub(u64::from(before[cat] > 0));
            s.with_state[cat] =
                (s.with_state[cat] + u64::from(p.bytes[cat] > 0)).saturating_sub(u64::from(had));
            out.push((shard_key(cat), s.shard[cat].to_le_bytes().to_vec()));
            let mut v = p.bytes[cat].to_le_bytes().to_vec();
            v.extend_from_slice(&p.queues[cat].to_le_bytes());
            out.push((container_key(cat, &container), v));
        }
        if !out.is_empty() {
            out.push((generation_key(), s.generation.to_le_bytes().to_vec()));
        }
        if crosses(alarm, cleanup, s.shard[CLEANUP]) {
            tracing::warn!(
                shard,
                bytes = s.shard[CLEANUP],
                alarm,
                "queue state: the cleanup slice is past 80%; it refuses nothing"
            );
        }
        out
    }

    /// Whether a queue of `container` holding `queue` bytes of `cat` is
    /// refused at the category's line: the shard at its slice, the container
    /// at or over its share of it, the queue at or over its share of the
    /// container's bytes (so the container's largest queue always is). The
    /// group's staged sums count.
    pub(crate) fn limited(&self, container: [u8; 16], cat: usize, queue: u64) -> bool {
        if cat == CLEANUP || queue == 0 {
            return false;
        }
        let slice = self.bound() / 100 * SLICE_PERCENT[cat];
        let guard = self.lock();
        let Some(sums) = guard.as_ref() else {
            return false;
        };
        let w = &sums.written;
        let (shard, with_state, p) = match &sums.staged {
            Some(s) => (
                s.shard,
                s.with_state,
                s.containers
                    .get(&container)
                    .or_else(|| w.containers.get(&container)),
            ),
            None => (w.shard, w.with_state, w.containers.get(&container)),
        };
        let Some(p) = p.filter(|_| shard[cat] >= slice) else {
            return false;
        };
        p.bytes[cat] >= slice / with_state[cat].max(1)
            && queue >= p.bytes[cat] / p.queues[cat].max(1)
    }

    /// The shard's bytes per category, the group's staged moves counted.
    #[cfg(test)]
    pub(crate) fn shard(&self) -> [u64; CATEGORIES] {
        self.lock().as_ref().map_or([0; CATEGORIES], |s| {
            s.staged.as_ref().map_or(s.written.shard, |st| st.shard)
        })
    }
}

#[cfg(test)]
impl crate::shard::ShardEngine {
    /// Tests: this engine's queue gauges.
    pub(crate) fn queue_gauges_for_tests(&self) -> &QueueGauges {
        &self.layout.queue_gauges
    }
}

/// `0xFF×16 ‖ 'N'`: every gauge row (the whole tag is the gauges'; ids
/// below 16 are unused).
fn prefix() -> Vec<u8> {
    crate::spill::runindex::sentinel_prefix(TAG)
}

/// The keys of every gauge row: ids 16 through 47.
pub(crate) fn key_range() -> (Vec<u8>, Vec<u8>) {
    let (mut from, mut to) = (prefix(), prefix());
    from.push(SHARD_ID);
    to.push(CONTAINER_END);
    (from, to)
}

/// The row holding the generation of the gauge rows the shard log holds.
pub(crate) fn generation_key() -> Vec<u8> {
    let mut k = prefix();
    k.push(GENERATION_ID);
    k
}

/// The generation a generation row holds.
pub(crate) fn read_generation(v: &[u8]) -> Option<u64> {
    read_u64s(v).map(|(generation, _)| generation)
}

fn shard_key(cat: usize) -> Vec<u8> {
    let mut k = prefix();
    k.push(SHARD_ID + u8::try_from(cat).unwrap_or(0));
    k
}

fn container_key(cat: usize, container: &[u8; 16]) -> Vec<u8> {
    let mut k = prefix();
    k.push(CONTAINER_ID + u8::try_from(cat).unwrap_or(0));
    k.extend_from_slice(container);
    k
}

/// Whether a sum went from under `line` to at or over it.
fn crosses(line: u64, before: u64, after: u64) -> bool {
    before < line && after >= line
}

/// A gauge row's value: its bytes, and for a container its queues.
fn read_u64s(v: &[u8]) -> Option<(u64, u64)> {
    let first = u64::from_le_bytes(v.get(..8)?.try_into().ok()?);
    let second = match v.get(8..16) {
        Some(b) => u64::from_le_bytes(b.try_into().ok()?),
        None if v.len() == 8 => 0,
        None => return None,
    };
    Some((first, second))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_container_over_its_share_at_the_line_is_refused_and_another_is_not() {
        let g = QueueGauges::default();
        g.set_bound_for_tests(1_000);
        // The active slice is 300 bytes.
        let (a, b) = ([1; 16], [2; 16]);
        let mut rows = g.apply("s", a, ([0; 5], [250, 0, 0, 0, 0]));
        rows.extend(g.apply("s", b, ([0; 5], [50, 0, 0, 0, 0])));
        assert_eq!(g.shard()[ACTIVE], 300, "at the line");
        assert!(g.limited(a, ACTIVE, 250), "a holds 250 of a 150 share");
        assert!(!g.limited(b, ACTIVE, 50), "b holds 50, under its share");
        assert!(!g.limited(a, FAILED, 250), "another category's slice");
        g.apply("s", a, ([250, 0, 0, 0, 0], [240, 0, 0, 0, 0]));
        assert!(!g.limited(a, ACTIVE, 240), "below the line again");
        let loaded = QueueGauges::default();
        loaded.set_bound_for_tests(1_000);
        let rows = {
            let mut all: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            all.extend(rows);
            all.extend(g.apply("s", a, ([240, 0, 0, 0, 0], [250, 0, 0, 0, 0])));
            all.into_iter().collect::<Vec<_>>()
        };
        loaded.load(&rows);
        assert_eq!(loaded.shard(), g.shard(), "the rows carry the sums");
        assert!(loaded.limited(a, ACTIVE, 250) && !loaded.limited(b, ACTIVE, 50));
    }

    #[test]
    fn within_a_container_only_a_queue_at_or_over_its_share_is_refused() {
        let g = QueueGauges::default();
        g.set_bound_for_tests(1_000);
        let a = [1; 16];
        g.apply("s", a, ([0; 5], [0, 0, 150, 0, 0]));
        g.apply("s", a, ([0; 5], [0, 0, 50, 0, 0]));
        assert!(
            g.limited(a, DELAYED, 150),
            "150 of the container's 200, two queues"
        );
        assert!(!g.limited(a, DELAYED, 50), "the small queue is not");
        assert!(!g.limited(a, CLEANUP, 150), "cleanup refuses nothing");
    }

    #[test]
    fn staged_moves_become_written_only_when_their_batch_was() {
        let g = QueueGauges::default();
        g.load(&[]);
        let rows = g.apply("s", [1; 16], ([0; 5], [0, 0, 0, 168, 0]));
        assert_eq!(g.staged_generation(), Some(1));
        assert!(rows.contains(&(generation_key(), 1u64.to_le_bytes().to_vec())));
        g.settle(false);
        assert_eq!(
            (g.staged_generation(), g.shard()),
            (None, [0; 5]),
            "never written"
        );
        g.apply("s", [1; 16], ([0; 5], [0, 0, 0, 168, 0]));
        assert_eq!(g.staged_generation(), Some(1), "the same generation again");
        g.settle(true);
        assert_eq!((g.staged_generation(), g.shard()[DEDUP]), (None, 168));
        let rows = g.apply("s", [1; 16], ([0, 0, 0, 168, 0], [0; 5]));
        assert!(rows.contains(&(generation_key(), 2u64.to_le_bytes().to_vec())));
        g.settle(true);
        let read = QueueGauges::default();
        read.load(&rows);
        assert_eq!((read.shard(), read.staged_generation()), ([0; 5], None));
    }

    #[test]
    fn the_containers_with_state_follow_containers_entering_and_leaving_a_category() {
        let g = QueueGauges::default();
        g.load(&[]);
        g.set_bound_for_tests(1_000);
        let (p1, p2, p3) = ([1; 16], [2; 16], [3; 16]);
        g.apply("s", p1, ([0; 5], [0, 0, 0, 150, 0]));
        g.apply("s", p2, ([0; 5], [0, 0, 0, 70, 0]));
        // 220 at a 200 line between two containers: shares of 100.
        assert!(g.limited(p1, DEDUP, 150) && !g.limited(p2, DEDUP, 70));
        g.apply("s", p1, ([0, 0, 0, 150, 0], [0; 5]));
        g.apply("s", p3, ([0; 5], [0, 0, 0, 150, 0]));
        for settled in [false, true] {
            if settled {
                g.settle(true);
            }
            assert!(g.limited(p3, DEDUP, 150), "settled {settled}");
            assert!(
                !g.limited(p2, DEDUP, 70),
                "still two containers: settled {settled}"
            );
            assert!(!g.limited(p1, DEDUP, 1), "it left: settled {settled}");
        }
    }

    #[test]
    fn the_cleanup_alarm_fires_once_as_the_sum_crosses_its_line() {
        assert!(crosses(80, 79, 80) && crosses(80, 0, 1_000));
        assert!(!crosses(80, 80, 90), "already past it");
        assert!(!crosses(80, 10, 79) && !crosses(80, 90, 10));
    }

    /// Every gauge row's key, byte for byte (layout 8): `FF×16 ‖ 'N' ‖ id`, a
    /// container's followed by its key; ids 16-20 the shard's five categories,
    /// 21 the generation, 32-36 a container's.
    #[test]
    fn every_gauge_key_is_pinned() {
        let ff = "ff".repeat(16);
        let key = "33".repeat(16);
        let hex = |k: Vec<u8>| crate::crypto::hex(&k);
        let pinned = [
            (hex(shard_key(0)), format!("{ff}4e10")),
            (hex(shard_key(4)), format!("{ff}4e14")),
            (hex(generation_key()), format!("{ff}4e15")),
            (hex(container_key(0, &[0x33; 16])), format!("{ff}4e20{key}")),
            (hex(container_key(4, &[0x33; 16])), format!("{ff}4e24{key}")),
            (hex(key_range().0), format!("{ff}4e10")),
            (hex(key_range().1), format!("{ff}4e30")),
        ];
        for (key, want) in pinned {
            assert_eq!(key, want);
        }
    }
}
