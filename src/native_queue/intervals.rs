//! A queue's released intervals (S3b-DESIGN §3.1, §7.3): the runs of its
//! sealed chunks the trim retired and decremented above the purge head's
//! `reclaimed` prefix, which the prefix has not reached yet because a chunk
//! below them is still referenced. Every chunk starting below `reclaimed`
//! and every chunk inside an interval is released: no pass selects it
//! again, and a fold or the cleaner keeps no entry of it.
//!
//! Row `h16 ‖ 'Q' ‖ 'k' ‖ last u64 BE` → `first u64 BE`: the interval
//! `[first, last]` of offsets, inclusive. Only the gc writes them, in the
//! batch of the decrements they record; the committer never reads or
//! writes them, a queue's delete never touches them, and the purge deletes
//! them with its last decrement (§5.9).
//!
//! A queue's chunks never join (no consolidation or copy takes a queue
//! chunk), so they tile `[0, absorbed)`: a chunk `[f, l]` follows the one
//! ending at `f - 1`, and two intervals that touch are one.
use std::collections::BTreeMap;

use super::keys;

/// A run of released offsets, both ends included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Interval {
    pub(crate) first: u64,
    pub(crate) last: u64,
}

/// The row key of the interval ending at `last`.
pub(crate) fn key(hash: &[u8; 16], last: u64) -> Vec<u8> {
    keys::by_offset(hash, keys::RELEASED, last)
}

/// The key range of a queue's interval rows.
pub(crate) fn key_range(hash: &[u8; 16]) -> (Vec<u8>, Vec<u8>) {
    (
        keys::family(hash, keys::RELEASED),
        keys::family(hash, keys::RELEASED + 1),
    )
}

pub(crate) fn encode(iv: &Interval) -> [u8; 8] {
    iv.first.to_be_bytes()
}

/// An interval row; refused unless its key ends in `last` after the
/// queue's family prefix and its value is a `first` at most `last`.
pub(crate) fn decode(key: &[u8], value: &[u8]) -> Result<Interval, String> {
    let last = key
        .get(keys::FAMILY_AT + 1..)
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map(u64::from_be_bytes);
    let first = <[u8; 8]>::try_from(value).ok().map(u64::from_be_bytes);
    match (first, last, key.get(keys::FAMILY_AT)) {
        (Some(first), Some(last), Some(&keys::RELEASED)) if first <= last => {
            Ok(Interval { first, last })
        }
        _ => Err(format!(
            "a released-interval row of {} and {} bytes",
            key.len(),
            value.len()
        )),
    }
}

/// What a queue has released: its prefix and its intervals above it, as a
/// pass reads them and as its releases move them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Released {
    /// Every chunk starting below it is released.
    pub(crate) reclaimed: u64,
    /// The intervals, by `last`.
    intervals: BTreeMap<u64, u64>,
}

impl Released {
    pub(crate) fn new(reclaimed: u64, intervals: &[Interval]) -> Self {
        let intervals = intervals.iter().map(|iv| (iv.last, iv.first)).collect();
        Self {
            reclaimed,
            intervals,
        }
    }

    /// Whether the chunk `[first, last]` is released: it starts below the
    /// prefix, or lies inside an interval.
    pub(crate) fn holds(&self, first: u64, last: u64) -> bool {
        first < self.reclaimed || self.inside(first, last)
    }

    /// Whether the chunk `[first, last]` lies inside an interval.
    pub(crate) fn inside(&self, first: u64, last: u64) -> bool {
        self.intervals
            .range(last..)
            .next()
            .is_some_and(|(_, &f)| f <= first)
    }

    /// Release the chunk `[first, last]`: at the prefix it moves the prefix
    /// past it and past every interval the prefix then reaches; elsewhere it
    /// joins the intervals, one with any it touches.
    pub(crate) fn release(&mut self, first: u64, last: u64) {
        if first <= self.reclaimed {
            self.reclaimed = self.reclaimed.max(last.saturating_add(1));
            let reached = |r: &Self| {
                r.intervals
                    .first_key_value()
                    .map(|(&l, &f)| (l, f))
                    .filter(|&(_, f)| f <= r.reclaimed)
            };
            while let Some((l, _)) = reached(self) {
                self.intervals.remove(&l);
                self.reclaimed = self.reclaimed.max(l.saturating_add(1));
            }
            return;
        }
        let (mut f, mut l) = (first, last);
        if let Some((&pl, &pf)) = self.intervals.range(..first).next_back()
            && pl.saturating_add(1) == first
        {
            self.intervals.remove(&pl);
            f = pf;
        }
        if let Some((&nl, &nf)) = self.intervals.range(last..).next()
            && last.saturating_add(1) == nf
        {
            self.intervals.remove(&nl);
            l = nl;
        }
        self.intervals.insert(l, f);
    }

    pub(crate) fn intervals(&self) -> Vec<Interval> {
        self.intervals
            .iter()
            .map(|(&last, &first)| Interval { first, last })
            .collect()
    }

    /// The row changes that take `before`'s intervals to this one's: the
    /// intervals to put, and the `last` of every row to delete.
    pub(crate) fn changes(&self, before: &Released) -> (Vec<Interval>, Vec<u64>) {
        let put = self
            .intervals()
            .into_iter()
            .filter(|iv| before.intervals.get(&iv.last) != Some(&iv.first))
            .collect();
        let delete = before
            .intervals
            .keys()
            .filter(|l| !self.intervals.contains_key(l))
            .copied()
            .collect();
        (put, delete)
    }
}

#[cfg(test)]
mod tests {
    use super::{Interval, Released, decode, encode, key, key_range};

    const H: [u8; 16] = [7; 16];

    fn iv(first: u64, last: u64) -> Interval {
        Interval { first, last }
    }

    #[test]
    fn an_interval_row_round_trips_and_refuses_any_other_shape() {
        let k = key(&H, 0x0102);
        assert_eq!(
            crate::crypto::hex(&k),
            format!("{}516b{:016x}", "07".repeat(16), 0x0102)
        );
        let (from, to) = key_range(&H);
        assert!(from <= k && k < to);
        assert_eq!(decode(&k, &encode(&iv(9, 0x0102))), Ok(iv(9, 0x0102)));
        let range = crate::native_queue::keys::range(&H, 0);
        for (bad_key, bad_value) in [
            (k.clone(), 0x0103u64.to_be_bytes().to_vec()),
            (k.clone(), vec![0; 7]),
            (k[..k.len() - 1].to_vec(), 1u64.to_be_bytes().to_vec()),
            (range, 0u64.to_be_bytes().to_vec()),
        ] {
            assert!(decode(&bad_key, &bad_value).is_err(), "{bad_key:02x?}");
        }
    }

    /// Chunks of four offsets: releases above the prefix join into
    /// intervals, one at the prefix moves it past every interval it then
    /// reaches, and the row changes are exactly the difference.
    #[test]
    fn releases_move_the_prefix_through_the_intervals_it_reaches() {
        let before = Released::new(4, &[iv(12, 15)]);
        let mut r = before.clone();
        assert!(r.holds(0, 3) && r.holds(12, 15) && !r.holds(4, 7));
        r.release(16, 19);
        assert_eq!(r.intervals(), [iv(12, 19)], "a touching chunk joins");
        r.release(24, 27);
        r.release(8, 11);
        assert_eq!(r.intervals(), [iv(8, 19), iv(24, 27)]);
        assert_eq!(
            r.changes(&before),
            (vec![iv(8, 19), iv(24, 27)], vec![15]),
            "puts and the one row deleted"
        );
        r.release(4, 7);
        assert_eq!((r.reclaimed, r.intervals()), (20, vec![iv(24, 27)]));
        assert!(r.holds(16, 19) && r.holds(24, 27) && !r.holds(20, 23));
        r.release(20, 23);
        assert_eq!((r.reclaimed, r.intervals()), (28, vec![]));
        assert_eq!(r.changes(&before), (vec![], vec![15]));
    }
}
