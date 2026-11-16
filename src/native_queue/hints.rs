//! A shard's trim hints (S3b-DESIGN §3.4, §7.3; F6, F13): which offsets of
//! which queue the trim pass should look at again. A batch that moves a
//! queue's frontier past sealed offsets, resolves a reference at a sealed
//! offset or seals a queue's pages puts one hint row naming them:
//!
//! - key `FF×16 ‖ 'Z' ‖ gseq u64 BE ‖ h16`. `gseq` is the committer's
//!   shard-wide sequence, so hints sort by when they were written, not by
//!   queue; it is seeded once per engine above every hint key and the
//!   trim's cursor (`seed`), and is not persisted.
//! - value `n u8 ‖ (a u64 BE, b u64 BE)×n`, `1 ≤ n ≤ 8`: half-open offset
//!   ranges `[a, b)`, sorted, disjoint.
//!
//! Ranges are coalesced at range-row granularity ([`super::range::SHIFT`]):
//! two that overlap, touch or lie in one range row's span become one, and
//! none is ever widened beyond the spans its offsets lie in. More than
//! eight write another row.
use crate::keyspace::SENTINEL;

/// The sentinel tag of a trim hint (`keyspace::tag::TRIM_HINT`).
pub(crate) const TAG: u8 = crate::keyspace::tag::TRIM_HINT;
/// The ranges one hint row holds at most.
pub(crate) const MAX_RANGES: usize = 8;
/// A hint key's length: the sentinel, the tag, `gseq`, the queue's hash.
const KEY_LEN: usize = 16 + 1 + 8 + 16;

/// The hint key of `gseq` for the queue `hash`.
pub(crate) fn key(gseq: u64, hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(KEY_LEN);
    k.extend_from_slice(&SENTINEL);
    k.push(TAG);
    k.extend_from_slice(&gseq.to_be_bytes());
    k.extend_from_slice(hash);
    k
}

/// A hint key's `gseq` and queue, or `None` for any other key.
pub(crate) fn parse_key(key: &[u8]) -> Option<(u64, [u8; 16])> {
    let (sentinel, rest) = key.split_first_chunk::<16>()?;
    let (&tag, rest) = rest.split_first()?;
    let (gseq, hash) = rest.split_first_chunk::<8>()?;
    let hash: [u8; 16] = hash.try_into().ok()?;
    (*sentinel == SENTINEL && tag == TAG).then_some((u64::from_be_bytes(*gseq), hash))
}

/// The key range holding every hint key.
pub(crate) fn key_range() -> (Vec<u8>, Vec<u8>) {
    let mut from = SENTINEL.to_vec();
    from.push(TAG);
    let mut to = SENTINEL.to_vec();
    to.push(TAG + 1);
    (from, to)
}

/// The trim's cursor row (`FF×16 ‖ 'L' ‖ 'Z'`): its value is the last hint
/// key the trim consumed.
pub(crate) fn cursor_key() -> Vec<u8> {
    let mut k = SENTINEL.to_vec();
    k.push(crate::keyspace::tag::CURSOR);
    k.push(TAG);
    k
}

/// The first `gseq` an engine may write: above the last hint key and above
/// the cursor's (when every hint was consumed and deleted only the cursor
/// says where the trim reads from, and a hint behind it is never read).
pub(crate) fn seed(last_key: Option<&[u8]>, cursor: Option<&[u8]>) -> Result<u64, String> {
    let gseq = |raw: &[u8], what: &str| {
        parse_key(raw)
            .map(|(gseq, _)| gseq)
            .ok_or_else(|| format!("the {what} is not a trim hint key: {raw:02x?}"))
    };
    let last = last_key.map(|k| gseq(k, "last hint key")).transpose()?;
    let cursor = cursor.map(|c| gseq(c, "trim cursor")).transpose()?;
    let top = last.max(cursor).map_or(0, |g| g.saturating_add(1));
    Ok(top.max(1))
}

/// A hint row's value.
pub(crate) fn encode(ranges: &[(u64, u64)]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 16 * ranges.len());
    v.push(u8::try_from(ranges.len()).unwrap_or(u8::MAX));
    for (a, b) in ranges {
        v.extend_from_slice(&a.to_be_bytes());
        v.extend_from_slice(&b.to_be_bytes());
    }
    v
}

/// A hint row's ranges; refused unless it holds 1 to 8 sorted, disjoint,
/// non-empty ranges and nothing else.
pub(crate) fn decode(raw: &[u8]) -> Result<Vec<(u64, u64)>, String> {
    let (&n, body) = raw.split_first().ok_or("an empty trim hint")?;
    let n = usize::from(n);
    if !(1..=MAX_RANGES).contains(&n) || body.len() != 16 * n {
        return Err(format!(
            "a trim hint of {} bytes naming {n} ranges",
            raw.len()
        ));
    }
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(n);
    for pair in body.as_chunks::<16>().0 {
        let (a, b) = pair.split_at(8);
        let a = u64::from_be_bytes(a.try_into().map_err(|_| "a short range")?);
        let b = u64::from_be_bytes(b.try_into().map_err(|_| "a short range")?);
        if a >= b || out.last().is_some_and(|&(_, end)| end > a) {
            return Err(format!("a trim hint range [{a}, {b}) out of order"));
        }
        out.push((a, b));
    }
    Ok(out)
}

/// The offsets a group's hints name for one queue, coalesced.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct HintRanges(Vec<(u64, u64)>);

impl HintRanges {
    /// Add `[a, b)` (nothing when empty).
    pub(crate) fn add(&mut self, a: u64, b: u64) {
        if a >= b {
            return;
        }
        let (mut a, mut b) = (a, b);
        self.0.retain(|&(x, y)| {
            let joins = x <= b && a <= y || same_span(x, y, a, b);
            if joins {
                (a, b) = (a.min(x), b.max(y));
            }
            !joins
        });
        let at = self.0.partition_point(|&(x, _)| x < a);
        self.0.insert(at, (a, b));
    }

    /// The ranges below `upto` (cut there).
    pub(crate) fn below(&self, upto: u64) -> Vec<(u64, u64)> {
        self.0
            .iter()
            .filter(|&&(a, _)| a < upto)
            .map(|&(a, b)| (a, b.min(upto)))
            .collect()
    }
}

/// Whether `[x, y)` and `[a, b)` both lie within one range row's span.
fn same_span(x: u64, y: u64, a: u64, b: u64) -> bool {
    let span = |o: u64| o >> super::range::SHIFT;
    let (s, t) = (span(x), span(y - 1));
    s == t && span(a) == s && span(b - 1) == s
}

#[cfg(kani)]
mod proofs;

#[cfg(test)]
mod tests {
    use super::{HintRanges, cursor_key, decode, encode, key, key_range, parse_key, seed};

    #[test]
    fn a_hint_key_sorts_by_gseq_and_names_its_queue() {
        let k = key(0x0102_0304_0506_0708, &[9; 16]);
        assert_eq!(
            crate::crypto::hex(&k),
            format!("{}5a0102030405060708{}", "ff".repeat(16), "09".repeat(16))
        );
        assert_eq!(parse_key(&k), Some((0x0102_0304_0506_0708, [9; 16])));
        assert!(key(1, &[0xFF; 16]) < key(2, &[0; 16]), "gseq first");
        let (from, to) = key_range();
        assert!(from <= k && k < to);
        for other in [
            cursor_key(),
            k[..k.len() - 1].to_vec(),
            [&k[..], b"x"].concat(),
        ] {
            assert_eq!(parse_key(&other), None, "{other:02x?}");
        }
        assert_eq!(
            crate::crypto::hex(&cursor_key()),
            format!("{}4c5a", "ff".repeat(16))
        );
    }

    #[test]
    fn the_seed_is_above_the_last_hint_and_the_cursor() {
        let (k5, k9) = (key(5, &[1; 16]), key(9, &[2; 16]));
        assert_eq!(seed(None, None), Ok(1));
        assert_eq!(seed(Some(&k5), None), Ok(6));
        assert_eq!(seed(None, Some(&k9)), Ok(10), "every hint consumed");
        assert_eq!(seed(Some(&k9), Some(&k5)), Ok(10));
        assert_eq!(seed(Some(&k5), Some(&k9)), Ok(10));
        assert!(seed(Some(b"not a key"), None).is_err());
    }

    #[test]
    fn a_hint_value_round_trips_and_refuses_any_other_shape() {
        let ranges = vec![(0, 3), (4_096, 4_100), (9_000, 9_001)];
        let raw = encode(&ranges);
        assert_eq!(raw.len(), 1 + 3 * 16);
        assert_eq!(decode(&raw), Ok(ranges));
        let nine: Vec<(u64, u64)> = (0..9).map(|i| (i * 10, i * 10 + 1)).collect();
        for bad in [
            Vec::new(),
            vec![0],
            encode(&nine),
            encode(&[(3, 3)]),
            encode(&[(5, 9), (8, 12)]),
            encode(&[(5, 9), (1, 2)]),
            [&encode(&[(1, 2)])[..], &[0]].concat(),
        ] {
            assert!(decode(&bad).is_err(), "{bad:02x?}");
        }
    }

    #[test]
    fn ranges_coalesce_within_a_range_row_and_when_they_touch_never_across_spans() {
        let mut h = HintRanges::default();
        h.add(10, 12);
        h.add(20, 21);
        assert_eq!(h.below(u64::MAX), [(10, 21)], "one span: one range");
        h.add(4_096, 4_098);
        h.add(8_200, 8_300);
        assert_eq!(
            h.below(u64::MAX),
            [(10, 21), (4_096, 4_098), (8_200, 8_300)],
            "spans apart stay apart"
        );
        h.add(4_000, 4_100);
        assert_eq!(
            h.below(u64::MAX),
            [(10, 21), (4_000, 4_100), (8_200, 8_300)],
            "overlap joins across a span boundary, no wider"
        );
        h.add(21, 4_000);
        assert_eq!(h.below(u64::MAX), [(10, 4_100), (8_200, 8_300)]);
        h.add(7, 7);
        assert_eq!(h.below(4_097), [(10, 4_097)], "cut at the boundary");
        assert_eq!(HintRanges::default().below(u64::MAX), []);
    }
}
