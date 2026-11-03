//! A queue's range rows (S3b-DESIGN §3.3): the sparse unresolved state of
//! one aligned range of [`SPAN`] offsets, present only while it holds
//! something. `open` counts the range's open deliveries and `sched` its
//! unresolved scheduled messages. Three bitmaps:
//! - `skip`: the scheduled offsets at or past the frontier, resolved or
//!   not, which the frontier passes without leasing;
//! - `cancel`: the immediate offsets at or past the frontier cancelled
//!   before they were taken, which the frontier passes too, and whose
//!   bytes leave the queued gauge only then (a cancel reads no page);
//! - `sbits`: the unresolved scheduled offsets, wherever they are.
//!
//! The keyless committer learns a message is scheduled at its send and
//! later only from these bits: the class is in the encrypted envelope.
//!
//! Value: `ver u8`, `open u32`, `sched u32` (little-endian), then the three
//! bitmaps, each either `0 ‖ n u16 ‖ (start u16, len u16)×n` (runs) or
//! `1 ‖ 512 bytes` (raw), whichever is shorter, so a row is at most
//! 1,548 bytes whatever its bits.

/// Offsets per range row.
pub(crate) const SPAN: u64 = 4_096;
/// [`SPAN`] as a bit index bound.
const SPAN_BITS: usize = 4_096;
/// Bits of a range row's offset index (`off >> SHIFT` keys the row).
pub(crate) const SHIFT: u32 = 12;
const WORDS: usize = SPAN_BITS / 64;
const RANGE_ROW_VERSION: u8 = 1;
const RUNS: u8 = 0;
const RAW: u8 = 1;

/// The row index of an offset.
pub(crate) fn index_of(offset: u64) -> u64 {
    offset >> SHIFT
}

/// The bit of an offset within its row.
fn bit_of(offset: u64) -> usize {
    usize::try_from(offset & (SPAN - 1)).unwrap_or(0)
}

/// A set of the [`SPAN`] offsets of one row.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Bits([u64; WORDS]);

impl Default for Bits {
    fn default() -> Self {
        Self([0; WORDS])
    }
}

impl std::fmt::Debug for Bits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.runs()).finish()
    }
}

impl Bits {
    pub(crate) fn contains(&self, offset: u64) -> bool {
        let b = bit_of(offset);
        self.0[b / 64] & (1 << (b % 64)) != 0
    }
    pub(crate) fn set(&mut self, offset: u64) {
        let b = bit_of(offset);
        self.0[b / 64] |= 1 << (b % 64);
    }
    pub(crate) fn clear(&mut self, offset: u64) {
        let b = bit_of(offset);
        self.0[b / 64] &= !(1 << (b % 64));
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.0.iter().all(|w| *w == 0)
    }
    pub(crate) fn count(&self) -> u32 {
        self.0.iter().map(|w| w.count_ones()).sum()
    }

    /// The set bits as `(start, len)` runs, ascending.
    fn runs(&self) -> Vec<(u16, u16)> {
        let mut out = Vec::new();
        let mut start: Option<usize> = None;
        for b in 0..=SPAN_BITS {
            let set = b < SPAN_BITS && self.0[b / 64] & (1 << (b % 64)) != 0;
            match (set, start) {
                (true, None) => start = Some(b),
                (false, Some(s)) => {
                    out.push((as_u16(s), as_u16(b - s)));
                    start = None;
                }
                _ => {}
            }
        }
        out
    }

    fn put(&self, v: &mut Vec<u8>) {
        let runs = self.runs();
        if 2 + runs.len() * 4 < WORDS * 8 {
            v.push(RUNS);
            v.extend_from_slice(&as_u16(runs.len()).to_le_bytes());
            for (s, n) in runs {
                v.extend_from_slice(&s.to_le_bytes());
                v.extend_from_slice(&n.to_le_bytes());
            }
        } else {
            v.push(RAW);
            for w in self.0 {
                v.extend_from_slice(&w.to_le_bytes());
            }
        }
    }

    /// Set the bits of `len` from `s`, a run that starts past `end` (the
    /// previous run's end and one gap bit) and fits the row; returns its
    /// own end.
    fn set_run(&mut self, s: usize, len: usize, end: usize) -> Result<usize, String> {
        if len == 0 || s < end || s + len > SPAN_BITS {
            return Err("queue range row: bad run".into());
        }
        for b in s..s + len {
            self.0[b / 64] |= 1 << (b % 64);
        }
        Ok(s + len + 1)
    }

    fn read(raw: &mut &[u8]) -> Result<Self, String> {
        let mut bits = Self::default();
        match take(raw, 1)?[0] {
            RUNS => {
                let n = u16::from_le_bytes(take(raw, 2)?.try_into().map_err(|_| "range row")?);
                let mut end = 0usize;
                for _ in 0..n {
                    let run = take(raw, 4)?;
                    let s = usize::from(u16::from_le_bytes([run[0], run[1]]));
                    let len = usize::from(u16::from_le_bytes([run[2], run[3]]));
                    end = bits.set_run(s, len, end)?;
                }
            }
            RAW => {
                for w in &mut bits.0 {
                    *w = u64::from_le_bytes(take(raw, 8)?.try_into().map_err(|_| "range row")?);
                }
            }
            other => return Err(format!("queue range row: bitmap form {other}")),
        }
        Ok(bits)
    }
}

/// A range row's value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RangeRow {
    /// Open deliveries (leased, retrying, failed) at the range's offsets.
    pub open: u32,
    /// Unresolved scheduled messages at the range's offsets (`sbits`' count).
    pub sched: u32,
    /// Scheduled offsets at or past the frontier.
    pub skip: Bits,
    /// Immediate offsets at or past the frontier, cancelled untaken.
    pub cancel: Bits,
    /// The unresolved scheduled offsets.
    pub sbits: Bits,
}

impl RangeRow {
    /// Whether the row holds nothing, so it is deleted rather than written.
    pub(crate) fn is_empty(&self) -> bool {
        self.open == 0
            && self.sched == 0
            && self.skip.is_empty()
            && self.cancel.is_empty()
            && self.sbits.is_empty()
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(16);
        v.push(RANGE_ROW_VERSION);
        v.extend_from_slice(&self.open.to_le_bytes());
        v.extend_from_slice(&self.sched.to_le_bytes());
        self.skip.put(&mut v);
        self.cancel.put(&mut v);
        self.sbits.put(&mut v);
        v
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self, String> {
        let mut rest = raw;
        let ver = take(&mut rest, 1)?[0];
        if ver != RANGE_ROW_VERSION {
            return Err(format!("queue range row: version {ver}"));
        }
        let open = u32::from_le_bytes(take(&mut rest, 4)?.try_into().map_err(|_| "range row")?);
        let sched = u32::from_le_bytes(take(&mut rest, 4)?.try_into().map_err(|_| "range row")?);
        let skip = Bits::read(&mut rest)?;
        let cancel = Bits::read(&mut rest)?;
        let sbits = Bits::read(&mut rest)?;
        if !rest.is_empty() {
            return Err("queue range row: trailing bytes".into());
        }
        if sbits.count() != sched {
            return Err(format!(
                "queue range row: {sched} scheduled, {} bits",
                sbits.count()
            ));
        }
        Ok(Self {
            open,
            sched,
            skip,
            cancel,
            sbits,
        })
    }
}

fn take<'a>(raw: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if raw.len() < n {
        return Err("queue range row: truncated".into());
    }
    let (head, tail) = raw.split_at(n);
    *raw = tail;
    Ok(head)
}

/// A bit index or run length of one row, which fits 16 bits.
fn as_u16(n: usize) -> u16 {
    u16::try_from(n).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_row_round_trips_in_the_shorter_form() {
        let mut row = RangeRow {
            open: 3,
            ..RangeRow::default()
        };
        for off in [
            4_096 * 7 + 1,
            4_096 * 7 + 2,
            4_096 * 7 + 3,
            4_096 * 7 + 4_095,
        ] {
            row.skip.set(off);
        }
        row.sbits.set(4_096 * 7 + 2);
        row.sched = 1;
        let raw = row.encode();
        // ver, open, sched; skip: runs [1, 3) and [4095, 1); cancel: none;
        // sbits: run [2, 1).
        assert_eq!(
            raw.len(),
            1 + 4 + 4 + (1 + 2 + 2 * 4) + (1 + 2) + (1 + 2 + 4)
        );
        assert_eq!(RangeRow::decode(&raw).unwrap(), row);
        // Every other bit: runs would be 8 KiB, so the raw form.
        let mut dense = RangeRow::default();
        for b in (0..SPAN).step_by(2) {
            dense.skip.set(b);
        }
        let raw = dense.encode();
        assert_eq!(raw.len(), 1 + 4 + 4 + (1 + 512) + (1 + 2) + (1 + 2));
        assert_eq!(RangeRow::decode(&raw).unwrap(), dense);
        assert_eq!(dense.skip.count(), 2_048);
    }

    #[test]
    fn a_range_row_refuses_what_it_did_not_write() {
        let mut row = RangeRow::default();
        row.sbits.set(9);
        row.sched = 1;
        let raw = row.encode();
        assert!(
            RangeRow::decode(&raw[..raw.len() - 1]).is_err(),
            "truncated"
        );
        let mut long = raw.clone();
        long.push(0);
        assert!(RangeRow::decode(&long).is_err(), "trailing");
        let mut miscount = raw.clone();
        miscount[5] = 2;
        assert!(
            RangeRow::decode(&miscount).is_err(),
            "count and bits disagree"
        );
        let mut version = raw;
        version[0] = 2;
        assert!(RangeRow::decode(&version).is_err(), "version");
    }

    #[test]
    fn offsets_map_to_rows_and_bits() {
        assert_eq!(index_of(4_095), 0);
        assert_eq!(index_of(4_096), 1);
        let mut b = Bits::default();
        b.set(4_096 * 3 + 10);
        assert!(b.contains(10) && b.contains(4_096 + 10));
        assert!(!b.contains(11));
        b.clear(10);
        assert!(b.is_empty());
    }
}
