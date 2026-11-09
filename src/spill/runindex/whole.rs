//! A whole run-index object, every layer checked (S3a §2.6): the trailer, the footer against its
//! CRC, and each run against its footer entry. A split reads its parent's run indexes through it.
//! It is not in `format`, which the `run_index` fuzz target compiles by path under a frozen
//! exception contract; `format` keeps its test-built copy, for its own tests, and the fuzz target
//! checks the same layers itself.

use super::format::{
    FooterEntry, TRAILER_LEN, Trailer, checked_footer, decode_run, decode_trailer, run_bloom,
    run_flags,
};
use crate::spill::directory::DirEntry;

/// One run of a decoded object: its footer entry and its entries.
pub(crate) type DecodedRun = (FooterEntry, Vec<DirEntry>);

/// A whole object, every check verified: its trailer and each run with its
/// footer entry, in hash order. `None` for any byte sequence `ObjectWriter`
/// does not write.
pub(crate) fn decode_object(raw: &[u8]) -> Option<(Trailer, Vec<DecodedRun>)> {
    let split = raw.len().checked_sub(TRAILER_LEN)?;
    let (rest, tail) = raw.split_at(split);
    let (trailer, footer_crc) = decode_trailer(tail)?;
    let footer_off = usize::try_from(trailer.footer_off).ok()?;
    let (runs, footer) = rest.split_at_checked(footer_off)?;
    let footer = checked_footer(footer, &trailer, footer_crc)?;
    let mut out = Vec::with_capacity(footer.len());
    for f in footer {
        let start = usize::try_from(f.run_off).ok()?;
        let run = runs.get(start..start.checked_add(f.run_len as usize)?)?;
        let entries = decode_run(run)?;
        let agrees = entries.len() == f.entries as usize
            && entries.first().map(|e| e.first) == Some(f.first)
            && entries.last().map(|e| e.last) == Some(f.last)
            && f.flags == run_flags(&entries)
            && f.bloom == run_bloom(&entries);
        if !agrees {
            return None;
        }
        out.push((f, entries));
    }
    Some((trailer, out))
}
