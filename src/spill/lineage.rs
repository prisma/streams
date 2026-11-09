//! Lineage objects (S6-DESIGN §2.3): what a leaf writes under an ancestor's
//! root about the ancestor's objects it holds and releases. They live under
//! `<root>/lineage/`, outside `hseg/` and `ridx/`, so the orphan sweep never
//! lists them.
//!
//! - Reference list `<root>/lineage/refs/<holder>`: the ancestor's objects the
//!   holder held at birth (written by the operation that made it a leaf).
//! - Supplementary list `<root>/lineage/refs/<holder>-d`: the holdings the
//!   operation's pause added to the reference list's (S4-DESIGN §7.3); the holder
//!   held at birth the union of the two.
//! - Release log `<root>/lineage/rel/<holder>/<epoch>-<seq>`: objects the
//!   holder released, one log per collector tick and ancestor, at most
//!   `RELEASE_LOG_MAX` entries; `<epoch>` is the opening's writer epoch and
//!   `<seq>` counts from 0 per opening, so a fenced writer's late PUT never
//!   collides.
//! - Released-all marker `<root>/lineage/rel/<holder>/all`: the holder holds
//!   nothing under the root any more.
//!
//! A list's and a log's body: `ver:u8=1 count:u32 BE`, then `count` entries
//! `kind:u8 epoch:u64 BE seq:u64 BE` (17 B) in strictly ascending order, then
//! `crc32c:u32 BE` of everything before it. An entry's home is the root's.

use crate::spill::crc32c::crc32c;
use crate::spill::gc::ObjectKind;

const VERSION: u8 = 1;
const ENTRY: usize = 17;

/// One of an ancestor's objects: its kind, epoch and seq.
pub(crate) type Entry = (ObjectKind, u64, u64);

/// `entries`' body, in order and without duplicates.
pub(crate) fn encode(entries: &[Entry]) -> Vec<u8> {
    let mut sorted = entries.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let count = u32::try_from(sorted.len()).unwrap_or(u32::MAX);
    let mut body = Vec::with_capacity(9 + ENTRY * sorted.len());
    body.push(VERSION);
    body.extend_from_slice(&count.to_be_bytes());
    for (kind, epoch, seq) in &sorted {
        body.push(kind.byte());
        body.extend_from_slice(&epoch.to_be_bytes());
        body.extend_from_slice(&seq.to_be_bytes());
    }
    let crc = crc32c(&body);
    body.extend_from_slice(&crc.to_be_bytes());
    body
}

/// A body's entries; `None` for a body that is not one `encode` wrote.
pub(crate) fn decode(body: &[u8]) -> Option<Vec<Entry>> {
    let (head, crc) = body.split_last_chunk::<4>()?;
    if crc32c(head) != u32::from_be_bytes(*crc) {
        return None;
    }
    let (&ver, rest) = head.split_first()?;
    let (count, mut rest) = rest.split_first_chunk::<4>()?;
    let count = usize::try_from(u32::from_be_bytes(*count)).ok()?;
    if ver != VERSION || rest.len() != count.checked_mul(ENTRY)? {
        return None;
    }
    let mut out: Vec<Entry> = Vec::with_capacity(count);
    while let Some((entry, tail)) = rest.split_first_chunk::<ENTRY>() {
        let (&kind, ids) = entry.split_first()?;
        let (epoch, seq) = ids.split_first_chunk::<8>()?;
        let next = (
            ObjectKind::of(kind)?,
            u64::from_be_bytes(*epoch),
            u64::from_be_bytes(seq.try_into().ok()?),
        );
        if out.last().is_some_and(|last| *last >= next) {
            return None;
        }
        out.push(next);
        rest = tail;
    }
    Some(out)
}

/// The release log `<seq>` of opening `epoch` of `holder` under `root`.
pub(crate) fn release_log_path(root: &str, holder: u32, epoch: u64, seq: u64) -> String {
    format!("{root}/lineage/rel/{holder}/{epoch:020}-{seq:020}")
}

/// `holder`'s reference list under `root`: what it held there at birth (S4's
/// split and S6's merge write it).
pub(crate) fn reference_list_path(root: &str, holder: u32) -> String {
    format!("{root}/lineage/refs/{holder}")
}

/// `holder`'s supplementary list under `root`: the holdings its birth's pause
/// added to its reference list's.
pub(crate) fn supplementary_list_path(root: &str, holder: u32) -> String {
    format!("{root}/lineage/refs/{holder}-d")
}

/// `holder`'s released-all marker under `root`.
pub(crate) fn released_all_path(root: &str, holder: u32) -> String {
    format!("{root}/lineage/rel/{holder}/all")
}

#[cfg(test)]
mod tests;
