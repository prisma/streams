#![cfg(test)]
//! A sealed read's plan over its candidates (`plan_steps`): a skip consumes
//! offsets only up to the window's end; the plan reports a gap only for an
//! offset no candidate holds below the window's end, never for a stop at
//! the byte budget, the fetch cap, the look-at cap or an entry whose sizes
//! cannot hold its pages.

use super::{Candidate, MAX_CHUNKS_LOOKED_AT, MAX_CHUNKS_PER_READ, Step, plan_steps};
use crate::shard::RingScan;
use crate::shard::seal::SealCounters;
use crate::spill::directory::{DirEntry, KeySummary};
use crate::spill::object::ObjectId;
use crate::spill::runindex::tiling::Span;

const LEN: u32 = 1000;

fn entry(first: u64, last: u64, len: u32, keys: Vec<u64>) -> Candidate {
    Candidate::Chunk(DirEntry {
        object: ObjectId {
            epoch: 1,
            seq: first,
        },
        chunk_off: 0,
        chunk_len: len,
        first,
        last,
        page_count: 1,
        keys: KeySummary::Keys(keys),
        anchors: Vec::new(),
    })
}

fn chunk(first: u64, last: u64) -> Candidate {
    entry(first, last, LEN, Vec::new())
}

fn scan(from: u64, to: u64, max_bytes: usize) -> RingScan {
    RingScan {
        from,
        to,
        max_bytes,
    }
}

/// A plan's steps as `(fetched, first, next)`, and its gap.
type Planned = (Vec<(bool, u64, u64)>, Option<u64>);

/// The plan of `candidates` over `scan`.
fn plan(candidates: &[Candidate], scan: RingScan, key: Option<&str>) -> Planned {
    let counters = SealCounters::default();
    let (steps, gap) = plan_steps(&counters, candidates, scan, key);
    let steps = steps
        .iter()
        .map(|s| match s {
            Step::Fetch(p) => (true, p.first, p.next),
            Step::Skip(through) => (false, *through, through + 1),
        })
        .collect();
    (steps, gap)
}

#[test]
fn a_plan_reports_a_gap_only_below_the_windows_end() {
    let big = usize::MAX;
    assert_eq!(
        plan(&[chunk(0, 4), chunk(5, 9)], scan(0, 10, big), None),
        (vec![(true, 0, 5), (true, 5, 10)], None),
        "tiled to the window's end"
    );
    assert_eq!(
        plan(&[chunk(0, 4), chunk(7, 9)], scan(0, 10, big), None),
        (vec![(true, 0, 5)], Some(5)),
        "offset 5 is in no chunk"
    );
    assert_eq!(
        plan(&[chunk(0, 4)], scan(0, 10, LEN as usize), None),
        (vec![(true, 0, 5)], None),
        "the byte budget is spent"
    );
    assert_eq!(
        plan(&[entry(0, 4, 0, Vec::new())], scan(0, 10, big), None),
        (Vec::new(), None),
        "an entry too short for its pages stops the plan"
    );
}

#[test]
fn a_plan_stops_without_a_gap_at_its_fetch_cap() {
    let n = MAX_CHUNKS_PER_READ as u64;
    let at_cap: Vec<_> = (0..n).map(|o| chunk(o, o)).collect();
    let (steps, gap) = plan(&at_cap, scan(0, 2 * n, usize::MAX), None);
    assert_eq!((steps.len(), gap), (MAX_CHUNKS_PER_READ, None));
    let below: Vec<_> = (0..n - 1).map(|o| chunk(o, o)).collect();
    let (steps, gap) = plan(&below, scan(0, 2 * n, usize::MAX), None);
    assert_eq!((steps.len(), gap), (MAX_CHUNKS_PER_READ - 1, Some(n - 1)));
}

#[test]
fn a_keyed_plan_stops_without_a_gap_at_its_look_at_cap() {
    let n = MAX_CHUNKS_LOOKED_AT as u64;
    let excluded = |count: u64| -> Vec<_> { (0..count).map(|o| chunk(o, o)).collect() };
    let (steps, gap) = plan(&excluded(n), scan(0, 2 * n, usize::MAX), Some("k"));
    assert_eq!((steps.len(), gap), (MAX_CHUNKS_LOOKED_AT, None));
    assert!(steps.iter().all(|&(fetched, _, _)| !fetched));
    let (steps, gap) = plan(&excluded(n - 1), scan(0, 2 * n, usize::MAX), Some("k"));
    assert_eq!((steps.len(), gap), (MAX_CHUNKS_LOOKED_AT - 1, Some(n - 1)));
}

#[test]
fn a_skip_consumes_offsets_only_up_to_the_windows_end() {
    let window = scan(0, 10, usize::MAX);
    let run = Candidate::Skip(Span {
        first: 0,
        last: 100,
    });
    assert_eq!(
        plan(&[run], window, Some("k")),
        (vec![(false, 9, 10)], None)
    );
    assert_eq!(
        plan(&[chunk(0, 100)], window, Some("k")),
        (vec![(false, 9, 10)], None),
        "a chunk the key summary excludes"
    );
}
