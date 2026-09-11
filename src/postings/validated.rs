//! Admission proof for binary-seekable immutable derived postings.
use super::{AbsRun, GAP_UNKNOWN};
use std::{
    ops::{Deref, Range},
    sync::Arc,
};

/// Only construction/extension validates the whole run array. Warm windows
/// borrow this immutable proof; callers cannot install an unchecked Arc.
#[derive(Clone)]
pub(crate) struct ValidatedRuns(Arc<[AbsRun]>);
impl ValidatedRuns {
    pub(crate) fn new(runs: Vec<AbsRun>) -> Option<Self> {
        validate(&runs)?;
        Some(Self(runs.into()))
    }
    pub(crate) fn empty() -> Self {
        Self(Arc::from([]))
    }

    /// Reloads may include an already-proven prefix. Validate the new source
    /// before clipping only that prefix, preserving a straddler's full weight.
    pub(crate) fn extend_after(&self, fresh: &Self, cut: u64) -> Option<Self> {
        if self.last().is_some_and(|r| r.start + r.count as u64 > cut) {
            return None;
        }
        let mut merged = self.to_vec();
        let tail = fresh
            .iter()
            .filter_map(|r| {
                let end = r.start + r.count as u64;
                (end > cut).then(|| AbsRun {
                    start: r.start.max(cut),
                    count: (end - r.start.max(cut)) as u32,
                    ..*r
                })
            })
            .collect();
        super::append_page_runs(&mut merged, tail)?;
        Self::new(merged)
    }
}
impl Deref for ValidatedRuns {
    type Target = [AbsRun];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(super) fn validate(runs: &[AbsRun]) -> Option<()> {
    let mut previous = None;
    let mut bytes = 0u64;
    for r in runs {
        if r.count == 0 || r.matching_bytes == 0 {
            return None;
        }
        let end = r.start.checked_add(u64::from(r.count))?;
        if previous.is_some_and(|end| end > r.start) {
            return None;
        }
        bytes = bytes.checked_add(r.matching_bytes)?;
        if r.gap_bytes_before != GAP_UNKNOWN {
            bytes = bytes.checked_add(r.gap_bytes_before)?;
        }
        previous = Some(end);
    }
    Some(())
}

/// Boundary estimates retain whole-run bytes, as canonical clipping did.
pub(crate) struct RunWindow {
    owner: ValidatedRuns,
    indices: Range<usize>,
    from: u64,
    upto: u64,
}
impl RunWindow {
    pub(crate) fn new(owner: ValidatedRuns, from: u64, upto: u64) -> Self {
        let start = owner.partition_point(|r| r.start + r.count as u64 <= from);
        let end = if from >= upto {
            start
        } else {
            start + owner[start..].partition_point(|r| r.start < upto)
        };
        Self {
            owner,
            indices: start..end,
            from,
            upto,
        }
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = AbsRun> + '_ {
        self.owner[self.indices.clone()].iter().map(|r| {
            let start = r.start.max(self.from);
            let end = (r.start + r.count as u64).min(self.upto);
            AbsRun {
                start,
                count: (end - start) as u32,
                ..*r
            }
        })
    }
}

#[cfg(test)]
#[path = "validated/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "validated/properties.rs"]
mod properties;
