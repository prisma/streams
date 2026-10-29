//! Shard-log pages as a read serves them. A page is admitted whole, but a
//! read window `[from, to)` may start or end inside it, so each admitted page
//! travels with the run of its records the window covers. Offsets stay the
//! unit of progress: a read's consumed progress is its last record.
use crate::crypto_page::CheckedPage;

/// One admitted page and the records `first..=last` of it a read serves.
/// The run is never empty and never leaves the page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageSlice {
    page: CheckedPage,
    first: u64,
    last: u64,
}

impl PageSlice {
    /// The records of `page` inside the window `[from, to)`, or None when the
    /// page holds none of them.
    pub(crate) fn clip(page: CheckedPage, from: u64, to: u64) -> Option<Self> {
        let first = page.first().max(from);
        let last = page.last().min(to.checked_sub(1)?);
        (first <= last).then_some(Self { page, first, last })
    }

    pub(crate) fn page(&self) -> &CheckedPage {
        &self.page
    }

    /// The first served record.
    pub(crate) fn first(&self) -> u64 {
        self.first
    }

    /// The last served record: the progress a read of this slice consumes.
    pub(crate) fn last(&self) -> u64 {
        self.last
    }

    /// The stored page bytes the slice was read from.
    pub(crate) fn stored_len(&self) -> usize {
        self.page.raw().len()
    }
}

/// The page slices one read returned, in offset order; it dereferences to
/// the slices.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PageSlices {
    slices: Vec<PageSlice>,
}

impl PageSlices {
    pub(crate) fn push(&mut self, slice: PageSlice) {
        self.slices.push(slice);
    }
}

impl std::ops::Deref for PageSlices {
    type Target = [PageSlice];
    fn deref(&self) -> &[PageSlice] {
        &self.slices
    }
}

impl<'a> IntoIterator for &'a PageSlices {
    type Item = &'a PageSlice;
    type IntoIter = std::slice::Iter<'a, PageSlice>;
    fn into_iter(self) -> Self::IntoIter {
        self.slices.iter()
    }
}
