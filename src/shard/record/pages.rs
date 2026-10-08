//! Shard-log pages as a read serves them. A page is admitted whole, but a
//! read window `[from, to)` may start or end inside it, so each admitted page
//! travels with the run of its records the window covers. Offsets stay the
//! unit everywhere: progress, density and the absorption-race check count
//! records, never pages.
use crate::crypto_page::CheckedPage;

/// One admitted page and the records `first..=last` of it a read serves.
/// The run is never empty and never leaves the page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageSlice {
    page: CheckedPage,
    first: u64,
    last: u64,
}

/// A slice's clear facts in the shape the absorption-race check and the
/// absorber's drain trace read: `offset` is the first served record.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SliceView {
    pub(crate) header: SliceHeader,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SliceHeader {
    pub(crate) offset: u64,
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

    /// How many records the slice serves.
    pub(crate) fn records(&self) -> u64 {
        self.last.saturating_sub(self.first).saturating_add(1)
    }

    /// The stored page bytes the slice was read from.
    pub(crate) fn stored_len(&self) -> usize {
        self.page.raw().len()
    }

    pub(crate) fn view(&self) -> SliceView {
        SliceView {
            header: SliceHeader { offset: self.first },
        }
    }
}

/// The page slices one read returned, in offset order. It dereferences to
/// the slices, but `len` counts the RECORDS they serve: a dense window
/// `[first, last]` has `len() == last - first + 1` however its records are
/// paged, which is what the absorption-race check compares.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PageSlices {
    slices: Vec<PageSlice>,
    records: usize,
}

impl PageSlices {
    pub(crate) fn push(&mut self, slice: PageSlice) {
        let records = usize::try_from(slice.records()).unwrap_or(usize::MAX);
        self.records = self.records.saturating_add(records);
        self.slices.push(slice);
    }

    /// The number of records the slices serve (not the number of slices).
    pub(crate) fn len(&self) -> usize {
        self.records
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.slices.is_empty()
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
