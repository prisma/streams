#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Checked stored-record admission and the result a stream read returns.
//! Corrupt stored pages fail before either matching or match-free progress
//! can be published. Layout 6 serves a stream's reads from its unsealed
//! pages in memory and its segment objects (`shard::seal::read`).
use super::{Deliver, ShardEngine, StreamHandle};
use crate::crypto_page::PageCorruption;
mod pages;
pub(crate) use pages::{PageSlice, PageSlices};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordCorruption {
    /// A stored page failed admission against its row key.
    Page(PageCorruption),
    /// A read met the page `[first, last]` where offset `due` was due: the
    /// pages below the absorbed boundary tile the offsets, so a page that
    /// starts before `due` overlaps the one before it and a page that
    /// starts after it follows a lost page.
    Misplaced { due: u64, first: u64, last: u64 },
    /// A read's pages ended at offset `due`, short of its window's end `to`:
    /// the pages that held `[due, to)` were lost.
    Missing { due: u64, to: u64 },
}
impl std::fmt::Display for RecordCorruption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stored record corruption: {self:?}")
    }
}
impl std::error::Error for RecordCorruption {}
impl From<RecordCorruption> for slatedb::Error {
    fn from(error: RecordCorruption) -> Self {
        Self::data(error.to_string())
    }
}

/// A range read's refusal split by owner (item 35): `Corrupt` is the row's
/// own bytes failing admission, so the same read fails the same way on
/// every retry; `Store` is whatever the database said, with its kind
/// (fence, unavailability, SST data) intact for the caller's policy.
#[derive(Debug)]
pub(crate) enum RangeReadError {
    Corrupt(RecordCorruption),
    Store(slatedb::Error),
}
impl From<RecordCorruption> for RangeReadError {
    fn from(error: RecordCorruption) -> Self {
        Self::Corrupt(error)
    }
}
impl From<slatedb::Error> for RangeReadError {
    fn from(error: slatedb::Error) -> Self {
        Self::Store(error)
    }
}
impl From<RangeReadError> for slatedb::Error {
    fn from(error: RangeReadError) -> Self {
        match error {
            RangeReadError::Corrupt(corruption) => corruption.into(),
            RangeReadError::Store(error) => error,
        }
    }
}

/// The pages holding offsets in [scan_from, durable_next), each sliced to
/// the records of the window, optionally filtered by routing key (clear page
/// metadata; no decryption needed). `last_offset` is the consumed progress:
/// the last record of the last inspected slice, matching or not.
#[derive(Default)]
pub(crate) struct FrameReadResult {
    pub frames: PageSlices,
    pub last_offset: Option<u64>,
}

#[cfg(test)]
#[expect(
    clippy::too_many_arguments,
    reason = "read_frames; a durable read names its engine, stream, start, key filter, byte budget and delivery separately as the application resolved them; a request struct would exist for this single boundary"
)]
pub(crate) async fn read_frames(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
    deliver: Deliver,
) -> Result<FrameReadResult, slatedb::Error> {
    read_frames_until(
        engine,
        handle,
        scan_from,
        u64::MAX,
        key_filter,
        max_bytes,
        deliver,
    )
    .await
}

/// Application pages bound examined offsets as well as selected bytes.
#[expect(
    clippy::too_many_arguments,
    reason = "read_frames_until; a bounded durable read names its engine, stream, window, key filter, byte budget and delivery separately as the application resolved them; a request struct would exist for this single boundary"
)]
pub(crate) async fn read_frames_until(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    scan_to: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
    deliver: Deliver,
) -> Result<FrameReadResult, slatedb::Error> {
    let window = super::RingScan {
        from: scan_from,
        to: scan_to,
        max_bytes,
    };
    Ok(engine
        .read_stream_window(handle, window, key_filter, deliver)
        .await?)
}
