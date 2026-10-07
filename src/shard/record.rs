#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Checked stored-record admission, shared by shard and history readers.
//! Invalid cache entries force a canonical storage read; corrupt stored rows
//! fail before either matching or match-free progress can be published.
//!
//! The shard log stores layout 5 pages under `hash16 ‖ 'p' ‖ last offset`.
//! A read of `[from, to)` scans from the key of `from`, so the first page it
//! meets holds `from`, and stops at the first page that starts at or after
//! `to`; a page holds at most `PAGE_MAX_RECORDS` records, so the scan never
//! needs keys past `to - 1 + PAGE_MAX_RECORDS`.
use super::{Deliver, ShardEngine, StreamHandle};
use crate::crypto::{DecodedFrame, decode_frame};
use crate::crypto_page::{
    CheckedPage, PAGE_MAX_RECORDS, PageCorruption, shard_page_key, shard_page_prefix,
};
mod checked;
mod pages;
pub(crate) use checked::CheckedFrame;
pub(crate) use pages::{PageSlice, PageSlices};
use slatedb::config::{DurabilityLevel, ScanOptions};

/// How far past `to - 1` a window's scan may have to read to meet the page
/// holding `to - 1`.
const PAGE_SPAN: u64 = PAGE_MAX_RECORDS as u64;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordCorruption {
    KeyWidth,
    Namespace,
    Frame,
    Offset {
        stored: u64,
        header: u64,
    },
    /// A shard-log page failed admission against its row key.
    Page(PageCorruption),
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

/// Decode one complete row, including the exact namespace, tag and offset.
/// The prefix comes from the canonical key encoder for the selected segment.
pub(crate) fn decode_row<'a>(
    key: &[u8],
    prefix: &[u8],
    raw: &'a [u8],
) -> Result<DecodedFrame<'a>, RecordCorruption> {
    if key.len() != prefix.len().saturating_add(8) {
        return Err(RecordCorruption::KeyWidth);
    }
    if !key.starts_with(prefix) {
        return Err(RecordCorruption::Namespace);
    }
    let offset = u64::from_be_bytes(
        key.get(prefix.len()..)
            .ok_or(RecordCorruption::KeyWidth)?
            .try_into()
            .map_err(|_| RecordCorruption::KeyWidth)?,
    );
    decode_at(raw, offset)
}

/// Ring offsets and stored-row offsets obey the same frame contract. The
/// compatible byte decoder remains separate: admission additionally requires
/// a complete AEAD tag and forbids unclassified trailing bytes.
pub(crate) fn decode_at(raw: &[u8], offset: u64) -> Result<DecodedFrame<'_>, RecordCorruption> {
    let frame = decode_frame(raw).ok_or(RecordCorruption::Frame)?;
    if raw.len() > crate::crypto::MAX_ENCODED_FRAME
        || frame.ciphertext.len() < 16
        || frame
            .header_len
            .saturating_add(4)
            .saturating_add(frame.ciphertext.len())
            != raw.len()
    {
        return Err(RecordCorruption::Frame);
    }
    if frame.header.offset != offset {
        return Err(RecordCorruption::Offset {
            stored: offset,
            header: frame.header.offset,
        });
    }
    Ok(frame)
}

/// The pages holding offsets in [scan_from, durable_next), each sliced to
/// the records of the window, optionally filtered by routing key (clear page
/// metadata; no decryption needed). `last_offset` is the consumed progress:
/// the last record of the last inspected slice, matching or not.
#[derive(Default)]
pub(crate) struct FrameReadResult {
    pub frames: PageSlices,
    pub last_offset: Option<u64>,
    pub(super) coverage: Option<DurableRingCoverage>,
}

/// Retained admission fact, not a caller-supplied permission to skip checks.
/// Weak ownership binds this proof to one physical DB opening and keeps its
/// allocation identity unique even after retirement. `hash` binds incarnation.
pub(super) struct DurableRingCoverage {
    owner: std::sync::Weak<slatedb::Db>,
    hash: [u8; 16],
    from: u64,
    to: u64,
}
impl DurableRingCoverage {
    pub(super) fn new(engine: &ShardEngine, hash: [u8; 16], from: u64, to: u64) -> Self {
        Self {
            owner: std::sync::Arc::downgrade(&engine.db),
            hash,
            from,
            to,
        }
    }
}
impl FrameReadResult {
    pub(crate) fn proves_durable_ring(
        &self,
        engine: &ShardEngine,
        hash: [u8; 16],
        from: u64,
    ) -> bool {
        self.coverage.as_ref().is_some_and(|proof| {
            proof.owner.as_ptr() == std::sync::Arc::as_ptr(&engine.db)
                && proof.hash == hash
                && proof.from == from
                && self.last_offset.and_then(|last| last.checked_add(1)) == Some(proof.to)
                && proof.to > from
        })
    }
}

/// Range-bounded frame read: scans `[scan_from, scan_to)` regardless of the
/// durable frontier. Offsets below the frontier are dense, so disjoint
/// ranges partition the log exactly — the absorber issues several of these
/// concurrently to hide per-chunk object-store latency (a serial 8 MB chunk
/// loop absorbed ~10k rec/s against a 150k rec/s ingest; bench 2026-07-14).
/// The absorber's windows start and end on page edges (the absorbed
/// boundary and the durable frontier are both page edges), so each slice it
/// receives is a whole page.
pub(crate) async fn read_frames_range(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    scan_to: u64,
    max_bytes: usize,
) -> Result<FrameReadResult, RangeReadError> {
    if scan_from >= scan_to {
        return Ok(FrameReadResult::default());
    }
    // Durable-tail fast path: live readers chase offsets the ring still
    // holds; the scan below is the canonical fallback (restart, eviction,
    // lagging consumers, ring off).
    let window = super::RingScan {
        from: scan_from,
        to: scan_to,
        max_bytes,
    };
    if let Some(hit) = engine.ring_read(handle, window, None) {
        return Ok(hit);
    }
    engine
        .scan_pages(handle.hash, window, None, DurabilityLevel::Remote)
        .await
}

impl ShardEngine {
    /// The canonical page scan of `window` at `durability`: every page that
    /// holds a record of `[from, to)`, sliced to the window, until the
    /// stored page bytes reach `max_bytes` (the page that reaches them is
    /// kept, so the first page always fits). A page of another routing key
    /// than `key_filter` is inspected and skipped without decrypting; its
    /// records still count as consumed progress.
    pub(super) async fn scan_pages(
        &self,
        hash: [u8; 16],
        window: super::RingScan,
        key_filter: Option<&str>,
        durability: DurabilityLevel,
    ) -> Result<FrameReadResult, RangeReadError> {
        let super::RingScan {
            from,
            to,
            max_bytes,
        } = window;
        let mut out = FrameReadResult::default();
        if from >= to {
            return Ok(out);
        }
        let prefix = shard_page_prefix(&hash);
        let bound = to.saturating_sub(1).saturating_add(PAGE_SPAN);
        let range = shard_page_key(&hash, from)..shard_page_key(&hash, bound);
        let mut iter = self
            .db
            .scan_with_options(
                range,
                &ScanOptions {
                    durability_filter: durability,
                    read_ahead_bytes: 2 * 1024 * 1024,
                    max_fetch_tasks: 4,
                    ..Default::default()
                },
            )
            .await?;
        let mut total = 0usize;
        while let Some(kv) = iter.next().await? {
            let page = CheckedPage::from_row(&kv.key, &prefix, kv.value)
                .map_err(RecordCorruption::Page)?;
            // The first page that starts at or after `to` ends the window.
            let Some(slice) = PageSlice::clip(page, from, to) else {
                break;
            };
            total = total.saturating_add(slice.stored_len());
            out.last_offset = Some(slice.last());
            if key_filter.is_none_or(|key| key == slice.page().routing_key()) {
                out.frames.push(slice);
            }
            if total >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// `window` as a read at `deliver` sees it. DURABLE reads try the ring
    /// first: it holds only durable pages, so an Applied read chasing the
    /// just-applied suffix scans (the suffix is memtable-resident, so the
    /// scan costs no store round-trip). Filtered reads use the keyed ring
    /// read (#272): page headers are plaintext, so the lane filter runs on
    /// the ring copy and the consumed offset still covers non-matching pages.
    async fn read_window(
        &self,
        handle: &StreamHandle,
        window: super::RingScan,
        key_filter: Option<&str>,
        deliver: Deliver,
    ) -> Result<FrameReadResult, slatedb::Error> {
        if window.from >= window.to {
            return Ok(FrameReadResult::default());
        }
        if deliver == Deliver::Durable
            && let Some(hit) = self.ring_read(handle, window, key_filter)
        {
            return Ok(hit);
        }
        Ok(self
            .scan_pages(handle.hash, window, key_filter, deliver.durability())
            .await?)
    }
}

/// Tail repair (R26-4) over pages: the stored bytes of the pages holding
/// `[absorbed, next)`. The pages must hold exactly those records, page after
/// page: a missing page inside the unabsorbed range is corruption, and
/// repairing over it would bake the hole into the ledger, so the engine
/// open fails instead.
pub(super) async fn unabsorbed_page_bytes(
    db: &slatedb::Db,
    hash: &[u8; 16],
    absorbed: u64,
    next: u64,
) -> anyhow::Result<u64> {
    let prefix = shard_page_prefix(hash);
    let mut rows = db
        .scan(shard_page_key(hash, absorbed)..shard_page_key(hash, next))
        .await?;
    let (mut sum, mut expected) = (0u64, absorbed);
    while let Some(row) = rows.next().await? {
        let page = CheckedPage::from_row(&row.key, &prefix, row.value)
            .map_err(|corruption| anyhow::anyhow!("tail repair: page {corruption:?}"))?;
        let starts_inside = expected == absorbed && page.first() <= absorbed;
        anyhow::ensure!(
            starts_inside || page.first() == expected,
            "tail repair found a page [{}, {}] where offset {expected} was due",
            page.first(),
            page.last(),
        );
        sum = sum
            .checked_add(page.raw().len() as u64)
            .ok_or_else(|| anyhow::anyhow!("tail repair overflow"))?;
        expected = page
            .last()
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("tail repair overflow"))?;
    }
    anyhow::ensure!(
        expected == next,
        "tail repair found pages for [{absorbed}, {expected}) of [{absorbed}, {next})",
    );
    Ok(sum)
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

impl Deliver {
    /// The shard-log view a read at this visibility observes. A tail scan and
    /// the absorbed boundary that revalidates it must share it (TLA-018-F1).
    fn durability(self) -> DurabilityLevel {
        match self {
            Deliver::Durable => DurabilityLevel::Remote,
            Deliver::Applied => DurabilityLevel::Memory,
        }
    }
}

impl ShardEngine {
    /// `(absorbed, history_v2)` from the stored tail row as a `visibility`
    /// read sees it: the strongest boundary a tail scan at that visibility
    /// can have observed trims for. An applied scan sees applied trims whose
    /// advance is not yet Remote-durable, so revalidating it against the
    /// Remote row would accept its hole as consumed (TLA-018-F1). The
    /// published handle state is NOT enough: trim deletes become scan-visible
    /// when their batch is written (applied) or durable, while `handle.state`
    /// advances only at publication or dispatch, which can lag arbitrarily
    /// under load (2026-07-27 boundary-race DST failure). Returned TOGETHER:
    /// a reader adopting a boundary with a stale in-memory layout flag would
    /// refuse a v2 history range as v1 (observed in the first-absorption
    /// flush-to-dispatch window).
    pub(crate) async fn visible_absorbed(
        &self,
        hash: &[u8; 16],
        visibility: Deliver,
    ) -> Result<(u64, bool), slatedb::Error> {
        #[cfg(test)]
        if let Ok((entered, release)) = TEST_MARKER_HOLD.try_with(Clone::clone) {
            entered.notify_one();
            release.notified().await;
        }
        let v = self
            .db
            .get_with_options(
                super::tail_key(hash),
                &slatedb::config::ReadOptions {
                    durability_filter: visibility.durability(),
                    ..Default::default()
                },
            )
            .await?;
        Ok(v.map(|b| super::stored_tail(&b))
            .transpose()?
            .map_or((0, false), |t| (t.absorbed, t.history_v2)))
    }
}

/// Application pages bound examined offsets as well as selected bytes.
#[expect(
    clippy::too_many_arguments,
    reason = "read_frames_until; a bounded durable read names its engine, stream, window, key filter, byte budget and delivery separately as the application resolved them; a request struct would exist for this single boundary"
)]
#[expect(
    clippy::unwrap_used,
    reason = "read_frames_until; a poisoned handle state may hold a partially advanced boundary; recovering it could serve frames past a boundary that was never committed"
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
    let end = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            Deliver::Durable => st.durable.next,
            // max() is defensive: `applied` loads equal to `durable`
            // and only the committer advances it, but a floor here
            // means Applied can never see LESS than a durable reader.
            Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        end.min(scan_to)
    };
    // The ring serves DURABLE windows only; see `read_window`.
    let window = super::RingScan {
        from: scan_from,
        to: end,
        max_bytes,
    };
    engine
        .read_window(handle, window, key_filter, deliver)
        .await
}

// Scoped to the actual read future, so a held redundant marker operation does
// not block handle warming, the absorber, or another test's engine.
#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_MARKER_HOLD: (std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>);
}

#[cfg(kani)]
mod proofs;
