//! Checked stored-record admission, shared by shard and history readers.
//! Invalid cache entries force a canonical storage read; corrupt stored rows
//! fail before either matching or match-free progress can be published.
use super::{Deliver, ShardEngine, StreamHandle, record_key};
use crate::crypto::{DecodedFrame, decode_frame};
mod checked;
pub use checked::CheckedFrame;
use slatedb::config::{DurabilityLevel, ScanOptions};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordCorruption {
    KeyWidth,
    Namespace,
    Frame,
    Offset { stored: u64, header: u64 },
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

/// Decode one complete row, including the exact namespace, tag and offset.
/// The prefix comes from the canonical key encoder for the selected segment.
pub(crate) fn decode_row<'a>(
    key: &[u8],
    prefix: &[u8],
    raw: &'a [u8],
) -> Result<DecodedFrame<'a>, RecordCorruption> {
    if key.len() != prefix.len() + 8 {
        return Err(RecordCorruption::KeyWidth);
    }
    if !key.starts_with(prefix) {
        return Err(RecordCorruption::Namespace);
    }
    let offset = u64::from_be_bytes(
        key[prefix.len()..]
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
        || frame.header_len + 4 + frame.ciphertext.len() != raw.len()
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

/// Frames with offset in [scan_from, durable_next), optionally filtered by
/// routing key (frame metadata; no decryption needed).
pub struct FrameReadResult {
    pub frames: Vec<CheckedFrame>,
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
            proof.owner.ptr_eq(&std::sync::Arc::downgrade(&engine.db))
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
pub async fn read_frames_range(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    scan_to: u64,
    max_bytes: usize,
) -> Result<FrameReadResult, slatedb::Error> {
    let hash = handle.hash;
    let mut out = FrameReadResult {
        frames: Vec::new(),
        last_offset: None,
        coverage: None,
    };
    if scan_from >= scan_to {
        return Ok(out);
    }
    // Durable-tail fast path: live readers chase offsets the ring still
    // holds; the scan below is the canonical fallback (restart, eviction,
    // lagging consumers, ring off).
    if let Some(hit) = engine.ring_read(handle, scan_from, scan_to, max_bytes) {
        return Ok(hit);
    }
    let prefix = record_key(&hash, 0);
    let range = record_key(&hash, scan_from)..record_key(&hash, scan_to);
    let mut iter = engine
        .db
        .scan_with_options(
            range,
            &ScanOptions {
                durability_filter: DurabilityLevel::Remote,
                read_ahead_bytes: 2 * 1024 * 1024,
                max_fetch_tasks: 4,
                ..Default::default()
            },
        )
        .await?;
    let mut total = 0usize;
    while let Some(kv) = iter.next().await? {
        let frame = CheckedFrame::from_row(&kv.key, &prefix[..17], kv.value)?;
        let off = frame.view().header.offset;
        total += frame.len();
        out.frames.push(frame);
        out.last_offset = Some(off);
        if total >= max_bytes {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
pub async fn read_frames(
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_frames_until(
    engine: &ShardEngine,
    handle: &StreamHandle,
    scan_from: u64,
    scan_to: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
    deliver: Deliver,
) -> Result<FrameReadResult, slatedb::Error> {
    let (hash, end) = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            Deliver::Durable => st.durable.next,
            // max() is defensive: `applied` loads equal to `durable`
            // and only the committer advances it, but a floor here
            // means Applied can never see LESS than a durable reader.
            Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        (handle.hash, end.min(scan_to))
    };
    let mut out = FrameReadResult {
        frames: Vec::new(),
        last_offset: None,
        coverage: None,
    };
    if scan_from >= end {
        return Ok(out);
    }
    // Durable-tail fast path (see read_frames_range). DURABLE reads
    // only: the ring holds only durable frames — an Applied read
    // chasing the just-applied suffix must scan (the suffix is
    // memtable-resident, so the scan costs no store round-trip).
    // Filtered reads use the keyed variant (#272): frame headers are
    // plaintext, so the lane filter runs on the ring copy and the
    // consumed offset still covers non-matching frames.
    if deliver == Deliver::Durable {
        let hit = match key_filter {
            None => engine.ring_read(handle, scan_from, end, max_bytes),
            Some(rk) => engine.ring_read_keyed(handle, scan_from, end, rk, max_bytes),
        };
        if let Some(hit) = hit {
            return Ok(hit);
        }
    }
    let prefix = record_key(&hash, 0);
    let range = record_key(&hash, scan_from)..record_key(&hash, end);
    let mut iter = engine
        .db
        .scan_with_options(
            range,
            &ScanOptions {
                durability_filter: match deliver {
                    Deliver::Durable => DurabilityLevel::Remote,
                    Deliver::Applied => DurabilityLevel::Memory,
                },
                read_ahead_bytes: 2 * 1024 * 1024,
                max_fetch_tasks: 4,
                ..Default::default()
            },
        )
        .await?;
    let mut total = 0usize;
    while let Some(kv) = iter.next().await? {
        let frame = CheckedFrame::from_row(&kv.key, &prefix[..17], kv.value)?;
        let off = frame.view().header.offset;
        total += frame.len();
        if !key_filter.is_some_and(|kf| frame.view().header.routing_key != kf) {
            out.frames.push(frame);
        }
        out.last_offset = Some(off);
        if total >= max_bytes {
            break;
        }
    }
    Ok(out)
}

// Scoped to the actual read future, so a held redundant marker operation does
// not block handle warming, the absorber, or another test's engine.
#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_MARKER_HOLD: (std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>);
}
