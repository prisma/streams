//! L6 spike M4: the read path for sealed ranges (design B §1.3; SPIKE-PLAN
//! M4; DESIGN §3.2 items 6 and 8; rBc G2-V).
//!
//! `read_range` serves an unkeyed window `[from, upto)` below a stream's
//! absorbed boundary from the segment objects the sealer wrote:
//! 1. it scans the stream's index rows (`hash16 ‖ 'i' ‖ last`) from the key
//!    of `from` **at the read's own visibility**: Memory for an applied
//!    read, Remote for a durable one. A reader that adopted a boundary from
//!    the Memory view (`absorption_race` → `visible_absorbed(Applied)`) then
//!    finds the index rows the same group committed with the trims;
//! 2. it plans one ranged GET per chunk, its byte range taken from the row's
//!    256 KiB anchors: from the last anchor at or before `from` to the first
//!    anchor past `upto` or past the byte budget, so a read never fetches
//!    more than one anchor interval beyond its budget;
//! 3. it issues the GETs, at most `MAX_IN_FLIGHT` (8) at once, in order;
//! 4. it admits every page (`CheckedPage`), checks that the pages tile the
//!    offsets, and clips them to `[from, upto)` (`PageSlice::clip`).
//!
//! A whole chunk is verified by its CRC. Part of a chunk (from or to an
//! anchor) is not covered by one CRC: each page is admitted against the
//! offsets the row and the previous page fix, and decryption authenticates
//! every header field (the segment hash is AAD), so a misplaced or altered
//! page still fails, later.
//!
//! **Missing and gone data.** A window offset with no index row below the
//! boundary the caller gave is an honest partial (`completed = false`),
//! never "expired" and never consumed. A 404 for an object whose index row
//! is live is a retryable error. There is no segment cache yet: walkers and
//! lagging readers pay one GET per chunk per read (the conservative case).
//!
//! Throwaway code on the never-merged `l6-spike` branch.

#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use object_store::ObjectStoreExt;
use slatedb::config::{DurabilityLevel, ScanOptions};

use super::super::{Deliver, ShardEngine};
use crate::crypto_page::{CheckedPage, page_extent};
use crate::shard::record::{PageSlice, PageSlices};
use crate::spill::index::{IndexRow, index_key, parse_index_key};
use crate::spill::object::{self, CHUNK_CRC_LEN, CHUNK_HEADER_LEN, DIR_ENTRY_LEN};
use crate::spill::stats::ReadStats;

/// The most ranged GETs one read has in flight.
pub(crate) const MAX_IN_FLIGHT: usize = 8;
/// The most chunks one read plans (a stream sealed in one-page chunks
/// otherwise turns a 4,096-offset window into 4,096 GETs); a read that
/// reaches it is a partial.
pub(crate) const MAX_CHUNKS_PER_READ: usize = 256;
/// Attempts per GET on a transient store error (not on a 404).
const GET_ATTEMPTS: u32 = 3;

/// What one read served.
#[derive(Debug, Default)]
pub(crate) struct SpillRead {
    pub(crate) frames: PageSlices,
    /// The last record served (the consumed progress).
    pub(crate) last: Option<u64>,
    /// Whether the read served its whole window.
    pub(crate) completed: bool,
    /// Why it stopped short (diagnostics and tests).
    pub(crate) stop: Stop,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Stop {
    #[default]
    Reached,
    /// The byte budget, or `MAX_CHUNKS_PER_READ`.
    Budget,
    /// No index row for an offset below the boundary: an honest partial.
    MissingRow,
}

#[derive(Debug)]
pub(crate) enum SpillReadError {
    /// A 404 for an object whose index row is live: retry the read.
    Retryable(String),
    /// An index row or a chunk failed its checks.
    Corrupt(String),
    Store(String),
}

impl std::fmt::Display for SpillReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(m) => write!(f, "{RETRYABLE_PREFIX}{m}"),
            Self::Corrupt(m) => write!(f, "l6 spike: sealed range corrupt: {m}"),
            Self::Store(m) => write!(f, "l6 spike: sealed range store error: {m}"),
        }
    }
}

/// The message prefix of a retryable sealed-range error (the HTTP read
/// adapter answers it 503).
pub(crate) const RETRYABLE_PREFIX: &str = "l6 spike retryable: ";

/// One planned ranged GET: bytes `[start, end)` of a chunk (offsets from the
/// chunk's header), whose first page starts at offset `first` and whose
/// pages end just before offset `next`.
#[derive(Clone, Debug)]
struct Plan {
    row: IndexRow,
    start: u32,
    end: u32,
    whole: bool,
    first: u64,
    next: u64,
}

/// Which level a read at `visibility` sees index rows at.
fn index_durability(visibility: Deliver) -> DurabilityLevel {
    match visibility {
        Deliver::Durable => DurabilityLevel::Remote,
        Deliver::Applied => DurabilityLevel::Memory,
    }
}

/// Serve `[from, upto)` of stream `hash` from its sealed chunks, at most
/// `max_bytes` stored page bytes (the page that reaches them is kept, as a
/// history scan keeps it). `engine` must run the spill arm.
///
/// `lane` filters by routing key from the pages' clear headers, as the
/// tail scan does (`ShardEngine::scan_pages`): a page of another lane is
/// inspected, not served, and its records still count as consumed
/// progress (`last`). It is a scan, not a postings read: the caller passes
/// only the default lane (`Some("")`, which the product surface sends for
/// every unkeyed read); a keyed read of sealed data is out of the spike.
pub(crate) async fn read_range(
    engine: &ShardEngine,
    hash: [u8; 16],
    from: u64,
    upto: u64,
    visibility: Deliver,
    max_bytes: usize,
    lane: Option<&str>,
) -> Result<SpillRead, SpillReadError> {
    let stats = engine.spill_stats().map(|s| &s.reads);
    let t0 = Instant::now();
    if let Some(s) = stats {
        s.calls.fetch_add(1, Ordering::Relaxed);
    }
    let result = engine
        .spill_read(hash, from, upto, visibility, max_bytes, lane, stats)
        .await;
    if let Some(s) = stats {
        s.read_ms(u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX));
        if let Ok(read) = &result {
            let c = match read.stop {
                _ if read.completed => &s.completed,
                Stop::MissingRow => &s.missing_row_partials,
                Stop::Reached | Stop::Budget => &s.budget_partials,
            };
            c.fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

impl ShardEngine {
    #[expect(clippy::too_many_arguments, reason = "L6 spike: the reader's window, visibility, budget and lane")]
    async fn spill_read(
        &self,
        hash: [u8; 16],
        from: u64,
        upto: u64,
        visibility: Deliver,
        max_bytes: usize,
        lane: Option<&str>,
        stats: Option<&ReadStats>,
    ) -> Result<SpillRead, SpillReadError> {
        if from >= upto {
            return Ok(SpillRead {
                completed: true,
                ..SpillRead::default()
            });
        }
        let max_bytes = max_bytes.max(1);
        let (plans, stop) = self
            .spill_plan(hash, from, upto, visibility, max_bytes as u64)
            .await?;
        if let Some(s) = stats {
            s.rows.fetch_add(plans.len() as u64, Ordering::Relaxed);
        }
        let in_flight = AtomicU64::new(0);
        // The futures are built first: a `map` closure that borrows here
        // trips the higher-ranked `Send` check of the routes above.
        let gets: Vec<_> = plans
            .iter()
            .map(|plan| self.spill_get(plan, stats, &in_flight))
            .collect();
        let bodies: Vec<Result<Bytes, SpillReadError>> =
            stream::iter(gets).buffered(MAX_IN_FLIGHT).collect().await;
        let mut out = SpillRead {
            stop,
            ..SpillRead::default()
        };
        let mut due = from;
        let mut total = 0usize;
        for (plan, body) in plans.iter().zip(bodies) {
            let pages = admit_plan(hash, plan, &body?)?;
            if let Some(s) = stats {
                s.pages.fetch_add(pages.len() as u64, Ordering::Relaxed);
            }
            for page in pages {
                if page.last() < from {
                    continue;
                }
                let Some(slice) = PageSlice::clip(page, from, upto) else {
                    break;
                };
                if slice.first() != due {
                    return Err(SpillReadError::Corrupt(format!(
                        "{}: offset {due} due, a page serves {} first",
                        crate::crypto::hex(&hash[..4]),
                        slice.first()
                    )));
                }
                due = slice.last().saturating_add(1);
                total = total.saturating_add(slice.stored_len());
                out.last = Some(slice.last());
                if lane.is_none_or(|key| key == slice.page().routing_key()) {
                    out.frames.push(slice);
                } else if let Some(s) = stats {
                    s.lane_skipped_pages.fetch_add(1, Ordering::Relaxed);
                }
                if total >= max_bytes {
                    out.stop = Stop::Budget;
                    out.completed = due >= upto;
                    return Ok(out);
                }
            }
        }
        out.completed = out.stop == Stop::Reached && due >= upto;
        if out.stop == Stop::Reached && !out.completed {
            // The planned bytes ran out before the window did (a plan
            // over-counts pages before `from`): a budget partial.
            out.stop = Stop::Budget;
        }
        Ok(out)
    }

    /// Steps 1-2: the index rows from the key of `from`, at the read's
    /// visibility, and one ranged GET per chunk within the budget.
    async fn spill_plan(
        &self,
        hash: [u8; 16],
        from: u64,
        upto: u64,
        visibility: Deliver,
        max_bytes: u64,
    ) -> Result<(Vec<Plan>, Stop), SpillReadError> {
        // Index blocks are small and read again by every read of the
        // stream: cache them (the shard DB's block cache), as a production
        // reader would.
        let opts = ScanOptions {
            durability_filter: self.spill_index_durability(visibility),
            cache_blocks: true,
            ..ScanOptions::default()
        };
        let mut rows = self
            .db
            .scan_with_options(index_key(&hash, from)..=index_key(&hash, u64::MAX), &opts)
            .await
            .map_err(|e| SpillReadError::Store(e.to_string()))?;
        let mut plans = Vec::new();
        let mut due = from;
        let mut budget = max_bytes;
        loop {
            if due >= upto {
                return Ok((plans, Stop::Reached));
            }
            if budget == 0 || plans.len() >= MAX_CHUNKS_PER_READ {
                return Ok((plans, Stop::Budget));
            }
            let Some(kv) = rows
                .next()
                .await
                .map_err(|e| SpillReadError::Store(e.to_string()))?
            else {
                return Ok((plans, Stop::MissingRow));
            };
            let corrupt = |what: String| SpillReadError::Corrupt(format!("{}: {what}", crate::crypto::hex(&hash[..4])));
            let (row_hash, last) = parse_index_key(&kv.key).ok_or_else(|| corrupt("not an index key".into()))?;
            let row = IndexRow::decode(&kv.value).map_err(|e| corrupt(format!("index row {last}: {e:?}")))?;
            if row_hash != hash || row.last != last {
                return Err(corrupt(format!("index row keyed {last} holds [{}, {}]", row.first, row.last)));
            }
            if row.first > due {
                // No row holds `due`: the hole an index read at the wrong
                // visibility would see (rBc G2-V). Never consumed.
                return Ok((plans, Stop::MissingRow));
            }
            if row.last < due || (!plans.is_empty() && row.first != due) {
                return Err(corrupt(format!("index rows overlap at {due}: [{}, {}]", row.first, row.last)));
            }
            let plan = plan_chunk(row, due, upto, budget)?;
            budget = budget.saturating_sub(u64::from(plan.end - plan.start));
            due = plan.next;
            plans.push(plan);
        }
    }

    /// The level index rows are scanned at: the read's own (tests can force
    /// another, the negative control).
    fn spill_index_durability(&self, visibility: Deliver) -> DurabilityLevel {
        #[cfg(test)]
        #[expect(clippy::unwrap_used, reason = "L6 spike: test override")]
        if let Some(level) = self
            .spill
            .as_ref()
            .and_then(|sp| *sp.index_durability_override.lock().unwrap())
        {
            return level;
        }
        index_durability(visibility)
    }

    /// Step 3: one ranged GET, retried on a transient error; a 404 is
    /// retryable for the caller (the row is live).
    async fn spill_get(
        &self,
        plan: &Plan,
        stats: Option<&ReadStats>,
        in_flight: &AtomicU64,
    ) -> Result<Bytes, SpillReadError> {
        let now = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(s) = stats {
            s.in_flight_max.fetch_max(now, Ordering::Relaxed);
        }
        let result = self.spill_get_inner(plan, stats).await;
        in_flight.fetch_sub(1, Ordering::AcqRel);
        result
    }

    async fn spill_get_inner(&self, plan: &Plan, stats: Option<&ReadStats>) -> Result<Bytes, SpillReadError> {
        let path = object::object_path(&crate::sharddir::shard_db_path(&self.prefix), plan.row.object);
        let path = object_store::path::Path::from(path);
        let at = plan.row.chunk_off;
        let range = at + u64::from(plan.start)..at + u64::from(plan.end);
        #[cfg(test)]
        self.spill_test_get_hook().await;
        let mut attempt = 0;
        loop {
            attempt += 1;
            if let Some(s) = stats {
                s.gets.fetch_add(1, Ordering::Relaxed);
            }
            match self.data_store.get_range(&path, range.clone()).await {
                Ok(body) => {
                    if let Some(s) = stats {
                        s.get_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                        let c = if plan.whole { &s.whole_chunks } else { &s.partial_chunks };
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    if body.len() as u64 != range.end - range.start {
                        return Err(SpillReadError::Corrupt(format!(
                            "{path}: GET {range:?} returned {} B",
                            body.len()
                        )));
                    }
                    return Ok(body);
                }
                Err(object_store::Error::NotFound { .. }) => {
                    if let Some(s) = stats {
                        s.not_found_retryable.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(SpillReadError::Retryable(format!(
                        "{path} not found while its index row [{}, {}] is live",
                        plan.row.first, plan.row.last
                    )));
                }
                Err(e) if attempt < GET_ATTEMPTS => {
                    if let Some(s) = stats {
                        s.get_retries.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::debug!("L6 sealed-range GET {path} {range:?} failed, retrying: {e}");
                    tokio::time::sleep(Duration::from_millis(5 << attempt)).await;
                }
                Err(e) => {
                    if let Some(s) = stats {
                        s.get_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(SpillReadError::Store(format!("{path} {range:?}: {e}")));
                }
            }
        }
    }
}

/// The ranged GET of `row`'s chunk for a read now due at `due`: from the
/// last anchor at or before `due` (or the first page) to the first anchor
/// past it that starts at or after `upto` or `budget` page bytes on (or the
/// chunk's end). The whole chunk, header and CRC included, when that is
/// every page.
fn plan_chunk(row: IndexRow, due: u64, upto: u64, budget: u64) -> Result<Plan, SpillReadError> {
    let region = CHUNK_HEADER_LEN + DIR_ENTRY_LEN * row.page_count as usize;
    let region = u32::try_from(region).map_err(|_| SpillReadError::Corrupt("chunk directory too large".into()))?;
    let pages_end = row
        .chunk_len
        .checked_sub(CHUNK_CRC_LEN as u32)
        .filter(|end| *end >= region)
        .ok_or_else(|| SpillReadError::Corrupt(format!("chunk of {} B holds no pages", row.chunk_len)))?;
    let (page, first, start) = row
        .start_for(due)
        .map_or((0, row.first, region), |a| (a.page, a.offset, a.byte));
    let end_anchor = row.anchors.iter().find(|a| {
        a.page > page && (a.offset >= upto || u64::from(a.byte.saturating_sub(start)) >= budget)
    });
    let (end, next) = end_anchor.map_or((pages_end, row.last + 1), |a| (a.byte, a.offset));
    let whole = page == 0 && end_anchor.is_none();
    let (start, end) = if whole { (0, row.chunk_len) } else { (start, end) };
    Ok(Plan {
        row,
        start,
        end,
        whole,
        first,
        next,
    })
}

/// Step 4: the plan's pages, admitted, in offset order from `plan.first` to
/// just before `plan.next`.
fn admit_plan(hash: [u8; 16], plan: &Plan, body: &Bytes) -> Result<Vec<CheckedPage>, SpillReadError> {
    let corrupt = |what: String| {
        SpillReadError::Corrupt(format!(
            "{} chunk [{}, {}] of {:016x}/{:016x}: {what}",
            crate::crypto::hex(&hash[..4]),
            plan.row.first,
            plan.row.last,
            plan.row.object.epoch,
            plan.row.object.seq
        ))
    };
    let raw: Vec<(u64, Bytes)> = if plan.whole {
        let chunk = object::decode_chunk(body).map_err(|e| corrupt(format!("{e:?}")))?;
        if chunk.hash != hash
            || chunk.first != plan.row.first
            || chunk.last != plan.row.last
            || chunk.pages.len() != plan.row.page_count as usize
            || u64::from(chunk.stored) != plan.row.stored
        {
            return Err(corrupt("the chunk's header disagrees with its index row".into()));
        }
        chunk.pages
    } else {
        let mut pages = Vec::new();
        let mut at = 0usize;
        while at < body.len() {
            let rest = body.get(at..).unwrap_or_default();
            let (len, last) = page_extent(rest).map_err(|e| corrupt(format!("page at byte {at}: {e:?}")))?;
            pages.push((last, body.slice(at..at + len)));
            at += len;
        }
        pages
    };
    let mut next = plan.first;
    let mut out = Vec::with_capacity(raw.len());
    for (last, bytes) in raw {
        let page = CheckedPage::admit(bytes, last).map_err(|e| corrupt(format!("page {last}: {e:?}")))?;
        if page.first() != next {
            return Err(corrupt(format!("offset {next} due, page [{}, {}]", page.first(), page.last())));
        }
        next = page.last() + 1;
        out.push(page);
    }
    if next != plan.next {
        return Err(corrupt(format!("pages end before {next}, the plan before {}", plan.next)));
    }
    Ok(out)
}

/// Retry hint for adapters: whether an error message is a retryable
/// sealed-range error.
pub(crate) fn is_retryable(message: &str) -> bool {
    message.contains(RETRYABLE_PREFIX)
}

#[cfg(test)]
impl ShardEngine {
    /// Tests: wait on a held GET (`SpillState::get_hold`) before each GET.
    async fn spill_test_get_hook(&self) {
        let hold = self.spill.as_ref().and_then(|sp| {
            #[expect(clippy::unwrap_used, reason = "L6 spike: test hook")]
            sp.get_hold.lock().unwrap().clone()
        });
        if let Some(hold) = hold {
            hold.entered.fetch_add(1, Ordering::AcqRel);
            let mut released = hold.release.subscribe();
            let _ = released.wait_for(|r| *r).await;
        }
    }
}

/// Tests: GETs park until released.
#[cfg(test)]
pub(crate) struct GetHold {
    pub(crate) entered: AtomicU64,
    pub(crate) release: tokio::sync::watch::Sender<bool>,
}

#[cfg(test)]
impl GetHold {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicU64::new(0),
            release: tokio::sync::watch::Sender::new(false),
        })
    }

    pub(crate) fn release(&self) {
        self.release.send_replace(true);
    }
}
