//! A stream's reads in layout 6 (DESIGN-6A §2.4).
//!
//! A handle's unsealed pages start at its applied boundary
//! (`StreamState::pending`); every offset below it is sealed, and its seal
//! published the chunks in the directory before the boundary moved
//! (`seal::steps`, step 5), so a read never meets a hole between the two
//! and needs no race check. A window `[from, to)` is served:
//! 1. below the boundary, from the ring when it holds the whole sealed
//!    part, else from the segment objects: the directory's chunks from the
//!    one holding `from`, one ranged GET per chunk (from the last anchor at
//!    or before the offset due to the first anchor past the window or the
//!    byte budget), at most `MAX_IN_FLIGHT` at once, every page admitted,
//!    tiled and clipped. A keyed read skips a chunk its key summary
//!    excludes without a GET (its offsets still count as consumed), and
//!    filters a fetched chunk's pages by their clear headers;
//! 2. from the boundary on, from the handle's unsealed pages in memory, up
//!    to the read's visibility (a durable read never sees a page past the
//!    durable frontier).
//!
//! A 404 for an object whose chunk the directory holds is retryable (the
//! directory names only objects whose PUT finished); a chunk that fails its
//! checks is corruption. A window offset no chunk holds below the boundary
//! ends the read short (an honest partial, never consumed).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use object_store::ObjectStoreExt;

use crate::crypto_page::{CheckedPage, page_extent};
use crate::shard::record::{FrameReadResult, PageSlice, RangeReadError, RecordCorruption};
use crate::shard::{Deliver, RingScan, ShardEngine, StreamHandle};
use crate::spill::directory::DirEntry;
use crate::spill::object::{self, CHUNK_CRC_LEN, CHUNK_HEADER_LEN, DIR_ENTRY_LEN};
use crate::spill::pending::PendingPage;

/// The most ranged GETs one read has in flight.
const MAX_IN_FLIGHT: usize = 8;
/// The most chunks one read fetches (a stream sealed in one-page chunks
/// would otherwise turn a wide window into thousands of GETs).
const MAX_CHUNKS_PER_READ: usize = 256;
/// The most chunks one keyed read looks at, skipped ones included.
const MAX_CHUNKS_LOOKED_AT: usize = 4096;
/// Attempts per GET on a transient store error (not on a 404).
const GET_ATTEMPTS: u32 = 3;

/// One planned ranged GET: bytes `[start, end)` of a chunk (from its
/// header), whose first page starts at offset `first` and whose pages end
/// just before offset `next`.
#[derive(Clone, Debug)]
struct Plan {
    entry: DirEntry,
    start: u32,
    end: u32,
    whole: bool,
    first: u64,
    next: u64,
}

/// One step of a sealed read.
enum Step {
    Fetch(Plan),
    /// A chunk the key summary excludes, consumed through this offset.
    Skip(u64),
}

/// One part of a read: what it served, and whether it reached its end.
struct Part {
    out: FrameReadResult,
    /// Stored bytes of every page inspected, served or not.
    inspected: usize,
    reached: bool,
}

impl Part {
    fn new() -> Self {
        Self {
            out: FrameReadResult::default(),
            inspected: 0,
            reached: false,
        }
    }

    /// Take `slice` (inspected; served when it matches `key_filter`);
    /// whether the budget is spent.
    fn take(&mut self, slice: PageSlice, key_filter: Option<&str>, max_bytes: usize) -> bool {
        self.inspected = self.inspected.saturating_add(slice.stored_len());
        self.out.last_offset = Some(slice.last());
        if key_filter.is_none_or(|key| key == slice.page().routing_key()) {
            self.out.frames.push(slice);
        }
        self.inspected >= max_bytes
    }
}

impl Part {
    /// Take the pages of one fetched chunk from `due` on, clipped to the
    /// window; whether the budget is spent (`reached` then set).
    fn take_pages(
        &mut self,
        pages: Vec<CheckedPage>,
        due: &mut u64,
        scan: RingScan,
        key_filter: Option<&str>,
    ) -> bool {
        for page in pages {
            let Some(slice) = PageSlice::clip(page, *due, scan.to) else {
                continue;
            };
            *due = slice.last().saturating_add(1);
            if self.take(slice, key_filter, scan.max_bytes) {
                self.reached = *due >= scan.to;
                return true;
            }
        }
        false
    }
}

impl ShardEngine {
    /// `window` of `handle`'s stream as a read at `deliver` sees it, at most
    /// `max_bytes` of stored pages inspected (the page that reaches them is
    /// kept, so the first page always fits). A page of another routing key
    /// than `key_filter` is inspected and not served; its records still
    /// count as consumed progress (`last_offset`).
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::read_stream_window; a poisoned stream state may hold a half-advanced boundary; recovering it could serve frames past a boundary that was never committed, whereas panicking fails this read, which then serves nothing instead of frames no commit backs"
    )]
    pub(crate) async fn read_stream_window(
        &self,
        handle: &StreamHandle,
        window: RingScan,
        key_filter: Option<&str>,
        deliver: Deliver,
    ) -> Result<FrameReadResult, RangeReadError> {
        self.refuse_closed_read(handle).await?;
        // The boundary and the unsealed pages above it are read under one
        // lock: a seal that commits after it drops pages from memory, but
        // only pages its directory entries already cover (TLA-018-F4).
        let (sealed_below, to, unsealed) = {
            let st = handle.state.lock().unwrap();
            let end = match deliver {
                Deliver::Durable => st.durable.next,
                Deliver::Applied => st.applied.next.max(st.durable.next),
            };
            let to = window.to.min(end);
            let start = window.from.max(st.applied.absorbed);
            let pages = unsealed_pages(&st.pending, start, to, window.max_bytes);
            (st.applied.absorbed, to, pages)
        };
        #[cfg(test)]
        self.layout.seal.before_window_read().await;
        let mut from = window.from;
        if from >= to {
            return Ok(FrameReadResult::default());
        }
        let mut part = Part::new();
        if from < sealed_below {
            let upto = to.min(sealed_below);
            let scan = RingScan {
                from,
                to: upto,
                max_bytes: window.max_bytes,
            };
            part = match self.ring_read(handle, scan, key_filter) {
                Some(hit) => Part {
                    inspected: hit.frames.iter().map(PageSlice::stored_len).sum(),
                    reached: hit.last_offset.and_then(|l| l.checked_add(1)) == Some(upto),
                    out: hit,
                },
                None => self.read_sealed(handle.hash, scan, key_filter).await?,
            };
            if !part.reached || part.inspected >= window.max_bytes {
                return Ok(part.out);
            }
            from = upto;
        }
        // An empty rest (the window ends at the boundary) reads nothing.
        let rest = RingScan {
            from,
            to,
            max_bytes: window.max_bytes,
        };
        read_unsealed(&unsealed, &mut part, rest, key_filter)?;
        Ok(part.out)
    }

    /// A closed engine serves nothing from memory: the read meets the
    /// store's close, as a store read would (a parked live session learns
    /// of a fence that way).
    async fn refuse_closed_read(&self, handle: &StreamHandle) -> Result<(), RangeReadError> {
        if !self.is_closed() && handle.owner.as_ptr() == Arc::as_ptr(&self.db) {
            return Ok(());
        }
        self.db.get(crate::shard::tail_key(&handle.hash)).await?;
        Err(RangeReadError::Store(slatedb::Error::unavailable(
            "the shard engine closed; retry".into(),
        )))
    }

    /// Step 1 from the segment objects.
    async fn read_sealed(
        &self,
        hash: [u8; 16],
        scan: RingScan,
        key_filter: Option<&str>,
    ) -> Result<Part, RangeReadError> {
        let sp = &self.layout.seal;
        self.seal_ready()
            .await
            .map_err(|e| RangeReadError::Store(slatedb::Error::unavailable(e.to_string())))?;
        sp.counters.sealed_reads.fetch_add(1, Ordering::Relaxed);
        let steps = self.plan_sealed(hash, scan, key_filter);
        if steps.is_empty() && scan.max_bytes != 0 {
            // A seal publishes its chunks before its commit moves the
            // boundary, and the open rebuilds them before any read: a window
            // below the boundary that no chunk covers lost them (#213).
            return Err(RecordCorruption::Missing {
                due: scan.from,
                to: scan.to,
            }
            .into());
        }
        let gets: Vec<_> = steps
            .iter()
            .filter_map(|step| match step {
                Step::Fetch(plan) => Some(self.get_chunk(plan)),
                Step::Skip(_) => None,
            })
            .collect();
        let mut bodies = stream::iter(gets).buffered(MAX_IN_FLIGHT);
        let mut part = Part::new();
        let mut due = scan.from;
        for step in &steps {
            let plan = match step {
                Step::Skip(through) => {
                    part.out.last_offset = Some(*through);
                    due = through.saturating_add(1);
                    continue;
                }
                Step::Fetch(plan) => plan,
            };
            let Some(body) = bodies.next().await else {
                break;
            };
            let served_before = part.out.frames.len();
            let pages = admit_plan(hash, plan, &body?)?;
            if part.take_pages(pages, &mut due, scan, key_filter) {
                return Ok(part);
            }
            if key_filter.is_some() && part.out.frames.len() == served_before {
                sp.counters.summary_misses.fetch_add(1, Ordering::Relaxed);
            }
        }
        part.reached = due >= scan.to;
        Ok(part)
    }

    /// The read's steps, from the chunk holding `scan.from`: a keyed read
    /// skips a chunk its key summary excludes (its offsets count as
    /// consumed, without a GET), every other chunk is fetched. The plan
    /// stops at the window's end, the byte budget, `MAX_CHUNKS_PER_READ`
    /// fetches, or an offset no chunk holds (never consumed).
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::plan_sealed; a poisoned directory may hold a half-inserted seal; recovering it could serve a sealed range from chunks no seal finished publishing, whereas panicking fails this read, which then serves nothing instead of frames no commit backs"
    )]
    fn plan_sealed(&self, hash: [u8; 16], scan: RingScan, key_filter: Option<&str>) -> Vec<Step> {
        let sp = &self.layout.seal;
        let entries = sp
            .directory
            .read()
            .unwrap()
            .from(&hash, scan.from, MAX_CHUNKS_LOOKED_AT);
        let mut steps = Vec::new();
        let (mut due, mut fetches) = (scan.from, 0usize);
        let mut budget = scan.max_bytes as u64;
        for entry in entries {
            if due >= scan.to || budget == 0 || fetches >= MAX_CHUNKS_PER_READ {
                break;
            }
            if entry.first > due || entry.last < due {
                break;
            }
            if let Some(key) = key_filter
                && !entry.keys.may_hold(key)
            {
                sp.counters.summary_skips.fetch_add(1, Ordering::Relaxed);
                let through = entry.last.min(scan.to - 1);
                due = through.saturating_add(1);
                steps.push(Step::Skip(through));
                continue;
            }
            let Some(plan) = Plan::of(entry, due, scan.to, budget) else {
                break;
            };
            budget = budget.saturating_sub(u64::from(plan.end - plan.start));
            due = plan.next;
            fetches += 1;
            steps.push(Step::Fetch(plan));
        }
        steps
    }

    /// One ranged GET, retried on a transient error; a 404 is retryable for
    /// the caller (the directory holds the chunk).
    async fn get_chunk(&self, plan: &Plan) -> Result<Bytes, RangeReadError> {
        let sp = &self.layout.seal;
        let path = object::object_path(
            &crate::sharddir::shard_db_path(&self.prefix),
            plan.entry.object,
        );
        let path = object_store::path::Path::from(path);
        let at = plan.entry.chunk_off;
        let range = at + u64::from(plan.start)..at + u64::from(plan.end);
        #[cfg(test)]
        sp.before_get().await;
        let mut attempt = 0;
        loop {
            attempt += 1;
            sp.counters.sealed_gets.fetch_add(1, Ordering::Relaxed);
            #[cfg(test)]
            sp.hooks
                .get_bytes
                .fetch_add(range.end - range.start, Ordering::Relaxed);
            match self.data_store.get_range(&path, range.clone()).await {
                Ok(body) if body.len() as u64 == range.end - range.start => return Ok(body),
                Ok(body) => {
                    return Err(sealed_corrupt(&format!(
                        "{path}: GET {range:?} returned {} B",
                        body.len()
                    )));
                }
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(RangeReadError::Store(slatedb::Error::unavailable(format!(
                        "sealed object {path} not found while the directory holds [{}, {}]",
                        plan.entry.first, plan.entry.last
                    ))));
                }
                Err(e) if attempt < GET_ATTEMPTS => {
                    tracing::debug!("sealed-range GET {path} {range:?} failed, retrying: {e}");
                    tokio::time::sleep(Duration::from_millis(5 << attempt)).await;
                }
                Err(e) => {
                    return Err(RangeReadError::Store(slatedb::Error::unavailable(format!(
                        "sealed-range GET {path} {range:?}: {e}"
                    ))));
                }
            }
        }
    }
}

impl Plan {
    /// The ranged GET of `entry`'s chunk for a read due at `due`: from the
    /// last anchor at or before `due` (or the first page) to the first
    /// anchor past it that starts at or after `upto` or `budget` page bytes
    /// on (or the chunk's end). The whole chunk, header and CRC included,
    /// when that is every page. None for an entry whose sizes cannot hold
    /// its pages.
    fn of(entry: DirEntry, due: u64, upto: u64, budget: u64) -> Option<Self> {
        let region =
            u32::try_from(CHUNK_HEADER_LEN + DIR_ENTRY_LEN * entry.page_count as usize).ok()?;
        let pages_end = entry
            .chunk_len
            .checked_sub(u32::try_from(CHUNK_CRC_LEN).ok()?)
            .filter(|end| *end >= region)?;
        let (page, first, start) = entry
            .start_for(due)
            .map_or((0, entry.first, region), |a| (a.page, a.offset, a.byte));
        // An anchor at or before the start holds offsets below `upto` and no
        // bytes past `start`, so it never ends the plan (the budget is
        // never zero here).
        let end_anchor = entry
            .anchors
            .iter()
            .find(|a| a.offset >= upto || u64::from(a.byte.saturating_sub(start)) >= budget);
        let (end, next) = end_anchor.map_or((pages_end, entry.last + 1), |a| (a.byte, a.offset));
        let whole = page == 0 && end_anchor.is_none();
        let (start, end) = if whole {
            (0, entry.chunk_len)
        } else {
            (start, end)
        };
        Some(Self {
            entry,
            start,
            end,
            whole,
            first,
            next,
        })
    }
}

fn sealed_corrupt(what: &str) -> RangeReadError {
    RangeReadError::Store(slatedb::Error::data(format!(
        "sealed range corrupt: {what}"
    )))
}

/// The plan's pages, admitted, in offset order from `plan.first` to just
/// before `plan.next`.
fn admit_plan(
    hash: [u8; 16],
    plan: &Plan,
    body: &Bytes,
) -> Result<Vec<CheckedPage>, RangeReadError> {
    let corrupt = |what: String| {
        sealed_corrupt(&format!(
            "{} chunk [{}, {}] of {:016x}/{:016x}: {what}",
            crate::crypto::hex(&hash[..4]),
            plan.entry.first,
            plan.entry.last,
            plan.entry.object.epoch,
            plan.entry.object.seq
        ))
    };
    let raw: Vec<(u64, Bytes)> = if plan.whole {
        let chunk = object::decode_chunk(body).map_err(|e| corrupt(format!("{e:?}")))?;
        // The pages are checked against the plan's first and last offsets
        // below; the stream and the page count only here.
        if chunk.hash != hash || chunk.pages.len() != plan.entry.page_count as usize {
            return Err(corrupt(
                "the chunk's header disagrees with its directory entry".into(),
            ));
        }
        chunk.pages
    } else {
        let mut pages = Vec::new();
        let mut at = 0usize;
        while let Some(rest) = body.get(at..).filter(|r| !r.is_empty()) {
            let (len, last) =
                page_extent(rest).map_err(|e| corrupt(format!("page at byte {at}: {e:?}")))?;
            pages.push((last, body.slice(at..at + len)));
            at += len;
        }
        pages
    };
    let mut next = plan.first;
    let mut out = Vec::with_capacity(raw.len());
    for (last, bytes) in raw {
        let page =
            CheckedPage::admit(bytes, last).map_err(|e| corrupt(format!("page {last}: {e:?}")))?;
        if page.first() != next {
            return Err(corrupt(format!(
                "offset {next} due, page [{}, {}]",
                page.first(),
                page.last()
            )));
        }
        next = page.last() + 1;
        out.push(page);
    }
    if next != plan.next {
        return Err(corrupt(format!(
            "pages end before {next}, the plan before {}",
            plan.next
        )));
    }
    Ok(out)
}

/// The unsealed pages holding `[start, to)` from `pending`, oldest first:
/// enough of them to pass `max_bytes` by one page (a read stops there).
pub(super) fn unsealed_pages(
    pending: &crate::spill::pending::PendingPages,
    start: u64,
    to: u64,
    max_bytes: usize,
) -> Vec<PendingPage> {
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for page in pending.from(start) {
        if page.0 >= to || bytes >= max_bytes {
            break;
        }
        bytes = bytes.saturating_add(page.2.len());
        out.push(page.clone());
    }
    out
}

/// Step 2: `[from, to)` from the unsealed pages the read captured with its
/// boundary, appended to `part`. They tile from the boundary, so a page
/// that does not continue the offsets is corruption.
fn read_unsealed(
    pages: &[PendingPage],
    part: &mut Part,
    window: RingScan,
    key_filter: Option<&str>,
) -> Result<(), RangeReadError> {
    let RingScan {
        from,
        to,
        max_bytes,
    } = window;
    let mut due = from;
    for (first, last, raw) in pages.iter().filter(|(_, last, _)| *last >= from) {
        if *first >= to {
            break;
        }
        let page = CheckedPage::admit(raw.clone(), *last).map_err(RecordCorruption::Page)?;
        let Some(slice) = PageSlice::clip(page, from, to) else {
            break;
        };
        if slice.first() != due {
            return Err(RecordCorruption::Misplaced {
                due,
                first: *first,
                last: *last,
            }
            .into());
        }
        due = slice.last().saturating_add(1);
        if part.take(slice, key_filter, max_bytes) {
            return Ok(());
        }
    }
    if due != to {
        return Err(RecordCorruption::Missing { due, to }.into());
    }
    part.reached = true;
    Ok(())
}
