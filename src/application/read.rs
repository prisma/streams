//! Canonical bounded segment read plan and page. HTTP and SSE render this
//! result; scanned progress never depends on the number of matching records.
use super::read_budget::{MAX_SCAN_BATCH_BYTES, PageBudget, SCAN_WINDOW};
use super::read_keys::ReadKeys;
use crate::crypto::StreamKey;
use crate::shard::{Deliver, ShardEngine, StreamHandle};
#[cfg(test)]
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Watermarks {
    pub durable: u64,
    pub applied: u64,
}

/// A read plan binds one physical incarnation/segment to its exact visibility
/// and resource budget. Segment handles carry the validated physical identity;
/// topology/lineage coordinators construct one plan per selected span.
pub(crate) struct ReadPlan<'a> {
    key: &'a StreamKey,
    epoch: &'a [u8; 16],
    handle: &'a Arc<StreamHandle>,
    engine: &'a Arc<ShardEngine>,
    range: ReadRange,
    selector: Option<&'a str>,
    max_bytes: usize,
    visibility: Deliver,
}

impl<'a> ReadPlan<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "ReadPlan::segment; the constructor takes every field of the plan it builds, as the caller resolved them; a builder would restate the plan's own fields"
    )]
    pub(crate) fn segment(
        key: &'a StreamKey,
        epoch: &'a [u8; 16],
        handle: &'a Arc<StreamHandle>,
        engine: &'a Arc<ShardEngine>,
        range: ReadRange,
        selector: Option<&'a str>,
        max_bytes: usize,
        visibility: Deliver,
    ) -> Self {
        Self {
            key,
            epoch,
            handle,
            engine,
            range,
            selector,
            max_bytes,
            visibility,
        }
    }
    pub(crate) async fn execute(self) -> Result<ReadPage, String> {
        execute_segment(self).await
    }
}

/// Compatibility call shape for in-crate engine probes; there is one executor.
#[expect(
    clippy::too_many_arguments,
    reason = "read_merged; the compatibility call shape lists the segment plan's fields positionally for the in-crate engine probes; a plan struct is what it builds"
)]
pub(crate) async fn read_merged(
    key: &StreamKey,
    epoch: &[u8; 16],
    handle: &Arc<StreamHandle>,
    engine: &Arc<ShardEngine>,
    from: u64,
    selector: Option<&str>,
    max_bytes: usize,
    visibility: Deliver,
) -> Result<ReadPage, String> {
    ReadPlan::segment(
        key,
        epoch,
        handle,
        engine,
        ReadRange::open(from),
        selector,
        max_bytes,
        visibility,
    )
    .execute()
    .await
}

impl ReadPage {
    /// Exclusive consumed position, including match-free scanned ranges.
    pub(crate) fn scanned_through(&self, start: u64) -> u64 {
        self.last
            .map(|last| last.saturating_add(1))
            .unwrap_or(start)
    }
    /// A reconnect must never promise that an applied-only suffix survived.
    pub(crate) fn durable_resume(&self, start: u64) -> u64 {
        self.scanned_through(start).min(self.watermarks.durable)
    }
}

pub(crate) use super::read_batch::{PlainBatch, PlainRec};
pub(crate) use super::read_continuation::{Continuation, ObservationKey, WriterHistory};
use super::read_decode::decode_frames_into;
pub(crate) use super::read_range::ReadRange;

pub(crate) struct ReadPage {
    pub(crate) watermarks: Watermarks,
    pub(crate) recs: PlainBatch,
    pub(crate) last: Option<u64>,
    pub(crate) end: u64,
    pub(crate) completed: bool,
}

// The read itself, free of `AppState` so the simulation harness can call
// the production reader instead of reimplementing it (`src/dst/runtime.rs`).
// A second copy would be a copy that can drift, and drift here means the
// oracle stops testing what production does.
#[expect(
    clippy::unwrap_used,
    reason = "execute_segment; a poisoned stream state may hold a half-advanced durable or applied frontier; recovering it could serve a page past a length never made durable or applied"
)]
async fn execute_segment(plan: ReadPlan<'_>) -> Result<ReadPage, String> {
    let ReadPlan {
        key,
        epoch,
        handle,
        engine,
        range,
        selector: key_filter,
        max_bytes,
        visibility: deliver,
    } = plan;
    let scan_from = range.from;
    // The sub-stream identity (AAD): for total-order streams this is the
    // incarnation hash; for per-key streams, the segment hash. Either way
    // it's the handle's identity.
    let hash = handle.hash;
    let (end, watermarks) = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            crate::shard::Deliver::Durable => st.durable.next,
            crate::shard::Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        (
            end,
            Watermarks {
                durable: st.durable.next,
                applied: st.applied.next.max(st.durable.next),
            },
        )
    };
    let read_end = end.min(range.end);
    let mut out = ReadPage {
        watermarks,
        recs: PlainBatch::default(),
        last: None,
        end,
        completed: true,
    };
    let mut budget = PageBudget::new(max_bytes);
    let mut subkeys = ReadKeys::new(key, epoch, hash);
    // Layout 6 reads sealed offsets from the segment objects and the rest
    // from the handle's unsealed pages; a seal publishes its chunks before
    // it moves the boundary, so the read meets no hole between the two.
    if scan_from < read_end {
        let part = crate::shard::record::read_frames_until(
            engine,
            handle,
            scan_from,
            scan_from.saturating_add(SCAN_WINDOW).min(read_end),
            key_filter,
            budget.remaining().min(MAX_SCAN_BATCH_BYTES),
            deliver,
        )
        .await
        .map_err(|e| e.to_string())?;
        let decoded_all = decode_frames_into(&part.frames, &mut subkeys, &mut out, &mut budget)?;
        if decoded_all && let Some(last) = part.last_offset {
            out.last = Some(last);
        }
    }
    let consumed_next = out.last.map(|o| o + 1).unwrap_or(scan_from);
    out.completed = consumed_next >= read_end;
    // Test builds: an unfiltered read must NEVER emit a gapped page.
    #[cfg(test)]
    if key_filter.is_none() {
        let mut expect = scan_from;
        for r in &out.recs {
            assert!(
                r.off <= expect,
                "read emitted a gap: expected <= {expect}, got {} (scan_from {scan_from}, last {:?})",
                r.off,
                out.last
            );
            expect = r.off + 1;
        }
        if let Some(l) = out.last {
            assert!(
                l < expect,
                "read over-claimed: last {l} beyond served {expect} (scan_from {scan_from})"
            );
        }
    }
    Ok(out)
}

use crate::registry::{Registry, StreamDesc};
use crate::shard_directory::ShardDirectory;

/// Capability for completing a published topology transition. The reader can
/// request that one operation but cannot reach transport or lifecycle state.
pub(crate) trait TopologyResume: Send + Sync {
    fn schedule(
        &self,
        descriptor: &StreamDesc,
    ) -> Result<super::request_work::Ticket, super::request_work::WorkError>;
}

/// The query owner: only catalog, physical readers, peer routing and key cache
/// are retained. Authorization and response lifetimes stay with adapters.
pub(crate) struct ReadService {
    pub(crate) registry: Arc<Registry>,
    pub(crate) shards: ShardDirectory,
    pub(crate) peer: crate::peer::PeerClient,
    pub(crate) ownership: crate::ownership::OwnershipService,
    pub(crate) keys: Arc<crate::history::KeyCache>,
    pub(crate) topology: Arc<dyn TopologyResume>,
}

impl ReadService {
    #[expect(
        clippy::too_many_arguments,
        reason = "ReadService::new; the service is assembled from its registry, shard directory, peer, ownership and topology collaborators as bootstrap wires them; a builder would restate the service's own fields"
    )]
    pub(crate) fn new(
        registry: Arc<Registry>,
        shards: ShardDirectory,
        peer: crate::peer::PeerClient,
        ownership: crate::ownership::OwnershipService,
        keys: Arc<crate::history::KeyCache>,
        topology: Arc<dyn TopologyResume>,
    ) -> Self {
        Self {
            registry,
            shards,
            peer,
            ownership,
            keys,
            topology,
        }
    }
    pub(crate) async fn read_stitched(
        &self,
        desc: &StreamDesc,
        key: &StreamKey,
        range: ReadRange,
        max_bytes: usize,
    ) -> Result<ReadPage, String> {
        read_stitched(self, desc, key, range, max_bytes).await
    }
    /// (engine, handle) for a stream's sole segment identity.
    pub(crate) async fn handle_of(
        &self,
        desc: &StreamDesc,
    ) -> Result<(Arc<ShardEngine>, Arc<crate::shard::StreamHandle>), String> {
        let ro = desc.resolve_segment("");
        let engine = self
            .shards
            .resolve(&ro.shard_route, crate::shard_directory::Adoption::Internal)
            .await
            .map_err(|e| format!("engine unavailable: {e:?}"))?;
        let handle = engine
            .stream_handle(ro.identity)
            .await
            .map_err(|e| e.to_string())?;
        Ok((engine, handle))
    }
}
/// A fork's ancestor chain, self-first: (descriptor, fork boundary,
/// epoch bytes). boundary = where the entry's OWN records begin.
/// Soft-deleted/expired ancestors still serve (their data backs this
/// fork); a hard-deleted ancestor is an integrity error.
type ForkChain = Vec<(StreamDesc, u64, [u8; 16])>;
type ForkChainFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<ForkChain, String>> + Send>>;
fn fork_chain_of(state: &ReadService, desc: &StreamDesc) -> ForkChainFuture {
    let state_reg = state.registry.clone();
    let desc = desc.clone();
    Box::pin(async move {
        // Bounded, cycle-free, and epoch-checked (audit P0): a stale
        // reference must be an integrity error, never a silent read of
        // a RECREATED source incarnation.
        const MAX_FORK_DEPTH: usize = 64;
        let mut chain: Vec<(StreamDesc, u64, [u8; 16])> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cur = desc;
        loop {
            if chain.len() >= MAX_FORK_DEPTH {
                return Err("fork chain exceeds the maximum depth".into());
            }
            if !seen.insert(format!("{}\u{0}{}", cur.name, cur.stream_epoch)) {
                return Err("fork chain contains a cycle".into());
            }
            let boundary = cur.forked_from.as_ref().map(|f| f.fork_offset).unwrap_or(0);
            let epoch = cur.epoch();
            let parent = cur.forked_from.as_ref().map(|f| {
                (
                    f.source.clone(),
                    cur.ref_in_project(&f.source),
                    f.source_epoch.clone(),
                )
            });
            chain.push((cur, boundary, epoch));
            let Some((src, src_ref, want_epoch)) = parent else {
                break;
            };
            let d = match state_reg.get(&src_ref).await {
                Ok(Some(d)) if !d.deleted => d,
                _ => return Err(format!("fork source '{src}' is gone")),
            };
            if !want_epoch.is_empty() && d.stream_epoch != want_epoch {
                return Err(format!(
                    "fork source '{src}' is a different incarnation                              (expected {want_epoch}, found {})",
                    d.stream_epoch
                ));
            }
            cur = d;
        }
        Ok(chain)
    })
}

/// Stitched fork read (pinned DS fork contract): records [from, ...)
/// in the stream's OWN offset numbering, served from the ancestor
/// chain below each fork boundary and from the stream itself at and
/// above its boundary. `end`/`completed` describe the OWN tail.
/// `last` is the CONSUMED boundary (last scanned offset), not the last
/// matching record: match-free scanned ranges and drained ancestors
/// count as progress (follow-up review finding 6), so filtered
/// callers never rescan a range the chain already proved empty.
#[expect(
    clippy::unwrap_used,
    reason = "read_stitched; a poisoned stream state may hold a half-advanced durable frontier; recovering it could stitch a page past a length never made durable"
)]
pub(crate) async fn read_stitched(
    state: &ReadService,
    desc: &StreamDesc,
    key: &StreamKey,
    range: ReadRange,
    max_bytes: usize,
) -> Result<ReadPage, String> {
    let from = range.from;
    let chain = fork_chain_of(state, desc).await?;
    // Every hop must accept the presented key (uniform-key chains; a
    // cross-key fork chain would decrypt garbage, so it is an error).
    for (d, _, _) in &chain {
        if key.fingerprint(&d.epoch()) != d.key_fingerprint {
            return Err("wrong key for a fork ancestor".into());
        }
    }
    // Own tail state.
    let (own_engine, own_handle) = state.handle_of(&chain[0].0).await?;
    let own_end = own_handle.state.lock().unwrap().durable.next;
    let mut out = ReadPage {
        watermarks: crate::application::read::Watermarks {
            durable: own_end,
            applied: own_end,
        },
        recs: PlainBatch::default(),
        last: None,
        end: own_end,
        completed: false,
    };
    let mut budget = PageBudget::new(max_bytes);
    let mut cursor = from;
    for _ in 0..(chain.len() * 4 + 8) {
        if cursor >= range.end {
            out.completed = true;
            break;
        }
        if budget.full() {
            break;
        }
        // Owner of `cursor`: the deepest entry whose boundary <= cursor.
        let Some(idx) = chain.iter().position(|(_, b, _)| *b <= cursor) else {
            return Err("fork chain has no owner for offset".into());
        };
        // Cap: the smallest child boundary above the cursor.
        let cap = chain[..idx]
            .iter()
            .map(|(_, b, _)| *b)
            .min()
            .unwrap_or(u64::MAX)
            .min(range.end);
        let (d, _, epoch) = &chain[idx];
        let (engine, handle) = if idx == 0 {
            (own_engine.clone(), own_handle.clone())
        } else {
            state.handle_of(d).await?
        };
        state.keys.put(handle.hash, key.clone(), *epoch);
        // The DEFAULT key only. `None` here meant "every routing key",
        // so a raw fork of a collection that product clients had
        // written keyed records to replayed all of them through the
        // standards route — the one surface whose contract is that it
        // IS the default-key stream.
        let part = ReadPlan::segment(
            key,
            epoch,
            &handle,
            &engine,
            ReadRange::bounded(cursor, cap),
            Some(""),
            budget.remaining(),
            crate::shard::Deliver::Durable,
        )
        .execute()
        .await?;
        // CONSUMED progress (finding 6): read_merged's `last` advances
        // over scanned NON-MATCHING ranges inside this ancestor, so the
        // child's consumed position moves by the SCAN boundary — capped
        // at the next owner's boundary, never by emitted records alone.
        let scanned_after = part.last.map(|l| l + 1).unwrap_or(cursor);
        let consumed_here = scanned_after.min(cap);
        let before = cursor;
        let admission = out
            .recs
            .append_selected(part.recs, cursor..cap, None, &mut budget, 0);
        if let Some(withheld) = admission.withheld {
            out.last = withheld.checked_sub(1);
            out.completed = false;
            return Ok(out);
        }
        let emitted = admission.last.is_some();
        if let Some(last) = admission.last {
            cursor = last + 1;
        }
        // Match-free scanned ranges are consumed progress: the child
        // never revisits them.
        cursor = cursor.max(consumed_here);
        if cursor > from {
            out.last = Some(cursor - 1);
        }
        if idx == 0 {
            // Own range: read_merged's completion IS the answer.
            out.end = part.end;
            out.completed = part.completed;
            break;
        }
        if part.completed || cursor >= cap {
            // Corruption guard (checked BEFORE the hop — the old order
            // compared AFTER forcing cursor to cap, which could never
            // be true): a completed ancestor whose durable end sits
            // below the fork boundary means records were lost —
            // surface it rather than silently hopping to the child.
            if part.completed && scanned_after < cap {
                return Err("fork ancestor ended below the fork boundary".into());
            }
            // Ancestor drained to the cap (or its whole range): hop to
            // the next owner at the cap.
            cursor = cursor.max(cap);
            if cursor > from {
                out.last = Some(cursor - 1);
            }
            continue;
        }
        if !emitted && cursor == before {
            // Budget too small for one record and no scanned progress:
            // honest partial.
            break;
        }
    }
    Ok(out)
}

/// A validated descriptor snapshot is the complete authority for a read's
/// physical spans. Epoch and topology cannot be supplied independently.
#[derive(Clone)]
pub(crate) struct ReadTopology {
    pub(crate) descriptor: StreamDesc,
    pub(crate) spans: Vec<crate::segmap::SegmentDesc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadPosition {
    pub(crate) segment: u32,
    /// Exclusive consumed position in this segment, never record count.
    pub(crate) after: u64,
}

impl ReadTopology {
    pub(crate) fn new(descriptor: &StreamDesc, selector: Option<&str>) -> Self {
        let mut spans: Vec<_> = descriptor
            .segments
            .as_ref()
            .map(|m| m.lineage().into_iter().cloned().collect())
            .unwrap_or_default();
        if spans.is_empty() {
            let resolved = descriptor.resolve_segment(selector.unwrap_or(""));
            spans.push(crate::segmap::SegmentDesc {
                seg_id: resolved.seg_id,
                lo: 0,
                hi: crate::segmap::KEYSPACE_END,
                shard_prefix: String::new(),
                route_hash: resolved.shard_route,
                created_ms: descriptor.created_ms,
                predecessors: vec![],
                successors: vec![],
                sealed_ms: None,
                sealed_next_offset: None,
            });
        }
        if let Some(key) = selector {
            let point = StreamDesc::key_point(key);
            spans.retain(|s| s.contains(point));
        }
        Self {
            descriptor: descriptor.clone(),
            spans,
        }
    }

    pub(crate) fn page_progress(
        &self,
        segment: u32,
        start: u64,
        page: &ReadPage,
    ) -> Option<(ReadPosition, ReadPosition, bool)> {
        let index = self.spans.iter().position(|s| s.seg_id == segment)?;
        let span = &self.spans[index];
        let cap = span.sealed_next_offset.unwrap_or(page.end);
        let consumed = page.scanned_through(start).min(cap.max(start));
        let drained = page.completed && consumed >= cap;
        let mut next = ReadPosition {
            segment,
            after: consumed,
        };
        let mut durable = ReadPosition {
            segment,
            after: page.durable_resume(start).min(cap.max(start)),
        };
        if drained
            && span.sealed_next_offset.is_some()
            && let Some(successor) = self.spans.get(index + 1)
        {
            next = ReadPosition {
                segment: successor.seg_id,
                after: 0,
            };
            durable = next;
        }
        Some((next, durable, drained))
    }
}

#[cfg(test)]
mod read_contract_tests {
    use super::*;
    #[test]
    fn r06_empty_filtered_page_has_consumed_progress() {
        let page = ReadPage {
            watermarks: Watermarks {
                durable: 40,
                applied: 40,
            },
            recs: PlainBatch::default(),
            last: Some(39),
            end: 40,
            completed: true,
        };
        assert_eq!(page.scanned_through(12), 40);
        assert_eq!(page.durable_resume(12), 40);
    }
    #[test]
    fn r06_applied_resume_is_clamped_after_rollback() {
        let page = ReadPage {
            watermarks: Watermarks {
                durable: 5,
                applied: 9,
            },
            recs: PlainBatch::default(),
            last: Some(8),
            end: 9,
            completed: true,
        };
        assert_eq!(page.scanned_through(0), 9);
        assert_eq!(page.durable_resume(0), 5);
        let retried = ReadPage {
            watermarks: Watermarks {
                durable: 5,
                applied: 5,
            },
            recs: PlainBatch::default(),
            last: None,
            end: 5,
            completed: true,
        };
        assert_eq!(retried.durable_resume(page.durable_resume(0)), 5);
    }
}

#[path = "read_request.rs"]
mod request;
pub(crate) use request::{
    ReadCommand, ReadFailure, ReadMode, ReadOutcome, ReadResultKind, ReadStart, ScanStart,
};

#[cfg(test)]
mod o2_tests {
    use super::*;
    use crate::crypto_page::{CheckedPage, PageCipher, PageLane};
    use crate::shard::record::{PageSlice, PageSlices};

    /// 64 records of 1 KiB, record `n` holding the byte `n`, sealed in four
    /// pages of 16 under the unkeyed lane and sliced as a scan of [0, 64)
    /// returns them.
    fn sixty_four_records(key: &StreamKey, epoch: &[u8; 16], segment: &[u8; 16]) -> PageSlices {
        let subkey = crate::crypto::derive_subkey(key, epoch, "", 0);
        let cipher = PageCipher::new(&subkey, segment);
        let lane = PageLane {
            key_version: 0,
            routing_key: "",
        };
        let records: Vec<Vec<u8>> = (0..64u8).map(|n| vec![n; 1024]).collect();
        let mut slices = PageSlices::default();
        for (chunk, first) in records.chunks(16).zip((0..).step_by(16)) {
            let sealed = cipher
                .seal(&lane, first, &crate::crypto_page::stamped(123, chunk))
                .unwrap();
            let page = CheckedPage::admit(Bytes::from(sealed.bytes), sealed.last).unwrap();
            slices.push(PageSlice::clip(page, 0, 64).unwrap());
        }
        slices
    }

    #[test]
    fn o2_batch_publishes_one_owner_only_after_authentication_and_admission() {
        let key = crate::crypto::StreamKey([7; 32]);
        let epoch = [8; 16];
        let hash = [9; 16];
        let frames = sixty_four_records(&key, &epoch, &hash);
        let page = || ReadPage {
            watermarks: Watermarks {
                durable: 64,
                applied: 64,
            },
            recs: PlainBatch::default(),
            last: None,
            end: 64,
            completed: true,
        };
        let mut out = page();
        assert!(
            decode_frames_into(
                &frames,
                &mut ReadKeys::new(&key, &epoch, hash),
                &mut out,
                &mut PageBudget::new(65536)
            )
            .unwrap()
        );
        let owner = out.recs.contiguous().unwrap();
        assert_eq!(owner.len(), 65536);
        assert_eq!(out.last, Some(63));
        assert_eq!(out.recs.len(), 64);
        for ((i, record), n) in out.recs.iter().enumerate().zip(0u8..) {
            assert_eq!(
                record.payload.as_ptr(),
                owner.as_ptr().wrapping_add(i * 1024)
            );
            assert!(record.payload.iter().all(|b| *b == n));
        }
        let mut partial = page();
        assert!(
            !decode_frames_into(
                &frames,
                &mut ReadKeys::new(&key, &epoch, hash),
                &mut partial,
                &mut PageBudget::new(1536)
            )
            .unwrap()
        );
        assert_eq!(partial.recs.len(), 1);
        assert_eq!(partial.last, Some(0));
        assert_eq!(partial.recs.contiguous().unwrap().len(), 1024);
        // A slow reader retaining the old immutable owner sees unchanged bytes
        // while another page is built and dropped.
        assert_eq!(owner.len(), 65536);
        assert!(out.recs[63].payload.iter().all(|b| *b == 63));
    }
}
