//! Canonical bounded segment read plan and page. HTTP and SSE render this
//! result; scanned progress never depends on the number of matching records.
use super::read_budget::{MAX_SCAN_BATCH_BYTES, PageBudget, SCAN_WINDOW};
use crate::crypto::{StreamKey, decode_frame, decrypt_frame_limited, derive_subkey};
use crate::shard::{Deliver, ShardEngine, StreamHandle};
use bytes::Bytes;
use std::collections::HashMap;
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
    from: u64,
    selector: Option<&'a str>,
    max_bytes: usize,
    visibility: Deliver,
}

impl<'a> ReadPlan<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn segment(
        key: &'a StreamKey,
        epoch: &'a [u8; 16],
        handle: &'a Arc<StreamHandle>,
        engine: &'a Arc<ShardEngine>,
        from: u64,
        selector: Option<&'a str>,
        max_bytes: usize,
        visibility: Deliver,
    ) -> Self {
        Self {
            key,
            epoch,
            handle,
            engine,
            from,
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
#[allow(clippy::too_many_arguments)]
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
        key, epoch, handle, engine, from, selector, max_bytes, visibility,
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

/// A decrypted record ready for response assembly.
#[derive(Clone)]
pub(crate) struct PlainRec {
    pub(crate) off: u64,
    pub(crate) payload: Bytes,
    /// Exact routing-key bytes from the frame header (product scan
    /// surfaces them per record; keyed reads ignore the field).
    pub(crate) rkey: String,
}

pub(crate) struct ReadPage {
    pub(crate) watermarks: Watermarks,
    pub(crate) recs: Vec<PlainRec>,
    pub(crate) last: Option<u64>,
    pub(crate) end: u64,
    pub(crate) completed: bool,
}

/// Decode raw stream-key-encrypted frames (v2 history or shard tail —
/// byte-identical formats) into plaintext records, charging the byte
/// budget per record.
/// Returns false at the first withheld matching record. Earlier filtered misses
/// are consumed, but the low-level scan's later cursor must then be discarded.
fn decode_frames_into(
    frames: &[Bytes],
    key: &StreamKey,
    epoch: &[u8; 16],
    hash: &[u8; 16],
    subkeys: &mut HashMap<(String, u32), [u8; 32]>,
    out: &mut ReadPage,
    budget: &mut PageBudget,
) -> Result<bool, String> {
    for raw in frames {
        let frame = decode_frame(raw).ok_or("bad frame")?;
        let offset = frame.header.offset;
        if !budget.metadata_fits(&frame.header.routing_key) {
            out.last = offset.checked_sub(1);
            return Ok(false);
        }
        let sk = *subkeys
            .entry((frame.header.routing_key.clone(), frame.header.key_version))
            .or_insert_with(|| {
                derive_subkey(
                    key,
                    epoch,
                    &frame.header.routing_key,
                    frame.header.key_version,
                )
            });
        let Some(pt) = decrypt_frame_limited(&sk, hash, &frame, raw, budget.decode_limit())? else {
            if out.recs.is_empty() {
                return Err("decoded record exceeds 32 MiB".into());
            }
            out.last = offset.checked_sub(1);
            return Ok(false);
        };
        if !budget.admit(pt.len(), &frame.header.routing_key) {
            out.last = offset.checked_sub(1);
            return Ok(false);
        }
        out.recs.push(PlainRec {
            off: offset,
            payload: Bytes::from(pt),
            rkey: frame.header.routing_key,
        });
        out.last = Some(offset);
    }
    Ok(true)
}

/// The merge itself, free of `AppState` so the simulation harness can call
/// the production reader instead of reimplementing the history/tail split
/// (`src/dst.rs`). A second copy of this boundary logic would be a copy
/// that can drift, and drift here means the oracle stops testing what
/// production does.
#[allow(clippy::too_many_arguments)]
/// Round-13 CODE-RED bisect: the repro's stream carries ONLY rk=""
/// records, so keyed reads must be dense too — armed by the test.
#[cfg(test)]
pub(crate) static TEST_ASSERT_KEYED_DENSE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

async fn execute_segment(plan: ReadPlan<'_>) -> Result<ReadPage, String> {
    let ReadPlan {
        key,
        epoch,
        handle,
        engine,
        from: scan_from,
        selector: key_filter,
        max_bytes,
        visibility: deliver,
    } = plan;
    // The sub-stream identity (AAD + history-DB path): for total-order
    // streams this is the incarnation hash; for per-key streams, the
    // segment hash. Either way it's the handle's identity.
    let hash = handle.hash;
    let (absorbed, end, mut hist_v2, route, watermarks) = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            crate::shard::Deliver::Durable => st.durable.next,
            // Applied extends visibility to the applied watermark; the
            // history boundary below stays durable-sourced (absorption
            // only ever operates on durable data, so boundary <= end).
            crate::shard::Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        (
            st.durable.absorbed,
            end,
            st.durable.history_v2,
            st.durable.route,
            Watermarks {
                durable: st.durable.next,
                applied: st.applied.next.max(st.durable.next),
            },
        )
    };
    let mut out = ReadPage {
        watermarks,
        recs: Vec::new(),
        last: None,
        end,
        completed: true,
    };
    let mut budget = PageBudget::new(max_bytes);
    let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();

    // The absorbed snapshot above and the tail scan below are a TOCTOU
    // pair: the absorber can advance the boundary AND durably trim the
    // shard log between them, leaving the tail scan a hole at
    // `[cursor, new_boundary)` that this loop would otherwise emit as a
    // "complete" page — permanently skipping records for a paginating
    // client (2026-07-27 boundary-race DST failure). Everything trim can
    // remove is already readable in history (the absorber flushes history
    // before the boundary advances), so on detecting an advance we
    // re-serve the gap from history and re-scan the tail. `boundary` only
    // moves forward and is capped by `end`, so the loop terminates; the
    // bound is paranoia, and falling out of it yields an honest
    // `completed = false` partial page.
    let mut cursor = scan_from; // next offset still needed
    let mut boundary = absorbed; // history serves [_, boundary)
    for _ in 0..16 {
        let hist_upto = boundary.min(end);
        if cursor < hist_upto && !budget.full() {
            if !hist_v2 {
                // The v1 per-stream layout was deleted in the clean
                // switch: an unabsorbed-below-boundary tail without the
                // v2 flag cannot exist in a fresh namespace.
                return Err("unsupported_storage_layout: v1 history".into());
            }
            let completed = decode_history_range(
                &ReadPlan::segment(
                    key, epoch, handle, engine, scan_from, key_filter, max_bytes, deliver,
                ),
                HistoryRange {
                    route,
                    identity: hash,
                    from: cursor,
                    upto: hist_upto,
                    absorbed: boundary,
                },
                &mut subkeys,
                &mut out,
                &mut budget,
            )
            .await?;
            if !completed {
                // Byte-truncated, or (v1) the reader cannot prove coverage
                // of this boundary yet: report the honest partial; the
                // caller re-polls from `last + 1`.
                out.completed = false;
                return Ok(out);
            }
            // Fully scanned with proven coverage: everything below
            // `hist_upto` is consumed even when the range yields no
            // records for this key filter.
            if hist_upto > 0 {
                out.last = Some(out.last.map_or(hist_upto - 1, |o| o.max(hist_upto - 1)));
            }
            #[cfg(test)]
            if TEST_ASSERT_KEYED_DENSE.load(std::sync::atomic::Ordering::Relaxed) {
                let mut expect = scan_from;
                for r in &out.recs {
                    assert!(
                        r.off <= expect,
                        "HISTORY leg gap: expect {expect} got {} (scan_from {scan_from}, hist_upto {hist_upto}, boundary {boundary}, filter {key_filter:?})",
                        r.off
                    );
                    expect = r.off + 1;
                }
                assert!(
                    hist_upto <= expect,
                    "HISTORY leg over-claim: hist_upto {hist_upto} beyond served {expect} (scan_from {scan_from}, boundary {boundary}, filter {key_filter:?})"
                );
            }
            cursor = hist_upto;
        }
        if budget.full() || cursor >= end {
            break;
        }
        let part = crate::shard::record::read_frames_until(
            engine,
            handle,
            cursor,
            cursor.saturating_add(SCAN_WINDOW).min(end),
            key_filter,
            budget.remaining().min(MAX_SCAN_BATCH_BYTES),
            deliver,
        )
        .await
        .map_err(|e| e.to_string())?;
        // Revalidate the scan against concurrent absorption before
        // trusting it.
        let raced_boundary =
            absorption_race(engine, hash, &part, cursor, end, key_filter.is_none()).await?;
        if let Some((durable, remote_v2)) = raced_boundary {
            if durable > boundary {
                // Adopt the remote LAYOUT FLAG with the remote boundary:
                // in the first absorption's flush-to-dispatch window the
                // in-memory snapshot still says v1 while the row that
                // moved the boundary already says v2 — mixing the two
                // refused a perfectly readable v2 range as v1.
                boundary = durable;
                hist_v2 = hist_v2 || remote_v2;
                continue; // the gap is in history now; re-serve from there
            }
            // A hole the boundary does not explain: never emit it as
            // consumed. Drop the tail and report the honest partial.
            out.completed = false;
            return Ok(out);
        }
        let decoded_all = decode_frames_into(
            &part.frames,
            key,
            epoch,
            &hash,
            &mut subkeys,
            &mut out,
            &mut budget,
        )?;
        if decoded_all && let Some(last) = part.last_offset {
            out.last = Some(out.last.map_or(last, |o| o.max(last)));
        }
        break;
    }
    let consumed_next = out.last.map(|o| o + 1).unwrap_or(scan_from);
    out.completed = consumed_next >= end;
    // Round-13 CODE-RED bisect (test builds): an unfiltered merged read
    // must NEVER emit a gapped page — any panic here localizes the
    // durable-skip to THIS layer.
    #[cfg(test)]
    if key_filter.is_none() {
        let mut expect = scan_from;
        for r in &out.recs {
            assert!(
                r.off <= expect,
                "read_merged emitted a gap: expected <= {expect}, got {} (scan_from {scan_from}, last {:?}, boundary-race?)",
                r.off,
                out.last
            );
            expect = r.off + 1;
        }
        if let Some(l) = out.last {
            assert!(
                l < expect,
                "read_merged over-claimed: last {l} beyond served {expect} (scan_from {scan_from})"
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
        from: u64,
        max_bytes: usize,
    ) -> Result<ReadPage, String> {
        read_stitched(self, desc, key, from, max_bytes).await
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
            match parent {
                None => break,
                Some((src, src_ref, want_epoch)) => {
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
            }
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_stitched(
    state: &ReadService,
    desc: &StreamDesc,
    key: &StreamKey,
    from: u64,
    max_bytes: usize,
) -> Result<ReadPage, String> {
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
        recs: Vec::new(),
        last: None,
        end: own_end,
        completed: false,
    };
    let mut budget = PageBudget::new(max_bytes);
    let mut cursor = from;
    for _ in 0..(chain.len() * 4 + 8) {
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
            .unwrap_or(u64::MAX);
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
        let part = read_merged(
            key,
            epoch,
            &handle,
            &engine,
            cursor,
            Some(""),
            budget.remaining(),
            crate::shard::Deliver::Durable,
        )
        .await?;
        // CONSUMED progress (finding 6): read_merged's `last` advances
        // over scanned NON-MATCHING ranges inside this ancestor, so the
        // child's consumed position moves by the SCAN boundary — capped
        // at the next owner's boundary, never by emitted records alone.
        let scanned_after = part.last.map(|l| l + 1).unwrap_or(cursor);
        let consumed_here = scanned_after.min(cap);
        let before = cursor;
        let mut emitted = false;
        for r in part.recs {
            if r.off >= cap {
                break;
            }
            if !budget.admit(r.payload.len(), &r.rkey) {
                out.last = r.off.checked_sub(1);
                out.completed = false;
                return Ok(out);
            }
            cursor = r.off + 1;
            out.recs.push(r);
            emitted = true;
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
        let mut spans = descriptor
            .segments
            .as_ref()
            .map(|m| m.segments.clone())
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
            spans.sort_by_key(|s| (s.created_ms, s.seg_id));
        } else {
            spans.sort_by_key(|s| s.seg_id);
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
            recs: vec![],
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
            recs: vec![],
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
            recs: vec![],
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
    ReadCommand, ReadFailure, ReadMode, ReadOutcome, ReadResultKind, ReadStart,
};

/// One history range retains its physical identity and proven boundary while
/// the canonical postings reader plans bounded parallel envelope fetches.
struct HistoryRange {
    route: [u8; 16],
    identity: [u8; 16],
    from: u64,
    upto: u64,
    absorbed: u64,
}
async fn decode_history_range(
    plan: &ReadPlan<'_>,
    range: HistoryRange,
    subkeys: &mut HashMap<(String, u32), [u8; 32]>,
    out: &mut ReadPage,
    budget: &mut PageBudget,
) -> Result<bool, String> {
    // v2: the range lives in the shard's SHARED partition,
    // read through the owner's open Db — no reader open, no
    // checkpoint, no coverage probe (this Db's flush is what
    // advanced the boundary). Frames decode like tail frames.
    // Keyed ranges resolve their postings runs through the
    // plan.engine's decoded slice cache (spec §7).
    let upto = range.upto.min(range.from.saturating_add(SCAN_WINDOW));
    let part = plan
        .engine
        .history_partition()
        .await
        .map_err(|e| e.to_string())?;
    let (frames, scan_last, completed) = match plan.selector {
        Some(rk) => crate::history::read_history2_keyed_cached(
            &plan.engine.postings_cache,
            &part,
            crate::crypto::RouteHash(range.route),
            crate::crypto::SegmentHash(range.identity),
            rk,
            range.from,
            upto,
            range.absorbed,
            budget.remaining().min(MAX_SCAN_BATCH_BYTES),
        )
        .await
        .map_err(|e| e.to_string())?,
        None => crate::history::read_history2(
            &part,
            crate::crypto::RouteHash(range.route),
            crate::crypto::SegmentHash(range.identity),
            range.from,
            upto,
            None,
            budget.remaining().min(MAX_SCAN_BATCH_BYTES),
        )
        .await
        .map_err(|e| e.to_string())?,
    };
    let decoded_all = decode_frames_into(
        &frames,
        plan.key,
        plan.epoch,
        &range.identity,
        subkeys,
        out,
        budget,
    )?;
    // consumed_to is first-class (review blocker): a partial
    // keyed page's cursor advances over every range the read
    // PROVED — index-verified match-free stretches and
    // mid-run truncation points — never inferred from the
    // last matching frame alone. Without this, a fat run
    // that planned zero frames re-polled the same position
    // forever.
    if decoded_all && let Some(sl) = scan_last.or_else(|| completed.then(|| upto.saturating_sub(1)))
    {
        out.last = Some(out.last.map_or(sl, |o| o.max(sl)));
    }

    Ok(decoded_all && completed && upto == range.upto)
}

/// A tail page is accepted only after ruling out a concurrent durable trim.
/// Unfiltered density is checked in O(1); filtered scans query the same shared
/// durable tracker because an empty match cannot prove absence of a trim.
async fn absorption_race(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    part: &crate::shard::FrameReadResult,
    cursor: u64,
    end: u64,
    unfiltered: bool,
) -> Result<Option<(u64, bool)>, String> {
    Ok(if unfiltered {
        // Unfiltered offsets below the durable frontier are dense,
        // so ANY gap in the page IS the absorb/trim race — head OR
        // MID-PAGE (round-13 CODE-RED: the 2026-07-27 guard checked
        // only the head; a mid-scan retire produced {..78, 88..}
        // pages that were consumed as complete, permanently
        // skipping the seam for every subscriber and every resume —
        // 11 durable records lost in field leg A1v2, reproduced
        // deterministically by cut_resume_never_skips_a_durable_record).
        let gap = if part.frames.is_empty() {
            cursor < end // nothing at all in a non-empty range
        } else {
            // O(1): dense pages satisfy count == last - first + 1,
            // so one head decode + the page's own last_offset
            // detects head AND mid-page gaps without touching the
            // hot path's per-frame budget (the O(n) version cost
            // the capacity gate ~2%).
            let first = match decode_frame(&part.frames[0]) {
                Some(f) => f.header.offset,
                None => return Err("bad frame".into()),
            };
            first > cursor
                || part
                    .last_offset
                    .is_some_and(|l| l + 1 - first != part.frames.len() as u64)
        };
        if gap {
            Some(
                engine
                    .durable_absorbed(&hash)
                    .await
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        }
    } else {
        // A filtered scan cannot distinguish "trimmed" from "did not
        // match", so always ask the remotely-durable tracker.
        let (durable, remote_v2) = engine
            .durable_absorbed(&hash)
            .await
            .map_err(|e| e.to_string())?;
        (durable > cursor).then_some((durable, remote_v2))
    })
}
