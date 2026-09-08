//! Durable collection scan snapshots and bounded page progress. The caller
//! verifies/signs the cursor; this owner freezes and executes its segment plan.
use super::read::{PlainBatch, ReadFailure, ReadPlan, ReadRange, ReadService};
use crate::crypto::StreamKey;
use crate::product_cursor::ScanCursor;
use crate::registry::StreamDesc;
use crate::shard_directory::{Adoption, ResolveError};

pub(crate) struct ScanCommand {
    pub descriptor: StreamDesc,
    pub key: StreamKey,
    pub cursor: Option<ScanCursor>,
    pub max_bytes: usize,
    pub now_ms: i64,
    pub lifetime_ms: i64,
}
pub(crate) struct ScanOutcome {
    pub records: PlainBatch,
    pub continuation: Option<ScanCursor>,
}
impl ReadService {
    pub(crate) async fn execute_scan(
        &self,
        command: ScanCommand,
    ) -> Result<ScanOutcome, ReadFailure> {
        let desc = &command.descriptor;
        if !super::creation::desc_alive(desc) {
            return Err(ReadFailure::Missing);
        }
        if desc.init.is_some() {
            return Err(ReadFailure::Creating);
        }
        if command.key.fingerprint(&desc.epoch()) != desc.key_fingerprint {
            return Err(ReadFailure::WrongKey);
        }
        let mut cursor = match command.cursor {
            Some(cursor) if cursor.epoch == desc.epoch() => cursor,
            Some(_) => return Err(ReadFailure::ChangedIncarnation),
            None => {
                self.snapshot_scan(desc, &command.key, command.now_ms, command.lifetime_ms)
                    .await?
            }
        };
        let mut budget = super::read_budget::PageBudget::new(command.max_bytes);
        let mut records = PlainBatch::default();
        'segments: while (cursor.current_index as usize) < cursor.segments.len() && !budget.full() {
            let (segment, end) = cursor.segments[cursor.current_index as usize];
            if cursor.current_offset >= end {
                cursor.current_index += 1;
                cursor.current_offset = 0;
                continue;
            }
            let page = self
                .read_scan_span(
                    desc,
                    &command.key,
                    segment,
                    ReadRange::bounded(cursor.current_offset, end),
                    budget.remaining(),
                )
                .await?;
            let consumed = page.scanned_through(cursor.current_offset).min(end);
            if page.completed && page.end < end {
                return Err(ReadFailure::Storage(
                    "durable scan snapshot frontier is no longer readable".into(),
                ));
            }
            let before = cursor.current_offset;
            let admission = records.append_selected(page.recs, before..end, None, &mut budget, 0);
            if let Some(withheld) = admission.withheld {
                cursor.current_offset = withheld;
                break 'segments;
            }
            cursor.current_offset = if page.completed { end } else { consumed };
            if cursor.current_offset == before && !page.completed {
                break;
            }
        }
        while let Some((_, end)) = cursor.segments.get(cursor.current_index as usize) {
            if cursor.current_offset < *end {
                break;
            }
            cursor.current_index += 1;
            cursor.current_offset = 0;
        }
        let complete = cursor.current_index as usize >= cursor.segments.len();
        Ok(ScanOutcome {
            records,
            continuation: (!complete).then_some(cursor),
        })
    }
    async fn snapshot_scan(
        &self,
        desc: &StreamDesc,
        key: &StreamKey,
        now_ms: i64,
        lifetime_ms: i64,
    ) -> Result<ScanCursor, ReadFailure> {
        let mut spans = super::read::ReadTopology::new(desc, None).spans;
        spans.sort_by_key(|span| (span.created_ms, span.seg_id));
        let mut segments = Vec::with_capacity(spans.len());
        for span in spans {
            let end = match span.sealed_next_offset {
                Some(end) => end,
                None => match self
                    .shards
                    .resolve(&desc.segment_route(&span), Adoption::External)
                    .await
                {
                    Ok(engine) => {
                        let identity = desc.dynamic_segment_identity(span.seg_id);
                        let handle = engine
                            .stream_handle(identity)
                            .await
                            .map_err(|e| ReadFailure::Storage(e.to_string()))?;
                        let local = handle.state.lock().unwrap().durable.next;
                        let (shared, _) = engine
                            .durable_absorbed(&identity)
                            .await
                            .map_err(|e| ReadFailure::Storage(e.to_string()))?;
                        local.max(shared)
                    }
                    Err(ResolveError::NotOwner { owner, .. }) => {
                        self.remote_scan_span(
                            desc,
                            key,
                            span.seg_id,
                            ReadRange::open(u64::MAX),
                            4096,
                            &owner,
                        )
                        .await?
                        .end
                    }
                    Err(error) => return Err(ReadFailure::Resolve(error)),
                },
            };
            segments.push((span.seg_id, end));
        }
        Ok(ScanCursor {
            epoch: desc.epoch(),
            map_version: desc.segments.as_ref().map_or(0, |map| map.version),
            segments,
            current_index: 0,
            current_offset: 0,
            expires_at_ms: now_ms.saturating_add(lifetime_ms),
        })
    }
    /// A scan names one physical segment. It does not inherit replay's hop or
    /// live-wait semantics; its frozen cursor owns the next segment decision.
    async fn read_scan_span(
        &self,
        desc: &StreamDesc,
        key: &StreamKey,
        segment: u32,
        range: ReadRange,
        budget: usize,
    ) -> Result<super::read::ReadPage, ReadFailure> {
        let route = desc
            .segment_route_by_id(segment)
            .ok_or(ReadFailure::InvalidCursor)?;
        let engine = match self.shards.resolve(&route, Adoption::External).await {
            Ok(engine) => engine,
            Err(ResolveError::NotOwner { owner, .. }) => {
                return self
                    .remote_scan_span(desc, key, segment, range, budget, &owner)
                    .await;
            }
            Err(error) => return Err(ReadFailure::Resolve(error)),
        };
        let identity = desc.dynamic_segment_identity(segment);
        let handle = engine
            .stream_handle(identity)
            .await
            .map_err(|e| ReadFailure::Storage(e.to_string()))?;
        self.keys.put(identity, key.clone(), desc.epoch());
        ReadPlan::segment(
            key,
            &desc.epoch(),
            &handle,
            &engine,
            range,
            None,
            budget,
            crate::shard::Deliver::Durable,
        )
        .execute()
        .await
        .map_err(ReadFailure::Storage)
    }
    async fn remote_scan_span(
        &self,
        desc: &StreamDesc,
        key: &StreamKey,
        segment: u32,
        range: ReadRange,
        budget: usize,
        owner: &str,
    ) -> Result<super::read::ReadPage, ReadFailure> {
        use base64::Engine;
        let target = super::read_remote::InternalTarget::of(desc, segment)
            .ok_or(ReadFailure::InvalidCursor)?;
        super::read_remote::remote_span_page(
            &self.peer,
            owner,
            desc,
            &target,
            range,
            budget,
            &base64::engine::general_purpose::STANDARD.encode(key.0),
        )
        .await
        .map(|page| page.out)
        .map_err(ReadFailure::Remote)
    }
}
