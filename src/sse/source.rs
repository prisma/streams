//! The v0 feed source: one LIVE segment of a single-segment stream,
//! read through the ordinary durable pipeline (LIVE-FEED Stage 3).
//! Fork and lineage adapters join later with the same trait shape.

use super::feed::{FeedSourceRead, SourceBatch};
use crate::registry::StreamDesc;
use crate::shard::{ShardEngine, StreamHandle};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

pub(crate) struct SingleSource {
    pub(crate) state: Arc<crate::http::AppState>,
    pub(crate) rk_filter: Option<String>,
    /// True when this feed serves the RAW singular surface (scalar
    /// control vocabulary).
    pub(crate) raw_surface: bool,
    pub(crate) desc: StreamDesc,
    pub(crate) key: crate::crypto::StreamKey,
    pub(crate) epoch: [u8; 16],
    pub(crate) engine: Arc<ShardEngine>,
    pub(crate) handle: Arc<StreamHandle>,
}

#[async_trait::async_trait]
impl FeedSourceRead for SingleSource {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch> {
        // FORKS: stitched reads traverse the ancestor chain and return
        // records in the CHILD's logical offset space — the same cursor
        // space every other lane uses.
        let out = if self.desc.forked_from.is_some() {
            crate::http::read_stitched(&self.state, &self.desc, &self.key, from, max_bytes)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        } else {
            crate::http::read_records(
                &self.state,
                &self.desc,
                &self.key,
                &self.epoch,
                &self.handle,
                &self.engine,
                from,
                self.rk_filter.as_deref(),
                max_bytes,
                crate::shard::Deliver::Durable,
            )
            .await
            .map_err(|e| anyhow::anyhow!(e))?
        };
        // HONEST scanned progress (finding 2): `last` advances over
        // NON-MATCHING ranges for filtered lanes — it is the consumed
        // boundary even when zero records matched.
        let scan_to = out.last.map(|x| x + 1).unwrap_or(from);
        Ok(SourceBatch {
            scan_from: from,
            scan_to,
            records: out.recs,
        })
    }

    fn frontier(&self) -> u64 {
        self.handle.state.lock().unwrap().durable.next
    }

    fn closed(&self) -> bool {
        self.handle.state.lock().unwrap().durable.closed
    }

    fn desc(&self) -> &StreamDesc {
        &self.desc
    }

    fn frame(&self, rec: &crate::http::PlainRec, up_to_date: bool, sealed: bool) -> Bytes {
        let data = crate::sse::wire::sse_data_event(&self.desc, &rec.payload);
        let next = rec.off + 1;
        // Default-key lane on the RAW singular surface folds the raw
        // scalar vocabulary; product lanes fold signed key cursors.
        let ctl = if self.raw_surface {
            crate::sse::wire::sse_control(next, None, up_to_date, sealed)
        } else {
            let tok = crate::product_cursor::KeyCursor {
                epoch: self.epoch,
                key_hash: crate::crypto::stream_hash(self.rk_filter.as_deref().unwrap_or("")),
                seg_id: self.desc.resolve_segment("").seg_id,
                offset: next,
            }
            .encode(&self.desc.project_id, &self.key);
            crate::sse::wire::sse_control_product(&tok, up_to_date, sealed)
        };
        let mut out = BytesMut::with_capacity(data.len() + ctl.len());
        out.extend_from_slice(data.as_bytes());
        out.extend_from_slice(ctl.as_bytes());
        out.freeze()
    }

    fn advance_notify(&self) -> &tokio::sync::Notify {
        &self.handle.notify
    }
}
