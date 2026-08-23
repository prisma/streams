//! The v0 feed source: one LIVE segment of a single-segment stream,
//! read through the ordinary durable pipeline (LIVE-FEED Stage 3).
//! Fork and lineage adapters join later with the same trait shape.

use super::feed::FeedSourceRead;
use crate::registry::StreamDesc;
use crate::shard::{ShardEngine, StreamHandle};
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct SingleSource {
    pub(crate) state: Arc<crate::http::AppState>,
    pub(crate) rk_filter: Option<String>,
    pub(crate) desc: StreamDesc,
    pub(crate) key: crate::crypto::StreamKey,
    pub(crate) epoch: [u8; 16],
    pub(crate) engine: Arc<ShardEngine>,
    pub(crate) handle: Arc<StreamHandle>,
}

#[async_trait::async_trait]
impl FeedSourceRead for SingleSource {
    async fn read(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> anyhow::Result<(Vec<crate::http::PlainRec>, bool)> {
        // FORKS: stitched reads traverse the ancestor chain and return
        // records in the CHILD's logical offset space — the same
        // FeedCursor space every other lane uses, so the session loop
        // is unchanged.
        if self.desc.forked_from.is_some() {
            let out =
                crate::http::read_stitched(&self.state, &self.desc, &self.key, from, max_bytes)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            return Ok((out.recs, out.completed));
        }
        let out = crate::http::read_records(
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
        .map_err(|e| anyhow::anyhow!(e))?;
        Ok((out.recs, out.completed))
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

    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes {
        Bytes::from(crate::sse::wire::sse_data_event(&self.desc, &rec.payload))
    }

    fn advance_notify(&self) -> &tokio::sync::Notify {
        &self.handle.notify
    }
}
