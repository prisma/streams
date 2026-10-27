//! A resident stream handle and the rows it keeps hot.
//!
//! The committer is the only writer of a stream's billing row, so once a
//! commit group read or wrote it through a resident handle the handle can
//! hold it: later groups take it from there instead of a store read on the
//! shard's one serial commit path. A group leaves its row on the handle
//! just before its write (`keep_hot_rows`);
//! a group refused before the write leaves nothing, and a failed write
//! retires the engine and every handle it holds (TLA-005-F5). An evicted
//! handle takes its rows with it, and the next one reloads them.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{ShardEngine, StreamHandle, StreamState, TailFields, TailRing, now_ms};
use crate::billing::SegmentBillingMetaV1;

/// The rows a resident handle holds for the committer.
#[derive(Default)]
pub(crate) struct HotRows {
    /// The billing row as the last staged group left it; `None`: not
    /// resident (a stream with no row is never held, and reads it again).
    billing: Option<SegmentBillingMetaV1>,
}

impl HotRows {
    /// The resident billing row, if any.
    pub(super) fn billing(&self) -> Option<&SegmentBillingMetaV1> {
        self.billing.as_ref()
    }

    /// Hold the billing row a group read or wrote, if it has one.
    pub(super) fn keep(&mut self, billing: Option<&SegmentBillingMetaV1>) {
        if let Some(row) = billing {
            self.billing = Some(row.clone());
        }
    }
}

impl ShardEngine {
    /// A fresh handle for `hash` at the stored `tail` (a native queue's when
    /// `native_queue`), nothing resident.
    pub(super) fn fresh_handle(
        &self,
        hash: [u8; 16],
        tail: TailFields,
        native_queue: bool,
    ) -> Arc<StreamHandle> {
        Arc::new(StreamHandle {
            owner: Arc::downgrade(&self.db),
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                producers: HashMap::new(),
                seqs: HashMap::new(),
                queue: crate::queue::QueueState::default(),
                hot: HotRows::default(),
            }),
            notify: Notify::new(),
            applied_notify: Notify::new(),
            ring: Mutex::new(TailRing::default()),
            last_touch_ms: now_ms().unsigned_abs().into(),
            pressure: std::sync::OnceLock::new(),
            native_queue: native_queue.into(),
        })
    }

    /// How many stream handles are resident. A count, so a poisoned
    /// handle map is still counted.
    pub(crate) fn resident_streams(&self) -> usize {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// The handle of `hash` if it is resident; never loads one. A poisoned
    /// handle map holds nothing resident.
    fn resident_handle(&self, hash: &[u8; 16]) -> Option<Arc<StreamHandle>> {
        self.streams.lock().ok()?.get(hash).cloned()
    }

    /// The stream's billing row for a commit group: the resident handle's
    /// when it holds one, else the store's (`load_billing_meta`).
    pub(super) async fn billing_row(
        &self,
        hash: [u8; 16],
    ) -> anyhow::Result<Option<SegmentBillingMetaV1>> {
        if let Some(handle) = self.resident_handle(&hash)
            && let Some(row) = handle.hot(|hot| hot.billing().cloned()).flatten()
        {
            return Ok(Some(row));
        }
        self.load_billing_meta(hash).await
    }
}

impl StreamHandle {
    /// Run `f` on the handle's hot rows under its state lock. A poisoned
    /// state holds nothing resident: `None`, and the caller reads the store.
    pub(super) fn hot<T>(&self, f: impl FnOnce(&mut HotRows) -> T) -> Option<T> {
        let mut state = self.state.lock().ok()?;
        Some(f(&mut state.hot))
    }
}
