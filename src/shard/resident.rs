//! A resident stream handle and the rows it keeps hot.
//!
//! The committer is the only writer of a stream's consumer records, so once
//! one is read or written through a resident handle the handle can hold it:
//! later commit groups take it from there instead of a store read on the
//! shard's one serial commit path. A group leaves its records on the handle
//! just before its write (`keep_hot_rows`); a group refused before the write
//! leaves nothing, and a failed write retires the engine and every handle it
//! holds (TLA-005-F5). An evicted handle takes its records with it, and the
//! next one reloads them. A billed stream's billing meta is resident the same
//! way, on the handle's applied tail (layout 6, f3: `billing_for_commit`).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::{ShardEngine, StreamHandle, StreamState, TailFields, TailRing, now_ms};
use crate::queue::{ConsumerLifecycle, ConsumerRecord};

/// The rows a resident handle holds for the committer.
#[derive(Default)]
pub(crate) struct HotRows {
    /// Consumer records by name as the last staged group left them. A
    /// consumer with no record, or a deleted one, is never held and reads
    /// the store again, so names a stream churns through do not pile up.
    // mt-lint: allow(name-keyed-map): consumer names are scoped by the handle's tenant-qualified stream hash
    configs: HashMap<String, ConsumerRecord>,
}

impl HotRows {
    /// Hold every consumer record a group read or wrote.
    pub(super) fn keep(
        &mut self,
        // mt-lint: allow(name-keyed-map): a group's consumer records of the one stream this handle is
        configs: &HashMap<String, ConsumerRecord>,
    ) {
        for (consumer, record) in configs {
            self.keep_config(consumer, record);
        }
    }

    /// The resident record of `consumer`, if any.
    pub(super) fn config(&self, consumer: &str) -> Option<&ConsumerRecord> {
        self.configs.get(consumer)
    }

    /// Hold `record` as `consumer`'s record; a deleted one lets go of it.
    pub(super) fn keep_config(&mut self, consumer: &str, record: &ConsumerRecord) {
        if record.state == ConsumerLifecycle::Deleted {
            self.configs.remove(consumer);
        } else {
            self.configs.insert(consumer.to_owned(), record.clone());
        }
    }
}

impl ShardEngine {
    /// A fresh handle for `hash` at the stored `tail` (a native queue's when
    /// `native_queue`) holding its unsealed `pending` pages, no row resident.
    pub(super) fn fresh_handle(
        &self,
        hash: [u8; 16],
        tail: TailFields,
        native_queue: bool,
        pending: crate::spill::pending::PendingPages,
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
                pending,
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
}

impl StreamHandle {
    /// Run `f` on the handle's hot rows under its state lock. A poisoned
    /// state holds nothing resident: `None`, and the caller reads the store.
    pub(super) fn hot<T>(&self, f: impl FnOnce(&mut HotRows) -> T) -> Option<T> {
        let mut state = self.state.lock().ok()?;
        Some(f(&mut state.hot))
    }
}
