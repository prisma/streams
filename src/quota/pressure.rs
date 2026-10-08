//! The weights of the round-13 project memory-pressure model and the
//! estimate every project's entry answers with them (version 2: reserved
//! read bytes exactly, parked waits by weight). The model's version,
//! its rationale and its manifest (`pressure_model_json`) stay beside the
//! entry it reads (`src/quota.rs`); the latch that refuses a project's
//! writes over the high-water mark is `ProjectAdmission::memory_gate`.

use std::sync::atomic::Ordering;

use super::ProjectAdmission;

pub(crate) const PRESSURE_SUB_WEIGHT_BYTES: u64 = 32 * 1024;
pub(crate) const PRESSURE_FEED_WEIGHT_BYTES: u64 = 16 * 1024;
pub(crate) const PRESSURE_DIRTY_STREAM_WEIGHT_BYTES: u64 = 64 * 1024;
/// One wait parked in the project's share (`parked`): a held connection
/// and its request future, #269's ~44 KB per parked connection rounded
/// up. A watch wait is a live subscription and weighs as one.
pub(crate) const PRESSURE_PARKED_WEIGHT_BYTES: u64 = 48 * 1024;

impl ProjectAdmission {
    /// `estimated_project_pressure_bytes` — named for what it is: a
    /// conservative model, not RSS attribution.
    pub(crate) fn estimated_pressure_bytes(&self) -> u64 {
        self.live_subs.load(Ordering::Relaxed) * PRESSURE_SUB_WEIGHT_BYTES
            + self.live_feeds.load(Ordering::Relaxed) * PRESSURE_FEED_WEIGHT_BYTES
            + self.retained_sse_bytes.load(Ordering::Relaxed)
            + self.buffered_body_bytes.load(Ordering::Relaxed)
            + self.queued_bytes.load(Ordering::Relaxed)
            + self.unabsorbed_frame_bytes.load(Ordering::Relaxed)
            + self.dirty_streams.load(Ordering::Relaxed) * PRESSURE_DIRTY_STREAM_WEIGHT_BYTES
            + self.read_held.load(Ordering::Relaxed)
            + self.parked.load(Ordering::Relaxed) * PRESSURE_PARKED_WEIGHT_BYTES
    }
}
