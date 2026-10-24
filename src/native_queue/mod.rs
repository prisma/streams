//! Native queues (docs/QUEUES-DESIGN.md): the shared vocabulary of the
//! committer's queue operations. A queue is a stream of kind `queue` whose
//! one-message pages and queue rows live in the shard log under
//! `hash ‖ 'Q' ‖ family` (§5); the committer applies every operation in the
//! group's single WriteBatch, durable on the group's watermark (G1).
//!
//! This module owns the operations, their replies, the configuration and
//! the row codecs; `shard::transaction::native_queue` applies them.
pub(crate) mod keys;
pub(crate) mod rows;
#[cfg(test)]
mod tests;
pub(crate) mod wakeup;

use serde::{Deserialize, Serialize};

/// The persisted queue kind (§4, D17): a queue's stream descriptor carries
/// this content type, the format of the envelopes its records hold. A
/// stream of this kind is reached only through `/v1/queues`.
pub(crate) const QUEUE_CONTENT_TYPE: &str = "application/vnd.prisma.queue+json";

/// Whether a descriptor's content type names the queue kind.
pub(crate) fn is_queue_kind(content_type: &str) -> bool {
    crate::registry::media_type(content_type) == QUEUE_CONTENT_TYPE
}

/// Hours, days and the clamps of the first release (§11).
pub(crate) const HOUR_MS: i64 = 3_600_000;
pub(crate) const DAY_MS: i64 = 24 * HOUR_MS;
pub(crate) const MAX_RETENTION_MS: i64 = 14 * DAY_MS;
pub(crate) const MAX_FAILURE_RETENTION_MS: i64 = 30 * DAY_MS;
pub(crate) const MAX_DEDUP_WINDOW_MS: i64 = 7 * DAY_MS;
pub(crate) const MIN_LEASE_MS: i64 = 1_000;
pub(crate) const MAX_LEASE_MS: i64 = 12 * HOUR_MS;
pub(crate) const MAX_BATCH: usize = 1_000;
pub(crate) const MAX_IN_FLIGHT: u32 = 10_000;
pub(crate) const MAX_ATTEMPTS: u32 = 100;
/// A release of a never-started message gives the attempt back this many
/// times in a row; the next counts as a retry (§6.2).
pub(crate) const FREE_RELEASES: u8 = 2;
/// Entries the sweep moves in one step (§6.8).
pub(crate) const SWEEP_STEP: usize = 256;
/// A sealed page at most this long also rides in the message's
/// availability entry, so a claim returns it without a page read (§6.1).
pub(crate) const INLINE_PAGE_MAX: usize = 2_048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Backoff {
    pub initial_ms: i64,
    pub max_ms: i64,
    pub multiplier: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial_ms: 1_000,
            max_ms: 12 * HOUR_MS,
            multiplier: 2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub(crate) struct GroupConfig {
    pub max_attempts: u32,
    pub backoff: Backoff,
    pub lease_ms: i64,
    pub max_in_flight: u32,
    pub max_batch: u32,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff: Backoff::default(),
            lease_ms: 30_000,
            max_in_flight: 1_000,
            max_batch: 100,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub(crate) struct QueueConfig {
    pub retention_ms: i64,
    pub failure_retention_ms: i64,
    pub max_queued_bytes: u64,
    pub max_queued_messages: u64,
    pub dedup_window_ms: i64,
    pub max_dedup_entries: u64,
    pub group: GroupConfig,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            retention_ms: 4 * DAY_MS,
            failure_retention_ms: 14 * DAY_MS,
            max_queued_bytes: 256 << 20,
            max_queued_messages: 1_000_000,
            dedup_window_ms: DAY_MS,
            max_dedup_entries: 1_000_000,
            group: GroupConfig::default(),
        }
    }
}

impl QueueConfig {
    /// The configuration's clamps (§11), or what is out of range.
    pub(crate) fn validate(&self) -> Result<(), String> {
        let g = &self.group;
        let checks: [(bool, &str); 10] = [
            (
                (HOUR_MS..=MAX_RETENTION_MS).contains(&self.retention_ms),
                "retentionMs must be 1 h to 14 d",
            ),
            (
                (HOUR_MS..=MAX_FAILURE_RETENTION_MS).contains(&self.failure_retention_ms),
                "failureRetentionMs must be 1 h to 30 d",
            ),
            (
                self.dedup_window_ms >= 0
                    && self.dedup_window_ms <= MAX_DEDUP_WINDOW_MS
                    && self.dedup_window_ms <= self.retention_ms,
                "dedupWindowMs must be 0 to 7 d and at most retentionMs",
            ),
            (self.max_queued_bytes > 0, "maxQueuedBytes must be positive"),
            (
                self.max_queued_messages > 0,
                "maxQueuedMessages must be positive",
            ),
            (
                (1..=MAX_ATTEMPTS).contains(&g.max_attempts),
                "group.maxAttempts must be 1 to 100",
            ),
            (
                (MIN_LEASE_MS..=MAX_LEASE_MS).contains(&g.lease_ms),
                "group.leaseMs must be 1 s to 12 h",
            ),
            (
                (1..=MAX_IN_FLIGHT).contains(&g.max_in_flight),
                "group.maxInFlight must be 1 to 10000",
            ),
            (
                g.max_batch >= 1 && g.max_batch as usize <= MAX_BATCH,
                "group.maxBatch must be 1 to 1000",
            ),
            (
                g.backoff.initial_ms >= 0
                    && g.backoff.max_ms >= g.backoff.initial_ms
                    && g.backoff.max_ms <= MAX_LEASE_MS
                    && (1..=16).contains(&g.backoff.multiplier),
                "group.backoff must have 0 <= initialMs <= maxMs <= 12 h and multiplier 1 to 16",
            ),
        ];
        match checks.iter().find(|(ok, _)| !ok) {
            Some((_, message)) => Err((*message).to_owned()),
            None => Ok(()),
        }
    }
    /// The fields fixed at creation (§8.1): a second create must match them.
    pub(crate) fn fixed_fields_match(&self, other: &Self) -> bool {
        self.retention_ms == other.retention_ms
            && self.failure_retention_ms == other.failure_retention_ms
    }
}

/// One message of a send: the sealed envelope plaintext and its plaintext
/// metadata (§5.1). The HTTP layer builds the envelope and the MAC'd tags.
#[derive(Debug, Clone)]
pub(crate) struct SendMessage {
    pub envelope: bytes::Bytes,
    pub deliver_at: Option<i64>,
    pub dedup_tag: Option<[u8; 16]>,
}

/// Lease tokens name a delivery by offset and lease generation (§6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Token {
    pub offset: u64,
    pub lease_gen: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettleKind {
    Ack,
    Retry { delay_ms: Option<i64> },
    Extend { lease_ms: i64 },
    Fail,
    Discard,
    Release,
}

#[derive(Debug, Clone)]
pub(crate) struct SettleItem {
    pub token: Token,
    pub kind: SettleKind,
    pub code: Option<String>,
}

/// Operations the committer applies to one queue.
#[derive(Debug)]
pub(crate) enum NqOp {
    Create {
        cfg: QueueConfig,
    },
    Get,
    Update {
        group: GroupConfig,
        max_queued_bytes: u64,
        max_queued_messages: u64,
    },
    BeginDelete,
    DeleteStep {
        max_rows: usize,
    },
    Send {
        messages: Vec<SendMessage>,
        subkey: [u8; 32],
        key_version: u32,
    },
    Claim {
        max: usize,
        max_bytes: u64,
        lease_ms: i64,
    },
    Settle {
        items: Vec<SettleItem>,
    },
    Pause {
        paused: bool,
    },
    Cancel {
        offsets: Vec<u64>,
    },
    Redrive {
        offsets: Vec<u64>,
        all: bool,
        limit: usize,
    },
    Discard {
        offsets: Vec<u64>,
    },
    ListFailed {
        after: Option<u64>,
        limit: usize,
    },
    Peek {
        after: Option<u64>,
        limit: usize,
    },
    Message {
        offset: u64,
    },
    Sweep,
    /// A collection pass covered the debt up to `generation` (§8.2): clear
    /// it, keeping any debt recorded after the pass captured it.
    ClearDebt {
        generation: u64,
        bytes: u64,
        captured_at: i64,
    },
    SetWakeup {
        url: Option<String>,
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum DeliveryState {
    Pending,
    Leased,
    Retrying,
    Failed,
}

/// A delivery as replies describe it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeliveryView {
    pub offset: u64,
    pub state: DeliveryState,
    pub lease_gen: u32,
    pub attempt: u32,
    pub cycle: u32,
    pub enqueued_at: i64,
    pub available_at: i64,
    pub lease_deadline: i64,
    pub failure: Option<FailureView>,
    /// The sealed page, when the claim read it from the availability
    /// entry; `None`: read it by its page key.
    #[serde(skip)]
    pub page: Option<bytes::Bytes>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailureView {
    pub cause: &'static str,
    pub code: Option<String>,
    pub attempts: u32,
    pub failed_at: i64,
    pub failure_expires_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SettleOutcome {
    Ok,
    Stale,
    Expired,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SettleResult {
    pub outcome: SettleOutcome,
    pub accepted_late: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendResult {
    pub offset: u64,
    pub enqueued_at: i64,
    pub available_at: i64,
    pub duplicate: bool,
}

/// Refusals a client sees as a specific code (§11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NqRefusal {
    NotFound,
    Deleting,
    ConfigConflict,
    InvalidConfig(String),
    QueueFull,
    DedupFull,
    DeliverAtBeyondRetention,
    Reclaimed,
    Internal(String),
}

#[derive(Debug, Clone)]
pub(crate) enum NqOut {
    Config {
        cfg: QueueConfig,
        created: bool,
    },
    Stats(Box<rows::StateRow>),
    Deleting,
    DeleteStepped {
        remaining: bool,
    },
    Sent(Vec<SendResult>),
    Claimed {
        deliveries: Vec<DeliveryView>,
        paused: bool,
    },
    Settled(Vec<SettleResult>),
    Paused(bool),
    Count(u64),
    Redriven {
        redriven: u64,
        remaining: u64,
        capacity: bool,
    },
    Deliveries {
        items: Vec<DeliveryView>,
        next: Option<u64>,
    },
    Message(DeliveryView),
    Swept {
        next_deadline: Option<i64>,
        wake: Option<WakeRequest>,
    },
    Wakeup {
        url: Option<String>,
        version: u32,
    },
}

pub(crate) type NqResult = Result<NqOut, NqRefusal>;

#[cfg(test)]
impl NqOut {
    /// The count a counting reply carries.
    pub(crate) fn count(&self) -> u64 {
        match self {
            NqOut::Count(n) => *n,
            other => panic!("not a count: {other:?}"),
        }
    }
}

/// A notification the sweep decided to send (§9.2); the sweeper posts it.
#[derive(Debug, Clone)]
pub(crate) struct WakeRequest {
    pub url: String,
    pub name: String,
    pub version: u32,
    pub due: u64,
    pub oldest_due_age_ms: i64,
}

/// Queue scans cache the blocks they read (pricing benchmark, 2026-10-24):
/// a claim and a sweep scan the same small index families again and again,
/// and SlateDB's default scan reads every sorted run's block from the
/// bucket each time (1M extra GETs in a 90-minute 1x run, $180 a month).
pub(crate) fn scan_options() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        cache_blocks: true,
        ..slatedb::config::ScanOptions::default()
    }
}

/// Jitter denominator: `rand` is a fraction in millionths.
pub(crate) const JITTER_SCALE: u64 = 1_000_000;

/// Backoff for the attempt that just failed (`attempts` ≥ 1), with full
/// jitter: `rand` is a fraction of the capped delay in millionths (§6.3).
pub(crate) fn backoff_ms(b: &Backoff, attempts: u32, rand: u64) -> i64 {
    let mut base = b.initial_ms.max(0);
    for _ in 1..attempts.min(64) {
        base = base.saturating_mul(i64::from(b.multiplier));
        if base >= b.max_ms {
            break;
        }
    }
    let capped = u128::try_from(base.min(b.max_ms).max(0)).unwrap_or(0);
    let scaled = capped * u128::from(rand.min(JITTER_SCALE)) / u128::from(JITTER_SCALE);
    i64::try_from(scaled).unwrap_or(i64::MAX)
}

/// Parked claims of a queue in this process (§6.1): a send, redrive or
/// settle that makes work claimable wakes them, so a long-poll claim waits
/// on its queue instead of polling the committer.
static WAITERS: std::sync::Mutex<
    std::collections::BTreeMap<[u8; 16], std::sync::Weak<tokio::sync::Notify>>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// The queue's waiter, shared by every claim parked on it.
pub(crate) fn waiter(hash: &[u8; 16]) -> std::sync::Arc<tokio::sync::Notify> {
    let mut map = WAITERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(live) = map.get(hash).and_then(std::sync::Weak::upgrade) {
        return live;
    }
    map.retain(|_, w| w.strong_count() > 0);
    let fresh = std::sync::Arc::new(tokio::sync::Notify::new());
    map.insert(*hash, std::sync::Arc::downgrade(&fresh));
    fresh
}

/// Wake every claim parked on the queue.
pub(crate) fn wake(hash: &[u8; 16]) {
    let map = WAITERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(live) = map.get(hash).and_then(std::sync::Weak::upgrade) {
        live.notify_waiters();
    }
}
