//! Queue profile (PROFILES.md §7, informed by Cloudflare Queues):
//! SQS/CF-style pull consumers as rebuildable state over the immutable
//! stream. Enqueue = append; message id = offset; consumer state (cursor +
//! leases + early-ack markers) lives in the shard log under the stream's
//! keyspace, mutated only by the committer and durable at the watermark.
//!
//! Cloudflare-informed choices: one combined ack+retry(+extend) call with
//! per-message retry delays; opaque-ish lease tokens validated permissively
//! (stale tokens are counted, never errors); `backlog` returned on every
//! pull (the consumer-autoscaling signal); `attempts` on every message.
//! Expiry is lazy: an expired lease is simply re-leasable at the next
//! receive (no sweeper task); a message exceeding maxDeliveries settles and
//! a reference record is appended under routing key `$dlq` — the DLQ is a
//! routing-key view, browsable and replayable with normal keyed reads.
//!
//! Keyspace (per stream hash, alongside t/r/q):
//!   <hash16> 'c' <consumer>              cursor (u64 LE): all below settled
//!   <hash16> 'l' <consumer> 0x00 <off BE> lease {deadline i64, count u32, gen u32}
//!   <hash16> 'x' <consumer> 0x00 <off BE> settled-above-cursor marker

use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Copy)]
pub struct Lease {
    pub deadline_ms: i64,
    pub delivery_count: u32,
    pub lease_gen: u32,
    /// Routing-key hash of the leased record — the per-key FIFO
    /// blocking identity (spec Stage 2 §2.3). Zeros on rows written by
    /// the un-keyed profile path.
    pub key_hash: [u8; 16],
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerState {
    pub cursor: u64,
    pub leases: BTreeMap<u64, Lease>,
    pub acked: BTreeSet<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueState {
    pub consumers: HashMap<String, ConsumerState>,
    pub loaded: bool,
}

pub fn cursor_key(hash: &[u8; 16], consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'c');
    k.extend_from_slice(consumer.as_bytes());
    k
}

pub fn lease_key(hash: &[u8; 16], consumer: &str, off: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(26 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'l');
    k.extend_from_slice(consumer.as_bytes());
    k.push(0);
    k.extend_from_slice(&off.to_be_bytes());
    k
}

pub fn ack_key(hash: &[u8; 16], consumer: &str, off: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(26 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'x');
    k.extend_from_slice(consumer.as_bytes());
    k.push(0);
    k.extend_from_slice(&off.to_be_bytes());
    k
}

pub fn encode_lease(l: &Lease) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.extend_from_slice(&l.deadline_ms.to_le_bytes());
    v.extend_from_slice(&l.delivery_count.to_le_bytes());
    v.extend_from_slice(&l.lease_gen.to_le_bytes());
    v.extend_from_slice(&l.key_hash);
    v
}

pub fn decode_lease(v: &[u8]) -> Option<Lease> {
    if v.len() < 16 {
        return None;
    }
    let mut key_hash = [0u8; 16];
    if v.len() >= 32 {
        key_hash.copy_from_slice(&v[16..32]);
    }
    Some(Lease {
        deadline_ms: i64::from_le_bytes(v[0..8].try_into().ok()?),
        delivery_count: u32::from_le_bytes(v[8..12].try_into().ok()?),
        lease_gen: u32::from_le_bytes(v[12..16].try_into().ok()?),
        key_hash,
    })
}

/// Consumer-group config row (spec Stage 2 §2.2), stored under the
/// PARENT identity — collection-scoped, unlike per-segment state.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConsumerConfig {
    #[serde(default = "d_vis")]
    pub visibility_timeout_ms: u32,
    #[serde(default = "d_att")]
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_stream: Option<String>,
    /// The target's incarnation at configuration time. A name alone is
    /// not an identity: delete the DLQ, recreate it with the same name
    /// and key, and poison records would silently start going to a
    /// different resource than the one that was approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_epoch: Option<String>,
    #[serde(default = "d_batch")]
    pub max_batch_records: u16,
}
fn d_vis() -> u32 {
    30_000
}
fn d_att() -> u32 {
    5
}
fn d_batch() -> u16 {
    10
}
impl Default for ConsumerConfig {
    fn default() -> Self {
        ConsumerConfig {
            visibility_timeout_ms: d_vis(),
            max_attempts: d_att(),
            dead_letter_stream: None,
            dead_letter_epoch: None,
            max_batch_records: d_batch(),
        }
    }
}

pub fn config_key(hash: &[u8; 16], consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'C');
    k.extend_from_slice(consumer.as_bytes());
    k
}

/// Parse "<off>:<gen>" lease tokens (permissive: None on malformed).
pub fn parse_token(t: &str) -> Option<(u64, u32)> {
    let (o, g) = t.split_once(':')?;
    Some((o.parse().ok()?, g.parse().ok()?))
}

pub enum QueueOp {
    Receive {
        consumer: String,
        max: usize,
        visibility_ms: u64,
        max_deliveries: u32,
        /// Per-key FIFO (spec Stage 2 §2.3): offset -> routing-key-hash
        /// map, pre-read by the HTTP layer from the merged (history +
        /// tail) reader. The scan stops at the first offset not covered
        /// — leasing a record whose key is unknown could jump a blocked
        /// key's queue.
        keys: std::collections::HashMap<u64, [u8; 16]>,
        /// Exclusive end of the pre-read coverage.
        covered_to: u64,
    },
    /// Idempotent config create/compare under the PARENT identity.
    ConfigPut {
        consumer: String,
        cfg: ConsumerConfig,
    },
    ConfigGet {
        consumer: String,
    },
    /// Delete config + every state row of this consumer under this
    /// identity.
    ConfigDelete {
        consumer: String,
    },
    Settle {
        consumer: String,
        acks: Vec<(u64, u32)>,
        retries: Vec<(u64, u32, u64)>, // (off, gen, delay_ms)
        extends: Vec<(u64, u32, u64)>, // (off, gen, visibility_ms)
        max_deliveries: u32,
    },
}

#[derive(Debug, Clone)]
pub enum QueueOut {
    Received {
        /// (offset, gen, attempts, key_hash) for each newly leased
        /// message.
        leased: Vec<(u64, u32, u32, [u8; 16])>,
        backlog: u64,
        /// Keyed mode: at-max-attempts candidates REPORTED, not
        /// settled — the HTTP layer appends to the DLQ stream durably
        /// FIRST, then acks the source with the lease token (spec
        /// §2.8; crash between the two re-runs idempotently). The
        /// expired lease keeps the key blocked until that completes.
        /// (offset, lease_gen, attempts, key_hash).
        poisoned: Vec<(u64, u32, u32, [u8; 16])>,
    },
    /// created = true -> 201; equal existing -> 200; None + conflict.
    Config {
        cfg: Option<ConsumerConfig>,
        created: bool,
        conflict: bool,
    },
    Settled {
        acked: usize,
        retried: usize,
        extended: usize,
        dlq: usize,
        backlog: u64,
        stale: usize,
        poisoned: Vec<(u64, u32, u32, [u8; 16])>,
    },
}
