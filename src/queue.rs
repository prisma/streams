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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

pub const RESIDENT_CONSUMER_CAPACITY: usize = 1_024;
pub const MAX_CONSUMER_OUTSTANDING: usize = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct Lease {
    pub deadline_ms: i64,
    pub delivery_count: u32,
    pub lease_gen: u32,
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
    pub loaded: HashSet<String>,
    pub order: VecDeque<String>,
}

impl QueueState {
    pub fn insert_loaded(&mut self, consumer: String, state: ConsumerState) {
        if self.loaded.insert(consumer.clone()) {
            self.order.push_back(consumer.clone());
        }
        self.consumers.insert(consumer, state);
    }

    pub fn trim_resident(&mut self) {
        while self.consumers.len() > RESIDENT_CONSUMER_CAPACITY {
            let Some(consumer) = self.order.pop_front() else {
                break;
            };
            self.loaded.remove(&consumer);
            self.consumers.remove(&consumer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_consumer_state_is_bounded() {
        let mut state = QueueState::default();
        for id in 0..=RESIDENT_CONSUMER_CAPACITY {
            state.insert_loaded(id.to_string(), ConsumerState::default());
        }
        state.trim_resident();

        assert_eq!(state.consumers.len(), RESIDENT_CONSUMER_CAPACITY);
        assert!(!state.loaded.contains("0"));
        assert!(
            state
                .loaded
                .contains(&RESIDENT_CONSUMER_CAPACITY.to_string())
        );
    }
}

pub fn cursor_key(hash: &[u8], consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(hash.len() + 1 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'c');
    k.extend_from_slice(consumer.as_bytes());
    k
}

pub fn lease_key(hash: &[u8], consumer: &str, off: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(hash.len() + 10 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'l');
    k.extend_from_slice(consumer.as_bytes());
    k.push(0);
    k.extend_from_slice(&off.to_be_bytes());
    k
}

pub fn ack_key(hash: &[u8], consumer: &str, off: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(hash.len() + 10 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'x');
    k.extend_from_slice(consumer.as_bytes());
    k.push(0);
    k.extend_from_slice(&off.to_be_bytes());
    k
}

pub fn encode_lease(l: &Lease) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&l.deadline_ms.to_le_bytes());
    v.extend_from_slice(&l.delivery_count.to_le_bytes());
    v.extend_from_slice(&l.lease_gen.to_le_bytes());
    v
}

pub fn decode_lease(v: &[u8]) -> Option<Lease> {
    if v.len() < 16 {
        return None;
    }
    Some(Lease {
        deadline_ms: i64::from_le_bytes(v[0..8].try_into().ok()?),
        delivery_count: u32::from_le_bytes(v[8..12].try_into().ok()?),
        lease_gen: u32::from_le_bytes(v[12..16].try_into().ok()?),
    })
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
        /// Subkey for encrypting DLQ reference records (routing key "$dlq"),
        /// derived by the handler which holds the stream key.
        dlq_subkey: [u8; 32],
    },
    Settle {
        consumer: String,
        acks: Vec<(u64, u32)>,
        retries: Vec<(u64, u32, u64)>, // (off, gen, delay_ms)
        extends: Vec<(u64, u32, u64)>, // (off, gen, visibility_ms)
        max_deliveries: u32,
        dlq_subkey: [u8; 32],
    },
}

#[derive(Debug, Clone)]
pub enum QueueOut {
    Received {
        /// (offset, gen, attempts) for each newly leased message.
        leased: Vec<(u64, u32, u32)>,
        backlog: u64,
    },
    Settled {
        acked: usize,
        retried: usize,
        extended: usize,
        dlq: usize,
        backlog: u64,
    },
}
