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
//!
//! ```text
//! <hash16> 'c' <consumer>               cursor (u64 LE): all below settled
//! <hash16> 'l' <consumer> 0x00 <off BE> lease {deadline i64, count u32, gen u32}
//! <hash16> 'x' <consumer> 0x00 <off BE> settled-above-cursor marker
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Lease {
    pub deadline_ms: i64,
    pub delivery_count: u32,
    pub lease_gen: u32,
    /// Routing-key hash of the leased record — the per-key FIFO
    /// blocking identity (spec Stage 2 §2.3). Zeros on rows written by
    /// the un-keyed profile path.
    pub key_hash: [u8; 16],
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ConsumerState {
    /// The consumer GENERATION these rows belong to. A recreated
    /// consumer is a new generation; rows and ops of dead generations
    /// are inert (round 16: deletion as a generation-fenced saga).
    pub cgen: u64,
    pub cursor: u64,
    pub leases: BTreeMap<u64, Lease>,
    pub acked: BTreeSet<u64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QueueState {
    // mt-lint: allow(name-keyed-map): consumer name inside ONE stream's queue state — project-scoped by containment
    pub consumers: HashMap<String, ConsumerState>,
    pub loaded: bool,
}

/// Row keys carry the consumer GENERATION (big-endian, after the name
/// separator) so a recreated consumer's rows can never collide with a
/// dead generation's residue:
///
/// ```text
/// <hash16> 'c' <consumer> 0x00 <gen BE>          cursor
/// <hash16> 'l' <consumer> 0x00 <gen BE> <off BE> lease
/// <hash16> 'x' <consumer> 0x00 <gen BE> <off BE> settled marker
/// ```
///
/// `state_prefix` (name + separator, NO generation) covers every
/// generation — cleanup deletes a consumer's rows across all of them.
pub(crate) fn state_prefix(hash: &[u8; 16], tag: u8, consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(18 + consumer.len());
    k.extend_from_slice(hash);
    k.push(tag);
    k.extend_from_slice(consumer.as_bytes());
    k.push(0);
    k
}

pub(crate) fn cursor_key(hash: &[u8; 16], consumer: &str, cgen: u64) -> Vec<u8> {
    let mut k = state_prefix(hash, b'c', consumer);
    k.extend_from_slice(&cgen.to_be_bytes());
    k
}

pub(crate) fn lease_key(hash: &[u8; 16], consumer: &str, cgen: u64, off: u64) -> Vec<u8> {
    let mut k = state_prefix(hash, b'l', consumer);
    k.extend_from_slice(&cgen.to_be_bytes());
    k.extend_from_slice(&off.to_be_bytes());
    k
}

pub(crate) fn ack_key(hash: &[u8; 16], consumer: &str, cgen: u64, off: u64) -> Vec<u8> {
    let mut k = state_prefix(hash, b'x', consumer);
    k.extend_from_slice(&cgen.to_be_bytes());
    k.extend_from_slice(&off.to_be_bytes());
    k
}

/// Canonical decoder for generation-qualified queue keys. Truncated or foreign
/// rows are corruption, never evidence that a generation has been deleted.
#[warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
pub(crate) fn decode_state_key<'a>(
    hash: &[u8; 16],
    tag: u8,
    key: &'a [u8],
) -> Result<(&'a str, u64, Option<u64>), &'static str> {
    let identity_error = "queue key identity mismatch";
    let (prefix, rest) = key.split_first_chunk::<16>().ok_or(identity_error)?;
    let (kind, rest) = rest.split_first().ok_or(identity_error)?;
    if prefix != hash || *kind != tag {
        return Err(identity_error);
    }
    let separator_error = "queue key missing separator";
    let separator = rest
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(separator_error)?;
    let (name, tail) = rest.split_at_checked(separator).ok_or(separator_error)?;
    let (_, tail) = tail.split_first().ok_or(separator_error)?;
    let name = std::str::from_utf8(name).map_err(|_| "queue key name is not UTF-8")?;
    if name.is_empty() {
        return Err("queue key name is empty");
    }
    let width_error = "queue key has invalid width";
    let (generation, offset) = match tag {
        b'c' => (
            u64::from_be_bytes(tail.try_into().map_err(|_| width_error)?),
            None,
        ),
        b'l' | b'x' => {
            let (generation, offset) = tail.split_first_chunk::<8>().ok_or(width_error)?;
            (
                u64::from_be_bytes(*generation),
                Some(u64::from_be_bytes(
                    offset.try_into().map_err(|_| width_error)?,
                )),
            )
        }
        _ => return Err("unknown queue key tag"),
    };
    Ok((name, generation, offset))
}

pub(crate) fn decode_counter(raw: &[u8]) -> Result<u64, &'static str> {
    Ok(u64::from_le_bytes(
        raw.try_into()
            .map_err(|_| "queue counter has invalid width")?,
    ))
}

pub(crate) fn decode_consumer_record(raw: &[u8]) -> Result<ConsumerRecord, serde_json::Error> {
    serde_json::from_slice(raw)
}

pub(crate) fn encode_lease(l: &Lease) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.extend_from_slice(&l.deadline_ms.to_le_bytes());
    v.extend_from_slice(&l.delivery_count.to_le_bytes());
    v.extend_from_slice(&l.lease_gen.to_le_bytes());
    v.extend_from_slice(&l.key_hash);
    v
}

pub(crate) fn decode_lease(v: &[u8]) -> Option<Lease> {
    if !matches!(v.len(), 16 | 32) {
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
pub(crate) struct ConsumerConfig {
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

/// The lease window. Every span the committer adds to `now_ms()` when it
/// writes a lease row (a pull's or an extend's visibility, a retry's delay,
/// the configured timeout they default to) is bounded here to 12 h. The
/// bound keeps `now + window` total in `shard/transaction/queue` (a `u32` of
/// milliseconds is under 50 days) and keeps every lease expiring: an
/// unbounded wire `u64` wrapped into the past or overflowed the committer,
/// and in between it wrote a lease that never expired, so its record was
/// never redelivered, never reached `maxAttempts`, never dead-lettered, and
/// its routing key stayed blocked.
pub(crate) const MAX_LEASE_WINDOW_MS: u32 = 12 * 3600 * 1000;
/// The floor a pull has always applied to visibility; an extend shares it.
const MIN_VISIBILITY_MS: u32 = 1_000;
/// A retry that names no delay releases its record after one second.
const DEFAULT_RETRY_DELAY_MS: u32 = 1_000;

/// The visibility a pull or an extend asked for, or the consumer's
/// configured timeout when it asked for none: 1 s ..= 12 h.
pub(crate) fn visibility_window_ms(requested: Option<u64>, cfg: &ConsumerConfig) -> u32 {
    let requested = requested.unwrap_or(u64::from(cfg.visibility_timeout_ms));
    bounded_window_ms(requested, MIN_VISIBILITY_MS)
}

/// The delay a retry asked for, or one second when it asked for none:
/// 0 ..= 12 h. Zero releases the record at once.
pub(crate) fn retry_delay_ms(requested: Option<u64>) -> u32 {
    let requested = requested.unwrap_or(u64::from(DEFAULT_RETRY_DELAY_MS));
    bounded_window_ms(requested, 0)
}

/// The timeout a consumer put stores: 1 s ..= 12 h, so the value
/// `visibility_window_ms` defaults to is already inside the window.
pub(crate) fn configured_visibility_ms(requested: u32) -> u32 {
    requested.clamp(MIN_VISIBILITY_MS, MAX_LEASE_WINDOW_MS)
}

/// A wire `u64` into the window: anything past `u32` is past 12 h, so the
/// failed conversion saturates rather than truncates.
#[warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
fn bounded_window_ms(requested: u64, floor: u32) -> u32 {
    u32::try_from(requested)
        .unwrap_or(u32::MAX)
        .clamp(floor, MAX_LEASE_WINDOW_MS)
}

/// Consumer lifecycle (round 16). `Deleted` is a TOMBSTONE, kept so
/// recreation allocates a strictly higher generation — the property
/// that makes late old-generation writes and residual rows inert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ConsumerLifecycle {
    Active,
    Deleting,
    Deleted,
}

/// What the parent-identity config row actually stores.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ConsumerRecord {
    pub generation: u64,
    pub state: ConsumerLifecycle,
    pub config: ConsumerConfig,
}

pub(crate) fn config_key(hash: &[u8; 16], consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'C');
    k.extend_from_slice(consumer.as_bytes());
    k
}

/// DURABLE consumer-generation fence: `<hash16> 'F' <consumer>` →
/// minimum live generation (u64 LE).
///
/// The engine-resident fence map survives handle eviction but NOT a
/// shard moving to another engine or instance — a fresh owner opened an
/// empty map, so a parked generation-1 Receive that arrived after the
/// move was accepted and re-created lease rows for a generation whose
/// deletion had already returned 204 (round-19 must-fix 3). The fence
/// therefore lives in the shard DB, written through the SAME ordered
/// committer path as the cleanup it guards, and every owner consults it
/// before Receive/Settle.
///
/// The row is tiny, monotonic, and long-lived: new generations are always
/// above it, so it never needs deleting.
pub(crate) fn fence_key(hash: &[u8; 16], consumer: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + consumer.len());
    k.extend_from_slice(hash);
    k.push(b'F');
    k.extend_from_slice(consumer.as_bytes());
    k
}

pub(crate) enum QueueOp {
    Receive {
        consumer: String,
        /// The consumer generation this op belongs to (from the config
        /// record the HTTP layer loaded). Fenced generations refuse.
        cgen: u64,
        max: usize,
        /// From `visibility_window_ms`: 1 s ..= 12 h, so `now + visibility_ms`
        /// is total and the lease always expires.
        visibility_ms: u32,
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
    /// Parent-identity lifecycle CAS: Active -> Deleting
    /// (`deleting: true`) or Deleting -> Deleted (`deleting: false`),
    /// fenced to the exact generation. Both directions are idempotent
    /// at their target state.
    ConfigLifecycle {
        consumer: String,
        expect_gen: u64,
        deleting: bool,
    },
    /// One BOUNDED segment-cleanup step for the deletion saga: install
    /// the generation fence (everything below `fence_below` is dead on
    /// this segment), then delete AT MOST `max_rows`/`max_bytes` worth
    /// of this consumer's state rows whose generation is strictly below
    /// the fence — durable rows plus anything a dead generation staged
    /// earlier in the same commit group. Rows at or above the fence are
    /// NEVER touched: a name is not an identity, and a stale deletion
    /// must not erase a recreated generation. The reply says whether
    /// more dead rows remain; the saga keeps stepping until none do.
    ConfigDeleteStep {
        consumer: String,
        fence_below: u64,
        max_rows: usize,
        max_bytes: usize,
    },
    Settle {
        consumer: String,
        cgen: u64,
        acks: Vec<(u64, u32)>,
        retries: Vec<(u64, u32, u32)>, // (off, gen, delay_ms from `retry_delay_ms`)
        extends: Vec<(u64, u32, u32)>, // (off, gen, visibility_ms from `visibility_window_ms`)
        max_deliveries: u32,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum QueueOut {
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
        rec: Option<ConsumerRecord>,
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
    /// Answer to `ConfigDeleteStep`: `complete` means no dead-generation
    /// rows remain on this segment (the fence is installed either way);
    /// `deleted_rows` is what THIS step staged for deletion.
    DeleteStep {
        complete: bool,
        #[allow(
            dead_code,
            reason = "QueueOut::DeleteStep::deleted_rows; exact deletion counts are consumed by generation-fencing tests but not the production adapter; dropping this measured result would weaken stale-replay and bounded-deletion assertions"
        )]
        deleted_rows: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        ConsumerConfig, MAX_LEASE_WINDOW_MS, ack_key, configured_visibility_ms, cursor_key,
        decode_state_key, lease_key, retry_delay_ms, state_prefix, visibility_window_ms,
    };
    use proptest::prelude::ProptestConfig;
    use proptest::prop_assert_eq;

    #[test]
    fn lease_windows_are_held_between_their_floor_and_twelve_hours() {
        assert_eq!(MAX_LEASE_WINDOW_MS, 43_200_000);
        let cfg = ConsumerConfig {
            visibility_timeout_ms: 45_000,
            ..ConsumerConfig::default()
        };
        for (requested, held) in [
            (None, 45_000),
            (Some(0), 1_000),
            (Some(999), 1_000),
            (Some(1_000), 1_000),
            (Some(5_000), 5_000),
            (Some(43_200_000), 43_200_000),
            (Some(43_200_001), 43_200_000),
            // One past u32::MAX: a truncating cast would read this as 0.
            (Some(4_294_967_296), 43_200_000),
            (Some(9_000_000_000_000_000_000), 43_200_000),
            (Some(u64::MAX), 43_200_000),
        ] {
            assert_eq!(
                visibility_window_ms(requested, &cfg),
                held,
                "visibility {requested:?}"
            );
        }
        let unbounded = ConsumerConfig {
            visibility_timeout_ms: 0,
            ..ConsumerConfig::default()
        };
        assert_eq!(
            visibility_window_ms(None, &unbounded),
            1_000,
            "a configured 0 is floored too"
        );
        for (requested, held) in [
            (None, 1_000),
            (Some(0), 0),
            (Some(1), 1),
            (Some(7), 7),
            (Some(43_200_000), 43_200_000),
            (Some(43_200_001), 43_200_000),
            (Some(4_294_967_296), 43_200_000),
            (Some(u64::MAX), 43_200_000),
        ] {
            assert_eq!(retry_delay_ms(requested), held, "retry {requested:?}");
        }
        for (requested, held) in [
            (0, 1_000),
            (1_000, 1_000),
            (30_000, 30_000),
            (43_200_001, 43_200_000),
            (u32::MAX, 43_200_000),
        ] {
            assert_eq!(
                configured_visibility_ms(requested),
                held,
                "configured {requested}"
            );
        }
    }

    #[test]
    fn queue_key_identity_and_name_errors_remain_distinct() {
        let hash = [7; 16];
        let valid = cursor_key(&hash, "worker", 9);
        for end in 0..17 {
            assert_eq!(
                decode_state_key(&hash, b'c', valid.get(..end).unwrap()),
                Err("queue key identity mismatch")
            );
        }
        assert_eq!(
            decode_state_key(&[8; 16], b'c', &valid),
            Err("queue key identity mismatch")
        );
        assert_eq!(
            decode_state_key(&hash, b'l', &valid),
            Err("queue key identity mismatch")
        );
        let mut missing = hash.to_vec();
        missing.extend_from_slice(b"cworker");
        assert_eq!(
            decode_state_key(&hash, b'c', &missing),
            Err("queue key missing separator")
        );
        let empty = cursor_key(&hash, "", 0);
        assert_eq!(
            decode_state_key(&hash, b'c', &empty),
            Err("queue key name is empty")
        );
        let mut invalid_utf8 = hash.to_vec();
        invalid_utf8.extend_from_slice(&[b'c', 255, 0]);
        invalid_utf8.extend_from_slice(&[0; 8]);
        assert_eq!(
            decode_state_key(&hash, b'c', &invalid_utf8),
            Err("queue key name is not UTF-8")
        );
    }

    #[test]
    fn queue_key_tags_require_their_exact_numeric_width() {
        let hash = [7; 16];
        let cases = [(b'c', 8, None), (b'l', 16, Some(0)), (b'x', 16, Some(0))]
            .into_iter()
            .flat_map(|(tag, width, offset)| {
                (0..=18).map(move |length| (tag, width, offset, length))
            });
        for (tag, width, offset, length) in cases {
            let mut raw = state_prefix(&hash, tag, "worker");
            raw.extend(std::iter::repeat_n(0, length));
            let expected = if length == width {
                Ok(("worker", 0, offset))
            } else {
                Err("queue key has invalid width")
            };
            assert_eq!(decode_state_key(&hash, tag, &raw), expected);
        }
        for length in [0, 8, 16] {
            let mut raw = state_prefix(&hash, b'?', "worker");
            raw.extend(std::iter::repeat_n(0, length));
            assert_eq!(
                decode_state_key(&hash, b'?', &raw),
                Err("unknown queue key tag")
            );
        }
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]

        #[test]
        fn quality_lease_windows_follow_the_u64_clamp(
            near in 0_u64..=100_000_000,
            anywhere in proptest::num::u64::ANY,
            configured in proptest::num::u32::ANY,
        ) {
            let cfg = ConsumerConfig { visibility_timeout_ms: configured, ..ConsumerConfig::default() };
            for requested in [near, anywhere] {
                prop_assert_eq!(u64::from(visibility_window_ms(Some(requested), &cfg)), requested.clamp(1_000, 43_200_000));
                prop_assert_eq!(u64::from(retry_delay_ms(Some(requested))), requested.min(43_200_000));
            }
            prop_assert_eq!(u64::from(visibility_window_ms(None, &cfg)), u64::from(configured).clamp(1_000, 43_200_000));
            prop_assert_eq!(configured_visibility_ms(configured), configured.clamp(1_000, 43_200_000));
        }

        #[test]
        fn quality_queue_state_key_roundtrip(
            name in "[a-z][a-z0-9-]{0,20}",
            generation in proptest::num::u64::ANY,
            offset in proptest::num::u64::ANY,
        ) {
            let hash = [7; 16];
            let cursor = cursor_key(&hash, &name, generation);
            let lease = lease_key(&hash, &name, generation, offset);
            let ack = ack_key(&hash, &name, generation, offset);
            prop_assert_eq!(decode_state_key(&hash, b'c', &cursor).unwrap(), (name.as_str(), generation, None));
            prop_assert_eq!(decode_state_key(&hash, b'l', &lease).unwrap(), (name.as_str(), generation, Some(offset)));
            prop_assert_eq!(decode_state_key(&hash, b'x', &ack).unwrap(), (name.as_str(), generation, Some(offset)));
            for (tag, mut malformed) in [(b'c', cursor), (b'l', lease), (b'x', ack)] {
                prop_assert_eq!(decode_state_key(&[8; 16], tag, &malformed), Err("queue key identity mismatch"));
                prop_assert_eq!(malformed.pop().is_some(), true);
                prop_assert_eq!(decode_state_key(&hash, tag, &malformed), Err("queue key has invalid width"));
                malformed.extend_from_slice(&[0, 0]);
                prop_assert_eq!(decode_state_key(&hash, tag, &malformed), Err("queue key has invalid width"));
            }
        }
    }
}
