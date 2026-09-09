//! Shard log engine: one SlateDB per shard, hash-first keyspace, committer +
//! durable-watermark acker (§3.4). Record values ARE the wire frames (§3.7):
//! encryption happens in the committer, after offset assignment, because the
//! the authenticated metadata includes the assigned offset.
//!
//! Keyspace (hash-first so a hash range is one contiguous split range):
//!   <hash16> 't'                 tail state
//!   <hash16> 'r' <offset u64 BE> record frame

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::config::{DurabilityLevel, WriteOptions};
use slatedb::{Db, WriteBatch};
use tokio::sync::{Notify, mpsc, oneshot};

pub(crate) mod record;
#[cfg(test)]
pub use record::read_frames;
pub use record::{FrameReadResult, read_frames_range};
mod commit_handoff;
use commit_handoff::{Attachment, CommitHandoff};
mod commit_plan;
mod history_partition;
mod lifecycle;
mod transaction;
pub use commit_plan::{AppendFinish, CloseReq, EnqueueError, SealFenceReq, UsageAckScope};
use commit_plan::{
    BillingAckDecision, ConsumerGeneration, DurableEffects, ProducerDecision, decide_billing_ack,
    decide_consumer_generation, decide_producer, seal_authorized,
};
pub(crate) use lifecycle::EngineShutdown;

pub fn tail_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(hash);
    k.push(b't');
    k
}

pub fn record_key(hash: &[u8; 16], offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(25);
    k.extend_from_slice(hash);
    k.push(b'r');
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

/// Tail value v3:
/// [ver u8=3][next u64][last_ts i64][logical u64][absorbed u64][trimmed u64][flags u8][seq_len u16][seq][route16?][trim_safe_to u64?][unabsorbed_bytes u64?]
///
/// `flags` is a bitmask: bit0 = closed, bit1 = history v2 (the stream's
/// absorbed range lives in the shared per-shard partition, not a
/// per-stream DB). The optional trailing route16 (the shard-routing
/// hash), trim_safe_to and unabsorbed_bytes are backward-compatible
/// extensions: v3 decoders read exactly `seq_len` seq bytes and ignore
/// trailing bytes. `trim_safe_to` is the highest offset physical
/// trimming may reach (the absorbed boundary as of the PREVIOUS
/// advance — one advance of lag so in-flight readers holding a stale
/// absorbed snapshot never lose their range); `unabsorbed_bytes` is the
/// exact stored frame bytes in [absorbed, next), maintained by the
/// committer so restart rediscovery sizes pending work truthfully
/// instead of estimating (a single 32 MiB record used to estimate as
/// 1 KiB and never re-absorb under the default policy). Downgrade
/// caveat: a pre-bitmask binary reads flags with `== 1`, so it would
/// see a closed+v2 stream (flags=3) as open — acceptable for
/// forward-only deployments, noted here because it is not zero.
fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let mut v = Vec::with_capacity(76 + seq.len());
    v.push(3);
    v.extend_from_slice(&t.next.to_le_bytes());
    v.extend_from_slice(&t.ts.to_le_bytes());
    v.extend_from_slice(&t.logical.to_le_bytes());
    v.extend_from_slice(&t.absorbed.to_le_bytes());
    v.extend_from_slice(&t.trimmed.to_le_bytes());
    let mut flags = 0u8;
    if t.closed {
        flags |= 1;
    }
    if t.history_v2 {
        flags |= 2;
    }
    v.push(flags);
    v.extend_from_slice(&(seq.len() as u16).to_le_bytes());
    v.extend_from_slice(seq);
    v.extend_from_slice(&t.route);
    v.extend_from_slice(&t.trim_safe_to.to_le_bytes());
    v.extend_from_slice(&t.unabsorbed_bytes.to_le_bytes());
    v
}

fn decode_tail(v: &[u8]) -> Option<TailFields> {
    if v.len() < 43 || (v[0] != 2 && v[0] != 3) {
        return None;
    }
    let v3 = v[0] == 3;
    let next = u64::from_le_bytes(v[1..9].try_into().ok()?);
    let ts = i64::from_le_bytes(v[9..17].try_into().ok()?);
    let logical = u64::from_le_bytes(v[17..25].try_into().ok()?);
    let absorbed = u64::from_le_bytes(v[25..33].try_into().ok()?);
    let trimmed = u64::from_le_bytes(v[33..41].try_into().ok()?);
    let (flags, seq_at) = if v3 { (v[41], 42usize) } else { (0u8, 41usize) };
    let seq_len = u16::from_le_bytes(v.get(seq_at..seq_at + 2)?.try_into().ok()?) as usize;
    let seq = if seq_len == 0 {
        None
    } else {
        Some(String::from_utf8(v.get(seq_at + 2..seq_at + 2 + seq_len)?.to_vec()).ok()?)
    };
    let route_at = seq_at + 2 + seq_len;
    // Historical tails end after seq, route, or trim_safe_to. A partial
    // known extension is corruption. Once the complete known suffix is
    // present, preserve the layout's forward-compatible trailing bytes.
    let extension_len = v.len().checked_sub(route_at)?;
    if (!matches!(extension_len, 0 | 16 | 24) && extension_len < 32) || flags & !3 != 0 {
        return None;
    }
    let route: [u8; 16] = v
        .get(route_at..route_at + 16)
        .and_then(|r| r.try_into().ok())
        .unwrap_or([0u8; 16]);
    let le8 = |at: usize| -> u64 {
        v.get(at..at + 8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    };
    let trim_safe_to = le8(route_at + 16);
    let unabsorbed_bytes = le8(route_at + 24);
    Some(TailFields {
        next,
        ts,
        logical,
        absorbed,
        trimmed,
        seq,
        closed: flags & 1 != 0,
        history_v2: flags & 2 != 0,
        route,
        trim_safe_to,
        unabsorbed_bytes,
    })
}

/// Existing malformed bytes must never initialize a fresh segment.
fn stored_tail(raw: &[u8]) -> Result<TailFields, slatedb::Error> {
    let tail = decode_tail(raw)
        .ok_or_else(|| slatedb::Error::data("invalid persisted tail encoding".into()))?;
    // Decoding owns byte compatibility; every serving/recovery reader also
    // validates the state before it can authorize an offset or a repair.
    if tail.trimmed > tail.absorbed
        || tail.absorbed > tail.next
        || tail.trim_safe_to > tail.absorbed
    {
        return Err(slatedb::Error::data("inconsistent persisted tail".into()));
    }
    Ok(tail)
}

/// Fixed-width metadata is exactly eight bytes; short and trailing bytes
/// indicate corruption. Absence is handled separately by the repository.
pub(crate) fn decode_cursor(raw: &[u8]) -> Result<u64, slatedb::Error> {
    let bytes: [u8; 8] = raw
        .try_into()
        .map_err(|_| slatedb::Error::data("invalid persisted cursor length".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

/// Test-only: encode a tail then STRIP the trailing exact-gauge field,
/// producing the pre-gauge layout older builds wrote. DST uses this to
/// prove the R26-4 open-time repair; production code never writes it.
#[cfg(test)]
pub fn encode_tail_without_gauge_for_tests(t: &TailFields) -> Vec<u8> {
    let mut v = encode_tail(t);
    v.truncate(v.len() - 8);
    v
}

#[cfg(test)]
pub fn decode_tail_for_tests(v: &[u8]) -> Option<TailFields> {
    decode_tail(v)
}

/// Test-only: the production tail encoder, exposed so golden tests can
/// pin the exact v3 byte layout without going through a shard engine.
#[cfg(test)]
pub fn encode_tail_for_tests(t: &TailFields) -> Vec<u8> {
    encode_tail(t)
}

/// Per-routing-key Stream-Seq row (ROUTING-V3 §3.6): seq is scoped to
/// the KEY, not the segment — a segment carries many keys' lanes.
pub fn seq_key(hash: &[u8; 16], key_hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(hash);
    k.push(b's');
    k.extend_from_slice(key_hash);
    k
}

pub fn producer_key(hash: &[u8; 16], key_hash: &[u8; 16], producer_id: &str) -> Vec<u8> {
    // <segment identity> 'q' <routing-key hash> <producer id> — producer
    // sessions are scoped per ROUTING KEY (review finding 5): one
    // producer id keeps independent sequence lanes for different keys,
    // and the scope does not change across a split (each key's lane
    // follows its key through the predecessor chain).
    let mut k = Vec::with_capacity(33 + producer_id.len());
    k.extend_from_slice(hash);
    k.push(b'q');
    k.extend_from_slice(key_hash);
    k.extend_from_slice(producer_id.as_bytes());
    k
}

/// Durable dirty-stream index (static audit P1): a marker per stream
/// with outstanding maintenance — unabsorbed tail (`absorbed < next`)
/// or pending physical trim (`trimmed < trim_safe_to`) — written in the
/// SAME committer batch as the tail it describes and deleted in the
/// batch that catches both up. A fresh owner scans this prefix once at
/// absorber start, so outstanding work is rediscovered after
/// restart/handoff without the customer ever touching the stream again.
/// Lives under a sentinel "hash" of all-0xFF; a truncated-SHA stream
/// hash CAN equal that value (p = 2^-128, astronomically unlikely, not
/// impossible), but the distinct tag byte `D` — no stream row uses it —
/// is what actually guarantees these keys never collide with
/// `<hash16><tag>` stream rows. The sentinel's job is only to sort the
/// index at the end of the keyspace for one cheap range scan. NOTE for
/// physical range splitting (future): these markers sort OUTSIDE every
/// stream's route range, so a range split cannot carry them into the
/// child by key range — the index needs a route-local representation
/// (or its own tracker partition) before splits land; static handoff
/// (new owner opens the whole shard DB) is unaffected.
const DIRTY_SENTINEL: [u8; 16] = [0xFF; 16];

pub fn dirty_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.extend_from_slice(&DIRTY_SENTINEL);
    k.push(b'D');
    k.extend_from_slice(hash);
    k
}

/// Durable maintenance summary for one stream.
///
/// R24-A. Maintenance backpressure previously derived its backlog from
/// two process-lifetime atomics (`INGEST_BYTES_TOTAL - ABSORB_BYTES_TOTAL`),
/// which is wrong in four independent ways:
///
///   * the ingest counter is bumped while the batch is being ASSEMBLED,
///     so a failed group write leaves phantom backlog the absorber can
///     never retire — a permanent, fictional 503;
///   * both counters reset on restart, so a shard holding hundreds of MB
///     of durable unabsorbed data reports zero backlog and accepts
///     writes, and `saturating_sub` then keeps reporting zero until new
///     ingest catches up with the historical absorbed count;
///   * they are process-wide, so after an ownership move the old owner
///     keeps shedding for a shard it no longer holds while the new owner
///     inherits the data with no history and admits freely;
///   * the "per-shard" figure actually read policy-DEFERRED bytes, which
///     is a sparse-absorption decision, not the eligible backlog.
///
/// The fix is to make the DURABLE row the source of truth. These bytes
/// ride in the existing dirty-stream index, which is already written in
/// the same committer batch as the tail it describes — so it commits if
/// and only if the append commits, survives restart, and moves with the
/// shard because it lives in the shard's own DB.
///
/// `oldest_unabsorbed_ms` is deliberately conservative: it is only ever
/// carried forward or cleared, never made younger, so a restart may
/// overstate age but can never reset it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamMaintenance {
    pub absorbed: u64,
    pub next: u64,
    pub unabsorbed_bytes: u64,
    pub oldest_unabsorbed_ms: i64,
}

/// v2 layout: absorbed, next, unabsorbed_bytes, oldest_unabsorbed_ms.
/// v1 rows (16 bytes) decode with zero bytes/age — an old row is still a
/// valid "this stream has outstanding maintenance" marker, it just
/// contributes nothing to the byte bound until its next commit.
fn dirty_value(m: &StreamMaintenance) -> [u8; 32] {
    let mut v = [0u8; 32];
    v[..8].copy_from_slice(&m.absorbed.to_le_bytes());
    v[8..16].copy_from_slice(&m.next.to_le_bytes());
    v[16..24].copy_from_slice(&m.unabsorbed_bytes.to_le_bytes());
    v[24..].copy_from_slice(&m.oldest_unabsorbed_ms.to_le_bytes());
    v
}

/// Test-only: the production dirty-row encoder, exposed so golden tests
/// can pin the exact 32-byte LE layout.
#[cfg(test)]
pub fn dirty_value_for_tests(m: &StreamMaintenance) -> [u8; 32] {
    dirty_value(m)
}

/// The one durable maintenance row per physical shard.
///
/// Sits beside the dirty-stream index under the same sentinel, with tag
/// `M`. Written in the SAME committer batch as the appends it accounts
/// for, and in the same absorbed-boundary batch that retires them — so
/// it commits if and only if the work it describes commits, and a failed
/// group write leaves it untouched.
/// Exact frame-byte flow, in the maintenance unit (R25-B). These are
/// process-lifetime observability counters, NOT admission inputs — the
/// admission source of truth is each engine's durable row.
pub static INGEST_FRAME_BYTES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static ABSORBED_FRAME_BYTES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn shard_maint_key() -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(&DIRTY_SENTINEL);
    k.push(b'M');
    k
}

/// Engine-owned maintenance state for one physical shard (R25-A).
///
/// The unit is EXACT ENCODED FRAME BYTES in `[absorbed, next)` — the
/// same unit `TailFields.unabsorbed_bytes` carries. The R24 version of
/// this accounting added uncompressed PAYLOAD bytes on append while
/// retiring compressed FRAME bytes on absorption; on the soak's all-`x`
/// records with FRAME_COMPRESS=1 that manufactured a 9.4% "absorption
/// ratio" and 3.87 GB of fictional backlog that were, in large part,
/// the benchmark's compression ratio. One unit, both directions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardMaintenance {
    pub version: u64,
    /// Exact encoded frame bytes still present in the shard tier and
    /// not covered by the durable absorbed boundary.
    pub unabsorbed_frame_bytes: u64,
    /// When the backlog moved 0 -> nonzero. Diagnostic only; it may
    /// conservatively predate the current oldest record.
    pub backlog_started_ms: i64,
    /// Last successful durable retirement of backlog bytes. This is the
    /// safe stall signal: unlike an "oldest record" approximation it
    /// does not become permanently old while absorption keeps making
    /// progress under continuous traffic.
    pub last_progress_ms: i64,
}

impl ShardMaintenance {
    /// Apply a committed delta. Retiring more than exists is an ERROR,
    /// not a saturation: it means the two sides of the accounting have
    /// diverged, and clamping would hide exactly the class of unit bug
    /// this type exists to prevent.
    pub fn apply_delta(
        self,
        added_frame_bytes: u64,
        retired_frame_bytes: u64,
        now_ms: i64,
    ) -> anyhow::Result<Self> {
        let available = self
            .unabsorbed_frame_bytes
            .checked_add(added_frame_bytes)
            .ok_or_else(|| anyhow::anyhow!("maintenance byte overflow"))?;
        anyhow::ensure!(
            retired_frame_bytes <= available,
            "maintenance retirement exceeds backlog: retire={} available={}",
            retired_frame_bytes,
            available,
        );
        let next = available - retired_frame_bytes;
        let mut out = self;
        out.version = out.version.saturating_add(1);
        out.unabsorbed_frame_bytes = next;
        if next == 0 {
            out.backlog_started_ms = 0;
            out.last_progress_ms = 0;
        } else {
            if self.unabsorbed_frame_bytes == 0 && added_frame_bytes > 0 {
                out.backlog_started_ms = now_ms;
                out.last_progress_ms = now_ms;
            }
            if retired_frame_bytes > 0 {
                out.last_progress_ms = now_ms;
            }
        }
        Ok(out)
    }

    /// Seconds since maintenance last made durable progress, while a
    /// backlog is outstanding. Zero when there is nothing to do.
    pub fn no_progress_secs(self, now_ms: i64) -> u64 {
        if self.unabsorbed_frame_bytes == 0 || self.last_progress_ms <= 0 {
            0
        } else {
            ((now_ms - self.last_progress_ms).max(0) / 1_000) as u64
        }
    }
}

const SHARD_MAINT_V2: u8 = 2;

pub fn encode_shard_maint(m: &ShardMaintenance) -> [u8; 40] {
    let mut v = [0u8; 40];
    v[0] = SHARD_MAINT_V2;
    v[8..16].copy_from_slice(&m.version.to_le_bytes());
    v[16..24].copy_from_slice(&m.unabsorbed_frame_bytes.to_le_bytes());
    v[24..32].copy_from_slice(&m.backlog_started_ms.to_le_bytes());
    v[32..40].copy_from_slice(&m.last_progress_ms.to_le_bytes());
    v
}

/// Row classification (R26-4). The R24 row was 16 untagged PAYLOAD-unit
/// bytes: on compressible data it overstates the frame backlog, but on
/// small incompressible frames the encoding overhead (headers, auth
/// tag) makes frames LARGER than payload, so it can also understate —
/// and an understated ledger makes the first exact retirement look like
/// over-retirement, which the checked accounting refuses forever. The
/// legacy value is therefore never trusted as frame bytes in either
/// direction: the opener rebuilds from the durable tails instead.
pub enum ShardMaintRow {
    Exact(ShardMaintenance),
    LegacyPayloadUnit,
}

pub fn decode_shard_maint_row(v: &[u8]) -> anyhow::Result<ShardMaintRow> {
    match v.len() {
        16 => Ok(ShardMaintRow::LegacyPayloadUnit),
        40 if v[0] == SHARD_MAINT_V2 => Ok(ShardMaintRow::Exact(ShardMaintenance {
            version: u64::from_le_bytes(v[8..16].try_into()?),
            unabsorbed_frame_bytes: u64::from_le_bytes(v[16..24].try_into()?),
            backlog_started_ms: i64::from_le_bytes(v[24..32].try_into()?),
            last_progress_ms: i64::from_le_bytes(v[32..40].try_into()?),
        })),
        _ => anyhow::bail!("unsupported shard maintenance row ({} bytes)", v.len()),
    }
}

/// Strict v2 decode: rows written by THIS build. A legacy 16-byte row
/// is an error here — callers that can meet one go through
/// `decode_shard_maint_row` and the rebuild path.
pub fn decode_shard_maint(v: &[u8]) -> anyhow::Result<ShardMaintenance> {
    match decode_shard_maint_row(v)? {
        ShardMaintRow::Exact(m) => Ok(m),
        ShardMaintRow::LegacyPayloadUnit => {
            anyhow::bail!("legacy payload-unit maintenance row; rebuild required")
        }
    }
}

/// Load this shard's durable maintenance state, or rebuild it from the
/// dirty index — synchronously, BEFORE the engine starts serving.
///
/// R25-A. The R24 restore ran asynchronously inside the absorber's
/// seed scan, which left four holes: the first request after a restart
/// could be admitted before the backlog was known; a late restore could
/// overwrite state a new append had already advanced; an old owner's
/// cleanup task could delete a new owner's entry; and process totals
/// went stale after ownership movement. Loading here, on the open path,
/// closes all four at once because the state cannot exist before the
/// engine and cannot outlive it.
///
/// A corrupt row or a failed rebuild scan is an ENGINE-OPEN FAILURE.
/// Translating either to "zero backlog" would silently disable the
/// safety bound exactly when the shard's state is least understood.
pub async fn load_or_rebuild_maintenance(db: &Db) -> anyhow::Result<ShardMaintenance> {
    match db.get(shard_maint_key()).await? {
        Some(v) => match decode_shard_maint_row(&v)? {
            ShardMaintRow::Exact(mut m) => {
                if m.unabsorbed_frame_bytes > 0 && m.last_progress_ms == 0 {
                    // A v2 row from before any progress cannot prove
                    // age. Start the clock now: byte limits still
                    // protect the process, and the stall clock begins
                    // honestly.
                    let now = now_ms();
                    m.backlog_started_ms = now;
                    m.last_progress_ms = now;
                }
                Ok(m)
            }
            // R26-4: the legacy value is PAYLOAD-unit — wrong in both
            // directions against exact frame accounting — so it is
            // ignored entirely and the ledger is rebuilt from the
            // durable tails.
            ShardMaintRow::LegacyPayloadUnit => rebuild_maintenance_from_tails(db).await,
        },
        None => rebuild_maintenance_from_tails(db).await,
    }
}

/// Rebuild the shard ledger from the dirty index + durable tails and
/// persist the exact v2 row. One-time cost per pre-existing shard.
///
/// R26-4 tail repair: a tail written before the exact gauge existed
/// decodes `unabsorbed_bytes == 0` while genuinely holding
/// `absorbed < next` — impossible for an exact tail (every encoded
/// frame is nonzero bytes), so that shape identifies a legacy row. Its
/// exact gauge is recomputed by summing the actual stored frames in
/// `[absorbed, next)` and the repaired tail is staged in the SAME
/// WriteBatch as the rebuilt shard row. Without this, the stream's
/// first boundary advance retires real frame bytes against a zero
/// ledger and the checked accounting (R26-3) refuses it forever.
async fn rebuild_maintenance_from_tails(db: &Db) -> anyhow::Result<ShardMaintenance> {
    let mut pfx = Vec::with_capacity(17);
    pfx.extend_from_slice(&DIRTY_SENTINEL);
    pfx.push(b'D');
    let mut total = 0u64;
    let mut repaired_tails: Vec<([u8; 16], TailFields)> = Vec::new();
    let mut iter = db.scan_prefix(&pfx[..], ..).await?;
    while let Some(kv) = iter.next().await? {
        if kv.key.len() != 33 {
            continue;
        }
        let mut h = [0u8; 16];
        h.copy_from_slice(&kv.key[17..33]);
        let Some(tail_raw) = db.get(tail_key(&h)).await? else {
            anyhow::bail!("dirty stream missing tail during maintenance rebuild");
        };
        let mut tail = stored_tail(&tail_raw)?;
        if tail.absorbed < tail.next && tail.unabsorbed_bytes == 0 {
            let mut sum = 0u64;
            let mut frames = db
                .scan(record_key(&h, tail.absorbed)..record_key(&h, tail.next))
                .await?;
            let mut count = 0u64;
            while let Some(rec) = frames.next().await? {
                sum = sum
                    .checked_add(rec.value.len() as u64)
                    .ok_or_else(|| anyhow::anyhow!("tail repair overflow"))?;
                count += 1;
            }
            // A missing frame row inside the unabsorbed range is
            // corruption, and repairing over it would bake the hole
            // into the ledger: fail the engine open instead.
            anyhow::ensure!(
                count == tail.next - tail.absorbed,
                "tail repair found {count} frames for range [{}, {})",
                tail.absorbed,
                tail.next,
            );
            tail.unabsorbed_bytes = sum;
            repaired_tails.push((h, tail.clone()));
        }
        total = total
            .checked_add(tail.unabsorbed_bytes)
            .ok_or_else(|| anyhow::anyhow!("maintenance rebuild overflow"))?;
    }

    let now = now_ms();
    let rebuilt = ShardMaintenance {
        version: 1,
        unabsorbed_frame_bytes: total,
        backlog_started_ms: if total > 0 { now } else { 0 },
        last_progress_ms: if total > 0 { now } else { 0 },
    };
    let mut wb = WriteBatch::new();
    for (h, tail) in &repaired_tails {
        wb.put(tail_key(h), encode_tail(tail));
    }
    wb.put(shard_maint_key(), encode_shard_maint(&rebuilt));
    db.write_with_options(wb, &WriteOptions::default()).await?;
    Ok(rebuilt)
}

pub fn decode_dirty_value(v: &[u8]) -> Option<StreamMaintenance> {
    // Preserve complete legacy fields (16/24 bytes) and current v2 (32).
    // Partial fields or unknown extensions are corrupt, never an empty marker.
    if !matches!(v.len(), 16 | 24 | 32) {
        return None;
    }
    let g8 = |o: usize| u64::from_le_bytes(v[o..o + 8].try_into().unwrap());
    Some(StreamMaintenance {
        absorbed: g8(0),
        next: g8(8),
        unabsorbed_bytes: if v.len() >= 24 { g8(16) } else { 0 },
        oldest_unabsorbed_ms: if v.len() >= 32 {
            i64::from_le_bytes(v[24..32].try_into().unwrap())
        } else {
            0
        },
    })
}

#[derive(Clone, Debug, Default)]
pub struct TailFields {
    pub next: u64,
    pub ts: i64,
    pub logical: u64,
    pub absorbed: u64,
    pub trimmed: u64,
    pub seq: Option<String>,
    pub closed: bool,
    /// This stream's absorbed range lives in the shared per-shard
    /// history partition (v2). Set by the first AbsorbedBatch that
    /// covers the stream; absorbed > 0 with this bit UNSET means legacy
    /// per-stream history (v1) and the stream stays v1.
    pub history_v2: bool,
    /// Shard-routing hash (stream_hash(name)); zeros for streams last
    /// written by callers without a name identity or by older binaries.
    pub route: [u8; 16],
    /// Highest offset physical trimming may reach: the absorbed boundary
    /// as of the PREVIOUS advance (one advance of lag, so in-flight
    /// readers holding a stale absorbed snapshot never lose their
    /// range). Trim maintenance moves `trimmed` toward this under a
    /// GLOBAL per-commit delete budget — boundary publication and
    /// physical trimming are decoupled so a 1,024-stream second
    /// absorption wave can never build one multi-gigabyte delete batch.
    pub trim_safe_to: u64,
    /// Exact stored frame bytes in [absorbed, next), maintained by the
    /// committer (appends add frame lengths; absorb advances subtract
    /// the bytes the absorber actually copied). Restart rediscovery
    /// reads this instead of estimating records × 1 KiB, so the default
    /// absorption policy's byte thresholds see the truth.
    pub unabsorbed_bytes: u64,
}

/// Read visibility level. `Durable` (the pinned default everywhere)
/// serves only storage-durable records. `Applied` additionally serves
/// the live tail's applied-but-not-yet-durable suffix — the product
/// surface's opt-in low-latency subscribe mode. Applied is a READ-SIDE
/// clamp only: acks, consumers, watches, absorption and trim stay
/// durable-gated exactly as before.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Deliver {
    #[default]
    Durable,
    Applied,
}

/// `durable` is what readers see; `applied` is what's in the memtable.
pub struct StreamState {
    pub durable: TailFields,
    pub applied: TailFields,
    /// Producer idempotence state: id -> (epoch, highest seq). Loaded from
    /// the durable `q` keys on first use, applied by the committer.
    /// producer id -> (epoch, seq, last_offset of that seq's commit,
    /// request hash of that commit — zeros when none was recorded).
    /// The offset makes a duplicate ack return the ORIGINAL committed
    /// offset instead of whatever the tail happens to be when the retry
    /// arrives — with interleaved appends those differ, and clients use
    /// the ack offset for read-your-write. The hash backs the product
    /// surface's 409 producer_sequence_reused (same tuple, different
    /// request).
    pub producers: HashMap<([u8; 16], String), (u64, u64, u64, [u8; 16])>,
    /// Per-routing-key Stream-Seq lanes (ROUTING-V3 §3.6), loaded
    /// lazily from the durable `s` rows and applied by the committer.
    pub seqs: HashMap<[u8; 16], String>,
    /// Queue-profile consumer state (loaded lazily by the committer).
    pub queue: crate::queue::QueueState,
}

pub struct StreamHandle {
    /// Physical database opening that admitted this handle and its durable ring.
    owner: std::sync::Weak<Db>,
    pub hash: [u8; 16],
    pub state: Mutex<StreamState>,
    pub notify: Notify,
    /// Fired by the committer at write success (apply), before the
    /// durability barrier — the wake for `Deliver::Applied` waiters.
    /// Durable waiters keep `notify` (fired only at durable dispatch);
    /// the separate wake keeps the pinned durable read path free of
    /// spurious wakeups.
    pub applied_notify: Notify,
    /// Durable-tail ring: recently-durable frames, published by
    /// dispatch_durable BEFORE acks go out, so a reader woken by an ack
    /// (or by tail notify) finds the record here without a DB scan.
    /// Empty unless ShardConfig.tail_ring_bytes > 0.
    pub ring: Mutex<TailRing>,
    /// Millisecond timestamp of the last lookup — feeds idle eviction
    /// (resident handles previously lived forever; a wide shard held
    /// 100k of them, the largest per-stream memory term).
    pub last_touch_ms: std::sync::atomic::AtomicU64,
    /// Round-13: this stream incarnation's durable-write pressure
    /// attribution (per-project memory admission). Bound once per
    /// resident handle from the tenant-qualified append path; the
    /// committer attributes group deltas at publish-on-success; Drop
    /// (idle eviction, shard close, owner movement) releases this
    /// instance's attribution exactly.
    pub pressure: std::sync::OnceLock<std::sync::Arc<crate::quota::StreamPressureBinding>>,
}

impl StreamHandle {
    /// Bind (idempotently) under the state lock: the seed is the
    /// APPLIED tail's exact unabsorbed_bytes at this instant, and the
    /// lock ordering against the committer's publish site guarantees
    /// the seed and the group-delta attribution never double- or
    /// under-count a group.
    pub fn bind_pressure(&self, adm: std::sync::Arc<crate::quota::ProjectAdmission>) {
        if self.pressure.get().is_some() {
            return;
        }
        let st = self.state.lock().unwrap();
        let _ = self.pressure.get_or_init(|| {
            std::sync::Arc::new(crate::quota::StreamPressureBinding::bind(
                adm,
                st.applied.unabsorbed_bytes,
            ))
        });
        drop(st);
    }
}

/// One durably-committed group's frames for one stream: a contiguous
/// offset range [first, next) in publish order.
pub struct RingBatch {
    pub first: u64,
    pub next: u64,
    pub frames: Vec<(u64, Bytes)>,
    pub bytes: usize,
}

#[derive(Default)]
pub struct TailRing {
    /// Contiguous in coverage: back.next of batch k == front.first of
    /// batch k+1 for consecutive batches (all publishes come through the
    /// same committer in offset order; eviction only pops the front).
    pub batches: std::collections::VecDeque<RingBatch>,
    pub bytes: usize,
}

impl TailRing {
    fn floor(&self) -> Option<u64> {
        self.batches.front().map(|b| b.first)
    }
    fn ceil(&self) -> Option<u64> {
        self.batches.back().map(|b| b.next)
    }
}

/// state-protocol feed: key IDs derived at append time, delivered to the
/// stream's touch journal only after the batch is durable (H2 hook).
/// `next_offset` is filled in by the committer once offsets are assigned so
/// wait responses can carry the covered stream offset (delta reads).
pub struct TouchFeed {
    pub journal: Arc<crate::touch::TouchJournal>,
    pub key_ids: Vec<u32>,
    pub next_offset: u64,
}

/// Producer identities the SERVER mints for records a client did not
/// coordinate (a seal's final append, a raw close carrying content).
/// They live in the same durable keyspace as public ones, so the wire
/// parser refuses this prefix — otherwise a caller could pre-create the
/// row and turn a later final append into a false duplicate.
pub const INTERNAL_PRODUCER_PREFIX: &str = "\u{0}prisma-internal\u{0}";

#[derive(Debug, Clone)]
pub struct ProducerReq {
    pub id: String,
    pub epoch: u64,
    pub seq: u64,
    /// Product-surface request hash (spec Stage 5 §7): 16 bytes over
    /// (operation kind, routing key, content type, body bytes, seal
    /// flag). None on the raw standards route — the pinned protocol's
    /// duplicate contract does not compare bodies.
    pub request_hash: Option<[u8; 16]>,
}

/// Validation failures that must be deferred until after the producer
/// duplicate check (a retry must return 204 even if e.g. the content type
/// no longer matches).
#[derive(Debug, Clone)]
pub enum DeferredErr {
    CtMismatch,
    BadBody(String),
}

/// Why a new producer sequence must not be accepted right now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SealedReject {
    Sealing,
    Sealed,
}

pub struct AppendReq {
    pub hash: [u8; 16],
    /// Shard-routing identity (`stream_hash(name)`), persisted into the
    /// tail so history v2 can key the shared partition route-first and a
    /// future shard split can clone by range. Zeros when the caller has
    /// no name-level identity (some DST paths); the v2 keyspace accepts
    /// a zero route, it just can't range-split those entries.
    pub route: [u8; 16],
    pub enqueued_at: std::time::Instant,
    /// Plaintext entries; encrypted in the committer with fresh stored nonces.
    pub entries: Vec<Bytes>,
    pub routing_key: String,
    /// stream_hash(routing_key), computed once at admission: the
    /// per-key Stream-Seq lane and postings/sketch identity.
    pub key_hash: [u8; 16],
    /// Predecessor segment identities for the routing key's lineage
    /// (nearest-first, empty for single-segment streams): the committer
    /// resolves producer state through this chain after a split
    /// (ROUTING-V3 §3.6) so a retry that committed on the sealed parent
    /// is suppressed by the child without consuming an offset.
    pub producer_lineage: Vec<[u8; 16]>,
    pub key_version: u32,
    pub subkey: [u8; 32],
    pub ts_hint_ms: Option<i64>,
    pub seq: Option<String>,
    pub bytes: usize,
    pub finish: AppendFinish,
    /// Billing attribution (docs/OBSERVABILITY-BILLING.md §6): when
    /// present, the committer updates the durable SegmentBillingMeta
    /// row in the SAME WriteBatch as the records. None on internal
    /// writes that are not customer ingest (fences, absorber copies).
    pub billing: Option<std::sync::Arc<crate::billing::BillingRef>>,
    /// The seal-claim generation that authorizes this append (present
    /// on every claim-authorized write: final-bearing closes, plain
    /// closes that installed an Empty claim, run_seal's segment closes,
    /// split/merge segment closes). Checked against the segment's
    /// fence AFTER duplicate detection and BEFORE any record is staged
    /// or the close applied: a stale generation means the claim this
    /// append belonged to was taken over, and its write must not land.
    pub seal_gen: Option<u64>,
    pub producer: Option<ProducerReq>,
    pub deferred_error: Option<DeferredErr>,
    /// The COLLECTION is sealing or sealed while this physical segment
    /// may still be open. Producer requests are admitted anyway so a
    /// retry can be recognised as a duplicate and answered with its
    /// original result — but a genuinely NEW sequence must be refused,
    /// which only the committer can tell apart. Evaluated after
    /// duplicate detection; None for ordinary appends.
    pub sealed_reject_new: Option<SealedReject>,
    pub touch: Option<TouchFeed>,
    /// Usage counters for the STREAM identity (http-side hash), so
    /// committer-side byte accounting lands on the same row as the
    /// request/record counters.
    pub usage: std::sync::Arc<crate::usage::Counters>,
    pub resp: oneshot::Sender<Result<AppendAck, AppendErr>>,
}

#[derive(Debug, Clone)]
pub struct AppendAck {
    pub last_offset: u64,
    pub next_offset: u64,
    pub closed: bool,
    /// Echoed on producer appends: (epoch, seq to report).
    pub producer: Option<(u64, u64)>,
    /// True for producer duplicates (204, body ignored).
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub enum AppendErr {
    SeqConflict {
        current: Option<String>,
    },
    /// Product surface only: same (producer, epoch, seq) with a
    /// DIFFERENT request hash (spec Stage 5 §7).
    ProducerSeqReused,
    Closed {
        next_offset: u64,
    },
    /// The seal claim authorizing this append was taken over: its
    /// generation is below the segment's fence. The write did not
    /// happen. Retryable — a live owner re-enters the claim (renewing
    /// its generation) and retries; a dead one's client is told the
    /// seal was superseded.
    SealSuperseded,
    ProducerGap {
        expected: u64,
        received: u64,
    },
    ProducerStale {
        current_epoch: u64,
    },
    ProducerEpochSeq,
    CtMismatch,
    BadBody(String),
    Internal(String),
    /// The shard was fenced by a new owner mid-request: retryable, the
    /// router converges within the anti-flap holdoff.
    Moved,
}

pub enum CommitOp {
    Append(AppendReq),
    /// Close without payload, encryption key, producer lane, or billing placeholders.
    Close(CloseReq),
    /// Queue-ordered takeover barrier without meaningless append fields.
    SealFence(SealFenceReq),
    /// Usage-outbox acknowledgment (§6.3): `_usage` durably holds the
    /// snapshot at `version` — delete the dirty marker iff no NEWER
    /// version exists, and delete the exact listed closed-month rows.
    /// Serialized through the committer so an append racing the drain
    /// keeps its newer version dirty. Fire-and-forget by design: a lost
    /// ack re-emits an identical snapshot, which the rollup
    /// deduplicates by version.
    UsageAck {
        hash: [u8; 16],
        scope: UsageAckScope,
        month_final_keys: Vec<Vec<u8>>,
    },
    /// Hard-delete/expiry closure (§6.2): advance the storage clock to
    /// the PERSISTED logical close instant (round-22 item 7 — the
    /// tombstone's stamp or the configured expiry, never "whenever
    /// this op finally ran"), zero the owned-bytes gauge, bump the
    /// version and mark dirty — the terminal storage observation for
    /// the incarnation. `close_ms <= 0` falls back to billing-now.
    BillingClose {
        hash: [u8; 16],
        close_ms: i64,
    },
    /// Durable fork-retention flag (round-22 item 7): a soft-deleted
    /// source retained by live forks keeps accruing storage under the
    /// fork billing contract; the flag must survive restarts and
    /// ownership moves on the row itself, not only in emitted
    /// snapshots.
    BillingRetained {
        hash: [u8; 16],
        retained: bool,
    },
    /// Queue-profile state transition (PROFILES.md §7): serialized with
    /// appends, durable at the watermark like everything else.
    Queue {
        hash: [u8; 16],
        op: crate::queue::QueueOp,
        resp: oneshot::Sender<Result<crate::queue::QueueOut, String>>,
    },
    /// One gather's worth of absorber confirmations, carried as a SINGLE
    /// committer message so every covered boundary lands in the same
    /// write batch deterministically (the per-stream sends only
    /// coalesced opportunistically — the committer could run between
    /// them). Each entry is (hash, new upto, frame bytes the absorber
    /// copied for that stream — decremented from the tail's
    /// unabsorbed_bytes gauge). Expanded into per-stream `Absorbed` ops
    /// at commit_group entry.
    AbsorbedBatch {
        streams: Vec<([u8; 16], u64, u64)>,
        v2: bool,
    },
    /// Trim maintenance pulse (flush ticker, whenever the trim-debt set
    /// is non-empty): round-robins streams with `trimmed <
    /// trim_safe_to` and emits record deletes under the commit group's
    /// GLOBAL trim budget. This is where the bulk of physical trimming
    /// happens — the `Absorbed` arm only advances boundaries and takes
    /// whatever budget is left over.
    TrimTick,
    /// Absorber confirmation: history tier now durably holds [.., upto).
    /// Advances the readers' boundary and trims previously-absorbed records
    /// (deferred one round so in-flight readers never lose their range).
    /// `v2` marks the range as living in the SHARED per-shard partition
    /// (docs/HISTORY-V2.md); the first advancing v2 op sets the stream's
    /// history_v2 flag, which gates the read path's history source. The
    /// v2 absorber flushes MANY streams once and then submits one of
    /// these per covered stream; they coalesce into the same committer
    /// batch, so the boundaries land in one tracker write-batch.
    Absorbed {
        hash: [u8; 16],
        upto: u64,
        /// Stored frame bytes the absorber copied for this advance.
        bytes: u64,
        v2: bool,
    },
    /// TrimTick expansion product (commit_group entry): one stream's
    /// budgeted trim step. Never sent over the channel directly.
    TrimStep {
        hash: [u8; 16],
    },
}

/// Notification to the absorber that a stream accumulated shard-log bytes.
#[derive(Debug, Clone)]
pub struct AbsorbSignal {
    pub hash: [u8; 16],
    pub appended_bytes: u64,
}

#[derive(Clone)]
pub struct ShardConfig {
    pub queue_reqs: usize,
    pub max_batch_reqs: usize,
    pub max_batch_bytes: usize,
    pub max_trim_per_op: u64,
    /// Commit pacing: once a drained group has at least this many requests
    /// (i.e. the stream is BUSY), keep gathering until `gather_window` so
    /// one flush cycle ships one big WAL SST instead of many tiny ones.
    pub pace_min_reqs: usize,
    pub gather_window: std::time::Duration,
    /// Group-commit WAL flushing: instead of waiting for SlateDB's fixed
    /// flush tick, a pump task flushes the WAL the moment the previous
    /// flush completes if commits are waiting. Under load the cadence
    /// self-clocks to the WAL PUT RTT (the in-flight PUT is the batching
    /// window); `wal_flush_gap` only bounds the SST mint rate when the
    /// PUT RTT is shorter than the gap — the object-churn ceiling stays
    /// exactly where the old tick put it.
    pub wal_group_commit: bool,
    pub wal_flush_gap: std::time::Duration,
    /// Post-ACK gather window. After a busy flush completes, the pump
    /// itself releases the acknowledgements for everything that flush made
    /// durable (an explicit barrier — not a scheduler race with the
    /// acker), then waits this long before freezing the next WAL. The
    /// point: closed-loop producers' next requests, issued in reaction to
    /// those acks, arrive DURING the window and join the next WAL instead
    /// of missing its freeze by a millisecond and waiting a full extra
    /// PUT behind it. Zero disables the barrier and the window (acks
    /// release only via the acker; the next freeze races the ack herd —
    /// measured to cost c2 ≈ 2×c1 append p50). Never delays an idle
    /// shard's first write: the pump only gathers after a flush that
    /// dispatched work.
    pub wal_post_ack_gather: std::time::Duration,
    /// Skip the gather when the NEXT WAL is already busy: a window only
    /// pays off when the coming generation is small (it exists to let an
    /// ack-triggered herd join); at saturation it is a pure latency and
    /// throughput tax. Thresholds are checked after ack dispatch, against
    /// what is already committed-but-unflushed.
    pub wal_gather_skip_reqs: u32,
    pub wal_gather_skip_bytes: u64,
    /// Durable-tail ring budget in bytes for THIS engine (0 = off). When
    /// on, dispatch_durable publishes each group's freshly-durable frames
    /// into a per-stream in-memory ring before releasing acks, and live
    /// tail reads are served from that ring instead of a SlateDB scan.
    /// The canonical read path (scan -> history) remains the source of
    /// truth for anything the ring no longer covers: restart, eviction,
    /// lagging consumers.
    pub tail_ring_bytes: usize,
    /// Evict resident stream handles idle at least this long (and
    /// referenced by nothing but the map). Zero disables. The durable
    /// dirty-stream index keeps unabsorbed evictees discoverable, so
    /// eviction never strands a tail.
    pub handle_idle_evict: std::time::Duration,
    /// Capacity cap on resident stream handles (0 disables): when the
    /// map exceeds this, the ticker evicts oldest-touched unreferenced
    /// handles down to the cap WITHOUT waiting for the idle threshold —
    /// a cardinality burst can otherwise accumulate rate × idle-window
    /// handles before the first one ages out. Referenced handles
    /// (strong_count > 1) are never evicted, so the map can exceed the
    /// cap by the number of streams actively in use.
    pub handle_max_resident: usize,
    /// GLOBAL cap on record-trim deletes per commit group, shared by
    /// every `Absorbed` advance and `TrimStep` in the group. Without it
    /// one gather's AbsorbedBatch over 1,024 mature streams ×
    /// max_trim_per_op could expand into a multi-gigabyte WriteBatch
    /// (67M deletes at the wide posture's TRIM_PER_OP=65536).
    /// `max_trim_per_op` remains the per-stream bound within a group.
    pub trim_global_budget: u64,
    /// Decoded postings-slice cache budget (spec §7.1), bytes.
    pub postings_cache_bytes: usize,
    /// Process-shared postings cache (review finding 7: ONE budget for
    /// the whole process, not one per engine — 32 engines x 16 MiB was
    /// a nominal 512 MiB). main.rs passes the global; tests pass None
    /// for hermetic per-engine caches (counters stay isolated).
    pub shared_postings_cache: Option<Arc<crate::postings_cache::PostingsCache>>,
    pub shared_history: Option<Arc<crate::history::HistoryResources>>,
    pub shared_usage: Option<Arc<crate::usage::UsageService>>,
    pub shared_ops: Option<Arc<crate::ops::OpsService>>,
    /// Writer-side frame compression policy for this engine (explicit,
    /// selected at engine construction — the codec does no ambient
    /// lookup). Readers accept both frame versions unconditionally.
    pub frame_compression: crate::crypto::FrameCompression,
    /// History/absorber knobs for the shard's shared history v2
    /// partition (from the process ServerConfig at construction).
    pub history: crate::config::HistoryConfig,
    /// The ONE process-wide compaction profile every SlateDB in this
    /// process opens with (R27-4/R28), resolved at engine construction.
    pub compactor_options: slatedb::config::CompactorOptions,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            queue_reqs: 65_536,
            max_batch_reqs: 16_384,
            max_batch_bytes: 48 * 1024 * 1024,
            max_trim_per_op: 8_192,
            pace_min_reqs: 32,
            gather_window: std::time::Duration::from_millis(15),
            wal_group_commit: false,
            wal_flush_gap: std::time::Duration::from_millis(25),
            wal_post_ack_gather: std::time::Duration::ZERO,
            handle_idle_evict: std::time::Duration::from_secs(600),
            handle_max_resident: 65_536,
            trim_global_budget: 65_536,
            postings_cache_bytes: crate::postings_cache::POSTINGS_CACHE_BYTES,
            shared_postings_cache: None,
            shared_history: None,
            shared_usage: None,
            shared_ops: None,
            wal_gather_skip_reqs: 32,
            wal_gather_skip_bytes: 1024 * 1024,
            tail_ring_bytes: 0,
            frame_compression: crate::crypto::FrameCompression::Disabled,
            history: crate::config::HistoryConfig::default(),
            compactor_options: crate::config::EngineConfig::default().compactor_options(),
        }
    }
}

/// Per-commit-group pipeline decomposition: where a request's time goes
/// between arriving at the committer and being durably acked.
#[derive(Clone, Copy, Debug)]
pub struct GroupTiming {
    pub ts_ms: i64,
    /// Oldest request's wait in the committer queue before this group.
    pub queue_wait_us: u32,
    /// Serial committer work: producer checks, encryption, WriteBatch build.
    pub encode_us: u32,
    /// db.write (memtable apply; blocks under byte backpressure).
    pub write_us: u32,
    /// Wait for the durable watermark (WAL flush + PUT).
    pub durable_wait_us: u32,
    pub reqs: u32,
    pub records: u32,
    pub bytes: u64,
}

struct InFlightGroup {
    seq: u64,
    /// Commit-pipeline instrumentation: when db.write returned.
    written_at: std::time::Instant,
    queue_wait_us: u32,
    encode_us: u32,
    /// How long db.write itself took (µs).
    write_us: u32,
    reqs: u32,
    records_n: u32,
    bytes: u64,
    effects: DurableEffects,
}

pub struct ShardEngine {
    pub prefix: String,
    pub db: Arc<Db>,
    /// R29 custody model. `last_external_seq`: the global adoption
    /// sequence value of the most recent EXTERNAL resolution of this
    /// engine (customer request paths only — never the sweep, walk or
    /// scaler). `sweep_custody`: 0 = not scheduler-held, otherwise the
    /// adoption-sequence value at which the sweep installed custody.
    /// Invariants enforced in billing.rs: custody installs only onto an
    /// engine with last_external_seq == 0 (any earlier external use —
    /// including a customer who coalesced into the sweep's own open —
    /// declines custody), an external resolution atomically revokes
    /// custody, and a close requires the installer's exact custody
    /// value with no newer external stamp.
    pub last_external_seq: std::sync::atomic::AtomicU64,
    pub sweep_custody: std::sync::atomic::AtomicU64,
    /// Engine-owned maintenance state (R25-A). The durable row in this
    /// shard's DB is authoritative; this is the published mirror,
    /// updated ONLY after the write carrying the row succeeds. Owned by
    /// the engine — not a process-global map — so restore-before-serve,
    /// ownership handoff, and old-owner-task cleanup are all scoped to
    /// the engine lifecycle automatically.
    maintenance: std::sync::RwLock<ShardMaintenance>,
    /// Per-shard backpressure latch (hysteresis lives with the shard).
    pub maintenance_shard_shed: std::sync::atomic::AtomicBool,
    /// Object store the shard's DBs live on — held so the engine can
    /// lazily open its shared history v2 partition.
    data_store: Arc<dyn object_store::ObjectStore>,
    /// Shared history v2 partition (docs/HISTORY-V2.md): ONE writer Db
    /// per shard at `{prefix}/history2`, opened lazily by whoever needs
    /// it first (absorber gather lane or a v2 history read) and shared —
    /// two independent opens would fence each other. Closed with the
    /// engine; a new shard owner's open fences this one at the slatedb
    /// layer, same dynamics as the per-stream v1 DBs.
    history2: Arc<history_partition::HistoryPartition>,
    /// Pre-built SlateDB settings for `history2` (history knobs + the
    /// ONE process-wide compactor profile, from the engine's ShardConfig).
    history2_settings: slatedb::config::Settings,
    streams: Mutex<HashMap<[u8; 16], Arc<StreamHandle>>>,
    /// Seal fences by segment identity — ENGINE-level, deliberately
    /// outside the evictable [`StreamHandle`], and deliberately WITHOUT
    /// any expiry: an AppendReq has no maximum queue residence (a
    /// timed-out HTTP handler drops only its receiver, and backpressure
    /// can hold the queue arbitrarily long), so no wall-clock bound on
    /// a fence is a proof about the request it exists to stop. One u64
    /// per ever-fenced segment, for the engine's lifetime, is the
    /// price of that proof; the map dies with the queue it protects. an AppendReq waiting in
    /// the committer channel holds only the stream hash, so a handle
    /// can be idle-evicted (or displaced by the resident cap) while a
    /// stale claim-authorized write is still queued, and a fence that
    /// lived in the handle would be reborn as zero when the committer
    /// reloaded it. This map dies with the engine and its queue —
    /// which is the exact lifetime the fence protects.
    seal_fences: Mutex<HashMap<[u8; 16], u64>>,
    tx: mpsc::Sender<CommitOp>,
    in_flight: Mutex<CommitHandoff>,
    /// Serializes durable-dispatch between the pump (post-flush barrier)
    /// and the acker (failsafe + fencing path). Group drains are already
    /// exclusive via the in_flight lock; this additionally keeps tail
    /// state updates applying in seq order across the two callers.
    dispatch_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    // tokio (not std) DELIBERATELY: both gates are held across awaits
    // by tests, and their non-test acquirers run as tasks on the shared
    // runtime. A std lock() there blocks the WORKER THREAD; the #115
    // hunt caught that in the act — the blocked worker stranded the
    // timer driver, the gate-holding test's own sleep/watchdog timers
    // died, and the release never came (deadlock, all threads parked).
    // An async lock parks the TASK and the runtime keeps breathing.
    commit_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    completion_pause: Mutex<Option<Arc<retirement_tests::CompletionPause>>>,
    #[cfg(test)]
    appends_enqueued: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fail_group_for: Mutex<Option<std::collections::HashSet<[u8; 16]>>>,
    #[cfg(test)]
    fail_config_scan: std::sync::atomic::AtomicBool,
    /// Consumer-generation fences (round 16): (identity, consumer) ->
    /// first LIVE generation. Installed by the deletion saga's segment
    /// op BEFORE its cleanup stages, so a Receive/Settle for a dead
    /// generation that is still in this committer's queue can never
    /// re-stage rows the cleanup ran too early to see. Engine-resident
    /// like the seal fences: it only has to outlive the queue that
    /// could contain stale ops; durably, dead generations are already
    /// harmless because generations live in the row keys.
    consumer_fences: Mutex<HashMap<([u8; 16], String), u64>>,
    #[cfg(test)]
    fail_group_tripped: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fail_next_absorbed_group: std::sync::atomic::AtomicBool,
    /// Pump telemetry: flushes issued, requests acked at the pump's own
    /// barrier, gather windows taken. acked/flushes is the requests-per-
    /// WAL figure the flush-scheduling change is judged by.
    pub pump_flushes: AtomicU64,
    pub pump_barrier_acked: AtomicU64,
    pub pump_gathers: AtomicU64,
    /// Windows skipped because the next generation was already busy
    /// (adaptive gather), and requests observed to arrive DURING applied
    /// windows (the herd the window exists to catch).
    pub pump_gathers_skipped_busy: AtomicU64,
    pub pump_gathered_reqs: AtomicU64,
    /// Per-flush ledger: what each pump flush actually shipped.
    /// requests_per_wal = flushed_reqs / flushes, delta'd by the harness.
    pub pump_flushed_reqs: AtomicU64,
    pub pump_flushed_records: AtomicU64,
    pub pump_flushed_bytes: AtomicU64,
    /// Ack-to-next-enqueue: µs from an ack dispatch to the FIRST client
    /// request that follows it — direct evidence the ack-triggered herd
    /// arrives within the gather window (sum/count; armed at dispatch).
    pub ack_to_enqueue_sum_us: AtomicU64,
    pub ack_to_enqueue_count: AtomicU64,
    ack_armed_at_us: AtomicU64,
    /// Monotonic epoch for cheap µs stamps.
    epoch: std::time::Instant,
    /// Durable-tail ring accounting. `ring_budget` is the remaining global
    /// byte allowance (config minus resident bytes; goes negative
    /// transiently during a publish, restored by eviction). `ring_fifo`
    /// mirrors publish order engine-wide: one entry per published batch,
    /// so popping its front always evicts the globally oldest batch.
    ring_enabled: bool,
    ring_budget: std::sync::atomic::AtomicI64,
    ring_fifo: Mutex<std::collections::VecDeque<Arc<StreamHandle>>>,
    pub ring_published: AtomicU64,
    pub ring_hits: AtomicU64,
    pub ring_misses: AtomicU64,
    /// Miss causes. below_floor = reader lagging behind eviction;
    /// above_ceil = reader knows an end the ring has not been handed yet
    /// (mid-dispatch); one miss can set both. empty = stream has no ring
    /// (never published, or fully evicted). With the ring enabled,
    /// hits + misses = ring_read attempts.
    pub ring_miss_below_floor: AtomicU64,
    pub ring_miss_above_ceil: AtomicU64,
    pub ring_miss_empty: AtomicU64,
    pub ring_evicted: AtomicU64,
    /// Resident bytes high-water mark; current residency is
    /// (config budget - ring_budget), exposed alongside it.
    pub ring_peak_bytes: AtomicU64,
    ring_cfg_bytes: u64,
    /// Idle threshold for resident-handle eviction (ShardConfig copy).
    handle_idle_evict: std::time::Duration,
    /// Capacity cap for resident handles (ShardConfig copy; 0 = off).
    handle_max_resident: usize,
    /// Streams with `trimmed < trim_safe_to`: physical-trim work the
    /// budgeted TrimTick maintenance still owes. Maintained by the
    /// committer after each successful group; seeded from the durable
    /// dirty index at absorber start and from tail loads. BTreeSet so
    /// the round-robin cursor is a plain range scan.
    trim_debt: Mutex<std::collections::BTreeSet<[u8; 16]>>,
    /// Round-robin position for TrimTick expansion.
    trim_cursor: Mutex<[u8; 16]>,
    /// Trim telemetry: deletes emitted in the last commit group that
    /// trimmed anything, the max ever emitted in one group (the bound
    /// the mature-second-wave gate reads), and a cumulative total.
    pub trim_deletes_last: AtomicU64,
    pub trim_deletes_max_batch: AtomicU64,
    pub trim_deletes_total: AtomicU64,
    /// Advances rejected by the layout seal (cross-lane absorb after the
    /// stream's history layout was decided). Nonzero means the absorber
    /// raced its own lane classification — harmless with the seal, but
    /// worth seeing.
    pub absorb_lane_dropped: AtomicU64,
    /// Decoded postings-slice cache (spec §7): keyed historical reads
    /// pay the index once per active window.
    pub postings_cache: Arc<crate::postings_cache::PostingsCache>,
    pub history_resources: Arc<crate::history::HistoryResources>,
    pub usage: Arc<crate::usage::UsageService>,
    pub ops: Arc<crate::ops::OpsService>,
    /// Level-triggered close signal for background tasks (see start()).
    close_tx: tokio::sync::watch::Sender<bool>,
    /// Handles for every task this engine spawned, so termination is a
    /// provable fact (`await_terminated`) instead of an assumption.
    tasks: lifecycle::EngineTasks,
    flush_wake: Notify,
    /// Group-commit pump wake: one permit means "commits landed since the
    /// pump last looked". Distinct from flush_wake, whose permit the acker
    /// loop consumes.
    pump_wake: Notify,
    absorb_tx: mpsc::Sender<AbsorbSignal>,
    /// Invoked when the shard db closes (fenced by a new owner / fatal):
    /// wired to TouchRegistry::close_shard so hanging /touch/wait clients
    /// get stale immediately instead of dangling until timeout.
    on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Set once when the shard db reports closed (fenced by a new owner or
    /// fatal). Everything still holding this engine — request handlers, the
    /// committer, the absorber — must fail fast / exit instead of retrying
    /// against a dead db (the "zombie engine" fuel of the absorption war).
    closed: std::sync::atomic::AtomicBool,
    /// Wall-clock ms when the current db.write began, 0 when idle. A
    /// nonzero value that stays old means the commit pipeline is BLOCKED
    /// (L0-full / unflushed-full while compaction lags) — admission must
    /// shed instead of letting appends hang into the front door's 30 s
    /// kill (the 2026-07-21 8-minute wedge).
    commit_write_started_ms: std::sync::atomic::AtomicI64,
    pub stats_appended: AtomicU64,
    /// Last commit-group timings for /v1/debug/timings.
    pub timings: Mutex<std::collections::VecDeque<GroupTiming>>,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Test-only fault injection for the durable dirty-index scan, keyed by
/// shard prefix so concurrent tests cannot poison each other. The
/// object-store fault substrate cannot reach this path deterministically
/// (SlateDB retries store faults internally), and the absorber's
/// scan-retry loop is exactly the code under test.
#[cfg(test)]
fn dirty_scan_faults() -> &'static Mutex<HashMap<String, u32>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u32>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arrange for the next `n` dirty-index scans on `prefix` to fail.
#[cfg(test)]
pub(crate) fn inject_dirty_scan_faults(prefix: &str, n: u32) {
    dirty_scan_faults()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), n);
}

impl ShardEngine {
    pub fn start(
        prefix: String,
        db: Arc<Db>,
        data_store: Arc<dyn object_store::ObjectStore>,
        cfg: ShardConfig,
        absorb_tx: mpsc::Sender<AbsorbSignal>,
        on_close: Option<Arc<dyn Fn() + Send + Sync>>,
        initial_maintenance: ShardMaintenance,
    ) -> Arc<ShardEngine> {
        let (tx, rx) = mpsc::channel(cfg.queue_reqs);
        // Level-triggered close signal for the background tasks. The
        // committer cannot rely on its channel closing: the engine itself
        // holds a sender, and the committer holds the engine — a retain
        // cycle that used to leave one committer task (and the whole
        // engine allocation) resident per shard move, forever.
        let (close_tx, _) = tokio::sync::watch::channel(false);
        // Pre-built settings for the shard's shared history v2 partition
        // (history knobs + the ONE process-wide compactor profile,
        // resolved from the process configuration at engine construction).
        let history2_settings =
            crate::history::history2_settings(&cfg.history, &cfg.compactor_options);
        let engine = Arc::new(ShardEngine {
            prefix,
            db,
            last_external_seq: std::sync::atomic::AtomicU64::new(0),
            sweep_custody: std::sync::atomic::AtomicU64::new(0),
            maintenance: std::sync::RwLock::new(initial_maintenance),
            maintenance_shard_shed: std::sync::atomic::AtomicBool::new(false),
            data_store,
            history2: Arc::new(history_partition::HistoryPartition::default()),
            history2_settings,
            streams: Mutex::new(HashMap::new()),
            seal_fences: Mutex::new(HashMap::new()),
            #[cfg(test)]
            commit_gate: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            completion_pause: Mutex::new(None),
            #[cfg(test)]
            appends_enqueued: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            fail_group_for: Mutex::new(None),
            #[cfg(test)]
            fail_config_scan: std::sync::atomic::AtomicBool::new(false),
            consumer_fences: Mutex::new(HashMap::new()),
            #[cfg(test)]
            fail_group_tripped: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fail_next_absorbed_group: std::sync::atomic::AtomicBool::new(false),
            tx,
            in_flight: Mutex::new(CommitHandoff::default()),
            dispatch_gate: tokio::sync::Mutex::new(()),
            pump_flushes: AtomicU64::new(0),
            pump_barrier_acked: AtomicU64::new(0),
            pump_gathers: AtomicU64::new(0),
            pump_gathers_skipped_busy: AtomicU64::new(0),
            pump_gathered_reqs: AtomicU64::new(0),
            pump_flushed_reqs: AtomicU64::new(0),
            pump_flushed_records: AtomicU64::new(0),
            pump_flushed_bytes: AtomicU64::new(0),
            ack_to_enqueue_sum_us: AtomicU64::new(0),
            ack_to_enqueue_count: AtomicU64::new(0),
            ack_armed_at_us: AtomicU64::new(0),
            epoch: std::time::Instant::now(),
            ring_enabled: cfg.tail_ring_bytes > 0,
            ring_budget: std::sync::atomic::AtomicI64::new(cfg.tail_ring_bytes as i64),
            ring_fifo: Mutex::new(std::collections::VecDeque::new()),
            ring_published: AtomicU64::new(0),
            ring_hits: AtomicU64::new(0),
            ring_misses: AtomicU64::new(0),
            ring_miss_below_floor: AtomicU64::new(0),
            ring_miss_above_ceil: AtomicU64::new(0),
            ring_miss_empty: AtomicU64::new(0),
            ring_evicted: AtomicU64::new(0),
            ring_peak_bytes: AtomicU64::new(0),
            ring_cfg_bytes: cfg.tail_ring_bytes as u64,
            handle_idle_evict: cfg.handle_idle_evict,
            handle_max_resident: cfg.handle_max_resident,
            trim_debt: Mutex::new(std::collections::BTreeSet::new()),
            trim_cursor: Mutex::new([0u8; 16]),
            trim_deletes_last: AtomicU64::new(0),
            trim_deletes_max_batch: AtomicU64::new(0),
            trim_deletes_total: AtomicU64::new(0),
            absorb_lane_dropped: AtomicU64::new(0),
            usage: cfg.shared_usage.clone().unwrap_or_else(|| {
                Arc::new(crate::usage::UsageService::new(
                    &crate::config::AdmissionConfig::default(),
                    Arc::new(crate::runtime::SystemClock::default()),
                ))
            }),
            ops: cfg
                .shared_ops
                .clone()
                .unwrap_or_else(|| Arc::new(crate::ops::OpsService::new())),
            history_resources: cfg.shared_history.clone().unwrap_or_else(|| {
                Arc::new(crate::history::HistoryResources::new(
                    &cfg.history,
                    usize::MAX,
                ))
            }),
            postings_cache: cfg.shared_postings_cache.clone().unwrap_or_else(|| {
                crate::postings_cache::PostingsCache::new(cfg.postings_cache_bytes)
            }),
            flush_wake: Notify::new(),
            pump_wake: Notify::new(),
            absorb_tx,
            on_close,
            closed: std::sync::atomic::AtomicBool::new(false),
            close_tx,
            tasks: lifecycle::EngineTasks::default(),
            commit_write_started_ms: std::sync::atomic::AtomicI64::new(0),
            stats_appended: AtomicU64::new(0),
            timings: Mutex::new(std::collections::VecDeque::new()),
        });
        // Group-commit flush pump: waits for a committed group, flushes the
        // WAL, and immediately flushes again if more groups arrived while
        // the PUT was in flight — the ack path stops paying the tick
        // alignment (avg tick/2) on top of the serial-PUT queue. The gap
        // check runs start-to-start, so when the PUT RTT exceeds the gap
        // (the normal Tigris case) it adds zero wait, and when the store is
        // faster than the gap it enforces the same max SST mint rate as the
        // old tick. SlateDB's own flush_interval stays on as a long
        // failsafe (shard_settings stretches it when the pump is enabled).
        if cfg.wal_group_commit {
            let pump = engine.clone();
            let gap = cfg.wal_flush_gap;
            let gather = cfg.wal_post_ack_gather;
            let skip_reqs = cfg.wal_gather_skip_reqs;
            let skip_bytes = cfg.wal_gather_skip_bytes;
            tracing::info!(
                shard = %pump.prefix,
                gap_ms = gap.as_millis() as u64,
                gather_ms = gather.as_millis() as u64,
                "WAL group-commit pump on"
            );
            engine.spawn_required("pump", async move {
                use slatedb::config::{FlushOptions, FlushType};
                let mut status_rx = pump.db.subscribe();
                let mut last_start: Option<std::time::Instant> = None;
                loop {
                    pump.pump_wake.notified().await;
                    if pump.is_closed() {
                        return;
                    }
                    // Only flush when a commit is actually awaiting
                    // durability. Without this, an ack-triggered client
                    // herd arrives just after a speculative empty flush
                    // froze the buffer and waits a full extra PUT behind
                    // it (closed-loop A/B measured 52 ms vs 29 ms
                    // durable_wait on identical load).
                    if pump.in_flight.lock().unwrap().pending().is_empty() {
                        continue;
                    }
                    if let Some(t0) = last_start {
                        let since = t0.elapsed();
                        if since < gap {
                            tokio::time::sleep(gap - since).await;
                            if pump.is_closed() {
                                return;
                            }
                        }
                    }
                    // Herd-settle: a synced herd's requests arrive within
                    // microseconds of each other, and the wake->freeze
                    // path is tight enough to split them into two WALs.
                    // 1 ms is far above their spread and far below the
                    // PUT RTT, so a solo producer pays ~1 ms and a herd
                    // stays one WAL.
                    if !gather.is_zero() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        if pump.is_closed() {
                            return;
                        }
                    }
                    last_start = Some(std::time::Instant::now());
                    // The seqs this flush is about to make durable: every
                    // group written before the freeze. Captured BEFORE the
                    // flush call, because the barrier below must wait for
                    // the watermark to cover them — `flush()` resolves
                    // before the status watch publishes the new durable
                    // seq (measured: at c2 the post-flush borrow saw a
                    // stale watermark on 1612 of 1626 flushes, silently
                    // reducing the barrier to the old acker race).
                    // Per-flush ledger + barrier target, captured together
                    // BEFORE the flush (capturing after could include — and
                    // wait on — the NEXT generation: deadlock shape).
                    let (target_seq, fl_reqs, fl_records, fl_bytes) = {
                        let q = pump.in_flight.lock().unwrap();
                        let q = q.pending();
                        (
                            q.last().map(|g| g.seq),
                            q.iter().map(|g| g.reqs as u64).sum::<u64>(),
                            q.iter().map(|g| g.records_n as u64).sum::<u64>(),
                            q.iter().map(|g| g.bytes).sum::<u64>(),
                        )
                    };
                    match pump
                        .db
                        .flush_with_options(FlushOptions {
                            flush_type: FlushType::Wal,
                        })
                        .await
                    {
                        Err(e) => {
                            if pump.is_closed() {
                                return;
                            }
                            tracing::warn!(shard = %pump.prefix, "group-commit WAL flush failed: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        Ok(()) => {
                            pump.pump_flushes.fetch_add(1, Ordering::Relaxed);
                            pump.pump_flushed_reqs.fetch_add(fl_reqs, Ordering::Relaxed);
                            pump.pump_flushed_records
                                .fetch_add(fl_records, Ordering::Relaxed);
                            pump.pump_flushed_bytes
                                .fetch_add(fl_bytes, Ordering::Relaxed);
                            if gather.is_zero() {
                                continue;
                            }
                            let Some(target) = target_seq else {
                                continue;
                            };
                            // Explicit barrier: wait for the durable
                            // watermark to actually cover what we froze,
                            // then release those acks HERE, synchronously.
                            // When dispatch_durable returns, every response
                            // this flush unblocked is on its way to a
                            // socket. The 250 ms ceiling is a failsafe
                            // (fencing, store stall): the acker still owns
                            // dispatch if we bail.
                            // The watch Ref is !Send, so copy out of it
                            // inside this block — nothing Ref-typed may
                            // survive to the gather sleep below.
                            // Ok(seq) = dispatch; Err(true) = watch gone
                            // (db closed, exit); Err(false) = skip
                            // (fenced, or failsafe timeout: the acker
                            // still owns dispatch).
                            let seen: Result<u64, bool> = {
                                match tokio::time::timeout(
                                    std::time::Duration::from_millis(250),
                                    status_rx.wait_for(|s| {
                                        s.durable_seq >= target || s.close_reason.is_some()
                                    }),
                                )
                                .await
                                {
                                    Ok(Ok(sref)) if sref.close_reason.is_some() => Err(false),
                                    Ok(Ok(sref)) => Ok(sref.durable_seq),
                                    Ok(Err(_)) => Err(true),
                                    Err(_) => Err(false),
                                }
                            };
                            let durable_seq = match seen {
                                Ok(seq) => seq,
                                Err(true) => return,
                                Err(false) => continue,
                            };
                            // Drain whatever the acker has not already
                            // taken. Who wins that race is irrelevant to
                            // the barrier: the acker fires on the same
                            // watch change, and this call blocks on the
                            // dispatch_gate until any concurrent acker
                            // dispatch has finished sending. Either way,
                            // when this returns, every ack this flush
                            // unblocked is on the wire.
                            let acked = pump.dispatch_durable(durable_seq).await;
                            pump.pump_barrier_acked
                                .fetch_add(acked as u64, Ordering::Relaxed);
                            // Arm the ack->next-enqueue probe: the next
                            // try_enqueue stamps the herd's reaction time.
                            pump.ack_armed_at_us.store(
                                pump.epoch.elapsed().as_micros().max(1) as u64,
                                Ordering::Relaxed,
                            );
                            // Gather ONLY when this completion proves
                            // concurrency: someone is already waiting in
                            // in_flight (they arrived mid-PUT — the herd
                            // has drifted across two WAL generations).
                            // The window lets the just-acked clients'
                            // follow-ups land in the same WAL as the
                            // waiter, re-syncing the herd. When nobody is
                            // waiting the shard is solo or the herd is in
                            // sync — either way a window would tax c1 by
                            // its full length for nothing (measured:
                            // +7 ms on c1 p50). NOT keyed on `acked > 0`:
                            // the acker fires on the same watch change
                            // and wins the dispatch race on >99 % of
                            // flushes; keying on it skipped the window on
                            // 1636 of 1651 c2 flushes.
                            // Adaptive: gather only when (a) the herd has
                            // drifted (someone already waits) AND (b) the
                            // next generation is still SMALL — a window in
                            // front of an already-big WAL is a pure tax
                            // (review #2's throughput concern at CDG's top
                            // tiers).
                            let (drifted, pend_reqs, pend_bytes) = {
                                let q = pump.in_flight.lock().unwrap();
                        let q = q.pending();
                                (
                                    !q.is_empty(),
                                    q.iter().map(|g| g.reqs).sum::<u32>(),
                                    q.iter().map(|g| g.bytes).sum::<u64>(),
                                )
                            };
                            if drifted && (pend_reqs >= skip_reqs || pend_bytes >= skip_bytes) {
                                pump.pump_gathers_skipped_busy
                                    .fetch_add(1, Ordering::Relaxed);
                            } else if drifted {
                                pump.pump_gathers.fetch_add(1, Ordering::Relaxed);
                                tokio::time::sleep(gather).await;
                                if pump.is_closed() {
                                    return;
                                }
                                // What the window caught: requests present
                                // now that were not pending when it opened.
                                let after: u32 =
                                    pump.in_flight.lock().unwrap().pending().iter().map(|g| g.reqs).sum();
                                pump.pump_gathered_reqs.fetch_add(
                                    after.saturating_sub(pend_reqs) as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            });
        }
        let committer = engine.clone();
        engine.spawn_required("committer", async move {
            committer.committer_loop(rx, cfg).await
        });
        let acker = engine.clone();
        engine.spawn_required("acker", async move { acker.acker_loop().await });
        // F1 recovery bound: `max_wal_flushes_before_l0_flush` has a 4096
        // upstream floor, so we cap the WAL replay window ourselves with a
        // periodic explicit memtable->L0 flush whenever data accumulated.
        let ticker = engine.clone();
        let mut ticker_closed = engine.close_tx.subscribe();
        engine.spawn_required("flush-ticker", async move {
            use slatedb::config::{FlushOptions, FlushType};
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut last_appended = 0u64;
            loop {
                tokio::select! {
                    _ = ticker_closed.changed() => {
                        if *ticker_closed.borrow() {
                            return;
                        }
                    }
                    _ = interval.tick() => {}
                }
                if ticker.is_closed() {
                    return;
                }
                let appended = ticker.stats_appended.load(Ordering::Relaxed);
                if appended != last_appended {
                    last_appended = appended;
                    if let Err(e) = ticker
                        .db
                        .flush_with_options(FlushOptions {
                            flush_type: FlushType::MemTable,
                        })
                        .await
                    {
                        tracing::warn!(shard = %ticker.prefix, "memtable flush tick failed: {e}");
                    }
                }
                let idle = ticker.handle_idle_evict;
                if !idle.is_zero() || ticker.handle_max_resident > 0 {
                    let evicted = ticker.evict_idle_handles(idle, ticker.handle_max_resident);
                    if evicted > 0 {
                        tracing::debug!(
                            shard = %ticker.prefix,
                            "evicted {evicted} idle stream handles"
                        );
                    }
                }
                ticker
                    .postings_cache
                    .sweep_idle(crate::postings_cache::POSTINGS_CACHE_IDLE);
                // Trim maintenance pulse: whenever streams owe
                // physical trims, queue one budgeted TrimTick.
                // try_send — a full committer queue means the next
                // tick retries; trim work is never urgent enough to
                // block behind.
                if !ticker.trim_debt.lock().unwrap().is_empty() {
                    let _ = ticker.tx.try_send(CommitOp::TrimTick);
                }
            }
        });
        engine
    }

    pub(crate) fn spawn_required(
        self: &Arc<Self>,
        role: &'static str,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.tasks.required(self, role, future);
    }

    pub(crate) fn required_task_failure(&self) -> Option<&'static str> {
        self.tasks.failure()
    }
    pub(crate) fn shutdown_handle(&self) -> EngineShutdown {
        self.tasks.handle()
    }
    #[cfg(test)]
    pub(crate) fn termination_complete(&self) -> bool {
        self.shutdown_handle().terminated()
    }

    /// Level-triggered notification also covers subscription after close.
    pub(crate) async fn closed(&self) {
        let mut closed = self.close_tx.subscribe();
        while !self.is_closed() {
            if closed.changed().await.is_err() {
                return;
            }
        }
    }

    /// Observe the engine's one owned shutdown. Timeout/cancellation only
    /// stops this observer; workers and storage closure retain their owner.
    pub async fn await_terminated(&self, timeout: std::time::Duration) -> Result<(), String> {
        self.begin_close();
        self.shutdown_handle().wait(timeout).await
    }

    /// Worker reservations may be released while a native store close is
    /// still blocked. This narrower milestone never reports full termination.
    #[cfg(test)]
    pub(crate) async fn await_workers(&self, timeout: std::time::Duration) -> Result<(), String> {
        self.begin_close();
        self.tasks.workers(timeout).await
    }

    pub fn try_enqueue(&self, req: AppendReq) -> Result<(), EnqueueError> {
        // Control-only closes enter the actor as a different command. Data
        // appends retain final-record completion atomically with their data.
        let command = if req.finish == AppendFinish::Close
            && req.entries.is_empty()
            && req.producer.is_none()
            && req.seq.is_none()
            && req.deferred_error.is_none()
            && req.sealed_reject_new.is_none()
        {
            CommitOp::Close(CloseReq {
                hash: req.hash,
                generation: req.seal_gen,
                resp: req.resp,
            })
        } else {
            CommitOp::Append(req)
        };
        self.try_command(command)
    }

    pub fn try_close(&self, req: CloseReq) -> Result<(), EnqueueError> {
        self.try_command(CommitOp::Close(req))
    }

    pub fn try_seal_fence(&self, req: SealFenceReq) -> Result<(), EnqueueError> {
        self.try_command(CommitOp::SealFence(req))
    }

    fn try_command(&self, command: CommitOp) -> Result<(), EnqueueError> {
        if self.is_closed() {
            return Err(EnqueueError::Closed);
        }
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => EnqueueError::Full,
            mpsc::error::TrySendError::Closed(_) => EnqueueError::Closed,
        })?;
        #[cfg(test)]
        self.appends_enqueued.fetch_add(1, Ordering::SeqCst);
        let armed = self.ack_armed_at_us.swap(0, Ordering::Relaxed);
        if armed != 0 {
            let now = self.epoch.elapsed().as_micros() as u64;
            self.ack_to_enqueue_sum_us
                .fetch_add(now.saturating_sub(armed), Ordering::Relaxed);
            self.ack_to_enqueue_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// True once the shard db reported closed (fenced by a new owner or a
    /// fatal storage error). Holders must stop using this engine.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Proactive close (rebalancer moved this shard away): mark closed,
    /// wake the pump, fail everything in flight NOW. Without this,
    /// requests already queued here hang until the new owner's fence
    /// propagates — clients sat out their full timeout (ladder D3:
    /// exactly one in-flight batch per worker lost at the move moment).
    pub fn begin_close(&self) {
        // This is the terminal handoff, shared with transaction publication,
        // no-write attachment and durable dispatch. Recover poisoning so a
        // failed worker still fences admission and retains shutdown authority.
        let stranded = {
            let mut handoff = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let stranded = handoff.retire();
            self.closed.store(true, Ordering::SeqCst);
            stranded
        };
        let first = stranded.is_some();
        if first {
            let _ = self.close_tx.send(true);
            self.pump_wake.notify_one();
            self.tasks
                .begin_close(self.db.clone(), self.history2.clone(), self.prefix.clone());
        }
        // Wake every parked live reader — on EVERY call, before the
        // double-close guard. A session parked on an idle tail has no
        // traffic to surface the fence to it: its next read re-checks
        // ownership (owned_here) and takes the typed WrongOwner
        // cutoff — but only if something wakes it AFTER the ownership
        // map moved. The slatedb fence can close the engine BEFORE
        // the loser's override mirror updates: that early wake finds
        // the OLD map and re-parks, and the fleet tick's later
        // begin_close used to early-return through this guard without
        // notifying — the parked session then held keep-alives
        // forever (round-11.4 fleet finding; the guarded-skip variant
        // reproduced only on slow CI runners).
        for h in self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            h.notify.notify_waiters();
            h.applied_notify.notify_waiters();
        }
        if !first {
            return; // already closing
        }
        for group in stranded.unwrap() {
            group.effects.reject(AppendErr::Moved);
        }
        if let Some(cb) = &self.on_close {
            cb();
        }
        self.ops.emit(
            crate::ops::OpsEvent::new(
                "engine_closed",
                format!(
                    "engine/{}/closed/{}",
                    self.prefix,
                    crate::shard::now_ms() / 1000
                ),
            )
            .shard(&self.prefix),
        );
    }

    /// How long the current commit db.write has been blocked (0 = idle).
    /// A sustained value means SlateDB backpressure (L0-full/unflushed-full
    /// with lagging compaction): admission should shed 429 instead of
    /// queueing appends into a hang.
    pub fn commit_blocked_ms(&self) -> i64 {
        let started = self.commit_write_started_ms.load(Ordering::SeqCst);
        if started == 0 {
            0
        } else {
            (now_ms() - started).max(0)
        }
    }

    #[cfg(test)]
    pub fn set_commit_write_started_ms(&self, v: i64) {
        self.commit_write_started_ms.store(v, Ordering::SeqCst);
    }

    /// Age of the oldest committed-but-not-durable group (0 = none). THE
    /// wedge signal for the common stall mode: db.write keeps succeeding
    /// (memtable has room) while the WAL-flush pipeline is stalled behind
    /// L0-full, so groups pile up here waiting for the durable watermark.
    /// The 2026-07-22 final gate run proved commit_blocked_ms alone misses
    /// this mode entirely (wedge_shed=0 through a 10-minute wedge).
    pub fn oldest_inflight_ms(&self) -> i64 {
        self.in_flight
            .lock()
            .unwrap()
            .pending()
            .first()
            .map(|g| g.written_at.elapsed().as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }

    /// Combined wedge signal: blocked commit write OR stale durability.
    pub fn wedge_ms(&self) -> i64 {
        self.commit_blocked_ms().max(self.oldest_inflight_ms())
    }

    pub async fn submit_absorbed(&self, hash: [u8; 16], upto: u64, bytes: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed {
                hash,
                upto,
                bytes,
                v2: false,
            })
            .await;
    }

    /// v2 boundary advance: the range is in the shared partition.
    pub async fn submit_absorbed_v2(&self, hash: [u8; 16], upto: u64, bytes: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed {
                hash,
                upto,
                bytes,
                v2: true,
            })
            .await;
    }

    /// One gather's boundary advances as a SINGLE committer message:
    /// every covered stream lands in the same write batch by
    /// construction (per-stream sends only coalesced opportunistically).
    /// Entries are (hash, new upto, frame bytes copied).
    pub async fn submit_absorbed_batch_v2(&self, streams: Vec<([u8; 16], u64, u64)>) {
        if streams.is_empty() {
            return;
        }
        let _ = self
            .tx
            .send(CommitOp::AbsorbedBatch { streams, v2: true })
            .await;
    }

    /// The history partition ONLY IF already open — the metrics path
    /// must never trigger an open (an idle shard would materialize a
    /// whole partition DB just to report zeros).
    pub fn history_partition_if_open(&self) -> Option<Arc<Db>> {
        self.history2.get()
    }

    /// The shard's shared history v2 partition, opened once and shared
    /// between the absorber's gather lane and v2 history reads. Values
    /// are raw stream-key-encrypted frames, so the partition needs no
    /// block transformer and no compression (frames compress before
    /// encryption; re-compressing ciphertext is pure waste).
    pub async fn history_partition(&self) -> Result<Arc<Db>, slatedb::Error> {
        if self.is_closed() {
            return Err(slatedb::Error::closed(
                "engine closed".into(),
                slatedb::CloseReason::Clean,
            ));
        }
        let path = crate::sharddir::history2_path(&self.prefix);
        let store = self.data_store.clone();
        let settings = self.history2_settings.clone();
        let cache = self.history_resources.cache.clone();
        self.history2
            .open(move || {
                crate::bootstrap::on_slatedb_rt(async move {
                    Db::builder(path.as_str(), store)
                        .with_settings(settings)
                        .with_db_cache(cache)
                        .build()
                        .await
                })
            })
            .await
    }

    /// Enumerate the durable dirty-stream index: every stream whose last
    /// committed batch left `absorbed < next`, with those two boundaries
    /// as of that batch. This is how a fresh owner rediscovers unabsorbed
    /// tails after restart/handoff WITHOUT materializing stream handles
    /// and without customer keys.
    /// Producer-state lookup through the routing key's predecessor
    /// chain (ROUTING-V3 §3.6): own identity first, then each sealed
    /// predecessor. A hit on a predecessor means the producer's last
    /// commit landed before a split — the caller stages it locally so
    /// the duplicate check answers with the ORIGINAL offset and no new
    /// offset is consumed.
    /// Split-safe Stream-Seq (review blocker 4): a sequence lane lives
    /// per (segment, routing key), but a child segment starts empty —
    /// without consulting its sealed predecessors, a sequence the
    /// PARENT already accepted would be accepted again on the child.
    /// Nearest identity wins, exactly like the producer chain.
    async fn load_seq_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
    ) -> Result<Option<String>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
                return Ok(String::from_utf8(v.to_vec()).ok());
            }
        }
        Ok(None)
    }

    async fn load_producer_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
        pid: &str,
    ) -> Result<Option<(u64, u64, u64, [u8; 16])>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            match self.db.get(producer_key(identity, key_hash, pid)).await? {
                Some(v) if v.len() >= 40 => {
                    let mut h = [0u8; 16];
                    h.copy_from_slice(&v[24..40]);
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::from_le_bytes(v[16..24].try_into().unwrap()),
                        h,
                    )));
                }
                Some(v) if v.len() >= 24 => {
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::from_le_bytes(v[16..24].try_into().unwrap()),
                        [0u8; 16],
                    )));
                }
                Some(v) if v.len() >= 16 => {
                    // Legacy 16-byte row: the commit offset is UNKNOWN.
                    // u64::MAX marks that (offset 0 is a perfectly valid
                    // commit — the old 0-sentinel answered the wrong
                    // offset for a first-record duplicate).
                    return Ok(Some((
                        u64::from_le_bytes(v[0..8].try_into().unwrap()),
                        u64::from_le_bytes(v[8..16].try_into().unwrap()),
                        u64::MAX,
                        [0u8; 16],
                    )));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Published maintenance state for admission decisions.
    pub fn maintenance_snapshot(&self) -> ShardMaintenance {
        *self.maintenance.read().unwrap()
    }

    /// Publish new maintenance state. Callers must only do this AFTER
    /// the write carrying the durable row has succeeded — that ordering
    /// is the entire fix for phantom backlog.
    pub fn publish_maintenance(&self, m: ShardMaintenance) {
        *self.maintenance.write().unwrap() = m;
    }

    #[cfg(test)]
    pub async fn scan_dirty_streams(&self) -> anyhow::Result<Vec<([u8; 16], u64, u64)>> {
        #[cfg(test)]
        {
            let mut faults = dirty_scan_faults().lock().unwrap();
            if let Some(n) = faults.get_mut(&self.prefix)
                && *n > 0
            {
                *n -= 1;
                anyhow::bail!("injected dirty-scan fault (test hook)");
            }
        }
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&DIRTY_SENTINEL);
        pfx.push(b'D');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != 33 || kv.value.len() < 16 {
                continue;
            }
            let mut h = [0u8; 16];
            h.copy_from_slice(&kv.key[17..33]);
            let absorbed = u64::from_le_bytes(kv.value[..8].try_into().unwrap());
            let next = u64::from_le_bytes(kv.value[8..16].try_into().unwrap());
            out.push((h, absorbed, next));
        }
        Ok(out)
    }

    pub async fn scan_dirty_streams_page(
        &self,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> anyhow::Result<(Vec<([u8; 16], u64, u64)>, bool)> {
        use std::ops::Bound;
        anyhow::ensure!(limit > 0, "dirty page limit must be positive");
        #[cfg(test)]
        {
            let mut faults = dirty_scan_faults().lock().unwrap();
            if let Some(n) = faults.get_mut(&self.prefix)
                && *n > 0
            {
                *n -= 1;
                anyhow::bail!("injected dirty-scan fault (test hook)");
            }
        }
        let mut prefix = DIRTY_SENTINEL.to_vec();
        prefix.push(b'D');
        let range = (
            after.map_or(Bound::Unbounded, |h| Bound::Excluded(h.to_vec())),
            Bound::<Vec<u8>>::Unbounded,
        );
        let mut scan = self.db.scan_prefix(prefix, range).await?;
        let mut rows = Vec::new();
        while let Some(kv) = scan.next().await? {
            if rows.len() == limit {
                return Ok((rows, true));
            }
            let hash: [u8; 16] = kv
                .key
                .get(17..)
                .ok_or_else(|| anyhow::anyhow!("invalid dirty-stream key"))?
                .try_into()?;
            let marker = decode_dirty_value(&kv.value)
                .ok_or_else(|| anyhow::anyhow!("invalid dirty-stream value"))?;
            anyhow::ensure!(
                marker.absorbed <= marker.next,
                "invalid dirty-stream boundaries"
            );
            rows.push((hash, marker.absorbed, marker.next));
        }
        Ok((rows, false))
    }

    /// The durable tail for one stream WITHOUT materializing a handle —
    /// the startup marker scan reads these for the exact pending state
    /// (unabsorbed_bytes, trim debt) of each marked stream; loading
    /// handles for every cold dirty stream is exactly what memory
    /// pruning must avoid.
    pub async fn tail_fields(&self, hash: &[u8; 16]) -> anyhow::Result<Option<TailFields>> {
        Ok(self
            .db
            .get(tail_key(hash))
            .await?
            .map(|raw| stored_tail(&raw))
            .transpose()?)
    }

    /// Enroll a stream in TrimTick maintenance (startup marker scan; the
    /// committer maintains the set itself for live streams).
    pub fn note_trim_debt(&self, hash: [u8; 16]) {
        self.trim_debt.lock().unwrap().insert(hash);
    }

    /// Queue one budgeted trim-maintenance pulse NOW (tests drive drain
    /// cadence with this; the 5 s flush ticker is the production driver).
    pub fn pump_trim_tick(&self) {
        let _ = self.tx.try_send(CommitOp::TrimTick);
    }

    /// (streams owing trims, last group's deletes, max deletes in any
    /// one group, cumulative deletes) — the mature-second-wave gate
    /// reads max ≤ trim_global_budget from here.
    pub fn trim_stats(&self) -> (usize, u64, u64, u64) {
        (
            self.trim_debt.lock().unwrap().len(),
            self.trim_deletes_last.load(Ordering::Relaxed),
            self.trim_deletes_max_batch.load(Ordering::Relaxed),
            self.trim_deletes_total.load(Ordering::Relaxed),
        )
    }

    /// The absorbed boundary as recorded by the REMOTELY-DURABLE tracker —
    /// the strongest boundary any `DurabilityLevel::Remote` scan of the
    /// shard log can have observed trims for. The published handle state is
    /// NOT enough for that purpose: trim deletes become scan-visible when
    /// their batch is durable, while `handle.state.durable` advances only
    /// at dispatch, which can lag durability arbitrarily under load
    /// (2026-07-27 boundary-race DST failure). Readers revalidating a tail
    /// scan against concurrent absorption must consult this.
    /// Remotely-durable `(absorbed, history_v2)` from the stored tail
    /// row. Returned TOGETHER because they must be read consistently: a
    /// reader that adopts a remote boundary while keeping a stale
    /// in-memory layout flag would refuse a v2 history range as v1
    /// (observed in the first-absorption flush-to-dispatch window).
    pub async fn durable_absorbed(&self, hash: &[u8; 16]) -> Result<(u64, bool), slatedb::Error> {
        #[cfg(test)]
        if let Ok((entered, release)) = record::TEST_MARKER_HOLD.try_with(Clone::clone) {
            entered.notify_one();
            release.notified().await;
        }
        let v = self
            .db
            .get_with_options(
                tail_key(hash),
                &slatedb::config::ReadOptions {
                    durability_filter: DurabilityLevel::Remote,
                    ..Default::default()
                },
            )
            .await?;
        Ok(v.map(|b| stored_tail(&b))
            .transpose()?
            .map_or((0, false), |t| (t.absorbed, t.history_v2)))
    }

    /// Durable consumer-cursor hint for the pull pre-read window. A
    /// stale value only widens the window; the committer's
    /// stop-at-uncovered rule keeps leasing exact.
    /// Create the tail row for a FORK's storage identity: its own
    /// record space begins at the fork boundary — nothing below exists
    /// under this identity (inherited records are served from the
    /// ancestor chain). No-op when the tail already exists (idempotent
    /// PUT retries).
    pub async fn seed_fork_tail(
        &self,
        hash: [u8; 16],
        route: [u8; 16],
        at: u64,
    ) -> Result<(), slatedb::Error> {
        if let Some(raw) = self.db.get(tail_key(&hash)).await? {
            stored_tail(&raw)?;
            return Ok(());
        }
        let t = TailFields {
            next: at,
            absorbed: at,
            trimmed: at,
            trim_safe_to: at,
            history_v2: true,
            route,
            ..Default::default()
        };
        self.db
            .put(&tail_key(&hash)[..], &encode_tail(&t)[..])
            .await
            .map(|_| ())
    }

    pub async fn queue_cursor(
        &self,
        hash: [u8; 16],
        consumer: &str,
        cgen: u64,
    ) -> Result<u64, slatedb::Error> {
        Ok(self
            .db
            .get(crate::queue::cursor_key(&hash, consumer, cgen))
            .await?
            .map(|v| decode_cursor(&v))
            .transpose()?
            .unwrap_or(0))
    }

    pub async fn submit_queue(
        &self,
        hash: [u8; 16],
        op: crate::queue::QueueOp,
    ) -> Result<crate::queue::QueueOut, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CommitOp::Queue { hash, op, resp: tx })
            .await
            .map_err(|_| "committer gone".to_string())?;
        // Entered-proof AFTER the send: the counter's contract is
        // "made the queue". Incrementing before the ASYNC send let a
        // test's ordering guard pass while this task was still
        // suspended short of the channel — a later submission could
        // overtake it (round-9 CI: a group-local delete overtook the
        // settle whose counter tick had already been observed).
        #[cfg(test)]
        self.appends_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        rx.await
            .map_err(|_| "committer dropped request".to_string())?
    }

    pub async fn stream_handle(&self, hash: [u8; 16]) -> Result<Arc<StreamHandle>, slatedb::Error> {
        if let Some(h) = self.streams.lock().unwrap().get(&hash) {
            h.last_touch_ms
                .store(now_ms() as u64, std::sync::atomic::Ordering::Relaxed);
            return Ok(h.clone());
        }
        let tail = match self.db.get(tail_key(&hash)).await? {
            Some(raw) => stored_tail(&raw)?,
            None => TailFields::default(),
        };
        // Trim-debt discovery on load: a stream evicted (or restarted)
        // mid-maintenance re-enters the TrimTick rotation the moment
        // anything touches it again. The absorber's startup marker scan
        // covers never-touched streams.
        if tail.trimmed < tail.trim_safe_to {
            self.trim_debt.lock().unwrap().insert(hash);
        }
        let handle = Arc::new(StreamHandle {
            owner: Arc::downgrade(&self.db),
            hash,
            state: Mutex::new(StreamState {
                durable: tail.clone(),
                applied: tail,
                producers: HashMap::new(),
                seqs: HashMap::new(),
                queue: crate::queue::QueueState::default(),
            }),
            notify: Notify::new(),
            applied_notify: Notify::new(),
            ring: Mutex::new(TailRing::default()),
            last_touch_ms: std::sync::atomic::AtomicU64::new(now_ms() as u64),
            pressure: std::sync::OnceLock::new(),
        });
        let mut map = self.streams.lock().unwrap();
        Ok(map.entry(hash).or_insert(handle).clone())
    }

    /// Evict resident handles idle at least `idle` and referenced by
    /// nobody but the map (strong_count == 1 — in-flight readers,
    /// waiters, ring publication and committer batches all hold clones,
    /// so anything in use is untouchable by construction). Then, if the
    /// map still exceeds `max_resident` (0 = uncapped), evict the
    /// OLDEST-touched unreferenced handles down to the cap regardless of
    /// idle age — time-based eviction alone lets a cardinality burst
    /// accumulate rate × idle-window handles before the first ages out.
    /// Referenced handles are never evicted, so the map can exceed the
    /// cap by the number of streams actively in use. Returns how many
    /// were dropped. A later touch reloads durable state from the shard
    /// DB, and the dirty-stream index keeps unabsorbed evictees
    /// discoverable.
    pub fn evict_idle_handles(&self, idle: std::time::Duration, max_resident: usize) -> usize {
        let mut map = self.streams.lock().unwrap();
        let before = map.len();
        if !idle.is_zero() {
            let cutoff = (now_ms() as u64).saturating_sub(idle.as_millis() as u64);
            map.retain(|_, h| {
                std::sync::Arc::strong_count(h) > 1
                    || h.last_touch_ms.load(std::sync::atomic::Ordering::Relaxed) > cutoff
            });
        }
        if max_resident > 0 && map.len() > max_resident {
            let mut evictable: Vec<(u64, [u8; 16])> = map
                .iter()
                .filter(|(_, h)| std::sync::Arc::strong_count(h) == 1)
                .map(|(k, h)| {
                    (
                        h.last_touch_ms.load(std::sync::atomic::Ordering::Relaxed),
                        *k,
                    )
                })
                .collect();
            evictable.sort_unstable();
            let excess = map.len() - max_resident;
            for (_, k) in evictable.into_iter().take(excess) {
                map.remove(&k);
            }
        }
        before - map.len()
    }

    pub fn resident_streams(&self) -> usize {
        self.streams.lock().unwrap().len()
    }

    /// Peek a resident handle's absorbed boundary WITHOUT materializing
    /// one (materialization is exactly what memory pruning must avoid).
    pub fn resident_absorbed(&self, hash: &[u8; 16]) -> Option<u64> {
        let h = self.streams.lock().unwrap().get(hash).cloned()?;
        let st = h.state.lock().unwrap();
        Some(st.durable.absorbed)
    }

    async fn committer_loop(self: Arc<Self>, mut rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {
        // The close signal is the ONLY way out: this task holds the engine
        // and the engine holds a sender, so `rx` can never report closed.
        loop {
            let first = tokio::select! {
                _ = self.closed() => {
                    // Fail everything still queued — their clients would
                    // otherwise hang into their own timeouts — then exit.
                    while let Ok(op) = rx.try_recv() {
                        match op {
                            CommitOp::Append(r) => {
                                let _ = r.resp.send(Err(AppendErr::Moved));
                            }
                    CommitOp::Close(CloseReq { resp, .. }) | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                        let _ = resp.send(Err(AppendErr::Moved));
                    }
                            CommitOp::Queue { resp, .. } => {
                                let _ = resp.send(Err("shard fenced/moved; retry".into()));
                            }
                            CommitOp::Absorbed { .. }
                            | CommitOp::AbsorbedBatch { .. }
                            | CommitOp::TrimTick
                            | CommitOp::UsageAck { .. }
                            | CommitOp::BillingClose { .. }
                            | CommitOp::BillingRetained { .. }
                            | CommitOp::TrimStep { .. } => {}
                        }
                    }
                    return;
                }
                got = rx.recv() => {
                    let Some(op) = got else { return };
                    op
                }
            };
            if self.is_closed() {
                // Set-before-subscribe race: honor the flag, fail the op we
                // just took plus the rest of the queue, and exit.
                match first {
                    CommitOp::Append(r) => {
                        let _ = r.resp.send(Err(AppendErr::Moved));
                    }
                    CommitOp::Close(CloseReq { resp, .. })
                    | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                        let _ = resp.send(Err(AppendErr::Moved));
                    }
                    CommitOp::Queue { resp, .. } => {
                        let _ = resp.send(Err("shard fenced/moved; retry".into()));
                    }
                    CommitOp::Absorbed { .. }
                    | CommitOp::AbsorbedBatch { .. }
                    | CommitOp::TrimTick
                    | CommitOp::UsageAck { .. }
                    | CommitOp::BillingClose { .. }
                    | CommitOp::BillingRetained { .. }
                    | CommitOp::TrimStep { .. } => {}
                }
                while let Ok(op) = rx.try_recv() {
                    match op {
                        CommitOp::Append(r) => {
                            let _ = r.resp.send(Err(AppendErr::Moved));
                        }
                        CommitOp::Close(CloseReq { resp, .. })
                        | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                            let _ = resp.send(Err(AppendErr::Moved));
                        }
                        CommitOp::Queue { resp, .. } => {
                            let _ = resp.send(Err("shard fenced/moved; retry".into()));
                        }
                        CommitOp::Absorbed { .. }
                        | CommitOp::AbsorbedBatch { .. }
                        | CommitOp::TrimTick
                        | CommitOp::UsageAck { .. }
                        | CommitOp::BillingClose { .. }
                        | CommitOp::BillingRetained { .. }
                        | CommitOp::TrimStep { .. } => {}
                    }
                }
                return;
            }
            // Test gate: while held, the committer parks HERE — after
            // taking the first op, before draining the rest — so a test
            // releases it with N ops queued and gets exactly one group
            // containing all of them. Deterministic group composition,
            // the primitive every same-group scenario needs.
            #[cfg(test)]
            {
                let _hold = self.commit_gate.lock().await;
            }
            let mut ops = vec![first];
            let mut bytes = match &ops[0] {
                CommitOp::Append(r) => r.bytes,
                _ => 0,
            };
            while ops.len() < cfg.max_batch_reqs && bytes < cfg.max_batch_bytes {
                match rx.try_recv() {
                    Ok(op) => {
                        if let CommitOp::Append(r) = &op {
                            bytes += r.bytes;
                        }
                        ops.push(op);
                    }
                    Err(_) => break,
                }
            }
            // PACING (throughput-critical): each commit group becomes ONE
            // write batch and therefore ~one WAL SST; the flusher PUTs WAL
            // SSTs serially at ~1/objstore-RTT. Committing eagerly makes
            // throughput = small-group × PUT-rate (measured 4 MB/s at 25 ms
            // RTT). Under load, gather up to `gather_window` so each flush
            // cycle ships one BIG group instead of many tiny ones — bursts
            // measured 90 MB/s through this exact path. Quiet streams skip
            // the wait entirely (latency unchanged at low rate).
            if ops.len() >= cfg.pace_min_reqs
                && ops.len() < cfg.max_batch_reqs
                && bytes < cfg.max_batch_bytes
            {
                let deadline = tokio::time::Instant::now() + cfg.gather_window;
                loop {
                    if ops.len() >= cfg.max_batch_reqs || bytes >= cfg.max_batch_bytes {
                        break;
                    }
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(op)) => {
                            if let CommitOp::Append(r) = &op {
                                bytes += r.bytes;
                            }
                            ops.push(op);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            self.commit_group(ops, &cfg).await;
        }
    }

    async fn commit_group(&self, ops: Vec<CommitOp>, cfg: &ShardConfig) {
        transaction::CommitTransaction::run(self, ops, cfg).await;
    }

    /// Publish one group's frames for one stream into its ring, then
    /// evict globally-oldest batches until the engine-wide budget is
    /// non-negative. FIFO mirrors publish order across streams, so its
    /// front IS the globally oldest batch.
    fn ring_publish(&self, handle: &Arc<StreamHandle>, recs: &[(u64, Bytes)]) {
        let bytes: usize = recs.iter().map(|(_, f)| f.len()).sum();
        let (first, next) = (recs[0].0, recs[recs.len() - 1].0 + 1);
        {
            let mut ring = handle.ring.lock().unwrap();
            // A shard handoff replays through a fresh engine, so within
            // one engine offsets only grow. If a gap somehow appears
            // (defensive: absorber trim races ahead), reset rather than
            // serve a hole.
            if ring.ceil().is_some_and(|c| c != first) {
                let dropped = ring.bytes;
                ring.batches.clear();
                ring.bytes = 0;
                self.ring_budget
                    .fetch_add(dropped as i64, Ordering::Relaxed);
                let mut fifo = self.ring_fifo.lock().unwrap();
                fifo.retain(|h| !Arc::ptr_eq(h, handle));
            }
            ring.batches.push_back(RingBatch {
                first,
                next,
                frames: recs.to_vec(),
                bytes,
            });
            ring.bytes += bytes;
        }
        self.ring_fifo.lock().unwrap().push_back(handle.clone());
        self.ring_published.fetch_add(1, Ordering::Relaxed);
        let after = self.ring_budget.fetch_sub(bytes as i64, Ordering::Relaxed) - bytes as i64;
        let resident = (self.ring_cfg_bytes as i64 - after).max(0) as u64;
        self.ring_peak_bytes.fetch_max(resident, Ordering::Relaxed);
        while self.ring_budget.load(Ordering::Relaxed) < 0 {
            let Some(victim) = self.ring_fifo.lock().unwrap().pop_front() else {
                break;
            };
            let mut ring = victim.ring.lock().unwrap();
            if let Some(b) = ring.batches.pop_front() {
                ring.bytes -= b.bytes;
                self.ring_budget
                    .fetch_add(b.bytes as i64, Ordering::Relaxed);
                self.ring_evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn ring_resident_bytes(&self) -> u64 {
        (self.ring_cfg_bytes as i64 - self.ring_budget.load(Ordering::Relaxed)).max(0) as u64
    }

    /// Serve [scan_from, scan_to) from the stream's ring if the ring
    /// covers scan_from. Returns None when it does not (caller falls back
    /// to the canonical scan). Mirrors the DB path's contract exactly:
    /// stop at max_bytes, end = scan_to, last_offset = progress.
    pub fn ring_read(
        &self,
        handle: &StreamHandle,
        scan_from: u64,
        scan_to: u64,
        max_bytes: usize,
    ) -> Option<FrameReadResult> {
        self.ring_read_selected(handle, scan_from, scan_to, None, max_bytes)
    }

    /// #272: the ring read for FILTERED durable reads — the hub pump
    /// (and any keyed tail chaser) reads one routing-key lane, and the
    /// unfiltered-only gate sent every such read to the DB scan,
    /// making the "ring-preferring" pump a fiction. Scans the covered
    /// range once, decodes only frame HEADERS (the payloads stay
    /// encrypted), keeps frames whose routing key matches, and — the
    /// part a naive filter misses — returns the CONSUMED offset over
    /// non-matching frames too, so keyed readers never rescan
    /// match-free ranges (the same first-class scanned progress the DB
    /// path reports). The stored-byte budget also charges filtered misses;
    /// truncation reports the last scanned offset at the cut.
    pub fn ring_read_keyed(
        &self,
        handle: &StreamHandle,
        scan_from: u64,
        scan_to: u64,
        rk: &str,
        max_bytes: usize,
    ) -> Option<FrameReadResult> {
        self.ring_read_selected(handle, scan_from, scan_to, Some(rk), max_bytes)
    }

    /// Both entry points use the same physical-owner, durable-frontier,
    /// retained-density and stored-byte policy. Selection affects returned
    /// frames only; every inspected row contributes to the coverage witness.
    fn ring_read_selected(
        &self,
        handle: &StreamHandle,
        scan_from: u64,
        scan_to: u64,
        selector: Option<&str>,
        max_bytes: usize,
    ) -> Option<FrameReadResult> {
        if !self.ring_enabled
            || handle.owner.as_ptr() != Arc::as_ptr(&self.db)
            || scan_from >= scan_to
            || scan_to > handle.state.lock().unwrap().durable.next
        {
            return None;
        }
        let ring = handle.ring.lock().unwrap();
        let (Some(floor), Some(ceil)) = (ring.floor(), ring.ceil()) else {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            self.ring_miss_empty.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if scan_from < floor || scan_to > ceil {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            if scan_from < floor {
                self.ring_miss_below_floor.fetch_add(1, Ordering::Relaxed);
            }
            if scan_to > ceil {
                self.ring_miss_above_ceil.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        let mut out = FrameReadResult {
            frames: Vec::new(),
            last_offset: None,
            coverage: None,
        };
        let mut total = 0usize;
        let mut expected = scan_from;
        for b in ring.batches.iter() {
            if b.next <= scan_from {
                continue;
            }
            if b.first >= scan_to {
                break;
            }
            for (off, f) in &b.frames {
                if *off < scan_from {
                    continue;
                }
                if *off >= scan_to {
                    break;
                }
                // Floor/ceiling alone cannot prove density after eviction or
                // malformed cached batch metadata. Every inspected row counts,
                // including filtered misses and a byte-limited final row.
                if *off != expected {
                    return None;
                }
                let checked = record::CheckedFrame::from_ring(f, *off, selector).ok()?;
                expected = off.checked_add(1)?;
                total += f.len();
                if let Some(checked) = checked {
                    out.frames.push(checked);
                }
                // Consumed progress covers NON-matching frames too.
                out.last_offset = Some(*off);
                if total >= max_bytes {
                    out.coverage = Some(record::DurableRingCoverage::new(
                        self,
                        handle.hash,
                        scan_from,
                        expected,
                    ));
                    self.ring_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(out);
                }
            }
        }
        if expected != scan_to {
            return None;
        }
        out.coverage = Some(record::DurableRingCoverage::new(
            self,
            handle.hash,
            scan_from,
            expected,
        ));
        self.ring_hits.fetch_add(1, Ordering::Relaxed);
        Some(out)
    }

    /// Test hook: fail the next commit group that contains an op for
    /// the given segment identity — the deterministic stand-in for a
    /// WriteBatch that reaches the store and dies. One-shot; the
    /// tripped counter is the entered-proof a test asserts instead of
    /// assuming its failpoint fired.
    /// Test hook: hold the COMMIT gate. While held, the committer
    /// takes at most one op and then parks before gathering; releasing
    /// the guard lets it drain everything queued meanwhile into ONE
    /// commit group. The companion to `fail_next_group_for` for
    /// deterministic same-group scenarios.
    #[cfg(test)]
    pub async fn test_hold_commit(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_gate.lock().await
    }

    /// Entered-proof for group-composition tests: client ops — appends,
    /// fences, and queue submissions — enqueued on this engine so far. A test polls the delta
    /// instead of sleeping and hoping its request made the queue.
    #[cfg(test)]
    pub fn appends_enqueued(&self) -> u64 {
        self.appends_enqueued
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test-only ground truth for deletion tests: the number of
    /// DURABLE state rows (cursor/lease/ack, every generation) this
    /// consumer still has under this identity. The generation model
    /// makes leaked residue invisible to behavioral asserts — a
    /// recreated consumer ignores dead generations by design — so
    /// burial is proven by counting rows, not by pulling.
    #[cfg(test)]
    /// Test-only residue factory: durable lease rows under an OLD
    /// consumer generation, modeling the multi-generation residue a
    /// crashed/raced deletion leaves behind (round-17 stress gate).
    #[cfg(test)]
    pub async fn seed_consumer_residue_rows(
        &self,
        hash: [u8; 16],
        consumer: &str,
        cgen: u64,
        n: u64,
    ) -> Result<(), String> {
        let mut wb = WriteBatch::new();
        for off in 0..n {
            wb.put(
                crate::queue::lease_key(&hash, consumer, cgen, off),
                crate::queue::encode_lease(&crate::queue::Lease {
                    deadline_ms: 0,
                    delivery_count: 1,
                    lease_gen: 1,
                    key_hash: [0u8; 16],
                }),
            );
        }
        self.db
            .write_with_options(wb, &slatedb::config::WriteOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub async fn count_consumer_state_rows(
        &self,
        hash: [u8; 16],
        consumer: &str,
    ) -> Result<usize, String> {
        let mut n = 0usize;
        for tag in *b"clx" {
            let pfx = crate::queue::state_prefix(&hash, tag, consumer);
            let mut iter = self
                .db
                .scan_prefix(&pfx[..], ..)
                .await
                .map_err(|e| e.to_string())?;
            loop {
                match iter.next().await {
                    Ok(Some(_)) => n += 1,
                    Ok(None) => break,
                    Err(e) => return Err(e.to_string()),
                }
            }
        }
        Ok(n)
    }

    /// One-shot: the next ConfigDelete's state-row scan reports a
    /// failure at the scan boundary — the deterministic stand-in for a
    /// store error mid-enumeration. The contract under test: a failed
    /// scan stages NOTHING and the consumer is untouched.
    #[cfg(test)]
    pub fn fail_next_config_scan(&self) {
        self.fail_config_scan
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_config_scan_failure(&self) -> bool {
        self.fail_config_scan
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Arm a one-shot failure for the next commit group that ADVANCES
    /// an absorbed boundary (R25-D): proves retirement is atomic with
    /// the boundary, which the client-append selector cannot reach.
    #[cfg(test)]
    pub fn fail_next_absorbed_group(&self) {
        self.fail_next_absorbed_group
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn fail_next_group_for(&self, identity: [u8; 16]) {
        self.fail_group_for
            .lock()
            .unwrap()
            .get_or_insert_with(std::collections::HashSet::new)
            .insert(identity);
    }

    /// Fence-map observability (round 12): the map is deliberately
    /// unbounded (no wall-clock expiry can be proven safe against a
    /// queue with no residence bound), so its cardinality must be
    /// visible before it could ever become material.
    pub fn seal_fence_stats(&self) -> (usize, u64) {
        let f = self.seal_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Usage-dirty index scan (§6.3): every segment whose durable
    /// billing state has versions `_usage` has not acknowledged.
    /// (hash, unacked version). One prefix scan; the drainer's
    /// discovery path after restart or ownership move.
    #[cfg(test)]
    pub async fn usage_dirty_scan(&self) -> anyhow::Result<Vec<([u8; 16], u64)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'U');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            if kv.key.len() != 33 || kv.value.len() < 8 {
                continue;
            }
            let mut h = [0u8; 16];
            h.copy_from_slice(&kv.key[17..33]);
            let v = u64::from_le_bytes(kv.value[..8].try_into().unwrap());
            out.push((h, v));
        }
        Ok(out)
    }

    /// Presence probe for residency decisions: at most one row from
    /// each outbox index, including orphaned final rows.
    pub async fn has_billing_debt(&self) -> anyhow::Result<bool> {
        for tag in *b"UV" {
            let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
            prefix.push(tag);
            if self
                .db
                .scan_prefix(prefix, ..)
                .await?
                .next()
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One bounded page of the dirty index, with an exclusive identity
    /// continuation. Only the caller's finite page is materialized.
    pub async fn usage_dirty_page(
        &self,
        after: Option<[u8; 16]>,
        limit: usize,
    ) -> anyhow::Result<(Vec<([u8; 16], u64)>, bool)> {
        use std::ops::Bound;
        anyhow::ensure!(limit > 0, "dirty page limit must be positive");
        let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
        prefix.push(b'U');
        let range = (
            after.map_or(Bound::Unbounded, |h| Bound::Excluded(h.to_vec())),
            Bound::<Vec<u8>>::Unbounded,
        );
        let mut scan = self.db.scan_prefix(&prefix, range).await?;
        let mut rows = Vec::new();
        while let Some(kv) = scan.next().await? {
            if rows.len() == limit {
                return Ok((rows, true));
            }
            let hash: [u8; 16] = kv
                .key
                .get(17..)
                .ok_or_else(|| anyhow::anyhow!("invalid usage dirty key"))?
                .try_into()?;
            rows.push((hash, decode_cursor(&kv.value)?));
        }
        Ok((rows, false))
    }

    /// Bounded finals for a single dirty segment. More finals keep its
    /// dirty marker alive even after this page's exact keys are acked.
    pub async fn usage_month_finals_page(
        &self,
        hash: [u8; 16],
        limit: usize,
    ) -> anyhow::Result<(Vec<(Vec<u8>, crate::billing::SegmentSnapshot)>, bool)> {
        anyhow::ensure!(limit > 0, "final page limit must be positive");
        let mut prefix = crate::billing::USAGE_DIRTY_SENTINEL.to_vec();
        prefix.push(b'V');
        prefix.extend_from_slice(&hash);
        let mut scan = self.db.scan_prefix(&prefix, ..).await?;
        let mut rows = Vec::new();
        while let Some(kv) = scan.next().await? {
            if rows.len() == limit {
                return Ok((rows, true));
            }
            rows.push((kv.key.to_vec(), serde_json::from_slice(&kv.value)?));
        }
        Ok((rows, false))
    }

    /// Missing means never billed. Read errors and invalid rows remain errors.
    pub async fn load_billing_meta(
        &self,
        hash: [u8; 16],
    ) -> anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>> {
        #[cfg(test)]
        if billing_read_faults()
            .lock()
            .unwrap()
            .remove(&self.prefix)
            .is_some()
        {
            anyhow::bail!("injected billing metadata read failure");
        }
        self.db
            .get(crate::billing::billing_meta_key(&hash))
            .await?
            .map(|v| {
                let meta: crate::billing::SegmentBillingMetaV1 = serde_json::from_slice(&v)
                    .map_err(|e| anyhow::anyhow!("invalid billing metadata: {e}"))?;
                anyhow::ensure!(
                    meta.v == 1 && !meta.stream_id.is_empty(),
                    "invalid billing metadata identity/version"
                );
                anyhow::ensure!(
                    meta.month_storage_byte_ms.is_empty()
                        || meta.month_storage_byte_ms.parse::<u128>().is_ok(),
                    "invalid billing byte-time"
                );
                anyhow::ensure!(
                    meta.storage_accounted_through_ms == 0 || (1..=12).contains(&meta.month_month),
                    "invalid billing month"
                );
                Ok(meta)
            })
            .transpose()
    }

    /// Legacy test convenience; production must handle missing and failed reads.
    #[cfg(test)]
    pub async fn billing_meta(
        &self,
        hash: [u8; 16],
    ) -> Option<crate::billing::SegmentBillingMetaV1> {
        self.load_billing_meta(hash)
            .await
            .expect("valid billing metadata in fixture")
    }

    /// Closed-month final snapshots awaiting ledger acknowledgment
    /// (sentinel-'V' rows): (exact key, snapshot).
    #[cfg(test)]
    pub async fn usage_month_finals(
        &self,
    ) -> anyhow::Result<Vec<(Vec<u8>, crate::billing::SegmentSnapshot)>> {
        let mut pfx = Vec::with_capacity(17);
        pfx.extend_from_slice(&crate::billing::USAGE_DIRTY_SENTINEL);
        pfx.push(b'V');
        let mut out = Vec::new();
        let mut iter = self.db.scan_prefix(&pfx[..], ..).await?;
        while let Some(kv) = iter.next().await? {
            out.push((kv.key.to_vec(), serde_json::from_slice(&kv.value)?));
        }
        Ok(out)
    }

    /// Acknowledge `_usage` durability for a segment's snapshot at
    /// `version` (+ exact month-final rows). Fire-and-forget through
    /// the committer — see CommitOp::UsageAck.
    pub fn submit_usage_ack(&self, hash: [u8; 16], version: u64, month_final_keys: Vec<Vec<u8>>) {
        let _ = self.tx.try_send(CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::ThroughVersion(version),
            month_final_keys,
        });
    }

    pub fn submit_usage_final_ack(&self, hash: [u8; 16], month_final_keys: Vec<Vec<u8>>) {
        let _ = self.tx.try_send(CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::FinalRowsOnly,
            month_final_keys,
        });
    }

    /// Terminal storage closure for a hard-deleted or expired segment
    /// (§6.2), accounted to the persisted logical close instant.
    /// AWAITED submission (round-22 item 7): the caller knows whether
    /// the closure entered the committer queue — a full queue is
    /// backpressure, never a silent drop; the registry-persisted debt
    /// plus the sweep reconciler retry anything that still fails.
    pub async fn submit_billing_close(&self, hash: [u8; 16], close_ms: i64) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingClose { hash, close_ms })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }

    /// Durably persist the fork-retention flag on the billing row
    /// (round-22 item 7); awaited like the closure.
    pub async fn submit_billing_retained(
        &self,
        hash: [u8; 16],
        retained: bool,
    ) -> Result<(), String> {
        self.tx
            .send(CommitOp::BillingRetained { hash, retained })
            .await
            .map_err(|_| "committer queue closed".to_string())
    }

    /// Consumer-fence cardinality (round 17): one non-expiring entry
    /// per (segment identity, deleted consumer name) — correct for
    /// safety, so its growth is surfaced instead of hidden. Any future
    /// cleanup must be proved by committer-queue progress, exactly like
    /// the seal fences; never wall-clock expiry.
    pub fn consumer_fence_stats(&self) -> (usize, u64) {
        let f = self.consumer_fences.lock().unwrap();
        let max = f.values().copied().max().unwrap_or(0);
        (f.len(), max)
    }

    /// Test hook: empty the engine-resident fence map WITHOUT touching
    /// the durable rows — the deterministic stand-in for "this shard
    /// just moved to another instance, which opened a fresh engine".
    /// The durable fence must still refuse dead generations.
    #[cfg(test)]
    pub fn forget_consumer_fences_for_test(&self) {
        self.consumer_fences.lock().unwrap().clear();
    }

    /// Test view of the DURABLE fence row (None = no row).
    #[cfg(test)]
    pub async fn durable_consumer_fence(&self, hash: [u8; 16], consumer: &str) -> Option<u64> {
        let k = crate::queue::fence_key(&hash, consumer);
        self.db
            .get(&k[..])
            .await
            .ok()
            .flatten()
            .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
    }

    #[cfg(test)]
    pub fn group_failures_tripped(&self) -> usize {
        self.fail_group_tripped
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test hook: hold the dispatch gate. While held, NEITHER the acker
    /// nor the pump can dispatch acks — the deterministic stand-in for
    /// "the acker is paused after durability, before response dispatch".
    #[cfg(test)]
    pub async fn test_hold_dispatch(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.dispatch_gate.lock().await
    }

    /// Release everything the durable watermark now covers: record
    /// timings, publish tail state, send producer/queue acks, feed the
    /// absorber and touch journals. Entirely synchronous, so the caller
    /// can rely on "when this returns, the acks are on their way" — the
    /// property the pump's gather window is built on. Returns requests
    /// dispatched. Called from the acker (watch-driven failsafe + the
    /// only path when the pump is off) and from the pump (explicit
    /// barrier right after its flush returns).
    async fn dispatch_durable(&self, durable_seq: u64) -> u32 {
        let _order = self.dispatch_gate.lock().await;
        // Claim only proven remote-durable groups while this owner is live.
        // Retirement after the claim does not revoke that already-durable
        // completion; effects run outside the synchronous handoff mutex.
        let ready = self.in_flight.lock().unwrap().take_durable(durable_seq);
        #[cfg(test)]
        if !ready.is_empty() {
            self.completion_checkpoint(retirement_tests::CompletionPhase::Durable)
                .await;
        }
        let mut dispatched = 0u32;
        for group in ready {
            dispatched += group.reqs;
            // Publish to the durable-tail ring FIRST: the ring ceiling
            // must already cover an offset by the time tail state (and
            // then an ack) makes that offset visible, or a reader woken
            // by the ack would miss the fast path — or worse, serve a
            // truncated range.
            for (handle, recs) in &group.effects.ring_pub {
                self.ring_publish(handle, recs);
            }
            {
                let wait_us = group.written_at.elapsed().as_micros().min(u32::MAX as u128) as u32;
                let mut t = self.timings.lock().unwrap();
                t.push_back(GroupTiming {
                    ts_ms: now_ms(),
                    queue_wait_us: group.queue_wait_us,
                    encode_us: group.encode_us,
                    write_us: group.write_us,
                    durable_wait_us: wait_us,
                    reqs: group.reqs,
                    records: group.records_n,
                    bytes: group.bytes,
                });
                if t.len() > 128 {
                    t.pop_front();
                }
            }
            for (handle, fields) in &group.effects.tails {
                handle.state.lock().unwrap().durable = fields.clone();
                handle.notify.notify_waiters();
            }
            for (usage, plaintext, frames) in group.effects.usage {
                usage
                    .plaintext_bytes
                    .fetch_add(plaintext, Ordering::Relaxed);
                usage.frame_bytes.fetch_add(frames, Ordering::Relaxed);
            }
            for (resp, res) in group.effects.acks {
                let _ = resp.send(res);
            }
            for (resp, out) in group.effects.queue_acks {
                let _ = resp.send(out);
            }
            for s in group.effects.signals {
                let _ = self.absorb_tx.try_send(s);
            }
            // H2: feed touch journals only after the data is durable and
            // reader-visible, so an invalidation always finds fresh data.
            for t in group.effects.touches {
                t.journal.ingest(&t.key_ids, t.next_offset);
            }
        }
        dispatched
    }

    async fn acker_loop(self: Arc<Self>) {
        let mut status_rx = self.db.subscribe();
        loop {
            if self.is_closed() {
                return;
            }
            let durable_seq = {
                let status = status_rx.borrow_and_update();
                if let Some(reason) = &status.close_reason {
                    tracing::error!(shard = %self.prefix, "shard db closed: {reason:?}");
                    self.begin_close();
                    return;
                }
                status.durable_seq
            };
            self.dispatch_durable(durable_seq).await;
            tokio::select! {
                _ = self.closed() => return,
                changed = status_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = self.flush_wake.notified() => {}
            }
        }
    }
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    use std::sync::Arc;

    /// R25-A: the delta rule. Retirement past the ledger is an ERROR —
    /// clamping would hide the exact unit-divergence class this type
    /// exists to prevent.
    #[test]
    fn apply_delta_is_checked_and_tracks_progress() {
        let m = ShardMaintenance::default();
        let m = m.apply_delta(1000, 0, 5_000).unwrap();
        assert_eq!(m.unabsorbed_frame_bytes, 1000);
        assert_eq!(m.backlog_started_ms, 5_000);
        assert_eq!(m.last_progress_ms, 5_000);
        assert_eq!(m.version, 1);

        // Later append: the backlog-start clock must NOT restart.
        let m = m.apply_delta(500, 0, 9_000).unwrap();
        assert_eq!(m.backlog_started_ms, 5_000, "backlog start must not reset");

        // Retirement refreshes the PROGRESS clock — the stall signal is
        // "time since durable progress", not "age of oldest record",
        // which stays permanently old under continuous traffic.
        let m = m.apply_delta(0, 600, 12_000).unwrap();
        assert_eq!(m.unabsorbed_frame_bytes, 900);
        assert_eq!(m.last_progress_ms, 12_000);
        assert_eq!(m.no_progress_secs(20_000), 8);

        // Full drain retires both clocks.
        let m = m.apply_delta(0, 900, 15_000).unwrap();
        assert_eq!(m.unabsorbed_frame_bytes, 0);
        assert_eq!(m.backlog_started_ms, 0);
        assert_eq!(m.no_progress_secs(99_000), 0);

        // Over-retirement is a loud error, never a silent clamp.
        assert!(
            ShardMaintenance::default().apply_delta(10, 11, 1).is_err(),
            "retiring more than exists must fail"
        );
    }

    /// R25-A/R26-4: the codec round-trips v2; the R24 16-byte row is
    /// classified LEGACY — its payload-unit value is never surfaced as
    /// frame bytes (it can under- OR overstate, and understatement makes
    /// the first exact retirement read as over-retirement forever).
    #[test]
    fn codec_roundtrips_v2_and_refuses_v1_values() {
        let m = ShardMaintenance {
            version: 7,
            unabsorbed_frame_bytes: 123_456,
            backlog_started_ms: 111,
            last_progress_ms: 222,
        };
        let got = decode_shard_maint(&encode_shard_maint(&m)).unwrap();
        assert_eq!(got, m);

        // R24 layout: [bytes u64][oldest_ms i64], 16 untagged bytes.
        let mut v1 = [0u8; 16];
        v1[..8].copy_from_slice(&987_654u64.to_le_bytes());
        v1[8..].copy_from_slice(&42i64.to_le_bytes());
        assert!(
            matches!(
                decode_shard_maint_row(&v1),
                Ok(ShardMaintRow::LegacyPayloadUnit)
            ),
            "16-byte row must classify as legacy"
        );
        assert!(
            decode_shard_maint(&v1).is_err(),
            "the strict decode must never surface a payload-unit value"
        );

        assert!(
            decode_shard_maint(&[0u8; 7]).is_err(),
            "corrupt row must error"
        );
        let mut bad = [0u8; 40];
        bad[0] = 99;
        assert!(
            decode_shard_maint(&bad).is_err(),
            "unknown version must error"
        );
    }

    /// R25-A: load semantics against a real DB — present row loads (with
    /// the progress clock initialized for v1 rows), missing row rebuilds
    /// from the dirty index + tails and PERSISTS the rebuilt row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_or_rebuild_covers_present_missing_and_corrupt() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // 1. Present v2 row: loads exactly.
        let db = Db::builder("m1/shard", store.clone())
            .build()
            .await
            .unwrap();
        let m = ShardMaintenance {
            version: 3,
            unabsorbed_frame_bytes: 555,
            backlog_started_ms: 10,
            last_progress_ms: 20,
        };
        let mut wb = WriteBatch::new();
        wb.put(shard_maint_key(), encode_shard_maint(&m));
        db.write_with_options(wb, &WriteOptions::default())
            .await
            .unwrap();
        assert_eq!(load_or_rebuild_maintenance(&db).await.unwrap(), m);
        db.close().await.unwrap();

        // 2. Present v1 row (payload-unit 777): the value is IGNORED and
        // the ledger is rebuilt from the exact tails (R26-4). The
        // persisted replacement is a v2 row.
        let db = Db::builder("m2/shard", store.clone())
            .build()
            .await
            .unwrap();
        let mut v1 = [0u8; 16];
        v1[..8].copy_from_slice(&777u64.to_le_bytes());
        let h0 = [9u8; 16];
        let mut wb = WriteBatch::new();
        wb.put(shard_maint_key(), v1);
        wb.put(
            tail_key(&h0),
            encode_tail(&TailFields {
                next: 5,
                absorbed: 2,
                unabsorbed_bytes: 300,
                ..Default::default()
            }),
        );
        wb.put(
            dirty_key(&h0),
            dirty_value(&StreamMaintenance {
                absorbed: 2,
                next: 5,
                ..Default::default()
            }),
        );
        db.write_with_options(wb, &WriteOptions::default())
            .await
            .unwrap();
        let got = load_or_rebuild_maintenance(&db).await.unwrap();
        assert_eq!(
            got.unabsorbed_frame_bytes, 300,
            "legacy value must be rebuilt from tails, never converted"
        );
        assert!(
            got.last_progress_ms > 0,
            "rebuilt backlog must start the stall clock"
        );
        let raw = db
            .get(shard_maint_key())
            .await
            .unwrap()
            .expect("row replaced");
        assert_eq!(raw.len(), 40, "the legacy row must be replaced by v2");
        db.close().await.unwrap();

        // 3. Missing row: rebuild from dirty index + tails, then persist.
        let db = Db::builder("m3/shard", store.clone())
            .build()
            .await
            .unwrap();
        let h1 = [1u8; 16];
        let h2 = [2u8; 16];
        let mut wb = WriteBatch::new();
        for (h, bytes) in [(h1, 300u64), (h2, 400u64)] {
            let t = TailFields {
                next: 10,
                absorbed: 4,
                unabsorbed_bytes: bytes,
                ..Default::default()
            };
            wb.put(tail_key(&h), encode_tail(&t));
            wb.put(
                dirty_key(&h),
                dirty_value(&StreamMaintenance {
                    absorbed: 4,
                    next: 10,
                    ..Default::default()
                }),
            );
        }
        db.write_with_options(wb, &WriteOptions::default())
            .await
            .unwrap();
        let got = load_or_rebuild_maintenance(&db).await.unwrap();
        assert_eq!(got.unabsorbed_frame_bytes, 700, "rebuild sums tail gauges");
        // And it persisted: a second load takes the row path.
        let raw = db
            .get(shard_maint_key())
            .await
            .unwrap()
            .expect("row persisted");
        assert_eq!(
            decode_shard_maint(&raw).unwrap().unabsorbed_frame_bytes,
            700
        );
        db.close().await.unwrap();

        // 4. Corrupt row: an engine-open FAILURE, never zero backlog.
        let db = Db::builder("m4/shard", store.clone())
            .build()
            .await
            .unwrap();
        let mut wb = WriteBatch::new();
        wb.put(shard_maint_key(), vec![9u8; 11]);
        db.write_with_options(wb, &WriteOptions::default())
            .await
            .unwrap();
        assert!(
            load_or_rebuild_maintenance(&db).await.is_err(),
            "corrupt maintenance row must fail the open"
        );
        db.close().await.unwrap();
    }
}

#[cfg(test)]
mod storage_decode_tests {
    use super::*;

    #[test]
    fn r12_supported_tail_versions_and_extensions() {
        let tail = TailFields {
            next: 9,
            absorbed: 4,
            trimmed: 2,
            trim_safe_to: 3,
            seq: Some("lane".into()),
            ..Default::default()
        };
        let full = encode_tail(&tail);
        let base = 44 + 4;
        for extension in [0, 16, 24, 32] {
            let decoded = stored_tail(&full[..base + extension]).unwrap();
            assert_eq!(decoded.next, 9);
            assert_eq!(decoded.seq.as_deref(), Some("lane"));
        }
        let mut v2 = full.clone();
        v2[0] = 2;
        v2.remove(41); // v2 has no flags
        assert_eq!(stored_tail(&v2[..43 + 4]).unwrap().next, 9);
        for len in 0..full.len() {
            if ![base, base + 16, base + 24].contains(&len) {
                assert!(stored_tail(&full[..len]).is_err(), "len={len}");
            }
        }
        let mut invalid = full.clone();
        invalid[0] = 99;
        assert!(stored_tail(&invalid).is_err());
        let mut invalid = full.clone();
        invalid[41] = 4;
        assert!(stored_tail(&invalid).is_err());
        let mut invalid = full;
        invalid[25..33].copy_from_slice(&10u64.to_le_bytes());
        assert!(stored_tail(&invalid).is_err());
    }

    #[test]
    fn r12_cursor_requires_exact_width() {
        for len in 0..=9 {
            let result = decode_cursor(&vec![0; len]);
            assert_eq!(result.is_ok(), len == 8, "len={len}");
        }
        assert_eq!(decode_cursor(&123u64.to_le_bytes()).unwrap(), 123);
    }

    #[test]
    fn r12_byte_compatibility_does_not_bypass_stored_state_validation() {
        let tail = TailFields {
            next: 9,
            absorbed: 4,
            trimmed: 2,
            trim_safe_to: 3,
            ..Default::default()
        };
        for version in [2, 3] {
            let mut encoded = encode_tail(&tail);
            if version == 2 {
                encoded[0] = 2;
                encoded.remove(41);
            }
            encoded.extend_from_slice(&[0xee; 7]);
            assert_eq!(stored_tail(&encoded).unwrap().next, tail.next);
            for (at, value) in [(25, 10u64), (33, 5), (encoded.len() - 23, 5)] {
                let mut inconsistent = encoded.clone();
                inconsistent[at..at + 8].copy_from_slice(&value.to_le_bytes());
                assert!(decode_tail(&inconsistent).is_some(), "byte layout is valid");
                assert!(stored_tail(&inconsistent).is_err(), "state is invalid");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r12_corrupt_tail_refuses_open_without_overwriting_records() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder("r12", store.clone()).build().await.unwrap());
        let hash = [12; 16];
        let mut wb = WriteBatch::new();
        wb.put(record_key(&hash, 0), b"retained ciphertext");
        wb.put(tail_key(&hash), b"broken tail");
        db.write(wb).await.unwrap().await_durable().await.unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r12".into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        assert!(engine.stream_handle(hash).await.is_err());
        assert!(engine.tail_fields(&hash).await.is_err());
        assert!(engine.durable_absorbed(&hash).await.is_err());
        assert!(engine.seed_fork_tail(hash, [1; 16], 0).await.is_err());
        assert_eq!(
            db.get(record_key(&hash, 0))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"retained ciphertext"
        );
        assert_eq!(
            db.get(tail_key(&hash)).await.unwrap().unwrap().as_ref(),
            b"broken tail"
        );
        assert!(!engine.streams.lock().unwrap().contains_key(&hash));
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
fn billing_read_faults() -> &'static Mutex<HashMap<String, ()>> {
    static FAULTS: std::sync::OnceLock<Mutex<HashMap<String, ()>>> = std::sync::OnceLock::new();
    FAULTS.get_or_init(Default::default)
}

#[cfg(test)]
mod billing_read_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r13_failed_accounting_reads_preserve_group_and_newer_dirty_version() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(Db::builder("r13", store.clone()).build().await.unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r13".into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        let hash = [13; 16];
        let meta = crate::billing::SegmentBillingMetaV1 {
            v: 1,
            stream_id: "existing".into(),
            usage_version: 8,
            ingest_payload_bytes_total: 923,
            owned_frame_bytes_current: 876,
            ..Default::default()
        };
        let encoded = serde_json::to_vec(&meta).unwrap();
        let dirty = crate::billing::usage_dirty_key(&hash);
        let key = crate::billing::billing_meta_key(&hash);
        let final_key = crate::billing::usage_month_final_key(&hash, 2026, 7);
        for corrupt in [false, true] {
            let before = if corrupt {
                b"invalid financial state".to_vec()
            } else {
                encoded.clone()
            };
            let mut wb = WriteBatch::new();
            wb.put(key.clone(), before.clone());
            wb.put(dirty.clone(), 8u64.to_le_bytes());
            wb.put(final_key.clone(), b"owed snapshot");
            wb.put(record_key(&hash, 0), b"retained record");
            db.write(wb).await.unwrap();
            for action in 0..3 {
                if !corrupt {
                    billing_read_faults()
                        .lock()
                        .unwrap()
                        .insert("r13".into(), ());
                }
                let accounting = match action {
                    0 => CommitOp::UsageAck {
                        hash,
                        scope: UsageAckScope::ThroughVersion(7),
                        month_final_keys: vec![final_key.clone()],
                    },
                    1 => CommitOp::BillingClose {
                        hash,
                        close_ms: 1000,
                    },
                    _ => CommitOp::BillingRetained {
                        hash,
                        retained: true,
                    },
                };
                let (tx, rx) = oneshot::channel();
                engine
                    .commit_group(
                        vec![
                            CommitOp::Queue {
                                hash,
                                op: crate::queue::QueueOp::ConfigGet {
                                    consumer: "c".into(),
                                },
                                resp: tx,
                            },
                            accounting,
                        ],
                        &ShardConfig::default(),
                    )
                    .await;
                assert!(
                    rx.await.unwrap().is_err(),
                    "no group success on required read failure"
                );
                assert_eq!(db.get(&key).await.unwrap().unwrap().as_ref(), &before);
                assert_eq!(
                    db.get(&dirty).await.unwrap().unwrap().as_ref(),
                    &8u64.to_le_bytes()
                );
                assert_eq!(
                    db.get(&final_key).await.unwrap().unwrap().as_ref(),
                    b"owed snapshot"
                );
                assert_eq!(
                    db.get(record_key(&hash, 0))
                        .await
                        .unwrap()
                        .unwrap()
                        .as_ref(),
                    b"retained record"
                );
            }
        }
        db.put(&key, encoded).await.unwrap();
        engine
            .commit_group(
                vec![CommitOp::UsageAck {
                    hash,
                    scope: UsageAckScope::ThroughVersion(7),
                    month_final_keys: vec![],
                }],
                &ShardConfig::default(),
            )
            .await;
        assert_eq!(
            db.get(&dirty).await.unwrap().unwrap().as_ref(),
            &8u64.to_le_bytes()
        );
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
mod commit_command_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r03_close_and_fence_wait_for_write_remote_durability_and_dispatch() {
        let store = crate::dst::FaultStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            303,
            crate::dst::FaultProfile::clean(),
        );
        let db = Arc::new(
            Db::builder("r03-barriers", store.clone())
                .with_settings(slatedb::config::Settings {
                    flush_interval: Some(std::time::Duration::from_millis(5)),
                    ..Default::default()
                })
                .build()
                .await
                .unwrap(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r03-barriers".into(),
            db.clone(),
            store.clone(),
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        let hash = [3; 16];
        let handle = engine.stream_handle(hash).await.unwrap();
        let commit_gate = engine.test_hold_commit().await;
        let dispatch_gate = engine.test_hold_dispatch().await;
        let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
        let (ctx, mut close) = oneshot::channel();
        let (ftx, mut fence) = oneshot::channel();
        engine
            .try_close(CloseReq {
                hash,
                generation: Some(1),
                resp: ctx,
            })
            .unwrap();
        engine
            .try_seal_fence(SealFenceReq {
                hash,
                generation: 2,
                resp: ftx,
            })
            .unwrap();
        assert!(matches!(
            close.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            !handle.state.lock().unwrap().applied.closed,
            "nothing applied before write gate"
        );
        drop(commit_gate);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while engaged.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(handle.state.lock().unwrap().applied.closed);
        let remote = slatedb::config::ReadOptions {
            durability_filter: DurabilityLevel::Remote,
            ..Default::default()
        };
        assert!(
            db.get_with_options(tail_key(&hash), &remote)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            close.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            fence.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        store.release_hold();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while db
                .get_with_options(tail_key(&hash), &remote)
                .await
                .unwrap()
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(close.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "remote durability alone does not bypass dispatch"
        );
        assert!(matches!(
            fence.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(dispatch_gate);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), close)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .closed
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), fence)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .closed
        );
        engine.begin_close();
        engine
            .await_terminated(std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let _ = db.close().await;
        let reopened = Db::builder("r03-barriers", store).build().await.unwrap();
        assert!(
            stored_tail(&reopened.get(tail_key(&hash)).await.unwrap().unwrap())
                .unwrap()
                .closed
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r03_failed_group_discards_close_and_fence_effects_together() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("r03-reject", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r03-reject".into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        let hash = [4; 16];
        let (ctx, close) = oneshot::channel();
        let (ftx, fence) = oneshot::channel();
        engine.fail_next_group_for(hash);
        engine
            .commit_group(
                vec![
                    CommitOp::Close(CloseReq {
                        hash,
                        generation: Some(1),
                        resp: ctx,
                    }),
                    CommitOp::SealFence(SealFenceReq {
                        hash,
                        generation: 2,
                        resp: ftx,
                    }),
                ],
                &ShardConfig::default(),
            )
            .await;
        assert!(close.await.unwrap().is_err());
        assert!(fence.await.unwrap().is_err());
        assert!(db.get(tail_key(&hash)).await.unwrap().is_none());
        assert!(
            !engine
                .stream_handle(hash)
                .await
                .unwrap()
                .state
                .lock()
                .unwrap()
                .applied
                .closed
        );
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
mod bounded_outbox_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("r09-outbox", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r09-outbox".into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            Default::default(),
        );
        let mut batch = WriteBatch::new();
        for id in 0..130u64 {
            let mut hash = [0; 16];
            hash[..8].copy_from_slice(&id.to_be_bytes());
            batch.put(crate::billing::usage_dirty_key(&hash), 8u64.to_le_bytes());
        }
        let hash = [0; 16];
        let meta = crate::billing::SegmentBillingMetaV1 {
            v: 1,
            stream_id: "s".into(),
            usage_version: 8,
            month_year: 2026,
            month_month: 7,
            ..Default::default()
        };
        batch.put(
            crate::billing::billing_meta_key(&hash),
            serde_json::to_vec(&meta).unwrap(),
        );
        for n in 0..35u32 {
            batch.put(
                crate::billing::usage_month_final_key(&hash, 2020 + (n / 12) as i32, n % 12 + 1),
                serde_json::to_vec(&meta.to_snapshot(true)).unwrap(),
            );
        }
        db.write(batch).await.unwrap();
        assert!(engine.has_billing_debt().await.unwrap());
        let (first, more) = engine.usage_dirty_page(None, 64).await.unwrap();
        assert!(more);
        assert_eq!(first.len(), 64);
        let (second, more) = engine
            .usage_dirty_page(Some(first.last().unwrap().0), 64)
            .await
            .unwrap();
        assert!(more);
        assert_eq!(second.len(), 64);
        assert!(first.last().unwrap().0 < second[0].0);
        let (last, more) = engine
            .usage_dirty_page(Some(second.last().unwrap().0), 64)
            .await
            .unwrap();
        assert!(!more);
        assert_eq!(last.len(), 2);
        let (finals, more) = engine.usage_month_finals_page(hash, 32).await.unwrap();
        assert!(more);
        assert_eq!(finals.len(), 32);
        engine
            .commit_group(
                vec![CommitOp::UsageAck {
                    hash,
                    scope: UsageAckScope::FinalRowsOnly,
                    month_final_keys: finals.into_iter().map(|(key, _)| key).collect(),
                }],
                &ShardConfig::default(),
            )
            .await;
        let (remaining, more) = engine.usage_month_finals_page(hash, 32).await.unwrap();
        assert!(!more);
        assert_eq!(remaining.len(), 3);
        assert_eq!(
            db.get(crate::billing::usage_dirty_key(&hash))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &8u64.to_le_bytes()
        );
        engine
            .commit_group(
                vec![CommitOp::UsageAck {
                    hash,
                    scope: UsageAckScope::ThroughVersion(8),
                    month_final_keys: remaining.into_iter().map(|(key, _)| key).collect(),
                }],
                &ShardConfig::default(),
            )
            .await;
        assert!(
            db.get(crate::billing::usage_dirty_key(&hash))
                .await
                .unwrap()
                .is_none()
        );
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
mod queue_publication_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r03_queue_refusal_from_staged_generation_shares_group_failure_and_dispatch() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            Db::builder("r03-queue-refusal", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "r03-queue-refusal".into(),
            db.clone(),
            store,
            ShardConfig::default(),
            tx,
            None,
            Default::default(),
        );
        let hash = [37; 16];
        for fail in [true, false] {
            let (created_tx, mut created) = oneshot::channel();
            let (conflict_tx, mut conflict) = oneshot::channel();
            let (close_tx, mut closed) = oneshot::channel();
            if fail {
                engine.fail_next_group_for(hash);
            }
            let dispatch = engine.test_hold_dispatch().await;
            engine
                .commit_group(
                    vec![
                        CommitOp::Queue {
                            hash,
                            op: crate::queue::QueueOp::ConfigPut {
                                consumer: "c".into(),
                                cfg: Default::default(),
                            },
                            resp: created_tx,
                        },
                        CommitOp::Queue {
                            hash,
                            op: crate::queue::QueueOp::ConfigLifecycle {
                                consumer: "c".into(),
                                expect_gen: 2,
                                deleting: true,
                            },
                            resp: conflict_tx,
                        },
                        CommitOp::Close(CloseReq {
                            hash,
                            generation: None,
                            resp: close_tx,
                        }),
                    ],
                    &ShardConfig::default(),
                )
                .await;
            if fail {
                // The conflict was derived from the uncommitted ConfigPut.
                // If the group fails, the consumer generation never existed.
                assert!(
                    created
                        .await
                        .unwrap()
                        .unwrap_err()
                        .contains("group write failed")
                );
                assert!(
                    conflict
                        .await
                        .unwrap()
                        .unwrap_err()
                        .contains("group write failed")
                );
                assert!(matches!(closed.await.unwrap(), Err(AppendErr::Internal(_))));
                assert!(
                    db.get(crate::queue::config_key(&hash, "c"))
                        .await
                        .unwrap()
                        .is_none()
                );
            } else {
                assert!(matches!(
                    created.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                assert!(matches!(
                    conflict.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                assert!(matches!(
                    closed.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                drop(dispatch);
                assert!(
                    tokio::time::timeout(std::time::Duration::from_secs(10), created)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_ok()
                );
                assert!(
                    tokio::time::timeout(std::time::Duration::from_secs(10), conflict)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err()
                        .contains("consumer_generation_conflict")
                );
                assert!(closed.await.unwrap().unwrap().closed);
                continue;
            }
        }
        engine.begin_close();
        let _ = db.close().await;
    }
}

#[cfg(test)]
#[path = "shard/durability_frontier_tests.rs"]
mod durability_frontier_tests;

#[cfg(test)]
mod queue_codec_tests;

#[cfg(test)]
mod record_scan_tests;

#[cfg(test)]
mod read_budget_tests;

#[cfg(test)]
mod task_lifecycle_tests;

#[cfg(test)]
impl ShardEngine {
    pub(crate) fn test_abort_task(&self, role: &str) -> tokio::task::AbortHandle {
        self.tasks.abort(role)
    }
}

#[cfg(test)]
mod transaction_tests;

#[cfg(test)]
mod retirement_tests;
