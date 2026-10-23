//! The committer's operations: every write a shard serializes.
use super::*;

#[expect(
    clippy::large_enum_variant,
    reason = "CommitOp; the append variant carries the request body and reply inline so the committer queue moves one allocation per op; boxing it would add a heap hop to the hottest path"
)]
pub(crate) enum CommitOp {
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
    /// this op finally ran"), zero the gauge and take one version: the
    /// terminal storage observation. A close that would change nothing
    /// is skipped whole. `close_ms <= 0` falls back to billing-now.
    BillingClose {
        hash: [u8; 16],
        close_ms: i64,
        /// Applied once its group is durable, or refused; never dropped
        /// silently.
        resp: BillingReply,
    },
    /// Durable fork-retention flag (round-22 item 7): a soft-deleted
    /// source retained by live forks keeps accruing storage under the
    /// fork billing contract; the flag must survive restarts and
    /// ownership moves on the row itself, not only in emitted
    /// snapshots.
    BillingRetained {
        hash: [u8; 16],
        retained: bool,
        /// Answered like the close's.
        resp: BillingReply,
    },
    /// Queue-profile state transition (PROFILES.md §7): serialized with
    /// appends, durable at the watermark like everything else.
    Queue {
        hash: [u8; 16],
        op: crate::queue::QueueOp,
        resp: oneshot::Sender<Result<crate::queue::QueueOut, String>>,
    },
    /// Native queue operation (docs/QUEUES-DESIGN.md): serialized with
    /// appends, durable at the watermark like everything else.
    NativeQueue {
        hash: [u8; 16],
        op: crate::native_queue::NqOp,
        resp: oneshot::Sender<crate::native_queue::NqResult>,
    },
    /// One gather's worth of absorber confirmations, carried as a SINGLE
    /// committer message so every covered boundary lands in the same
    /// write batch deterministically (the per-stream sends only
    /// coalesced opportunistically). Each entry names the stream, its new
    /// upto and the bytes copied to reach it, retired from the tail's
    /// ledger only from its boundary. Expanded into `Absorbed` ops.
    AbsorbedBatch {
        streams: Vec<AbsorbedAdvance>,
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
    /// (one advance behind; `TailFields::trim_safe_to` says what that covers).
    /// `v2` marks the range as living in the SHARED per-shard partition
    /// (docs/HISTORY-V2.md); the first advancing v2 op sets the stream's
    /// history_v2 flag, which gates the read path's history source. Only
    /// the AbsorbedBatch expansion builds this op, and its one producer
    /// always sets `v2`: the per-stream lane is deleted, and the seal keeps
    /// the tails it absorbed out of the shared partition.
    Absorbed {
        hash: [u8; 16],
        upto: u64,
        /// Copied bytes; retired only when they start at the absorbed boundary.
        bytes: CopiedBytes,
        v2: bool,
    },
    /// TrimTick expansion product (commit_group entry): one stream's
    /// budgeted trim step. Never sent over the channel directly.
    TrimStep {
        hash: [u8; 16],
    },
}
