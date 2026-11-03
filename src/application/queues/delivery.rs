//! Send, claim and settle (docs/QUEUES-DESIGN.md §5.1, §6.1-§6.2, §7.3).
use std::time::Duration;

use serde::Serialize;

use super::envelope::{self, Shown};
use super::wire::{ClaimInput, SendInput, SettleOp, SettleVerb};
use super::{
    FailureClass, Message, QueueFailure, QueueKey, QueueService, QueueTarget, fail, unexpected,
};
use crate::native_queue::{
    DeliveryView, MAX_BATCH, MAX_LEASE_MS, MIN_LEASE_MS, NqOp, NqOut, SendResult, SettleItem,
    SettleKind, SettleOutcome, Token,
};

/// The longest a claim parks for work (§6.1, under the edge's 30 s).
const MAX_WAIT_MS: i64 = 25_000;
const DEFAULT_MAX_BYTES: i64 = 1 << 20;
const MAX_CLAIM_BYTES: i64 = 8 << 20;
/// Settle operations in one request.
pub(crate) const MAX_SETTLE_OPS: usize = 1_000;
const MAX_ERROR_TEXT: usize = 1_024;

/// One send's answer.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Sent {
    pub(crate) id: String,
    pub(crate) enqueued_at: i64,
    pub(crate) available_at: i64,
    pub(crate) duplicate: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Settled {
    token: String,
    outcome: SettleOutcome,
    accepted_late: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Claimed {
    messages: Vec<Message>,
    paused: bool,
    /// `"shard_queue_state"` when the shard's queue state bound leased
    /// nothing new (edge change #246); absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    limited: Option<&'static str>,
}

impl QueueService {
    /// Send `inputs` atomically (§6.6): every message or none.
    pub(crate) async fn send(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        inputs: Vec<SendInput>,
    ) -> Result<Vec<Sent>, QueueFailure> {
        if inputs.is_empty() || inputs.len() > MAX_BATCH {
            return Err(fail(
                FailureClass::Invalid,
                "invalid_message",
                "a batch carries 1 to 1,000 messages",
            ));
        }
        let now = crate::shard::now_ms();
        let mut messages = Vec::with_capacity(inputs.len());
        for input in inputs {
            let env = envelope::envelope_of(input, now)?;
            messages.push(envelope::sealable(key, &env, true)?);
        }
        let results = self.send_sealed(t, key, messages).await?;
        Ok(results.iter().map(|r| sent(t, r)).collect())
    }

    async fn send_sealed(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        messages: Vec<crate::native_queue::SendMessage>,
    ) -> Result<Vec<SendResult>, QueueFailure> {
        // The L0 watermark (`shard::layout::l0_watermark`), as for appends:
        // a send waits while the shard's log nears SlateDB's L0 stall; the
        // consumers' claims and settles, which drain it, stay admitted.
        if let Some(retry) = t.engine.shed_l0_write() {
            return Err(QueueFailure {
                retry_after: Some(retry),
                ..fail(
                    FailureClass::Unavailable,
                    "shard_l0_backpressure",
                    "the shard's log is at its L0 watermark; retry after compaction drains it",
                )
            });
        }
        let op = NqOp::Send {
            messages,
            subkey: key.subkey,
            key_version: 0,
        };
        match self.op(t, op).await? {
            NqOut::Sent(results) => {
                crate::native_queue::wake(&t.hash);
                Ok(results)
            }
            other => Err(unexpected(&other)),
        }
    }

    /// Claim from the default group; with nothing due, park on the
    /// queue's waiter until a send or settle in this process wakes it, at
    /// most 5 s at a time for work that falls due by time, until `waitMs`
    /// (an empty claim writes nothing, so a parked claim costs no store
    /// writes). While it waits the request is parked (`admission::park`),
    /// as a pull's wait is.
    pub(crate) async fn claim(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        input: ClaimInput,
    ) -> Result<Claimed, QueueFailure> {
        let op = self.claim_op(t, &input).await?;
        let wait = input.wait_ms.unwrap_or(0).clamp(0, MAX_WAIT_MS);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait.unsigned_abs());
        let waiter = crate::native_queue::waiter(&t.hash);
        loop {
            // Registered before the claim, so a send between the claim and
            // the wait is not missed.
            let mut woken = std::pin::pin!(waiter.notified());
            woken.as_mut().enable();
            let (deliveries, paused, limited) = match self.op(t, op.clone_claim()).await? {
                NqOut::Claimed {
                    deliveries,
                    paused,
                    limited,
                } => (deliveries, paused, limited),
                other => return Err(unexpected(&other)),
            };
            let now = tokio::time::Instant::now();
            if !deliveries.is_empty() || paused || now >= deadline {
                self.reserve_leased(t, &deliveries).await?;
                let messages = self.leased_messages(t, key, &deliveries).await?;
                return Ok(Claimed {
                    messages,
                    paused,
                    limited: limited.then_some("shard_queue_state"),
                });
            }
            let nap = Duration::from_secs(5).min(deadline - now);
            // Parked while it waits (shared cells M1, layout 6A package 1):
            // out of the write gate's active count, in the live pool and its
            // project's share; a wait that finds no place waits active.
            let _parked = crate::admission::park::park();
            if tokio::time::timeout(nap, woken).await.is_ok() {
                // Let the waking commit's other parked claims race fairly.
                tokio::task::yield_now().await;
            }
        }
    }

    async fn claim_op(&self, t: &QueueTarget, input: &ClaimInput) -> Result<ClaimOp, QueueFailure> {
        let negative = [input.max, input.max_bytes, input.lease_ms, input.wait_ms]
            .iter()
            .any(|v| v.is_some_and(|v| v < 0));
        if negative {
            return Err(fail(
                FailureClass::Invalid,
                "invalid_claim",
                "claim fields are non-negative",
            ));
        }
        let lease_ms = match input.lease_ms {
            Some(ms) => ms,
            None => self.state(t).await?.cfg.group.lease_ms,
        };
        let max = input.max.unwrap_or(10).clamp(1, MAX_BATCH as i64);
        Ok(ClaimOp {
            max: usize::try_from(max).unwrap_or(1),
            max_bytes: input
                .max_bytes
                .unwrap_or(DEFAULT_MAX_BYTES)
                .clamp(1, MAX_CLAIM_BYTES)
                .unsigned_abs(),
            lease_ms: lease_ms.clamp(MIN_LEASE_MS, MAX_LEASE_MS),
        })
    }

    /// Reserve the read memory a claim's leased messages take before their
    /// pages are read (`admission::park::reserve_read`): exactly their bytes,
    /// never the claim's budget, so a claim holds memory only for what it
    /// returns. Without room after the reservation's wait every lease of the
    /// claim is released with its attempt given back and the claim is
    /// refused: 429 `container_memory_pressure` at its project's line, 503
    /// `read_memory_busy` for the instance's read memory, both retryable
    /// after 1 s.
    async fn reserve_leased(
        &self,
        t: &QueueTarget,
        deliveries: &[DeliveryView],
    ) -> Result<(), QueueFailure> {
        let bytes = deliveries.iter().map(|d| u64::from(d.bytes)).sum::<u64>();
        if bytes == 0 {
            return Ok(());
        }
        let Err(refusal) = crate::admission::park::reserve_read(bytes).await else {
            return Ok(());
        };
        let items = deliveries
            .iter()
            .map(|d| internal(d, SettleKind::Unlease))
            .collect();
        give_back(t, items).await?;
        Err(memory_refused(refusal))
    }

    /// The messages of a claim's leases (S3b-DESIGN §6.5). A message whose
    /// envelope will not open fails alone as `unreadable` and the others
    /// are answered. A read the store does not answer gives every lease
    /// back without spending an attempt or a release and answers 503 (§6.1).
    async fn leased_messages(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        deliveries: &[DeliveryView],
    ) -> Result<Vec<Message>, QueueFailure> {
        let shown = Shown {
            token: true,
            ..Shown::default()
        };
        let mut messages = Vec::with_capacity(deliveries.len());
        let mut unreadable = Vec::new();
        for view in deliveries {
            let read = match &view.page {
                Some(raw) => envelope::open(t, key, view.offset, raw.clone())
                    .map(Some)
                    .map_err(envelope::ReadFailure::Unreadable),
                None => envelope::read(t, key, view.offset).await,
            };
            match read {
                Ok(Some(env)) => messages.push(envelope::message(t, view, env, shown)),
                Ok(None) => {}
                Err(envelope::ReadFailure::Unreadable(why)) => {
                    tracing::warn!(offset = view.offset, "queue message unreadable: {why}");
                    unreadable.push(view.offset);
                }
                Err(envelope::ReadFailure::Store(error)) => {
                    give_back(t, unserved(deliveries, &unreadable)).await?;
                    return Err(fail(FailureClass::Unavailable, "claim_read_failed", error));
                }
            }
        }
        if !unreadable.is_empty() {
            let items = deliveries
                .iter()
                .filter(|d| unreadable.contains(&d.offset))
                .map(|d| internal(d, SettleKind::Unreadable))
                .collect();
            give_back(t, items).await?;
        }
        Ok(messages)
    }

    /// Settle by lease token, answering in request order. A token of
    /// another queue or incarnation, or none at all, is `stale`.
    pub(crate) async fn settle(
        &self,
        t: &QueueTarget,
        ops: Vec<SettleOp>,
    ) -> Result<Vec<Settled>, QueueFailure> {
        if ops.len() > MAX_SETTLE_OPS {
            return Err(fail(
                FailureClass::Invalid,
                "invalid_settle",
                "a settle carries at most 1,000 operations",
            ));
        }
        let mut items = Vec::new();
        let mut parsed = Vec::with_capacity(ops.len());
        for op in &ops {
            let token = crate::native_queue::keys::parse_lease_token(&t.hash, &op.token);
            if let Some(token) = token {
                items.push(settle_item(token, op)?);
            }
            parsed.push(token.is_some());
        }
        let results = if items.is_empty() {
            Vec::new()
        } else {
            let settled = self.op(t, NqOp::Settle { items }).await?;
            // A retry or release makes work claimable again.
            crate::native_queue::wake(&t.hash);
            match settled {
                NqOut::Settled(results) => results,
                other => return Err(unexpected(&other)),
            }
        };
        let mut results = results.into_iter();
        Ok(ops
            .into_iter()
            .zip(parsed)
            .map(|(op, ok)| {
                let r = ok.then(|| results.next()).flatten();
                Settled {
                    token: op.token,
                    outcome: r.as_ref().map_or(SettleOutcome::Stale, |r| r.outcome),
                    accepted_late: r.is_some_and(|r| r.accepted_late),
                }
            })
            .collect())
    }

    /// Republish (§7.3): each named message's envelope, read with the
    /// key, sent again as a new message with its lineage; the dedup index
    /// is neither checked nor written, and the new message is due now.
    pub(crate) async fn republish(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        ids: &[String],
    ) -> Result<Vec<(String, String)>, QueueFailure> {
        let mut from = Vec::new();
        let mut messages = Vec::new();
        for offset in t.offsets(ids) {
            // A message whose envelope will not open cannot be republished:
            // it is left out of the answer, as a reclaimed one is.
            let env = match envelope::read(t, key, offset).await {
                Ok(env) => env,
                Err(envelope::ReadFailure::Unreadable(_)) => None,
                Err(e @ envelope::ReadFailure::Store(_)) => {
                    return Err(fail(
                        FailureClass::Unavailable,
                        "read_failed",
                        e.to_string(),
                    ));
                }
            };
            let Some(mut env) = env else {
                continue;
            };
            let id = t.id(offset);
            env.lineage.origin_id =
                Some(env.lineage.origin_id.take().unwrap_or_else(|| id.clone()));
            env.lineage.republished_from = Some(id.clone());
            env.deliver_at = None;
            messages.push(envelope::sealable(key, &env, false)?);
            from.push(id);
        }
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let results = self.send_sealed(t, key, messages).await?;
        Ok(from
            .into_iter()
            .zip(results.iter().map(|r| t.id(r.offset)))
            .collect())
    }
}

fn sent(t: &QueueTarget, r: &SendResult) -> Sent {
    Sent {
        id: t.id(r.offset),
        enqueued_at: r.enqueued_at,
        available_at: r.available_at,
        duplicate: r.duplicate,
    }
}

/// A claim's committer arguments, reissued on each long-poll round.
struct ClaimOp {
    max: usize,
    max_bytes: u64,
    lease_ms: i64,
}

impl ClaimOp {
    fn clone_claim(&self) -> NqOp {
        NqOp::Claim {
            max: self.max,
            max_bytes: self.max_bytes,
            lease_ms: self.lease_ms,
        }
    }
}

/// A claim refused for read memory, as a page read is refused.
fn memory_refused(refusal: crate::admission::read_memory::ReadRefusal) -> QueueFailure {
    use crate::admission::read_memory::ReadRefusal;
    let failure = match refusal {
        ReadRefusal::Project => fail(
            FailureClass::Exhausted,
            "container_memory_pressure",
            "the container's estimated memory pressure is over its per-container backstop; retry",
        ),
        ReadRefusal::Instance => fail(
            FailureClass::Unavailable,
            "read_memory_busy",
            "the instance's read memory is held by other reads; retry",
        ),
    };
    QueueFailure {
        retry_after: Some(1),
        ..failure
    }
}

/// Settle a claim's leases the server could not serve, waiting for room in
/// the committer's queue (`ShardEngine::submit_native_queue`): the claim's
/// last act before it answers, which must not be refused `Busy` after its
/// claim was committed, or the leases would stay held until they expire
/// under an answer that says nothing was written.
async fn give_back(t: &QueueTarget, items: Vec<SettleItem>) -> Result<(), QueueFailure> {
    t.engine
        .submit_native_queue(t.hash, NqOp::Settle { items })
        .await?;
    Ok(())
}

/// Every lease of a claim the server could not finish serving: those whose
/// envelope would not open fail, the rest are given back.
fn unserved(deliveries: &[DeliveryView], unreadable: &[u64]) -> Vec<SettleItem> {
    let kind = |d: &DeliveryView| {
        if unreadable.contains(&d.offset) {
            SettleKind::Unreadable
        } else {
            SettleKind::Unlease
        }
    };
    deliveries.iter().map(|d| internal(d, kind(d))).collect()
}

/// An internal settle of a lease the server could not serve: `Unlease`
/// after a transient failure of its own, `Unreadable` for an envelope that
/// will not open. Neither spends the customer's attempts or releases.
fn internal(view: &DeliveryView, kind: SettleKind) -> SettleItem {
    SettleItem {
        token: Token {
            offset: view.offset,
            lease_gen: view.lease_gen,
        },
        kind,
        code: None,
    }
}

/// One operation for the committer: `code` is at most 64 characters of
/// `[a-z0-9_.-]`, `error` text at most 1 KiB (§6.2).
fn settle_item(token: Token, op: &SettleOp) -> Result<SettleItem, QueueFailure> {
    let invalid = |m: &str| fail(FailureClass::Invalid, "invalid_settle", m);
    if op.code.as_deref().is_some_and(|c| !valid_code(c)) {
        return Err(invalid("code is at most 64 characters of [a-z0-9_.-]"));
    }
    if op.error.as_ref().is_some_and(|e| e.len() > MAX_ERROR_TEXT) {
        return Err(invalid("error text is at most 1 KiB"));
    }
    if op.delay_ms.is_some_and(|d| d < 0) {
        return Err(invalid("delayMs must not be negative"));
    }
    let kind = match op.op {
        SettleVerb::Ack => SettleKind::Ack,
        SettleVerb::Retry => SettleKind::Retry {
            delay_ms: op.delay_ms,
        },
        SettleVerb::Extend => {
            let Some(lease_ms) = op.lease_ms else {
                return Err(invalid("extend carries leaseMs"));
            };
            SettleKind::Extend {
                lease_ms: lease_ms.clamp(MIN_LEASE_MS, MAX_LEASE_MS),
            }
        }
        SettleVerb::Fail => SettleKind::Fail,
        SettleVerb::Discard => SettleKind::Discard,
        SettleVerb::Release => SettleKind::Release,
    };
    Ok(SettleItem {
        token,
        kind,
        code: op.code.clone(),
    })
}

fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-')
        })
}
