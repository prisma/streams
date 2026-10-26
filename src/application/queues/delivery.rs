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
            let (deliveries, paused) = match self.op(t, op.clone_claim()).await? {
                NqOut::Claimed { deliveries, paused } => (deliveries, paused),
                other => return Err(unexpected(&other)),
            };
            let now = tokio::time::Instant::now();
            if !deliveries.is_empty() || paused || now >= deadline {
                let messages = self.leased_messages(t, key, &deliveries).await?;
                return Ok(Claimed { messages, paused });
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

    /// The messages of a claim's leases. A read that fails releases every
    /// lease of the claim with its attempt given back and answers 503
    /// (§6.1).
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
        for view in deliveries {
            let read = match &view.page {
                Some(raw) => envelope::open(t, key, view.offset, raw.clone()).map(Some),
                None => envelope::read(t, key, view.offset).await,
            };
            match read {
                Ok(Some(env)) => messages.push(envelope::message(t, view, env, shown)),
                Ok(None) => {}
                Err(error) => {
                    let items = deliveries
                        .iter()
                        .map(|d| release(d.offset, d.lease_gen))
                        .collect();
                    give_back(t, items).await?;
                    return Err(fail(FailureClass::Unavailable, "claim_read_failed", error));
                }
            }
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
            let Some(mut env) = envelope::read(t, key, offset)
                .await
                .map_err(|e| fail(FailureClass::Unavailable, "read_failed", e))?
            else {
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

/// Release a claim's leases with their attempts given back, waiting for
/// room in the committer's queue (`ShardEngine::submit_native_queue`): the
/// claim's last act before it answers, which must not be refused `Busy`
/// after its claim was committed, or the leases would stay held until they
/// expire under an answer that says nothing was written.
async fn give_back(t: &QueueTarget, items: Vec<SettleItem>) -> Result<(), QueueFailure> {
    t.engine
        .submit_native_queue(t.hash, NqOp::Settle { items })
        .await?;
    Ok(())
}

fn release(offset: u64, lease_gen: u32) -> SettleItem {
    SettleItem {
        token: Token { offset, lease_gen },
        kind: SettleKind::Release,
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
