//! The DST reference model and oracle: what the workload believes it
//! did (`OpLog`), the audit over what a reader drained, and the
//! production-path read oracle. Split out of the dst catch-all
//! (PR 3.2.1).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{Coverage, mech};

// ---- the reference model --------------------------------------------

/// Identity of one *attempt* at one logical client operation.
pub type AttemptId = (u64, u32);

/// Terminal state of one attempt, as the client would classify it.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Durably acknowledged, with the offset the server reported.
    Acked { last_offset: u64, duplicate: bool },
    /// The server decided against it before committing anything.
    Rejected,
    /// The request may or may not have committed: no response, or an
    /// ambiguous fencing error.
    Unknown,
}

/// What the workload believes it did.
#[derive(Default, Debug)]
pub struct OpLog {
    /// Per routing key, attempts that were acknowledged, in ack order.
    pub acked: HashMap<String, Vec<AttemptId>>,
    /// Attempts the server definitively rejected: they must never appear.
    pub rejected: HashSet<AttemptId>,
    /// Attempts with an unresolved outcome: absent or present are both
    /// legal, twice is not.
    pub unknown: HashSet<AttemptId>,
    /// Logical operations driven with producer idempotence: across all of
    /// an operation's attempts, at most one may be stored.
    pub idempotent: HashSet<u64>,
    /// Every attempt the workload ISSUED (whatever its outcome). An
    /// observed record that belongs to no issued attempt is a fabrication
    /// — a class the old audit tolerated because it only checked that
    /// acked attempts were present, never that present attempts were
    /// issued.
    pub issued: HashSet<AttemptId>,
    /// Offset reported by the server for each acked attempt, so a read can
    /// be checked against what the client was told, not just for presence.
    pub acked_offsets: HashMap<AttemptId, u64>,
}

impl OpLog {
    pub fn total_acked(&self) -> usize {
        self.acked.values().map(|v| v.len()).sum()
    }

    fn all_acked(&self) -> HashSet<AttemptId> {
        self.acked.values().flatten().copied().collect()
    }

    /// Audit what a reader actually drained. `observed` is per routing key,
    /// in read order.
    pub fn audit(&self, observed: &HashMap<String, Vec<AttemptId>>) -> Result<(), String> {
        // The ledger must be self-consistent, or a harness bug could
        // silently weaken every check below.
        let acked = self.all_acked();
        if let Some(a) = self.rejected.intersection(&acked).next() {
            return Err(format!(
                "harness bug: op{}#{} recorded as both acked and rejected",
                a.0, a.1
            ));
        }

        let mut seen_count: HashMap<AttemptId, usize> = HashMap::new();
        for attempts in observed.values() {
            for a in attempts {
                *seen_count.entry(*a).or_insert(0) += 1;
            }
        }

        // I7: every observed record belongs to an issued attempt. Only
        // enforced when the workload actually tracked issuance, so hand-
        // built oracle unit tests stay valid.
        if !self.issued.is_empty()
            && let Some(a) = seen_count.keys().find(|a| !self.issued.contains(a))
        {
            return Err(format!(
                "I7 violated: op{}#{} is readable but was never issued",
                a.0, a.1
            ));
        }

        // I3: nothing is stored twice.
        if let Some((a, n)) = seen_count.iter().find(|(_, n)| **n > 1) {
            return Err(format!(
                "I3 violated: attempt op{}#{} stored {n} times",
                a.0, a.1
            ));
        }

        // I5: a definitively rejected attempt never appears.
        if let Some(a) = self.rejected.iter().find(|a| seen_count.contains_key(a)) {
            return Err(format!(
                "I5 violated: op{}#{} was rejected but is readable",
                a.0, a.1
            ));
        }

        // I1 + I2, per key.
        for (key, acked) in &self.acked {
            let seen = observed.get(key).cloned().unwrap_or_default();
            for a in acked {
                if !seen.contains(a) {
                    return Err(format!(
                        "I1 violated: key {key} acked op{}#{} but it is not readable",
                        a.0, a.1
                    ));
                }
            }
            let mut it = seen.iter();
            for want in acked {
                if !it.any(|got| got == want) {
                    return Err(format!(
                        "I2 violated: key {key} op{}#{} out of acknowledged order",
                        want.0, want.1
                    ));
                }
            }
        }

        // I6: an idempotent operation commits at most once, however many
        // times its client retried.
        for op in &self.idempotent {
            let stored: Vec<AttemptId> = seen_count
                .keys()
                .copied()
                .filter(|(o, _)| o == op)
                .collect();
            if stored.len() > 1 {
                return Err(format!(
                    "I6 violated: idempotent op{op} stored {} times ({stored:?})",
                    stored.len()
                ));
            }
        }
        Ok(())
    }
}

// ---- workload --------------------------------------------------------

/// Drives logical client operations, with retries, against a real engine.
pub struct Workload {
    next_op: u64,
    /// Next producer sequence per routing key. Producer sequences are
    /// per-(producer id) and must start at 0 and be contiguous — an epoch
    /// bump with a non-zero sequence is rejected outright
    /// (`AppendErr::ProducerEpochSeq`). A retry reuses the SAME sequence:
    /// that reuse is what makes it idempotent.
    producer_seq: HashMap<String, u64>,
    /// Attempts per logical operation before the client gives up.
    pub max_attempts: u32,
    pub coverage: Arc<Coverage>,
}

impl Workload {
    pub fn new(coverage: Arc<Coverage>) -> Self {
        Workload {
            next_op: 1,
            producer_seq: HashMap::new(),
            max_attempts: 3,
            coverage,
        }
    }

    /// One attempt, classified as the client would classify it.
    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        op: u64,
        attempt: u32,
        producer: Option<crate::shard::ProducerReq>,
    ) -> Outcome {
        use crate::shard::{AppendErr, AppendReq};
        let payload = serde_json::json!({ "op": op, "att": attempt, "k": rk }).to_string();
        let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(payload.into_bytes())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_hash: crate::crypto::stream_hash(rk),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            seal_fence_to: None,
            billing: None,
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            // Never entered the engine: definitely not committed.
            return Outcome::Rejected;
        }
        match rx.await {
            Ok(Ok(ack)) => Outcome::Acked {
                last_offset: ack.last_offset,
                duplicate: ack.duplicate,
            },
            // The engine reached a decision before committing anything.
            Ok(Err(
                AppendErr::SeqConflict { .. }
                | AppendErr::ProducerSeqReused
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
                | AppendErr::SealSuperseded
                | AppendErr::CtMismatch
                | AppendErr::BadBody(_),
            )) => Outcome::Rejected,
            // Fenced, closed, or failed mid-flight: the write may or may
            // not have landed. Exactly the state the soak wedge produced.
            Ok(Err(AppendErr::Moved | AppendErr::Closed { .. } | AppendErr::Internal(_))) => {
                Outcome::Unknown
            }
            // Responder dropped: the request's fate is unobservable.
            Err(_) => Outcome::Unknown,
        }
    }

    /// One attempt with an explicit producer identity and an optional
    /// **client deadline** — the public boundary. A deadline that expires
    /// leaves the server's append running and yields `Unknown`, which is
    /// exactly the operational shape storage faults produce (slow, not
    /// failed). Returns the raw outcome; the caller owns the ledger.
    #[allow(clippy::too_many_arguments)]
    pub async fn attempt_with_deadline(
        &self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        body: &str,
        producer: Option<crate::shard::ProducerReq>,
        deadline: Option<std::time::Duration>,
    ) -> Outcome {
        use crate::shard::{AppendErr, AppendReq};
        let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(body.as_bytes().to_vec())],
            usage: crate::usage::counters(&hash),
            routing_key: rk.to_string(),
            key_hash: crate::crypto::stream_hash(rk),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            close: false,
            producer,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            seal_fence_to: None,
            billing: None,
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            return Outcome::Rejected;
        }
        let got = match deadline {
            Some(d) => match tokio::time::timeout(d, rx).await {
                Ok(r) => r,
                Err(_) => {
                    // The client stopped waiting. The append is still
                    // running server-side: this is the ambiguity.
                    self.coverage.hit(mech::CLIENT_DEADLINE_EXPIRED);
                    self.coverage.hit(mech::APPEND_UNKNOWN);
                    return Outcome::Unknown;
                }
            },
            None => rx.await,
        };
        match got {
            Ok(Ok(ack)) => {
                if ack.duplicate {
                    self.coverage.hit(mech::PRODUCER_DUPLICATE);
                }
                self.coverage.hit(mech::APPEND_ACKED);
                Outcome::Acked {
                    last_offset: ack.last_offset,
                    duplicate: ack.duplicate,
                }
            }
            Ok(Err(
                AppendErr::SeqConflict { .. }
                | AppendErr::ProducerSeqReused
                | AppendErr::ProducerGap { .. }
                | AppendErr::ProducerStale { .. }
                | AppendErr::ProducerEpochSeq
                | AppendErr::SealSuperseded
                | AppendErr::CtMismatch
                | AppendErr::BadBody(_),
            )) => {
                self.coverage.hit(mech::APPEND_REJECTED);
                Outcome::Rejected
            }
            Ok(Err(AppendErr::Moved | AppendErr::Closed { .. } | AppendErr::Internal(_))) => {
                self.coverage.hit(mech::APPEND_UNKNOWN);
                Outcome::Unknown
            }
            Err(_) => {
                self.coverage.hit(mech::APPEND_UNKNOWN);
                Outcome::Unknown
            }
        }
    }

    /// One logical operation, retried like a production client: a retry
    /// after an unknown outcome is a NEW attempt of the SAME operation.
    ///
    /// With `idempotent`, every attempt carries the same producer sequence,
    /// so the engine must suppress the duplicate (I6). Without it, a retry
    /// may legitimately commit twice — which is exactly why the oracle
    /// tracks operations rather than payloads.
    pub async fn append(
        &mut self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        idempotent: bool,
        log: &mut OpLog,
    ) -> Outcome {
        self.append_to(&[engine], hash, key, rk, idempotent, log)
            .await
    }

    /// One logical operation, failing over across owners.
    ///
    /// Attempt `i` goes to `engines[min(i, len-1)]`, which is what a client
    /// following `Streams-Replay-To` does after a shard moves: same logical
    /// operation, same producer sequence, new owner. The retry is only
    /// idempotent if producer state survived the handoff — which is the
    /// property this exists to test.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_to(
        &mut self,
        engines: &[&Arc<crate::shard::ShardEngine>],
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        rk: &str,
        idempotent: bool,
        log: &mut OpLog,
    ) -> Outcome {
        let op = self.next_op;
        self.next_op += 1;
        let pseq = if idempotent {
            log.idempotent.insert(op);
            let e = self.producer_seq.entry(rk.to_string()).or_insert(0);
            let s = *e;
            *e += 1;
            s
        } else {
            0
        };
        let mut last = Outcome::Unknown;
        for attempt in 0..self.max_attempts {
            if attempt > 0 {
                self.coverage.hit(mech::APPEND_RETRIED);
            }
            let producer = idempotent.then(|| crate::shard::ProducerReq {
                id: format!("dst-producer-{rk}"),
                epoch: 1,
                seq: pseq,
                request_hash: None,
            });
            let engine = engines[(attempt as usize).min(engines.len() - 1)];
            log.issued.insert((op, attempt));
            last = self
                .attempt(engine, hash, key, rk, op, attempt, producer)
                .await;
            match &last {
                Outcome::Acked {
                    duplicate,
                    last_offset,
                } => {
                    log.acked_offsets.insert((op, attempt), *last_offset);
                    if *duplicate {
                        self.coverage.hit(mech::PRODUCER_DUPLICATE);
                    }
                    self.coverage.hit(mech::APPEND_ACKED);
                    log.acked
                        .entry(rk.to_string())
                        .or_default()
                        .push((op, attempt));
                    return last;
                }
                Outcome::Rejected => {
                    self.coverage.hit(mech::APPEND_REJECTED);
                    log.rejected.insert((op, attempt));
                    return last;
                }
                Outcome::Unknown => {
                    self.coverage.hit(mech::APPEND_UNKNOWN);
                    log.unknown.insert((op, attempt));
                    // retry
                }
            }
        }
        last
    }

    /// `per_key` operations for each routing key.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &mut self,
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        key: &crate::crypto::StreamKey,
        routing_keys: &[&str],
        per_key: u64,
        idempotent: bool,
        log: &mut OpLog,
    ) {
        for _ in 0..per_key {
            for rk in routing_keys {
                self.append(engine, hash, key, rk, idempotent, log).await;
            }
        }
    }
}

// ---- reader ----------------------------------------------------------

/// Read everything back **through the production merged reader**
/// (`http::read_merged`): history tier for `[0, absorbed)`, shard log for
/// `[absorbed, next)`.
///
/// Reimplementing that boundary here would mean the oracle tests a copy of
/// the read path rather than the read path, and a copy is free to drift.
/// One history-reader service per store, defaults suitable for
/// correctness scenarios. Budget scenarios construct their own (pinned
/// poll, chosen cap) and hold it across reads.
pub async fn drain_observed(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    coverage: &Coverage,
) -> HashMap<String, Vec<AttemptId>> {
    let mut out: HashMap<String, Vec<AttemptId>> = HashMap::new();
    let Ok(handle) = engine.stream_handle(hash).await else {
        return out;
    };
    if handle.state.lock().unwrap().durable.absorbed > 0 {
        coverage.hit(mech::READ_FROM_HISTORY);
    }
    let mut from = 0u64;
    // Bounded: each pass must advance `from`, and the loop stops the first
    // time it cannot.
    for _ in 0..1024 {
        let res = match crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            None,
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        {
            Ok(r) => r,
            Err(_) => return out,
        };
        if std::env::var("DST_DRAIN_TRACE").is_ok() {
            let offs: Vec<u64> = res.recs.iter().map(|r| r.off).collect();
            eprintln!(
                "DRAIN from={from} n={} last={:?} completed={} end={} offs={offs:?}",
                res.recs.len(),
                res.last,
                res.completed,
                res.end
            );
        }
        for rec in &res.recs {
            let Ok(v) = serde_json::from_slice::<serde_json::Value>(&rec.payload) else {
                continue;
            };
            let (Some(op), Some(att), Some(k)) = (
                v.get("op").and_then(|x| x.as_u64()),
                v.get("att").and_then(|x| x.as_u64()),
                v.get("k").and_then(|x| x.as_str()),
            ) else {
                continue;
            };
            out.entry(k.to_string()).or_default().push((op, att as u32));
        }
        if res.completed {
            return out;
        }
        // An incomplete page that made no progress is a transient: the
        // reader raced the absorbed boundary (an honest empty page asks
        // the caller to re-poll, and read_merged only says `completed`
        // when the page really reached `end`). Retry — the pass bound
        // above keeps a genuinely wedged engine from hanging the oracle,
        // and the audit then reports the missing records honestly.
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            _ => {}
        }
    }
    out
}
