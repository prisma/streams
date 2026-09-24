//! Active consumer delivery, lease settlement and durable dead-letter handoff.
use super::{
    AuthorizedConsumerContext, AuthorizedStreamContext, ConsumerFailure, ConsumerService,
    DeliveryMessage, FailureClass, PullInput, PullOutcome, SettleInput, SettleOutcome,
    consumer_segments, failure,
};
use crate::application::append::AppendCode;
use crate::application::consumer_remote::relay_queue_cursor;
use crate::application::read_remote::InternalTarget;
use crate::registry::StreamDesc;
use bytes::Bytes;
use serde_json::value::RawValue;
use std::sync::Arc;

#[expect(
    clippy::too_many_lines,
    reason = "pull; the pull walks the lineage oldest-first and each segment's skip, stop and deliver verdicts depend on the walk so far; splitting it would separate the verdicts from the walk that orders them"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "pull; the walk nests each foreign segment's drain probe and each sealed segment's cursor check inside the lineage loop; flattening it would separate the skip from the segment it skips"
)]
pub(crate) async fn pull(
    context: AuthorizedConsumerContext,
    doc: PullInput,
) -> Result<PullOutcome, ConsumerFailure> {
    let AuthorizedConsumerContext {
        stream:
            AuthorizedStreamContext {
                service: state,
                desc,
                key: skey,
                epoch,
                consumer: cname,
            },
        record,
    } = context;
    let cgen = record.generation;
    let cfg = record.config;
    let max = doc
        .max
        .unwrap_or(cfg.max_batch_records as usize)
        .clamp(1, cfg.max_batch_records as usize);
    let visibility = crate::queue::visibility_window_ms(doc.visibility_ms, &cfg);
    let wait = doc.wait_ms.unwrap_or(0).min(25_000);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait);

    let lineage = consumer_segments(&desc);

    'outer: loop {
        // Walk the lineage oldest-first. A sealed, fully-settled
        // segment is skipped; a sealed segment with backlog STOPS the
        // walk (strict predecessor-first — successors of an undrained
        // predecessor never deliver); an empty LIVE segment yields to
        // its siblings (split leaves hold disjoint key ranges, so no
        // ordering constraint exists between them).
        let mut total_backlog = 0u64;
        for (seg_id, identity, route, sealed_end) in lineage.iter().copied() {
            let engine = match state.engine_for(&route).await {
                Ok(e) => e,
                Err(r) => {
                    // Cross-owner pull: a FOREIGN drained predecessor or
                    // empty live sibling must not stop the walk — probe
                    // its cursor/tail on the owner and skip past it. A
                    // foreign segment with deliverable backlog keeps the
                    // ownership 409 (leases are owner-local; the router
                    // replays the pull to the owner, which now skips OUR
                    // segments the same way — converges).
                    if foreign_segment_drained(
                        &state,
                        &desc,
                        seg_id,
                        &cname,
                        cgen,
                        sealed_end,
                        r.owner.as_deref(),
                    )
                    .await
                    {
                        continue;
                    }
                    return Err(r);
                }
            };
            if let Some(end) = sealed_end {
                let cursor = engine
                    .queue_cursor(identity, &cname, cgen)
                    .await
                    .map_err(|m| {
                        failure(
                            FailureClass::Unavailable,
                            "queue_unavailable",
                            &m.to_string(),
                            None,
                            true,
                        )
                    })?;
                if cursor >= end {
                    continue; // drained predecessor
                }
            }
            let DeliveryCoverage {
                keys_map,
                by_off,
                covered_to,
            } = read_coverage(
                DeliveryRead {
                    state: &state,
                    key: &skey,
                    epoch,
                    consumer: &cname,
                    generation: cgen,
                },
                &engine,
                identity,
            )
            .await?;
            #[cfg(test)]
            crate::failpoints::pause_pull_before_receive(&desc.name).await;
            let qout = engine
                .submit_queue(
                    identity,
                    crate::queue::QueueOp::Receive {
                        consumer: cname.clone(),
                        cgen,
                        max,
                        visibility_ms: visibility,
                        max_deliveries: cfg.max_attempts,
                        keys: keys_map,
                        covered_to,
                    },
                )
                .await;
            let (leased, backlog, poisoned) = match qout {
                Ok(crate::queue::QueueOut::Received {
                    leased,
                    backlog,
                    poisoned,
                }) => (leased, backlog, poisoned),
                Ok(_) => unreachable!("receive answers Received"),
                Err(m) if m.starts_with("consumer_not_found") => {
                    return Err(failure(
                        FailureClass::Missing,
                        "consumer_not_found",
                        &m,
                        None,
                        false,
                    ));
                }
                Err(m) if m.starts_with("consumer_generation_fenced") => {
                    return Err(failure(
                        FailureClass::Conflict,
                        "consumer_deleted",
                        &m,
                        None,
                        false,
                    ));
                }
                Err(m) => {
                    return Err(failure(FailureClass::Internal, "internal", &m, None, true));
                }
            };
            // Poison blocks only its own key: the leases this Receive granted
            // are delivered whether or not the handoff settled anything.
            let handoff = dlq_and_settle(
                &state, &desc, &cfg, cgen, &cname, &skey, &epoch, identity, &engine, seg_id,
                &poisoned, &by_off,
            )
            .await;
            if !leased.is_empty() {
                let now = crate::shard::now_ms();
                let messages = delivery_messages(
                    MessageContext {
                        desc: &desc,
                        key: &skey,
                        epoch,
                        segment: seg_id,
                        generation: cgen,
                        deadline_ms: now + visibility as i64,
                    },
                    &leased,
                    &by_off,
                )?;
                let delivered_payload: u64 = leased
                    .iter()
                    .filter_map(|(off, ..)| by_off.get(off))
                    .map(|(_, p)| p.len() as u64)
                    .sum();
                return Ok(PullOutcome {
                    messages,
                    backlog: total_backlog + backlog,
                    payload_bytes: delivered_payload,
                    descriptor: desc,
                });
            }
            if handoff.settled > 0 {
                continue 'outer; // settled poison may have unblocked keys
            }
            total_backlog += backlog;
            if sealed_end.is_some() && backlog > 0 {
                // Undrained sealed predecessor (all remaining records
                // leased/blocked): successors must wait.
                break;
            }
        }
        if tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'outer;
        }
        return Ok(PullOutcome {
            messages: Vec::new(),
            backlog: total_backlog,
            payload_bytes: 0,
            descriptor: desc,
        });
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "foreign_segment_drained; the probe takes the descriptor, segment, consumer, generation, sealed end and owner separately as the walk resolved them; a probe struct would exist only for this signature"
)]
async fn foreign_segment_drained(
    state: &Arc<ConsumerService>,
    desc: &StreamDesc,
    segment: u32,
    consumer: &str,
    generation: u64,
    sealed_end: Option<u64>,
    owner: Option<&str>,
) -> bool {
    let Some(base) = owner.and_then(|owner| state.peer.url_for(owner)) else {
        return false;
    };
    let Some(target) = InternalTarget::of(desc, segment) else {
        return false;
    };
    let Some((cursor, tail)) =
        relay_queue_cursor(state, &base, &desc.sref(), &target, consumer, generation).await
    else {
        return false;
    };
    match sealed_end {
        Some(end) => cursor >= end,
        None => tail <= cursor,
    }
}

/// Encode only leases granted by the committer, with the same key, stream
/// epoch, segment and consumer generation used for the durable Receive.
#[derive(Clone, Copy)]
struct MessageContext<'a> {
    desc: &'a StreamDesc,
    key: &'a crate::crypto::StreamKey,
    epoch: [u8; 16],
    segment: u32,
    generation: u64,
    deadline_ms: i64,
}
fn delivery_messages(
    context: MessageContext<'_>,
    leased: &[(u64, u32, u32, [u8; 16])],
    by_off: &DeliveryRecords,
) -> Result<Vec<DeliveryMessage>, ConsumerFailure> {
    let MessageContext {
        desc,
        key: skey,
        epoch,
        segment: seg_id,
        generation: cgen,
        deadline_ms,
    } = context;
    let mut messages = Vec::with_capacity(leased.len());
    for (off, lease_gen, attempts, kh) in leased {
        let Some((rkey, payload)) = by_off.get(off) else {
            continue;
        };
        let msg = crate::product_cursor::MessageId {
            epoch,
            key_hash: *kh,
            seg_id,
            offset: *off,
        };
        let lease = crate::product_cursor::LeaseToken {
            msg: msg.clone(),
            lease_gen: *lease_gen,
            consumer_gen: cgen,
            deadline_ms,
        };
        let value = served_value(desc, payload).map_err(|error| {
            let message = format!("the record at offset {off} cannot be served: {error}");
            failure(FailureClass::Internal, "internal", &message, None, false)
        })?;
        messages.push(DeliveryMessage {
            id: msg.encode(&desc.project_id, skey),
            routing_key: rkey.to_owned(),
            attempts: *attempts,
            lease_token: lease.encode(&desc.project_id, skey),
            value,
        });
    }
    Ok(messages)
}

/// The value a consumer surface serves for a stored payload: a JSON
/// collection's stored record text unchanged (validated when it was
/// stored), any other collection's bytes as a base64 string. Stored bytes
/// that are not JSON are an explicit error, never a `null`.
fn served_value(desc: &StreamDesc, payload: &[u8]) -> serde_json::Result<Box<RawValue>> {
    if desc.is_json() {
        serde_json::from_slice(payload)
    } else {
        use base64::Engine;
        serde_json::value::to_raw_value(&base64::engine::general_purpose::STANDARD.encode(payload))
    }
}

/// The dead-letter copy of one message. The fields are in the order the
/// envelope has always had (serde_json's sorted map), and `value` is the
/// source's stored text embedded unparsed, so the copy's value is
/// byte-identical to the source record and the request hash over this body
/// depends on stored bytes alone, never on a parser or formatter.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DeadLetterCopy<'a> {
    attempts: u32,
    consumer: &'a str,
    message_id: &'a str,
    routing_key: &'a str,
    source_stream: &'a str,
    value: Box<RawValue>,
}

/// A durable read pins the stream key/epoch and consumer generation used by Receive.
struct DeliveryRead<'a> {
    state: &'a ConsumerService,
    key: &'a crate::crypto::StreamKey,
    epoch: [u8; 16],
    consumer: &'a str,
    generation: u64,
}
#[derive(Default)]
struct DeliveryRecords {
    batch: crate::application::read::PlainBatch,
    positions: std::collections::HashMap<u64, usize>,
}
impl DeliveryRecords {
    fn new(batch: crate::application::read::PlainBatch) -> Self {
        let positions = batch
            .iter()
            .enumerate()
            .map(|(index, record)| (record.off, index))
            .collect();
        Self { batch, positions }
    }
    fn get(&self, off: &u64) -> Option<(&str, &[u8])> {
        let record = &self.batch[*self.positions.get(off)?];
        Some((&record.rkey, &record.payload))
    }
}

struct DeliveryCoverage {
    keys_map: std::collections::HashMap<u64, [u8; 16]>,
    by_off: DeliveryRecords,
    covered_to: u64,
}
async fn read_coverage(
    input: DeliveryRead<'_>,
    engine: &Arc<crate::shard::ShardEngine>,
    identity: [u8; 16],
) -> Result<DeliveryCoverage, ConsumerFailure> {
    let DeliveryRead {
        state,
        key: skey,
        epoch,
        consumer: cname,
        generation: cgen,
    } = input;
    let handle = match engine.stream_handle(identity).await {
        Ok(h) => h,
        Err(e) => {
            return Err(failure(
                FailureClass::Internal,
                "internal",
                &e.to_string(),
                None,
                true,
            ));
        }
    };
    state.keys.put(identity, skey.clone(), epoch);
    let cursor = engine
        .queue_cursor(identity, cname, cgen)
        .await
        .map_err(|m| {
            failure(
                FailureClass::Unavailable,
                "queue_unavailable",
                &m.to_string(),
                None,
                true,
            )
        })?;
    let out = match crate::application::read::read_merged(
        skey,
        &epoch,
        &handle,
        engine,
        cursor,
        None,
        4 << 20,
        crate::shard::Deliver::Durable,
    )
    .await
    {
        Ok(o) => o,
        Err(m) => {
            return Err(failure(FailureClass::Internal, "internal", &m, None, true));
        }
    };
    let mut keys_map: std::collections::HashMap<u64, [u8; 16]> = Default::default();
    let mut covered_to = cursor;
    for r in &out.recs {
        keys_map.insert(r.off, crate::crypto::stream_hash(&r.rkey));
        covered_to = covered_to.max(r.off + 1);
    }
    Ok(DeliveryCoverage {
        keys_map,
        by_off: DeliveryRecords::new(out.recs),
        covered_to,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "settle; settlement decides the acknowledged, released, dead-lettered and expired outcomes against one durable Receive; splitting it would separate the outcomes from the leases they settle"
)]
pub(crate) async fn settle(
    context: AuthorizedConsumerContext,
    doc: SettleInput,
) -> Result<SettleOutcome, ConsumerFailure> {
    let AuthorizedConsumerContext {
        stream:
            AuthorizedStreamContext {
                service: state,
                desc,
                key: skey,
                epoch,
                consumer: cname,
            },
        record,
    } = context;
    let cgen = record.generation;
    let cfg = record.config;
    // Tokens name their segment: group per segment, one committer
    // settle each. Invalid or foreign tokens are counted, never errors
    // (spec §2.5).
    let lineage = consumer_segments(&desc);
    let mut stale_local = 0usize;
    type SegOps = (Vec<(u64, u32)>, Vec<(u64, u32, u32)>, Vec<(u64, u32, u32)>);
    let mut per_seg: std::collections::HashMap<u32, SegOps> = Default::default();
    let mut tok = |t: &str| -> Option<(u32, u64, u32)> {
        match crate::product_cursor::LeaseToken::decode(t, &desc.project_id, &skey, &epoch) {
            // A token from a DELETED consumer generation is stale by
            // definition — even if the name has since been recreated,
            // this lease belongs to a dead incarnation (round 16).
            Ok(lt)
                if lt.consumer_gen == cgen
                    && lineage.iter().any(|(sid, ..)| *sid == lt.msg.seg_id) =>
            {
                Some((lt.msg.seg_id, lt.msg.offset, lt.lease_gen))
            }
            _ => {
                stale_local += 1;
                None
            }
        }
    };
    for i in &doc.acks {
        if let Some((sid, o, g)) = tok(&i.lease_token) {
            per_seg.entry(sid).or_default().0.push((o, g));
        }
    }
    for i in &doc.retries {
        if let Some((sid, o, g)) = tok(&i.lease_token) {
            per_seg.entry(sid).or_default().1.push((
                o,
                g,
                crate::queue::retry_delay_ms(i.delay_ms),
            ));
        }
    }
    for i in &doc.extends {
        if let Some((sid, o, g)) = tok(&i.lease_token) {
            per_seg.entry(sid).or_default().2.push((
                o,
                g,
                crate::queue::visibility_window_ms(i.visibility_ms, &cfg),
            ));
        }
    }
    let (mut acked, mut retried, mut extended, mut dlq, mut backlog, mut stale) =
        (0usize, 0usize, 0usize, 0usize, 0u64, 0usize);
    let mut dlq_blocked = 0usize;
    for (sid, (acks, retries, extends)) in per_seg {
        let Some((seg_id, identity, route, _)) = lineage.iter().find(|(s, ..)| *s == sid).copied()
        else {
            continue;
        };
        let engine = match state.engine_for(&route).await {
            Ok(e) => e,
            Err(r) => return Err(r),
        };
        let out = engine
            .submit_queue(
                identity,
                crate::queue::QueueOp::Settle {
                    consumer: cname.clone(),
                    cgen,
                    acks,
                    retries,
                    extends,
                    max_deliveries: cfg.max_attempts,
                },
            )
            .await;
        let (a, r, e2, d, bl, st2, poisoned) = match out {
            Ok(crate::queue::QueueOut::Settled {
                acked,
                retried,
                extended,
                dlq,
                backlog,
                stale,
                poisoned,
            }) => (acked, retried, extended, dlq, backlog, stale, poisoned),
            Ok(_) => unreachable!("settle answers Settled"),
            Err(m) if m.starts_with("consumer_not_found") => {
                return Err(failure(
                    FailureClass::Missing,
                    "consumer_not_found",
                    &m,
                    None,
                    false,
                ));
            }
            Err(m) if m.starts_with("consumer_generation_fenced") => {
                return Err(failure(
                    FailureClass::Conflict,
                    "consumer_deleted",
                    &m,
                    None,
                    false,
                ));
            }
            Err(m) => {
                return Err(failure(FailureClass::Internal, "internal", &m, None, true));
            }
        };
        acked += a;
        retried += r;
        extended += e2;
        backlog += bl;
        stale += st2;
        if poisoned.is_empty() {
            dlq += d;
        } else {
            let handle = match engine.stream_handle(identity).await {
                Ok(h) => h,
                Err(e) => {
                    return Err(failure(
                        FailureClass::Internal,
                        "internal",
                        &e.to_string(),
                        None,
                        true,
                    ));
                }
            };
            state.keys.put(identity, skey.clone(), epoch);
            let lo = poisoned.iter().map(|(o, ..)| *o).min().unwrap_or(0);
            let mut by_off: DeliveryRecords = Default::default();
            if let Ok(out) = crate::application::read::read_merged(
                &skey,
                &epoch,
                &handle,
                &engine,
                lo,
                None,
                4 << 20,
                crate::shard::Deliver::Durable,
            )
            .await
            {
                by_off = DeliveryRecords::new(out.recs);
            }
            let handoff = dlq_and_settle(
                &state, &desc, &cfg, cgen, &cname, &skey, &epoch, identity, &engine, seg_id,
                &poisoned, &by_off,
            )
            .await;
            dlq += handoff.settled;
            dlq_blocked += handoff.blocked;
        }
    }
    Ok(SettleOutcome {
        acked,
        retried,
        extended,
        dlq,
        stale: stale + stale_local,
        backlog,
        dlq_blocked,
    })
}

/// What one dead-letter pass did with the poisoned leases it was handed. A
/// lease it did not settle is still held by the source, so its key stays
/// blocked and a later pass retries it.
#[must_use]
#[derive(Default)]
struct DeadLetterPass {
    settled: usize,
    /// Leases retained because the target refused them or could not be confirmed.
    blocked: usize,
}

// Preserve the explicit source incarnation and one bounded poisoned segment.
#[expect(
    clippy::too_many_arguments,
    reason = "dlq_and_settle; the dead-letter path takes the stream, consumer, key, epoch, identity, engine and segment separately as settlement resolved them, with the stored records its copies embed; a context struct would exist only for this signature"
)]
#[expect(
    clippy::too_many_lines,
    reason = "dlq_and_settle; the dead-letter copies of the stored records, their appends (an own-sequence reuse proving an earlier copy) and the settlement of the poisoned leases are one bounded sequence over the same leases; splitting it would separate the appends from the leases they release"
)]
async fn dlq_and_settle(
    state: &Arc<ConsumerService>,
    desc: &StreamDesc,
    cfg: &crate::queue::ConsumerConfig,
    cgen: u64,
    cname: &str,
    skey: &crate::crypto::StreamKey,
    epoch: &[u8; 16],
    identity: [u8; 16],
    engine: &Arc<crate::shard::ShardEngine>,
    seg_id: u32,
    poisoned: &[(u64, u32, u32, [u8; 16])],
    by_off: &DeliveryRecords,
) -> DeadLetterPass {
    let mut pass = DeadLetterPass::default();
    if poisoned.is_empty() {
        return pass;
    }
    // The target must still be the incarnation that was configured. One that
    // is not blocks every lease of this pass alike, so it is decided once.
    let target = match &cfg.dead_letter_stream {
        Some(dlq) => match state.registry.get(&desc.ref_in_project(dlq)).await {
            Ok(Some(t)) if Some(&t.stream_epoch) == cfg.dead_letter_epoch.as_ref() => {
                Some((dlq, t))
            }
            lookup => {
                tracing::warn!(
                    stream = %desc.name,
                    consumer = %cname,
                    dead_letter_stream = %dlq,
                    lookup_error = ?lookup.as_ref().err(),
                    "dead-letter target is unreadable or a different incarnation \
                     than the one configured; source leases retained"
                );
                pass.blocked = poisoned.len();
                return pass;
            }
        },
        None => None,
    };
    for (off, lgen, attempts, kh) in poisoned {
        if let Some((dlq, target)) = &target {
            let Some((rkey, payload)) = by_off.get(off) else {
                // Outside this pass's read window; a later pull retries.
                continue;
            };
            let msg_id = crate::product_cursor::MessageId {
                epoch: *epoch,
                key_hash: *kh,
                seg_id,
                offset: *off,
            }
            .encode(&desc.project_id, skey);
            let copy = served_value(desc, payload).and_then(|value| {
                serde_json::to_string(&DeadLetterCopy {
                    attempts: *attempts,
                    consumer: cname,
                    message_id: &msg_id,
                    routing_key: rkey,
                    source_stream: &desc.name,
                    value,
                })
            });
            let body = match copy {
                Ok(body) => body,
                Err(error) => {
                    pass.blocked += 1;
                    tracing::warn!(stream=%desc.name,consumer=%cname,offset=off,error=%error,"dead-letter copy cannot embed the stored record; source lease retained");
                    continue;
                }
            };
            let request_hash = crate::application::append::product_request_hash(
                false,
                "",
                &target.content_type,
                body.as_bytes(),
                false,
            );
            let wire_body = if target.is_json() {
                Bytes::from(format!("[{body}]"))
            } else {
                Bytes::from(body)
            };
            let pid = format!("dlq:{cname}:{}", &msg_id[..msg_id.len().min(200)]);
            let command = crate::application::append::AppendCommand {
                sref: desc.ref_in_project(dlq),
                key: skey.clone(),
                producer: Some(crate::shard::ProducerReq {
                    id: pid,
                    epoch: 1,
                    seq: 0,
                    request_hash: Some(request_hash),
                }),
                content_type: Some(target.content_type.clone()),
                routing_key: String::new(),
                body: wire_body,
                close: false,
                seal_auth: None,
                request_hash: Some(request_hash),
                expected_epoch: Some(target.epoch()),
                body_charge: None,
                close_identity: None,
                sequence: None,
                ts_hint_ms: None,
                key_version: 0,
            };
            match state.append.execute(command).await {
                Ok(_) => {}
                // The producer names this one message, so a committed
                // sequence 0 IS its delivery, even when the copy that
                // committed differs from this one (built by an earlier
                // release before a crash kept its settle from running).
                Err(error) if error.code == AppendCode::ProducerSequenceReused => {
                    tracing::info!(stream=%desc.name,consumer=%cname,dead_letter_stream=%dlq,"dead-letter copy already committed under this message's producer; settling the source lease");
                }
                Err(error) => {
                    pass.blocked += usize::from(error.definitively_rejected());
                    tracing::warn!(stream=%desc.name,consumer=%cname,dead_letter_stream=%dlq,error=%error,"dead-letter delivery failed; source lease retained");
                    continue;
                }
            }
        }
        if let Ok(crate::queue::QueueOut::Settled { acked, .. }) = engine
            .submit_queue(
                identity,
                crate::queue::QueueOp::Settle {
                    consumer: cname.to_string(),
                    cgen,
                    acks: vec![(*off, *lgen)],
                    retries: Vec::new(),
                    extends: Vec::new(),
                    max_deliveries: cfg.max_attempts,
                },
            )
            .await
        {
            pass.settled += acked;
        }
    }
    pass
}

#[cfg(test)]
mod tests {
    use super::{DeadLetterCopy, served_value};

    /// A dead-letter copy embeds the source's stored text unparsed, so its
    /// value is byte-identical to the source record, and its producer hash
    /// is fixed by stored bytes alone (golden): no parser or formatter
    /// change can turn a retried handoff into a sequence conflict.
    #[test]
    fn dead_letter_copies_embed_the_stored_record_and_hash_golden() {
        let desc = crate::sse::feed::tests::test_desc("src");
        let stored = br#"{"f":1.7802719962921167e-19,"b":1E+2,"a":1,"a":2}"#;
        let value = served_value(&desc, stored).unwrap();
        assert_eq!(value.get().as_bytes(), stored);
        let copy = |value| DeadLetterCopy {
            attempts: 3,
            consumer: "work",
            message_id: "m1",
            routing_key: "k",
            source_stream: "src",
            value,
        };
        let record = br#"{"f":1.7802719962921167e-19}"#;
        let body = serde_json::to_string(&copy(served_value(&desc, record).unwrap())).unwrap();
        assert_eq!(
            body,
            r#"{"attempts":3,"consumer":"work","messageId":"m1","routingKey":"k","sourceStream":"src","value":{"f":1.7802719962921167e-19}}"#
        );
        let hash = crate::application::append::product_request_hash(
            false,
            "",
            "application/json",
            body.as_bytes(),
            false,
        );
        assert_eq!(
            crate::crypto::hex(&hash),
            "39b34621c0efb3a17c2f7969b86f8072"
        );
        assert!(served_value(&desc, b"{\"a\":").is_err(), "never a null");
    }
}
