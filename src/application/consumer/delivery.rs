//! Active consumer delivery, lease settlement and durable dead-letter handoff.
use super::{
    AuthorizedConsumerContext, AuthorizedStreamContext, ConsumerFailure, ConsumerService,
    DeliveryMessage, FailureClass, PullInput, PullOutcome, SettleInput, SettleOutcome,
    consumer_segments, failure,
};
use crate::application::consumer_remote::relay_queue_cursor;
use crate::application::read_remote::InternalTarget;
use crate::registry::StreamDesc;
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;

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
    let visibility = doc
        .visibility_ms
        .unwrap_or(cfg.visibility_timeout_ms as u64)
        .clamp(1_000, 12 * 3600 * 1000);
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
            if !poisoned.is_empty() {
                let _ = dlq_and_settle(
                    &state, &desc, &cfg, cgen, &cname, &skey, &epoch, identity, route, seg_id,
                    &poisoned, &by_off,
                )
                .await;
                // Settling poison may have drained this segment or
                // unblocked keys — restart the walk.
                continue 'outer;
            }
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
                );
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
    by_off: &std::collections::HashMap<u64, (String, Bytes)>,
) -> Vec<DeliveryMessage> {
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
        let value: serde_json::Value = if desc.is_json() {
            serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null)
        } else {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(payload))
        };
        messages.push(DeliveryMessage {
            id: msg.encode(&desc.project_id, skey),
            routing_key: rkey.clone(),
            attempts: *attempts,
            lease_token: lease.encode(&desc.project_id, skey),
            value,
        });
    }
    messages
}

/// A durable read pins the stream key/epoch and consumer generation used by Receive.
struct DeliveryRead<'a> {
    state: &'a ConsumerService,
    key: &'a crate::crypto::StreamKey,
    epoch: [u8; 16],
    consumer: &'a str,
    generation: u64,
}
struct DeliveryCoverage {
    keys_map: std::collections::HashMap<u64, [u8; 16]>,
    by_off: std::collections::HashMap<u64, (String, Bytes)>,
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
    let mut by_off: std::collections::HashMap<u64, (String, Bytes)> = Default::default();
    let mut covered_to = cursor;
    for r in &out.recs {
        keys_map.insert(r.off, crate::crypto::stream_hash(&r.rkey));
        by_off.insert(r.off, (r.rkey.clone(), r.payload.clone()));
        covered_to = covered_to.max(r.off + 1);
    }
    Ok(DeliveryCoverage {
        keys_map,
        by_off,
        covered_to,
    })
}

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
    type SegOps = (Vec<(u64, u32)>, Vec<(u64, u32, u64)>, Vec<(u64, u32, u64)>);
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
            per_seg
                .entry(sid)
                .or_default()
                .1
                .push((o, g, i.delay_ms.unwrap_or(1_000)));
        }
    }
    for i in &doc.extends {
        if let Some((sid, o, g)) = tok(&i.lease_token) {
            per_seg.entry(sid).or_default().2.push((
                o,
                g,
                i.visibility_ms.unwrap_or(cfg.visibility_timeout_ms as u64),
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
            let mut by_off: std::collections::HashMap<u64, (String, Bytes)> = Default::default();
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
                for r in &out.recs {
                    by_off.insert(r.off, (r.rkey.clone(), r.payload.clone()));
                }
            }
            let (d, b) = dlq_and_settle(
                &state, &desc, &cfg, cgen, &cname, &skey, &epoch, identity, route, seg_id,
                &poisoned, &by_off,
            )
            .await;
            dlq += d;
            dlq_blocked += b;
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

// Preserve the explicit source incarnation and one bounded poisoned segment.
#[allow(clippy::too_many_arguments)]
async fn dlq_and_settle(
    state: &Arc<ConsumerService>,
    desc: &StreamDesc,
    cfg: &crate::queue::ConsumerConfig,
    cgen: u64,
    cname: &str,
    skey: &crate::crypto::StreamKey,
    epoch: &[u8; 16],
    identity: [u8; 16],
    route: [u8; 16],
    seg_id: u32,
    poisoned: &[(u64, u32, u32, [u8; 16])],
    by_off: &std::collections::HashMap<u64, (String, Bytes)>,
) -> (usize, usize) {
    let mut settled = 0usize;
    // Deliveries the target refused for a reason retrying cannot fix.
    let mut blocked = 0usize;
    // The target must still be the incarnation that was configured.
    let dlq_target = match (&cfg.dead_letter_stream, &cfg.dead_letter_epoch) {
        (Some(dlq), Some(want)) => match state.registry.get(&desc.ref_in_project(dlq)).await {
            Ok(Some(t)) if &t.stream_epoch == want => Some(t),
            _ => None,
        },
        _ => None,
    };
    for (off, lgen, attempts, kh) in poisoned {
        if let Some(dlq) = &cfg.dead_letter_stream {
            if dlq_target.is_none() {
                blocked += 1;
                tracing::warn!(
                    stream = %desc.name,
                    consumer = %cname,
                    dead_letter_stream = %dlq,
                    "dead-letter target is a different incarnation than the one \
                     configured; refusing to deliver"
                );
                continue;
            }
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
            let value: serde_json::Value = if desc.is_json() {
                serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null)
            } else {
                use base64::Engine;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(payload))
            };
            let body = json!({
                "sourceStream": desc.name,
                "consumer": cname,
                "messageId": msg_id,
                "routingKey": rkey,
                "attempts": attempts,
                "value": value,
            })
            .to_string();
            let target = dlq_target
                .as_ref()
                .expect("configured incarnation checked above");
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
            if let Err(error) = state.append.execute(command).await {
                if error.definitively_rejected() {
                    blocked += 1;
                    tracing::warn!(stream=%desc.name,consumer=%cname,dead_letter_stream=%dlq,error=%error,"dead-letter delivery refused; source lease retained");
                }
                continue;
            }
        }
        let engine = match state.engine_for(&route).await {
            Ok(e) => e,
            Err(_) => continue,
        };
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
            settled += acked;
        }
    }
    (settled, blocked)
}
