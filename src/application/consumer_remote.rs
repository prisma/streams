//! Bounded consumer peer operations. Incarnation coordinates cross every hop.
use super::consumer::ConsumerService;
use crate::application::read_remote::InternalTarget;
use serde_json::json;
use std::sync::Arc;

pub(super) async fn relay_queue_cursor(
    state: &Arc<ConsumerService>,
    base: &str,
    sref: &crate::tenant::TenantStreamRef,
    target: &InternalTarget,
    cname: &str,
    cgen: u64,
) -> Option<(u64, u64)> {
    let mut req = crate::peer::client()
        .get(format!(
            "{base}/v1/internal/queue-cursor/{}",
            crate::peer::encode_stream_name_path(sref.name().as_str())
        ))
        .timeout(std::time::Duration::from_secs(15))
        .header("streams-internal-consumer", cname)
        .header("streams-internal-gen", cgen.to_string());
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    let mk = |bearer: Option<&str>| {
        let mut req = req.try_clone().expect("fleet GET request is clonable");
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    let v: serde_json::Value = match state.peer.send(mk).await {
        Ok(r) if r.status().is_success() => r.json().await.ok()?,
        _ => return None,
    };
    Some((v["cursor"].as_u64()?, v["tail"].as_u64()?))
}

pub(super) async fn relay_sweep_segment(
    state: &Arc<ConsumerService>,
    base: &str,
    sref: &crate::tenant::TenantStreamRef,
    target: &InternalTarget,
    cname: &str,
    fence_below: u64,
    steps_left: &std::sync::Arc<std::sync::atomic::AtomicI64>,
) -> Result<(), (&'static str, String)> {
    let seg_id = target.seg_id;
    /// Per-relay chunk. Reserved ATOMICALLY before the request goes out
    /// (round-19): eight concurrent sweeps that each merely READ
    /// steps_left could each ask for a full chunk and collectively blow
    /// past the per-request step budget.
    const RELAY_CHUNK: i64 = 128;
    loop {
        // Reserve first, refund the unused remainder after the reply —
        // a load-then-send left the budget shared, not partitioned.
        let mut reserved = 0i64;
        loop {
            let cur = steps_left.load(std::sync::atomic::Ordering::SeqCst);
            if cur <= 0 {
                break;
            }
            let take = cur.min(RELAY_CHUNK);
            if steps_left
                .compare_exchange(
                    cur,
                    cur - take,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                reserved = take;
                break;
            }
        }
        if reserved <= 0 {
            return Err((
                "segment_cleanup_incomplete",
                format!(
                    "segment {seg_id} still has rows after this request's \
                     cleanup budget; progress is durable — retry to resume"
                ),
            ));
        }
        let mut req = crate::peer::client()
            .post(format!(
                "{base}/v1/internal/sweep-segment/{}",
                crate::peer::encode_stream_name_path(sref.name().as_str())
            ))
            .timeout(std::time::Duration::from_secs(30))
            .json(&json!({
                "consumer": cname,
                "segId": seg_id,
                "fenceBelow": fence_below,
                "maxSteps": reserved,
            }));
        for (k, v) in target.headers() {
            req = req.header(k, v);
        }
        let mk = |bearer: Option<&str>| {
            let mut req = req.try_clone().expect("fleet sweep request is clonable");
            if let Some(t) = bearer {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            req
        };
        let reply: Option<serde_json::Value> = match state.peer.send(mk).await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        };
        let Some(v) = reply else {
            // Refund: the peer may have used nothing at all.
            steps_left.fetch_add(reserved, std::sync::atomic::Ordering::SeqCst);
            return Err((
                "segment_unavailable",
                format!(
                    "segment {seg_id}'s owner did not complete the relayed \
                     sweep; the deletion is incomplete — retry"
                ),
            ));
        };
        let used = v["steps"].as_i64().unwrap_or(reserved).clamp(0, reserved);
        steps_left.fetch_add(reserved - used, std::sync::atomic::Ordering::SeqCst);
        if v["complete"].as_bool() == Some(true) {
            return Ok(());
        }
    }
}
