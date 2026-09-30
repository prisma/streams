//! The sender of a relayed seal fence (NEXT-WORK §5 F1-a). A takeover
//! coordinated on an instance that does not own the old final's segment
//! fences it at the owner over `POST /v1/internal/seal-fence`. The owner
//! answers its committer's closed-report only after the fence is durable,
//! and this side turns nothing but that parsed answer into a verdict.
use super::SealError;
use crate::application::topology::TopologyService;
use crate::registry::StreamDesc;

/// The relay's deadline: the segment-close relay's.
const RELAY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(40);

/// The owner's closed-report, the only answer that is a verdict.
#[derive(serde::Deserialize)]
struct ClosedReport {
    closed: bool,
}

/// Relay the fence at `fence_to` on segment `seg_id` to `owner`, the
/// instance the shard directory named. A 200 whose body parses as the
/// closed-report is the owner's barrier: `false` proves the fence durable
/// and staged after every earlier final, so no append below it can close
/// the segment afterwards; `true` that the old close committed. Every other
/// outcome leaves the seal resumable, with the old claim untouched: no
/// published peer URL, a refused credential, a 404 or a 409 of any code
/// (a redirect is never followed; the fence goes only where this instance
/// resolved it), a 5xx, an unparsable body, a transport error or the
/// deadline. The client's retry re-reserves and re-fences, and the fence
/// is idempotent at the owner.
pub(super) async fn relay_seal_fence(
    topology: &TopologyService,
    desc: &StreamDesc,
    seg_id: u32,
    fence_to: u64,
    owner: &str,
) -> Result<bool, SealError> {
    let base = topology.peer.url_for(owner).ok_or_else(|| {
        SealError::Resumable(format!("segment owner {owner} has no published peer URL"))
    })?;
    let target = crate::application::read_remote::InternalTarget::of(desc, seg_id)
        .ok_or(SealError::InvalidClaim)?;
    let url = format!(
        "{base}/v1/internal/seal-fence/{}?fence_to={fence_to}",
        crate::peer::encode_stream_name_path(&desc.name)
    );
    let mk = |bearer: Option<&str>| {
        let mut req = crate::peer::client()
            .post(url.as_str())
            .timeout(RELAY_TIMEOUT);
        for (k, v) in target.headers() {
            req = req.header(k, v);
        }
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match topology.peer.send(mk).await {
        Ok(r) if r.status() == reqwest::StatusCode::OK => r
            .json::<ClosedReport>()
            .await
            .map(|report| report.closed)
            .map_err(|e| {
                SealError::Resumable(format!(
                    "seal-fence relay to {owner}: unparsable answer: {e}"
                ))
            }),
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body.chars().take(300).collect();
            tracing::warn!(owner, seg_id, %status, "seal-fence relay refused: {excerpt}");
            Err(SealError::Resumable(format!(
                "seal-fence relay to {owner} answered {status}"
            )))
        }
        Err(e) => Err(SealError::Resumable(format!(
            "seal-fence relay to {owner} failed: {e}"
        ))),
    }
}
