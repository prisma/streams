//! Incarnation-bound peer read protocol. Only this network adapter decodes the remote wire page.
use crate::registry::StreamDesc;
use bytes::Bytes;

/// The target of a fleet-internal peer RPC. **A name is not an
/// identity** (the hardening program's central rule): a relay naming
/// only `(stream, segment)` binds to whatever descriptor occupies that
/// name when the request LANDS, so a delete/recreate in flight lets a
/// stale request read — or fence and delete — the replacement's state
/// (round-19 ABA findings). Every internal request therefore carries
/// the sender's incarnation and the identity it derived, and the
/// receiver re-derives both before touching anything.
pub(crate) struct InternalTarget {
    /// §16: the physical target is PROJECT-QUALIFIED on the wire —
    /// workspace id is deliberately not part of it.
    pub project_id: crate::tenant::ProjectId,
    pub stream_epoch: [u8; 16],
    pub seg_id: u32,
    pub identity: [u8; 16],
}

impl InternalTarget {
    pub fn of(desc: &StreamDesc, seg_id: u32) -> Option<Self> {
        desc.segment_route_by_id(seg_id)?;
        Some(InternalTarget {
            project_id: desc.project_id.clone(),
            stream_epoch: desc.epoch(),
            seg_id,
            identity: desc.dynamic_segment_identity(seg_id),
        })
    }
    pub fn headers(&self) -> [(&'static str, String); 4] {
        [
            (
                "streams-internal-project",
                self.project_id.as_str().to_string(),
            ),
            (
                "streams-internal-epoch",
                crate::crypto::hex(&self.stream_epoch),
            ),
            ("streams-internal-seg", self.seg_id.to_string()),
            (
                "streams-internal-identity",
                crate::crypto::hex(&self.identity),
            ),
        ]
    }
}

/// Round-11.2: typed remote-span outcomes — no failure collapses into
/// an unclassified None. `Retryable`/`Transport` stay source retries;
/// everything else is a typed cutoff the session surfaces.
#[derive(Debug)]
pub(crate) enum RemoteSpanError {
    /// The contacted instance is not the owner and named another.
    WrongOwner {
        owner: String,
    },
    /// 401 AFTER send_fleet's one forced workload-token refresh.
    Unauthorized,
    /// 404/410: the segment no longer exists at the owner.
    TargetGone,
    /// The owner refused the incarnation-bound target.
    TargetMismatch,
    /// 429/503-class: retry with backoff.
    Retryable {
        status: u16,
        code: Option<String>,
    },
    /// A second ownership redirect in one operation is refused.
    RedirectLoop {
        first: String,
        second: String,
    },
    Transport(String),
    InvalidResponse(String),
}

pub(crate) struct RemoteSpanPage {
    pub(crate) out: super::read::ReadPage,
    /// The owner that actually served the page (may differ from the
    /// initial owner after the one verified redirect).
    pub(crate) owner: String,
}

/// One remote sealed-span page, following AT MOST one verified
/// ownership redirect (round-11 locked architecture): the redirect
/// owner must be a nonempty canonical instance name, different from
/// the one just contacted, and present in the TRUSTED peer table —
/// a URL from the peer is never accepted.
pub(crate) async fn remote_span_page(
    peer: &crate::peer::PeerClient,
    initial_owner: &str,
    name: &str,
    target: &InternalTarget,
    from: u64,
    max_bytes: usize,
    key_b64: &str,
) -> Result<RemoteSpanPage, RemoteSpanError> {
    let mut owner = initial_owner.to_string();
    for hop in 0..2u8 {
        let Some(base) = peer.url_for(&owner) else {
            return Err(RemoteSpanError::Transport(format!(
                "owner {owner} has no entry in the trusted peer table"
            )));
        };
        match scan_page_once(peer, &base, name, target, from, max_bytes, key_b64).await {
            Ok(out) => return Ok(RemoteSpanPage { out, owner }),
            Err(RemoteSpanError::WrongOwner { owner: next }) => {
                if hop == 1 {
                    return Err(RemoteSpanError::RedirectLoop {
                        first: owner,
                        second: next,
                    });
                }
                if next.is_empty() || next == owner || !peer.has_peer(&next) {
                    return Err(RemoteSpanError::InvalidResponse(format!(
                        "redirect names an unroutable owner {next:?}"
                    )));
                }
                owner = next;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("two hops always return")
}

/// One page against ONE peer base, with the full response mapping.
pub(crate) async fn scan_page_once(
    peer: &crate::peer::PeerClient,
    base: &str,
    name: &str,
    target: &InternalTarget,
    from: u64,
    max_bytes: usize,
    key_b64: &str,
) -> Result<super::read::ReadPage, RemoteSpanError> {
    let mut req = crate::peer::client()
        .get(format!(
            "{base}/v1/internal/segment-scan/{}",
            crate::peer::encode_stream_name_path(name)
        ))
        .timeout(std::time::Duration::from_secs(20))
        .header("streams-internal-from", from.to_string())
        .header("streams-internal-max-bytes", max_bytes.to_string())
        .header("stream-encryption-key", key_b64);
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
    let resp = match peer.send(mk).await {
        Ok(r) => r,
        Err(e) => return Err(RemoteSpanError::Transport(e.to_string())),
    };
    let status = resp.status().as_u16();
    if status == 401 {
        // send_fleet already forced ONE workload-token refresh.
        return Err(RemoteSpanError::Unauthorized);
    }
    if status == 404 || status == 410 {
        return Err(RemoteSpanError::TargetGone);
    }
    if status == 429 || status == 503 {
        let code = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
        return Err(RemoteSpanError::Retryable { status, code });
    }
    if status == 409 {
        let replay = resp
            .headers()
            .get("streams-replay-to")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let code = resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string));
        return match (code.as_deref(), replay) {
            (Some("not_ring_owner"), Some(owner)) => Err(RemoteSpanError::WrongOwner { owner }),
            (Some("target_mismatch") | Some("invalid_target"), _) => {
                Err(RemoteSpanError::TargetMismatch)
            }
            (c, _) => Err(RemoteSpanError::InvalidResponse(format!(
                "409 with code {c:?}"
            ))),
        };
    }
    if !(200..300).contains(&status) {
        return Err(RemoteSpanError::InvalidResponse(format!("status {status}")));
    }
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => return Err(RemoteSpanError::InvalidResponse(e.to_string())),
    };
    parse_scan_page(&v).ok_or_else(|| RemoteSpanError::InvalidResponse("malformed page".into()))
}

fn parse_scan_page(v: &serde_json::Value) -> Option<super::read::ReadPage> {
    use base64::Engine as _;
    let recs = v["items"]
        .as_array()?
        .iter()
        .map(|it| {
            Some(super::read::PlainRec {
                off: it["off"].as_u64()?,
                rkey: it["rk"].as_str()?.to_string(),
                payload: Bytes::from(
                    base64::engine::general_purpose::STANDARD
                        .decode(it["p"].as_str()?)
                        .ok()?,
                ),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(super::read::ReadPage {
        watermarks: crate::application::read::Watermarks {
            durable: v["end"].as_u64()?,
            applied: v["end"].as_u64()?,
        },
        recs,
        last: v["last"].as_u64(),
        end: v["end"].as_u64()?,
        completed: v["completed"].as_bool()?,
    })
}
