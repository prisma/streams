//! Incarnation-bound peer read protocol. Only this network adapter decodes the remote wire page.
use super::read_wire::{self, WireRecord};
use crate::registry::StreamDesc;

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
    desc: &StreamDesc,
    target: &InternalTarget,
    from: u64,
    max_bytes: usize,
    key_b64: &str,
) -> Result<RemoteSpanPage, RemoteSpanError> {
    // Bind every wire field to the same validated incarnation before looking up
    // a peer. A caller cannot pair one stream path with another stream's target.
    if target.project_id != desc.project_id
        || target.stream_epoch != desc.epoch()
        || desc.segment_route_by_id(target.seg_id).is_none()
        || target.identity != desc.dynamic_segment_identity(target.seg_id)
    {
        return Err(RemoteSpanError::TargetMismatch);
    }
    let mut owner = initial_owner.to_string();
    for hop in 0..2u8 {
        let Some(base) = peer.url_for(&owner) else {
            return Err(RemoteSpanError::Transport(format!(
                "owner {owner} has no entry in the trusted peer table"
            )));
        };
        match scan_page_once(peer, &base, desc, target, from, max_bytes, key_b64).await {
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
async fn scan_page_once(
    peer: &crate::peer::PeerClient,
    base: &str,
    desc: &StreamDesc,
    target: &InternalTarget,
    from: u64,
    max_bytes: usize,
    key_b64: &str,
) -> Result<super::read::ReadPage, RemoteSpanError> {
    let mut req = crate::peer::client()
        .get(format!(
            "{base}/v1/internal/segment-scan/{}",
            crate::peer::encode_stream_name_path(&desc.name)
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
    let bytes = read_wire::body(resp.content_length(), resp.bytes_stream()).await?;
    read_wire::scan_page(&bytes, max_bytes)
}

/// Bounded peer page DTO. Every resume field is mandatory; decoding a missing
/// cursor or watermark is a protocol error, never a successful zero position.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct WireReadPage {
    epoch: String,
    #[serde(deserialize_with = "read_wire::records")]
    records: Vec<WireRecord>,
    next: super::read::ReadPosition,
    durable: Option<super::read::ReadPosition>,
    pending_from: Option<usize>,
    up_to_date: bool,
    closed: bool,
    kind: super::read::ReadResultKind,
    segmented: bool,
    identity: [u8; 16],
    scan_from: u64,
    end: u64,
}
impl WireReadPage {
    pub(crate) fn from_outcome(out: &super::read::ReadOutcome) -> Self {
        use base64::Engine;
        Self {
            epoch: out.descriptor.stream_epoch.clone(),
            records: out
                .records
                .iter()
                .map(|record| WireRecord {
                    off: record.off,
                    key: record.rkey.clone(),
                    payload: base64::engine::general_purpose::STANDARD.encode(&record.payload),
                })
                .collect(),
            next: out.next,
            durable: out.durable,
            pending_from: out.pending_from,
            up_to_date: out.up_to_date,
            closed: out.closed,
            kind: out.kind,
            segmented: out.segmented,
            identity: out.identity,
            scan_from: out.scan_from,
            end: out.end,
        }
    }
    fn into_outcome(
        self,
        command: &super::read::ReadCommand,
    ) -> Result<super::read::ReadOutcome, RemoteSpanError> {
        if self.epoch != command.descriptor.stream_epoch
            || command
                .descriptor
                .segment_route_by_id(self.next.segment)
                .is_none()
            || self
                .durable
                .is_some_and(|p| command.descriptor.segment_route_by_id(p.segment).is_none())
        {
            return Err(RemoteSpanError::TargetMismatch);
        }
        if self
            .pending_from
            .is_some_and(|index| index >= self.records.len())
        {
            return Err(RemoteSpanError::InvalidResponse(
                "invalid pending record index".into(),
            ));
        }
        let records = read_wire::decode_records(self.records, command.max_bytes)?;
        Ok(super::read::ReadOutcome {
            descriptor: command.descriptor.clone(),
            records,
            next: self.next,
            durable: self.durable,
            pending_from: self.pending_from,
            up_to_date: self.up_to_date,
            closed: self.closed,
            kind: self.kind,
            segmented: self.segmented,
            identity: self.identity,
            scan_from: self.scan_from,
            end: self.end,
            waited: false,
            wait_micros: 0,
            read_micros: 0,
        })
    }
}

/// The public read coordinator's peer adapter. Bounded pages only; live waits
/// stay with the effective owner. Redirect destinations come from the trusted
/// peer table and at most one ownership redirect is followed.
pub(crate) async fn remote_read_page(
    peer: &crate::peer::PeerClient,
    initial_owner: &str,
    command: &super::read::ReadCommand,
    segment: u32,
    from: u64,
) -> Result<super::read::ReadOutcome, super::read::ReadFailure> {
    use super::read::ReadFailure;
    let target =
        InternalTarget::of(&command.descriptor, segment).ok_or(ReadFailure::InvalidCursor)?;
    let key = command.key.as_ref().ok_or(ReadFailure::MissingKey)?;
    use base64::Engine;
    let key = base64::engine::general_purpose::STANDARD.encode(key.0);
    let offset = if from == u64::MAX {
        "now".to_string()
    } else {
        crate::offsets::encode_ep(segment, crate::offsets::Offset(from.checked_sub(1)))
    };
    let mut owner = initial_owner.to_string();
    for hop in 0..2 {
        let base = peer.url_for(&owner).ok_or_else(|| {
            ReadFailure::Remote(RemoteSpanError::Transport(format!("unknown peer {owner}")))
        })?;
        let mut query = vec![("offset", offset.clone())];
        if let Some(selector) = &command.selector {
            query.push(("key", selector.clone()));
        }
        if matches!(command.mode, super::read::ReadMode::Head) {
            query.push(("head", "1".into()));
        }
        let mut request = crate::peer::client()
            .get(format!(
                "{base}/v1/internal/segment-read/{}",
                crate::peer::encode_stream_name_path(&command.descriptor.name)
            ))
            .query(&query)
            .timeout(std::time::Duration::from_secs(20))
            .header("streams-internal-read-page", "1")
            .header("stream-encryption-key", &key)
            .header("streams-internal-max-bytes", command.max_bytes.to_string());
        for (name, value) in target.headers() {
            request = request.header(name, value);
        }
        if command.visibility == crate::shard::Deliver::Applied {
            request = request.header("streams-internal-deliver", "applied");
        }
        let response = peer
            .send(|bearer| {
                let mut request = request.try_clone().expect("read request is clonable");
                if let Some(token) = bearer {
                    request = request.bearer_auth(token);
                }
                request
            })
            .await
            .map_err(|e| ReadFailure::Remote(RemoteSpanError::Transport(e.to_string())))?;
        let status = response.status();
        if status.as_u16() == 409
            && let Some(next) = response
                .headers()
                .get("streams-replay-to")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        {
            if hop == 0 && next != owner && peer.has_peer(&next) {
                owner = next;
                continue;
            }
            return Err(ReadFailure::Remote(RemoteSpanError::RedirectLoop {
                first: owner,
                second: next,
            }));
        }
        if !status.is_success() {
            return Err(match status.as_u16() {
                404 => ReadFailure::Missing,
                410 => ReadFailure::Gone,
                401 => ReadFailure::Remote(RemoteSpanError::Unauthorized),
                409 => ReadFailure::ChangedIncarnation,
                429 | 503 => ReadFailure::Remote(RemoteSpanError::Retryable {
                    status: status.as_u16(),
                    code: None,
                }),
                _ => ReadFailure::Remote(RemoteSpanError::InvalidResponse(format!(
                    "read peer status {status}"
                ))),
            });
        }
        let bytes = read_wire::body(response.content_length(), response.bytes_stream())
            .await
            .map_err(ReadFailure::Remote)?;
        let page: WireReadPage = serde_json::from_slice(&bytes)
            .map_err(|e| ReadFailure::Remote(RemoteSpanError::InvalidResponse(e.to_string())))?;
        return page.into_outcome(command).map_err(ReadFailure::Remote);
    }
    unreachable!("two bounded attempts always return")
}
