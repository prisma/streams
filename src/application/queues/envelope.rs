//! Message envelopes (docs/QUEUES-DESIGN.md §5.1): the JSON record a send
//! seals as one page, and the `Message` a claim, get, failed list or peek
//! returns after opening that page with the request's key.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{FailureClass, QueueKey, QueueTarget, SendInput, fail};
use crate::native_queue::{DeliveryState, DeliveryView, SendMessage};

/// The record size limit an envelope must fit (413 `message_too_large`).
pub(super) const MAX_ENVELOPE_BYTES: usize = 131_072;
const MAX_HEADERS: usize = 16;
const MAX_HEADER_BYTES: usize = 4_096;

/// Where a republished message came from (§7.3). A message sent directly
/// stores no lineage: its origin is itself.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub(super) struct StoredLineage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) origin_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) republished_from: Option<String>,
}

/// The sealed record of one message.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Envelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) deliver_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) dedup_key: Option<String>,
    pub(super) content_type: String,
    #[serde(default)]
    // mt-lint: allow(name-keyed-map): message headers, keyed by header name; no stream identity
    pub(super) headers: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) lineage: StoredLineage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) body: Option<Box<RawValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) body_base64: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LineageOut {
    origin_id: String,
    republished_from: Option<String>,
}

/// A failed delivery's record (§7.1); `error` text is not stored by the
/// first release, so it is always null.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailureOut {
    cause: &'static str,
    code: Option<String>,
    error: Option<String>,
    attempts: u32,
    failed_at: i64,
    failure_expires_at: i64,
}

/// A message as the routes return it.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Message {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    attempt: u32,
    cycle: u32,
    enqueued_at: i64,
    available_at: i64,
    lease_deadline: i64,
    body: Option<Box<RawValue>>,
    body_base64: Option<String>,
    content_type: String,
    // mt-lint: allow(name-keyed-map): message headers, keyed by header name; no stream identity
    headers: BTreeMap<String, String>,
    key: Option<String>,
    dedup_key: Option<String>,
    lineage: LineageOut,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<FailureOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<DeliveryState>,
}

/// What a returned message shows beside its envelope.
#[derive(Clone, Copy, Default)]
pub(super) struct Shown {
    /// The lease token (claims only).
    pub(super) token: bool,
    pub(super) failure: bool,
    pub(super) state: bool,
}

pub(super) fn message(
    t: &QueueTarget,
    view: &DeliveryView,
    env: Envelope,
    shown: Shown,
) -> Message {
    let id = t.id(view.offset);
    let token = shown.token.then(|| {
        crate::native_queue::keys::lease_token(
            &t.hash,
            crate::native_queue::Token {
                offset: view.offset,
                lease_gen: view.lease_gen,
            },
        )
    });
    let failure = view
        .failure
        .as_ref()
        .filter(|_| shown.failure)
        .map(|f| FailureOut {
            cause: f.cause,
            code: f.code.clone(),
            error: None,
            attempts: f.attempts,
            failed_at: f.failed_at,
            failure_expires_at: f.failure_expires_at,
        });
    Message {
        lineage: LineageOut {
            origin_id: env.lineage.origin_id.unwrap_or_else(|| id.clone()),
            republished_from: env.lineage.republished_from,
        },
        id,
        token,
        attempt: view.attempt,
        cycle: view.cycle,
        enqueued_at: view.enqueued_at,
        available_at: view.available_at,
        lease_deadline: view.lease_deadline,
        body: env.body,
        body_base64: env.body_base64,
        content_type: env.content_type,
        headers: env.headers,
        key: env.key,
        dedup_key: env.dedup_key,
        failure,
        state: shown.state.then_some(view.state),
    }
}

/// The envelope of `input`, validated: exactly one body, at most 16
/// string headers of at most 4 KiB, at most one of `deliverAt` and
/// `delayMs`, and an envelope within the record limit.
pub(super) fn envelope_of(input: SendInput, now: i64) -> Result<Envelope, super::QueueFailure> {
    let invalid = |m: &str| fail(FailureClass::Invalid, "invalid_message", m);
    let body_base64 = match (&input.body, input.body_base64.as_deref()) {
        (Some(_), None) => None,
        (None, Some(b64)) => {
            Some(normalize_base64(b64).ok_or_else(|| invalid("bodyBase64 is not base64"))?)
        }
        _ => {
            return Err(invalid(
                "a message carries exactly one of body and bodyBase64",
            ));
        }
    };
    let headers = input.headers.unwrap_or_default();
    let header_bytes: usize = headers.iter().map(|(k, v)| k.len() + v.len()).sum();
    if headers.len() > MAX_HEADERS || header_bytes > MAX_HEADER_BYTES {
        return Err(invalid("headers are at most 16 entries and 4 KiB"));
    }
    let deliver_at = match (input.deliver_at, input.delay_ms) {
        (Some(_), Some(_)) => {
            return Err(invalid(
                "a message carries at most one of deliverAt and delayMs",
            ));
        }
        (_, Some(ms)) if ms < 0 => return Err(invalid("delayMs must not be negative")),
        (at, None) => at,
        (None, Some(ms)) => Some(now.saturating_add(ms)),
    };
    let default_type = if body_base64.is_some() {
        "application/octet-stream"
    } else {
        "application/json"
    };
    Ok(Envelope {
        deliver_at,
        key: input.key,
        dedup_key: input.dedup_key,
        content_type: input
            .content_type
            .unwrap_or_else(|| default_type.to_string()),
        headers,
        lineage: StoredLineage::default(),
        body: input.body,
        body_base64,
    })
}

/// Standard padded base64 of the bytes `raw` encodes in standard or
/// URL-safe base64, padded or not.
fn normalize_base64(raw: &str) -> Option<String> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let bytes = STANDARD
        .decode(raw)
        .or_else(|_| STANDARD_NO_PAD.decode(raw))
        .or_else(|_| URL_SAFE.decode(raw))
        .or_else(|_| URL_SAFE_NO_PAD.decode(raw))
        .ok()?;
    Some(STANDARD.encode(bytes))
}

/// The committer's message: the serialized envelope, its availability and
/// its dedup tag. A republished message never carries a tag (§7.3).
pub(super) fn sealable(
    key: &QueueKey,
    env: &Envelope,
    dedup: bool,
) -> Result<SendMessage, super::QueueFailure> {
    let bytes = serde_json::to_vec(env)
        .map_err(|e| fail(FailureClass::Internal, "internal", e.to_string()))?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(fail(
            FailureClass::TooLarge,
            "message_too_large",
            format!(
                "the message envelope is {} bytes; the limit is {MAX_ENVELOPE_BYTES}",
                bytes.len()
            ),
        ));
    }
    Ok(SendMessage {
        envelope: bytes::Bytes::from(bytes),
        deliver_at: env.deliver_at,
        dedup_tag: env
            .dedup_key
            .as_deref()
            .filter(|_| dedup)
            .map(|d| dedup_tag(key, d)),
    })
}

/// The dedup index tag of `dedup_key`: an HMAC under a subkey of the
/// stream key (§5.1), so the server stores no dedup key in the clear.
fn dedup_tag(key: &QueueKey, dedup_key: &str) -> [u8; 16] {
    use hmac::{Hmac, Mac};
    let base = crate::crypto::derive_subkey(&key.key, &key.epoch, "\u{0}queue-dedup-v1\u{0}", 0);
    let Ok(mut mac) = <Hmac<sha2::Sha256> as Mac>::new_from_slice(&base) else {
        unreachable!("HMAC-SHA256 accepts a key of any length")
    };
    mac.update(dedup_key.as_bytes());
    let full = mac.finalize().into_bytes();
    let mut tag = [0u8; 16];
    tag.copy_from_slice(&full[..16]);
    tag
}

/// The envelope of the message at `offset`, by a point get of its page
/// (§6.1): `None` once the page is gone.
pub(super) async fn read(
    t: &QueueTarget,
    key: &QueueKey,
    offset: u64,
) -> Result<Option<Envelope>, String> {
    use crate::crypto_page::{CheckedPage, PageCipher, PageDecoder};
    let raw = t
        .engine
        .db
        .get(crate::crypto_page::shard_page_key(&t.hash, offset))
        .await
        .map_err(|e| e.to_string())?;
    let Some(raw) = raw else { return Ok(None) };
    let page = CheckedPage::admit(raw, offset).map_err(|e| format!("queue page: {e:?}"))?;
    let cipher = PageCipher::new(&key.subkey, &t.hash);
    let opened = cipher
        .open_within(&page, &mut PageDecoder::default(), usize::MAX)
        .map_err(|e| format!("queue page: {e:?}"))?
        .ok_or("queue page over its limit")?;
    let record = opened.records().next().ok_or("empty queue page")?;
    serde_json::from_slice(record.payload)
        .map(Some)
        .map_err(|e| format!("queue envelope: {e}"))
}
