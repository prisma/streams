//! Fleet-safe system-stream append (round-21 blocker 5): local first,
//! one relay hop to the ring owner.
//!
//! The decision between "create it here", "another instance owns it"
//! and "it failed" is read from the TYPED append and creation results,
//! never from a status code: a creation the ring refuses carries its
//! owner, and only an owner-bearing refusal relays. Ambiguity is safe
//! end to end because every record downstream deduplicates by
//! deterministic id / source sequence.

use super::is_reserved_stream;
use crate::application::append::{AppendCode, AppendFailure, AppendOutcome, AppendResult};
use crate::application::creation::{CreateCommand, CreationError};
use crate::http::AppState;
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use std::sync::Arc;

/// Why a local system append did not land. Typed so a caller can
/// follow an ownership bounce and otherwise report code and message.
#[derive(Debug)]
pub(crate) enum LocalFailure {
    /// The system key is not a valid stream key or header value.
    Key(String),
    Append(AppendFailure),
    Create(CreationError),
}

impl LocalFailure {
    /// The instance the ring assigned the stream to, when the refusal
    /// was ownership: the relay target.
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Append(AppendFailure { owner, .. })
            | Self::Create(CreationError { owner, .. }) => owner.as_deref(),
            Self::Key(_) => None,
        }
    }
}

impl std::fmt::Display for LocalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(message) => write!(f, "invalid_key: {message}"),
            Self::Append(error) => write!(f, "append {error}"),
            Self::Create(error) => write!(f, "create {}: {}", error.code, error.message),
        }
    }
}

fn system_headers(key: &str) -> Result<HeaderMap, LocalFailure> {
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|e| LocalFailure::Key(e.to_string()))?,
    );
    hdrs.insert("content-type", HeaderValue::from_static("application/json"));
    Ok(hdrs)
}

async fn attempt(
    state: &Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    hdrs: HeaderMap,
    body: Bytes,
) -> AppendResult {
    crate::http::append_typed(
        state.clone(),
        sref,
        hdrs,
        axum::body::Body::from(body),
        None,
        None,
        None,
    )
    .await
}

/// Append `body` to the reserved stream `sref` on THIS instance,
/// creating the stream on first use: the raw-surface semantics
/// (`append_typed`, `CreationService::create`) with their typed
/// results. A not-found append creates and appends again; every other
/// refusal is returned as it was decided, owner included.
pub(crate) async fn append_local(
    state: &Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    key: &str,
    body: Bytes,
) -> Result<AppendOutcome, LocalFailure> {
    let stream_key = crate::crypto::StreamKey::from_b64(key).map_err(LocalFailure::Key)?;
    let hdrs = system_headers(key)?;
    match attempt(state, sref.clone(), hdrs.clone(), body.clone()).await {
        Err(error) if error.code == AppendCode::NotFound => {}
        result => return result.map_err(LocalFailure::Append),
    }
    state
        .creation_service()
        .create(CreateCommand {
            sref: sref.clone(),
            key: stream_key,
            content_type: Some("application/json".to_string()),
            ttl_secs: None,
            expires_at_ms: None,
            close: false,
            body: Bytes::new(),
            fork: None,
        })
        .await
        .map_err(LocalFailure::Create)?;
    attempt(state, sref, hdrs, body)
        .await
        .map_err(LocalFailure::Append)
}

/// Append to a reserved system stream from ANY fleet member: local
/// first; on an ownership refusal the body relays ONCE to the owner's
/// fleet-internal telemetry endpoint, authenticated with the fleet
/// credential and carrying the system key.
// mt-lint: allow(name-param-shared-core): system ledger under the system project; names are crate constants (_usage, _ops_*), never customer input
pub(crate) async fn system_append(
    state: &Arc<AppState>,
    stream: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    debug_assert!(is_reserved_stream(stream));
    let body = Bytes::from(body);
    // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
    let sref = crate::tenant::system_project().stream_ref(stream);
    let failure = match append_local(state, sref, key, body.clone()).await {
        Ok(_) => return Ok(()),
        Err(failure) => failure,
    };
    // Ownership bounce: relay once to the owner.
    match failure.owner().and_then(|owner| state.peer.url_for(owner)) {
        Some(base) => relay(state, &base, stream, key, body).await,
        None => Err(format!("system append {stream}: {failure}")),
    }
}

/// One relay hop: the owner's fleet-internal telemetry endpoint.
// mt-lint: allow(name-param-shared-core): system ledger under the system project; the name is a crate constant encoded into the relay path
async fn relay(
    state: &Arc<AppState>,
    base: &str,
    stream: &str,
    key: &str,
    body: Bytes,
) -> Result<(), String> {
    let mk = |bearer: Option<&str>| {
        let mut req = crate::http::peer_client()
            .post(format!(
                "{base}/v1/internal/telemetry-append/{}",
                crate::http::encode_stream_name_path(stream)
            ))
            .timeout(std::time::Duration::from_secs(20))
            .header("stream-encryption-key", key)
            .header("content-type", "application/json")
            .body(body.clone());
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match state.peer.send(mk).await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => Err(format!("telemetry relay {stream}: {}", resp.status())),
        Err(e) => Err(format!("telemetry relay {stream}: {e}")),
    }
}
