//! Prisma product surface: `/v1/streams/{name}` (product-surface spec,
//! Stages 4–8). The PLURAL route is the Prisma collection API — typed
//! creation, routing-key records, consumers, watches, lifecycle — with
//! `Prisma-*` header names and product cursors. The SINGULAR
//! `/v1/stream/{name}` route remains the pinned Durable Streams
//! standards surface (the default-key sequence) and is untouched here.
//!
//! Clean-switch discipline (spec §0): removed experimental product
//! names (`Stream-Encryption-Key`, `Stream-Key`, `?key=` on THIS
//! route) are rejected, never translated. `__ds` is reserved on both
//! surfaces. Descriptors are written at `LAYOUT_VERSION` only.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use crate::http::AppState;
use crate::registry::{LAYOUT_VERSION, StreamDesc, WatchDefinition};

/// Reserved protocol control namespace (appendix §2.6): never a
/// customer stream name, on either surface.
pub const RESERVED_ROOT: &str = "__ds";

/// Final path segments reserved for product subresources. Explicit
/// route matching happens before wildcard names (spec Stage 8 §4.1);
/// with hierarchical names on one wildcard route, that means these
/// cannot terminate a stream name.
const RESERVED_FINAL_SEGMENTS: [&str; 3] = ["records", "consumers", "watches"];

/// Stable product error shape (spec Stage 8 §11).
pub fn perr(
    status: StatusCode,
    code: &str,
    message: &str,
    details: Option<serde_json::Value>,
    retryable: bool,
) -> Response {
    let mut e = json!({
        "code": code,
        "message": message,
        "retryable": retryable,
    });
    if let Some(d) = details {
        e["details"] = d;
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(json!({ "error": e }).to_string()))
        .unwrap()
}

/// Canonical stream-name validation (spec Stage 8 §4.1). The wildcard
/// path arrives percent-decoded exactly once by the router; this
/// validates the DECODED form.
pub fn canonical_name(raw: &str) -> Result<String, Response> {
    let bad = |code: &str, msg: &str| Err(perr(StatusCode::BAD_REQUEST, code, msg, None, false));
    if raw.is_empty() || raw.len() > 512 {
        return bad("invalid_name", "stream name must be 1-512 UTF-8 bytes");
    }
    if raw.chars().any(|c| c.is_control()) {
        return bad("invalid_name", "control characters are not allowed");
    }
    let segments: Vec<&str> = raw.split('/').collect();
    for seg in &segments {
        if seg.is_empty() {
            return bad("invalid_name", "empty path segments are not allowed");
        }
        if *seg == "." || *seg == ".." {
            return bad("invalid_name", "'.' and '..' segments are not allowed");
        }
    }
    if segments[0] == RESERVED_ROOT {
        return bad("invalid_name", "the __ds namespace is reserved");
    }
    if let Some(last) = segments.last() {
        if RESERVED_FINAL_SEGMENTS.contains(last) {
            return bad(
                "invalid_name",
                "'records', 'consumers' and 'watches' are reserved subresource names",
            );
        }
    }
    Ok(raw.to_string())
}

/// The experimental product names this route REJECTS instead of
/// translating (spec Stage 8 §5).
fn reject_legacy_inputs(headers: &HeaderMap, query: &str) -> Option<Response> {
    for h in ["stream-encryption-key", "stream-key"] {
        if headers.contains_key(h) {
            return Some(perr(
                StatusCode::BAD_REQUEST,
                "unknown_field",
                &format!(
                    "{h} is not a product-surface field; use Prisma-Encryption-Key / \
                     Prisma-Routing-Key"
                ),
                None,
                false,
            ));
        }
    }
    for pair in query.split('&') {
        let k = pair.split('=').next().unwrap_or("");
        if k == "key" || k == "offset" {
            return Some(perr(
                StatusCode::BAD_REQUEST,
                "unknown_field",
                &format!("'{k}' is not a product-surface query field"),
                None,
                false,
            ));
        }
    }
    None
}

// ---- typed creation document (spec Stage 7, v1 core) -----------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDoc {
    format: FormatDoc,
    #[serde(default)]
    expiry: Option<ExpiryDoc>,
    #[serde(default)]
    watches: Option<Vec<WatchDefinition>>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum FormatDoc {
    Json,
    Bytes {
        #[serde(rename = "contentType", default)]
        content_type: Option<String>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiryDoc {
    #[serde(default)]
    idle: Option<String>,
    #[serde(default)]
    at: Option<String>,
}

const MAX_CONFIG_BODY: usize = 256 * 1024;
const MAX_WATCH_DEFS: usize = 64;
const MAX_WATCH_FIELDS: usize = 16;

/// Parse a duration like "30d" / "12h" / "45m" / "30s" / plain seconds
/// into whole seconds (Stage 7 §7: equivalent spellings normalize to
/// the same integer).
fn parse_idle_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = match s.chars().last()? {
        'd' => (&s[..s.len() - 1], 86_400),
        'h' => (&s[..s.len() - 1], 3_600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    let v: u64 = num.parse().ok()?;
    (v > 0).then_some(v.checked_mul(mult)?)
}

struct ParsedCreate {
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    watches: Vec<WatchDefinition>,
}

fn parse_create_doc(body: &Bytes) -> Result<ParsedCreate, Response> {
    if body.len() > MAX_CONFIG_BODY {
        return Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_config",
            "configuration body exceeds 256 KiB",
            None,
            false,
        ));
    }
    let doc: CreateDoc = if body.is_empty() {
        return Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_config",
            "a typed JSON creation document is required (format.kind)",
            None,
            false,
        ));
    } else {
        match serde_json::from_slice(body) {
            Ok(d) => d,
            Err(e) => {
                return Err(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    &format!("configuration parse: {e}"),
                    None,
                    false,
                ));
            }
        }
    };
    let content_type = match &doc.format {
        FormatDoc::Json => "application/json".to_string(),
        FormatDoc::Bytes { content_type } => content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
    };
    let (ttl_secs, expires_at_ms) = match &doc.expiry {
        None => (None, None),
        Some(e) => match (&e.idle, &e.at) {
            (Some(_), Some(_)) => {
                return Err(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    "expiry.idle and expiry.at are mutually exclusive",
                    None,
                    false,
                ));
            }
            (Some(idle), None) => match parse_idle_secs(idle) {
                Some(v) => (Some(v), None),
                None => {
                    return Err(perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_config",
                        "invalid expiry.idle duration",
                        None,
                        false,
                    ));
                }
            },
            (None, Some(at)) => match chrono::DateTime::parse_from_rfc3339(at) {
                Ok(ts) if ts.timestamp_millis() > crate::shard::now_ms() => {
                    (None, Some(ts.timestamp_millis()))
                }
                _ => {
                    return Err(perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_config",
                        "expiry.at must be a future RFC 3339 timestamp",
                        None,
                        false,
                    ));
                }
            },
            (None, None) => (None, None),
        },
    };
    let watches = doc.watches.unwrap_or_default();
    if !watches.is_empty() {
        if content_type != "application/json" {
            return Err(perr(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                "watches require format.kind = json",
                None,
                false,
            ));
        }
        if watches.len() > MAX_WATCH_DEFS {
            return Err(perr(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                "too many watch definitions (max 64)",
                None,
                false,
            ));
        }
        let mut names = std::collections::HashSet::new();
        for w in &watches {
            if w.name.is_empty()
                || w.name.len() > 128
                || w.name.contains('/')
                || w.name.chars().any(|c| c.is_control())
                || w.name == "."
                || w.name == ".."
            {
                return Err(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    "invalid watch name",
                    None,
                    false,
                ));
            }
            if !names.insert(&w.name) {
                return Err(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    "duplicate watch name",
                    None,
                    false,
                ));
            }
            if w.fields.is_empty() || w.fields.len() > MAX_WATCH_FIELDS {
                return Err(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    "watch needs 1-16 fields",
                    None,
                    false,
                ));
            }
            for f in &w.fields {
                if !f.starts_with('/') {
                    return Err(perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_config",
                        "watch fields are JSON pointers starting with '/'",
                        None,
                        false,
                    ));
                }
            }
        }
    }
    Ok(ParsedCreate {
        content_type,
        ttl_secs,
        expires_at_ms,
        watches,
    })
}

// ---- entry -----------------------------------------------------------

/// Everything under `/v1/streams/{*path}`: subresource suffixes are
/// parsed here because stream names are hierarchical (spec Stage 8:
/// explicit matching before wildcard interpretation).
pub async fn product_entry(
    state: Arc<AppState>,
    path: String,
    method: Method,
    headers: HeaderMap,
    query: String,
    body: Bytes,
) -> Response {
    if let Some(r) = reject_legacy_inputs(&headers, &query) {
        return r;
    }
    // Verb suffix on the final segment: name:seal, name:scan, …
    let (path, verb) = match path.rsplit_once(':') {
        Some((p, v)) if !v.contains('/') => (p.to_string(), Some(v.to_string())),
        _ => (path, None),
    };
    // Subresource split (reserved final segments).
    if let Some((stream, rest)) = split_subresource(&path) {
        let name = match canonical_name(stream) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let _ = name;
        return perr(
            StatusCode::NOT_IMPLEMENTED,
            "not_implemented",
            &format!("product subresource '{rest}' lands with its stage (spec Stages 2/4/6)"),
            None,
            false,
        );
    }
    let name = match canonical_name(&path) {
        Ok(n) => n,
        Err(r) => return r,
    };
    match (method.clone(), verb.as_deref()) {
        (Method::PUT, None) => product_create(state, name, headers, body).await,
        (Method::GET, None) => product_metadata(state, name).await,
        (Method::DELETE, None) => crate::http::product_delete(state, name).await,
        (Method::POST, Some("seal")) => product_seal(state, name, headers).await,
        (_, Some("scan")) => perr(
            StatusCode::NOT_IMPLEMENTED,
            "not_implemented",
            "scan lands with Stage 6",
            None,
            false,
        ),
        _ => perr(
            StatusCode::NOT_FOUND,
            "unknown_route",
            "no such product operation",
            None,
            false,
        ),
    }
}

/// `customers/acme/orders/records...` → (stream, subresource-with-args).
fn split_subresource(path: &str) -> Option<(&str, &str)> {
    for marker in ["/records", "/consumers", "/watches"] {
        if let Some(idx) = path
            .find(&format!("{marker}/"))
            .or_else(|| path.ends_with(marker).then(|| path.len() - marker.len()))
        {
            let stream = &path[..idx];
            let rest = &path[idx + 1..];
            if !stream.is_empty() {
                return Some((stream, rest));
            }
        }
    }
    None
}

fn product_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("prisma-encryption-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

async fn product_create(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(key_b64) = product_key(&headers) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        );
    };
    let key = match crate::crypto::StreamKey::from_b64(&key_b64) {
        Ok(k) => k,
        Err(m) => return perr(StatusCode::BAD_REQUEST, "invalid_key", &m, None, false),
    };
    let cfg = match parse_create_doc(&body) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Some(r) = crate::http::ring_owner_check(&state, &name) {
        return r;
    }

    let build_fresh = || {
        let mut d = crate::http::fresh_desc_product(
            &state,
            &name,
            &key,
            cfg.content_type.clone(),
            cfg.ttl_secs,
            cfg.expires_at_ms,
        );
        d.watch_definitions = cfg.watches.clone();
        d.layout_version = LAYOUT_VERSION;
        d
    };
    // Idempotent compare (Stage 7 §7): normalized protocol config plus
    // normalized watch config.
    let same_config = |d: &StreamDesc| {
        crate::registry::media_type(&d.content_type)
            == crate::registry::media_type(&cfg.content_type)
            && d.ttl_secs == cfg.ttl_secs
            && (cfg.ttl_secs.is_some() || d.expires_at_ms == cfg.expires_at_ms)
            && d.watch_definitions == cfg.watches
    };
    let validate_live = |d: StreamDesc| -> Result<StreamDesc, Response> {
        if !same_config(&d) {
            return Err(perr(
                StatusCode::CONFLICT,
                "config_mismatch",
                "stream exists with different immutable configuration",
                None,
                false,
            ));
        }
        let epoch: [u8; 16] = crate::crypto::unhex(&d.stream_epoch)
            .and_then(|v| v.try_into().ok())
            .unwrap_or_default();
        if d.key_fingerprint != key.fingerprint(&epoch) {
            return Err(perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            ));
        }
        Ok(d)
    };

    let existing = match state.registry.get(&name).await {
        Ok(v) => v,
        Err(e) => {
            return perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
                None,
                true,
            );
        }
    };
    let (created, desc) = match existing {
        Some(d) if crate::http::desc_alive(&d) => match validate_live(d) {
            Ok(d) => (false, d),
            Err(r) => return r,
        },
        Some(_) => match state
            .registry
            .recreate(&name, build_fresh(), |d| !crate::http::desc_alive(d))
            .await
        {
            Ok((true, d)) => (true, d),
            Ok((false, winner)) => match validate_live(winner) {
                Ok(d) => (false, d),
                Err(r) => return r,
            },
            Err(e) => {
                return perr(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                    None,
                    true,
                );
            }
        },
        None => match state.registry.create(build_fresh()).await {
            Ok((true, d)) => (true, d),
            Ok((false, d)) => match validate_live(d) {
                Ok(d) => (false, d),
                Err(r) => return r,
            },
            Err(e) => {
                return perr(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                    None,
                    true,
                );
            }
        },
    };
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    metadata_response(&desc, status)
}

fn metadata_response(desc: &StreamDesc, status: StatusCode) -> Response {
    let created_at = chrono::DateTime::from_timestamp_millis(desc.created_ms)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default();
    let expiry = match (desc.ttl_secs, desc.expires_at_ms) {
        (Some(t), _) => json!({ "idle": format!("{t}s") }),
        (None, Some(at)) => json!({
            "at": chrono::DateTime::from_timestamp_millis(at)
                .map(|t| t.to_rfc3339())
                .unwrap_or_default()
        }),
        _ => serde_json::Value::Null,
    };
    let mut out = json!({
        "name": desc.name,
        "contentType": desc.content_type,
        "createdAt": created_at,
        "sealed": desc.sealed,
    });
    if !expiry.is_null() {
        out["expiry"] = expiry;
    }
    if !desc.watch_definitions.is_empty() {
        out["watches"] = serde_json::to_value(&desc.watch_definitions).unwrap();
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(out.to_string()))
        .unwrap()
}

async fn product_metadata(state: Arc<AppState>, name: String) -> Response {
    match state.registry.get(&name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => metadata_response(&d, StatusCode::OK),
        Ok(_) => perr(
            StatusCode::NOT_FOUND,
            "not_found",
            "stream not found",
            None,
            false,
        ),
        Err(e) => perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &e.to_string(),
            None,
            true,
        ),
    }
}

/// Collection seal (Stage 8 §7, v1: seal-only; atomic final append
/// lands with the lifecycle stage). Durable + monotonic + idempotent.
async fn product_seal(state: Arc<AppState>, name: String, _headers: HeaderMap) -> Response {
    let desc = match state.registry.get(&name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => d,
        Ok(_) => {
            return perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            );
        }
        Err(e) => {
            return perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
                None,
                true,
            );
        }
    };
    if !desc.sealed {
        match state
            .registry
            .cas_update(&name, |d| {
                if d.sealed {
                    return false;
                }
                d.sealed = true;
                true
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return perr(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                    None,
                    true,
                );
            }
        }
    }
    // Seal every live segment identity so appends stop and the raw
    // default-key view reports Stream-Closed at its tail.
    state.registry.invalidate(&name);
    if let Ok(Some(d)) = state.registry.get(&name).await {
        let live: Vec<u32> = match &d.segments {
            Some(m) => m
                .segments
                .iter()
                .filter(|s| s.is_live())
                .map(|s| s.seg_id)
                .collect(),
            None => vec![0],
        };
        for seg_id in live {
            crate::scaler3::seal_segment_identity(&state, &d, seg_id).await;
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(json!({ "sealed": true }).to_string()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_rules() {
        assert!(canonical_name("orders").is_ok());
        assert!(canonical_name("customers/acme/orders").is_ok());
        assert!(canonical_name("").is_err());
        assert!(canonical_name(&"x".repeat(513)).is_err());
        assert!(canonical_name("a//b").is_err());
        assert!(canonical_name("a/./b").is_err());
        assert!(canonical_name("a/../b").is_err());
        assert!(canonical_name("__ds/x").is_err());
        assert!(canonical_name("__ds").is_err());
        assert!(canonical_name("a/records").is_err());
        assert!(canonical_name("a/consumers").is_err());
        assert!(canonical_name("a/watches").is_err());
        assert!(canonical_name("has\u{7}bell").is_err());
    }

    #[test]
    fn subresource_split() {
        assert_eq!(
            split_subresource("customers/acme/orders/records"),
            Some(("customers/acme/orders", "records"))
        );
        assert_eq!(
            split_subresource("orders/consumers/fulfilment"),
            Some(("orders", "consumers/fulfilment"))
        );
        assert_eq!(split_subresource("orders"), None);
    }

    #[test]
    fn idle_durations() {
        assert_eq!(parse_idle_secs("30d"), Some(30 * 86_400));
        assert_eq!(parse_idle_secs("12h"), Some(12 * 3_600));
        assert_eq!(parse_idle_secs("90"), Some(90));
        assert_eq!(parse_idle_secs("0d"), None);
        assert_eq!(parse_idle_secs("x"), None);
    }
}
