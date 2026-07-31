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
        if rest == "records" {
            return match (method.clone(), verb.as_deref()) {
                (Method::POST, None) => product_append(state, name, headers, body, false).await,
                (Method::POST, Some("batch")) => {
                    product_append(state, name, headers, body, true).await
                }
                (Method::GET, None) => product_read(state, name, headers, &query, None).await,
                (Method::GET, Some("long-poll")) => {
                    product_read(state, name, headers, &query, Some("long-poll")).await
                }
                (Method::GET, Some("sse")) => {
                    product_read(state, name, headers, &query, Some("sse")).await
                }
                _ => perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "records accepts POST (append) and GET (read)",
                    None,
                    false,
                ),
            };
        }
        return perr(
            StatusCode::NOT_IMPLEMENTED,
            "not_implemented",
            &format!("product subresource '{rest}' lands with its stage (spec Stage 2)"),
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
        (Method::GET, Some("scan")) => product_scan(state, name, headers, &query).await,
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

// ---- Stage 4: append and appendMany ---------------------------------

const MAX_BATCH_RECORDS: usize = 10_000;
const MAX_ROUTING_KEY_BYTES: usize = 1_024;

/// Both product append routes compile to the ONE committer command the
/// raw surface uses (spec Stage 4 §4): the handler parses the PRODUCT
/// contract — explicit single/batch semantics, Prisma-* names — then
/// drives the shared append path. A single JSON append wraps the value
/// as [value], the protocol's own one-level flattening rule, so an
/// array-valued record stays ONE message; a batch passes its elements
/// straight through.
async fn product_append(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    batch: bool,
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
    let routing_key = headers
        .get("prisma-routing-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if routing_key.len() > MAX_ROUTING_KEY_BYTES {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_routing_key",
            "routing key exceeds 1,024 bytes",
            None,
            false,
        );
    }
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
    let is_json = crate::registry::media_type(&desc.content_type) == "application/json";
    if batch && !is_json {
        // Spec Stage 4 §2.3: no framed byte-batch format is standardized.
        return perr(
            StatusCode::METHOD_NOT_ALLOWED,
            "batch_unsupported_format",
            "records:batch requires a JSON stream",
            None,
            false,
        );
    }
    // Validation order (Stage 4 §5): JSON syntax and batch shape are
    // checked BEFORE enqueue; the shared path handles producer
    // duplicate recognition ahead of later-validation rejections.
    let (wire_body, count): (Bytes, usize) = if is_json {
        if batch {
            let elems: Vec<&serde_json::value::RawValue> = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    return perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_body",
                        &format!("batch must be a JSON array: {e}"),
                        None,
                        false,
                    );
                }
            };
            if elems.is_empty() {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "empty_batch",
                    "appendMany requires at least one record",
                    None,
                    false,
                );
            }
            if elems.len() > MAX_BATCH_RECORDS {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "batch_too_large",
                    "appendMany accepts at most 10,000 records",
                    None,
                    false,
                );
            }
            (body.clone(), elems.len())
        } else {
            if serde_json::from_slice::<&serde_json::value::RawValue>(&body).is_err() {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_body",
                    "append requires one JSON value",
                    None,
                    false,
                );
            }
            // [value]: one-level flattening stores exactly one message,
            // preserving array-valued records (retains a body slice; no
            // DOM reserialization).
            let mut w = Vec::with_capacity(body.len() + 2);
            w.push(b'[');
            w.extend_from_slice(&body);
            w.push(b']');
            (Bytes::from(w), 1)
        }
    } else {
        if body.is_empty() {
            return perr(
                StatusCode::BAD_REQUEST,
                "empty_body",
                "append requires a non-empty body",
                None,
                false,
            );
        }
        (body.clone(), 1)
    };

    // Drive the ONE shared append path with internally-constructed
    // inputs (this is product parsing feeding the engine surface, not a
    // legacy-input translator).
    let mut ih = HeaderMap::new();
    if let Ok(v) = axum::http::HeaderValue::from_str(&key_b64) {
        ih.insert("stream-encryption-key", v);
    }
    if !routing_key.is_empty() {
        if let Ok(v) = axum::http::HeaderValue::from_str(&routing_key) {
            ih.insert("stream-key", v);
        }
    }
    if let Ok(v) = axum::http::HeaderValue::from_str(&desc.content_type) {
        ih.insert("content-type", v);
    }
    for h in ["producer-id", "producer-epoch", "producer-seq"] {
        if let Some(v) = headers.get(h) {
            ih.insert(h, v.clone());
        }
    }
    let has_producer = headers.contains_key("producer-id");
    let raw = crate::http::append(
        state.clone(),
        name.clone(),
        ih,
        axum::body::Body::from(wire_body),
    )
    .await;
    translate_append_response(
        &state,
        &desc,
        &key_b64,
        &routing_key,
        count,
        has_producer,
        raw,
    )
    .await
}

/// Map the shared path's protocol response into the product contract:
/// {cursor, count, duplicate, sealed} on success, the stable product
/// error schema otherwise.
#[allow(clippy::too_many_arguments)]
async fn translate_append_response(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    key_b64: &str,
    routing_key: &str,
    count: usize,
    has_producer: bool,
    raw: Response,
) -> Response {
    let status = raw.status();
    // The raw route answers 204 for every non-producer append; only a
    // PRODUCER 204 means duplicate.
    let dup = has_producer && status == StatusCode::NO_CONTENT;
    if status.is_success() {
        let next_tok = raw
            .headers()
            .get("stream-next-offset")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let sealed = raw.headers().contains_key("stream-closed");
        // Decode the raw token into (seg_id, next) — plain tokens are
        // segment 0, epoch tokens carry their segment.
        let (seg_id, next) = match crate::offsets::parse_ep(&next_tok) {
            Ok((e, o)) => (e, o.scan_from()),
            Err(_) => match crate::offsets::Offset::parse(&next_tok) {
                Ok(o) => (0, o.scan_from()),
                Err(_) => (0, 0),
            },
        };
        let cursor = match crate::crypto::StreamKey::from_b64(key_b64) {
            Ok(k) => {
                let epoch = desc.epoch_bytes().unwrap_or_default();
                crate::product_cursor::KeyCursor {
                    epoch,
                    key_hash: crate::crypto::stream_hash(routing_key),
                    seg_id,
                    offset: next,
                }
                .encode(&k)
            }
            Err(_) => String::new(),
        };
        let _ = state;
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(
                json!({
                    "cursor": cursor,
                    "count": if dup { 0 } else { count },
                    "duplicate": dup,
                    "sealed": sealed,
                })
                .to_string(),
            ))
            .unwrap();
    }
    // Error translation: keep the status, restate in the product schema.
    let (code, message, retryable) = match status.as_u16() {
        404 => ("not_found", "stream not found", false),
        403 => ("stale_or_wrong_credentials", "forbidden", false),
        409 if raw.headers().contains_key("stream-closed") => {
            ("sealed", "collection is sealed", false)
        }
        409 => ("conflict", "producer or configuration conflict", false),
        413 => ("body_too_large", "request body exceeds the limit", false),
        429 => ("rate_limited", "admission or rate limit", true),
        503 => ("temporarily_unavailable", "retry shortly", true),
        _ => ("append_failed", "append failed", false),
    };
    let mut r = perr(status, code, message, None, retryable);
    if let Some(ra) = raw.headers().get("retry-after") {
        r.headers_mut().insert("retry-after", ra.clone());
    }
    r
}

// ---- Stage 6: read, subscribe, scan ---------------------------------

const SCAN_TTL_MS: i64 = 6 * 3600 * 1000;
const SCAN_DEFAULT_BYTES: usize = 4 << 20;
const READ_MAX_BYTES_CAP: usize = 8 << 20;

/// Query-string map with one-shot percent-decoding of values. Product
/// SDKs percent-encode routing keys; '+' is NOT treated as a space.
fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    fn pct(v: &str) -> String {
        let b = v.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 < b.len() + 1 {
                let hex = b.get(i + 1..i + 3);
                if let Some(h) = hex {
                    if let Ok(x) = u8::from_str_radix(std::str::from_utf8(h).unwrap_or("zz"), 16) {
                        out.push(x);
                        i += 3;
                        continue;
                    }
                }
            }
            out.push(b[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            Some((k.to_string(), pct(v)))
        })
        .collect()
}

fn multi_or_pending(desc: &StreamDesc) -> bool {
    desc.segments
        .as_ref()
        .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
}

/// Product cursor position -> raw offset token, mirroring the raw
/// dispatch's own token-class rule (plain tokens on the single-segment
/// path, epoch tokens on the lineage path).
fn cursor_to_raw_token(desc: &StreamDesc, seg_id: u32, offset: u64) -> String {
    let off = if offset == 0 {
        crate::offsets::Offset::START
    } else {
        crate::offsets::Offset(Some(offset - 1))
    };
    if multi_or_pending(desc) {
        crate::offsets::encode_ep(seg_id, off)
    } else {
        off.encode()
    }
}

/// Raw Stream-Next-Offset token -> (segment, next offset). Plain tokens
/// belong to the stream's sole segment.
fn raw_token_to_pos(desc: &StreamDesc, rk: &str, tok: &str) -> (u32, u64) {
    match crate::offsets::parse_ep(tok) {
        Ok((e, o)) => (e, o.scan_from()),
        Err(_) => match crate::offsets::Offset::parse(tok) {
            Ok(o) => (desc.resolve_segment(rk).seg_id, o.scan_from()),
            Err(_) => (desc.resolve_segment(rk).seg_id, 0),
        },
    }
}

/// Earliest lineage position for a key — where `from: "beginning"`
/// starts when a live transport needs an explicit token.
fn start_token(desc: &StreamDesc, rk: &str) -> String {
    if multi_or_pending(desc) {
        let point = StreamDesc::key_point(rk);
        let first = desc
            .segments
            .as_ref()
            .and_then(|m| {
                m.segments
                    .iter()
                    .filter(|sg| sg.contains(point))
                    .min_by_key(|sg| (sg.created_ms, sg.seg_id))
            })
            .map(|sg| sg.seg_id)
            .unwrap_or(0);
        crate::offsets::encode_ep(first, crate::offsets::Offset::START)
    } else {
        crate::offsets::Offset::START.encode()
    }
}

fn translate_read_error(raw: Response) -> Response {
    let status = raw.status();
    let (code, message, retryable) = match status.as_u16() {
        404 => ("not_found", "stream not found", false),
        403 => ("wrong_key", "encryption key mismatch", false),
        410 => ("gone", "stream expired or deleted", false),
        429 => ("rate_limited", "admission or rate limit", true),
        400 => ("invalid_cursor", "invalid cursor or read position", false),
        503 => ("temporarily_unavailable", "retry shortly", true),
        _ => ("read_failed", "read failed", true),
    };
    let mut r = perr(status, code, message, None, retryable);
    if let Some(ra) = raw.headers().get("retry-after") {
        r.headers_mut().insert("retry-after", ra.clone());
    }
    r
}

/// Product keyed read (spec Stage 6 §2-4): parse the PRODUCT contract —
/// routingKey, signed cursor, maxBytes — then drive the one shared read
/// dispatch (lineage traversal, seal-gap discipline, long-poll waiters,
/// SSE streamers) with internally constructed inputs, and restate the
/// response in product terms. A product response never carries a
/// Stream-* header.
async fn product_read(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    query: &str,
    live: Option<&'static str>,
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
    let q = parse_query(query);
    let rk = q.get("routingKey").cloned().unwrap_or_default();
    if rk.len() > 1024 {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_routing_key",
            "routing key exceeds 1,024 bytes",
            None,
            false,
        );
    }
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
    let (skey, epoch) = match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        crate::http::KeyCheck::Wrong => {
            return perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            );
        }
        _ => {
            return perr(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Prisma-Encryption-Key required",
                None,
                false,
            );
        }
    };
    let kh = crate::crypto::stream_hash(&rk);

    // Cursor -> raw offset token. The three token classes are enforced
    // here: a scan cursor (or garbage) on the key-read endpoint is a
    // 400, never a misread.
    let offset: Option<String> = match q.get("cursor").map(String::as_str) {
        None | Some("") | Some("beginning") => {
            if live.is_some() {
                Some(start_token(&desc, &rk))
            } else {
                None
            }
        }
        Some("now") => Some("now".to_string()),
        Some(c) => match crate::product_cursor::KeyCursor::decode(c, &skey, &epoch, &kh) {
            Ok(kc) => Some(cursor_to_raw_token(&desc, kc.seg_id, kc.offset)),
            Err("wrong_cursor_kind") => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "cursor is not a key cursor for this endpoint",
                    None,
                    false,
                );
            }
            Err(_) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "cursor does not belong to this stream and routing key",
                    None,
                    false,
                );
            }
        },
    };
    let max_bytes = q
        .get("maxBytes")
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP));
    let timeout = q
        .get("waitMs")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|n| format!("{n}ms"));

    let params = crate::http::ReadParams {
        offset,
        format: None,
        live: live.map(str::to_string),
        timeout,
        key: Some(rk.clone()),
        cursor: None,
        sig: None,
        max_bytes,
    };
    let mut ih = HeaderMap::new();
    if let Ok(v) = axum::http::HeaderValue::from_str(&key_b64) {
        ih.insert("stream-encryption-key", v);
    }
    let surface = if live == Some("sse") {
        crate::http::SseSurface::Product
    } else {
        crate::http::SseSurface::Raw
    };
    let raw = crate::http::read_inner(state, name, params, ih, false, true, surface).await;

    // SSE connections stream product control frames already; pass the
    // stream through untouched. Anything else is translated.
    if raw
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        == Some("text/event-stream")
    {
        return raw;
    }
    let status = raw.status();
    if !status.is_success() {
        return translate_read_error(raw);
    }
    let next_tok = raw
        .headers()
        .get("stream-next-offset")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let up_to_date = raw.headers().contains_key("stream-up-to-date");
    let sealed = raw.headers().contains_key("stream-closed");
    let content_type = raw
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or(axum::http::HeaderValue::from_static("application/json"));
    let (seg_id, next) = raw_token_to_pos(&desc, &rk, &next_tok);
    let cursor_out = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: kh,
        seg_id,
        offset: next,
    }
    .encode(&skey);
    let (parts, body) = raw.into_parts();
    let mut r = Response::builder()
        .status(parts.status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-store")
        .header("Prisma-Next-Cursor", cursor_out);
    if up_to_date {
        r = r.header("Prisma-Up-To-Date", "true");
    }
    if sealed {
        r = r.header("Prisma-Sealed", "true");
    }
    r.body(body).unwrap()
}

/// Segment id -> (engine identity, shard route) for scan resume. The
/// map only gains segments, so a cursor segment the map no longer knows
/// is an invalid cursor, not a silent restart.
fn seg_identity(desc: &StreamDesc, seg_id: u32) -> Option<([u8; 16], [u8; 16])> {
    match &desc.segments {
        Some(map) if !map.segments.is_empty() => map
            .segments
            .iter()
            .find(|sg| sg.seg_id == seg_id)
            .map(|sg| {
                (
                    desc.dynamic_segment_identity(seg_id),
                    desc.segment_route(sg),
                )
            }),
        _ => {
            let ro = desc.resolve_segment("");
            (ro.seg_id == seg_id).then_some((ro.identity, ro.shard_route))
        }
    }
}

/// Cross-key snapshot scan (spec Stage 6 §5): deterministic traversal
/// of the segments captured at snapshot creation, each record that
/// existed then exactly once, in (creation, segment id, offset) order —
/// explicitly NOT a cross-key append order. The snapshot lives entirely
/// in the signed cursor; creating a scan costs no control-plane write.
async fn product_scan(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    query: &str,
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
    let q = parse_query(query);
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
    let (skey, epoch) = match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        crate::http::KeyCheck::Wrong => {
            return perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            );
        }
        _ => {
            return perr(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Prisma-Encryption-Key required",
                None,
                false,
            );
        }
    };
    let now = crate::shard::now_ms();

    let sc = match q
        .get("cursor")
        .map(String::as_str)
        .filter(|c| !c.is_empty())
    {
        Some(c) => match crate::product_cursor::ScanCursor::decode(c, &skey, &epoch, now) {
            Ok(sc) => sc,
            Err("scan_expired") => {
                return perr(
                    StatusCode::GONE,
                    "scan_expired",
                    "scan snapshot expired; start a new scan",
                    None,
                    false,
                );
            }
            Err("wrong_cursor_kind") => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "cursor is not a scan cursor",
                    None,
                    false,
                );
            }
            Err(_) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "invalid scan cursor",
                    None,
                    false,
                );
            }
        },
        None => {
            // Snapshot creation: every segment in (creation, id) order
            // with its boundary frozen NOW — later appends and later
            // successors are excluded by construction.
            let mut segs: Vec<(u32, u64)> = Vec::new();
            let map_version = match &desc.segments {
                Some(map) if !map.segments.is_empty() => {
                    let mut v: Vec<_> = map.segments.iter().collect();
                    v.sort_by_key(|sg| (sg.created_ms, sg.seg_id));
                    for sg in v {
                        let end = match sg.sealed_next_offset {
                            Some(e) => e,
                            None => {
                                let identity = desc.dynamic_segment_identity(sg.seg_id);
                                let engine = match state.engine_for(&desc.segment_route(sg)).await {
                                    Ok(e) => e,
                                    Err(r) => return translate_read_error(r),
                                };
                                match engine.stream_handle(identity).await {
                                    Ok(h) => h.state.lock().unwrap().durable.next,
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
                        };
                        segs.push((sg.seg_id, end));
                    }
                    map.version
                }
                _ => {
                    let ro = desc.resolve_segment("");
                    let engine = match state.engine_for(&ro.shard_route).await {
                        Ok(e) => e,
                        Err(r) => return translate_read_error(r),
                    };
                    let end = match engine.stream_handle(ro.identity).await {
                        Ok(h) => h.state.lock().unwrap().durable.next,
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
                    segs.push((ro.seg_id, end));
                    0
                }
            };
            crate::product_cursor::ScanCursor {
                epoch,
                map_version,
                segments: segs,
                current_index: 0,
                current_offset: 0,
                expires_at_ms: now + SCAN_TTL_MS,
            }
        }
    };

    let max = q
        .get("maxBytes")
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP))
        .unwrap_or(SCAN_DEFAULT_BYTES);
    let is_json = crate::registry::media_type(&desc.content_type) == "application/json";

    let mut idx = sc.current_index as usize;
    let mut off = sc.current_offset;
    let mut spent = 0usize;
    let mut body = Vec::with_capacity(4096);
    body.push(b'[');
    let mut n_items = 0usize;
    while idx < sc.segments.len() && spent < max {
        let (seg_id, snap_end) = sc.segments[idx];
        if off >= snap_end {
            idx += 1;
            off = 0;
            continue;
        }
        let Some((identity, route)) = seg_identity(&desc, seg_id) else {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "scan cursor names an unknown segment",
                None,
                false,
            );
        };
        let engine = match state.engine_for(&route).await {
            Ok(e) => e,
            Err(r) => return translate_read_error(r),
        };
        let handle = match engine.stream_handle(identity).await {
            Ok(h) => h,
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
        state.keys.put(identity, skey.clone(), epoch);
        let out =
            match crate::http::read_merged(&skey, &epoch, &handle, &engine, off, None, max - spent)
                .await
            {
                Ok(o) => o,
                Err(m) => {
                    return perr(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &m,
                        None,
                        true,
                    );
                }
            };
        let mut progressed = false;
        for r in &out.recs {
            if r.off >= snap_end {
                break;
            }
            if n_items > 0 {
                body.push(b',');
            }
            body.extend_from_slice(b"{\"routingKey\":");
            body.extend_from_slice(
                serde_json::to_string(&r.rkey)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            if is_json {
                body.extend_from_slice(b",\"value\":");
                body.extend_from_slice(&r.payload);
            } else {
                use base64::Engine;
                body.extend_from_slice(b",\"valueB64\":\"");
                body.extend_from_slice(
                    base64::engine::general_purpose::STANDARD
                        .encode(&r.payload)
                        .as_bytes(),
                );
                body.push(b'"');
            }
            body.push(b'}');
            n_items += 1;
            spent += r.payload.len() + r.rkey.len() + 24;
            off = r.off + 1;
            progressed = true;
        }
        if out.completed {
            // Everything below the snapshot boundary was served (the
            // durable end only grows past it).
            off = snap_end;
        } else if !progressed {
            break; // budget exhausted mid-record; resume from `off`
        }
    }
    body.push(b']');
    let complete = idx >= sc.segments.len();
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store");
    if complete {
        r = r.header("Prisma-Scan-Complete", "true");
    } else {
        let next = crate::product_cursor::ScanCursor {
            epoch,
            map_version: sc.map_version,
            segments: sc.segments.clone(),
            current_index: idx as u32,
            current_offset: off,
            expires_at_ms: sc.expires_at_ms,
        };
        r = r.header("Prisma-Next-Scan-Cursor", next.encode(&skey));
    }
    r.body(Body::from(body)).unwrap()
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
