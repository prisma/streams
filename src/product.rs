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
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
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
    // A name that itself reads as a subresource path would be
    // unaddressable: `x/consumers/records` as a COLLECTION can never be
    // written, because that URL already means consumer "records" on
    // collection `x`. Refuse it at creation rather than hand out a name
    // whose own URL points somewhere else.
    if split_subresource(raw).is_some() {
        return bad(
            "invalid_name",
            "this name is already a subresource path (…/records, …/consumers/{name}, …/watches/…)",
        );
    }
    Ok(raw.to_string())
}

/// The experimental product names this route REJECTS instead of
/// translating (spec Stage 8 §5).
fn reject_legacy_inputs(headers: &HeaderMap, query: &str, method: &Method) -> Option<Response> {
    // Removed experimental names are rejected, never translated (spec
    // Stage 1 §6, Stage 7 §12, Stage 8 §5): credential/routing names,
    // the profile machinery, and header-based configuration.
    for h in [
        "stream-encryption-key",
        "stream-key",
        "stream-profile",
        "stream-touch-templates",
        "stream-queue-max-deliveries",
        "stream-ordering",
        "stream-segments",
        "stream-scaling",
        "stream-ttl",
        "stream-expires-at",
    ] {
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
        // Reads take ?routingKey=; appends take the Prisma-Routing-Key
        // HEADER. A query parameter on an append used to be ignored in
        // silence, which writes the record to the DEFAULT key and looks
        // exactly like success — the caller then reads their key back
        // empty. Say so instead.
        if k == "routingKey" && !matches!(*method, Method::GET | Method::HEAD) {
            return Some(perr(
                StatusCode::BAD_REQUEST,
                "unknown_field",
                "routingKey is a query field for reads; appends carry the                  Prisma-Routing-Key header",
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

/// Every resource the product surface defines. Requests are classified
/// into exactly one of these BEFORE anything is authorized, because
/// authorization differs per resource and a substring test cannot tell
/// these apart: collection names are hierarchical, so `acme/watches/x/
/// keys/y/extra` is a perfectly legal COLLECTION whose path contains
/// every fragment a watch URL has. Deciding "this looks like a signed
/// watch" by `path.contains()` let that name — and its `/records`
/// subresource — skip the account token entirely.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ProductRoute {
    Collection {
        name: String,
    },
    Records {
        name: String,
    },
    Consumer {
        name: String,
        consumer: String,
    },
    Watches {
        name: String,
    },
    Watch {
        name: String,
        watch: String,
    },
    /// The ONE route that can authorize itself, with a signature.
    WatchWait {
        name: String,
        watch: String,
        key: String,
    },
}

/// Split a trailing `:verb` off the final segment. Only the known verbs
/// count — a colon is legal inside a collection name.
pub(crate) fn strip_verb(path: &str) -> (&str, Option<&str>) {
    const VERBS: [&str; 7] = [
        "batch", "long-poll", "sse", "pull", "settle", "seal", "scan",
    ];
    match path.rsplit_once(':') {
        Some((p, v)) if !v.contains('/') && VERBS.contains(&v) => (p, Some(v)),
        _ => (path, None),
    }
}

/// Parse a request path into exactly one resource. Pure: no auth, no
/// state, no I/O — so the auth gate can run before the body is read.
pub(crate) fn classify_route(path: &str) -> Result<ProductRoute, Response> {
    let (path, _) = strip_verb(path);
    let Some((stream, rest)) = split_subresource(path) else {
        return Ok(ProductRoute::Collection {
            name: canonical_name(path).map_err(|r| r)?,
        });
    };
    let name = canonical_name(stream)?;
    if rest == "records" {
        return Ok(ProductRoute::Records { name });
    }
    if let Some(cname) = rest.strip_prefix("consumers/") {
        let Some(consumer) = valid_consumer_name(cname) else {
            return Err(perr(
                StatusCode::BAD_REQUEST,
                "invalid_consumer_name",
                "consumer names are one path-safe segment, 1-128 bytes",
                None,
                false,
            ));
        };
        return Ok(ProductRoute::Consumer { name, consumer });
    }
    if rest == "watches" {
        return Ok(ProductRoute::Watches { name });
    }
    if let Some(wrest) = rest.strip_prefix("watches/") {
        // `{watch}/keys/{key}` is the signed observation resource, and
        // it is exact: the watch name is one segment, the key is one
        // segment, and nothing may follow.
        if let Some((watch, key)) = wrest.split_once("/keys/") {
            if !watch.is_empty()
                && !watch.contains('/')
                && !key.is_empty()
                && !key.contains('/')
            {
                return Ok(ProductRoute::WatchWait {
                    name,
                    watch: watch.to_string(),
                    key: key.to_string(),
                });
            }
            return Err(perr(
                StatusCode::NOT_FOUND,
                "unknown_route",
                "watch observation URLs are /watches/{watch}/keys/{key}",
                None,
                false,
            ));
        }
        if wrest.contains('/') {
            return Err(perr(
                StatusCode::NOT_FOUND,
                "unknown_route",
                "watch names are one path segment",
                None,
                false,
            ));
        }
        return Ok(ProductRoute::Watch {
            name,
            watch: wrest.to_string(),
        });
    }
    Err(perr(
        StatusCode::NOT_FOUND,
        "unknown_route",
        &format!("unknown product subresource '{rest}'"),
        None,
        false,
    ))
}

/// ACCOUNT authorization (spec Stage 8 §14). The token authorizes
/// account/product operations; the encryption key is a SEPARATE
/// credential that proves record access, and neither substitutes for
/// the other. The one exception is an exact signed watch-observation
/// URL, a delegated capability that authorizes itself — verified
/// against the descriptor's persisted verifier inside the handler.
///
/// Returns the 401 to send, or None when the request may proceed.
pub(crate) fn product_auth_gate(
    state: &AppState,
    path: &str,
    method: &Method,
    query: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    if method == Method::OPTIONS {
        return None; // preflights carry no credentials, by definition
    }
    let capability = matches!(classify_route(path), Ok(ProductRoute::WatchWait { .. }))
        && method == Method::GET
        && query.split('&').any(|kv| kv.starts_with("sig="));
    if capability || crate::http::authorized(state, headers) {
        return None;
    }
    Some(perr(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "bearer token required",
        None,
        false,
    ))
}

/// Product responses are browser-facing: a preflight that passes and an
/// actual response the browser then blocks is no better than no CORS at
/// all. Applied to EVERY plural-route response — successes, errors,
/// 204s, long polls, SSE — and it must expose the product's own headers
/// or a browser client cannot read cursors, sealed state, or Retry-After.
pub(crate) fn with_product_cors(mut resp: Response) -> Response {
    let h = resp.headers_mut();
    if !h.contains_key("access-control-allow-origin") {
        h.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    }
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static(
            "content-type, retry-after, prisma-next-cursor, prisma-up-to-date, \
             prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, \
             prisma-routing-key",
        ),
    );
    resp
}


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
    // Browser preflight: answered before authorization, because a
    // preflight carries no credentials by definition (the browser sends
    // Authorization only on the actual request).
    if method == Method::OPTIONS {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("access-control-allow-origin", "*")
            .header(
                "access-control-allow-methods",
                "GET, PUT, POST, DELETE, OPTIONS",
            )
            // Authorization is a forbidden-wildcard request header: a
            // browser does NOT treat `*` as covering it, so a bearer
            // request fails preflight even though everything else works.
            .header(
                "access-control-allow-headers",
                "authorization, content-type, prisma-encryption-key, \
                 prisma-routing-key, producer-id, producer-epoch, producer-seq, \
                 if-none-match",
            )
            .header("access-control-expose-headers", "*")
            .header("access-control-max-age", "600")
            .body(Body::empty())
            .unwrap();
    }
    // Authorized by PARSED route (see product_auth_gate). The wrapper
    // already ran this before reading the body; repeating it is cheap
    // and keeps direct callers safe.
    if let Some(r) = product_auth_gate(&state, &path, &method, &query, &headers) {
        return r;
    }
    if let Some(r) = reject_legacy_inputs(&headers, &query, &method) {
        return r;
    }
    let (_, verb) = strip_verb(&path);
    let verb = verb.map(str::to_string);
    let route = match classify_route(&path) {
        Ok(r) => r,
        Err(r) => return r,
    };
    match route {
        ProductRoute::Records { name } => {
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
        ProductRoute::Consumer {
            name,
            consumer: cname,
        } => {
            return match (method.clone(), verb.as_deref()) {
                (Method::PUT, None) => {
                    product_consumer_put(state, name, cname, headers, body).await
                }
                (Method::GET, None) => product_consumer_get(state, name, cname, headers).await,
                (Method::DELETE, None) => {
                    product_consumer_delete(state, name, cname, headers).await
                }
                (Method::POST, Some("pull")) => {
                    product_consumer_pull(state, name, cname, headers, body).await
                }
                (Method::POST, Some("settle")) => {
                    product_consumer_settle(state, name, cname, headers, body).await
                }
                _ => perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "consumers accept PUT/GET/DELETE and POST :pull/:settle",
                    None,
                    false,
                ),
            };
        }
        ProductRoute::Watches { name } => {
            return if method == Method::GET {
                product_watches_list(state, name).await
            } else {
                perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "watches are read-only (GET)",
                    None,
                    false,
                )
            };
        }
        ProductRoute::Watch { name, watch } => {
            return if method == Method::GET {
                product_watch_get(state, name, watch).await
            } else {
                perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "watches are read-only (GET)",
                    None,
                    false,
                )
            };
        }
        ProductRoute::WatchWait { name, watch, key } => {
            return if method == Method::GET {
                product_watch_wait(state, name, watch, key, headers, &query).await
            } else {
                perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "watches are read-only (GET)",
                    None,
                    false,
                )
            };
        }
        ProductRoute::Collection { .. } => {}
    }
    let name = match canonical_name(&strip_verb(&path).0.to_string()) {
        Ok(n) => n,
        Err(r) => return r,
    };
    match (method.clone(), verb.as_deref()) {
        (Method::PUT, None) => product_create(state, name, headers, body).await,
        (Method::GET, None) => product_metadata(state, name).await,
        (Method::DELETE, None) => crate::http::product_delete(state, name).await,
        (Method::POST, Some("seal")) => product_seal(state, name, headers, body).await,
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

/// `customers/acme/orders/records` → (`customers/acme/orders`, `records`).
///
/// Collection names are hierarchical, so a subresource is a SUFFIX of
/// the path, matched against the shapes the product routes actually
/// define (spec §4.1: "product subresource suffixes are not parsed from
/// the wildcard name because explicit route matching occurs first").
/// Searching for the first `/records/` anywhere instead would split
/// `customers/records/2026/records` after `customers` and address a
/// collection nobody named.
///
/// Shapes are tried longest-first, and a candidate only wins if what
/// remains is addressable as a collection — that is what resolves
/// `x/consumers/records`, where the trailing segment is a consumer
/// named "records" and not a records subresource, since no collection
/// may be called `x/consumers`.
fn split_subresource(path: &str) -> Option<(&str, &str)> {
    let seg: Vec<&str> = path.split('/').collect();
    let n = seg.len();
    // (segments consumed from the end, the shape's leading keyword)
    let shapes: [(usize, &str); 5] = [
        (4, "watches"), // watches/{watch}/keys/{key}
        (2, "watches"), // watches/{watch}
        (2, "consumers"), // consumers/{consumer}
        (1, "watches"), // watches
        (1, "records"), // records
    ];
    for (take, head) in shapes {
        if n <= take || seg[n - take] != head {
            continue;
        }
        if take == 4 && seg[n - 2] != "keys" {
            continue;
        }
        let stream_len: usize = seg[..n - take].iter().map(|s| s.len() + 1).sum();
        let stream = &path[..stream_len - 1];
        if stream.is_empty() || !addressable_name(stream) {
            continue;
        }
        return Some((stream, &path[stream_len..]));
    }
    None
}

/// Whether a prefix could be a collection name at all. A name may not
/// end in a reserved subresource word (enforced at create), so a
/// candidate split that would require one is not a real split.
fn addressable_name(name: &str) -> bool {
    !name
        .rsplit('/')
        .next()
        .is_some_and(|last| RESERVED_FINAL_SEGMENTS.contains(&last))
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
        // Persist the URL-signature verifier now, while a key holder is
        // here to derive it from. The server never stores the stream
        // key, so this is its only chance: after create, a signed watch
        // URL must verify on any process, cold, with nothing cached.
        if let Some(ep) = d.epoch_bytes() {
            use base64::Engine;
            let tok = crate::crypto::touch_token(&key, &ep);
            d.watch_sig_key = Some(
                base64::engine::general_purpose::STANDARD
                    .encode(crate::crypto::wait_sig_key(&tok, &ep)),
            );
        }
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
            .recreate(&name, build_fresh(), |d| {
                // Never replace a descriptor that still backs live
                // forks (audit P0: the product path replaced the
                // soft-deleted/expired sources the raw path blocks,
                // because desc_alive() is false for both).
                !crate::http::desc_alive(d) && !d.soft_deleted && d.fork_children.is_empty()
            })
            .await
        {
            Ok((true, d)) => (true, d),
            Ok((false, winner)) => {
                // The predicate declined: either a live winner (normal
                // idempotent path) or a retained fork source, which is
                // a conflict rather than a config mismatch.
                if winner.soft_deleted || !winner.fork_children.is_empty() {
                    return perr(
                        StatusCode::CONFLICT,
                        "gone",
                        "name is retained for live forks",
                        None,
                        false,
                    );
                }
                match validate_live(winner) {
                    Ok(d) => (false, d),
                    Err(r) => return r,
                }
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
        // The incarnation salt. Not a secret — it is the HKDF salt, and
        // it grants nothing on its own — but a client needs it to
        // derive watch-observation signatures from the stream key
        // without a round trip. Only collections that HAVE watches
        // carry it, since nothing else uses it client-side.
        out["epoch"] = json!(desc.stream_epoch);
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
        // A half-built collection is not a collection yet: reporting its
        // metadata would describe content that is not durable.
        Ok(Some(d)) if crate::http::desc_alive(&d) && crate::http::initializing(&d) => perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            None,
            true,
        ),
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
async fn product_seal(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Atomic final append (spec Stage 8 §7.2): {final, routingKey}
    // rides ONE committer command with the closure — the same
    // append-and-close the raw protocol defines. Producer headers
    // dedup a retried final append.
    if !body.is_empty() {
        #[derive(serde::Deserialize, Default)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        struct SealDoc {
            // Double Option: serde collapses a PRESENT `null` into
            // `None`, so `{"final": null}` silently became a seal with
            // no final record — dropping a perfectly valid JSON null
            // that the SDK sends whenever T admits it. The outer layer
            // is presence, the inner is the value.
            #[serde(default, deserialize_with = "deserialize_some")]
            r#final: Option<Option<serde_json::Value>>,
            #[serde(default)]
            routing_key: Option<String>,
        }
        let doc: SealDoc = match serde_json::from_slice(&body) {
            Ok(d) => d,
            Err(e) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_body",
                    &format!("seal request: {e}"),
                    None,
                    false,
                );
            }
        };
        if let Some(fin) = doc.r#final.map(|v| v.unwrap_or(serde_json::Value::Null)) {
            // EVERY deterministic error first. Publishing the intent
            // before validating let a request that could never complete
            // — no key, wrong key, unusable routing key — leave the
            // collection permanently Sealing, owing a final record from
            // a caller who was refused.
            let Some(key_b64) = product_key(&headers) else {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "missing_key",
                    "Prisma-Encryption-Key required",
                    None,
                    false,
                );
            };
            match state.registry.get(&name).await {
                Ok(Some(d)) if crate::http::desc_alive(&d) => {
                    if crate::http::initializing(&d) {
                        return perr(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "creating",
                            "stream is still being created; retry",
                            None,
                            true,
                        );
                    }
                    if !matches!(
                        crate::http::check_key(Some(&key_b64), &d),
                        crate::http::KeyCheck::Ok(..)
                    ) {
                        return perr(
                            StatusCode::FORBIDDEN,
                            "wrong_key",
                            "encryption key mismatch",
                            None,
                            false,
                        );
                    }
                }
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
            }
            // Everything that can PERMANENTLY prevent the promised
            // append, checked before the promise is made. A seal intent
            // that names a record the append path will always reject
            // leaves the collection sealing forever, owing something
            // undeliverable.
            let rk = doc.routing_key.clone().unwrap_or_default();
            if rk.len() > MAX_ROUTING_KEY_BYTES {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_routing_key",
                    "routing key exceeds 1,024 bytes",
                    None,
                    false,
                );
            }
            if axum::http::HeaderValue::from_str(&rk).is_err() {
                // It travels as a header on the internal append; a value
                // that cannot be one would silently land on the DEFAULT
                // key while the durable intent names another.
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_routing_key",
                    "routing key contains characters that cannot be transmitted",
                    None,
                    false,
                );
            }
            for h in ["producer-id", "producer-epoch", "producer-seq"] {
                if let Some(v) = headers.get(h) {
                    if v.to_str().is_err() {
                        return perr(
                            StatusCode::BAD_REQUEST,
                            "invalid_producer",
                            &format!("{h} is not a valid header value"),
                            None,
                            false,
                        );
                    }
                }
            }
            let has_any_producer = ["producer-id", "producer-epoch", "producer-seq"]
                .iter()
                .any(|h| headers.contains_key(*h));
            if has_any_producer
                && !["producer-id", "producer-epoch", "producer-seq"]
                    .iter()
                    .all(|h| headers.contains_key(*h))
            {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_producer",
                    "producer requests need Producer-Id, Producer-Epoch and Producer-Seq",
                    None,
                    false,
                );
            }
            {
                // A record larger than the ingest bucket's CAPACITY can
                // never be admitted, however long the caller waits.
                let l = crate::usage::limits();
                let bytes = fin.to_string().len() as f64;
                if bytes > l.bytes_per_sec * l.burst_secs {
                    return perr(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "payload_too_large",
                        "the final record exceeds the per-stream ingest capacity",
                        None,
                        false,
                    );
                }
            }
            // Only now: enter Sealing. Ordinary appends are refused from
            // here, so nothing can land between the final record and the
            // segment closes, and the operation id makes the final
            // append itself idempotent under retry.
            let op_id = seal_op_id(&fin, doc.routing_key.as_deref().unwrap_or_default());
            let intent = crate::registry::SealIntent::Final {
                routing_key: doc.routing_key.clone().unwrap_or_default(),
                request_hash: op_id.clone(),
                final_committed: false,
            };
            match enter_sealing(&state, &name, &op_id, intent).await {
                Ok(()) => {}
                // Empty message = this exact seal already completed.
                Err(m) if m.is_empty() => {
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::CACHE_CONTROL, "no-store")
                        .body(Body::from(json!({ "sealed": true }).to_string()))
                        .unwrap();
                }
                Err(m) => return perr(StatusCode::CONFLICT, "sealed", &m, None, false),
            }
            let mut ih = HeaderMap::new();
            if let Ok(v) = axum::http::HeaderValue::from_str(&key_b64) {
                ih.insert("prisma-encryption-key", v);
            }
            if let Some(rk) = &doc.routing_key {
                if let Ok(v) = axum::http::HeaderValue::from_str(rk) {
                    ih.insert("prisma-routing-key", v);
                }
            }
            for h in ["producer-id", "producer-epoch", "producer-seq"] {
                if let Some(v) = headers.get(h) {
                    ih.insert(h, v.clone());
                }
            }
            // The final record carries the seal operation as its
            // producer identity when the caller supplied none, so a
            // resumed seal never writes a second final record.
            if !ih.contains_key("producer-id") {
                // Header-SAFE id: a header value may not contain control
                // bytes, so a NUL-delimited id silently fails to insert
                // (and the append then loses its producer identity).
                if let Ok(v) = axum::http::HeaderValue::from_str(&format!("prisma.seal.{op_id}")) {
                    ih.insert("producer-id", v);
                    ih.insert("producer-epoch", axum::http::HeaderValue::from_static("1"));
                    ih.insert("producer-seq", axum::http::HeaderValue::from_static("0"));
                }
            }
            let resp = product_append_sealing(
                state.clone(),
                name.clone(),
                ih,
                Bytes::from(fin.to_string()),
                op_id.clone(),
            )
            .await;
            if !resp.status().is_success() {
                return resp;
            }
            // The record is durable: record that BEFORE any segment
            // closes, so the transition can be finished by anyone from
            // here on and by nobody else before.
            if let Err(e) = mark_final_committed(&state, &name, &op_id).await {
                return perr(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e, None, true);
            }
            return match run_seal(&state, &name, Some(op_id)).await {
                Ok(()) => Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::from(json!({ "sealed": true }).to_string()))
                    .unwrap(),
                Err(m) => perr(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &m,
                    None,
                    true,
                ),
            };
        }
    }
    product_seal_only(state, name, headers).await
}

/// Enter Sealing for a seal-with-final operation. A different seal
/// already in flight is a conflict; the SAME operation resumes.
/// What a seal-intent CAS actually did. A declined CAS is not a
/// failure and not a success — it is information, and treating it as
/// either is how a collection ends up stuck (Sealing over a pending
/// split, which phase B then refuses to finish) or how a client is told
/// `{"sealed": true}` about a descriptor that is still mid-transition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EnterSeal {
    Installed,
    /// This exact operation already owns the IN-FLIGHT transition.
    AlreadyOurs,
    /// This exact operation already finished: answer idempotent success.
    AlreadyCompleted,
    /// Somebody else's seal is already terminal.
    AlreadySealed,
    /// A topology transition is in flight; resolve it and retry.
    PendingTopology,
    /// Somebody else's seal owns the collection.
    Conflicting(String),
    Missing,
}

/// Install a seal intent, classifying every outcome. THE serialization
/// point: it installs only over a descriptor that is open, unclaimed
/// and topologically quiet, so once it wins, phase A cannot start a new
/// transition (phase A refuses sealing) and phase B cannot publish one
/// (phase B refuses sealing).
pub(crate) async fn enter_sealing_cas(
    state: &Arc<AppState>,
    name: &str,
    op_id: &str,
    intent: &crate::registry::SealIntent,
) -> Result<EnterSeal, String> {
    let mut outcome = EnterSeal::Missing;
    let installed = state
        .registry
        .cas_update(name, |d| {
            if !crate::http::desc_alive(d) {
                outcome = EnterSeal::Missing;
                return false;
            }
            if d.sealed {
                outcome = if d.seal_op.as_deref() == Some(op_id) && !op_id.is_empty() {
                    EnterSeal::AlreadyCompleted
                } else {
                    EnterSeal::AlreadySealed
                };
                return false;
            }
            if let Some(sl) = &d.sealing {
                outcome = if sl.operation_id == op_id {
                    EnterSeal::AlreadyOurs
                } else if sl.owes_final() {
                    EnterSeal::Conflicting(
                        "a seal with a final record is in flight; retry that request to finish it"
                            .into(),
                    )
                } else if op_id.is_empty() {
                    // A plain close joins a plain sealing.
                    EnterSeal::AlreadyOurs
                } else {
                    EnterSeal::Conflicting("a different seal operation is in flight".into())
                };
                return false;
            }
            // Topology must be quiet BEFORE the intent exists. Installing
            // over a pending transition deadlocks the collection.
            if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                outcome = EnterSeal::PendingTopology;
                return false;
            }
            d.sealing = Some(crate::registry::SealState {
                operation_id: op_id.to_string(),
                intent: intent.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            outcome = EnterSeal::Installed;
            true
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(name);
    Ok(if installed {
        EnterSeal::Installed
    } else {
        outcome
    })
}

/// Drive [`enter_sealing_cas`] to a decision, resolving topology when it
/// is what stands in the way.
pub(crate) async fn claim_seal(
    state: &Arc<AppState>,
    name: &str,
    op_id: &str,
    intent: &crate::registry::SealIntent,
) -> Result<EnterSeal, String> {
    for _ in 0..6 {
        match enter_sealing_cas(state, name, op_id, intent).await? {
            EnterSeal::PendingTopology => {
                // Finish the transition, then race for the intent again.
                crate::scaler3::resume(state, name).await;
                state.registry.invalidate(name);
            }
            other => return Ok(other),
        }
    }
    Err("a split or merge kept the collection busy; the seal is resumable".into())
}

async fn enter_sealing(
    state: &Arc<AppState>,
    name: &str,
    op_id: &str,
    intent: crate::registry::SealIntent,
) -> Result<(), String> {
    match claim_seal(state, name, op_id, &intent).await? {
        EnterSeal::Installed | EnterSeal::AlreadyOurs => Ok(()),
        // Empty message = this exact seal already completed.
        EnterSeal::AlreadyCompleted => Err(String::new()),
        EnterSeal::AlreadySealed => Err("collection is already sealed".into()),
        EnterSeal::Conflicting(m) => Err(m),
        EnterSeal::Missing => Ok(()),
        EnterSeal::PendingTopology => {
            Err("a split or merge is in flight; the seal is resumable".into())
        }
    }
}

/// Distinguishes an ABSENT field from one present as `null`.
fn deserialize_some<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

/// Identity of a seal-with-final operation: the record it promised,
/// under the routing key it promised it for. A retry of the same seal
/// derives the same id and resumes; anything else is a different
/// operation and may not finish this one.
pub(crate) fn seal_op_id(final_value: &serde_json::Value, routing_key: &str) -> String {
    use sha2::{Digest, Sha256};
    // Versioned and LENGTH-DELIMITED. Concatenating the record and the
    // routing key made distinct requests collide: `{"final":1,
    // "routingKey":"23"}` and `{"final":12,"routingKey":"3"}` both hash
    // "123", so after the first sealed, the second looked like an
    // idempotent replay and returned success without ever writing the
    // record it asked for.
    let record = final_value.to_string();
    let mut h = Sha256::new();
    h.update(b"prisma-seal-v1\0");
    h.update((routing_key.len() as u64).to_le_bytes());
    h.update(routing_key.as_bytes());
    h.update((record.len() as u64).to_le_bytes());
    h.update(record.as_bytes());
    crate::crypto::hex(&h.finalize()[..16])
}

/// Publish the Sealing intent for a RAW close, before the physical
/// segment closes. Refuses when another operation still owes a final
/// record — that seal must finish first, or its record would be lost.
pub(crate) async fn begin_sealing_for_close(
    state: &Arc<AppState>,
    name: &str,
    intent: crate::registry::SealIntent,
) -> Result<(), String> {
    let op = match &intent {
        crate::registry::SealIntent::Empty => String::new(),
        crate::registry::SealIntent::Final {
            routing_key,
            request_hash,
            ..
        } => seal_op_id_raw(request_hash, routing_key),
    };
    match claim_seal(state, name, &op, &intent).await? {
        EnterSeal::Installed | EnterSeal::AlreadyOurs | EnterSeal::AlreadyCompleted => Ok(()),
        EnterSeal::AlreadySealed => Ok(()), // already terminal; the close is a no-op
        EnterSeal::Missing => Ok(()),
        EnterSeal::Conflicting(m) => Err(m),
        EnterSeal::PendingTopology => {
            Err("a split or merge is in flight; retry the close".into())
        }
    }
}

/// Operation identity for a raw close that carries content: the same
/// versioned, length-delimited envelope the product seal uses, over the
/// request's own bytes.
pub(crate) fn seal_op_id_raw(request_hash: &str, routing_key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"prisma-seal-v1-raw\0");
    h.update((routing_key.len() as u64).to_le_bytes());
    h.update(routing_key.as_bytes());
    h.update((request_hash.len() as u64).to_le_bytes());
    h.update(request_hash.as_bytes());
    crate::crypto::hex(&h.finalize()[..16])
}

/// Record that a final-bearing seal's record is durable. Must happen
/// before any segment closes: after this the transition can be finished
/// by anyone, and before it, only by the operation that owes the record.
pub(crate) async fn mark_final_committed(
    state: &Arc<AppState>,
    name: &str,
    op_id: &str,
) -> Result<(), String> {
    let mut already = false;
    let marked = state
        .registry
        .cas_update_retry(name, |d| match &mut d.sealing {
            Some(sl) if sl.operation_id == op_id => match &mut sl.intent {
                crate::registry::SealIntent::Final {
                    final_committed, ..
                } if !*final_committed => {
                    *final_committed = true;
                    true
                }
                _ => {
                    already = true; // ours, already marked
                    false
                }
            },
            _ => false,
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(name);
    if marked || already {
        return Ok(());
    }
    // Declined and not ours: the intent we were completing is gone or
    // belongs to someone else. Saying nothing here let the caller close
    // segments for a record this operation never owned.
    match state.registry.get(name).await {
        Ok(Some(d)) if d.sealed => Ok(()), // already terminal
        Ok(Some(_)) => Err("the seal intent this record belongs to is no longer in flight".into()),
        Ok(None) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

/// Descriptor-side collection seal, shared by the product seal route
/// and the RAW close path (spec Stage 8 §7.4: a raw close seals the
/// entire collection; §16.3: product seal and raw close agree on one
/// monotonic state). Idempotent; errors are logged by callers that
/// cannot surface them.
pub(crate) async fn seal_descriptor(state: &Arc<AppState>, name: &str) -> Result<(), String> {
    let desc = match state.registry.get(name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return Err("stream is still being created".into());
            }
            d
        }
        Ok(_) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    if desc.sealed {
        return Ok(());
    }
    state
        .registry
        .cas_update(name, |d| {
            if d.sealed {
                return false;
            }
            d.sealed = true;
            true
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn product_seal_only(state: Arc<AppState>, name: String, _headers: HeaderMap) -> Response {
    // A seal that still owes a final record is a CONFLICT for anyone
    // else, not an internal failure: the caller has to retry the
    // operation that carries the record.
    if let Ok(Some(d)) = state.registry.get(&name).await {
        if d.sealing.as_ref().is_some_and(|sl| sl.owes_final()) {
            return perr(
                StatusCode::CONFLICT,
                "sealing",
                "a seal with a final record is in flight; retry that request to finish it",
                None,
                true,
            );
        }
    }
    match run_seal(&state, &name, None).await {
        Ok(()) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(json!({ "sealed": true }).to_string()))
            .unwrap(),
        Err(m) => perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &m,
            None,
            true,
        ),
    }
}

/// The collection seal transition (audit P0). Open -> Sealing -> every
/// live segment closed -> Sealed. Idempotent and resumable: any request
/// that observes Sealing finishes the same transition, so a crash
/// between the final append, the segment closes and publication can
/// never leave a descriptor claiming sealed over writable segments.
///
/// `op` names the seal operation when a final record is part of it, so
/// a retry resumes instead of appending a second final record.
pub(crate) async fn run_seal(
    state: &Arc<AppState>,
    name: &str,
    op: Option<String>,
) -> Result<(), String> {
    let desc = match state.registry.get(name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => d,
        Ok(_) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    if desc.sealed {
        return Ok(()); // terminal
    }
    // A seal that still owes a final record may only be completed by
    // that operation. A plain `:seal` arriving after a crashed
    // seal-with-final used to close the segments and publish Sealed,
    // dropping the final record for good.
    // Closing is forbidden while a final record is owed — by ANYONE,
    // including the operation that owes it. The owner marks the record
    // durable first (`mark_final_committed`), which is what clears this;
    // making the rule depend on caller identity meant a matching op id
    // could close the segments with the record still unwritten.
    if let Some(sl) = &desc.sealing {
        if sl.owes_final() {
            return Err(if op.as_deref() == Some(sl.operation_id.as_str()) {
                "this seal has not committed its final record yet".into()
            } else {
                "a seal with a final record is in flight; retry that request to finish it"
                    .to_string()
            });
        }
    }
    // A topology transition in flight is resolved BEFORE the seal
    // takes its snapshot of live segments. Otherwise the two interleave:
    // the seal closes what it can see, publishes Sealed, and the
    // transition's phase B then publishes a fresh live child. Phase B
    // now refuses once the lifecycle has moved, and this is the other
    // half — finish the transition first so the snapshot is complete.
    // 1. Claim the transition. This CAS — not a preceding read — is the
    //    serialization point: it installs only over an open, unclaimed,
    //    topologically quiet descriptor, resolving a pending split or
    //    merge first. Installing Sealing over pending work deadlocked
    //    the collection, because phase B then refuses to finish it.
    let op_id = op.clone().unwrap_or_default();
    if desc.sealing.is_none() || desc.sealing.as_ref().is_some_and(|s| s.operation_id != op_id) {
        match claim_seal(state, name, &op_id, &crate::registry::SealIntent::Empty).await? {
            EnterSeal::Installed
            | EnterSeal::AlreadyOurs
            | EnterSeal::AlreadyCompleted
            | EnterSeal::AlreadySealed
            | EnterSeal::Missing => {}
            EnterSeal::Conflicting(m) => return Err(m),
            EnterSeal::PendingTopology => {
                return Err(
                    "a split or merge is in flight and did not settle; the seal is resumable"
                        .into(),
                );
            }
        }
    }
    // 2. Close every live segment identity. Idempotent per segment.
    state.registry.invalidate(name);
    let d = match state.registry.get(name).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
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
        if crate::scaler3::seal_segment_identity(state, &d, seg_id)
            .await
            .is_none()
        {
            // A segment that would not close leaves the collection in
            // Sealing; the next seal request (or retry) resumes it.
            return Err(format!("segment {seg_id} did not close; seal is resumable"));
        }
    }
    // 3. Publish SEALED only now — and only if no topology transition
    //    reappeared while the segments were closing.
    let published = state
        .registry
        .cas_update_retry(name, |d| {
            if d.sealed {
                return false;
            }
            if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                return false;
            }
            d.sealed = true;
            d.seal_op = d
                .sealing
                .as_ref()
                .map(|s| s.operation_id.clone())
                .or(op.clone());
            d.sealing = None;
            true
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(name);
    // Success is PROVEN, never assumed. The CAS above declines when a
    // transition reappeared or another writer moved the state, and
    // returning Ok regardless told clients `{"sealed": true}` about a
    // descriptor that was still Sealing with a split pending.
    let final_state = match state.registry.get(name).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()), // gone: nothing left to seal
        Err(e) => return Err(e.to_string()),
    };
    if !crate::http::desc_alive(&final_state) {
        return Ok(());
    }
    if final_state.sealed && final_state.sealing.is_none() {
        return Ok(());
    }
    let _ = published;
    Err(format!(
        "the seal did not reach a terminal state (sealed={}, sealing={}, pending={}); it is resumable",
        final_state.sealed,
        final_state.sealing.is_some(),
        final_state
            .segments
            .as_ref()
            .is_some_and(|m| m.pending.is_some())
    ))
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
async fn product_append_sealing(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    op_id: String,
) -> Response {
    product_append_inner(state, name, headers, body, false, true, Some(op_id)).await
}

/// Appends refuse a collection that is sealed OR sealing — only the
/// seal operation's own final record may write during Sealing, and it
/// goes through product_append_sealing with seal_after set (audit P0).
fn refuse_if_sealed(desc: &StreamDesc, is_seal_final: bool) -> Option<Response> {
    if desc.sealed {
        return Some(perr(
            StatusCode::CONFLICT,
            "sealed",
            "collection is sealed",
            None,
            false,
        ));
    }
    if desc.sealing.is_some() && !is_seal_final {
        return Some(perr(
            StatusCode::CONFLICT,
            "sealed",
            "collection is being sealed",
            None,
            false,
        ));
    }
    None
}

async fn product_append(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    batch: bool,
) -> Response {
    product_append_inner(state, name, headers, body, batch, false, None).await
}

async fn product_append_inner(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    batch: bool,
    seal_after: bool,
    // TRUSTED: the seal operation whose final record this is.
    seal_auth: Option<String>,
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
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
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
    if let Some(r) = refuse_if_sealed(&desc, seal_after) {
        return r;
    }
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
    if let Ok(v) = axum::http::HeaderValue::from_str(&desc.content_type) {
        ih.insert("content-type", v);
    }
    for h in ["producer-id", "producer-epoch", "producer-seq"] {
        if let Some(v) = headers.get(h) {
            ih.insert(h, v.clone());
        }
    }
    if seal_after {
        ih.insert(
            "stream-closed",
            axum::http::HeaderValue::from_static("true"),
        );
    }
    let has_producer = headers.contains_key("producer-id");
    // Stage 5 §7: the product request hash covers (operation kind,
    // routing key, content type, body bytes, seal flag) — computed over
    // the PRODUCT body, before any wire re-shaping.
    let request_hash: [u8; 16] = {
        use sha2::{Digest, Sha256};
        let mut hx = Sha256::new();
        hx.update(if batch {
            b"\x01batch\x00".as_slice()
        } else {
            b"\x01single\x00".as_slice()
        });
        hx.update((routing_key.len() as u64).to_le_bytes());
        hx.update(routing_key.as_bytes());
        hx.update(desc.content_type.as_bytes());
        hx.update([u8::from(seal_after)]); // seal flag (spec Stage 5 §7)
        hx.update(&body);
        hx.finalize()[..16].try_into().unwrap()
    };
    let raw = crate::http::append(
        state.clone(),
        name.clone(),
        ih,
        axum::body::Body::from(wire_body),
        has_producer.then_some(request_hash),
        Some(routing_key.clone()),
        // The seal's own final record authorizes itself through this
        // trusted parameter, not through a header a client could send.
        seal_auth.clone(),
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
        let (seg_id, tail_next) = match crate::offsets::parse_ep(&next_tok) {
            Ok((e, o)) => (e, o.scan_from()),
            Err(_) => match crate::offsets::Offset::parse(&next_tok) {
                Ok(o) => (0, o.scan_from()),
                Err(_) => (0, 0),
            },
        };
        // The internal ack header carries the ORIGINAL commit offset —
        // on a duplicate that is the first attempt's position, which is
        // what read-your-write resumes from (spec Stage 5 §7 "return
        // the original result"). Clamped to the live tail: a duplicate
        // answered from a sealed predecessor's row reports an offset in
        // the predecessor's space, and a cursor past the live tail
        // would silently skip records.
        let next = raw
            .headers()
            .get("x-ack-last-offset")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|_| dup)
            .map(|last| (last.wrapping_add(1)).min(tail_next))
            .unwrap_or(tail_next);
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
    // Error translation: lift the machine code from the shared path's
    // error body where one exists (the producer taxonomy — spec Stage 5
    // §9 — depends on it), else map by status.
    let retry_after = raw.headers().get("retry-after").cloned();
    let sealed_hdr = raw.headers().contains_key("stream-closed");
    let expected = raw
        .headers()
        .get("producer-expected-seq")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let received = raw
        .headers()
        .get("producer-received-seq")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let cur_epoch = raw
        .headers()
        .get("producer-epoch")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let raw_code = match axum::body::to_bytes(raw.into_body(), 64 * 1024).await {
        Ok(b) => serde_json::from_slice::<serde_json::Value>(&b)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string)),
        Err(_) => None,
    };
    let (code, message, details, retryable): (&str, &str, Option<serde_json::Value>, bool) =
        match raw_code.as_deref() {
            Some("producer_seq_gap") => (
                "producer_gap",
                "producer sequence gap",
                Some(json!({"expected": expected, "received": received})),
                false,
            ),
            Some("producer_stale_epoch") => (
                "stale_producer_epoch",
                "producer epoch is stale",
                Some(json!({"currentEpoch": cur_epoch})),
                false,
            ),
            Some("producer_sequence_reused") => (
                "producer_sequence_reused",
                "same producer sequence with a different request",
                None,
                false,
            ),
            Some("producer_epoch_seq") => (
                "producer_epoch_must_start_at_zero",
                "a new producer epoch must start at sequence 0",
                None,
                false,
            ),
            Some("stream_closed") => ("sealed", "collection is sealed", None, false),
            Some("content_type_mismatch") => (
                "content_type_mismatch",
                "content type mismatch",
                None,
                false,
            ),
            _ => match status.as_u16() {
                404 => ("not_found", "stream not found", None, false),
                403 => ("stale_or_wrong_credentials", "forbidden", None, false),
                409 if sealed_hdr => ("sealed", "collection is sealed", None, false),
                409 => (
                    "conflict",
                    "producer or configuration conflict",
                    None,
                    false,
                ),
                413 => (
                    "body_too_large",
                    "request body exceeds the limit",
                    None,
                    false,
                ),
                429 => ("rate_limited", "admission or rate limit", None, true),
                503 => ("temporarily_unavailable", "retry shortly", None, true),
                _ => ("append_failed", "append failed", None, false),
            },
        };
    let mut r = perr(status, code, message, details, retryable);
    if let Some(ra) = retry_after {
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
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
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
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
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

// ---- Stage 2a: consumer groups --------------------------------------

fn valid_consumer_name(n: &str) -> Option<String> {
    if n.is_empty()
        || n.len() > 128
        || n.contains('/')
        || n == "."
        || n == ".."
        || n.chars().any(|c| c.is_control())
        || n.contains(':')
    {
        return None;
    }
    Some(n.to_string())
}

/// (desc, stream key, epoch) or an error response — the shared entry
/// discipline for every consumer operation.
async fn consumer_ctx(
    state: &Arc<AppState>,
    name: &str,
    headers: &HeaderMap,
) -> Result<(StreamDesc, crate::crypto::StreamKey, [u8; 16]), Response> {
    let Some(key_b64) = product_key(headers) else {
        return Err(perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        ));
    };
    let desc = match state.registry.get(name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return Err(perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                ));
            }
            d
        }
        Ok(_) => {
            return Err(perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            ));
        }
        Err(e) => {
            return Err(perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
                None,
                true,
            ));
        }
    };
    match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => Ok((desc, k, e)),
        crate::http::KeyCheck::Wrong => Err(perr(
            StatusCode::FORBIDDEN,
            "wrong_key",
            "encryption key mismatch",
            None,
            false,
        )),
        _ => Err(perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        )),
    }
}

/// Config ops live on the PARENT identity's committer lane.
async fn consumer_config_op(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    op: crate::queue::QueueOp,
) -> Result<crate::queue::QueueOut, Response> {
    let route = crate::crypto::stream_hash(&desc.name);
    let engine = state
        .engine_for(&route)
        .await
        .map_err(translate_read_error)?;
    engine
        .submit_queue(desc.storage_hash(), op)
        .await
        .map_err(|m| {
            perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &m,
                None,
                true,
            )
        })
}

fn consumer_cfg_json(cname: &str, cfg: &crate::queue::ConsumerConfig) -> String {
    json!({
        "name": cname,
        "visibilityTimeoutMs": cfg.visibility_timeout_ms,
        "maxAttempts": cfg.max_attempts,
        "deadLetterStream": cfg.dead_letter_stream,
        "maxBatchRecords": cfg.max_batch_records,
    })
    .to_string()
}

async fn product_consumer_put(
    state: Arc<AppState>,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (desc, _k, _e) = match consumer_ctx(&state, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    #[derive(serde::Deserialize, Default)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Doc {
        visibility_timeout_ms: Option<u32>,
        max_attempts: Option<u32>,
        dead_letter_stream: Option<String>,
        max_batch_records: Option<u16>,
    }
    let doc: Doc = if body.is_empty() {
        Doc::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(d) => d,
            Err(e) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    &format!("consumer config: {e}"),
                    None,
                    false,
                );
            }
        }
    };
    let mut cfg = crate::queue::ConsumerConfig::default();
    if let Some(v) = doc.visibility_timeout_ms {
        cfg.visibility_timeout_ms = v.clamp(1_000, 12 * 3600 * 1000);
    }
    if let Some(v) = doc.max_attempts {
        cfg.max_attempts = v.clamp(1, 1_000);
    }
    if let Some(v) = doc.max_batch_records {
        cfg.max_batch_records = v.clamp(1, 1_000);
    }
    if let Some(d) = doc.dead_letter_stream {
        if canonical_name(&d).is_err() {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                "deadLetterStream is not a valid stream name",
                None,
                false,
            );
        }
        // DLQ capability model. A dead-letter record is written with the
        // SOURCE stream's encryption key, because that is the only key
        // the delivery path holds — there is no key-exchange step and
        // the server never stores stream keys. So the target must be a
        // real, writable collection under THAT key, and configuring the
        // link requires presenting a key valid for both. Validating it
        // here turns a silent, permanent delivery block (the poisoned
        // key stays leased forever while every DLQ append 403s) into an
        // error the caller sees while it can still fix it.
        if d == name {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                "deadLetterStream must not be the source collection",
                None,
                false,
            );
        }
        let target = match state.registry.get(&d).await {
            Ok(Some(t)) if crate::http::desc_alive(&t) && !crate::http::initializing(&t) => t,
            Ok(_) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "unknown_dead_letter_stream",
                    "deadLetterStream does not exist; create it first, with the same encryption key",
                    None,
                    false,
                );
            }
            Err(_) => {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    "registry unavailable",
                    None,
                    true,
                );
            }
        };
        if target.sealed || target.sealing.is_some() {
            return perr(
                StatusCode::BAD_REQUEST,
                "dead_letter_sealed",
                "deadLetterStream is sealed and cannot accept dead-letter records",
                None,
                false,
            );
        }
        let same_key = product_key(&headers).is_some_and(|kb| {
            matches!(
                crate::http::check_key(Some(&kb), &target),
                crate::http::KeyCheck::Ok(..)
            )
        });
        if !same_key {
            return perr(
                StatusCode::BAD_REQUEST,
                "dead_letter_key_mismatch",
                "deadLetterStream uses a different encryption key; dead-letter delivery writes with the source collection's key",
                None,
                false,
            );
        }
        cfg.dead_letter_epoch = Some(target.stream_epoch.clone());
        cfg.dead_letter_stream = Some(d);
    }
    let out = match consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigPut {
            consumer: cname.clone(),
            cfg,
        },
    )
    .await
    {
        Ok(o) => o,
        Err(r) => return r,
    };
    match out {
        crate::queue::QueueOut::Config {
            conflict: true,
            cfg: Some(existing),
            ..
        } => {
            let mut r = perr(
                StatusCode::CONFLICT,
                "consumer_config_conflict",
                "consumer exists with different configuration",
                serde_json::from_str(&consumer_cfg_json(&cname, &existing)).ok(),
                false,
            );
            r.headers_mut().insert(
                header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            r
        }
        crate::queue::QueueOut::Config {
            cfg: Some(c),
            created,
            ..
        } => Response::builder()
            .status(if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            })
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(consumer_cfg_json(&cname, &c)))
            .unwrap(),
        _ => perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "unexpected config outcome",
            None,
            true,
        ),
    }
}

async fn product_consumer_get(
    state: Arc<AppState>,
    name: String,
    cname: String,
    headers: HeaderMap,
) -> Response {
    let (desc, _k, _e) = match consumer_ctx(&state, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.clone(),
        },
    )
    .await
    {
        Ok(crate::queue::QueueOut::Config { cfg: Some(c), .. }) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(consumer_cfg_json(&cname, &c)))
            .unwrap(),
        Ok(_) => perr(
            StatusCode::NOT_FOUND,
            "unknown_consumer",
            "no such consumer",
            None,
            false,
        ),
        Err(r) => r,
    }
}

async fn product_consumer_delete(
    state: Arc<AppState>,
    name: String,
    cname: String,
    headers: HeaderMap,
) -> Response {
    let (desc, _k, _e) = match consumer_ctx(&state, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // Config under the parent identity; state rows under every segment
    // identity the consumer may have touched.
    if let Err(r) = consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigDelete {
            consumer: cname.clone(),
        },
    )
    .await
    {
        return r;
    }
    if let Some(map) = &desc.segments {
        for sg in &map.segments {
            let identity = desc.dynamic_segment_identity(sg.seg_id);
            if let Ok(engine) = state.engine_for(&desc.segment_route(sg)).await {
                let _ = engine
                    .submit_queue(
                        identity,
                        crate::queue::QueueOp::ConfigDelete {
                            consumer: cname.clone(),
                        },
                    )
                    .await;
            }
        }
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .unwrap()
}

/// Consumer delivery order across segment lineage (spec §2.9): every
/// segment oldest-first, each (seg_id, identity, route, sealed_end).
/// A sealed predecessor is fully settled before any successor delivers
/// — whole-segment draining, which implies the spec's per-key rule
/// conservatively (successors never deliver a key whose predecessor
/// backlog is unsettled, because they deliver nothing until the
/// predecessor is empty).
fn consumer_segments(desc: &StreamDesc) -> Vec<(u32, [u8; 16], [u8; 16], Option<u64>)> {
    match &desc.segments {
        Some(map) if !map.segments.is_empty() => {
            let mut v: Vec<_> = map.segments.iter().collect();
            v.sort_by_key(|sg| (sg.created_ms, sg.seg_id));
            v.iter()
                .map(|sg| {
                    (
                        sg.seg_id,
                        desc.dynamic_segment_identity(sg.seg_id),
                        desc.segment_route(sg),
                        sg.sealed_next_offset,
                    )
                })
                .collect()
        }
        _ => {
            let ro = desc.resolve_segment("");
            vec![(ro.seg_id, ro.identity, ro.shard_route, None)]
        }
    }
}

async fn load_consumer_cfg(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    cname: &str,
) -> Result<crate::queue::ConsumerConfig, Response> {
    match consumer_config_op(
        state,
        desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.to_string(),
        },
    )
    .await?
    {
        crate::queue::QueueOut::Config { cfg: Some(c), .. } => Ok(c),
        _ => Err(perr(
            StatusCode::NOT_FOUND,
            "unknown_consumer",
            "no such consumer; create it first",
            None,
            false,
        )),
    }
}

/// DLQ transition (spec §2.8): append the DLQ record to the configured
/// dead-letter stream with a producer identity derived from the message
/// id (crash-idempotent), and only after that is durable, ack the
/// source lease. No dead-letter stream configured -> the poison is
/// dropped by acking directly.
#[allow(clippy::too_many_arguments)]
async fn dlq_and_settle(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    cfg: &crate::queue::ConsumerConfig,
    cname: &str,
    key_b64: &str,
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
    let dlq_identity_ok = match (&cfg.dead_letter_stream, &cfg.dead_letter_epoch) {
        (Some(dlq), Some(want)) => match state.registry.get(dlq).await {
            Ok(Some(t)) => &t.stream_epoch == want,
            _ => false,
        },
        _ => true,
    };
    for (off, lgen, attempts, kh) in poisoned {
        if let Some(dlq) = &cfg.dead_letter_stream {
            if !dlq_identity_ok {
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
            .encode(skey);
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
            let mut ih = HeaderMap::new();
            if let Ok(v) = axum::http::HeaderValue::from_str(key_b64) {
                ih.insert("prisma-encryption-key", v);
            }
            let pid = format!("dlq:{cname}:{}", &msg_id[..msg_id.len().min(200)]);
            if let Ok(v) = axum::http::HeaderValue::from_str(&pid) {
                ih.insert("producer-id", v);
            }
            ih.insert("producer-epoch", axum::http::HeaderValue::from_static("1"));
            ih.insert("producer-seq", axum::http::HeaderValue::from_static("0"));
            let resp =
                product_append(state.clone(), dlq.clone(), ih, Bytes::from(body), false).await;
            if !resp.status().is_success() {
                // DLQ append not durable: leave the lease; the key stays
                // blocked and a later pass retries idempotently. The
                // link is validated when the consumer is configured, so
                // a client-error status here means the target drifted
                // afterwards (deleted, re-created under another key,
                // sealed). That never resolves on its own, so say it
                // out loud instead of blocking the key in silence.
                let st = resp.status();
                if st.is_client_error() && st != StatusCode::TOO_MANY_REQUESTS {
                    blocked += 1;
                    tracing::warn!(
                        stream = %desc.name,
                        consumer = %cname,
                        dead_letter_stream = %dlq,
                        status = st.as_u16(),
                        "dead-letter delivery permanently refused; the key stays blocked \
                         until the target accepts the source collection's key again"
                    );
                }
                continue;
            }
        }
        let engine = match state.engine_for(&route).await {
            Ok(e) => e,
            Err(_) => continue,
        };
        if engine
            .submit_queue(
                identity,
                crate::queue::QueueOp::Settle {
                    consumer: cname.to_string(),
                    acks: vec![(*off, *lgen)],
                    retries: Vec::new(),
                    extends: Vec::new(),
                    max_deliveries: cfg.max_attempts,
                },
            )
            .await
            .is_ok()
        {
            settled += 1;
        }
    }
    (settled, blocked)
}

async fn product_consumer_pull(
    state: Arc<AppState>,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key_b64 = product_key(&headers).unwrap_or_default();
    let (desc, skey, epoch) = match consumer_ctx(&state, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let cfg = match load_consumer_cfg(&state, &desc, &cname).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    #[derive(serde::Deserialize, Default)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct PullDoc {
        max: Option<usize>,
        wait_ms: Option<u64>,
        visibility_ms: Option<u64>,
    }
    let doc: PullDoc = if body.is_empty() {
        PullDoc::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(d) => d,
            Err(e) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_body",
                    &format!("pull request: {e}"),
                    None,
                    false,
                );
            }
        }
    };
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
                Err(r) => return translate_read_error(r),
            };
            if let Some(end) = sealed_end {
                let cursor = engine.queue_cursor(identity, &cname).await.unwrap_or(0);
                if cursor >= end {
                    continue; // drained predecessor
                }
            }
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
            let cursor = engine.queue_cursor(identity, &cname).await.unwrap_or(0);
            let out = match crate::http::read_merged(
                &skey,
                &epoch,
                &handle,
                &engine,
                cursor,
                None,
                4 << 20,
            )
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
            let mut keys_map: std::collections::HashMap<u64, [u8; 16]> = Default::default();
            let mut by_off: std::collections::HashMap<u64, (String, Bytes)> = Default::default();
            let mut covered_to = cursor;
            for r in &out.recs {
                keys_map.insert(r.off, crate::crypto::stream_hash(&r.rkey));
                by_off.insert(r.off, (r.rkey.clone(), r.payload.clone()));
                covered_to = covered_to.max(r.off + 1);
            }
            let qout = engine
                .submit_queue(
                    identity,
                    crate::queue::QueueOp::Receive {
                        consumer: cname.clone(),
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
            if !poisoned.is_empty() {
                let _ = dlq_and_settle(
                    &state, &desc, &cfg, &cname, &key_b64, &skey, &epoch, identity, route, seg_id,
                    &poisoned, &by_off,
                )
                .await;
                // Settling poison may have drained this segment or
                // unblocked keys — restart the walk.
                continue 'outer;
            }
            if !leased.is_empty() {
                let now = crate::shard::now_ms();
                let mut messages = Vec::with_capacity(leased.len());
                for (off, lease_gen, attempts, kh) in &leased {
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
                        deadline_ms: now + visibility as i64,
                    };
                    let value: serde_json::Value = if desc.is_json() {
                        serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null)
                    } else {
                        use base64::Engine;
                        serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(payload),
                        )
                    };
                    messages.push(json!({
                        "id": msg.encode(&skey),
                        "routingKey": rkey,
                        "attempts": attempts,
                        "leaseToken": lease.encode(&skey),
                        "value": value,
                    }));
                }
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::from(
                        json!({"messages": messages, "backlog": total_backlog + backlog})
                            .to_string(),
                    ))
                    .unwrap();
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
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(
                json!({"messages": [], "backlog": total_backlog}).to_string(),
            ))
            .unwrap();
    }
}

async fn product_consumer_settle(
    state: Arc<AppState>,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key_b64 = product_key(&headers).unwrap_or_default();
    let (desc, skey, epoch) = match consumer_ctx(&state, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let cfg = match load_consumer_cfg(&state, &desc, &cname).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    #[derive(serde::Deserialize, Default)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Item {
        lease_token: String,
        #[serde(default)]
        delay_ms: Option<u64>,
        #[serde(default)]
        visibility_ms: Option<u64>,
    }
    #[derive(serde::Deserialize, Default)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct Doc {
        #[serde(default)]
        acks: Vec<Item>,
        #[serde(default)]
        retries: Vec<Item>,
        #[serde(default)]
        extends: Vec<Item>,
    }
    let doc: Doc = match serde_json::from_slice(&body) {
        Ok(d) => d,
        Err(e) => {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                &format!("settle request: {e}"),
                None,
                false,
            );
        }
    };
    // Tokens name their segment: group per segment, one committer
    // settle each. Invalid or foreign tokens are counted, never errors
    // (spec §2.5).
    let lineage = consumer_segments(&desc);
    let mut stale_local = 0usize;
    type SegOps = (Vec<(u64, u32)>, Vec<(u64, u32, u64)>, Vec<(u64, u32, u64)>);
    let mut per_seg: std::collections::HashMap<u32, SegOps> = Default::default();
    let mut tok = |t: &str| -> Option<(u32, u64, u32)> {
        match crate::product_cursor::LeaseToken::decode(t, &skey, &epoch) {
            Ok(lt) if lineage.iter().any(|(sid, ..)| *sid == lt.msg.seg_id) => {
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
            Err(r) => return translate_read_error(r),
        };
        let out = engine
            .submit_queue(
                identity,
                crate::queue::QueueOp::Settle {
                    consumer: cname.clone(),
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
            let lo = poisoned.iter().map(|(o, ..)| *o).min().unwrap_or(0);
            let mut by_off: std::collections::HashMap<u64, (String, Bytes)> = Default::default();
            if let Ok(out) =
                crate::http::read_merged(&skey, &epoch, &handle, &engine, lo, None, 4 << 20).await
            {
                for r in &out.recs {
                    by_off.insert(r.off, (r.rkey.clone(), r.payload.clone()));
                }
            }
            let (d, b) = dlq_and_settle(
                &state, &desc, &cfg, &cname, &key_b64, &skey, &epoch, identity, route, seg_id,
                &poisoned, &by_off,
            )
            .await;
            dlq += d;
            dlq_blocked += b;
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(
            json!({
                "acked": acked, "retried": retried, "extended": extended,
                "dlq": dlq, "stale": stale + stale_local, "backlog": backlog,
                // Non-zero means the dead-letter target is refusing the
                // source collection's key: those keys stay blocked.
                "dlqBlocked": dlq_blocked,
            })
            .to_string(),
        ))
        .unwrap()
}

// ---- Stage 2b: watches ----------------------------------------------

/// Journal template registration shape for a stream's immutable watch
/// definitions.
pub(crate) fn watch_pinned(desc: &StreamDesc) -> Vec<(String, Vec<String>)> {
    desc.watch_definitions
        .iter()
        .map(|w| (w.name.clone(), w.fields.clone()))
        .collect()
}

/// Canonical watch-key value encoding (spec Stage 2 §3.3): JSON
/// serialization, so "1" (string), 1 (number), true, null, arrays and
/// objects are all distinct. A missing pointer produces NO key for the
/// definition.
///
/// This encoding is NORMATIVE and cross-language — the SDK derives the
/// same watch key offline (see `sdk/src/index.ts`), so the two must
/// agree byte for byte. Two places where a naive `to_string()` would
/// not: object keys are sorted (serde's map already is, JavaScript's
/// is not), and a float with no fractional part is written as an
/// integer, because serde writes `1.0` where JSON.stringify writes `1`.
fn canonical_arg(v: &serde_json::Value) -> String {
    use serde_json::Value as V;
    match v {
        V::Number(n) => match n.as_f64() {
            Some(f) if n.as_i64().is_none() && n.as_u64().is_none() && f.fract() == 0.0 => {
                format!("{}", f as i64)
            }
            _ => n.to_string(),
        },
        V::Array(a) => {
            let items: Vec<String> = a.iter().map(canonical_arg).collect();
            format!("[{}]", items.join(","))
        }
        V::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        V::String((*k).clone()),
                        canonical_arg(m.get(*k).unwrap_or(&V::Null))
                    )
                })
                .collect();
            format!("{{{}}}", items.join(","))
        }
        other => other.to_string(),
    }
}

fn watch_arg(v: Option<&serde_json::Value>) -> Option<String> {
    v.map(canonical_arg)
}

/// The 64-bit watch key for (definition, extracted values), hex16 on
/// the wire. Field order is significant and preserved (spec §3.2) —
/// the definition id hashes fields AS DECLARED.
pub(crate) fn watch_key_hex(name: &str, fields: &[String], values: &[String]) -> String {
    let tid = crate::touch_keys::template_id(name, fields);
    crate::touch_keys::key_hex(crate::touch_keys::watch_key(tid, values))
}

/// Watch-journal key ids for one committed JSON record.
pub(crate) fn product_watch_ids(
    defs: &[crate::registry::WatchDefinition],
    record: &serde_json::Value,
) -> Vec<u32> {
    let mut out = Vec::new();
    for def in defs {
        let mut values = Vec::with_capacity(def.fields.len());
        let mut complete = true;
        for ptr in &def.fields {
            match watch_arg(record.pointer(ptr)) {
                Some(v) => values.push(v),
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        let tid = crate::touch_keys::template_id(&def.name, &def.fields);
        out.push(crate::touch_keys::key_id_of_u64(
            crate::touch_keys::watch_key(tid, &values),
        ));
    }
    out
}

fn watch_def_json(w: &crate::registry::WatchDefinition) -> serde_json::Value {
    json!({"name": w.name, "fields": w.fields})
}

async fn product_watches_list(state: Arc<AppState>, name: String) -> Response {
    let desc = match state.registry.get(&name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
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
    let defs: Vec<_> = desc.watch_definitions.iter().map(watch_def_json).collect();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(json!({"watches": defs}).to_string()))
        .unwrap()
}

async fn product_watch_get(state: Arc<AppState>, name: String, w: String) -> Response {
    let desc = match state.registry.get(&name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
        _ => {
            return perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            );
        }
    };
    match desc.watch_definitions.iter().find(|d| d.name == w) {
        Some(def) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(watch_def_json(def).to_string()))
            .unwrap(),
        None => perr(
            StatusCode::NOT_FOUND,
            "unknown_watch",
            "no such watch definition",
            None,
            false,
        ),
    }
}

/// The observation endpoint (spec §3.5): long-poll one watch key. The
/// URL sig is an OBSERVATION capability derived from the stream key —
/// it grants no decryption, append, consumer, or management rights;
/// holders of the stream key authenticate directly.
async fn product_watch_wait(
    state: Arc<AppState>,
    name: String,
    w: String,
    key_hex: String,
    headers: HeaderMap,
    query: &str,
) -> Response {
    let desc = match state.registry.get(&name).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
        _ => {
            return perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            );
        }
    };
    if !desc.watch_definitions.iter().any(|d| d.name == w) {
        return perr(
            StatusCode::NOT_FOUND,
            "unknown_watch",
            "no such watch definition",
            None,
            false,
        );
    }
    let key_hex = key_hex.trim_end_matches('/').to_ascii_lowercase();
    if key_hex.len() != 16 || u64::from_str_radix(&key_hex, 16).is_err() {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_watch_key",
            "watch key must be 16 hex chars",
            None,
            false,
        );
    }
    let q = parse_query(query);
    let Some(epoch) = desc.epoch_bytes() else {
        return perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "descriptor missing epoch",
            None,
            true,
        );
    };
    // Auth: the derived URL sig (observation capability), or the full
    // encryption key.
    // The sig chain is derivable by any stream-key holder, offline:
    //   touch_token(key, epoch) -> wait_sig_key -> wait_url_sig(keyHex).
    // The server holds no stream keys, so it verifies against the
    // wait_sig_key persisted in the descriptor at create. That is what
    // makes an issued URL durable: it keeps working across restarts,
    // on a process that has never seen this stream, and for a
    // collection that has not been appended to in days.
    let sig_ok = q.get("sig").is_some_and(|sig| {
        use base64::Engine;
        let Some(stored) = desc.watch_sig_key.as_deref() else {
            return false;
        };
        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(stored) else {
            return false;
        };
        let Ok(sk) = <[u8; 32]>::try_from(raw.as_slice()) else {
            return false;
        };
        let expect = crate::crypto::wait_url_sig(&sk, &key_hex);
        // Constant-time: a byte-at-a-time comparison would leak the
        // signature one probe at a time to a caller who can time it.
        let got = sig.trim().to_ascii_lowercase();
        expect.len() == got.len()
            && expect
                .bytes()
                .zip(got.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    });
    if !sig_ok {
        let key_ok = product_key(&headers).is_some_and(|kb| {
            matches!(
                crate::http::check_key(Some(&kb), &desc),
                crate::http::KeyCheck::Ok(..)
            )
        });
        if !key_ok {
            return perr(
                StatusCode::FORBIDDEN,
                "watch_unauthorized",
                "a valid sig or Prisma-Encryption-Key is required",
                None,
                false,
            );
        }
    }
    // Cache the key for sig derivation on later keyless waits.
    if let Some(kb) = product_key(&headers) {
        if let crate::http::KeyCheck::Ok(k, e) = crate::http::check_key(Some(&kb), &desc) {
            state.keys.put(desc.storage_hash(), k, e);
        }
    }
    let journal = state
        .touch
        .journal(desc.storage_hash(), &watch_pinned(&desc));
    let cursor = q
        .get("cursor")
        .map(String::as_str)
        .unwrap_or("now")
        .to_string();
    let timeout = q
        .get("timeoutMs")
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_secs(25))
        .min(std::time::Duration::from_secs(25));
    let key_id = crate::touch_keys::key_id_of(&key_hex);
    let out = journal.wait(&cursor, vec![key_id], timeout).await;

    use crate::touch::WaitOutcome;
    let stream_cursor = |end: u64| {
        let ro = desc.resolve_segment("");
        crate::product_cursor::KeyCursor {
            epoch,
            key_hash: crate::crypto::stream_hash(""),
            seg_id: ro.seg_id,
            offset: end,
        }
    };
    let (status, body) = match out {
        WaitOutcome::Touched {
            cursor,
            end_offset,
            proven,
            cacheable: _,
        } => (
            StatusCode::OK,
            json!({
                "invalidated": true,
                "reason": if proven { "changed" } else { "resync" },
                "cursor": cursor,
                "streamCursor": state
                    .keys
                    .get(&desc.storage_hash())
                    .map(|(k, _)| stream_cursor(end_offset).encode(&k)),
            }),
        ),
        WaitOutcome::Stale { cursor } => (
            StatusCode::OK,
            json!({
                // A stale cursor is an explicit RESYNC, never a silent
                // false (spec §3.5).
                "invalidated": true,
                "reason": "resync",
                "cursor": cursor,
            }),
        ),
        WaitOutcome::Timeout { cursor, end_offset } => (
            StatusCode::OK,
            json!({
                "invalidated": false,
                "cursor": cursor,
                "streamCursor": state
                    .keys
                    .get(&desc.storage_hash())
                    .map(|(k, _)| stream_cursor(end_offset).encode(&k)),
            }),
        ),
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// GET /v1/streams — the paginated product catalog (spec Stage 8 §10).
/// One object-store LIST over the registry prefix; no per-stream GET
/// fan-out beyond the descriptors the page returns.
pub async fn product_list(state: Arc<AppState>, query: String, headers: HeaderMap) -> Response {
    // Its own route entry (not through product_entry): gate it here.
    if !crate::http::authorized(&state, &headers) {
        return perr(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
            None,
            false,
        );
    }
    let q = parse_query(&query);
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    // Opaque cursor (audit P0): the wire form is not an editable stream
    // name. It encodes the position to continue from.
    let after: Option<String> = match q.get("cursor").filter(|c| !c.is_empty()) {
        None => None,
        Some(c) => {
            use base64::Engine;
            match base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(c.as_bytes())
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
            {
                Some(n) => Some(n),
                None => {
                    return perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_cursor",
                        "invalid catalog cursor",
                        None,
                        false,
                    );
                }
            }
        }
    };
    let page = match state.registry.list_page(after.as_deref(), limit).await {
        Ok(p) => p,
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
    let items: Vec<serde_json::Value> = page
        .streams
        .iter()
        .map(|d| {
            json!({
                "name": d.name,
                "contentType": d.content_type,
                "sealed": d.sealed,
                "createdAt": d.created_ms,
            })
        })
        .collect();
    let mut body = json!({ "streams": items });
    // The walk continues while the PROVIDER has more, never "while the
    // page came back full". A page that crossed a run of tombstoned,
    // expired or half-built streams is short but not final, and ending
    // there hides every live stream behind the run.
    if !page.exhausted {
        if let Some(n) = page.next_after {
            use base64::Engine;
            body["cursor"] =
                json!(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(n.as_bytes()));
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body.to_string()))
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
