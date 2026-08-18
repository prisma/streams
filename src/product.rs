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
pub use crate::tenant::RESERVED_ROOT;

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
    if let Some(last) = segments.last()
        && RESERVED_FINAL_SEGMENTS.contains(last)
    {
        return bad(
            "invalid_name",
            "'records', 'consumers' and 'watches' are reserved subresource names",
        );
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
    // The structural half of these rules also lives in
    // tenant::CanonicalStreamName (the identity-layer type). This
    // assertion pins the two validators together at every call in
    // debug/test builds; `validators_agree` in tenant_name_tests pins
    // them in CI.
    debug_assert!(
        crate::tenant::CanonicalStreamName::new(raw).is_ok(),
        "canonical_name accepted a name CanonicalStreamName rejects: {raw:?}"
    );
    Ok(raw.to_string())
}

/// Typed variant of [`canonical_name`] for tenant-qualified call sites
/// (Stage 4): same rules, same error responses, returns the checked
/// identity type instead of a bare String.
#[allow(dead_code)] // consumed from MT Stage 4 surface conversion on
pub fn canonical_stream_name(raw: &str) -> Result<crate::tenant::CanonicalStreamName, Response> {
    canonical_name(raw)?;
    Ok(crate::tenant::CanonicalStreamName::new(raw)
        .expect("canonical_name and CanonicalStreamName agree"))
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
    /// Customer usage lookup (§10): control-plane metadata — bearer
    /// auth, no record key, a rollup point read.
    Usage {
        name: String,
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
        "batch",
        "long-poll",
        "sse",
        "pull",
        "settle",
        "seal",
        "scan",
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
            name: canonical_name(path)?,
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
    if rest == "usage" || rest == "usage/current" {
        return Ok(ProductRoute::Usage { name });
    }
    if let Some(wrest) = rest.strip_prefix("watches/") {
        // `{watch}/keys/{key}` is the signed observation resource, and
        // it is exact: the watch name is one segment, the key is one
        // segment, and nothing may follow.
        if let Some((watch, key)) = wrest.split_once("/keys/") {
            if !watch.is_empty() && !watch.contains('/') && !key.is_empty() && !key.contains('/') {
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
/// §15: the observation capability arrives as `Authorization:
/// Prisma-Watch <cap>` (preferred) or, for EventSource clients, a
/// SHORT-LIVED `cap=` query parameter. The retired unexpiring `sig=`
/// design is gone — clean switch.
fn watch_capability_carrier(path: &str, method: &Method, query: &str, headers: &HeaderMap) -> bool {
    matches!(classify_route(path), Ok(ProductRoute::WatchWait { .. }))
        && method == Method::GET
        && (query.split('&').any(|kv| kv.starts_with("cap="))
            || headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("Prisma-Watch ")))
}

/// MULTITENANCY Stage 5 shadow trial: run a NON-capability product
/// bearer through the full customer pipeline and count the outcome.
/// Observation only, and called from the REQUEST WRAPPERS exactly once
/// per request — the auth gate itself runs twice by design
/// (wrapper + entry defense-in-depth) and must stay side-effect free.
pub(crate) fn shadow_observe_request(
    state: &AppState,
    path: &str,
    method: &Method,
    query: &str,
    headers: &HeaderMap,
) {
    if state.auth.mode != crate::auth::AuthMode::Shadow
        || watch_capability_carrier(path, method, query, headers)
    {
        return;
    }
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    state
        .auth
        .shadow_observe(bearer, crate::shard::now_ms() / 1000);
}

/// §6.1: the route/method -> required-scope matrix. `None` only for
/// the watch-wait route, which authorizes itself with a §15
/// capability. Compound rules (fork creation adds forks.create +
/// source read; DLQ configuration adds dlq.configure on the target)
/// are enforced where those requests are RECOGNIZED — the gate cannot
/// see a request body — and land with Stage 5c.
pub(crate) fn required_scope(
    route: &ProductRoute,
    verb: Option<&str>,
    method: &Method,
) -> Option<crate::tenant::Scope> {
    use crate::tenant::Scope as S;
    let read = *method == Method::GET || *method == Method::HEAD;
    Some(match route {
        ProductRoute::Collection { .. } => {
            if *method == Method::PUT {
                S::Create
            } else if *method == Method::DELETE || verb == Some("seal") {
                S::LifecycleManage
            } else if verb == Some("scan") {
                // :scan pages back DECRYPTED record bodies — a bulk
                // record read, so it takes records.read, NOT the
                // metadata scope the other Collection GETs use (§6.1).
                // Without this, a metadata-only credential (which a
                // create/monitor service legitimately holds, along with
                // the stream key) could export every record, and
                // revoking records.read would not cut off record access.
                S::RecordsRead
            } else {
                S::MetadataRead
            }
        }
        ProductRoute::Records { .. } => {
            if *method == Method::POST {
                S::RecordsAppend
            } else {
                S::RecordsRead
            }
        }
        ProductRoute::Consumer { .. } => {
            if verb == Some("pull") {
                S::ConsumersPull
            } else if verb == Some("settle") {
                S::ConsumersSettle
            } else if read {
                // Reading a consumer's config/positions is stream
                // metadata; mutation is configuration.
                S::MetadataRead
            } else {
                S::ConsumersConfigure
            }
        }
        ProductRoute::Watches { .. } | ProductRoute::Watch { .. } => {
            if read {
                S::MetadataRead
            } else {
                S::WatchesManage
            }
        }
        ProductRoute::Usage { .. } => S::UsageRead,
        ProductRoute::WatchWait { .. } => return None,
    })
}

fn route_stream_name(route: &ProductRoute) -> &str {
    match route {
        ProductRoute::Collection { name }
        | ProductRoute::Records { name }
        | ProductRoute::Consumer { name, .. }
        | ProductRoute::Watches { name }
        | ProductRoute::Watch { name, .. }
        | ProductRoute::Usage { name }
        | ProductRoute::WatchWait { name, .. } => name,
    }
}

/// One response per fail-closed reason class (§7.1/§8.1):
/// 421 wrong_cell (placement — the credential is FINE, so never 401,
/// which would make clients refresh it), 503 for the cell's OWN feed
/// staleness (retryable, not the client's fault), 403 for verified-
/// but-denied (suspension, revocation, scope, prefix), 401 for
/// everything a fresh token could fix.
pub(crate) fn auth_failure_response(e: &crate::auth::AuthError) -> Response {
    use crate::auth::AuthError as E;
    let (status, msg, retryable) = match e {
        E::WrongCell => (
            StatusCode::MISDIRECTED_REQUEST,
            "this cell does not serve the project; re-resolve the              project's endpoint (the credential itself is fine)",
            false,
        ),
        E::PolicyStale | E::GrantsStale | E::KeysStale => (
            StatusCode::SERVICE_UNAVAILABLE,
            "this cell's authorization data is stale; retry",
            true,
        ),
        E::ProjectNotActive(_) => (StatusCode::FORBIDDEN, "the project is not active", false),
        E::CredentialNotActive(_) => (StatusCode::FORBIDDEN, "the credential is not active", false),
        E::MissingScope(_) => (
            StatusCode::FORBIDDEN,
            "the credential does not grant the scope this operation requires",
            false,
        ),
        E::PrefixDenied => (
            StatusCode::FORBIDDEN,
            "the credential's stream grant does not cover this stream",
            false,
        ),
        _ => (
            StatusCode::UNAUTHORIZED,
            "the bearer token failed verification",
            false,
        ),
    };
    let mut r = perr(status, e.kind(), msg, None, retryable);
    if matches!(e, E::WrongCell) {
        // §8.1 fallback form: the header survives body-less handling.
        r.headers_mut().insert(
            "prisma-error-code",
            axum::http::HeaderValue::from_static("wrong_cell"),
        );
    }
    // Journal scope (§10.4): wrong_cell is PLACEMENT (§8.1 — the
    // credential is fine) and the stale classes are the cell's OWN
    // feed health — neither is a denial of the caller. Journaling
    // them would let placement churn or a feed outage flood the
    // bounded queue and evict real security events.
    match e {
        E::WrongCell | E::PolicyStale | E::GrantsStale | E::KeysStale => r,
        _ => crate::audit::tag(r, e.kind()),
    }
}

/// Enforce-mode authentication: the customer token IS the product
/// credential (the deployment bearer remains only on the raw/debug/
/// operator surfaces). Pure over the published snapshots.
pub(crate) fn enforce_customer(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<crate::auth::RequestPrincipal, Response> {
    let Some(token) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return Err(crate::audit::tag(
            perr(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "a customer bearer token is required",
                None,
                false,
            ),
            "unauthorized",
        ));
    };
    // Stage 5d: no tenant bridge. The verified principal's project
    // SELECTS the storage identity for the whole request (addressing,
    // quotas, catalog), so a token for another project addresses that
    // project's own streams — and never this one's. Placement is
    // §8.1's job, enforced inside verify_customer from the policy.
    state
        .auth
        .verify_customer(token, crate::shard::now_ms() / 1000)
        .map_err(|e| auth_failure_response(&e))
}

/// Ok(None): allowed without a principal (Off/Shadow modes, preflights,
/// §15 capability carriers). Ok(Some(p)): enforce-mode verified — the
/// principal travels to handlers for the body-visible scope checks
/// (§6.1 Stage-5c block). Err: the refusal response.
/// §17.3 server backstop: acquire the project's admission slot for
/// this request. Off/shadow requests (no verified project) pass — the
/// gateway and cell safety limits own them; the backstop exists so a
/// verified project cannot starve its neighbors on a shared cell.
pub(crate) fn project_admission(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Result<Option<crate::quota::QuotaGuard>, Response> {
    let Some(p) = principal else { return Ok(None) };
    // Review item 5: the quotas rode in on the principal — the exact
    // snapshot this request verified against, never a second read.
    state
        .quotas
        .admit(&p.project_id, &p.quotas, crate::shard::now_ms())
        .map(Some)
        .map_err(|r| crate::audit::tag_project(quota_refusal_response(&r), &p.project_id))
}

/// Read admission: refuse while the project's read-byte bucket is in
/// debt from earlier responses (§17.2 post-hoc volume metering).
fn check_read_quota(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Option<Response> {
    let p = principal?;
    state
        .quotas
        .check_read(&p.project_id, &p.quotas, crate::shard::now_ms())
        .err()
        .map(|r| crate::audit::tag_project(quota_refusal_response(&r), &p.project_id))
}

/// Debit the SERVED read bytes (sized bodies only — streaming bodies
/// are governed by the live-subscription slot instead).
fn debit_read_response(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
    resp: &Response,
) {
    let Some(p) = principal else { return };
    if !resp.status().is_success() {
        return;
    }
    // The handler-built Response carries no content-length header (the
    // server stamps it at serve time); a SIZED body reports its exact
    // length through the size hint. Streaming bodies (no exact size)
    // are governed by the subscription slot instead.
    let Some(bytes) = axum::body::HttpBody::size_hint(resp.body()).exact() else {
        return;
    };
    state
        .quotas
        .debit_read(&p.project_id, &p.quotas, bytes, crate::shard::now_ms());
}

/// Attach a live-subscription slot to a STREAMING response: the guard
/// travels inside the body stream's state and releases when the stream
/// ends or the client disconnects — not when the handler returns.
fn attach_subscription_guard(resp: Response, guard: crate::quota::SubscriptionGuard) -> Response {
    if !resp.status().is_success() {
        return resp; // a refused subscribe holds no slot
    }
    use futures_util::StreamExt;
    let (parts, body) = resp.into_parts();
    let guarded = futures_util::stream::unfold(
        (body.into_data_stream(), guard),
        |(mut inner, guard)| async move { inner.next().await.map(|frame| (frame, (inner, guard))) },
    );
    Response::from_parts(parts, Body::from_stream(guarded))
}

/// §17.3 refusal classes on the wire.
fn quota_refusal_response(refusal: &crate::quota::QuotaRefusal) -> Response {
    let resp = match refusal {
        crate::quota::QuotaRefusal::Rate { retry_after_secs } => {
            let mut r = perr(
                StatusCode::TOO_MANY_REQUESTS,
                "project_rate_limit",
                "the project's rate quota is exhausted; retry",
                None,
                true,
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string()) {
                r.headers_mut().insert("retry-after", v);
            }
            r
        }
        crate::quota::QuotaRefusal::Concurrency => perr(
            StatusCode::TOO_MANY_REQUESTS,
            "project_concurrency_limit",
            "too many inflight requests for this project; retry",
            None,
            true,
        ),
        crate::quota::QuotaRefusal::TrackerCapacity => perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "project_tracker_capacity",
            "this instance cannot track additional projects right now; retry",
            None,
            true,
        ),
    };
    crate::audit::tag(
        resp,
        match refusal {
            crate::quota::QuotaRefusal::Rate { .. } => "project_rate_limit",
            crate::quota::QuotaRefusal::Concurrency => "project_concurrency_limit",
            crate::quota::QuotaRefusal::TrackerCapacity => "project_tracker_capacity",
        },
    )
}

pub(crate) fn product_auth_gate(
    state: &AppState,
    path: &str,
    method: &Method,
    query: &str,
    headers: &HeaderMap,
) -> Result<Option<crate::auth::RequestPrincipal>, Response> {
    if method == Method::OPTIONS {
        return Ok(None); // preflights carry no credentials, by definition
    }
    if watch_capability_carrier(path, method, query, headers) {
        return Ok(None); // §15: the capability is the credential
    }
    if state.auth.mode == crate::auth::AuthMode::Enforce {
        // §9 order: the exact route parses FIRST (grammar errors are
        // not authentication outcomes), then authenticate, then
        // authorize scope + prefix. No legacy fallback: in enforce the
        // customer token is the only product credential.
        let route = classify_route(path)?;
        let principal = enforce_customer(state, headers)?;
        let (_, verb) = strip_verb(path);
        if let Some(scope) = required_scope(&route, verb, method)
            && let Err(e) = principal.require(scope)
        {
            return Err(crate::audit::tag_project(
                auth_failure_response(&e),
                &principal.project_id,
            ));
        }
        if let Err(e) = principal.require_stream(route_stream_name(&route)) {
            return Err(crate::audit::tag_project(
                auth_failure_response(&e),
                &principal.project_id,
            ));
        }
        return Ok(Some(principal));
    }
    if crate::http::authorized(state, headers) {
        return Ok(None);
    }
    Err(perr(
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
    // Internal plumbing never reaches the wire.
    h.remove("x-ack-closed");
    if !h.contains_key("access-control-allow-origin") {
        h.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    }
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static(
            "content-type, retry-after, prisma-next-cursor, prisma-up-to-date, \
             prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, \
             prisma-routing-key, prisma-durable-cursor, prisma-pending-from, \
             prisma-consumer-version",
        ),
    );
    resp
}

/// Everything under `/v1/streams/{*path}`: subresource suffixes are
/// parsed here because stream names are hierarchical (spec Stage 8:
/// explicit matching before wildcard interpretation).
/// Operation-count metering at the dispatch choke point (§4.5's
/// non-priced dimensions). Bytes are metered where payloads are in
/// hand; OPERATIONS are counted here so no handler forgets them. The
/// registry read is a warm cache hit for a request that just succeeded.
enum OpKind {
    Append,
    Queue,
}

async fn meter_op_if_ok(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    ok: bool,
    kind: OpKind,
) {
    if !ok {
        return;
    }
    if let Ok(Some(desc)) = state.registry.get(sref).await {
        match kind {
            OpKind::Append => crate::billing::meter_append_request(state, &desc),
            OpKind::Queue => crate::billing::meter_queue_op(state, &desc),
        }
    }
}

pub async fn product_entry(
    state: Arc<AppState>,
    path: String,
    method: Method,
    headers: HeaderMap,
    query: String,
    body: Bytes,
    principal: Option<crate::auth::RequestPrincipal>,
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
                 if-none-match, prisma-consumer-version",
            )
            .header("access-control-expose-headers", "*")
            .header("access-control-max-age", "600")
            .body(Body::empty())
            .unwrap();
    }
    // Authorized by PARSED route (see product_auth_gate). The wrapper
    // already ran this before reading the body; repeating it is cheap
    // and keeps direct callers safe. The wrapper's principal (if any)
    // wins; the re-run only re-derives one for direct callers.
    let principal = match product_auth_gate(&state, &path, &method, &query, &headers) {
        Ok(p2) => principal.or(p2),
        Err(r) => return r,
    };
    // Stage 5d: the VERIFIED principal selects the tenant-qualified
    // storage identity. Off/shadow requests (and §15 capability
    // carriers, until the capability wire carries the project —
    // review item 3) address the deployment tenant, which is the
    // single-tenant posture those modes run in.
    let tenant: crate::tenant::ProjectId = principal
        .as_ref()
        .map(|p| p.project_id.clone())
        .unwrap_or_else(|| state.tenant.clone());
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
                (Method::POST, None) => {
                    let r = product_append(
                        state.clone(),
                        &tenant,
                        name.clone(),
                        headers,
                        body,
                        false,
                        principal.as_ref(),
                    )
                    .await;
                    let ok = r.status().is_success();
                    meter_op_if_ok(&state, &tenant.stream_ref(&name), ok, OpKind::Append).await;
                    r
                }
                (Method::POST, Some("batch")) => {
                    let r = product_append(
                        state.clone(),
                        &tenant,
                        name.clone(),
                        headers,
                        body,
                        true,
                        principal.as_ref(),
                    )
                    .await;
                    let ok = r.status().is_success();
                    meter_op_if_ok(&state, &tenant.stream_ref(&name), ok, OpKind::Append).await;
                    r
                }
                (Method::GET, None) => {
                    if let Some(r) = check_read_quota(&state, principal.as_ref()) {
                        return r;
                    }
                    let resp =
                        product_read(state.clone(), &tenant, name, headers, &query, None).await;
                    debit_read_response(&state, principal.as_ref(), &resp);
                    resp
                }
                (Method::GET, Some("long-poll")) => {
                    if let Some(r) = check_read_quota(&state, principal.as_ref()) {
                        return r;
                    }
                    let resp = product_read(
                        state.clone(),
                        &tenant,
                        name,
                        headers,
                        &query,
                        Some("long-poll"),
                    )
                    .await;
                    debit_read_response(&state, principal.as_ref(), &resp);
                    resp
                }
                (Method::GET, Some("sse")) => {
                    // A live subscription consumes a §17.2 slot for the
                    // STREAM's lifetime, not the handler's.
                    let sub = match principal.as_ref() {
                        Some(p) => {
                            match state.quotas.admit_subscription(&p.project_id, &p.quotas) {
                                Ok(g) => g,
                                Err(refusal) => {
                                    return crate::audit::tag_project(
                                        quota_refusal_response(&refusal),
                                        &p.project_id,
                                    );
                                }
                            }
                        }
                        None => None,
                    };
                    let resp =
                        product_read(state, &tenant, name, headers, &query, Some("sse")).await;
                    match sub {
                        Some(g) => attach_subscription_guard(resp, g),
                        None => resp,
                    }
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
                    product_consumer_put(
                        state,
                        &tenant,
                        name,
                        cname,
                        headers,
                        body,
                        principal.as_ref(),
                    )
                    .await
                }
                (Method::GET, None) => {
                    product_consumer_get(state, &tenant, name, cname, headers).await
                }
                (Method::DELETE, None) => {
                    product_consumer_delete(state, &tenant, name, cname, headers).await
                }
                (Method::POST, Some("pull")) => {
                    product_consumer_pull(state, &tenant, name, cname, headers, body).await
                }
                (Method::POST, Some("settle")) => {
                    let r = product_consumer_settle(
                        state.clone(),
                        &tenant,
                        name.clone(),
                        cname,
                        headers,
                        body,
                    )
                    .await;
                    let ok = r.status().is_success();
                    meter_op_if_ok(&state, &tenant.stream_ref(&name), ok, OpKind::Queue).await;
                    r
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
        ProductRoute::Usage { name } => {
            return match method {
                Method::GET => product_usage(state, &tenant, name, &query).await,
                _ => perr(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "usage accepts GET",
                    None,
                    false,
                ),
            };
        }
        ProductRoute::Watches { name } => {
            return if method == Method::GET {
                product_watches_list(state, &tenant, name).await
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
                product_watch_get(state, &tenant, name, watch).await
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
                product_watch_wait(state, &tenant, name, watch, key, headers, &query).await
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
    let name = match canonical_name(strip_verb(&path).0) {
        Ok(n) => n,
        Err(r) => return r,
    };
    match (method.clone(), verb.as_deref()) {
        (Method::PUT, None) => {
            product_create(state, &tenant, name, headers, body, principal.as_ref()).await
        }
        (Method::GET, None) => product_metadata(state, &tenant, name).await,
        (Method::DELETE, None) => crate::http::product_delete(state, &tenant, name).await,
        (Method::POST, Some("seal")) => product_seal(state, &tenant, name, headers, body).await,
        (Method::GET, Some("scan")) => product_scan(state, &tenant, name, headers, &query).await,
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
    let shapes: [(usize, &str); 7] = [
        (4, "watches"),   // watches/{watch}/keys/{key}
        (2, "watches"),   // watches/{watch}
        (2, "consumers"), // consumers/{consumer}
        (2, "usage"),     // usage/current
        (1, "watches"),   // watches
        (1, "records"),   // records
        (1, "usage"),     // usage (§10 customer lookup)
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    principal: Option<&crate::auth::RequestPrincipal>,
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
    // §6.1 Stage-5c: watch definitions ride the create BODY, so the
    // watches.manage scope is only checkable here — streams.create
    // alone must not attach watches (their capabilities are then
    // derivable offline by any key holder, §15).
    if !cfg.watches.is_empty()
        && let Some(p) = principal
        && let Err(e) = p.require(crate::tenant::Scope::WatchesManage)
    {
        return crate::audit::tag_project(auth_failure_response(&e), &p.project_id);
    }
    if let Some(r) = crate::http::ring_owner_check(&state, &tenant.stream_ref(&name)) {
        return r;
    }

    let build_fresh = || {
        let mut d = crate::http::fresh_desc_product(
            &state,
            tenant,
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
            return Err(crate::audit::tag(
                perr(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "encryption key mismatch",
                    None,
                    false,
                ),
                "wrong_key",
            ));
        }
        Ok(d)
    };

    let existing = match state.registry.get(&tenant.stream_ref(&name)).await {
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
            .recreate(&tenant.stream_ref(&name), build_fresh(), |d| {
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

async fn product_metadata(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
) -> Response {
    match state.registry.get(&tenant.stream_ref(&name)).await {
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Atomic final append (spec Stage 8 §7.2): {final, routingKey}
    // rides ONE committer command with the closure — the same
    // append-and-close the raw protocol defines. Producer headers
    // dedup a retried final append.
    // ONE descriptor read for the whole seal request. Its epoch is
    // what every downstream step — the claim, the final append,
    // the mark, the publication — is fenced to. Fetching a fresh
    // epoch later rebound the seal to whatever descriptor owned
    // the name by then: a delete+recreate under the same key
    // between validation and claim had the request seal a
    // replacement nobody asked it to touch.
    let validated = match state.registry.get(&tenant.stream_ref(&name)).await {
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
    let validated_epoch = validated.stream_epoch.clone();
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
            if !matches!(
                crate::http::check_key(Some(&key_b64), &validated),
                crate::http::KeyCheck::Ok(..)
            ) {
                return crate::audit::tag(
                    perr(
                        StatusCode::FORBIDDEN,
                        "wrong_key",
                        "encryption key mismatch",
                        None,
                        false,
                    ),
                    "wrong_key",
                );
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
                if let Some(v) = headers.get(h)
                    && v.to_str().is_err()
                {
                    return perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_producer",
                        &format!("{h} is not a valid header value"),
                        None,
                        false,
                    );
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
            // Capacity, measured on the EXACT wire body the append will
            // build — a single product record travels as `[value]`, two
            // bytes longer than the value itself, and a value on the
            // boundary would otherwise pass here and be refused there,
            // leaving the intent behind.
            if let Some(kind) =
                crate::usage::permanently_unadmittable(fin.to_string().len() as u64 + 2, 1)
            {
                return perr(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    &format!("the final record exceeds the per-stream ingest {kind} capacity"),
                    None,
                    false,
                );
            }
            // Only now: enter Sealing. Ordinary appends are refused from
            // here, so nothing can land between the final record and the
            // segment closes, and the operation id makes the final
            // append itself idempotent under retry.
            let hv = |h: &str| {
                headers
                    .get(h)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            let (pid, pep, pseq) = (hv("producer-id"), hv("producer-epoch"), hv("producer-seq"));
            let op_id = seal_op_id_full(
                &fin,
                doc.routing_key.as_deref().unwrap_or_default(),
                (!pid.is_empty()).then_some((pid.as_str(), pep.as_str(), pseq.as_str())),
            );
            let intent = crate::registry::SealIntent::Final {
                routing_key: doc.routing_key.clone().unwrap_or_default(),
                request_hash: op_id.clone(),
                final_committed: false,
            };
            #[cfg(test)]
            crate::http::fork_failpoints::pause_product_seal_before_claim(&name).await;
            let ticket = match enter_sealing(
                &state,
                &tenant.stream_ref(&name),
                &op_id,
                intent,
                &validated_epoch,
            )
            .await
            {
                Ok(t) => t,
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
            };
            let mut ih = HeaderMap::new();
            if let Ok(v) = axum::http::HeaderValue::from_str(&key_b64) {
                ih.insert("prisma-encryption-key", v);
            }
            if let Some(rk) = &doc.routing_key
                && let Ok(v) = axum::http::HeaderValue::from_str(rk)
            {
                ih.insert("prisma-routing-key", v);
            }
            for h in ["producer-id", "producer-epoch", "producer-seq"] {
                if let Some(v) = headers.get(h) {
                    ih.insert(h, v.clone());
                }
            }
            #[cfg(test)]
            crate::http::fork_failpoints::pause_product_final_before_append(&name).await;
            let resp = product_append_sealing(
                state.clone(),
                &tenant.stream_ref(&name),
                ih,
                Bytes::from(fin.to_string()),
                op_id.clone(),
                ticket.generation,
                ticket.epoch.clone(),
            )
            .await;
            if !resp.status().is_success() {
                // Definitive rejection: this seal can never deliver the
                // record it promised, so it must not leave the intent
                // behind — a collection stuck Sealing refuses ordinary
                // appends AND cannot be finished by a plain seal.
                // Producer state (gap, stale epoch, sequence reuse) and
                // capacity verdicts are only knowable in the committer,
                // which is why the pre-checks cannot cover them.
                //
                // Ambiguous or transient outcomes keep the intent: the
                // record may yet be durable, and the transition stays
                // resumable by an exact retry.
                let st = resp.status();
                let (resp, code) = take_error_code(resp).await;
                // ONE policy with the raw surface (round 12): the old
                // inline list here named codes the product translator
                // never emits, so stale-epoch was "retained" on paper
                // and definitive in fact.
                let definitive = crate::http::final_code_disposition(st, code.as_deref())
                    == crate::http::FinalDisposition::DefinitivelyRejected;
                if definitive
                    && let Err(e) = abandon_seal_intent(
                        &state,
                        &tenant.stream_ref(&name),
                        &op_id,
                        &ticket.epoch,
                        ticket.generation,
                    )
                    .await
                {
                    tracing::error!(stream = %name, "abandoning a refused seal intent: {e}");
                }
                return resp;
            }
            // A success is not enough: it must be OUR final write. A
            // duplicate of some earlier append that did not close the
            // segment answers 2xx too, and treating that as the final
            // would seal the collection without ever writing the record
            // this operation promised.
            let closed_by_us = resp
                .headers()
                .get("x-ack-closed")
                .and_then(|v| v.to_str().ok())
                == Some("true");
            if !closed_by_us {
                if let Err(e) = abandon_seal_intent(
                    &state,
                    &tenant.stream_ref(&name),
                    &op_id,
                    &ticket.epoch,
                    ticket.generation,
                )
                .await
                {
                    tracing::error!(stream = %name, "abandoning a non-closing seal attempt: {e}");
                }
                return perr(
                    StatusCode::CONFLICT,
                    "producer_sequence_reused",
                    "this producer sequence already committed a record that did not seal the \
                     collection; use a fresh sequence for the final record",
                    None,
                    false,
                );
            }
            // The record is durable: record that BEFORE any segment
            // closes, so the transition can be finished by anyone from
            // here on and by nobody else before.
            if let Err(e) = mark_final_committed(
                &state,
                &tenant.stream_ref(&name),
                &op_id,
                &ticket.epoch,
                ticket.generation,
            )
            .await
            {
                // The record is durable but the transition could not be
                // recorded as owning it — a takeover or a recreation
                // moved the state. NEVER proceed to run_seal from here:
                // sealing under a claim this operation no longer holds
                // is exactly the ABA the fence exists to stop.
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "seal_incomplete",
                    &e,
                    None,
                    true,
                );
            }
            return match run_seal(
                &state,
                &tenant.stream_ref(&name),
                Some(op_id),
                &ticket.epoch,
                Some(ticket.generation),
            )
            .await
            {
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
    product_seal_only(state, tenant, name, headers, validated_epoch).await
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
    /// The claim is ours, with the generation every claim-authorized
    /// append must carry. Installation ALLOCATES the generation and a
    /// same-operation re-entry RE-allocates it (renewal): the claim is
    /// a lease, and an actively retrying owner must always hold a
    /// generation no fence can be above.
    Installed {
        generation: u64,
    },
    /// This exact operation already owns the IN-FLIGHT transition. The
    /// re-entry renewed the claim (fresh timestamp, fresh generation).
    AlreadyOurs {
        generation: u64,
    },
    /// This exact operation already finished: answer idempotent success.
    AlreadyCompleted,
    /// Somebody else's seal is already terminal.
    AlreadySealed,
    /// A topology transition is in flight; resolve it and retry.
    PendingTopology,
    /// Somebody else's seal owns the collection.
    Conflicting(String),
    /// The live claim's lease lapsed. Takeover is PERMITTED but not
    /// performed by the CAS: the old operation's final append may still
    /// be queued inside the committer, so the old generation must be
    /// fenced there — and the fence's closed-report consulted — before
    /// anything replaces this claim. [`claim_seal`] runs that protocol.
    AbandonedClaim {
        old_op: String,
        old_gen: u64,
        old_intent: crate::registry::SealIntent,
    },
    Missing,
}

/// Install a seal intent, classifying every outcome. THE serialization
/// point: it installs only over a descriptor that is open, unclaimed
/// and topologically quiet, so once it wins, phase A cannot start a new
/// transition (phase A refuses sealing) and phase B cannot publish one
/// (phase B refuses sealing).
pub(crate) async fn enter_sealing_cas(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, String> {
    let mut outcome = EnterSeal::Missing;
    let installed = state
        .registry
        .cas_update(sref, |d| {
            // A seal belongs to the incarnation it was issued
            // against. The name can be deleted and recreated
            // while this operation is in flight, and sealing the
            // replacement closes a stream nobody asked to close.
            if d.stream_epoch != expect_epoch {
                outcome = EnterSeal::Missing;
                return false;
            }
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
                // An owed final belongs to the operation that promised
                // it — until that operation is demonstrably gone. A
                // claim older than SEAL_CLAIM_MS is ELIGIBLE for
                // takeover, but the CAS never performs one: the old
                // operation's append may still be queued, so its
                // generation has to be fenced through the committer
                // first (claim_seal owns that protocol).
                let abandoned =
                    crate::shard::now_ms() - sl.claimed_ms > crate::registry::SEAL_CLAIM_MS;
                if sl.operation_id == op_id && !op_id.is_empty() {
                    // RENEWAL: the owner is demonstrably alive, so the
                    // lease and the generation both refresh. The fresh
                    // generation is what protects an active owner from
                    // a fence left by an aborted takeover reservation —
                    // every allocation is above every earlier fence.
                    d.seal_gen_counter += 1;
                    let g = d.seal_gen_counter;
                    let sl = d.sealing.as_mut().unwrap();
                    sl.claim_generation = g;
                    sl.claimed_ms = crate::shard::now_ms();
                    outcome = EnterSeal::AlreadyOurs { generation: g };
                    return true;
                }
                if sl.operation_id == op_id || (op_id.is_empty() && !sl.owes_final()) {
                    // A plain close JOINS a non-owing sealing — and the
                    // join RENEWS, exactly like an owner's retry. A
                    // standing generation can sit below a fence left
                    // by a takeover race that lost after fencing; a
                    // joiner that merely shared it would inherit that
                    // wedge. Renewal allocates above every fence ever
                    // set, so whoever actually drives the transition
                    // can always close the segments.
                    d.seal_gen_counter += 1;
                    let g = d.seal_gen_counter;
                    let sl = d.sealing.as_mut().unwrap();
                    sl.claim_generation = g;
                    sl.claimed_ms = crate::shard::now_ms();
                    outcome = EnterSeal::AlreadyOurs { generation: g };
                    return true;
                }
                outcome = if sl.owes_final() && abandoned {
                    EnterSeal::AbandonedClaim {
                        old_op: sl.operation_id.clone(),
                        old_gen: sl.claim_generation,
                        old_intent: sl.intent.clone(),
                    }
                } else if sl.owes_final() {
                    EnterSeal::Conflicting(
                        "a seal with a final record is in flight; retry that request to finish it"
                            .into(),
                    )
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
            d.seal_gen_counter += 1;
            d.sealing = Some(crate::registry::SealState {
                operation_id: op_id.to_string(),
                intent: intent.clone(),
                claimed_ms: crate::shard::now_ms(),
                claim_generation: d.seal_gen_counter,
            });
            outcome = EnterSeal::Installed {
                generation: d.seal_gen_counter,
            };
            true
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    let _ = installed;
    Ok(outcome)
}

/// Drive [`enter_sealing_cas`] to a decision, resolving topology when it
/// is what stands in the way.
pub(crate) async fn claim_seal(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, String> {
    for _ in 0..6 {
        match enter_sealing_cas(state, sref, op_id, intent, expect_epoch).await? {
            EnterSeal::PendingTopology => {
                // Finish the transition, then race for the intent again.
                crate::scaler3::resume(state, sref).await;
                state.registry.invalidate(sref);
            }
            EnterSeal::AbandonedClaim {
                old_op,
                old_gen,
                old_intent,
            } => {
                match take_over_abandoned(
                    state,
                    sref,
                    expect_epoch,
                    op_id,
                    intent,
                    &old_op,
                    old_gen,
                    &old_intent,
                )
                .await?
                {
                    Some(outcome) => return Ok(outcome),
                    // The claim moved while we were fencing (renewed,
                    // completed, replaced): whatever it is now decides.
                    None => state.registry.invalidate(sref),
                }
            }
            other => return Ok(other),
        }
    }
    Err("a split or merge kept the collection busy; the seal is resumable".into())
}

/// Take over a lapsed final-bearing claim — the ONLY way one is ever
/// replaced, and the wall clock is never the whole argument. Order:
///
/// 1. RESERVE a generation above the old one (a descriptor CAS that
///    only bumps the allocator; the claim is untouched and must still
///    be exactly the lapsed one we saw).
/// 2. FENCE the old generation through the committer of the segment
///    the old final targeted. The fence is processed in queue order,
///    so its answer proves every append enqueued before it — the old
///    operation's final included, however long it sat — has been
///    decided, and no append below the reservation can commit after.
/// 3. Consult the fence's closed-report. CLOSED means the old final
///    won its race after all: its record is durable and the segment is
///    shut, so the old transition is COMPLETED on its behalf and the
///    caller is told the collection sealed under the other operation.
///    NOT CLOSED means it can never commit now — only then does the
///    new claim replace the old, expecting it unchanged.
///
/// A timestamp decides only when this protocol may START; whether the
/// old operation is really gone is decided by the fence.
#[allow(clippy::too_many_arguments)]
async fn take_over_abandoned(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    old_op: &str,
    old_gen: u64,
    old_intent: &crate::registry::SealIntent,
) -> Result<Option<EnterSeal>, String> {
    // 1. Reserve.
    let mut reserved = 0u64;
    let same_claim = |sl: &crate::registry::SealState| {
        sl.operation_id == old_op && sl.claim_generation == old_gen
    };
    let ok = state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| match &d.sealing {
            Some(sl) if same_claim(sl) => {
                d.seal_gen_counter += 1;
                reserved = d.seal_gen_counter;
                true
            }
            _ => false,
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    if !ok {
        return Ok(None);
    }
    // 2/3. Fence the old final's segment and read the verdict.
    let routing_key = match old_intent {
        crate::registry::SealIntent::Final { routing_key, .. } => routing_key.clone(),
        // Only owed finals are ever taken over.
        crate::registry::SealIntent::Empty => String::new(),
    };
    let closed =
        crate::http::fence_segment_for_key(state, sref, expect_epoch, &routing_key, reserved)
            .await?;
    if closed {
        // The old operation's close committed: its record is durable
        // and its segment shut. Finish ITS transition — the record must
        // not be stranded behind an unmarked intent — and report the
        // collection sealed under the other operation.
        mark_final_committed(state, sref, old_op, expect_epoch, old_gen).await?;
        // Boxed: completing the old transition re-enters run_seal ->
        // claim_seal, and the compiler needs the cycle broken.
        Box::pin(run_seal(
            state,
            sref,
            Some(old_op.to_string()),
            expect_epoch,
            Some(old_gen),
        ))
        .await?;
        return Ok(Some(EnterSeal::AlreadySealed));
    }
    // 4. Install the new claim over the (still unchanged) old one.
    if install_reserved_claim(
        state,
        sref,
        expect_epoch,
        old_op,
        old_gen,
        op_id,
        intent,
        reserved,
    )
    .await?
    {
        return Ok(Some(EnterSeal::Installed {
            generation: reserved,
        }));
    }
    Ok(None)
}

/// The takeover's installation CAS: replace the (still unchanged)
/// lapsed claim with the new one — and ONLY if the caller's
/// reservation is still the NEWEST allocation. Two takeovers can
/// reserve against the same lapsed claim (the reservation deliberately
/// leaves it in place); both fence, and the segment keeps the higher
/// fence. If the LOWER reservation then installed, the live claim's
/// generation would sit below the fence and every close it issues
/// would be refused: a collection held Sealing by its own recovery
/// protocol. The counter check makes the newest reservation the only
/// installable one; an older one restarts the protocol from the top.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn install_reserved_claim(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    old_op: &str,
    old_gen: u64,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    reserved: u64,
) -> Result<bool, String> {
    let installed = state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| match &d.sealing {
            Some(sl)
                if sl.operation_id == old_op
                    && sl.claim_generation == old_gen
                    && d.seal_gen_counter == reserved =>
            {
                d.sealing = Some(crate::registry::SealState {
                    operation_id: op_id.to_string(),
                    intent: intent.clone(),
                    claimed_ms: crate::shard::now_ms(),
                    claim_generation: reserved,
                });
                true
            }
            _ => false,
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    Ok(installed)
}

/// The execution token a claimed seal operates under: the incarnation
/// it was issued against and the generation its appends must carry.
/// Everything after the claim — the final append, the mark, the
/// segment closes, the publication — is fenced by BOTH.
#[derive(Debug, Clone)]
pub(crate) struct SealTicket {
    pub epoch: String,
    pub generation: u64,
}

async fn enter_sealing(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<SealTicket, String> {
    // The epoch is the caller's VALIDATED one — never re-fetched here.
    // A second lookup between validation and claim was the ABA window.
    match claim_seal(state, sref, op_id, &intent, expect_epoch).await? {
        EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
            Ok(SealTicket {
                epoch: expect_epoch.to_string(),
                generation,
            })
        }
        // Empty message = this exact seal already completed.
        EnterSeal::AlreadyCompleted => Err(String::new()),
        EnterSeal::AlreadySealed => Err("collection is already sealed".into()),
        EnterSeal::Conflicting(m) => Err(m),
        EnterSeal::Missing => Err("collection not found".into()),
        EnterSeal::PendingTopology => {
            Err("a split or merge is in flight; the seal is resumable".into())
        }
        EnterSeal::AbandonedClaim { .. } => unreachable!("claim_seal resolves abandoned claims"),
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
pub(crate) fn seal_op_id_full(
    final_value: &serde_json::Value,
    routing_key: &str,
    producer: Option<(&str, &str, &str)>,
) -> String {
    use sha2::{Digest, Sha256};
    // The identity covers the WHOLE attempt, not just the record. Two
    // requests carrying the same final value under the same key but
    // different producer coordination are different operations: sharing
    // one id let a request that was definitively refused tear down the
    // intent a concurrent valid attempt was still committing under.
    let record = final_value.to_string();
    let (pid, pep, pseq) = producer.unwrap_or(("", "", ""));
    let mut h = Sha256::new();
    h.update(b"prisma-seal-v2\0");
    for part in [routing_key, &record, pid, pep, pseq] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    crate::crypto::hex(&h.finalize()[..16])
}

/// Identity of a raw close that carries content. The raw surface has
/// no typed final record, so the identity is the create-request hash
/// plus EVERY coordination input the committer can rule on: producer
/// trio, explicit sequence, timestamp. Two closes that agree on all of
/// it are the same operation and may resume each other; anything else
/// is a different one and may not finish this seal.
pub(crate) fn seal_op_id_semantic(
    request_hash: &str,
    routing_key: &str,
    coordination: &[String],
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"prisma-seal-raw-v2\0");
    for part in std::iter::once(routing_key)
        .chain(std::iter::once(request_hash))
        .chain(coordination.iter().map(|s| s.as_str()))
    {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    crate::crypto::hex(&h.finalize()[..16])
}

/// Read an error response's `error.code` without consuming it: the
/// committer's verdict is only in the body, and the caller still has to
/// return the response verbatim.
async fn take_error_code(resp: Response) -> (Response, Option<String>) {
    let (parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        // Unreadable body: no verdict, so the caller treats it as one
        // it cannot classify — which keeps the intent.
        Err(_) => return (Response::from_parts(parts, Body::empty()), None),
    };
    let code = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get("error")?.get("code")?.as_str().map(|s| s.to_string()));
    (Response::from_parts(parts, Body::from(bytes)), code)
}

/// Publish the Sealing intent for a RAW close, before the physical
/// segment closes. Refuses when another operation still owes a final
/// record — that seal must finish first, or its record would be lost.
pub(crate) async fn begin_sealing_for_close(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    intent: crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<Option<u64>, String> {
    // The intent's request_hash IS the operation id: one identity,
    // computed once by the request that owns it. The epoch is the
    // ADMISSION descriptor's — the close is fenced to the incarnation
    // it was admitted against, like every other lifecycle decision.
    let op = match &intent {
        crate::registry::SealIntent::Empty => String::new(),
        crate::registry::SealIntent::Final { request_hash, .. } => request_hash.clone(),
    };
    match claim_seal(state, sref, &op, &intent, expect_epoch).await? {
        EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
            Ok(Some(generation))
        }
        EnterSeal::AlreadyCompleted => Ok(None),
        EnterSeal::AlreadySealed => Ok(None), // already terminal; the close is a no-op
        EnterSeal::Missing => Ok(None),
        EnterSeal::Conflicting(m) => Err(m),
        EnterSeal::PendingTopology => Err("a split or merge is in flight; retry the close".into()),
        EnterSeal::AbandonedClaim { .. } => unreachable!("claim_seal resolves abandoned claims"),
    }
}

/// Renew an owed-final claim for its OWN exact retry: fresh lease,
/// fresh generation. Returns the new generation, or None when the
/// claim is no longer this operation's to renew.
pub(crate) async fn renew_owed_claim(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
) -> Result<Option<u64>, String> {
    let mut renewed = None;
    state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| {
            let counter = &mut d.seal_gen_counter;
            *counter += 1;
            let g = *counter;
            match d.sealing.as_mut() {
                Some(sl) if sl.operation_id == op_id && sl.owes_final() => {
                    sl.claim_generation = g;
                    sl.claimed_ms = crate::shard::now_ms();
                    renewed = Some(g);
                    true
                }
                _ => {
                    renewed = None;
                    false
                }
            }
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    Ok(renewed)
}

/// Release an intent this operation owns and has NOT committed. Only
/// ever our own, only ever while it still owes its record — a seal that
/// already wrote its final is finished by `run_seal`, not undone here.
pub(crate) async fn abandon_seal_intent(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
    expect_gen: u64,
) -> Result<(), String> {
    // Epoch- and generation-fenced: operation ids do not embed the
    // incarnation, so a name-scoped release could clear an EQUIVALENT
    // intent installed on a recreated stream — or one a takeover had
    // since re-issued under a new generation. Releasable while the
    // promise is UNDELIVERED (an owed final, or an Empty claim whose
    // close turned out to be a spent producer tuple); a final that is
    // already durable is finished by run_seal, never undone here.
    state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| match &d.sealing {
            Some(sl)
                if sl.operation_id == op_id
                    && sl.claim_generation == expect_gen
                    && !matches!(
                        sl.intent,
                        crate::registry::SealIntent::Final {
                            final_committed: true,
                            ..
                        }
                    ) =>
            {
                d.sealing = None;
                true
            }
            _ => false,
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    Ok(())
}

/// Record that a final-bearing seal's record is durable. Must happen
/// before any segment closes: after this the transition can be finished
/// by anyone, and before it, only by the operation that owes the record.
pub(crate) async fn mark_final_committed(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
    expect_gen: u64,
) -> Result<(), String> {
    let mut already = false;
    // The epoch is the one this OPERATION was issued against — never a
    // fresh read. Reading the current incarnation here only proved the
    // CAS raced nothing between the read and itself; a stale operation
    // reading the replacement's epoch would then happily mark (and go
    // on to seal) a collection it was never part of.
    let marked = state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| match &mut d.sealing {
            Some(sl) if sl.operation_id == op_id && sl.claim_generation == expect_gen => {
                match &mut sl.intent {
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
                }
            }
            _ => false,
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    if marked || already {
        return Ok(());
    }
    // Declined and not ours: the intent we were completing is gone,
    // belongs to someone else, or lives on a different incarnation.
    // Saying nothing here let the caller close segments for a record
    // this operation never owned — and the ONLY acceptable silent
    // answer is "OUR seal already completed on OUR incarnation".
    match state.registry.get(sref).await {
        Ok(Some(d))
            if d.sealed
                && d.stream_epoch == expect_epoch
                && d.seal_op.as_deref() == Some(op_id) =>
        {
            Ok(())
        }
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
pub(crate) async fn seal_descriptor(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> Result<(), String> {
    let desc = match state.registry.get(sref).await {
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
        .cas_update(sref, |d| {
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

async fn product_seal_only(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    _headers: HeaderMap,
    validated_epoch: String,
) -> Response {
    // No pre-refusal on an outstanding final-bearing intent: run_seal's
    // claim path answers it properly — a LIVE claim is a 409 conflict,
    // and a lapsed one goes through the takeover protocol, so a plain
    // `:seal` really can recover a collection whose sealer died. (The
    // old pre-check made that impossible and contradicted the
    // documented recovery story.) The epoch is the one the KEY was
    // validated against, not a fresh read.
    #[cfg(test)]
    crate::http::fork_failpoints::pause_product_seal_before_claim(&name).await;
    match run_seal(
        &state,
        &tenant.stream_ref(&name),
        None,
        &validated_epoch,
        None,
    )
    .await
    {
        Ok(()) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(json!({ "sealed": true }).to_string()))
            .unwrap(),
        Err(m) => {
            // A live final-bearing claim is the one CONFLICT here; the
            // rest are resumable states of our own transition.
            let conflict = m.contains("final record is in flight");
            perr(
                if conflict {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                },
                if conflict { "sealing" } else { "internal" },
                &m,
                None,
                true,
            )
        }
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
    sref: &crate::tenant::TenantStreamRef,
    op: Option<String>,
    expect_epoch: &str,
    claim_gen: Option<u64>,
) -> Result<(), String> {
    let desc = match state.registry.get(sref).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => d,
        Ok(_) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    // The transition this call drives belongs to ONE incarnation. A
    // name-scoped run_seal re-fetched whatever descriptor owned the
    // name and could claim and seal a replacement created while the
    // caller was in flight.
    if desc.stream_epoch != expect_epoch {
        return Err("the collection this seal was issued against no longer exists".into());
    }
    if desc.sealed {
        // Terminal — but only OUR terminal counts as our success. A
        // caller driving a specific operation must not report
        // completion because somebody else's seal got there first.
        if let Some(o) = op.as_deref()
            && !o.is_empty()
            && desc.seal_op.as_deref() != Some(o)
        {
            return Err("the collection sealed under a different operation".into());
        }
        return Ok(());
    }
    // An OWED final is decided by the claim path below, not by a
    // pre-read: a live claim answers Conflicting (the caller must let
    // that operation finish), and a lapsed one goes through the
    // takeover protocol — which is what makes a plain `:seal` a real
    // recovery tool instead of a permanent 409. The one caller who may
    // proceed while its claim is a Final is the OWNER after its mark
    // (final_committed=true no longer owes); an owner that has not
    // marked cannot get here, because segment closes and publication
    // both refuse an owing claim.
    if let Some(sl) = &desc.sealing
        && sl.owes_final()
        && op.as_deref() == Some(sl.operation_id.as_str())
    {
        return Err("this seal has not committed its final record yet".into());
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
    // The generation every close this call issues will carry. Owners
    // arrive with theirs (their claim is installed and, for a final,
    // already marked); everyone else claims here and uses what the
    // claim allocates.
    let mut our_gen = claim_gen;
    // Resuming a claim that is already ours by identity (a planted
    // recovery, a plain close joining a plain sealing): adopt its
    // standing generation — the segment closes below must carry it.
    if our_gen.is_none()
        && let Some(sl) = &desc.sealing
        && sl.operation_id == op_id
    {
        our_gen = Some(sl.claim_generation);
    }
    if desc.sealing.is_none()
        || desc
            .sealing
            .as_ref()
            .is_some_and(|s| s.operation_id != op_id)
    {
        match claim_seal(
            state,
            sref,
            &op_id,
            &crate::registry::SealIntent::Empty,
            expect_epoch,
        )
        .await?
        {
            EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
                our_gen = Some(generation);
            }
            EnterSeal::AlreadyCompleted | EnterSeal::AlreadySealed => return Ok(()),
            EnterSeal::Missing => {}
            EnterSeal::Conflicting(m) => return Err(m),
            EnterSeal::PendingTopology => {
                return Err(
                    "a split or merge is in flight and did not settle; the seal is resumable"
                        .into(),
                );
            }
            EnterSeal::AbandonedClaim { .. } => {
                unreachable!("claim_seal resolves abandoned claims")
            }
        }
    }
    // 2. Close every live segment identity. Idempotent per segment.
    state.registry.invalidate(sref);
    let d = match state.registry.get(sref).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    if d.stream_epoch != expect_epoch {
        return Err("the collection this seal was issued against no longer exists".into());
    }
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
        if crate::scaler3::seal_segment_identity(state, &d, seg_id, our_gen)
            .await
            .is_none()
        {
            // A segment that would not close leaves the collection in
            // Sealing; the next seal request (or retry) resumes it.
            // (This includes a close refused by a seal fence: the claim
            // generation lapsed, and a retry — which renews it — is the
            // correct way back in.)
            return Err(format!("segment {seg_id} did not close; seal is resumable"));
        }
    }
    // 3. Publish SEALED only now — and only if no topology transition
    //    reappeared while the segments were closing.
    let published = state
        .registry
        .cas_update_incarnation(sref, expect_epoch, |d| {
            if d.sealed {
                return false;
            }
            if d.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
                return false;
            }
            // Publication requires the claim to still be THE ONE this
            // call drove. The generation is the whole identity — every
            // install, join and renewal hands out (or shares) exactly
            // one, so equality here means "the claim I closed segments
            // under still stands". A plain close that JOINED another
            // operation's non-owing claim carries that claim's
            // generation and may finish it; a caller whose claim was
            // taken over holds a stale generation and may not stamp
            // Sealed over the successor's still-working transition.
            match (&d.sealing, our_gen) {
                (Some(sl), Some(g)) => {
                    if sl.claim_generation != g || sl.owes_final() {
                        return false;
                    }
                }
                _ => return false,
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
    state.registry.invalidate(sref);
    // Success is PROVEN, never assumed. The CAS above declines when a
    // transition reappeared or another writer moved the state, and
    // returning Ok regardless told clients `{"sealed": true}` about a
    // descriptor that was still Sealing with a split pending.
    let final_state = match state.registry.get(sref).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()), // gone: nothing left to seal
        Err(e) => return Err(e.to_string()),
    };
    if !crate::http::desc_alive(&final_state) {
        return Ok(());
    }
    // The proof is about THIS incarnation and THIS operation. A
    // replacement created (and even sealed) under the same name
    // between publication and this read is somebody else's resource;
    // reporting success against it violates the very guarantee the
    // rest of the machine establishes.
    if final_state.stream_epoch != expect_epoch {
        return Err("the collection this seal was issued against no longer exists".into());
    }
    if final_state.sealed && final_state.sealing.is_none() {
        if let Some(o) = op.as_deref()
            && !o.is_empty()
            && final_state.seal_op.as_deref() != Some(o)
        {
            return Err("the collection sealed under a different operation".into());
        }
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
    sref: &crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    body: Bytes,
    op_id: String,
    generation: u64,
    epoch: String,
) -> Response {
    product_append_inner(
        state,
        sref.project_id(),
        sref.name().as_str().to_string(),
        headers,
        body,
        false,
        true,
        Some(crate::http::SealAuthz {
            op_id,
            generation,
            epoch,
        }),
        // Internal: the seal's final record is lifecycle work, not
        // customer append volume.
        None,
    )
    .await
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    batch: bool,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Response {
    product_append_inner(
        state, tenant, name, headers, body, batch, false, None, principal,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // request context, not tunables
async fn product_append_inner(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    batch: bool,
    seal_after: bool,
    // TRUSTED: the seal operation whose final record this is, with the
    // claim generation and incarnation its write is fenced under.
    seal_auth: Option<crate::http::SealAuthz>,
    principal: Option<&crate::auth::RequestPrincipal>,
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
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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
    // §17.2 append-volume backstop, with the EXACT parsed shape: the
    // request payload size and the true record count (batch-aware).
    // Internal writers (DLQ delivery, the seal's final record) carry
    // no principal and are bounded by their own mechanisms.
    if let Some(p) = principal
        && let Err(refusal) = state.quotas.admit_append(
            &p.project_id,
            &p.quotas,
            body.len() as u64,
            count as u64,
            crate::shard::now_ms(),
        )
    {
        return crate::audit::tag_project(quota_refusal_response(&refusal), &p.project_id);
    }

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
        tenant.stream_ref(&name),
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
                .encode(&desc.project_id, &k)
            }
            Err(_) => String::new(),
        };
        let _ = state;
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            // Internal, stripped at the edge: whether THIS ack closed the
            // stream. The seal needs it to tell its own final write from
            // a duplicate of an earlier, non-closing append.
            .header(
                "x-ack-closed",
                raw.headers()
                    .get("x-ack-closed")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("false"),
            )
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
    //
    // Ownership bounce first: an append routed to a segment another
    // instance owns must keep Streams-Replay-To visible, or routers
    // cannot converge and every post-split append to a foreign child
    // fails as an opaque "conflict" (the two-instance rig lost every
    // such record silently — the client saw 409, the hammer didn't
    // check, and the child segments stayed empty).
    if status.as_u16() == 409
        && let Some(to) = raw.headers().get("streams-replay-to").cloned()
    {
        let mut r = perr(
            status,
            "not_stream_owner",
            "another instance owns the target segment; retry through the router",
            None,
            true,
        );
        r.headers_mut().insert("streams-replay-to", to);
        return r;
    }
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
            // R25-H: the maintenance refusal is a TYPED contract —
            // clients key retry policy off this code, and the generic
            // 503 translation was erasing it on the product surface
            // while the raw surface kept it. One refusal, one name.
            Some("maintenance_backpressure") => (
                "maintenance_backpressure",
                "maintenance backlog exceeds its bound; retry after it drains",
                None,
                true,
            ),
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
    // §10.4: the shared append core answers key mismatches as a bare
    // 403, restated here — key-guessing probes on the append path
    // journal like every other denial.
    if code == "stale_or_wrong_credentials" {
        r = crate::audit::tag(r, "stale_or_wrong_credentials");
    }
    r
}

// ---- Stage 6: read, subscribe, scan ---------------------------------

const SCAN_TTL_MS: i64 = 6 * 3600 * 1000;
const SCAN_DEFAULT_BYTES: usize = 4 << 20;
const READ_MAX_BYTES_CAP: usize = 8 << 20;

/// Query-string map with one-shot percent-decoding of values. Product
/// SDKs percent-encode routing keys; '+' is NOT treated as a space.
/// Strict query validation for a public route: the query-string
/// equivalent of `deny_unknown_fields` on the creation document.
///
/// R23-4. The first pass of this fix only taught the records read
/// handler to refuse values it could not parse; the same
/// `.and_then(parse).ok()` pattern survived on scan, watch and catalog,
/// where a malformed `maxBytes` / `timeoutMs` / `limit` still collapsed
/// into the route default. Three failure shapes are refused here:
///
///   unknown key      the caller believes a parameter works that does
///                    not, and silently gets default behaviour
///   duplicate key    ?maxBytes=10&maxBytes=99999 — last-wins is a
///                    silent choice between two stated intents
///   unparseable      handled per-value by [`q_num`]
fn strict_query(
    query: &str,
    allowed: &[&str],
) -> Result<std::collections::HashMap<String, String>, Response> {
    let mut seen: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let key = pair.split('=').next().unwrap_or("");
        if key.is_empty() {
            continue;
        }
        *seen.entry(key.to_string()).or_insert(0) += 1;
    }
    for (key, count) in &seen {
        if !allowed.contains(&key.as_str()) {
            return Err(perr(
                StatusCode::BAD_REQUEST,
                "unknown_parameter",
                &format!(
                    "unknown query parameter \"{key}\"; this route accepts: {}",
                    allowed.join(", ")
                ),
                None,
                false,
            ));
        }
        if *count > 1 {
            return Err(perr(
                StatusCode::BAD_REQUEST,
                "duplicate_parameter",
                &format!("query parameter \"{key}\" given {count} times"),
                None,
                false,
            ));
        }
    }
    Ok(parse_query(query))
}

/// Parse one numeric query value strictly: a value we cannot read is a
/// client mistake, never a request for the default.
fn q_num<T: std::str::FromStr>(
    q: &std::collections::HashMap<String, String>,
    key: &str,
    code: &'static str,
) -> Result<Option<T>, Response> {
    match q.get(key) {
        None => Ok(None),
        Some(v) => v.parse::<T>().map(Some).map_err(|_| {
            perr(
                StatusCode::BAD_REQUEST,
                code,
                &format!("{key} must be a non-negative integer"),
                None,
                false,
            )
        }),
    }
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    fn pct(v: &str) -> String {
        let b = v.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 < b.len() + 1 {
                let hex = b.get(i + 1..i + 3);
                if let Some(h) = hex
                    && let Ok(x) = u8::from_str_radix(std::str::from_utf8(h).unwrap_or("zz"), 16)
                {
                    out.push(x);
                    i += 3;
                    continue;
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
    // An ownership bounce is NOT a cursor condition: mapping it to
    // cursor_beyond_tail told SDKs to rewind healthy cursors and — by
    // dropping Streams-Replay-To — hid the one signal routers use to
    // converge (the fleet campaign's cross-owner lineage reads died
    // exactly here). Preserve it as its own retryable error.
    if status.as_u16() == 409
        && let Some(to) = raw.headers().get("streams-replay-to").cloned()
    {
        let mut r = perr(
            status,
            "not_stream_owner",
            "another instance owns the target segment; retry through the router",
            None,
            true,
        );
        r.headers_mut().insert("streams-replay-to", to);
        return r;
    }
    let (code, message, retryable) = match status.as_u16() {
        404 => ("not_found", "stream not found", false),
        403 => ("wrong_key", "encryption key mismatch", false),
        410 => ("gone", "stream expired or deleted", false),
        429 => ("rate_limited", "admission or rate limit", true),
        400 => ("invalid_cursor", "invalid cursor or read position", false),
        // deliver=applied only: a session cursor minted past a
        // pre-durability suffix a crash discarded. Not retryable with
        // the SAME cursor — the client resumes from its durable cursor.
        409 => (
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
            false,
        ),
        503 => ("temporarily_unavailable", "retry shortly", true),
        _ => ("read_failed", "read failed", true),
    };
    let mut r = perr(status, code, message, None, retryable);
    if let Some(ra) = raw.headers().get("retry-after") {
        r.headers_mut().insert("retry-after", ra.clone());
    }
    // §10.4: shared-core key mismatches surface here (the core answers
    // a bare 403); key-guessing probes journal like every other
    // denial. The other codes are availability/positioning, not
    // denials of the caller.
    if code == "wrong_key" {
        r = crate::audit::tag(r, "wrong_key");
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
    tenant: &crate::tenant::ProjectId,
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
    // R24-D: strict on this route too. The first pass only made
    // malformed NUMERIC values fail; unknown keys and duplicated
    // scalars were still silently accepted here.
    let q = match strict_query(
        query,
        &["cursor", "deliver", "maxBytes", "routingKey", "waitMs"],
    ) {
        Ok(q) => q,
        Err(r) => return r,
    };
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
    // Opt-in low-latency visibility (spec: subscribe deliver mode):
    // `applied` serves the live tail before storage durability. The
    // records arrive marked (Prisma-Pending-From) and the resume cursor
    // (Prisma-Durable-Cursor) stays clamped to the durable frontier.
    let deliver = match q.get("deliver").map(String::as_str) {
        None | Some("durable") => crate::shard::Deliver::Durable,
        Some("applied") => crate::shard::Deliver::Applied,
        Some(_) => {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_deliver",
                "deliver must be \"durable\" or \"applied\"",
                None,
                false,
            );
        }
    };
    if deliver == crate::shard::Deliver::Applied && live == Some("sse") {
        return perr(
            StatusCode::BAD_REQUEST,
            "deliver_sse_unsupported",
            "deliver=applied works with reads and long-poll subscribe, not SSE",
            None,
            false,
        );
    }
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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
    if deliver == crate::shard::Deliver::Applied && desc.forked_from.is_some() {
        // The fork read path has its own serving machine; bounded scope
        // for the mode's first release. Explicit refusal beats a silent
        // durable downgrade.
        return perr(
            StatusCode::BAD_REQUEST,
            "deliver_unsupported_fork",
            "deliver=applied is not supported on forked streams",
            None,
            false,
        );
    }
    let (skey, epoch) = match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        crate::http::KeyCheck::Wrong => {
            return crate::audit::tag(
                perr(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "encryption key mismatch",
                    None,
                    false,
                ),
                "wrong_key",
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
        Some(c) => {
            match crate::product_cursor::KeyCursor::decode(c, &desc.project_id, &skey, &epoch, &kh)
            {
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
            }
        }
    };
    // CHAOS-4: a value we cannot parse is a client mistake, not a
    // request for the default. Silently substituting the 8 MiB default
    // for `maxBytes=-5` hands back up to 8 MiB to a caller that asked
    // for a small page, and dropping an unparseable `waitMs` turns a
    // long poll into a hot retry loop. `deliver` and `routingKey`
    // already answer 400 here; these two now agree.
    //
    // A parseable-but-tiny maxBytes still clamps up to the 4 KiB floor:
    // a budget below one record cannot be honoured and every read must
    // make progress.
    let max_bytes = match q.get("maxBytes") {
        None => None,
        Some(v) => match v.parse::<usize>() {
            Ok(n) => Some(n.clamp(4096, READ_MAX_BYTES_CAP)),
            Err(_) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_max_bytes",
                    "maxBytes must be a non-negative integer",
                    None,
                    false,
                );
            }
        },
    };
    let timeout = match q.get("waitMs") {
        None => None,
        Some(v) => match v.parse::<u64>() {
            Ok(n) => Some(format!("{n}ms")),
            Err(_) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_wait_ms",
                    "waitMs must be a non-negative integer",
                    None,
                    false,
                );
            }
        },
    };

    let params = crate::http::ReadParams {
        offset,
        format: None,
        live: live.map(str::to_string),
        timeout,
        key: Some(rk.clone()),
        cursor: None,
        sig: None,
        max_bytes,
        deliver,
        no_fanout: false,
        internal: false,
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
    let raw = crate::http::read_inner(
        state,
        tenant.stream_ref(&name),
        name,
        params,
        ih,
        false,
        true,
        surface,
    )
    .await;

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
    .encode(&desc.project_id, &skey);
    // deliver=applied: restate the durable-clamped resume position as a
    // signed product cursor, and pass the provisional-suffix marker
    // through. Absent in durable mode (the raw machine only emits these
    // headers when Applied was requested).
    let durable_tok = raw
        .headers()
        .get("stream-durable-offset")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let pending_from = raw
        .headers()
        .get("stream-pending-from")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let (parts, body) = raw.into_parts();
    let mut r = Response::builder()
        .status(parts.status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-store")
        .header("Prisma-Next-Cursor", cursor_out);
    if let Some(dt) = durable_tok {
        let (dseg, dnext) = raw_token_to_pos(&desc, &rk, &dt);
        let dcur = crate::product_cursor::KeyCursor {
            epoch,
            key_hash: kh,
            seg_id: dseg,
            offset: dnext,
        }
        .encode(&desc.project_id, &skey);
        r = r.header("Prisma-Durable-Cursor", dcur);
    }
    if let Some(pf) = pending_from {
        r = r.header("Prisma-Pending-From", pf);
    }
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
    tenant: &crate::tenant::ProjectId,
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
    let q = match strict_query(query, &["cursor", "maxBytes"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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
            return crate::audit::tag(
                perr(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "encryption key mismatch",
                    None,
                    false,
                ),
                "wrong_key",
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
        Some(c) => {
            match crate::product_cursor::ScanCursor::decode(c, &desc.project_id, &skey, &epoch, now)
            {
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
            }
        }
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
                                    Ok(e) => Some(e),
                                    Err(r) => {
                                        // Cross-owner snapshot: the live
                                        // tail comes from the owner via
                                        // the internal head probe.
                                        let peer = crate::http::replay_peer_url(&state, &r)
                                            .map(|(_, b)| b);
                                        let relayed = match peer {
                                            Some(base) => {
                                                match InternalTarget::of(&desc, sg.seg_id) {
                                                    Some(t) => {
                                                        relay_segment_tail(
                                                            &state, &base, &desc.name, &t, &key_b64,
                                                        )
                                                        .await
                                                    }
                                                    None => None,
                                                }
                                            }
                                            None => None,
                                        };
                                        match relayed {
                                            Some(end) => {
                                                segs.push((sg.seg_id, end));
                                                continue;
                                            }
                                            None => return translate_read_error(r),
                                        }
                                    }
                                };
                                let engine = engine.expect("ok branch");
                                let local = match engine.stream_handle(identity).await {
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
                                // Adopt the shared durable tracker like the
                                // read path does: a handle that lived through
                                // an own->lose->own-again cycle keeps a LOCAL
                                // counter frozen at its last stint while the
                                // interim owner's commits sit in the store
                                // (fleet3 leg C: a snapshot froze a live
                                // segment at 1,013 of 1,826 and exports were
                                // silently short forever after).
                                match engine.durable_absorbed(&identity).await {
                                    Ok((remote, _)) => local.max(remote),
                                    Err(_) => local,
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

    let max = match q_num::<usize>(&q, "maxBytes", "invalid_max_bytes") {
        Ok(v) => v
            .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP))
            .unwrap_or(SCAN_DEFAULT_BYTES),
        Err(r) => return r,
    };
    let is_json = crate::registry::media_type(&desc.content_type) == "application/json";

    let mut idx = sc.current_index as usize;
    let mut off = sc.current_offset;
    let mut spent = 0usize;
    let mut body = Vec::with_capacity(4096);
    body.push(b'[');
    let mut n_items = 0usize;
    let mut bill_bytes = 0u64;
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
        let out = match state.engine_for(&route).await {
            Ok(engine) => {
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
                match crate::http::read_merged(
                    &skey,
                    &epoch,
                    &handle,
                    &engine,
                    off,
                    None,
                    max - spent,
                    crate::shard::Deliver::Durable,
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
                }
            }
            Err(r) => {
                // Cross-owner scan page: fetch this segment's slice
                // (records WITH routing keys) from its owner.
                let peer = crate::http::replay_peer_url(&state, &r).map(|(_, b)| b);
                let relayed = match peer {
                    Some(base) => match InternalTarget::of(&desc, seg_id) {
                        Some(t) => {
                            relay_segment_scan(
                                &state,
                                &base,
                                &desc.name,
                                &t,
                                off,
                                max - spent,
                                &key_b64,
                            )
                            .await
                        }
                        None => None,
                    },
                    None => None,
                };
                match relayed {
                    Some(o) => o,
                    None => return translate_read_error(r),
                }
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
            bill_bytes += r.payload.len() as u64;
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
        r = r.header(
            "Prisma-Next-Scan-Cursor",
            next.encode(&desc.project_id, &skey),
        );
    }
    // §4.2/§5: one scan operation, payload bytes only. Pages fetched
    // from peer owners are included HERE (this is the public delivery)
    // and never metered by the internal endpoint that served them.
    crate::billing::meter_read(&state, &desc, bill_bytes, n_items as u64);
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
    tenant: &crate::tenant::ProjectId,
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
    let desc = match state.registry.get(&tenant.stream_ref(name)).await {
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
        crate::http::KeyCheck::Wrong => Err(crate::audit::tag(
            perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            ),
            "wrong_key",
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
    let route = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
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

/// Opaque consumer version: `{stream_epoch, consumer_generation}`,
/// base64url-encoded. Returned from consumer PUT/GET as
/// `Prisma-Consumer-Version` and REQUIRED on DELETE — a deletion names
/// an incarnation, never a name (round-17 P0: a stale retry by name
/// deleted the replacement consumer; an unpinned saga could rebind to
/// a recreated stream). Not signed: possessing delete authorization is
/// the capability, the token only pins WHICH incarnation it targets.
pub(crate) fn consumer_version_token(epoch: &[u8; 16], generation: u64) -> String {
    use base64::Engine;
    let mut v = [0u8; 24];
    v[..16].copy_from_slice(epoch);
    v[16..].copy_from_slice(&generation.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v)
}

fn parse_consumer_version(tok: &str) -> Option<([u8; 16], u64)> {
    use base64::Engine;
    let v = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tok.as_bytes())
        .ok()?;
    if v.len() != 24 {
        return None;
    }
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(&v[..16]);
    let generation = u64::from_be_bytes(v[16..].try_into().ok()?);
    Some((epoch, generation))
}

async fn product_consumer_put(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Response {
    let (desc, _k, epoch) = match consumer_ctx(&state, tenant, &name, &headers).await {
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
        // §6.1 review item 4 — the compound rule, each leg independent:
        // the GATE already authorized the source consumer
        // (consumers.configure + prefix over the source); wiring a
        // dead-letter target additionally takes dlq.configure AND the
        // credential's prefix grant over the DESTINATION stream —
        // same-project lookup alone is not authorization.
        if let Some(p) = principal {
            if let Err(e) = p.require(crate::tenant::Scope::DlqConfigure) {
                return crate::audit::tag_project(auth_failure_response(&e), &p.project_id);
            }
            if let Err(e) = p.require_stream(&d) {
                return crate::audit::tag_project(auth_failure_response(&e), &p.project_id);
            }
        }
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
        // The link binds inside the SOURCE collection's project — a
        // dead-letter target in another project is unrepresentable.
        let target = match state.registry.get(&desc.ref_in_project(&d)).await {
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
            rec: Some(existing),
            ..
        } if existing.state == crate::queue::ConsumerLifecycle::Deleting => {
            // The name is owned by an in-flight deletion until the
            // saga settles; recreating now would race its fan-out.
            // The response CARRIES the deleting incarnation's version
            // token (round 18): if the client that started the
            // deletion died without persisting it, this is the public
            // way for ANY process to obtain the token and resume the
            // saga (DELETE with it), instead of the consumer staying
            // Deleting forever.
            let mut r = perr(
                StatusCode::CONFLICT,
                "consumer_deleting",
                "a deletion of this consumer is in progress; resume it by \
                 retrying DELETE with the Prisma-Consumer-Version on this \
                 response, or retry this create shortly",
                None,
                true,
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&consumer_version_token(
                &epoch,
                existing.generation,
            )) {
                r.headers_mut().insert("prisma-consumer-version", v);
            }
            r
        }
        crate::queue::QueueOut::Config {
            conflict: true,
            rec: Some(existing),
            ..
        } => {
            let mut r = perr(
                StatusCode::CONFLICT,
                "consumer_config_conflict",
                "consumer exists with different configuration",
                serde_json::from_str(&consumer_cfg_json(&cname, &existing.config)).ok(),
                false,
            );
            r.headers_mut().insert(
                header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            r
        }
        crate::queue::QueueOut::Config {
            rec: Some(c),
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
            .header(
                "Prisma-Consumer-Version",
                consumer_version_token(&epoch, c.generation),
            )
            .body(Body::from(consumer_cfg_json(&cname, &c.config)))
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
) -> Response {
    let (desc, _k, epoch) = match consumer_ctx(&state, tenant, &name, &headers).await {
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
        Ok(crate::queue::QueueOut::Config { rec: Some(c), .. })
            if c.state == crate::queue::ConsumerLifecycle::Active =>
        {
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-store")
                .header(
                    "Prisma-Consumer-Version",
                    consumer_version_token(&epoch, c.generation),
                )
                .body(Body::from(consumer_cfg_json(&cname, &c.config)))
                .unwrap()
        }
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

/// Per-step cleanup budgets (rows staged per committer submit) and the
/// per-REQUEST step budget. A million-row residue is deleted across
/// many bounded, durably-committed steps — each retryable request makes
/// monotone progress against the reduced durable row set instead of
/// rebuilding one unbounded batch (round-17 P0).
const CONSUMER_DELETE_STEP_ROWS: usize = 4096;
const CONSUMER_DELETE_STEP_BYTES: usize = 1 << 20;
const CONSUMER_DELETE_REQUEST_STEPS: u32 = 512;
/// Segments are physically independent (own engines, own rows): sweep
/// them concurrently, boundedly.
const CONSUMER_DELETE_SEGMENT_CONCURRENCY: usize = 8;

// ---- fleet-internal segment fan-out (cross-owner consumer ops) ------

fn json_ok(v: serde_json::Value) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(v.to_string()))
        .unwrap()
}

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
        Some(InternalTarget {
            project_id: desc.project_id.clone(),
            stream_epoch: desc.epoch_bytes()?,
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

/// §16 receiver side, step one: construct the registry identity FROM
/// THE SENDER'S project header — never from the deployment tenant —
/// so a multi-project cell can never bind a peer's request to the
/// wrong project's stream. The reserved system project is legal here
/// (§10.4: only internal workload identity touches system streams).
pub(crate) fn internal_sref(
    headers: &HeaderMap,
    name: &str,
) -> Result<crate::tenant::TenantStreamRef, Response> {
    let invalid = |why: &str| perr(StatusCode::BAD_REQUEST, "invalid_target", why, None, false);
    let raw = headers
        .get("streams-internal-project")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| invalid("internal requests must carry the project header"))?;
    let project =
        crate::tenant::ProjectId::new(raw).map_err(|_| invalid("malformed internal project id"))?;
    let cn = crate::tenant::CanonicalStreamName::new(name)
        .map_err(|_| invalid("non-canonical internal stream name"))?;
    Ok(crate::tenant::TenantStreamRef::new(project, cn))
}

/// Receiver-side verification of an internal RPC target against the
/// descriptor that currently owns the name. Returns (segment, derived
/// identity), or a response the handler must return unchanged:
/// epoch mismatch, unknown segment, or identity disagreement all answer
/// `409 stale_target` WITHOUT touching any state — the caller's
/// incarnation is gone and its request must never bind to the
/// replacement.
pub(crate) fn verify_internal_target(
    desc: &StreamDesc,
    headers: &HeaderMap,
) -> Result<(u32, [u8; 16]), Response> {
    let h = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let stale = |why: &str| {
        perr(
            StatusCode::CONFLICT,
            "stale_target",
            &format!("internal target does not match the current incarnation ({why})"),
            None,
            false,
        )
    };
    let (Some(epoch_hex), Some(seg_id)) = (
        h("streams-internal-epoch"),
        h("streams-internal-seg").and_then(|v| v.parse::<u32>().ok()),
    ) else {
        return Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_target",
            "internal requests must carry epoch and segment headers",
            None,
            false,
        ));
    };
    let Some(want_epoch) =
        crate::crypto::unhex(&epoch_hex).and_then(|v| <[u8; 16]>::try_from(v).ok())
    else {
        return Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_target",
            "malformed internal epoch",
            None,
            false,
        ));
    };
    if desc.epoch_bytes() != Some(want_epoch) {
        return Err(stale("epoch"));
    }
    // §16: the loaded descriptor must belong to the project the sender
    // addressed. With the registry lookup itself keyed by the header
    // project this is a pure corruption check — but it stays, because
    // a silent project swap here would be a cross-tenant bind.
    if let Some(p) = headers
        .get("streams-internal-project")
        .and_then(|v| v.to_str().ok())
        && p != desc.project_id.as_str()
    {
        return Err(stale("project"));
    }
    // Segment 0 is the implicit single segment and always exists.
    let known = seg_id == 0
        || desc
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.iter().any(|sg| sg.seg_id == seg_id));
    if !known {
        return Err(stale("segment"));
    }
    let identity = desc.dynamic_segment_identity(seg_id);
    if let Some(want_id) = h("streams-internal-identity") {
        let matches = crate::crypto::unhex(&want_id)
            .and_then(|v| <[u8; 16]>::try_from(v).ok())
            .is_some_and(|w| w == identity);
        if !matches {
            return Err(stale("identity"));
        }
    }
    Ok((seg_id, identity))
}
//
// A split child lives on its own shard route, so a consumer's segments
// can be owned by different instances. The saga driver and the pull
// walk relay the per-segment piece to its owner over the fleet-internal
// endpoints below (bearer = the fleet's shared token; depth is one —
// the handlers never relay again). Ownership 409s from the handlers
// flow back and the caller surfaces its normal retryable error.

/// Relay one segment's ConfigDeleteStep loop to its owner. Chunks the
/// caller's remaining step budget so a relayed segment obeys the same
/// per-request bound as a local one (durable progress either way).
async fn relay_sweep_segment(
    state: &Arc<AppState>,
    base: &str,
    name: &str,
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
        let mut req = crate::http::peer_client()
            .post(format!(
                "{base}/v1/internal/sweep-segment/{}",
                crate::http::encode_stream_name_path(name)
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
        if let Some(t) = &state.fleet_internal_token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let reply: Option<serde_json::Value> = match req.send().await {
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

/// Fleet-internal sweep target: run bounded ConfigDeleteStep rounds for
/// ONE locally-owned segment. fence_below arrives from the caller so
/// the generation-fenced cleanup semantics (round 17) hold unchanged.
pub(crate) async fn internal_sweep_segment(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !crate::http::fleet_internal_authorized(&state, &headers) {
        return crate::http::internal_unauthorized();
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Doc {
        consumer: String,
        seg_id: u32,
        fence_below: u64,
        max_steps: i64,
    }
    let doc: Doc = match serde_json::from_slice(&body) {
        Ok(d) => d,
        Err(e) => {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                &e.to_string(),
                None,
                false,
            );
        }
    };
    let sref = match internal_sref(&headers, &name) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) => d,
        _ => return perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false),
    };
    // ABA GUARD (round-19): a stale sweep must never fence or delete a
    // RECREATED stream's consumer state. Verified before the engine is
    // even opened, so a mismatch touches nothing.
    let (seg_id, identity) = match verify_internal_target(&desc, &headers) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if seg_id != doc.seg_id {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_target",
            "segment header and body disagree",
            None,
            false,
        );
    }
    let route = desc.segment_route_by_id(seg_id);
    let engine = match state.engine_for(&route).await {
        Ok(e) => e,
        Err(r) => return r, // ownership moved: the 409 tells the relayer
    };
    let mut steps = 0i64;
    loop {
        if steps >= doc.max_steps.clamp(1, CONSUMER_DELETE_REQUEST_STEPS as i64) {
            return json_ok(json!({"complete": false, "steps": steps}));
        }
        steps += 1;
        match engine
            .submit_queue(
                identity,
                crate::queue::QueueOp::ConfigDeleteStep {
                    consumer: doc.consumer.clone(),
                    fence_below: doc.fence_below,
                    max_rows: CONSUMER_DELETE_STEP_ROWS,
                    max_bytes: CONSUMER_DELETE_STEP_BYTES,
                },
            )
            .await
        {
            Ok(crate::queue::QueueOut::DeleteStep { complete: true, .. }) => {
                return json_ok(json!({"complete": true, "steps": steps}));
            }
            Ok(crate::queue::QueueOut::DeleteStep {
                complete: false, ..
            }) => continue,
            other => {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "segment_cleanup_failed",
                    &format!("relayed cleanup step failed: {other:?}"),
                    None,
                    true,
                );
            }
        }
    }
}

/// Fleet-internal consumer-cursor probe for ONE locally-owned segment:
/// (queue cursor, durable tail). Lets a pull walk skip a FOREIGN drained
/// predecessor and yield past a FOREIGN empty live sibling without
/// taking the segment's engine — the two cases whole-request replay
/// cannot converge on (each owner would bounce on the other's segment).
pub(crate) async fn internal_queue_cursor(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    if !crate::http::fleet_internal_authorized(&state, &headers) {
        return crate::http::internal_unauthorized();
    }
    let q = |h: &str| {
        headers
            .get(h)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let (Some(consumer), Some(cgen)) = (
        q("streams-internal-consumer"),
        q("streams-internal-gen").and_then(|v| v.parse::<u64>().ok()),
    ) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "consumer/gen headers required",
            None,
            false,
        );
    };
    let sref = match internal_sref(&headers, &name) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) => d,
        _ => return perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false),
    };
    // ABA GUARD: cursor/tail state of a RECREATED stream must never be
    // reported to a caller holding the previous incarnation.
    let (seg_id, identity) = match verify_internal_target(&desc, &headers) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let route = desc.segment_route_by_id(seg_id);
    let engine = match state.engine_for(&route).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    let cursor = engine
        .queue_cursor(identity, &consumer, cgen)
        .await
        .unwrap_or(0);
    let local = match engine.stream_handle(identity).await {
        Ok(h) => h.state.lock().unwrap().durable.next,
        Err(_) => 0,
    };
    // Same adoption as the read path — the local counter understates a
    // segment whose interim commits landed under another owner.
    let tail = match engine.durable_absorbed(&identity).await {
        Ok((remote, _)) => local.max(remote),
        Err(_) => local,
    };
    json_ok(json!({"cursor": cursor, "tail": tail}))
}

/// Relay a cursor/tail probe to a segment's owner. None on any failure
/// — the caller falls back to its normal ownership error.
async fn relay_queue_cursor(
    state: &Arc<AppState>,
    base: &str,
    name: &str,
    target: &InternalTarget,
    cname: &str,
    cgen: u64,
) -> Option<(u64, u64)> {
    let mut req = crate::http::peer_client()
        .get(format!(
            "{base}/v1/internal/queue-cursor/{}",
            crate::http::encode_stream_name_path(name)
        ))
        .timeout(std::time::Duration::from_secs(15))
        .header("streams-internal-consumer", cname)
        .header("streams-internal-gen", cgen.to_string());
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    if let Some(t) = &state.fleet_internal_token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let v: serde_json::Value = match req.send().await {
        Ok(r) if r.status().is_success() => r.json().await.ok()?,
        _ => return None,
    };
    Some((v["cursor"].as_u64()?, v["tail"].as_u64()?))
}

/// Fleet-internal scan-page source: read_merged over the wire for ONE
/// locally-owned segment, records with their routing keys (a raw page
/// carries payloads only, and scan items surface routingKey per
/// record). Parameters ride internal headers; the stream key rides its
/// normal header because the payloads must be decrypted here.
pub(crate) async fn internal_segment_scan(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    if !crate::http::fleet_internal_authorized(&state, &headers) {
        return crate::http::internal_unauthorized();
    }
    let q = |h: &str| {
        headers
            .get(h)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let (Some(from), Some(max_bytes), Some(key_b64)) = (
        q("streams-internal-from").and_then(|v| v.parse::<u64>().ok()),
        q("streams-internal-max-bytes")
            .and_then(|v| v.parse::<usize>().ok())
            // Clamped to the public scan ceiling: an internal budget
            // header must not buy a larger page than the operation it
            // relays for (round-19 security finding).
            .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP)),
        q("stream-encryption-key"),
    ) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "from/max-bytes/key headers required",
            None,
            false,
        );
    };
    let sref = match internal_sref(&headers, &name) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) => d,
        _ => return perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false),
    };
    // ABA GUARD: never serve a recreated stream's records to a caller
    // that asked about the previous incarnation.
    let (seg_id, identity) = match verify_internal_target(&desc, &headers) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (skey, epoch) = match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        _ => return perr(StatusCode::FORBIDDEN, "wrong_key", "key", None, false),
    };
    let route = desc.segment_route_by_id(seg_id);
    let engine = match state.engine_for(&route).await {
        Ok(e) => e,
        Err(r) => return r,
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
    let out = match crate::http::read_merged(
        &skey,
        &epoch,
        &handle,
        &engine,
        from,
        None,
        max_bytes,
        crate::shard::Deliver::Durable,
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
    use base64::Engine as _;
    let items: Vec<serde_json::Value> = out
        .recs
        .iter()
        .map(|r| {
            json!({
                "off": r.off,
                "rk": r.rkey,
                "p": base64::engine::general_purpose::STANDARD.encode(&r.payload),
            })
        })
        .collect();
    json_ok(json!({
        "items": items,
        "last": out.last,
        "end": out.end,
        "completed": out.completed,
    }))
}

/// Relay one scan page's segment read to its owner; None on failure.
async fn relay_segment_scan(
    state: &Arc<AppState>,
    base: &str,
    name: &str,
    target: &InternalTarget,
    from: u64,
    max_bytes: usize,
    key_b64: &str,
) -> Option<crate::http::ReadOut> {
    let mut req = crate::http::peer_client()
        .get(format!(
            "{base}/v1/internal/segment-scan/{}",
            crate::http::encode_stream_name_path(name)
        ))
        .timeout(std::time::Duration::from_secs(20))
        .header("streams-internal-from", from.to_string())
        .header("streams-internal-max-bytes", max_bytes.to_string())
        .header("stream-encryption-key", key_b64);
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    if let Some(t) = &state.fleet_internal_token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let v: serde_json::Value = match req.send().await {
        Ok(r) if r.status().is_success() => r.json().await.ok()?,
        _ => return None,
    };
    use base64::Engine as _;
    let recs = v["items"]
        .as_array()?
        .iter()
        .map(|it| {
            Some(crate::http::PlainRec {
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
    Some(crate::http::ReadOut {
        recs,
        last: v["last"].as_u64(),
        end: v["end"].as_u64()?,
        completed: v["completed"].as_bool()?,
    })
}

/// Relay a segment-tail probe (scan snapshot creation) via the internal
/// segment read's head path: Stream-Next-Offset on the reply IS the
/// segment's durable end.
async fn relay_segment_tail(
    state: &Arc<AppState>,
    base: &str,
    name: &str,
    target: &InternalTarget,
    key_b64: &str,
) -> Option<u64> {
    let tok = crate::offsets::encode_ep(target.seg_id, crate::offsets::Offset::START);
    let mut req = crate::http::peer_client()
        .get(format!(
            "{base}/v1/internal/segment-read/{}?offset={tok}&head=1",
            crate::http::encode_stream_name_path(name)
        ))
        .timeout(std::time::Duration::from_secs(15))
        .header("stream-encryption-key", key_b64);
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    if let Some(t) = &state.fleet_internal_token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let r = match req.send().await {
        Ok(r) if r.status().is_success() => r,
        _ => return None,
    };
    let tok = r.headers().get("stream-next-offset")?.to_str().ok()?;
    let (_, off) = crate::offsets::parse_ep(tok).ok()?;
    Some(off.scan_from())
}

async fn product_consumer_delete(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
) -> Response {
    // Entry ordering is DELIBERATE and differs from consumer_ctx
    // (round 18): the version token's stream epoch is compared BEFORE
    // the encryption key is validated. A client retrying a stale
    // DELETE after the collection was deleted and recreated under a
    // DIFFERENT key holds the OLD key — the honest answer is the
    // no-touch 204 ("your target is gone"), not 403. Bearer
    // authorization already ran at the route gate; the key check
    // still guards every path that touches a LIVE target.
    let Some(key_b64) = product_key(&headers) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        );
    };
    let no_touch_204 = || {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::empty())
            .unwrap()
    };
    let Some(vtok) = headers
        .get("prisma-consumer-version")
        .and_then(|v| v.to_str().ok())
    else {
        return perr(
            StatusCode::BAD_REQUEST,
            "missing_consumer_version",
            "DELETE requires Prisma-Consumer-Version (returned by consumer create/get, \
             and by the consumer_deleting conflict); a deletion targets an incarnation, \
             not a name",
            None,
            false,
        );
    };
    let Some((expect_epoch, expect_gen)) = parse_consumer_version(vtok) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_consumer_version",
            "Prisma-Consumer-Version is not a version token from this server",
            None,
            false,
        );
    };
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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
            // The collection is gone; so is the token's target.
            return no_touch_204();
        }
        Err(e) => {
            return perr(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                &format!("registry unavailable: {e}"),
                None,
                true,
            );
        }
    };
    let Some(epoch) = desc.epoch_bytes() else {
        return perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "bad descriptor",
            None,
            true,
        );
    };
    if expect_epoch != epoch {
        // The stream incarnation the version was minted under no longer
        // exists — the old target died with it. Idempotent success, the
        // CURRENT stream untouched, and deliberately BEFORE the key
        // check (the old client may hold a rotated-away key).
        return no_touch_204();
    }
    // Same incarnation: from here on we may touch live state, so the
    // key must validate.
    if !matches!(
        crate::http::check_key(Some(&key_b64), &desc),
        crate::http::KeyCheck::Ok(..)
    ) {
        return crate::audit::tag(
            perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            ),
            "wrong_key",
        );
    }
    // Collection-wide deletion as a GENERATION-FENCED SAGA (rounds
    // 16-17). Invariant: 204 means the TARGETED INCARNATION's deletion
    // is collection-wide — every segment's dead-generation rows are
    // gone and no write of that generation can land afterwards. Any
    // failure propagates; the retry (same endpoint, same version)
    // resumes from the Deleting state and the durably reduced row set.
    //
    //   0. The request names an INCARNATION, not a name: the required
    //      Prisma-Consumer-Version pins {stream epoch, consumer
    //      generation}. A stale retry whose target no longer exists
    //      gets an idempotent 204 and touches NOTHING (round-17 ABA).
    //   1. Parent record: Active -> Deleting, fenced to the exact
    //      generation. New pull/settle refuse from this instant.
    //   2. Every segment (current AND predecessor — the pull lineage):
    //      install the generation fence, then delete the dead
    //      generations' rows in bounded steps, segments swept
    //      concurrently. Any engine/submit failure -> 503, no 204.
    //   3. Re-read the segment map — REFUSING a changed stream epoch —
    //      and repeat until stable across a fan-out round (a split
    //      racing the saga gets its new children swept too).
    //   4. Parent record: Deleting -> Deleted (a TOMBSTONE, kept so
    //      recreation allocates generation+1 and dead-generation
    //      residue stays inert forever).
    let rec = match consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.clone(),
        },
    )
    .await
    {
        Ok(crate::queue::QueueOut::Config { rec, .. }) => rec,
        Ok(_) => unreachable!("ConfigGet answers Config"),
        Err(r) => return r,
    };
    let rec = match rec {
        None => {
            // The version claims a generation this server never made
            // (or whose tombstone is gone — impossible pre-GC). With
            // no record at all there is nothing to protect and nothing
            // to do.
            return no_touch_204();
        }
        Some(r) => r,
    };
    if rec.generation > expect_gen {
        // The named generation is already dead and buried (the record
        // has moved on — tombstone or a recreated consumer). The old
        // target is gone; the CURRENT generation is another
        // incarnation's property. Idempotent success, no mutation.
        return no_touch_204();
    }
    if rec.generation < expect_gen {
        // A version newer than the record is impossible from an honest
        // client: refuse without mutating anything.
        return perr(
            StatusCode::CONFLICT,
            "consumer_version_conflict",
            "the presented consumer version is newer than the server's record",
            None,
            false,
        );
    }
    if rec.state == crate::queue::ConsumerLifecycle::Deleted {
        // Exactly the targeted generation, already fully deleted.
        return no_touch_204();
    }
    let cgen = rec.generation;
    if rec.state == crate::queue::ConsumerLifecycle::Active
        && let Err(r) = consumer_config_op(
            &state,
            &desc,
            crate::queue::QueueOp::ConfigLifecycle {
                consumer: cname.clone(),
                expect_gen: cgen,
                deleting: true,
            },
        )
        .await
    {
        return r;
    }
    // Fan out until the segment set is stable across a full round.
    // Segments are swept CONCURRENTLY (bounded) and each segment is
    // stepped to completion within this request's step budget.
    let steps_left = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
        CONSUMER_DELETE_REQUEST_STEPS as i64,
    ));
    let mut cur_desc = desc.clone();
    for _round in 0..5 {
        let segs = consumer_segments(&cur_desc);
        // The incarnation this round's sweep is bound to. A relayed
        // step carries it so a peer can refuse the request outright if
        // the name has since been recreated (round-19 ABA).
        let round_epoch = cur_desc.epoch_bytes();
        let sweeps = segs.iter().copied().map(|(seg_id, identity, route, _)| {
            let state = state.clone();
            let cname = cname.clone();
            let name = name.clone();
            let project = cur_desc.project_id.clone();
            let steps_left = steps_left.clone();
            async move {
                let engine = match state.engine_for(&route).await {
                    Ok(e) => e,
                    Err(r) => {
                        // Cross-owner sweep fan-out: run this segment's
                        // DeleteStep loop on its owner. The borrow of r
                        // ends before the await (axum Body is !Sync).
                        let peer = crate::http::replay_peer_url(&state, &r).map(|(_, b)| b);
                        if let Some(base) = peer {
                            let Some(stream_epoch) = round_epoch else {
                                return Err((
                                    "segment_unavailable",
                                    format!(
                                        "segment {seg_id}: no incarnation to bind the \
                                         relayed sweep to; retry"
                                    ),
                                ));
                            };
                            let t = InternalTarget {
                                project_id: project.clone(),
                                stream_epoch,
                                seg_id,
                                identity,
                            };
                            return relay_sweep_segment(
                                &state,
                                &base,
                                &name,
                                &t,
                                &cname,
                                cgen + 1,
                                &steps_left,
                            )
                            .await;
                        }
                        return Err((
                            "segment_unavailable",
                            format!(
                                "segment {seg_id}'s owner is unavailable; the deletion \
                                 is incomplete — retry"
                            ),
                        ));
                    }
                };
                loop {
                    if steps_left.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) <= 0 {
                        return Err((
                            "segment_cleanup_incomplete",
                            format!(
                                "segment {seg_id} still has rows after this request's \
                                 cleanup budget; progress is durable — retry to resume"
                            ),
                        ));
                    }
                    match engine
                        .submit_queue(
                            identity,
                            crate::queue::QueueOp::ConfigDeleteStep {
                                consumer: cname.clone(),
                                fence_below: cgen + 1,
                                max_rows: CONSUMER_DELETE_STEP_ROWS,
                                max_bytes: CONSUMER_DELETE_STEP_BYTES,
                            },
                        )
                        .await
                    {
                        Ok(crate::queue::QueueOut::DeleteStep { complete: true, .. }) => {
                            return Ok(());
                        }
                        Ok(crate::queue::QueueOut::DeleteStep {
                            complete: false, ..
                        }) => continue,
                        Ok(_) => {
                            return Err((
                                "segment_cleanup_failed",
                                format!(
                                    "segment {seg_id} cleanup answered an unexpected \
                                     outcome; the deletion is incomplete — retry"
                                ),
                            ));
                        }
                        Err(m) => {
                            return Err((
                                "segment_cleanup_failed",
                                format!(
                                    "segment {seg_id} cleanup failed ({m}); the \
                                     deletion is incomplete — retry"
                                ),
                            ));
                        }
                    }
                }
            }
        });
        use futures_util::StreamExt as _;
        let results: Vec<Result<(), (&'static str, String)>> = futures_util::stream::iter(sweeps)
            .buffer_unordered(CONSUMER_DELETE_SEGMENT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for r in results {
            if let Err((code, msg)) = r {
                return perr(StatusCode::SERVICE_UNAVAILABLE, code, &msg, None, true);
            }
        }
        let mut swept_ids: Vec<u32> = segs.iter().map(|(id, ..)| *id).collect();
        swept_ids.sort_unstable();
        #[cfg(test)]
        crate::http::fork_failpoints::pause_consumer_saga_before_refresh(&name).await;
        // FAIL-CLOSED refresh (round 18). Completion is proven by a
        // SUCCESSFUL post-sweep read of the authoritative map: the
        // segments swept this round must equal the segments visible
        // AFTER the sweep, with no topology transition pending. The
        // previous shape treated a refresh error — or a vanished
        // descriptor — as "keep the cached map", which could let a
        // stale pre-split map look stable for two rounds and publish
        // a false collection-wide 204.
        state.registry.invalidate(&tenant.stream_ref(&name));
        let fresh = match state.registry.get(&tenant.stream_ref(&name)).await {
            Ok(Some(d)) if crate::http::desc_alive(&d) => d,
            Ok(_) => {
                // The collection is gone mid-saga; so is the target.
                // Nothing to finalize, nothing to touch.
                return no_touch_204();
            }
            Err(e) => {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "segment_map_unverified",
                    &format!(
                        "cannot verify the segment map after cleanup ({e}); \
                         the deletion is incomplete — retry"
                    ),
                    None,
                    true,
                );
            }
        };
        // EPOCH PIN (round-17 P0): the refresh is by NAME, and the
        // name may now belong to a recreated stream. This saga's
        // authority extends only to the incarnation it targeted — a
        // changed epoch means the old stream (and with it the old
        // consumer) is gone. Idempotent success, replacement
        // untouched.
        if fresh.epoch_bytes() != Some(epoch) {
            return no_touch_204();
        }
        let pending = fresh.segments.as_ref().is_some_and(|m| m.pending.is_some());
        let mut fresh_ids: Vec<u32> = consumer_segments(&fresh)
            .iter()
            .map(|(id, ..)| *id)
            .collect();
        fresh_ids.sort_unstable();
        if !pending && fresh_ids == swept_ids {
            // Everything that can hold this consumer's rows was swept
            // AFTER its fence went up, and the authoritative map —
            // read successfully, transition-free — confirms no segment
            // escaped the sweep. (A split landing after this read
            // cannot mint state for a Deleting consumer: pulls consult
            // the parent record first.)
            if let Err(r) = consumer_config_op(
                &state,
                &desc,
                crate::queue::QueueOp::ConfigLifecycle {
                    consumer: cname.clone(),
                    expect_gen: cgen,
                    deleting: false,
                },
            )
            .await
            {
                return r;
            }
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())
                .unwrap();
        }
        cur_desc = fresh;
    }
    perr(
        StatusCode::SERVICE_UNAVAILABLE,
        "segment_map_unstable",
        "the collection kept splitting during deletion; retry",
        None,
        true,
    )
}

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

async fn load_consumer_record(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    cname: &str,
) -> Result<crate::queue::ConsumerRecord, Response> {
    match consumer_config_op(
        state,
        desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.to_string(),
        },
    )
    .await?
    {
        crate::queue::QueueOut::Config { rec: Some(r), .. }
            if r.state == crate::queue::ConsumerLifecycle::Active =>
        {
            Ok(r)
        }
        crate::queue::QueueOut::Config { rec: Some(r), .. }
            if r.state == crate::queue::ConsumerLifecycle::Deleting =>
        {
            Err(perr(
                StatusCode::CONFLICT,
                "consumer_deleting",
                "this consumer is being deleted",
                None,
                false,
            ))
        }
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
    cgen: u64,
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
        (Some(dlq), Some(want)) => match state.registry.get(&desc.ref_in_project(dlq)).await {
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
            let resp = product_append(
                state.clone(),
                &desc.project_id,
                dlq.clone(),
                ih,
                Bytes::from(body),
                false,
                None,
            )
            .await;
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
                    cgen,
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key_b64 = product_key(&headers).unwrap_or_default();
    let (desc, skey, epoch) = match consumer_ctx(&state, tenant, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rec = match load_consumer_record(&state, &desc, &cname).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let cgen = rec.generation;
    let cfg = rec.config;
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
                Err(r) => {
                    // Cross-owner pull: a FOREIGN drained predecessor or
                    // empty live sibling must not stop the walk — probe
                    // its cursor/tail on the owner and skip past it. A
                    // foreign segment with deliverable backlog keeps the
                    // ownership 409 (leases are owner-local; the router
                    // replays the pull to the owner, which now skips OUR
                    // segments the same way — converges).
                    let peer = crate::http::replay_peer_url(&state, &r).map(|(_, b)| b);
                    if let Some(base) = peer
                        && let Some((cur, tail)) = match InternalTarget::of(&desc, seg_id) {
                            Some(t) => {
                                relay_queue_cursor(&state, &base, &desc.name, &t, &cname, cgen)
                                    .await
                            }
                            None => None,
                        }
                    {
                        match sealed_end {
                            Some(end) if cur >= end => continue,
                            None if tail <= cur => continue,
                            _ => {}
                        }
                    }
                    return translate_read_error(r);
                }
            };
            if let Some(end) = sealed_end {
                let cursor = engine
                    .queue_cursor(identity, &cname, cgen)
                    .await
                    .unwrap_or(0);
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
            let cursor = engine
                .queue_cursor(identity, &cname, cgen)
                .await
                .unwrap_or(0);
            let out = match crate::http::read_merged(
                &skey,
                &epoch,
                &handle,
                &engine,
                cursor,
                None,
                4 << 20,
                crate::shard::Deliver::Durable,
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
            #[cfg(test)]
            crate::http::fork_failpoints::pause_pull_before_receive(&desc.name).await;
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
                    return perr(StatusCode::NOT_FOUND, "consumer_not_found", &m, None, false);
                }
                Err(m) if m.starts_with("consumer_generation_fenced") => {
                    return perr(StatusCode::CONFLICT, "consumer_deleted", &m, None, false);
                }
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
                    &state, &desc, &cfg, cgen, &cname, &key_b64, &skey, &epoch, identity, route,
                    seg_id, &poisoned, &by_off,
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
                        consumer_gen: cgen,
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
                        "id": msg.encode(&desc.project_id, &skey),
                        "routingKey": rkey,
                        "attempts": attempts,
                        "leaseToken": lease.encode(&desc.project_id, &skey),
                        "value": value,
                    }));
                }
                let delivered_payload: u64 = leased
                    .iter()
                    .filter_map(|(off, ..)| by_off.get(off))
                    .map(|(_, p)| p.len() as u64)
                    .sum();
                crate::billing::meter_pull(&state, &desc, delivered_payload, messages.len() as u64);
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
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key_b64 = product_key(&headers).unwrap_or_default();
    let (desc, skey, epoch) = match consumer_ctx(&state, tenant, &name, &headers).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rec = match load_consumer_record(&state, &desc, &cname).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let cgen = rec.generation;
    let cfg = rec.config;
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
            Err(r) => return translate_read_error(r),
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
                return perr(StatusCode::NOT_FOUND, "consumer_not_found", &m, None, false);
            }
            Err(m) if m.starts_with("consumer_generation_fenced") => {
                return perr(StatusCode::CONFLICT, "consumer_deleted", &m, None, false);
            }
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
            if let Ok(out) = crate::http::read_merged(
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
                &state, &desc, &cfg, cgen, &cname, &key_b64, &skey, &epoch, identity, route,
                seg_id, &poisoned, &by_off,
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

async fn product_watches_list(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
) -> Response {
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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

async fn product_watch_get(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    w: String,
) -> Response {
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
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
/// Global pre-auth token bucket for capability-driven registry
/// lookups (SR-3): generous for real clients, a hard wall for forged-
/// capability scanning. 500/s refill, 1000 burst.
fn cap_lookup_allowed() -> bool {
    static B: std::sync::OnceLock<std::sync::Mutex<(f64, i64)>> = std::sync::OnceLock::new();
    const RATE: f64 = 500.0;
    const BURST: f64 = 1000.0;
    let now = crate::shard::now_ms();
    let m = B.get_or_init(|| std::sync::Mutex::new((BURST, now)));
    let mut g = m.lock().unwrap();
    let dt = ((now - g.1).max(0) as f64) / 1000.0;
    g.0 = (g.0 + dt * RATE).min(BURST);
    g.1 = now;
    if g.0 >= 1.0 {
        g.0 -= 1.0;
        true
    } else {
        false
    }
}

async fn product_watch_wait(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    w: String,
    key_hex: String,
    headers: HeaderMap,
    query: &str,
) -> Response {
    // ONE refusal for every "no valid capability for this URL" shape —
    // missing stream, deleted stream, bad/expired/forged capability —
    // so an unauthenticated prober cannot use this route (reachable
    // without any token, §15) as a stream-existence oracle. Existence-
    // revealing answers (creating, unknown watch) come only AFTER the
    // capability or key verifies.
    let refuse = || {
        // Journaled (§10.4): capability probing is exactly the class a
        // security review reconstructs. No project — the refusal fires
        // BEFORE anything verifies, and unverified claims are never
        // identity.
        crate::audit::tag(
            perr(
                StatusCode::FORBIDDEN,
                "watch_unauthorized",
                "a valid observation capability or Prisma-Encryption-Key is required",
                None,
                false,
            ),
            "watch_unauthorized",
        )
    };
    // Review item 3: a capability CARRIES its project (wire v2), so a
    // bearer-free observation resolves the right project's same-named
    // stream BEFORE the registry lookup. Key-holder requests (no cap)
    // stay on the request tenant. A cap whose project fails the ID
    // grammar gets the uniform refusal, same as a forged signature.
    let cap_early = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Prisma-Watch "))
        .map(str::to_string)
        .or_else(
            || match strict_query(query, &["cursor", "cap", "timeoutMs"]) {
                Ok(q) => q.get("cap").cloned(),
                Err(_) => None,
            },
        );
    let cap_tenant = match &cap_early {
        Some(c) => match crate::crypto::watch_capability_project(c) {
            Some(p) => Some(p),
            None => return refuse(),
        },
        None => None,
    };
    // SR-3 pre-auth backstop: a forged capability drives one
    // project-qualified registry lookup BEFORE anything verifies.
    // Bound the global rate so capability probing cannot become
    // registry pressure; the uniform refusal keeps the non-oracle.
    if cap_tenant.is_some() && !cap_lookup_allowed() {
        return refuse();
    }
    let lookup_tenant = cap_tenant.as_ref().unwrap_or(tenant);
    let desc = match state.registry.get(&lookup_tenant.stream_ref(&name)).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => d,
        _ => return refuse(),
    };
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
    let q = match strict_query(query, &["cursor", "cap", "timeoutMs"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let Some(epoch) = desc.epoch_bytes() else {
        return perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "descriptor missing epoch",
            None,
            true,
        );
    };
    // Auth (§15): a watch-observation CAPABILITY, or the full
    // encryption key. The capability chain is derivable by any
    // stream-key holder, offline:
    //   touch_token(key, epoch) -> wait_sig_key -> watch_capability_sig
    // and the server verifies against the wait_sig_key persisted in
    // the descriptor at create — no stream keys held server-side, so
    // capabilities keep working across restarts and cold processes.
    // Unlike the retired `sig=` design the capability is bound to
    // project + name + epoch + watch + key + METHOD + EXPIRY, expires
    // in <=5 minutes, and is accepted from the Prisma-Watch
    // Authorization scheme (preferred) or a `cap=` query parameter
    // for EventSource clients. REDACTION: never log the query string
    // or Authorization header of this route.
    let cap = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Prisma-Watch "))
        .map(str::to_string)
        .or_else(|| q.get("cap").cloned());
    let cap_ok = cap.is_some_and(|c| {
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
        crate::crypto::verify_watch_capability(
            &c,
            &sk,
            &desc.sref(),
            &desc.stream_epoch,
            &w,
            &key_hex,
            "GET",
            crate::shard::now_ms() / 1000,
        )
    });
    if !cap_ok {
        let key_ok = product_key(&headers).is_some_and(|kb| {
            matches!(
                crate::http::check_key(Some(&kb), &desc),
                crate::http::KeyCheck::Ok(..)
            )
        });
        if !key_ok {
            return refuse();
        }
    }
    // Authorized from here on: these answers reveal state and must not
    // be reachable by an unauthenticated probe.
    //
    // SR-3 (Søren review): the verified capability IS a restricted
    // principal for lookup_tenant's project. Hold it to the CURRENT
    // project policy and occupy the project's admission capacity for
    // the WHOLE wait — a suspended project's capabilities die with the
    // policy, and capability waiters cannot bypass the §17.3 ceilings.
    let _cap_admission = if state.auth.mode == crate::auth::AuthMode::Enforce {
        match state.auth.status_and_quotas(lookup_tenant) {
            None => {
                // Not in this cell's policy snapshot: not served here.
                return crate::audit::tag_project(refuse(), lookup_tenant);
            }
            Some((status, quotas)) => {
                if !matches!(status, crate::project_policy::ProjectStatus::Active) {
                    return crate::audit::tag_project(
                        crate::audit::tag(
                            perr(
                                StatusCode::FORBIDDEN,
                                "project_not_active",
                                "the project is not active",
                                None,
                                false,
                            ),
                            "project_not_active",
                        ),
                        lookup_tenant,
                    );
                }
                match state
                    .quotas
                    .admit(lookup_tenant, &quotas, crate::shard::now_ms())
                {
                    Ok(g) => Some(g),
                    Err(r) => {
                        return crate::audit::tag_project(
                            quota_refusal_response(&r),
                            lookup_tenant,
                        );
                    }
                }
            }
        }
    } else {
        None
    };
    if crate::http::initializing(&desc) {
        return perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            None,
            true,
        );
    }
    if !desc.watch_definitions.iter().any(|d| d.name == w) {
        return perr(
            StatusCode::NOT_FOUND,
            "unknown_watch",
            "no such watch definition",
            None,
            false,
        );
    }
    // Cache the key for sig derivation on later keyless waits.
    if let Some(kb) = product_key(&headers)
        && let crate::http::KeyCheck::Ok(k, e) = crate::http::check_key(Some(&kb), &desc)
    {
        state.keys.put(desc.storage_hash(), k, e);
    }
    let journal = state.touch.journal(
        desc.storage_hash(),
        crate::crypto::RouteHash::for_stream(&desc.sref()),
        &watch_pinned(&desc),
    );
    let cursor = q
        .get("cursor")
        .map(String::as_str)
        .unwrap_or("now")
        .to_string();
    let timeout = match q_num::<u64>(&q, "timeoutMs", "invalid_timeout_ms") {
        Ok(v) => v
            .map(std::time::Duration::from_millis)
            .unwrap_or(std::time::Duration::from_secs(25))
            .min(std::time::Duration::from_secs(25)),
        Err(r) => return r,
    };
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
                    .map(|(k, _)| stream_cursor(end_offset).encode(&desc.project_id, &k)),
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
                    .map(|(k, _)| stream_cursor(end_offset).encode(&desc.project_id, &k)),
            }),
        ),
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        // §15: observation responses must never leak the capability
        // through a Referer header.
        .header("referrer-policy", "no-referrer")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// GET /v1/streams — the paginated product catalog (spec Stage 8 §10).
/// One object-store LIST over the registry prefix; no per-stream GET
/// fan-out beyond the descriptors the page returns.
pub async fn product_list(state: Arc<AppState>, query: String, headers: HeaderMap) -> Response {
    // Its own route entry (not through product_entry): gate it here.
    // In enforce mode the principal is retained so the listing can be
    // filtered to the credential's §6.2 prefix grant — catalog.read
    // authorizes listing, the grant bounds WHICH names are returned.
    let principal = if state.auth.mode == crate::auth::AuthMode::Enforce {
        match enforce_customer(&state, &headers) {
            Ok(p) => {
                if let Err(e) = p.require(crate::tenant::Scope::CatalogRead) {
                    return crate::audit::tag_project(auth_failure_response(&e), &p.project_id);
                }
                Some(p)
            }
            Err(r) => return r,
        }
    } else {
        if !crate::http::authorized(&state, &headers) {
            return perr(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "bearer token required",
                None,
                false,
            );
        }
        None
    };
    // SR-3: the catalog is a customer route like any other — it holds
    // the project's admission slot for the handler's duration instead
    // of offering a quota-free side door.
    let _admission = match project_admission(&state, principal.as_ref()) {
        Ok(g) => g,
        Err(r) => return r,
    };
    let q = match strict_query(&query, &["limit", "cursor", "prefix"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let limit = match q_num::<usize>(&q, "limit", "invalid_limit") {
        Ok(v) => v.unwrap_or(100).clamp(1, 1000),
        Err(r) => return r,
    };
    let list_project = principal
        .as_ref()
        .map(|p| p.project_id.clone())
        .unwrap_or_else(|| state.tenant.clone());
    // Review item 3: the cursor is versioned, PROJECT-BOUND and (when
    // the deployment key is set) signed — a cursor from another
    // project, another key posture, or the retired bare-base64 form is
    // invalid_cursor, never a silent reposition.
    let after: Option<String> = match q.get("cursor").filter(|c| !c.is_empty()) {
        None => None,
        Some(c) => match crate::product_cursor::CatalogCursor::decode(
            c,
            &list_project,
            state.catalog_cursor_key.as_ref(),
        ) {
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
        },
    };
    let page = match state
        .registry
        .list_page(&list_project, after.as_deref(), limit)
        .await
    {
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
        // A prefix-restricted credential must not learn the names of
        // streams outside its grant. The opaque cursor still walks the
        // full underlying ordering (it is page.next_after, not derived
        // from this filtered view), so pagination stays correct.
        .filter(|d| {
            principal
                .as_ref()
                .map(|p| p.grant.permits(&d.name))
                .unwrap_or(true)
        })
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
    if !page.exhausted
        && let Some(n) = page.next_after
    {
        body["cursor"] = json!(
            crate::product_cursor::CatalogCursor {
                project: list_project.clone(),
                last_name: n,
            }
            .encode(state.catalog_cursor_key.as_ref())
        );
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body.to_string()))
        .unwrap()
}

// ---- customer usage API (docs/OBSERVABILITY-BILLING.md §10) ----------

/// GET /v1/streams/{name}/usage[?month=YYYY-MM] and .../usage/current.
/// Control-plane metadata: bearer-authorized, NO record key required,
/// answered from the rollup with a point read (never a ledger scan).
async fn product_usage(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    query: &str,
) -> Response {
    // R25-E: validate the query BEFORE availability checks — a
    // malformed request is the CLIENT's error whatever this instance's
    // billing posture, and a 503 for a typo'd parameter teaches callers
    // to retry requests that can never succeed.
    let q = match strict_query(query, &["month", "streamId"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let Some(rollup) = state.rollup.get() else {
        return perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "usage_unavailable",
            "the usage rollup is not running on this instance",
            None,
            true,
        );
    };
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
        Ok(Some(d)) => d,
        Ok(None) => {
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
    let now = crate::shard::now_ms();
    let (cy, cm) = crate::billing::utc_year_month(now);
    let current = crate::billing::month_str(cy, cm);
    let month = q.get("month").cloned().unwrap_or_else(|| current.clone());
    if crate::billing::parse_month(&month).is_none() {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_month",
            "month must be YYYY-MM",
            None,
            false,
        );
    }
    let mut id = crate::billing::identity_of_query(&state, &desc);
    // Historical incarnation lookup (round-21 dashboard gap): after a
    // delete/recreate, ?streamId= addresses a PRIOR incarnation's rows
    // directly — invoice history survives the live resource.
    if let Some(sid) = q.get("streamId").map(String::as_str) {
        id.stream_id = sid.to_string();
    }
    let row: crate::rollup::MonthRow = rollup
        .month_row(&month, &id.account_id, &id.project_id, &id.stream_id)
        .await
        .unwrap_or_default();
    let is_current = month == current;
    // Round-21 blocker 2: a retained-but-idle stream has no month row
    // yet for the CURRENT month — the durable segment index still knows
    // its gauge, so provisional storage never reads as zero.
    let (fallback_byte_ms, fallback_owned) = if is_current && row.segments.is_empty() {
        let states = rollup
            .stream_segment_states(&id.account_id, &id.project_id, &id.stream_id)
            .await;
        let mstart = {
            let (y, m) = crate::billing::parse_month(&month).unwrap();
            crate::billing::month_start_ms(y, m)
        };
        let bms: u128 = states
            .iter()
            .map(|s| {
                let from = s.storage_accounted_through_ms.max(mstart);
                (now - from).max(0) as u128 * s.owned_frame_bytes_current as u128
            })
            .sum();
        let owned: u64 = states.iter().map(|s| s.owned_frame_bytes_current).sum();
        (bms, owned)
    } else {
        (0, 0)
    };
    let byte_ms = if is_current {
        row.storage_byte_ms_provisional(&month, now)
            .max(fallback_byte_ms)
    } else {
        row.storage_byte_ms()
    };
    let month_ms = {
        let (y, m) = crate::billing::parse_month(&month).unwrap();
        let (ny, nm) = crate::billing::next_month(y, m);
        (crate::billing::month_start_ms(ny, nm) - crate::billing::month_start_ms(y, m)) as u128
    };
    let avg_bytes = byte_ms / month_ms.max(1);
    let gb_month = byte_ms as f64 / month_ms as f64 / 1e9;
    let name_agg = rollup
        .name_row(&month, &id.account_id, &id.project_id, &id.stream_name)
        .await;
    let status = if row.finalized_at_ms.is_some() {
        if row.corrections.is_empty() {
            "finalized"
        } else {
            "corrected"
        }
    } else {
        "provisional"
    };
    json_ok(json!({
        "projectId": id.project_id,
        "streamId": id.stream_id,
        "streamName": id.stream_name,
        "month": month,
        "status": status,
        "ingestPayloadBytes": row.ingest_bytes(),
        "ingestRecords": row.ingest_records(),
        "readPayloadBytes": row.read_payload_bytes,
        "readRecords": row.read_records,
        "readOperations": row.read_operations,
        "queueOperations": row.queue_operations,
        "appendRequests": row.append_requests,
        "storageByteSeconds": (byte_ms / 1000).to_string(),
        "averageStoredBytes": avg_bytes as u64,
        "gbMonth": gb_month,
        "ownedStoredBytesNow": row.owned_bytes_now().max(fallback_owned),
        "updatedAt": row.updated_ms,
        "finalizedAt": row.finalized_at_ms,
        "corrections": row.corrections.len(),
        // Round-22 item 8: base + materialized corrections = what the
        // invoice will actually say, plus the audit trail itself.
        "effective": row.effective(),
        "correctionTotals": row.corr,
        "correctionList": row.corrections.iter().map(|c| serde_json::json!({
            "id": c.correction_id,
            "version": c.correction_version,
            "sourceEventId": c.source_event_id,
            "reason": c.reason,
            "createdAt": c.created_at_ms,
            "ingestPayloadBytesDelta": c.ingest_payload_bytes_delta,
            "ingestRecordsDelta": c.ingest_records_delta,
            "readPayloadBytesDelta": c.read_payload_bytes_delta,
            "readRecordsDelta": c.read_records_delta,
            "readOperationsDelta": c.read_operations_delta,
            "queueOperationsDelta": c.queue_operations_delta,
            "appendRequestsDelta": c.append_requests_delta,
            "storageByteMsDelta": c.storage_byte_ms_delta,
        })).collect::<Vec<_>>(),
        "nameAggregate": name_agg.as_ref().map(|a| serde_json::json!({
            "ingestPayloadBytes": a.ingest_bytes,
            "readPayloadBytes": a.read_payload_bytes,
            "storageByteSeconds": (a.storage_byte_ms.parse::<u128>().unwrap_or(0) / 1000).to_string(),
        })),
        "incarnations": name_agg.map(|a| a.incarnations).unwrap_or_default(),
        "metering": {
            "readFlushIntervalSeconds": crate::billing::READ_FLUSH_INTERVAL_MS / 1000,
            "possibleReadLossWindowSeconds": crate::billing::READ_FLUSH_INTERVAL_MS / 1000,
        }
    }))
}

/// GET /v1/projects/{project}/usage[?month=YYYY-MM] (round-22 doc
/// item D3): the project-level rollup answer — aggregate totals,
/// correction sums, and effective values. Bearer-authenticated like
/// every product control-plane read. Under the one-project-per-cell
/// deployment contract the {project} segment must match this cell's
/// configured project.
pub async fn project_usage(
    state: Arc<AppState>,
    authority: &crate::tenant::ProjectId,
    project: String,
    query: &str,
) -> Response {
    let q = match strict_query(query, &["month"]) {
        Ok(q) => q,
        Err(r) => return r,
    };

    // Stage 5d: the path must name the AUTHORITATIVE project for this
    // request — the verified principal's in enforce, the deployment
    // tenant otherwise. Grammar-invalid and foreign ids get the same
    // not-found answer (no grammar oracle), and the check precedes the
    // availability probe: a wrong path is the client's error whatever
    // this instance's rollup posture.
    let names_authority = crate::tenant::ProjectId::new(&project)
        .map(|p| p == *authority)
        .unwrap_or(false);
    if !names_authority {
        // Journaled (§10.4): a verified principal probing FOREIGN
        // project usage is the single most review-relevant denial
        // class, deliberately shaped as 404 on the wire.
        return crate::audit::tag_project(
            crate::audit::tag(
                perr(
                    StatusCode::NOT_FOUND,
                    "unknown_project",
                    "the path does not name this request's project",
                    None,
                    false,
                ),
                "unknown_project",
            ),
            authority,
        );
    }
    let Some(rollup) = state.rollup.get() else {
        return perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "usage_unavailable",
            "the usage rollup is not running on this instance",
            None,
            true,
        );
    };
    let now = crate::shard::now_ms();
    let (cy, cm) = crate::billing::utc_year_month(now);
    let current = crate::billing::month_str(cy, cm);
    let month = q.get("month").cloned().unwrap_or_else(|| current.clone());
    if crate::billing::parse_month(&month).is_none() {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_month",
            "month must be YYYY-MM",
            None,
            false,
        );
    }
    // Stage 7: rows land under the workspace-at-event; query and
    // report under the SAME resolution the meter used.
    let account = if state.auth.mode != crate::auth::AuthMode::Off {
        state
            .auth
            .workspace_for(authority)
            .map(|w| w.as_str().to_string())
            .unwrap_or_else(|| state.account_id.clone())
    } else {
        state.account_id.clone()
    };
    let agg = rollup
        .project_row(&month, &account, &project)
        .await
        .unwrap_or_default();
    let byte_ms: u128 = agg.storage_byte_ms.parse().unwrap_or(0);
    json_ok(json!({
        "accountId": account,
        "projectId": project,
        "month": month,
        "ingestPayloadBytes": agg.ingest_bytes,
        "ingestRecords": agg.ingest_records,
        "readPayloadBytes": agg.read_payload_bytes,
        "readRecords": agg.read_records,
        "readOperations": agg.read_operations,
        "queueOperations": agg.queue_operations,
        "appendRequests": agg.append_requests,
        "storageByteSeconds": (byte_ms / 1000).to_string(),
        "correctionTotals": agg.corr,
        "effective": {
            "ingestPayloadBytes": crate::rollup::eff_u64(agg.ingest_bytes, agg.corr.ingest_payload_bytes_delta),
            "ingestRecords": crate::rollup::eff_u64(agg.ingest_records, agg.corr.ingest_records_delta),
            "readPayloadBytes": crate::rollup::eff_u64(agg.read_payload_bytes, agg.corr.read_payload_bytes_delta),
            "readRecords": crate::rollup::eff_u64(agg.read_records, agg.corr.read_records_delta),
            "readOperations": crate::rollup::eff_u64(agg.read_operations, agg.corr.read_operations_delta),
            "queueOperations": crate::rollup::eff_u64(agg.queue_operations, agg.corr.queue_operations_delta),
            "appendRequests": crate::rollup::eff_u64(agg.append_requests, agg.corr.append_requests_delta),
            "storageByteSeconds": (crate::rollup::eff_u128(byte_ms, &agg.corr.storage_byte_ms_delta) / 1000).to_string(),
        },
    }))
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
    fn validators_agree() {
        // Pins product::canonical_name to tenant::CanonicalStreamName:
        // every ACCEPT of canonical_name must be an ACCEPT of the
        // identity type (canonical_name may be STRICTER — it adds the
        // addressability rules — never looser). Divergence here means
        // an identity-layer bypass.
        let corpus = [
            "orders",
            "customers/acme/orders",
            "a",
            "a/b/c",
            "a/__ds",
            "records-ish",
            "a/recordsx",
            "deep/a/b/c/d/e",
            "",
            "a//b",
            "/a",
            "a/",
            "a/./b",
            "a/../b",
            ".",
            "..",
            "__ds",
            "__ds/x",
            "has\u{7}bell",
            "a/records",
            "a/consumers",
            "a/watches",
        ];
        for raw in corpus {
            let product_ok = canonical_name(raw).is_ok();
            let tenant_ok = crate::tenant::CanonicalStreamName::new(raw).is_ok();
            assert!(
                !product_ok || tenant_ok,
                "canonical_name accepted {raw:?} but CanonicalStreamName rejected it"
            );
        }
        let long = "x".repeat(513);
        assert!(canonical_name(&long).is_err());
        assert!(crate::tenant::CanonicalStreamName::new(&long).is_err());
        // The typed entry point returns the same acceptance set as
        // canonical_name itself.
        assert!(canonical_stream_name("customers/acme").is_ok());
        assert!(canonical_stream_name("a/records").is_err());
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

    // Round-19 ABA: a peer RPC that names only (stream, segment) binds
    // to whatever descriptor holds that name when it LANDS. These pin
    // the guard that makes a stale relay refuse instead.
    fn desc_with(name: &str, epoch_hex: &str) -> StreamDesc {
        StreamDesc {
            name: name.to_string(),
            account_id: None,
            project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
            stream_epoch: epoch_hex.to_string(),
            seal_gen_counter: 0,
            key_fingerprint: String::new(),
            created_ms: 0,
            expires_at_ms: None,
            deleted: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            content_type: "application/json".to_string(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    fn target_headers(d: &StreamDesc, seg: u32) -> HeaderMap {
        let t = InternalTarget::of(d, seg).expect("descriptor has an epoch");
        let mut h = HeaderMap::new();
        for (k, v) in t.headers() {
            h.insert(k, axum::http::HeaderValue::from_str(&v).unwrap());
        }
        h
    }

    #[test]
    fn internal_target_accepts_its_own_incarnation() {
        let d = desc_with("orders", &"11".repeat(16));
        let h = target_headers(&d, 0);
        let (seg, id) = verify_internal_target(&d, &h).expect("same incarnation must verify");
        assert_eq!(seg, 0);
        assert_eq!(id, d.dynamic_segment_identity(0));
    }

    #[test]
    fn internal_target_refuses_a_recreated_stream() {
        // The saga/read was issued against incarnation X...
        let x = desc_with("orders", &"11".repeat(16));
        let h = target_headers(&x, 0);
        // ...and the name now holds incarnation Y. The request must NOT
        // bind: a stale sweep would otherwise fence and delete Y's
        // generation-1 consumer state.
        let y = desc_with("orders", &"22".repeat(16));
        let err = verify_internal_target(&y, &h).expect_err("recreation must refuse");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_target_refuses_a_foreign_project() {
        // Same name, same epoch, DIFFERENT project: the §16 corruption
        // check must refuse the bind even when every other coordinate
        // matches — a silent project swap here is a cross-tenant bind.
        let d = desc_with("orders", &"55".repeat(16));
        let h = target_headers(&d, 0);
        let mut foreign = d.clone();
        foreign.project_id = crate::tenant::ProjectId::new("proj-other").unwrap();
        let err = verify_internal_target(&foreign, &h).expect_err("foreign project must refuse");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_target_refuses_an_unknown_segment() {
        let d = desc_with("orders", &"33".repeat(16));
        let mut h = target_headers(&d, 0);
        h.insert(
            "streams-internal-seg",
            axum::http::HeaderValue::from_static("7"),
        );
        let err = verify_internal_target(&d, &h).expect_err("unknown segment must refuse");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_target_refuses_a_mismatched_identity() {
        let d = desc_with("orders", &"44".repeat(16));
        let mut h = target_headers(&d, 0);
        h.insert(
            "streams-internal-identity",
            axum::http::HeaderValue::from_str(&crate::crypto::hex(&[9u8; 16])).unwrap(),
        );
        let err = verify_internal_target(&d, &h).expect_err("identity mismatch must refuse");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_target_requires_the_headers() {
        let d = desc_with("orders", &"55".repeat(16));
        let err = verify_internal_target(&d, &HeaderMap::new())
            .expect_err("an untargeted internal request must be rejected");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // Regression (two-instance rig): an ownership 409 translated to
    // cursor_beyond_tail told SDKs to rewind healthy cursors, and
    // dropping Streams-Replay-To hid the only signal routers use to
    // converge — cross-owner lineage reads died as fake tail overruns
    // and every post-split append to a foreign child failed opaquely.
    #[test]
    fn ownership_bounce_survives_read_translation() {
        let mut raw = crate::http::err_resp(
            StatusCode::CONFLICT,
            "not_ring_owner",
            "shard 000 belongs to streams-2",
        );
        raw.headers_mut().insert(
            "streams-replay-to",
            axum::http::HeaderValue::from_static("streams-2"),
        );
        let out = translate_read_error(raw);
        assert_eq!(out.status(), StatusCode::CONFLICT);
        assert_eq!(
            out.headers()
                .get("streams-replay-to")
                .and_then(|v| v.to_str().ok()),
            Some("streams-2")
        );
    }

    #[test]
    fn plain_409_still_reads_as_beyond_tail() {
        // The deliver=applied rewind contract is untouched: a 409
        // WITHOUT a replay target keeps its cursor_beyond_tail meaning.
        let raw = crate::http::err_resp(StatusCode::CONFLICT, "conflict", "beyond tail");
        let out = translate_read_error(raw);
        assert_eq!(out.status(), StatusCode::CONFLICT);
        assert!(out.headers().get("streams-replay-to").is_none());
    }
}
