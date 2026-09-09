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
use crate::registry::{StreamDesc, WatchDefinition};

/// Reserved protocol control namespace (appendix §2.6): never a
/// customer stream name, on either surface.
pub use crate::tenant::RESERVED_ROOT;

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

/// A PRODUCT-ADDRESSABLE stream name (WP-03/PR 5): the structural
/// identity ([`crate::tenant::CanonicalStreamName`] — validated there,
/// nowhere else) plus the product route's extra addressability rules
/// (reserved subresource final segments; names that already read as a
/// subresource path). The previous shape duplicated the structural
/// checks here and pinned agreement with a debug assertion; now the
/// identity layer is THE validator and this type adds only what is
/// product-specific.
pub use crate::application::names::ProductStreamName;

/// Canonical stream-name validation (spec Stage 8 §4.1). The wildcard
/// path arrives percent-decoded exactly once by the router; this
/// validates the DECODED form. Thin wire adapter over
/// [`ProductStreamName`] — the String return is the legacy shape; call
/// sites migrate to the typed value as WP-03 proceeds.
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
pub fn canonical_name(raw: &str) -> Result<String, Response> {
    match ProductStreamName::try_from(raw) {
        Ok(p) => Ok(p.as_str().to_string()),
        Err(e) => Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_name",
            e.message(),
            None,
            false,
        )),
    }
}

/// Typed variant of [`canonical_name`] for tenant-qualified call sites
/// (Stage 4): same rules, same error responses, returns the checked
/// identity type. No reconstruction, no expect (WP-03/PR 5): the value
/// IS the one validation produced.
#[allow(dead_code)] // consumed from MT Stage 4 surface conversion on
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
pub fn canonical_stream_name(raw: &str) -> Result<crate::tenant::CanonicalStreamName, Response> {
    match ProductStreamName::try_from(raw) {
        Ok(p) => Ok(p.into_canonical()),
        Err(e) => Err(perr(
            StatusCode::BAD_REQUEST,
            "invalid_name",
            e.message(),
            None,
            false,
        )),
    }
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

use crate::application::creation::ProductCreateConfig as ParsedCreate;

#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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

/// Round-13: the per-project memory-pressure backstop for a WRITE by
/// this principal. Some(response) = the typed, project-audited,
/// retryable refusal; None = admitted. Order (review): after the
/// ordinary project quotas, before the global RSS emergency gate the
/// shared append path applies.
pub(crate) fn project_memory_gate(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Option<Response> {
    let high = state.admission.project_memory_pressure_bytes();
    if high == 0 {
        return None;
    }
    let p = principal?;
    let adm = state.quotas.pressure_handle(&p.project_id)?;
    if adm.memory_gate(
        &p.project_id,
        high,
        state.admission.project_memory_release_pct(),
    ) {
        return Some(crate::audit::tag_project(
            quota_refusal_response(&crate::quota::QuotaRefusal::MemoryPressure),
            &p.project_id,
        ));
    }
    None
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
        crate::quota::QuotaRefusal::StreamLimit => perr(
            StatusCode::TOO_MANY_REQUESTS,
            "stream_limit",
            "the project is at its max_streams quota",
            None,
            false,
        ),
        crate::quota::QuotaRefusal::QueuedBytes => perr(
            StatusCode::TOO_MANY_REQUESTS,
            "queued_bytes",
            "the project's queued append bytes are at the ceiling; retry",
            None,
            true,
        ),
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
        crate::quota::QuotaRefusal::MemoryPressure => {
            let mut r = perr(
                StatusCode::TOO_MANY_REQUESTS,
                "project_memory_pressure",
                "the project's estimated memory pressure is over its per-project backstop; retry",
                None,
                true,
            );
            r.headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("1"));
            r
        }
    };
    crate::audit::tag(
        resp,
        match refusal {
            crate::quota::QuotaRefusal::Rate { .. } => "project_rate_limit",
            crate::quota::QuotaRefusal::Concurrency => "project_concurrency_limit",
            crate::quota::QuotaRefusal::TrackerCapacity => "project_tracker_capacity",
            crate::quota::QuotaRefusal::StreamLimit => "project_stream_limit",
            crate::quota::QuotaRefusal::QueuedBytes => "project_queued_bytes",
            crate::quota::QuotaRefusal::MemoryPressure => "project_memory_pressure",
        },
    )
}

/// Authentication state at the transport boundary. A capability carrier is
/// deliberately unverified: it cannot supply tenant admission or attribution.
pub(crate) enum ProductAuthorization {
    Preflight,
    CapabilityCarrier,
    Principal(Box<crate::auth::RequestPrincipal>),
    Deployment,
}

impl ProductAuthorization {
    pub(crate) fn principal(&self) -> Option<&crate::auth::RequestPrincipal> {
        match self {
            Self::Principal(principal) => Some(principal),
            _ => None,
        }
    }
}

#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
pub(crate) fn product_auth_gate(
    state: &AppState,
    path: &str,
    method: &Method,
    query: &str,
    headers: &HeaderMap,
) -> Result<ProductAuthorization, Response> {
    if method == Method::OPTIONS {
        return Ok(ProductAuthorization::Preflight);
    }
    if watch_capability_carrier(path, method, query, headers) {
        return Ok(ProductAuthorization::CapabilityCarrier);
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
        return Ok(ProductAuthorization::Principal(Box::new(principal)));
    }
    if crate::http::authorized(state, headers) {
        return Ok(ProductAuthorization::Deployment);
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
    authorization: ProductAuthorization,
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
    // Entry owns verification and admission; preserve its explicit authority.
    let principal = authorization.principal().cloned();
    // Stage 5d: the VERIFIED principal selects the tenant-qualified
    // storage identity. Off/shadow requests (and §15 capability
    // carriers, until the capability wire carries the project —
    // review item 3) address the deployment tenant, which is the
    // single-tenant posture those modes run in.
    let tenant: crate::tenant::ProjectId = principal
        .as_ref()
        .map(|p| p.project_id.clone())
        // mt-lint: allow(state-tenant-read): Off/Shadow single-tenant posture (Stage 5d) — enforce-mode requests always carry a principal
        .unwrap_or_else(|| state.deployment.deployment_tenant().clone());
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
                    let resp = product_read(
                        state.clone(),
                        &tenant,
                        name,
                        headers,
                        &query,
                        None,
                        principal.as_ref().map(|pr| pr.lease()),
                    )
                    .await;
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
                        principal.as_ref().map(|pr| pr.lease()),
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
                    let resp = product_read(
                        state,
                        &tenant,
                        name,
                        headers,
                        &query,
                        Some("sse"),
                        principal.as_ref().map(|pr| pr.lease()),
                    )
                    .await;
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
            let access = match &authorization {
                ProductAuthorization::Principal(p) => {
                    crate::application::consumer::ConsumerAccess::Account(p)
                }
                ProductAuthorization::Deployment => {
                    crate::application::consumer::ConsumerAccess::Deployment
                }
                ProductAuthorization::CapabilityCarrier | ProductAuthorization::Preflight => {
                    return perr(
                        StatusCode::FORBIDDEN,
                        "unauthorized",
                        "consumer authorization required",
                        None,
                        false,
                    );
                }
            };
            return match (method.clone(), verb.as_deref()) {
                (Method::PUT, None) => {
                    product_consumer_put(state, &tenant, name, cname, headers, body, access).await
                }
                (Method::GET, None) => {
                    product_consumer_get(state, &tenant, name, cname, headers, access).await
                }
                (Method::DELETE, None) => {
                    product_consumer_delete(state, &tenant, name, cname, headers, access).await
                }
                (Method::POST, Some("pull")) => {
                    product_consumer_pull(state, &tenant, name, cname, headers, body, access).await
                }
                (Method::POST, Some("settle")) => {
                    let r = product_consumer_settle(
                        state.clone(),
                        &tenant,
                        name.clone(),
                        cname,
                        headers,
                        body,
                        access,
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
                product_watch_wait(
                    state,
                    &tenant,
                    name,
                    watch,
                    key,
                    headers,
                    &query,
                    match &authorization {
                        ProductAuthorization::Principal(p) => {
                            crate::application::watch::WatchAccess::AdmittedAccount(p)
                        }
                        ProductAuthorization::CapabilityCarrier => {
                            crate::application::watch::WatchAccess::CapabilityCarrier
                        }
                        ProductAuthorization::Deployment => {
                            crate::application::watch::WatchAccess::Deployment
                        }
                        ProductAuthorization::Preflight => {
                            unreachable!("preflight returned before dispatch")
                        }
                    },
                )
                .await
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
use crate::application::names::split_subresource;

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
    let result = state
        .creation_service()
        .create_product(
            tenant.stream_ref(&name),
            key,
            cfg,
            principal.map(|p| &p.quotas),
        )
        .await;
    match result {
        Ok((created, desc)) => metadata_response(
            &desc,
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
        ),
        Err(crate::application::creation::ProductCreateError::Quota(refusal)) => {
            let response = quota_refusal_response(&refusal);
            if let Some(p) = principal {
                crate::audit::tag_project(response, &p.project_id)
            } else {
                response
            }
        }
        Err(crate::application::creation::ProductCreateError::Creation(error)) => {
            use crate::application::creation::CreationFailure as F;
            let status = match error.kind {
                F::Invalid => StatusCode::BAD_REQUEST,
                F::Conflict => StatusCode::CONFLICT,
                F::Missing => StatusCode::NOT_FOUND,
                F::Gone => StatusCode::GONE,
                F::WrongKey => StatusCode::FORBIDDEN,
                F::Storage => StatusCode::INTERNAL_SERVER_ERROR,
                F::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
                F::Overloaded => StatusCode::TOO_MANY_REQUESTS,
                F::Ambiguous => StatusCode::REQUEST_TIMEOUT,
                F::Opening => StatusCode::SERVICE_UNAVAILABLE,
            };
            let retryable = matches!(
                error.kind,
                F::Storage | F::Overloaded | F::Ambiguous | F::Opening
            );
            let mut response = perr(status, error.code, &error.message, None, retryable);
            if error.kind == F::WrongKey {
                response = crate::audit::tag(response, "wrong_key");
            }
            if let Some(owner) = error.owner.and_then(|v| v.parse().ok()) {
                response.headers_mut().insert("streams-replay-to", owner);
            }
            if let Some(p) = principal {
                response = crate::audit::tag_project(response, &p.project_id);
            }
            response
        }
    }
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
            if let Some(kind) = state
                .runtime
                .usage
                .permanently_unadmittable(fin.to_string().len() as u64 + 2, 1)
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
            let sref = tenant.stream_ref(&name);
            let lifecycle = state.lifecycle_service();
            let routing_key = doc.routing_key.clone().unwrap_or_default();
            let mut final_headers = headers.clone();
            final_headers.insert(
                "prisma-routing-key",
                axum::http::HeaderValue::from_str(&routing_key).expect("validated routing key"),
            );
            let result = crate::application::lifecycle::seal_final(
                &lifecycle,
                crate::application::lifecycle::FinalSealRequest {
                    stream: &sref,
                    epoch: &validated_epoch,
                    operation: &op_id,
                    routing_key: &routing_key,
                },
                |auth| async {
                    #[cfg(test)]
                    crate::failpoints::pause_product_final_before_append(&name).await;
                    product_append_sealing(
                        state.clone(),
                        &sref,
                        &validated,
                        &final_headers,
                        Bytes::from(fin.to_string()),
                        auth,
                    )
                    .await
                    .map(|ack| crate::application::lifecycle::FinalRecordAck { closed: ack.closed })
                    .map_err(|error| {
                        let disposition = if error.definitively_rejected() {
                            crate::application::lifecycle::FinalDisposition::DefinitivelyRejected
                        } else {
                            crate::application::lifecycle::FinalDisposition::AmbiguousOrTransient
                        };
                        crate::application::lifecycle::FinalRecordFailure { error, disposition }
                    })
                },
            )
            .await;
            return match result {
                Ok(()) => json_ok(json!({"sealed":true})),
                Err(crate::application::lifecycle::SealFinalError::Append(error)) => {
                    render_product_append_error(error)
                }
                Err(crate::application::lifecycle::SealFinalError::Lifecycle(error)) => {
                    seal_error_response(&name, &error)
                }
                Err(crate::application::lifecycle::SealFinalError::SequenceReused) => perr(
                    StatusCode::CONFLICT,
                    "producer_sequence_reused",
                    "this producer sequence already committed a record that did not seal the collection; use a fresh sequence for the final record",
                    None,
                    false,
                ),
            };
        }
    }
    product_seal_only(state, tenant, name, headers, validated_epoch).await
}

/// Distinguishes an ABSENT field from one present as `null`.
fn deserialize_some<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(d).map(Some)
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
    crate::failpoints::pause_product_seal_before_claim(&name).await;
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
        Err(m) => seal_error_response(&name, &m),
    }
}

/// Follow-up review finding 3: a RESUMABLE seal failure is not an
/// invariant failure — answering it 500 told clients the service broke
/// when the honest answer is "retry later" (and produced worse client
/// behavior than the machine's own contract warrants). Classification:
///   * another live final-bearing claim -> 409 sealing (unchanged);
///   * resumable states of this transition (close refused/pending,
///     topology busy, publication declined, resolution failed)
///     -> 503 seal_incomplete, retryable;
///   * everything else (invariant/corruption/store) -> 500 internal.
pub(crate) fn seal_error_response(
    stream: &str,
    error: &crate::application::lifecycle::SealError,
) -> Response {
    use crate::application::lifecycle::SealError;
    let (status, code) = match error {
        SealError::Conflict(_) => (StatusCode::CONFLICT, "sealing"),
        SealError::Resumable(_) => (StatusCode::SERVICE_UNAVAILABLE, "seal_incomplete"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    tracing::error!(stream = %stream, status = %status, code, "seal failed: {error}");
    perr(status, code, &error.to_string(), None, true)
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
    desc: &StreamDesc,
    headers: &HeaderMap,
    body: Bytes,
    auth: crate::application::lifecycle::SealAuthz,
) -> crate::application::append::AppendResult {
    use crate::application::append::{AppendCode, AppendFailure, FailureClass};
    let key_b64 = product_key(headers).ok_or_else(|| {
        AppendFailure::new(
            FailureClass::Invalid,
            AppendCode::MissingKey,
            "Prisma-Encryption-Key required",
        )
    })?;
    let routing_key = headers
        .get("prisma-routing-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let wire_body = if desc.is_json() {
        let mut bytes = Vec::with_capacity(body.len() + 2);
        bytes.push(b'[');
        bytes.extend_from_slice(&body);
        bytes.push(b']');
        Bytes::from(bytes)
    } else {
        body.clone()
    };
    submit_product_append(
        state,
        sref,
        desc,
        &key_b64,
        routing_key,
        headers,
        &body,
        wire_body,
        false,
        Some(auth),
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
    seal_auth: Option<crate::application::lifecycle::SealAuthz>,
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
    // SR2-4 queued_append_bytes: the body is charged to the project
    // BEFORE the committer sees it and released when this handler's
    // awaited append DECIDES (the guard drops on return, every path).
    let _queued_charge = if let Some(p) = principal {
        match state
            .quotas
            .charge_queued(&p.project_id, &p.quotas, body.len() as u64)
        {
            Ok(g) => g,
            Err(r) => {
                return crate::audit::tag_project(quota_refusal_response(&r), &p.project_id);
            }
        }
    } else {
        None
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
    // Round-13 admission order step 7: the project memory latch,
    // after the ordinary quotas above, before the shared append
    // path's global RSS gate.
    if let Some(r) = project_memory_gate(&state, principal) {
        return r;
    }

    let key = match crate::crypto::StreamKey::from_b64(&key_b64) {
        Ok(key) => key,
        Err(_) => {
            return render_product_append_error(crate::application::append::AppendFailure::new(
                crate::application::append::FailureClass::Denied,
                crate::application::append::AppendCode::WrongKey,
                "key mismatch",
            ));
        }
    };
    let result = submit_product_append(
        state.clone(),
        &tenant.stream_ref(&name),
        &desc,
        &key_b64,
        &routing_key,
        &headers,
        &body,
        wire_body,
        batch,
        seal_auth,
    )
    .await;
    render_product_append(&desc, &key, &routing_key, count, result)
}

#[allow(clippy::too_many_arguments)] // Parsed protocol fields converge into one typed application command.
async fn submit_product_append(
    state: Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    desc: &StreamDesc,
    key_b64: &str,
    routing_key: &str,
    headers: &HeaderMap,
    body: &Bytes,
    wire_body: Bytes,
    batch: bool,
    seal_auth: Option<crate::application::lifecycle::SealAuthz>,
) -> crate::application::append::AppendResult {
    let seal_after = seal_auth.is_some();
    let has_producer = headers.contains_key("producer-id");
    // Stage 5 §7: the product request hash covers (operation kind,
    // routing key, content type, body bytes, seal flag) — computed over
    // the PRODUCT body, before any wire re-shaping.
    let request_hash = crate::application::append::product_request_hash(
        batch,
        routing_key,
        &desc.content_type,
        body,
        seal_after,
    );
    use crate::application::append::{AppendCode, AppendCommand, AppendFailure, FailureClass};
    let key = crate::crypto::StreamKey::from_b64(key_b64).map_err(|_| {
        AppendFailure::new(FailureClass::Denied, AppendCode::WrongKey, "key mismatch")
    })?;
    let service = state.append_service();
    let prepared = service
        .prepare(
            sref,
            crate::application::append::AppendKey::Provided(key.clone()),
        )
        .await?;
    let producer = crate::application::append::parse_producer(
        headers
            .get("producer-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        headers
            .get("producer-epoch")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        headers
            .get("producer-seq")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
    )
    .map_err(|message| {
        AppendFailure::new(FailureClass::Invalid, AppendCode::InvalidProducer, message)
    })?;
    service.check_memory().await?;
    service
        .execute_prepared(
            prepared,
            AppendCommand {
                sref: sref.clone(),
                expected_epoch: Some(desc.epoch()),
                key,
                body: wire_body,
                producer,
                content_type: Some(desc.content_type.clone()),
                routing_key: routing_key.to_string(),
                close: seal_after,
                seal_auth,
                request_hash: has_producer.then_some(request_hash),
                sequence: None,
                ts_hint_ms: None,
                key_version: 0,
                close_identity: None,
                body_charge: None,
            },
        )
        .await
}

/// Map the shared path's protocol response into the product contract:
/// {cursor, count, duplicate, sealed} on success, the stable product
/// error schema otherwise.
#[allow(clippy::too_many_arguments)]
fn render_product_append(
    desc: &StreamDesc,
    key: &crate::crypto::StreamKey,
    routing_key: &str,
    count: usize,
    result: crate::application::append::AppendResult,
) -> Response {
    let out = match result {
        Ok(out) => out,
        Err(error) => return render_product_append_error(error),
    };
    let next = if out.duplicate {
        out.last_offset.saturating_add(1).min(out.next_offset)
    } else {
        out.next_offset
    };
    let cursor = crate::product_cursor::KeyCursor {
        epoch: desc.epoch(),
        key_hash: crate::crypto::stream_hash(routing_key),
        seg_id: out.seg_id,
        offset: next,
    }
    .encode(&desc.project_id, key);
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE,"application/json").header(header::CACHE_CONTROL,"no-store")
        .body(Body::from(json!({"cursor":cursor,"count":if out.duplicate {0}else{count},"duplicate":out.duplicate,"sealed":out.closed}).to_string())).unwrap()
}

fn render_product_append_error(error: crate::application::append::AppendFailure) -> Response {
    use crate::application::append::{AppendCode as C, FailureClass as F};
    let status = crate::http::append_failure_status(&error);
    let (code, message, details, retryable) = match error.code {
        C::NotOwner => (
            "not_stream_owner",
            "another instance owns the target segment; retry through the router",
            None,
            true,
        ),
        C::ProducerGap => (
            "producer_gap",
            "producer sequence gap",
            Some(json!({"expected":error.expected(),"received":error.received()})),
            false,
        ),
        C::ProducerStale => (
            "stale_producer_epoch",
            "producer epoch is stale",
            Some(json!({"currentEpoch":error.producer_epoch()})),
            false,
        ),
        C::ProducerSequenceReused => (
            "producer_sequence_reused",
            "same producer sequence with a different request",
            None,
            false,
        ),
        C::ProducerEpochSeq => (
            "producer_epoch_must_start_at_zero",
            "a new producer epoch must start at sequence 0",
            None,
            false,
        ),
        C::StreamClosed => ("sealed", "collection is sealed", None, false),
        C::MaintenanceBackpressure => (
            "maintenance_backpressure",
            "maintenance backlog exceeds its bound; retry after it drains",
            None,
            true,
        ),
        C::ContentTypeMismatch => (
            "content_type_mismatch",
            "content type mismatch",
            None,
            false,
        ),
        _ => match error.class {
            F::Missing => ("not_found", "stream not found", None, false),
            F::Denied => ("stale_or_wrong_credentials", "forbidden", None, false),
            F::Conflict => (
                "conflict",
                "producer or configuration conflict",
                None,
                false,
            ),
            F::Invalid if status == StatusCode::PAYLOAD_TOO_LARGE => (
                "body_too_large",
                "request body exceeds the limit",
                None,
                false,
            ),
            F::Capacity => ("rate_limited", "admission or rate limit", None, true),
            F::Unavailable => ("temporarily_unavailable", "retry shortly", None, true),
            _ => ("append_failed", "append failed", None, false),
        },
    };
    let mut r = perr(status, code, message, details, retryable);
    if let Some(retry) = error.retry_after {
        r.headers_mut().insert(
            "retry-after",
            axum::http::HeaderValue::from_str(&retry.to_string()).unwrap(),
        );
    }
    if let Some(owner) = error.owner
        && let Ok(owner) = axum::http::HeaderValue::from_str(&owner)
    {
        r.headers_mut().insert("streams-replay-to", owner);
    }
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (k.to_string(), pct(v))
        })
        .collect()
}

async fn product_read(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    query: &str,
    live: Option<&'static str>,
    lease: Option<crate::auth::AuthLease>,
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

    let start = match q.get("cursor").map(String::as_str) {
        None | Some("") | Some("beginning") => crate::application::read::ReadStart::Beginning,
        Some("now") => crate::application::read::ReadStart::Now,
        Some(cursor) => match crate::product_cursor::KeyCursor::decode(
            cursor,
            &desc.project_id,
            &skey,
            &epoch,
            &kh,
        ) {
            Ok(cursor) => crate::application::read::ReadStart::Position(
                crate::application::read::ReadPosition {
                    segment: cursor.seg_id,
                    after: cursor.offset,
                },
            ),
            Err(_) => {
                return render_product_read_failure(
                    crate::application::read::ReadFailure::InvalidCursor,
                );
            }
        },
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

    use crate::application::read::{ReadCommand, ReadMode};
    let command = ReadCommand {
        descriptor: desc,
        key: Some(skey.clone()),
        start,
        selector: Some(rk.clone()),
        mode: if live == Some("long-poll") {
            ReadMode::LongPoll(
                q.get("waitMs")
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(std::time::Duration::from_millis)
                    .unwrap_or(std::time::Duration::from_secs(3)),
            )
        } else {
            ReadMode::Replay
        },
        visibility: deliver,
        max_bytes: max_bytes.unwrap_or(READ_MAX_BYTES_CAP),
        tail_max_bytes: state.config.http.tail_max_bytes,
        allow_remote: true,
        refresh: true,
    };
    if let Err(error) = state
        .creation_service()
        .renew_ttl(&command.descriptor)
        .await
    {
        let mut response = perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "ttl_renewal_unavailable",
            &error.to_string(),
            None,
            true,
        );
        response
            .headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from_static("1"));
        return response;
    }
    if live == Some("sse") {
        let params = crate::http::ReadParams {
            offset: None,
            format: None,
            live: Some("sse".into()),
            timeout,
            key: Some(rk),
            cursor: None,
            sig: None,
            max_bytes,
            deliver,
            no_fanout: false,
            internal: false,
            lease,
            internal_lease: None,
        };
        return crate::http::serve_read_sse(
            state,
            command,
            params,
            crate::http::SseSurface::Product,
        )
        .await;
    }
    match state.read_service().execute_read(command).await {
        Ok(outcome) => render_product_read(&state, &skey, &rk, outcome),
        Err(error) => render_product_read_failure(error),
    }
}

fn render_product_read(
    state: &AppState,
    key: &crate::crypto::StreamKey,
    routing_key: &str,
    out: crate::application::read::ReadOutcome,
) -> Response {
    use crate::application::read::ReadResultKind;
    let cursor = |position: crate::application::read::ReadPosition| {
        crate::product_cursor::KeyCursor {
            epoch: out.descriptor.epoch(),
            key_hash: crate::crypto::stream_hash(routing_key),
            seg_id: position.segment,
            offset: position.after,
        }
        .encode(&out.descriptor.project_id, key)
    };
    let mut response = Response::builder()
        .status(if out.kind == ReadResultKind::Timeout {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, &out.descriptor.content_type)
        .header(header::CACHE_CONTROL, "no-store")
        .header("Prisma-Next-Cursor", cursor(out.next));
    if let Some(durable) = out.durable {
        response = response.header("Prisma-Durable-Cursor", cursor(durable));
    }
    if let Some(index) = out.pending_from {
        response = response.header("Prisma-Pending-From", index.to_string());
    }
    if out.up_to_date {
        response = response.header("Prisma-Up-To-Date", "true");
    }
    if out.closed {
        response = response.header("Prisma-Sealed", "true");
    }
    let payload = if out.kind == ReadResultKind::Timeout {
        Bytes::new()
    } else {
        crate::http::read_payload(&out, false, Some(key), Some(routing_key), false)
    };
    crate::http::meter_read_outcome(state, &out);
    response.body(Body::from(payload)).unwrap()
}

pub(crate) fn render_product_read_failure(
    error: crate::application::read::ReadFailure,
) -> Response {
    use crate::application::read::ReadFailure as E;
    let (status, code, message, retry, owner) = match error {
        E::Missing | E::Gone => (
            StatusCode::NOT_FOUND,
            "not_found",
            "stream not found",
            false,
            None,
        ),
        E::Creating => (
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            true,
            None,
        ),
        E::MissingKey => (
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            false,
            None,
        ),
        E::WrongKey => (
            StatusCode::FORBIDDEN,
            "wrong_key",
            "encryption key mismatch",
            false,
            None,
        ),
        E::InvalidCursor => (
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "cursor is outside this stream's readable lineage",
            false,
            None,
        ),
        E::ChangedIncarnation => (
            StatusCode::CONFLICT,
            "target_mismatch",
            "stream incarnation changed",
            false,
            None,
        ),
        E::CursorBeyondTail => (
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
            false,
            None,
        ),
        E::KeylessLive => (
            StatusCode::BAD_REQUEST,
            "keyless_live",
            "live reads require a routing key",
            false,
            None,
        ),
        E::AppliedFork => (
            StatusCode::BAD_REQUEST,
            "deliver_unsupported_fork",
            "deliver=applied is not supported on forked streams",
            false,
            None,
        ),
        E::Resolve(crate::shard_directory::ResolveError::NotOwner { owner, .. }) => (
            StatusCode::CONFLICT,
            "not_ring_owner",
            "stream is owned by another instance; retry",
            true,
            Some(owner),
        ),
        E::Resolve(_) | E::Remote(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "read service unavailable; retry",
            true,
            None,
        ),
        E::Storage(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "read failed",
            true,
            None,
        ),
    };
    let mut response = perr(status, code, message, None, retry);
    if let Some(owner) = owner
        && let Ok(owner) = axum::http::HeaderValue::from_str(&owner)
    {
        response.headers_mut().insert("streams-replay-to", owner);
    }
    if code == "wrong_key" {
        response = crate::audit::tag(response, "wrong_key");
    }
    response
}

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
                Ok(sc) => Some(sc),
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
        None => None,
    };

    let max = match q_num::<usize>(&q, "maxBytes", "invalid_max_bytes") {
        Ok(v) => v
            .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP))
            .unwrap_or(SCAN_DEFAULT_BYTES),
        Err(r) => return r,
    };
    let outcome = match state
        .read_service()
        .execute_scan(crate::application::read_scan::ScanCommand {
            descriptor: desc.clone(),
            key: skey.clone(),
            cursor: sc,
            max_bytes: max,
            now_ms: now,
            lifetime_ms: SCAN_TTL_MS,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => return render_product_read_failure(error),
    };
    let is_json = desc.is_json();
    let mut body = Vec::with_capacity(4096);
    body.push(b'[');
    for (index, record) in outcome.records.iter().enumerate() {
        if index > 0 {
            body.push(b',');
        }
        body.extend_from_slice(b"{\"routingKey\":");
        body.extend_from_slice(
            serde_json::to_string(&record.rkey)
                .expect("string serialization")
                .as_bytes(),
        );
        if is_json {
            body.extend_from_slice(b",\"value\":");
            body.extend_from_slice(&record.payload);
        } else {
            use base64::Engine;
            body.extend_from_slice(b",\"valueB64\":\"");
            body.extend_from_slice(
                base64::engine::general_purpose::STANDARD
                    .encode(&record.payload)
                    .as_bytes(),
            );
            body.push(b'"');
        }
        body.push(b'}');
    }
    body.push(b']');
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(cursor) = outcome.continuation {
        response = response.header(
            "Prisma-Next-Scan-Cursor",
            cursor.encode(&desc.project_id, &skey),
        );
    } else {
        response = response.header("Prisma-Scan-Complete", "true");
    }
    crate::billing::meter_read(
        &state,
        &desc,
        outcome
            .records
            .iter()
            .map(|record| record.payload.len() as u64)
            .sum(),
        outcome.records.len() as u64,
    );
    response.body(Body::from(body)).unwrap()
}

// ---- Stage 2a: consumer groups --------------------------------------

use crate::application::names::valid_consumer_name;

// (desc, stream key, epoch) or an error response — the shared entry
// discipline for every consumer operation.

// Config ops live on the PARENT identity's committer lane.

/// Opaque consumer version: `{stream_epoch, consumer_generation}`,
/// base64url-encoded. Returned from consumer PUT/GET as
/// `Prisma-Consumer-Version` and REQUIRED on DELETE — a deletion names
/// an incarnation, never a name (round-17 P0: a stale retry by name
/// deleted the replacement consumer; an unpinned saga could rebind to
/// a recreated stream). Not signed: possessing delete authorization is
/// the capability, the token only pins WHICH incarnation it targets.
pub(crate) use crate::application::consumer::consumer_version_token;

fn consumer_failure_response(error: crate::application::consumer::ConsumerFailure) -> Response {
    use crate::application::consumer::FailureClass as C;
    if let Some(debt) = &error.deletion_debt {
        tracing::debug!(stream=%debt.stream,consumer=%debt.consumer,generation=debt.generation,epoch=%crate::crypto::hex(&debt.epoch),code=error.code,"consumer cleanup remains resumable");
    }

    if let Some(auth) = error.auth {
        return auth_failure_response(&auth);
    }
    let status = match error.class {
        C::Invalid => StatusCode::BAD_REQUEST,
        C::Denied => StatusCode::FORBIDDEN,
        C::Missing => StatusCode::NOT_FOUND,
        C::Conflict => StatusCode::CONFLICT,
        C::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        C::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = perr(
        status,
        error.code,
        &error.message,
        error.details.map(|details| *details),
        error.retryable,
    );
    if error.code == "wrong_key" {
        response = crate::audit::tag(response, "wrong_key");
    }
    if let Some(version) = error
        .version
        .and_then(|v| axum::http::HeaderValue::from_str(&v).ok())
    {
        response
            .headers_mut()
            .insert("prisma-consumer-version", version);
    }
    if let Some(owner) = error
        .owner
        .and_then(|v| axum::http::HeaderValue::from_str(&v).ok())
    {
        response.headers_mut().insert("streams-replay-to", owner);
    }
    response
}
fn consumer_config_response(
    cname: &str,
    out: crate::application::consumer::ConfigOutcome,
) -> Response {
    Response::builder()
        .status(if out.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .header(
            "prisma-consumer-version",
            consumer_version_token(&out.epoch, out.record.generation),
        )
        .body(Body::from(
            crate::application::consumer::config_value(cname, &out.record.config).to_string(),
        ))
        .unwrap()
}
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
fn consumer_key(headers: &HeaderMap) -> Result<String, Response> {
    product_key(headers).ok_or_else(|| {
        perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        )
    })
}
async fn product_consumer_put(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            &tenant.stream_ref(&name),
            cname.clone(),
            &key,
            &access,
            crate::tenant::Scope::ConsumersConfigure,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let doc = if body.is_empty() {
        crate::application::consumer::ConfigInput::default()
    } else {
        match serde_json::from_slice::<crate::application::consumer::ConfigInput>(&body) {
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
    match crate::application::consumer::put(context, doc, access).await {
        Ok(out) => consumer_config_response(&cname, out),
        Err(e) => consumer_failure_response(e),
    }
}

async fn product_consumer_get(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            &tenant.stream_ref(&name),
            cname.clone(),
            &key,
            &access,
            crate::tenant::Scope::MetadataRead,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    match crate::application::consumer::get(context).await {
        Ok(out) => consumer_config_response(&cname, out),
        Err(e) => consumer_failure_response(e),
    }
}

// Per-step cleanup budgets (rows staged per committer submit) and the
// per-REQUEST step budget. A million-row residue is deleted across
// many bounded, durably-committed steps — each retryable request makes
// monotone progress against the reduced durable row set instead of
// rebuilding one unbounded batch (round-17 P0).
// Segments are physically independent (own engines, own rows): sweep
// them concurrently, boundedly.

// ---- fleet-internal segment fan-out (cross-owner consumer ops) ------

fn json_ok(v: serde_json::Value) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(v.to_string()))
        .unwrap()
}

pub(crate) use crate::application::read_remote::InternalTarget;

/// §16 receiver side, step one: construct the registry identity FROM
/// THE SENDER'S project header — never from the deployment tenant —
/// so a multi-project cell can never bind a peer's request to the
/// wrong project's stream. The reserved system project is legal here
/// (§10.4: only internal workload identity touches system streams).
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
#[allow(
    clippy::result_large_err,
    reason = "transport boundary returns Axum wire response directly; application errors stay compact"
)]
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
    let matches = h("streams-internal-identity")
        .and_then(|value| crate::crypto::unhex(&value))
        .and_then(|value| <[u8; 16]>::try_from(value).ok())
        .is_some_and(|value| value == identity);
    if !matches {
        return Err(stale("identity"));
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

// Relay one segment's ConfigDeleteStep loop to its owner. Chunks the
// caller's remaining step budget so a relayed segment obeys the same
// per-request bound as a local one (durable progress either way).

/// Fleet-internal sweep target: run bounded ConfigDeleteStep rounds for
/// ONE locally-owned segment. fence_below arrives from the caller so
/// the generation-fenced cleanup semantics (round 17) hold unchanged.
pub(crate) async fn internal_sweep_segment(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !crate::http::fleet_operation_authorized(
        &state,
        &headers,
        crate::http::InternalOperation::ConsumerSweep,
    ) {
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
    let Some(route) = desc.segment_route_by_id(seg_id) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "unknown_segment",
            "segment is not part of this incarnation",
            None,
            false,
        );
    };
    match state
        .consumer_service()
        .sweep_local(
            InternalTarget {
                project_id: desc.project_id.clone(),
                stream_epoch: desc.epoch(),
                seg_id,
                identity,
            },
            route,
            doc.consumer,
            doc.fence_below,
            doc.max_steps,
        )
        .await
    {
        Ok(out) => json_ok(serde_json::to_value(out).expect("sweep outcome serializable")),
        Err(e) => consumer_failure_response(e),
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
    if !crate::http::fleet_operation_authorized(
        &state,
        &headers,
        crate::http::InternalOperation::QueueCursor,
    ) {
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
    let Some(route) = desc.segment_route_by_id(seg_id) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "unknown_segment",
            "segment is not part of this incarnation",
            None,
            false,
        );
    };
    match state
        .consumer_service()
        .queue_position(
            InternalTarget {
                project_id: desc.project_id.clone(),
                stream_epoch: desc.epoch(),
                seg_id,
                identity,
            },
            route,
            &consumer,
            cgen,
        )
        .await
    {
        Ok(out) => json_ok(serde_json::to_value(out).expect("queue position serializable")),
        Err(e) => consumer_failure_response(e),
    }
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
    if !crate::http::fleet_operation_authorized(
        &state,
        &headers,
        crate::http::InternalOperation::SegmentScan,
    ) {
        return crate::http::internal_unauthorized();
    }
    let q = |h: &str| headers.get(h).and_then(|v| v.to_str().ok());
    let (Some(from), Some(end), Some(max_bytes), Some(key_b64)) = (
        q("streams-internal-from").and_then(|v| v.parse::<u64>().ok()),
        // Older peers omit the bound; malformed present bounds are errors.
        headers
            .get("streams-internal-end")
            .map_or(Some(u64::MAX), |v| v.to_str().ok()?.parse::<u64>().ok()),
        q("streams-internal-max-bytes")
            .and_then(|v| v.parse::<usize>().ok())
            // Clamped to the public scan ceiling: an internal budget
            // header must not buy a larger page than the operation it
            // relays for (round-19 security finding).
            .map(|v| v.clamp(1, READ_MAX_BYTES_CAP)),
        q("stream-encryption-key"),
    ) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "from/max-bytes/key required; end must be a valid u64 when present",
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
    let (skey, epoch) = match crate::http::check_key(Some(key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        _ => return perr(StatusCode::FORBIDDEN, "wrong_key", "key", None, false),
    };
    let Some(route) = desc.segment_route_by_id(seg_id) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "unknown_segment",
            "segment is not part of this incarnation",
            None,
            false,
        );
    };
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
    let out = match crate::application::read::ReadPlan::segment(
        &skey,
        &epoch,
        &handle,
        &engine,
        crate::application::read::ReadRange::bounded(from, end),
        None,
        max_bytes,
        crate::shard::Deliver::Durable,
    )
    .execute()
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

async fn product_consumer_delete(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let Some(token) = headers
        .get("prisma-consumer-version")
        .and_then(|v| v.to_str().ok())
    else {
        return perr(
            StatusCode::BAD_REQUEST,
            "missing_consumer_version",
            "DELETE requires Prisma-Consumer-Version (returned by consumer create/get, and by the consumer_deleting conflict); a deletion targets an incarnation, not a name",
            None,
            false,
        );
    };
    let Some(version) = crate::application::consumer::parse_consumer_version(token) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "invalid_consumer_version",
            "Prisma-Consumer-Version is not a version token from this server",
            None,
            false,
        );
    };
    match crate::application::consumer::delete(
        state.consumer_service(),
        tenant.stream_ref(&name),
        cname,
        key,
        version,
        access,
    )
    .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::empty())
            .unwrap(),
        Err(e) => consumer_failure_response(e),
    }
}

/// DLQ transition (spec §2.8): append the DLQ record to the configured
/// dead-letter stream with a producer identity derived from the message
/// id (crash-idempotent), and only after that is durable, ack the
/// source lease. No dead-letter stream configured -> the poison is
/// dropped by acking directly.
#[allow(clippy::too_many_arguments)]
async fn product_consumer_pull(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            &tenant.stream_ref(&name),
            cname.clone(),
            &key,
            &access,
            crate::tenant::Scope::ConsumersPull,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let context = match service.active(context).await {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let doc = if body.is_empty() {
        crate::application::consumer::PullInput::default()
    } else {
        match serde_json::from_slice::<crate::application::consumer::PullInput>(&body) {
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
    match crate::application::consumer::pull(context, doc).await {
        Ok(out) => {
            if !out.messages.is_empty() {
                crate::billing::meter_pull(
                    &state,
                    &out.descriptor,
                    out.payload_bytes,
                    out.messages.len() as u64,
                );
            }
            json_ok(json!({"messages":out.messages,"backlog":out.backlog}))
        }
        Err(e) => consumer_failure_response(e),
    }
}

async fn product_consumer_settle(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            &tenant.stream_ref(&name),
            cname.clone(),
            &key,
            &access,
            crate::tenant::Scope::ConsumersSettle,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let context = match service.active(context).await {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let doc = match serde_json::from_slice::<crate::application::consumer::SettleInput>(&body) {
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
    match crate::application::consumer::settle(context, doc).await {
        Ok(out) => json_ok(serde_json::to_value(out).expect("settle outcome serializable")),
        Err(e) => consumer_failure_response(e),
    }
}

// ---- Stage 2b: watches ----------------------------------------------

#[cfg(test)]
#[cfg(test)]
pub(crate) use crate::application::watch::watch_key_hex;

fn watch_def_json(w: &crate::registry::WatchDefinition) -> serde_json::Value {
    json!({"name": w.name, "fields": w.fields})
}

async fn product_watches_list(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
) -> Response {
    match state
        .watch_service()
        .definitions(&tenant.stream_ref(&name))
        .await
    {
        Ok(definitions) => json_ok(
            json!({ "watches": definitions.iter().map(watch_def_json).collect::<Vec<_>>() }),
        ),
        Err(error) => watch_failure_response(error),
    }
}

async fn product_watch_get(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    watch: String,
) -> Response {
    match state
        .watch_service()
        .definitions(&tenant.stream_ref(&name))
        .await
    {
        Ok(definitions) => match definitions
            .iter()
            .find(|definition| definition.name == watch)
        {
            Some(definition) => json_ok(watch_def_json(definition)),
            None => watch_failure_response(crate::application::watch::WatchFailure::UnknownWatch),
        },
        Err(error) => watch_failure_response(error),
    }
}

#[allow(clippy::too_many_arguments)] // Parsed protocol fields converge into one typed application command.
async fn product_watch_wait(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    watch: String,
    key_hex: String,
    headers: HeaderMap,
    query: &str,
    access: crate::application::watch::WatchAccess<'_>,
) -> Response {
    use crate::application::watch::{Observation, WatchCredentials};
    // Parse a carrier without surfacing syntax diagnostics until proof exists.
    let parsed_query = strict_query(query, &["cursor", "cap", "timeoutMs"]);
    let capability = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Prisma-Watch "))
        .map(str::to_string)
        .or_else(|| {
            parsed_query
                .as_ref()
                .ok()
                .and_then(|query| query.get("cap").cloned())
        });
    let service = state.watch_service();
    let verified = match service
        .authenticate(
            &tenant.stream_ref(&name),
            watch,
            key_hex,
            WatchCredentials {
                capability,
                encryption_key: product_key(&headers),
            },
            access,
        )
        .await
    {
        Ok(proof) => proof,
        Err(error) => return watch_failure_response(error),
    };
    let query = match parsed_query {
        Ok(query) => query,
        Err(response) => return response,
    };
    let cursor = query
        .get("cursor")
        .cloned()
        .unwrap_or_else(|| "now".to_string());
    let timeout = match q_num::<u64>(&query, "timeoutMs", "invalid_timeout_ms") {
        Ok(value) => std::time::Duration::from_millis(value.unwrap_or(25_000).min(25_000)),
        Err(response) => return response,
    };
    let body = match service.wait(verified, cursor, timeout).await {
        Ok(Observation::Touched {
            cursor,
            proven,
            stream_cursor,
        }) => json!({
            "invalidated": true, "reason": if proven { "changed" } else { "resync" },
            "cursor": cursor, "streamCursor": stream_cursor,
        }),
        Ok(Observation::Stale { cursor }) => {
            json!({"invalidated": true, "reason": "resync", "cursor": cursor})
        }
        Ok(Observation::Timeout {
            cursor,
            stream_cursor,
        }) => json!({"invalidated": false, "cursor": cursor, "streamCursor": stream_cursor}),
        Err(error) => return watch_failure_response(error),
    };
    let mut response = json_ok(body);
    response
        .headers_mut()
        .insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    response
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
        // mt-lint: allow(state-tenant-read): Off/Shadow single-tenant posture (Stage 5d) — enforce-mode requests always carry a principal
        .unwrap_or_else(|| state.deployment.deployment_tenant().clone());
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
    let row: crate::rollup::MonthRow = match rollup
        .month_row(&month, &id.account_id, &id.project_id, &id.stream_id)
        .await
    {
        Ok(row) => row.unwrap_or_default(),
        Err(error) => {
            return perr(
                StatusCode::SERVICE_UNAVAILABLE,
                "usage_unavailable",
                &error.to_string(),
                None,
                true,
            );
        }
    };
    let is_current = month == current;
    // Round-21 blocker 2: a retained-but-idle stream has no month row
    // yet for the CURRENT month — the durable segment index still knows
    // its gauge, so provisional storage never reads as zero.
    let (fallback_byte_ms, fallback_owned) = if is_current && row.segments.is_empty() {
        let states = match rollup
            .stream_segment_states(&id.account_id, &id.project_id, &id.stream_id)
            .await
        {
            Ok(states) => states,
            Err(error) => {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "usage_unavailable",
                    &error.to_string(),
                    None,
                    true,
                );
            }
        };
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
    let name_agg = match rollup
        .name_row(&month, &id.account_id, &id.project_id, &id.stream_name)
        .await
    {
        Ok(row) => row,
        Err(error) => {
            return perr(
                StatusCode::SERVICE_UNAVAILABLE,
                "usage_unavailable",
                &error.to_string(),
                None,
                true,
            );
        }
    };
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
            .unwrap_or_else(|| state.deployment.account_id().to_string())
    } else {
        state.deployment.account_id().to_string()
    };
    let agg = match rollup.project_row(&month, &account, &project).await {
        Ok(row) => row.unwrap_or_default(),
        Err(error) => {
            return perr(
                StatusCode::SERVICE_UNAVAILABLE,
                "usage_unavailable",
                &error.to_string(),
                None,
                true,
            );
        }
    };
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
pub(crate) use crate::application::lifecycle::EnterSeal;
pub(crate) use crate::application::lifecycle::seal_op_id_full;
#[cfg(test)]
pub(crate) async fn enter_sealing_cas(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, crate::application::lifecycle::SealError> {
    crate::application::lifecycle::enter_sealing_cas(
        &state.lifecycle_service(),
        sref,
        op_id,
        intent,
        expect_epoch,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn claim_seal(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, crate::application::lifecycle::SealError> {
    crate::application::lifecycle::claim_seal(
        &state.lifecycle_service(),
        sref,
        op_id,
        intent,
        expect_epoch,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)] // Test adapter exercises all coordinates of a durable claim.
pub(crate) async fn install_reserved_claim(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    old_op: &str,
    old_gen: u64,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    reserved: u64,
) -> Result<bool, crate::application::lifecycle::SealError> {
    crate::application::lifecycle::install_reserved_claim(
        &state.lifecycle_service(),
        sref,
        expect_epoch,
        old_op,
        old_gen,
        op_id,
        intent,
        reserved,
    )
    .await
}

pub(crate) async fn run_seal(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    op: Option<String>,
    expect_epoch: &str,
    claim_gen: Option<u64>,
) -> Result<(), crate::application::lifecycle::SealError> {
    crate::application::lifecycle::run_seal(
        &state.lifecycle_service(),
        sref,
        op,
        expect_epoch,
        claim_gen,
    )
    .await
}

fn watch_failure_response(error: crate::application::watch::WatchFailure) -> Response {
    use crate::application::watch::WatchFailure;
    match error {
        WatchFailure::Unauthorized(project) => {
            let response = crate::audit::tag(
                perr(
                    StatusCode::FORBIDDEN,
                    "watch_unauthorized",
                    "a valid observation capability or Prisma-Encryption-Key is required",
                    None,
                    false,
                ),
                "watch_unauthorized",
            );
            match project {
                Some(project) => crate::audit::tag_project(response, &project),
                None => response,
            }
        }
        WatchFailure::PolicyStale(project) => crate::audit::tag_project(
            perr(
                StatusCode::SERVICE_UNAVAILABLE,
                "policy_stale",
                "project policy is stale; retry shortly",
                None,
                true,
            ),
            &project,
        ),
        WatchFailure::ProjectInactive(project) => crate::audit::tag_project(
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
            &project,
        ),
        WatchFailure::Quota(project, refusal) => {
            crate::audit::tag_project(quota_refusal_response(&refusal), &project)
        }
        WatchFailure::InvalidKey => perr(
            StatusCode::BAD_REQUEST,
            "invalid_watch_key",
            "watch key must be 16 hex chars",
            None,
            false,
        ),
        WatchFailure::Creating => perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            None,
            true,
        ),
        WatchFailure::NotFound => perr(
            StatusCode::NOT_FOUND,
            "not_found",
            "stream not found",
            None,
            false,
        ),
        WatchFailure::UnknownWatch => perr(
            StatusCode::NOT_FOUND,
            "unknown_watch",
            "no such watch definition",
            None,
            false,
        ),
        WatchFailure::Storage(message) => perr(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &message,
            None,
            true,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WP-03/PR 5: the wire error MESSAGES are pinned across the
    /// single-sourcing — same code, same body text per case, including
    /// the multi-violation precedence corner (`__ds/..` reports the
    /// dot segment — since PR 4.1 that order is owned by the canonical
    /// layer, not re-scanned here).
    #[test]
    fn name_error_messages_are_wire_pinned() {
        let msg = |raw: &str| match ProductStreamName::try_from(raw) {
            Ok(_) => panic!("{raw:?} must be rejected"),
            Err(e) => e.message(),
        };
        assert_eq!(msg(""), "stream name must be 1-512 UTF-8 bytes");
        assert_eq!(
            msg(&"x".repeat(513)),
            "stream name must be 1-512 UTF-8 bytes"
        );
        assert_eq!(msg("has\u{7}bell"), "control characters are not allowed");
        assert_eq!(msg("a//b"), "empty path segments are not allowed");
        assert_eq!(msg("a/./b"), "'.' and '..' segments are not allowed");
        assert_eq!(msg("__ds/x"), "the __ds namespace is reserved");
        assert_eq!(
            msg("a/records"),
            "'records', 'consumers' and 'watches' are reserved subresource names"
        );
        assert_eq!(
            msg("a/consumers/b"),
            "this name is already a subresource path (…/records, …/consumers/{name}, …/watches/…)"
        );
        // The precedence corner: reserved root AND a dot segment —
        // the segment message wins, as the old scan order dictated.
        assert_eq!(msg("__ds/.."), "'.' and '..' segments are not allowed");
        assert_eq!(msg("__ds//x"), "empty path segments are not allowed");
    }

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
        crate::registry::PersistedDescriptor {
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
        .try_into()
        .expect("valid descriptor fixture")
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
        let mut foreign = d.to_persisted();
        foreign.project_id = crate::tenant::ProjectId::new("proj-other").unwrap();
        let foreign = StreamDesc::try_from(foreign).unwrap();
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
        let out = render_product_read_failure(crate::application::read::ReadFailure::Resolve(
            crate::shard_directory::ResolveError::NotOwner {
                prefix: "000".into(),
                owner: "streams-2".into(),
            },
        ));
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
        // The applied rollback verdict is an explicit typed failure; it does
        // not infer rewind semantics from an unrelated HTTP status.
        let out =
            render_product_read_failure(crate::application::read::ReadFailure::CursorBeyondTail);
        assert_eq!(out.status(), StatusCode::CONFLICT);
        assert!(out.headers().get("streams-replay-to").is_none());
    }
}
