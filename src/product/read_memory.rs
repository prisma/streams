//! The product surface's memory backstops after project admission (round
//! 13 and shared cells H3): a WRITE is refused while its project's
//! estimated pressure is over its line (`project_memory_gate`), and a PAGE
//! READ (a records read or long-poll, or a scan) and a CONSUMER PULL
//! reserve the bytes they may hold before they run
//! (`admission::read_memory`). Established SSE delivery and every other
//! read continue while a project is engaged.
//!
//! A read reserves `min(maxBytes, 8 MiB)`, the route's own page budget
//! (`product_read`, `product_scan`): an unparseable `maxBytes` reserves
//! the cap and is refused 400 by the route, as before. A pull, which also
//! passes the write gate, reserves what one walk of its lineage reads
//! before it leases (`PULL_COVERAGE_BYTES`), once its body is buffered, so
//! its own reservation never weighs on its own body (landing call C1); its
//! batch replaces the reservation when it renders. Its project's memory
//! line makes it wait, then refuses it (429 `project_memory_pressure`, as
//! a write's); the instance's read memory makes it wait, then refuses it
//! (503 `read_memory_busy`). Both are retryable after 1 s.

use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::Response;

use super::{
    AppState, ProductRoute, READ_MAX_BYTES_CAP, SCAN_DEFAULT_BYTES, classify_route, parse_query,
    perr, project_memory_gate, quota_refusal_response, strip_verb,
};
use crate::admission::body::BodyRefusal;
use crate::admission::read_memory::{ReadHold, ReadRefusal};
use crate::application::consumer::PULL_COVERAGE_BYTES;
use crate::auth::RequestPrincipal;

/// The memory backstops for one admitted request on the product surface
/// before its body (`path` is the route's whole wildcard path, subresource
/// and verb included): a write's latch and a page read's reservation; a
/// pull reserves its coverage in `body`, after its body. The refusal to
/// answer with, or `None` to serve it.
pub(crate) async fn refusal(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    path: &str,
    query: &str,
) -> Option<Response> {
    if method == Method::POST
        && let Some(refusal) = project_memory_gate(state, principal)
    {
        return Some(refusal);
    }
    reserve(state, principal, page_bytes(method, path, query)?).await
}

/// Reserve `bytes` for the request's page in its project's read bytes and
/// the instance's read memory: the refusal to answer, or `None` once the
/// hold is bound to the request.
async fn reserve(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    bytes: u64,
) -> Option<Response> {
    let project = principal.and_then(|p| state.quotas.read_bytes(&p.project_id));
    let line = state.admission.project_memory_pressure_bytes();
    match ReadHold::reserve(&state.admission, project, line, bytes).await {
        Ok(hold) => {
            crate::admission::park::bind_read(hold);
            None
        }
        Err(ReadRefusal::Project) => Some(memory_pressure(principal)),
        Err(ReadRefusal::Instance) => Some(read_memory_busy()),
    }
}

/// 429 `project_memory_pressure`, retryable, tagged with the project.
fn memory_pressure(principal: Option<&RequestPrincipal>) -> Response {
    let refusal = quota_refusal_response(&crate::quota::QuotaRefusal::MemoryPressure);
    match principal {
        Some(p) => crate::audit::tag_project(refusal, &p.project_id),
        None => refusal,
    }
}

/// The request's body: a `POST` or `PUT` buffers it under its project's
/// memory line (`admission::body`), and a consumer pull then reserves its
/// coverage, so the pull's own reservation is never one of the bytes its
/// body is buffered beside (landing call C1). Every other method discards
/// its body without polling it. `Err` is the refusal to answer.
pub(crate) async fn body(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    path: &str,
    incoming: axum::body::Body,
) -> Result<(bytes::Bytes, Option<crate::quota::BufferedBodyGuard>), Box<Response>> {
    if method != Method::POST && method != Method::PUT {
        return Ok((bytes::Bytes::new(), None));
    }
    let buffered = crate::admission::body::buffer_within(
        incoming,
        state.config.cli.max_request_body_bytes,
        principal.and_then(|p| state.quotas.pressure_handle(&p.project_id)),
        state.admission.project_memory_pressure_bytes(),
    )
    .await
    .map_err(|refusal| Box::new(body_refusal(refusal, principal)))?;
    if let Some(coverage) = pull_coverage(method, path)
        && let Some(refusal) = reserve(state, principal, coverage).await
    {
        return Err(Box::new(refusal));
    }
    Ok(buffered)
}

/// A body that was not buffered: 413 `body_too_large` past the body
/// limit, as before, and 429 `project_memory_pressure` (retryable after
/// 1 s, as a read's) when it would have taken its project past the line.
fn body_refusal(refusal: BodyRefusal, principal: Option<&RequestPrincipal>) -> Response {
    match refusal {
        BodyRefusal::TooLarge => perr(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            "request body exceeds the limit",
            None,
            false,
        ),
        BodyRefusal::MemoryPressure => memory_pressure(principal),
    }
}

/// The coverage a consumer pull on the route `path` reserves once its body
/// is buffered: `None` for every other request.
fn pull_coverage(method: &Method, path: &str) -> Option<u64> {
    let route = classify_route(path).ok()?;
    match (method == Method::POST, route, strip_verb(path).1) {
        (true, ProductRoute::Consumer { .. }, Some("pull")) => {
            u64::try_from(PULL_COVERAGE_BYTES).ok()
        }
        _ => None,
    }
}

/// The bytes a page read on the route `path` may hold: `None` for every
/// request that renders no page before its body is buffered.
fn page_bytes(method: &Method, path: &str, query: &str) -> Option<u64> {
    if method != Method::GET {
        return None;
    }
    let route = classify_route(path).ok()?;
    let verb = strip_verb(path).1;
    let default = match (route, verb) {
        (ProductRoute::Records { .. }, None | Some("long-poll")) => READ_MAX_BYTES_CAP,
        (ProductRoute::Collection { .. }, Some("scan")) => SCAN_DEFAULT_BYTES,
        _ => return None,
    };
    let asked = parse_query(query)
        .get("maxBytes")
        .and_then(|v| v.parse::<usize>().ok())
        .map_or(default, |v| v.clamp(4096, READ_MAX_BYTES_CAP));
    u64::try_from(asked).ok()
}

fn read_memory_busy() -> Response {
    let mut r = perr(
        StatusCode::SERVICE_UNAVAILABLE,
        "read_memory_busy",
        "the instance's read memory is held by other reads; retry",
        None,
        true,
    );
    r.headers_mut()
        .insert("retry-after", HeaderValue::from_static("1"));
    r
}

/// A page of `served` bytes rendered for this request: its read hold
/// becomes the page's exact size, made here for a page no admission
/// reserved (a consumer pull's batch).
pub(super) fn settle(state: &AppState, principal: Option<&RequestPrincipal>, served: usize) {
    crate::admission::park::settle_read(u64::try_from(served).unwrap_or(u64::MAX), || {
        let project = principal.and_then(|p| state.quotas.read_bytes(&p.project_id));
        ReadHold::unreserved(&state.admission, project)
    });
}
