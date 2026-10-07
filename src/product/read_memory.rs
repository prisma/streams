//! The product surface's memory backstops after project admission (round
//! 13 and shared cells H3): a WRITE is refused while its project's
//! estimated pressure is over its line (`project_memory_gate`), and a PAGE
//! READ (a records read or long-poll, or a scan) reserves the bytes its
//! page may hold before it runs (`admission::read_memory`). Established
//! SSE delivery and every other read continue while a project is engaged.
//!
//! A read reserves `min(maxBytes, 8 MiB)`, the route's own page budget
//! (`product_read`, `product_scan`): an unparseable `maxBytes` reserves
//! the cap and is refused 400 by the route, as before. Its project's
//! memory line refuses it at once (429 `project_memory_pressure`, as a
//! write's); the instance's read memory makes it wait, then refuses it
//! (503 `read_memory_busy`). Both are retryable after 1 s.

use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::Response;

use super::{
    AppState, ProductRoute, READ_MAX_BYTES_CAP, SCAN_DEFAULT_BYTES, classify_route, parse_query,
    perr, project_memory_gate, quota_refusal_response, strip_verb,
};
use crate::admission::read_memory::{ReadHold, ReadRefusal};
use crate::auth::RequestPrincipal;

/// The memory backstops for one admitted request on the product surface
/// (`path` is the route's whole wildcard path, subresource and verb
/// included): the refusal to answer with, or `None` to serve it.
pub(crate) async fn refusal(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    path: &str,
    query: &str,
) -> Option<Response> {
    if method == Method::POST {
        return project_memory_gate(state, principal);
    }
    let bytes = page_bytes(method, path, query)?;
    let project = principal.and_then(|p| state.quotas.read_bytes(&p.project_id));
    let line = state.admission.project_memory_pressure_bytes();
    match ReadHold::reserve(&state.admission, project, line, bytes).await {
        Ok(hold) => {
            crate::admission::park::bind_read(hold);
            None
        }
        Err(ReadRefusal::Project) => {
            let refusal = quota_refusal_response(&crate::quota::QuotaRefusal::MemoryPressure);
            Some(match principal {
                Some(p) => crate::audit::tag_project(refusal, &p.project_id),
                None => refusal,
            })
        }
        Err(ReadRefusal::Instance) => Some(read_memory_busy()),
    }
}

/// The bytes a page read on the route `path` may hold: `None` for every
/// request that renders no page.
fn page_bytes(method: &Method, path: &str, query: &str) -> Option<u64> {
    if method != Method::GET {
        return None;
    }
    let default = match (classify_route(path).ok()?, strip_verb(path).1) {
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
