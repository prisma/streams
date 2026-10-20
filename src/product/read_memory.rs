//! The product surface's memory backstops after project admission (round
//! 13 and shared cells H3): a WRITE is refused while its project's
//! estimated pressure is over its line (`project_memory_gate`), an APPEND
//! while the instance is over its RSS shed line, its body read and dropped
//! rather than buffered (`application::append::memory_gate`, in `body`), and
//! a PAGE
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
//!
//! A request that writes the registry (a create, recreate or fork, a
//! delete, a seal) is refused while its project's store budget is in debt
//! (public-beta phase B step 14(b), owner question Q1, decided (b') on
//! 2026-10-12): 429 `project_rate_limit` with the seconds until it is not, as
//! the request-rate bucket answers. Every other route only pays into the
//! budget (`quota::parked`).
//!
//! The catalog (`GET /v1/streams`, its own route) is admitted here too
//! (public-beta phase B item B6): `admit_catalog` binds its project's share
//! after project admission, so a listing pays for the descriptors it reads
//! and is refused while the store budget is in debt, and
//! `catalog_page_reserved` reserves what its page may hold, its
//! descriptors counted decoded (`Registry::catalog_page_bytes`, about
//! 5.3 MiB), as a page read does. Its
//! answer is names only, so the reservation is released once it answers.

use axum::http::{HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;

use super::{
    AppState, ProductOperation, ProductRoute, READ_MAX_BYTES_CAP, SCAN_DEFAULT_BYTES,
    classify_route, parse_query, perr, project_admission, project_memory_gate,
    quota_refusal_response, strip_verb,
};
use crate::admission::body::BodyRefusal;
use crate::admission::read_memory::{ReadHold, ReadRefusal};
use crate::application::consumer::PULL_COVERAGE_BYTES;
use crate::auth::RequestPrincipal;
use crate::quota::QuotaGuard;
use crate::registry::{CatalogPage, Registry};
use crate::tenant::ProjectId;

/// The memory backstops for one admitted request on the product surface
/// before its body (`path` is the route's whole wildcard path, subresource
/// and verb included): a write's latch, a registry write's store budget
/// and a page read's reservation; a pull reserves its coverage in `body`,
/// after its body. The refusal to answer with, or `None` to serve it.
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
    if writes_registry(method, path)
        && let Some(refusal) = store_budget_refusal(state, principal)
    {
        return Some(refusal);
    }
    let bytes = page_bytes(method, path, query)?;
    reserve(state, project_of(principal), bytes).await
}

/// How long an append refused over the shed line has to finish sending the
/// body the server reads and drops before answering.
const SHED_DISCARD_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// The page a records read without `maxBytes` renders and reserves on a
/// shared cell (owner call Shared cells Q1 (B), the fallback Layer B's
/// `read_memory_busy` triggered): eight of them fit where one 8 MiB
/// default did.
const SHARED_CELL_PAGE_BYTES: usize = 1 << 20;

/// The query a product request is served with. On a shared cell
/// (`PROJECT_SHARE_K` > 1) a records read or long-poll that names no
/// `maxBytes` is served as if it named the shared cell's default page,
/// 1 MiB, so its reservation (`page_bytes`) and its page (`product_read`)
/// both take it; every other request, and every request on a dedicated
/// cell, keeps its query as sent.
pub(crate) fn paged_query(state: &AppState, method: &Method, path: &str, uri: &Uri) -> String {
    let query = uri.query().unwrap_or("");
    query_on_cell(state.config.cli.project_share_k, method, path, query)
}

/// `query` as `paged_query` serves it on a cell of `share_k` projects.
fn query_on_cell(share_k: u64, method: &Method, path: &str, query: &str) -> String {
    let default_paged = share_k > 1
        && method == Method::GET
        && matches!(
            (classify_route(path), strip_verb(path).1),
            (Ok(ProductRoute::Records { .. }), None | Some("long-poll"))
        )
        && !parse_query(query).contains_key("maxBytes");
    match (default_paged, query.is_empty()) {
        (false, _) => query.to_string(),
        (true, true) => format!("maxBytes={SHARED_CELL_PAGE_BYTES}"),
        (true, false) => format!("{query}&maxBytes={SHARED_CELL_PAGE_BYTES}"),
    }
}

impl AppState {
    /// The catalog's admission: project admission, as `product_entry`
    /// admits, then the request's project share, so the listing pays for
    /// the store work it makes, then the store budget, which refuses it
    /// while in debt (14(b)'s bind for the catalog). The guard holds the
    /// project's request slot; `Err` is the refusal to answer.
    pub(crate) fn admit_catalog(
        &self,
        principal: Option<&RequestPrincipal>,
    ) -> Result<Option<QuotaGuard>, Box<Response>> {
        let guard = project_admission(self, principal).map_err(Box::new)?;
        crate::admission::park::bind_principal(&self.quotas, principal);
        match store_budget_refusal(self, principal) {
            Some(refusal) => Err(Box::new(refusal)),
            None => Ok(guard),
        }
    }

    /// `project`'s catalog page, read once what it may hold is reserved in
    /// the project's read bytes and the instance's read memory, as a page
    /// read reserves; the hold is the request's until it answers. `Err` is
    /// the refusal to answer: 429 `project_memory_pressure` or 503
    /// `read_memory_busy` after the reservation's wait, or 500 `internal`
    /// when the page fails.
    pub(crate) async fn catalog_page_reserved(
        &self,
        project: &ProjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage, Box<Response>> {
        let bytes = Registry::catalog_page_bytes(limit);
        if let Some(refusal) = reserve(self, Some(project), bytes).await {
            return Err(Box::new(refusal));
        }
        self.registry
            .list_page(project, after, limit)
            .await
            .map_err(|e| {
                Box::new(perr(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                    None,
                    true,
                ))
            })
    }
}

/// The verified principal's project, if any.
fn project_of(principal: Option<&RequestPrincipal>) -> Option<&ProjectId> {
    principal.map(|p| &p.project_id)
}

/// Whether the request is a records append, single or batch: over the shed
/// line it is refused before its body is read.
fn appends(method: &Method, path: &str) -> bool {
    let Ok(route) = classify_route(path) else {
        return false;
    };
    matches!(
        ProductOperation::resolve(&route, strip_verb(path).1, method),
        Some(ProductOperation::Append | ProductOperation::AppendBatch)
    )
}

/// Whether the request names an operation that writes the registry: a
/// create (a recreate and a fork are creates), a delete or a seal.
fn writes_registry(method: &Method, path: &str) -> bool {
    let Ok(route) = classify_route(path) else {
        return false;
    };
    matches!(
        ProductOperation::resolve(&route, strip_verb(path).1, method),
        Some(ProductOperation::Create | ProductOperation::Delete | ProductOperation::Seal)
    )
}

/// 429 `project_rate_limit`, tagged with the project, while its store
/// budget is in debt; `None` without a verified project.
fn store_budget_refusal(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
) -> Option<Response> {
    let p = principal?;
    let share = state.quotas.park_share(&p.project_id, &p.quotas)?;
    let retry_after_secs = share.store_ops_available(crate::shard::now_ms()).err()?;
    let refusal = crate::quota::QuotaRefusal::Rate { retry_after_secs };
    Some(crate::audit::tag_project(
        quota_refusal_response(&refusal),
        &p.project_id,
    ))
}

/// Reserve `bytes` for the request's page in its project's read bytes and
/// the instance's read memory: the refusal to answer, or `None` once the
/// hold is bound to the request.
async fn reserve(state: &AppState, project: Option<&ProjectId>, bytes: u64) -> Option<Response> {
    let held = project.and_then(|p| state.quotas.read_bytes(p));
    let line = state.admission.project_memory_pressure_bytes();
    match ReadHold::reserve(&state.admission, held, line, bytes).await {
        Ok(hold) => {
            crate::admission::park::bind_read(hold);
            None
        }
        Err(ReadRefusal::Project) => Some(memory_pressure(project)),
        Err(ReadRefusal::Instance) => Some(read_memory_busy()),
    }
}

/// 429 `project_memory_pressure`, retryable, tagged with the project.
fn memory_pressure(project: Option<&ProjectId>) -> Response {
    let refusal = quota_refusal_response(&crate::quota::QuotaRefusal::MemoryPressure);
    match project {
        Some(project) => crate::audit::tag_project(refusal, project),
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
    if appends(method, path)
        && let Err(failure) =
            crate::application::append::memory_gate(&state.admission, &state.runtime.history).await
    {
        let limit = u64::try_from(state.config.cli.max_request_body_bytes).unwrap_or(u64::MAX);
        crate::admission::body::discard_within(incoming, limit, SHED_DISCARD_WAIT).await;
        return Err(Box::new(super::append_error::render_product_append_error(
            failure,
        )));
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
        && let Some(refusal) = reserve(state, project_of(principal), coverage).await
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
        BodyRefusal::MemoryPressure => memory_pressure(project_of(principal)),
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

#[cfg(test)]
mod tests;
