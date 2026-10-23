//! Queue streams on the stream surfaces (docs/QUEUES-DESIGN.md §4): a
//! queue is reached only through `/v1/queues`, so every `/v1/streams` and
//! raw `/v1/stream` request that names a queue stream is refused 409
//! `queue_via_queue_routes` (edge change #186) before its handler runs.
use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::MethodRouter;

use super::super::AppState;
use super::super::internal::DescriptorReadAnswer;

fn queue_via_queue_routes() -> Response {
    super::super::perr(
        StatusCode::CONFLICT,
        "queue_via_queue_routes",
        "this stream is a queue; use the /v1/queues routes",
        None,
        false,
    )
}

/// Whether `sref` names a live stream of the queue kind.
async fn names_a_queue(
    state: &AppState,
    sref: &crate::tenant::TenantStreamRef,
) -> Result<bool, object_store::Error> {
    Ok(state.registry.get(sref).await?.is_some_and(|d| {
        crate::application::creation::desc_alive(&d)
            && crate::native_queue::is_queue_kind(&d.content_type)
    }))
}

/// The product entry's name refusals, after authentication and admission
/// and before the body is read: the system namespace (the leading `_`
/// segment belongs to the telemetry planes and no customer credential
/// reaches it; usage lookups under `{name}/usage` are a customer stream's
/// sub-resource and unaffected), then a queue stream under any route.
// mt-lint: allow(name-param-shared-core): the product entry's route path at ingress, qualified below with the verified principal's project
pub(crate) async fn stream_refusal(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
    method: &Method,
    name: &str,
) -> Option<Response> {
    if crate::billing::is_reserved_stream(name) {
        let mut r = crate::audit::tag(
            super::super::perr(
                StatusCode::FORBIDDEN,
                "reserved_stream",
                "names beginning with '_' are reserved for the system",
                None,
                false,
            ),
            "reserved_stream",
        );
        if let Some(p) = principal {
            r = crate::audit::tag_project(r, &p.project_id);
        }
        return Some(r);
    }
    // A preflight and a capability carrier name no tenant here.
    let enforce = state.auth.mode == crate::auth::AuthMode::Enforce;
    if method == Method::OPTIONS || (enforce && principal.is_none()) {
        return None;
    }
    let route = super::super::classify_route(name).ok()?;
    // The self-authorizing watch wait reads its descriptor through the
    // watch lookup budget (phase B step 15); a read here first would fill
    // the cache unbudgeted. A queue feeds no watch journal, so a wait on
    // one observes nothing a queue route should own.
    if matches!(route, super::super::ProductRoute::WatchWait { .. }) {
        return None;
    }
    let stream = super::super::route_stream_name(&route);
    // mt-lint: allow(stream-ref-construction): ingress — the product entry's stream qualified with the verified principal's project, as product.rs does
    let sref = super::tenant_of(state, principal).stream_ref(stream);
    // The request's descriptor read: a store failure answers as every
    // product handler's own read does (edge change #64).
    match names_a_queue(state, &sref).await {
        Ok(queue) => queue.then(queue_via_queue_routes),
        Err(e) => Some(e.descriptor_read_answer()),
    }
}

/// `route` (the raw `/v1/stream/{*name}` surface) with queue streams
/// refused for the deployment credential; any other request passes to the
/// route, which answers it as before.
pub(crate) fn guard_raw(
    state: &Arc<AppState>,
    route: MethodRouter<Arc<AppState>>,
) -> MethodRouter<Arc<AppState>> {
    // Built once at boot: the shard sweepers sign wake-up notifications
    // with this deployment's root from the first moment, not from the first
    // queue request after a restart (Fable code review H3).
    if let Some(root) = state.catalog_cursor_key {
        crate::native_queue::wakeup::install_root(root);
    }
    route.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        raw_guard,
    ))
}

async fn raw_guard(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    req: Request,
    next: Next,
) -> Response {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let authorized = state.bearer.authorizes(presented, state.auth.mode);
    let canonical = crate::tenant::CanonicalStreamName::new(&name).is_ok();
    if authorized && canonical && req.method() != Method::OPTIONS {
        // mt-lint: allow(raw-adapter-sref): the raw surface's own route layer, deriving the identity stream_entry_inner derives for the same path
        let sref = state.deployment.raw_adapter_sref(&name);
        // An unreadable registry decides nothing here: the raw handler
        // reads the descriptor again and answers.
        if names_a_queue(&state, &sref).await.unwrap_or(false) {
            return queue_via_queue_routes();
        }
    }
    next.run(req).await
}
