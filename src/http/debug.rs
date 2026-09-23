//! The operator debug surface's one authorization gate.
//!
//! The debug routes MUTATE production state or expose per-stream data
//! (round-19). MF1 once found the documented gate missing; after it the
//! gate lived as a copy in each handler, which a new route could forget.
//! Here the gate is the mount: `gated` is the only way the debug table
//! reaches the router, so no route under /v1/debug is served past it.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;

use super::{AppState, authorized, err_resp};

/// The debug table behind the deployment bearer. The table gets its own
/// fallback because axum drops a nested router's default one: the layer
/// would then never see an unrouted path, and an anonymous probe could
/// tell routed from unrouted by 401 versus 404. With the token, an
/// unrouted path keeps the bare 404.
pub(super) fn gated(state: &Arc<AppState>, table: Router<Arc<AppState>>) -> Router<Arc<AppState>> {
    table
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(from_fn_with_state(state.clone(), require_deployment_bearer))
}

/// The deployment bearer, checked before method routing, extractors or
/// any handler run: a missing or wrong token is 401 on every debug path.
/// Off mode with no bearer configured stays open (SR-5 local development).
async fn require_deployment_bearer(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if !authorized(&state, request.headers()) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    next.run(request).await
}
