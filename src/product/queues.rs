//! The `/v1/queues` surface (docs/QUEUES-DESIGN.md §11, the first
//! release's HTTP contract; edge change #186): route grammar, the same
//! authentication, project admission and tenancy as `/v1/streams` (one
//! queue is one stream of the queue kind), and the refusal of queue
//! streams on the stream surfaces. `application::queues` owns the
//! workflows; `handlers` renders them.
mod guard;
mod handlers;
mod overload;

pub(crate) use guard::{guard_raw, stream_refusal};

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;

use super::AppState;
use crate::application::queues::{FailureClass, QueueFailure, QueueService};
use crate::tenant::Scope;

/// The queue routes, merged into the service router. Each route folds the
/// `Streams-<Name>` header aliases like the stream product routes.
pub(crate) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/queues", crate::header_alias::aliased(any(list_entry)))
        .route(
            "/v1/queues/{*path}",
            crate::header_alias::aliased(any(queue_entry)),
        )
}

impl AppState {
    pub(crate) fn queue_service(self: &Arc<Self>) -> QueueService {
        // The shard sweepers sign wake-up notifications with the same root.
        if let Some(root) = self.catalog_cursor_key {
            crate::native_queue::wakeup::install_root(root);
        }
        QueueService {
            registry: self.registry.clone(),
            shards: self.shards.clone(),
            creation: self.creation_service(),
            work: self.runtime.request_work.clone(),
            touch: self.touch.clone(),
            wake_root: self.catalog_cursor_key,
        }
    }
}

/// The resource a queue path names, below the queue itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resource {
    Queue,
    /// `{name}:cancel`, `{name}:republish`.
    QueueVerb(&'static str),
    Messages,
    MessagesBatch,
    Message(String),
    /// `groups/default:<verb>`.
    GroupVerb(&'static str),
    /// `groups/default/<view>`: `failed`, `pending`, `wakeup`.
    GroupView(&'static str),
}

const QUEUE_VERBS: [&str; 2] = ["cancel", "republish"];
const GROUP_VERBS: [&str; 6] = ["claim", "settle", "pause", "resume", "redrive", "discard"];
const GROUP_VIEWS: [&str; 3] = ["failed", "pending", "wakeup"];

fn not_route(code: &str, message: &str) -> Response {
    super::perr(StatusCode::NOT_FOUND, code, message, None, false)
}

/// Split a queue path into the queue name and the resource. The route
/// grammar wins over a name that contains a resource's shape, as on the
/// stream surface.
pub(crate) fn parse(path: &str) -> Result<(String, Resource), Box<Response>> {
    let (name, resource) = if let Some((name, rest)) = path.rsplit_once("/groups/") {
        (name, group_resource(rest)?)
    } else if let Some((name, id)) = path
        .rsplit_once("/messages/")
        .filter(|(_, id)| !id.is_empty() && !id.contains('/'))
    {
        (name, Resource::Message(id.to_string()))
    } else if let Some(name) = path.strip_suffix("/messages:batch") {
        (name, Resource::MessagesBatch)
    } else if let Some(name) = path.strip_suffix("/messages") {
        (name, Resource::Messages)
    } else if let Some((name, verb)) = path
        .rsplit_once(':')
        .and_then(|(n, v)| QUEUE_VERBS.iter().find(|q| **q == v).map(|q| (n, *q)))
    {
        (name, Resource::QueueVerb(verb))
    } else {
        (path, Resource::Queue)
    };
    Ok((super::canonical_name(name).map_err(Box::new)?, resource))
}

fn group_resource(rest: &str) -> Result<Resource, Box<Response>> {
    let (group, tail) = match rest.find([':', '/']) {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    if group != "default" {
        return Err(Box::new(not_route(
            "group_not_found",
            "the first release has one group, `default`",
        )));
    }
    if let Some(verb) = tail.strip_prefix(':')
        && let Some(v) = GROUP_VERBS.iter().find(|v| **v == verb)
    {
        return Ok(Resource::GroupVerb(v));
    }
    if let Some(view) = tail.strip_prefix('/')
        && let Some(v) = GROUP_VIEWS.iter().find(|v| **v == view)
    {
        return Ok(Resource::GroupView(v));
    }
    Err(Box::new(not_route(
        "unknown_route",
        "no such queue group resource",
    )))
}

/// The scopes an operation demands. Queue scopes map onto the stream
/// scopes for the first release: send is an append; consume, manage and
/// inspect read and write records; create, delete and the queue's own
/// configuration and stats are the stream's lifecycle and metadata.
fn scopes(resource: &Resource, method: &Method) -> Option<&'static [Scope]> {
    const RW: &[Scope] = &[Scope::RecordsRead, Scope::RecordsAppend];
    Some(match (resource, method.clone()) {
        (Resource::Queue, Method::PUT) => &[Scope::Create],
        (Resource::Queue, Method::GET) => &[Scope::MetadataRead],
        (Resource::Queue, Method::DELETE) => &[Scope::LifecycleManage],
        (Resource::Messages | Resource::MessagesBatch, Method::POST) => &[Scope::RecordsAppend],
        (Resource::Queue, Method::PATCH)
        | (Resource::QueueVerb(_) | Resource::GroupVerb(_), Method::POST)
        | (Resource::Message(_), Method::GET)
        | (Resource::GroupView("failed" | "pending"), Method::GET)
        | (Resource::GroupView("wakeup"), Method::PUT | Method::DELETE) => RW,
        _ => return None,
    })
}

/// Authenticate as the stream product surface does: in enforce mode the
/// customer token, its scopes and its stream grant; otherwise the
/// deployment bearer. `None` is the deployment posture.
// mt-lint: allow(name-param-shared-core): the queue route's name at ingress, checked against the principal's stream grant only
fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
    demanded: &[Scope],
    name: Option<&str>,
) -> Result<Option<crate::auth::RequestPrincipal>, Box<Response>> {
    if state.auth.mode != crate::auth::AuthMode::Enforce {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if state.bearer.authorizes(presented, state.auth.mode) {
            return Ok(None);
        }
        return Err(Box::new(super::perr(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
            None,
            false,
        )));
    }
    let principal = super::enforce_customer(state, headers).map_err(Box::new)?;
    let checked = demanded
        .iter()
        .try_for_each(|s| principal.require(*s))
        .and_then(|()| name.map_or(Ok(()), |n| principal.require_stream(n)));
    if let Err(e) = checked {
        return Err(Box::new(crate::audit::tag_project(
            super::auth_failure_response(&e),
            &principal.project_id,
        )));
    }
    Ok(Some(principal))
}

fn preflight() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            ("access-control-allow-origin", "*"),
            (
                "access-control-allow-methods",
                "GET, PUT, POST, PATCH, DELETE, OPTIONS",
            ),
            (
                "access-control-allow-headers",
                "authorization, content-type, prisma-encryption-key, streams-encryption-key",
            ),
            ("access-control-max-age", "600"),
        ],
    )
        .into_response()
}

/// A queue refusal on the wire: the product error shape, the owner to
/// replay to when another instance owns the queue.
pub(crate) fn failure_response(f: QueueFailure) -> Response {
    let status = match f.class {
        FailureClass::Invalid => StatusCode::BAD_REQUEST,
        FailureClass::Denied => StatusCode::FORBIDDEN,
        FailureClass::Missing => StatusCode::NOT_FOUND,
        FailureClass::Conflict => StatusCode::CONFLICT,
        FailureClass::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        FailureClass::Exhausted => StatusCode::TOO_MANY_REQUESTS,
        FailureClass::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        FailureClass::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut r = super::perr(status, f.code, &f.message, None, f.retryable);
    if f.class == FailureClass::Denied {
        r = crate::audit::tag(r, "wrong_key");
    }
    if let Some(owner) = f.owner.and_then(|o| o.parse().ok()) {
        r.headers_mut().insert("streams-replay-to", owner);
    }
    r
}

/// `GET /v1/queues`: the project's queues.
async fn list_entry(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    req: Request,
) -> Response {
    let resp = if method == Method::OPTIONS {
        preflight()
    } else if method != Method::GET {
        super::perr(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "the queue list accepts GET",
            None,
            false,
        )
    } else {
        let query = req.uri().query().unwrap_or("").to_string();
        list(&state, &headers, &query).await
    };
    crate::audit::observe_denial(&state, "/v1/queues", &method, &resp);
    super::with_product_cors(resp)
}

async fn list(state: &Arc<AppState>, headers: &HeaderMap, query: &str) -> Response {
    let principal = match authenticate(state, headers, &[Scope::CatalogRead], None) {
        Ok(p) => p,
        Err(r) => return *r,
    };
    let _admission = match super::project_admission(state, principal.as_ref()) {
        Ok(g) => g,
        Err(r) => return r,
    };
    overload::judge(state, principal.as_ref());
    if let Some(refusal) = super::read_memory::blamed_refusal(state, principal.as_ref()) {
        return refusal;
    }
    let tenant = tenant_of(state, principal.as_ref());
    let after = super::parse_query(query).remove("cursor");
    match state.queue_service().list(&tenant, after).await {
        Ok((names, next)) => {
            let queues = names
                .into_iter()
                .filter(|n| {
                    principal
                        .as_ref()
                        .is_none_or(|p| p.require_stream(n).is_ok())
                })
                .map(|name| serde_json::json!({ "name": name }))
                .collect::<Vec<_>>();
            handlers::json(
                StatusCode::OK,
                &serde_json::json!({ "queues": queues, "next": next }),
            )
        }
        Err(f) => failure_response(f),
    }
}

/// The verified principal's project, or the deployment tenant in the
/// single-tenant postures.
fn tenant_of(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> crate::tenant::ProjectId {
    principal
        .map(|p| p.project_id.clone())
        // mt-lint: allow(state-tenant-read): Off/Shadow single-tenant posture (Stage 5d) — enforce-mode requests always carry a principal
        .unwrap_or_else(|| state.deployment.deployment_tenant().clone())
}

/// Everything under `/v1/queues/{*path}`; every response passes the
/// denial observer once.
async fn queue_entry(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    method: Method,
    req: Request,
) -> Response {
    let resp = queue_entry_inner(&state, &path, &method, req).await;
    crate::audit::observe_denial(&state, &path, &method, &resp);
    super::with_product_cors(resp)
}

async fn queue_entry_inner(
    state: &Arc<AppState>,
    path: &str,
    method: &Method,
    req: Request,
) -> Response {
    if method == Method::OPTIONS {
        return preflight();
    }
    let (name, resource) = match parse(path) {
        Ok(p) => p,
        Err(r) => return *r,
    };
    let Some(demanded) = scopes(&resource, method) else {
        return super::perr(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "this queue resource does not accept the method",
            None,
            false,
        );
    };
    let principal = match authenticate(state, req.headers(), demanded, Some(&name)) {
        Ok(p) => p,
        Err(r) => return *r,
    };
    let _admission = match super::project_admission(state, principal.as_ref()) {
        Ok(g) => g,
        Err(r) => return r,
    };
    overload::judge(state, principal.as_ref());
    if crate::billing::is_reserved_stream(&name) {
        return super::perr(
            StatusCode::FORBIDDEN,
            "reserved_stream",
            "names beginning with '_' are reserved for the system",
            None,
            false,
        );
    }
    let (parts, incoming) = req.into_parts();
    let body = match overload::body(state, principal.as_ref(), incoming).await {
        Ok(body) => body,
        Err(r) => return *r,
    };
    // mt-lint: allow(stream-ref-construction): ingress — the queue route's name qualified with the verified principal's project, as product.rs does
    let sref = tenant_of(state, principal.as_ref()).stream_ref(&name);
    let resp = handlers::dispatch(
        state,
        handlers::Request::new(sref, resource, method, parts, body),
        principal.as_ref(),
    )
    .await;
    match principal {
        Some(p) => crate::audit::tag_project(resp, &p.project_id),
        None => resp,
    }
}
