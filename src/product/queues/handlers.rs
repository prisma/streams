//! The queue routes' handlers: decode the request document, run the
//! workflow, render its answer (the HTTP contract of docs/QUEUES-DESIGN.md
//! §11).
use std::sync::Arc;

use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use super::super::AppState;
use super::{Resource, failure_response};
use crate::application::queues::{
    ClaimInput, CreateFailure, FailureClass, IdsInput, PageAt, QueueFailure, QueueService,
    QueueTarget, RedriveInput, SendBatchInput, SendInput, SettleInput, View, WakeupInput,
    decode_key, fail,
};
use crate::native_queue::QueueConfig;

/// One authenticated, admitted queue request.
pub(super) struct Request {
    sref: crate::tenant::TenantStreamRef,
    resource: Resource,
    method: Method,
    headers: HeaderMap,
    query: String,
    body: Bytes,
}

impl Request {
    pub(super) fn new(
        sref: crate::tenant::TenantStreamRef,
        resource: Resource,
        method: &Method,
        parts: axum::http::request::Parts,
        body: Bytes,
    ) -> Self {
        let query = parts.uri.query().unwrap_or("").to_string();
        Self {
            sref,
            resource,
            method: method.clone(),
            headers: parts.headers,
            query,
            body,
        }
    }
}

/// What a handler sees: the parsed request with its body read.
struct Call<'a> {
    svc: QueueService,
    sref: crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    query: String,
    body: Bytes,
    principal: Option<&'a crate::auth::RequestPrincipal>,
}

type Answer = Result<Response, QueueFailure>;

pub(super) async fn dispatch(
    state: &Arc<AppState>,
    r: Request,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Response {
    let call = Call {
        svc: state.queue_service(),
        sref: r.sref,
        headers: r.headers,
        query: r.query,
        body: r.body,
        principal,
    };
    let answer = match (r.resource, r.method) {
        (Resource::Queue, m) => queue(&call, &m).await,
        (Resource::Messages, _) => send(&call, false).await,
        (Resource::MessagesBatch, _) => send(&call, true).await,
        (Resource::Message(id), _) => message(&call, &id).await,
        (Resource::QueueVerb(verb) | Resource::GroupVerb(verb), _) => verb_call(&call, verb).await,
        (Resource::GroupView("wakeup"), m) => wakeup(&call, m == Method::PUT).await,
        (Resource::GroupView(view), _) => page(&call, view == "failed").await,
    };
    answer.unwrap_or_else(failure_response)
}

pub(super) fn json<T: Serialize>(status: StatusCode, value: &T) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => (
            status,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            body,
        )
            .into_response(),
        Err(e) => failure_response(fail(FailureClass::Internal, "internal", e.to_string())),
    }
}

/// The request document; an empty body is the type's default.
fn doc<T: DeserializeOwned + Default>(body: &Bytes) -> Result<T, QueueFailure> {
    if body.is_empty() {
        return Ok(T::default());
    }
    required(body)
}

fn required<T: DeserializeOwned>(body: &Bytes) -> Result<T, QueueFailure> {
    serde_json::from_slice(body).map_err(|e| {
        fail(
            FailureClass::Invalid,
            "invalid_body",
            format!("request document: {e}"),
        )
    })
}

impl Call<'_> {
    fn key_header(&self) -> Option<&str> {
        self.headers
            .get("prisma-encryption-key")
            .and_then(|v| v.to_str().ok())
    }
    async fn target(&self) -> Result<QueueTarget, QueueFailure> {
        self.svc.target(&self.sref).await
    }
    fn name(&self) -> &str {
        self.sref.name().as_str()
    }
}

async fn queue(c: &Call<'_>, method: &Method) -> Answer {
    match *method {
        Method::PUT => create(c).await,
        Method::GET => stats(c).await,
        Method::DELETE => {
            let t = c.target().await?;
            c.svc.delete(&t).await?;
            Ok(json(StatusCode::ACCEPTED, &json!({ "deleting": true })))
        }
        _ => {
            let t = c.target().await?;
            let patch = required::<serde_json::Value>(&c.body)?;
            let cfg = c.svc.update(&t, patch).await?;
            Ok(json(
                StatusCode::OK,
                &json!({ "name": c.name(), "config": cfg }),
            ))
        }
    }
}

async fn create(c: &Call<'_>) -> Answer {
    let key = decode_key(c.key_header())?;
    let cfg: QueueConfig =
        doc(&c.body).map_err(|f| fail(FailureClass::Invalid, "invalid_config", f.message))?;
    let quotas = c.principal.map(|p| &p.quotas);
    match c.svc.create(&c.sref, key, cfg, quotas).await {
        Ok((created, cfg)) => {
            let status = if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            Ok(json(status, &json!({ "name": c.name(), "config": cfg })))
        }
        Err(CreateFailure::Queue(f)) => Err(f),
        Err(CreateFailure::Quota(r)) => Ok(super::super::quota_refusal_response(&r)),
    }
}

/// `GET /v1/queues/{name}`: configuration and stats. A deleting queue
/// resumes its cleanup.
async fn stats(c: &Call<'_>) -> Answer {
    let t = c.target().await?;
    let s = c.svc.state(&t).await?;
    if s.deleting {
        c.svc.resume_cleanup(&t);
    }
    let open = s.leased.saturating_add(s.retrying);
    Ok(json(
        StatusCode::OK,
        &json!({
            "name": c.name(),
            "config": s.cfg,
            "deleting": s.deleting,
            "stats": {
                "due": s.queued_messages.saturating_sub(open),
                "scheduled": null,
                "leased": s.leased,
                "retrying": s.retrying,
                "failed": s.failed,
                "oldestDueAgeMs": null,
                "nextAvailableAt": null,
                "paused": s.paused,
                "retentionGap": { "count": s.gap_count },
                "queuedBytes": s.queued_bytes,
                "queuedMessages": s.queued_messages,
                "counters": s.counters,
            },
        }),
    ))
}

async fn send(c: &Call<'_>, batch: bool) -> Answer {
    let t = c.target().await?;
    let key = QueueService::key(&t, c.key_header())?;
    let inputs = if batch {
        required::<SendBatchInput>(&c.body)?.messages
    } else {
        vec![required::<SendInput>(&c.body)?]
    };
    let sent = c.svc.send(&t, &key, inputs).await?;
    if batch {
        return Ok(json(StatusCode::CREATED, &json!({ "results": sent })));
    }
    let status = if sent.iter().any(|s| s.duplicate) {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok(json(status, &sent.first()))
}

async fn message(c: &Call<'_>, id: &str) -> Answer {
    let t = c.target().await?;
    let key = QueueService::key(&t, c.key_header())?;
    Ok(json(StatusCode::OK, &c.svc.message(&t, &key, id).await?))
}

/// The `:verb` operations of the queue and of its default group.
async fn verb_call(c: &Call<'_>, verb: &str) -> Answer {
    let t = c.target().await?;
    let body = match verb {
        "claim" => {
            let key = QueueService::key(&t, c.key_header())?;
            let input: ClaimInput = doc(&c.body)?;
            value(&c.svc.claim(&t, &key, input).await?)?
        }
        "settle" => {
            let input: SettleInput = required(&c.body)?;
            json!({ "results": c.svc.settle(&t, input.ops).await? })
        }
        "pause" | "resume" => json!({ "paused": c.svc.pause(&t, verb == "pause").await? }),
        "redrive" => value(&c.svc.redrive(&t, doc::<RedriveInput>(&c.body)?).await?)?,
        "discard" | "cancel" => {
            let input: IdsInput = doc(&c.body)?;
            let n = c.svc.count_op(&t, &input.ids, verb == "cancel").await?;
            let field = if verb == "cancel" {
                "cancelled"
            } else {
                "discarded"
            };
            json!({ field: n })
        }
        _ => republish(c, &t).await?,
    };
    Ok(json(StatusCode::OK, &body))
}

async fn republish(c: &Call<'_>, t: &QueueTarget) -> Result<serde_json::Value, QueueFailure> {
    let key = QueueService::key(t, c.key_header())?;
    let input: IdsInput = doc(&c.body)?;
    let pairs = c.svc.republish(t, &key, &input.ids).await?;
    let results: Vec<_> = pairs
        .into_iter()
        .map(|(from, id)| json!({ "from": from, "id": id }))
        .collect();
    Ok(json!({ "results": results }))
}

fn value<T: Serialize>(v: &T) -> Result<serde_json::Value, QueueFailure> {
    serde_json::to_value(v).map_err(|e| fail(FailureClass::Internal, "internal", e.to_string()))
}

/// The failed (`failed`) or pending view: `?limit=&cursor=`.
async fn page(c: &Call<'_>, failed: bool) -> Answer {
    let t = c.target().await?;
    let key = QueueService::key(&t, c.key_header())?;
    let mut q = super::super::parse_query(&c.query);
    let limit = match q.remove("limit") {
        Some(l) => Some(l.parse::<usize>().map_err(|_| {
            fail(
                FailureClass::Invalid,
                "invalid_query",
                "limit is a positive integer",
            )
        })?),
        None => None,
    };
    let view = if failed { View::Failed } else { View::Pending };
    let at = PageAt {
        cursor: q.remove("cursor").filter(|c| !c.is_empty()),
        limit,
    };
    let page = c.svc.page(&t, &key, view, at).await?;
    Ok(json(StatusCode::OK, &page))
}

/// `PUT` (set, answering the secret once) or `DELETE` the default group's
/// wake-up subscription.
async fn wakeup(c: &Call<'_>, set: bool) -> Answer {
    let t = c.target().await?;
    if !set {
        c.svc.wakeup(&t, None).await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    let input: WakeupInput = required(&c.body)?;
    Ok(json(
        StatusCode::OK,
        &c.svc.wakeup(&t, Some(input.url)).await?,
    ))
}
