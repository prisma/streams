//! `POST /v1/projects/{project}/live:wait`: the multiplexed wait (layout 6A
//! package 2, docs/refactor/WIRE-MATRIX.md §2.24). One long-poll carries
//! every tail, consumer-group pull and watch a client waits on, so a client
//! waiting on twenty streams holds one connection, one place in the live
//! pool and one in its project's share, not twenty of each.
//!
//! The server keeps nothing between requests: every request carries its
//! whole subscription set and the client keeps every cursor. A request:
//!
//! 1. is admitted as a page read is (project admission, fair shedding,
//!    read debt, its body under the project's memory line), takes one
//!    subscription unit a watched stream (`admission::live_units`) and
//!    reserves its answer, `maxBytes`;
//! 2. resolves every subscription: one that fails (a stream the token's
//!    grants do not cover, a wrong key, a missing consumer or watch) is
//!    answered with its error at once, without failing the rest;
//! 3. parks (one place in the live pool and its project's share, however
//!    many streams it watches) and answers every subscription that is
//!    ready (tails with records past their cursor, groups whose walk
//!    leases, watches touched, and any subscription from `now`, whose
//!    cursor it pins); with none ready it waits on every watched stream's
//!    notifications, its reservation released, until one is ready or
//!    `waitMs` (25 s at most) passes, then takes back what its woken tails
//!    appended;
//! 4. answers 200 `{"results": [...]}` with an entry per answered
//!    subscription in request order (none at the deadline but its watches'
//!    cursors), meters one read operation for the answer on its first
//!    tail's stream, the records and bytes per tail, and one queue
//!    operation per non-empty lease batch.
//!
//! Tails share `maxBytes` evenly, so one hot stream cannot starve the
//! rest: a tail with more to read answers `upToDate: false` and is ready
//! again at once.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use serde_json::value::RawValue;
use tokio::time::Instant;

use super::AppState;
use super::{auth_failure_response, enforce_customer, perr, project_admission, with_product_cors};
use crate::admission::live_units::UnitRefusal;
use crate::application::consumer::ConsumerAccess;
use crate::application::watch::{Observation, WatchAccess};
use crate::auth::RequestPrincipal;
use crate::registry::StreamDesc;
use crate::tenant::{ProjectId, Scope};

mod cursor_memo;
mod group;
mod tail;
mod watch;
mod wire;

/// The route's path, for the router and for the admission controller's
/// data-plane test.
const PATH: &str = "/v1/projects/{project}/live:wait";
/// The largest body a request may send: 256 subscriptions of about 1 KiB.
const MAX_BODY: usize = 256 << 10;

/// A subscription's refusal: the answer its per-stream route would give.
type Refusal = Box<Response>;

pub(crate) fn routes() -> Router<Arc<AppState>> {
    Router::new().route(PATH, axum::routing::post(entry).options(preflight))
}

/// Whether `path` is served by the data plane: a stream route, or this
/// one. The in-flight survival bound and the idle release count both.
pub(crate) fn data_plane(path: &str) -> bool {
    path.starts_with("/v1/stream")
        || path
            .strip_prefix("/v1/projects/")
            .is_some_and(|rest| rest.ends_with("/live:wait"))
}

async fn preflight() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    let headers = response.headers_mut();
    headers.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("POST, OPTIONS"),
    );
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("authorization, content-type"),
    );
    headers.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("*"),
    );
    headers.insert("access-control-max-age", HeaderValue::from_static("600"));
    response
}

/// Every answer passes the denial observer once, as the product entry's do.
async fn entry(
    State(state): State<Arc<AppState>>,
    Path(project): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let route = req.uri().path().to_string();
    let method = req.method().clone();
    let resp = with_product_cors(admitted(&state, project, req).await);
    crate::audit::observe_denial(&state, &route, &method, &resp);
    resp
}

/// Authenticate, check the path names the request's project, admit it and
/// serve it.
async fn admitted(state: &Arc<AppState>, project: String, req: axum::extract::Request) -> Response {
    let headers = req.headers().clone();
    super::shadow_observe_request(state, req.uri().path(), req.method(), "", &headers);
    let principal = match authority(state, &headers) {
        Ok(principal) => principal,
        Err(refusal) => return *refusal,
    };
    let tenant = principal.as_ref().map_or_else(
        // mt-lint: allow(state-tenant-read): Off/Shadow single-tenant posture (Stage 5d) — enforce-mode requests always carry a principal
        || state.deployment.deployment_tenant().clone(),
        |p| p.project_id.clone(),
    );
    if ProjectId::new(&project).ok().as_ref() != Some(&tenant) {
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
            &tenant,
        );
    }
    let _slot = match project_admission(state, principal.as_ref()) {
        Ok(slot) => slot,
        Err(refusal) => return refusal,
    };
    let body = req.into_body();
    let (body, _buffered) = match state
        .admit_live_wait(principal.as_ref(), body, MAX_BODY)
        .await
    {
        Ok(admitted) => admitted,
        Err(refusal) => return *refusal,
    };
    let request = match wire::Request::parse(&body) {
        Ok(request) => request,
        Err(wire::Invalid::Body(message)) => {
            return perr(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                &message,
                None,
                false,
            );
        }
        Err(wire::Invalid::TooMany(n)) => return too_many(n),
    };
    let units = state.admission.live_units().take(
        principal.as_ref().map(|p| &p.project_id),
        request.subs.len() as u64,
        state.config.cli.project_share_k,
    );
    let _units = match units {
        Ok(units) => units,
        Err(refusal) => return units_refusal(refusal, &tenant),
    };
    if let Err(refusal) = state
        .reserve_live_answer(principal.as_ref(), request.max_bytes as u64)
        .await
    {
        return *refusal;
    }
    Serve {
        state,
        principal: principal.as_ref(),
        project: &tenant,
        deadline: Instant::now() + request.wait,
        max_bytes: request.max_bytes,
    }
    .serve(request.subs)
    .await
}

/// The request's verified principal under enforce, `None` for the
/// deployment bearer otherwise; `Err` the refusal.
fn authority(state: &AppState, headers: &HeaderMap) -> Result<Option<RequestPrincipal>, Refusal> {
    if state.auth.mode == crate::auth::AuthMode::Enforce {
        return enforce_customer(state, headers).map(Some).map_err(Box::new);
    }
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if state.bearer.authorizes(presented, state.auth.mode) {
        return Ok(None);
    }
    Err(Box::new(perr(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "bearer token required",
        None,
        false,
    )))
}

/// 429 `live_subscription_limit`: more subscriptions than one request
/// carries. Not retryable: the same request is refused again.
fn too_many(n: usize) -> Response {
    perr(
        StatusCode::TOO_MANY_REQUESTS,
        "live_subscription_limit",
        &format!(
            "a live wait carries at most {} subscriptions; this one carries {n}",
            wire::MAX_SUBS
        ),
        None,
        false,
    )
}

/// The units refusal: 429 `live_subscription_limit` for a project at its
/// share, 503 `subscription_capacity` for a full instance; both retryable,
/// as other requests release their units.
fn units_refusal(refusal: UnitRefusal, project: &ProjectId) -> Response {
    let (status, code, message, retry) = match refusal {
        UnitRefusal::Project => (
            StatusCode::TOO_MANY_REQUESTS,
            "live_subscription_limit",
            "the project's live waits already watch its share of this cell's streams; retry",
            "1",
        ),
        UnitRefusal::Instance => (
            StatusCode::SERVICE_UNAVAILABLE,
            "subscription_capacity",
            "instance at live-subscription capacity; retry another instance or later",
            "5",
        ),
    };
    let mut response = perr(status, code, message, None, true);
    response
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static(retry));
    crate::audit::tag_project(crate::audit::tag(response, code), project)
}

/// One admitted request.
struct Serve<'a> {
    state: &'a Arc<AppState>,
    principal: Option<&'a RequestPrincipal>,
    /// The request's verified project (the deployment tenant outside
    /// enforce), every subscription's namespace.
    project: &'a ProjectId,
    deadline: Instant,
    /// The answer's byte budget, shared by its tails.
    max_bytes: usize,
}

/// A resolved subscription.
enum Kind<'a> {
    Tail(tail::Tail),
    Group(group::Group),
    Watch(watch::Watch<'a>, Option<Result<Observation, Refusal>>),
}

struct Slot<'a> {
    index: usize,
    kind: Kind<'a>,
}

/// One answered subscription.
enum Answer {
    Tail(Box<tail::TailAnswer>),
    Group(group::GroupAnswer),
    Watch(serde_json::Value),
    Error(u16, serde_json::Value),
}

/// What woke a waiting request.
enum Woken {
    Signal,
    Watch(usize, Result<Observation, Refusal>),
}

impl<'a> Serve<'a> {
    async fn serve(self, subs: Vec<wire::Sub>) -> Response {
        let deadline = self.deadline;
        let ids: Vec<String> = subs.iter().map(|s| s.id.clone()).collect();
        let mut answers: Vec<Option<Answer>> = ids.iter().map(|_| None).collect();
        let mut slots = Vec::with_capacity(subs.len());
        for (index, sub) in subs.into_iter().enumerate() {
            let stream = sub.stream.clone();
            match self.prepare(sub).await {
                Ok(kind) => slots.push(Slot { index, kind }),
                Err(refusal) => {
                    crate::audit::observe_denial(self.state, &stream, &Method::POST, &refusal);
                    answers[index] = Some(error_answer(refusal).await);
                }
            }
        }
        let first_tail = slots.iter().find_map(|slot| match &slot.kind {
            Kind::Tail(t) => Some(t.descriptor().clone()),
            Kind::Group(_) | Kind::Watch(..) => None,
        });
        for slot in &mut slots {
            if let Kind::Tail(t) = &mut slot.kind {
                t.arm(self.state).await;
            }
        }
        // A multiplexed wait is a wait for its whole life: parked from here,
        // so its first walks and their waits for read memory never count
        // among the write gate's active requests; every read below takes
        // its reservation back first.
        let parked = crate::admission::park::park();
        loop {
            self.act(&mut slots, &mut answers).await;
            if answers.iter().any(Option::is_some) || Instant::now() >= deadline {
                break;
            }
            crate::admission::park::release_read();
            self.wait(&mut slots).await;
        }
        drop(parked);
        self.watch_cursors(&mut slots, &mut answers).await;
        self.render(&ids, answers, first_tail.as_ref())
    }

    /// Resolve one subscription; `Err` is its answer.
    async fn prepare(&self, sub: wire::Sub) -> Result<Kind<'a>, Refusal> {
        let name = super::canonical_name(&sub.stream).map_err(Box::new)?;
        // mt-lint: allow(stream-ref-construction): the multiplexed wait's ingress — each subscription names its stream in the body, resolved here once under the request's verified project, as the product entry resolves its path
        let sref = self.project.stream_ref(&name);
        match sub.spec {
            wire::Spec::Tail(spec) => {
                if let Some(p) = self.principal {
                    p.require(Scope::RecordsRead)
                        .and_then(|()| p.require_stream(&name))
                        .map_err(|e| Box::new(auth_failure_response(&e)))?;
                }
                let tail = tail::Tail::prepare(self.state, &sref, spec);
                Ok(Kind::Tail(tail.await?))
            }
            wire::Spec::Group(spec) => {
                let access = self.access();
                let group = group::Group::prepare(self.state, &access, sref, spec);
                Ok(Kind::Group(group.await?))
            }
            wire::Spec::Watch(spec) => {
                let access = match self.principal {
                    Some(p) => WatchAccess::AdmittedAccount(p),
                    None => WatchAccess::Deployment,
                };
                let service = watch::service(self.state);
                let watch = watch::Watch::prepare(&service, &sref, spec, access);
                Ok(Kind::Watch(watch.await?, None))
            }
        }
    }

    fn access(&self) -> ConsumerAccess<'a> {
        match self.principal {
            Some(p) => ConsumerAccess::Account(p),
            None => ConsumerAccess::Deployment,
        }
    }

    /// Answer every ready subscription and drop it from the wait. The
    /// parked request first takes back what its ready tails appended (no
    /// room by the deadline: its tails stay unanswered, as a woken
    /// long-poll's do).
    async fn act(&self, slots: &mut Vec<Slot<'a>>, answers: &mut [Option<Answer>]) {
        let deadline = self.deadline;
        let ready_tails = slots
            .iter()
            .filter(|slot| matches!(&slot.kind, Kind::Tail(t) if t.ready()))
            .count();
        let tails_room = ready_tails == 0 || {
            let appended = slots
                .iter()
                .filter_map(|slot| match &slot.kind {
                    Kind::Tail(t) if t.ready() => Some(t.appended()),
                    Kind::Tail(_) | Kind::Group(_) | Kind::Watch(..) => None,
                })
                .try_fold(0u64, |sum, measured| measured.map(|m| sum + m));
            crate::admission::park::resume_at_most(deadline, || appended).await
        };
        let share = (self.max_bytes / ready_tails.max(1)).max(wire::MIN_MAX_BYTES);
        for slot in slots.iter_mut() {
            let answer = match &mut slot.kind {
                Kind::Tail(t) if tails_room && t.ready() => self.read(t, share).await,
                Kind::Group(g) if g.ready() => self.walk(g, deadline).await,
                Kind::Watch(w, outcome) => self.observed(w, outcome).await,
                Kind::Tail(_) | Kind::Group(_) => None,
            };
            if answer.is_some() {
                answers[slot.index] = answer;
            }
        }
        slots.retain(|slot| answers[slot.index].is_none());
    }

    /// One tail read of at most `share` bytes. Nothing read: the tail
    /// re-arms, and one that cannot is answered `not_owner`.
    async fn read(&self, t: &mut tail::Tail, share: usize) -> Option<Answer> {
        match t.read(self.state, share).await {
            Ok(Some(answer)) => Some(Answer::Tail(Box::new(answer))),
            Ok(None) => {
                t.arm(self.state).await;
                t.elsewhere().then(unwatchable)
            }
            Err(refusal) => Some(error_answer(refusal).await),
        }
    }

    /// One group walk with its coverage reserved until the deadline; no
    /// room by then leaves the group unanswered. The walk first waits while
    /// its project is in read debt, as a waiting pull's does (the walk that
    /// leased nothing pays its coverage in `pull`).
    async fn walk(&self, g: &mut group::Group, deadline: Instant) -> Option<Answer> {
        use crate::admission::park::ReadReservation;
        if !crate::admission::park::out_of_read_debt(deadline).await {
            return None;
        }
        let reservation = crate::admission::park::reserve_read_until(g.coverage(), deadline).await;
        if matches!(reservation, ReadReservation::NoRoom) {
            return None;
        }
        let answer = match g.walk(self.state, &self.access()).await {
            Ok(Some(answer)) => Some(Answer::Group(answer)),
            Ok(None) => None,
            Err(refusal) => Some(error_answer(refusal).await),
        };
        drop(reservation);
        answer
    }

    /// A watch's answer: a wait that touched or resynchronised, a refusal,
    /// or, for a watch from `now`, the journal's cursor at once.
    async fn observed(
        &self,
        w: &watch::Watch<'a>,
        outcome: &mut Option<Result<Observation, Refusal>>,
    ) -> Option<Answer> {
        let observed = match outcome.take() {
            Some(observed) => observed,
            None if w.immediate() => w.observe(&watch::service(self.state), Duration::ZERO).await,
            None => return None,
        };
        match observed {
            Ok(o) if watch::ready(&o) || w.immediate() => {
                Some(Answer::Watch(super::watch_event(o)))
            }
            Ok(_) => None,
            Err(refusal) => Some(error_answer(refusal).await),
        }
    }

    /// Wait until a watched stream moves, a lease expires, a watch answers
    /// or the deadline passes. Every notification is registered before the
    /// subscriptions are looked at again, so none between is missed.
    async fn wait(&self, slots: &mut [Slot<'a>]) {
        let deadline = self.deadline;
        let service = watch::service(self.state);
        let woken = {
            let mut wakes = FuturesUnordered::new();
            for (i, slot) in slots.iter().enumerate() {
                wakes_of(&mut wakes, i, slot, &service, deadline);
            }
            if slots.iter().any(Slot::ready) {
                return;
            }
            tokio::select! {
                woken = wakes.next() => woken,
                () = tokio::time::sleep_until(deadline) => None,
            }
        };
        if let Some(Woken::Watch(i, observed)) = woken
            && let Kind::Watch(_, outcome) = &mut slots[i].kind
        {
            *outcome = Some(observed);
        }
    }

    /// At the end, every watch still waiting is answered with the journal's
    /// cursor now, as a per-stream wait's timeout is.
    async fn watch_cursors(&self, slots: &mut [Slot<'a>], answers: &mut [Option<Answer>]) {
        let service = watch::service(self.state);
        for slot in slots.iter_mut() {
            let Kind::Watch(w, outcome) = &mut slot.kind else {
                continue;
            };
            let observed = match outcome.take() {
                Some(o) => o,
                None => w.observe(&service, Duration::ZERO).await,
            };
            answers[slot.index] = Some(match observed {
                Ok(o) => Answer::Watch(super::watch_event(o)),
                Err(refusal) => error_answer(refusal).await,
            });
        }
    }

    /// The 200 answer: entries in request order, metered and debited. The
    /// answer's one read operation goes to its first tail entry's stream,
    /// or, with none, to its first tail's.
    fn render(
        &self,
        ids: &[String],
        answers: Vec<Option<Answer>>,
        first_tail: Option<&StreamDesc>,
    ) -> Response {
        let mut entries = Vec::new();
        let mut operation = 1;
        for (id, answer) in ids.iter().zip(answers) {
            let Some(answer) = answer else { continue };
            if let Answer::Tail(t) = &answer {
                self.meter(&t.descriptor, t.payload_bytes, t.record_count, operation);
                operation = 0;
            }
            entries.push(Entry::of(id, answer));
        }
        if let (1, Some(desc)) = (operation, first_tail) {
            self.meter(desc, 0, 0, 1);
        }
        let body = match serde_json::to_string(&Results { results: entries }) {
            Ok(body) => body,
            Err(e) => return internal(&e.to_string()),
        };
        super::debit_read_bytes(self.state, self.principal, body.len());
        let mut response = Response::new(Body::from(body));
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }

    fn meter(&self, desc: &StreamDesc, bytes: u64, records: u64, operations: u64) {
        self.state.billing.meter_read(
            &crate::billing::identity_of(self.state, desc),
            crate::billing::RowDelta {
                read_payload_bytes: bytes,
                read_records: records,
                read_operations: operations,
                ..Default::default()
            },
        );
    }
}

impl Slot<'_> {
    /// Whether the subscription may answer without waiting.
    fn ready(&self) -> bool {
        match &self.kind {
            Kind::Tail(t) => t.ready(),
            Kind::Group(g) => g.ready(),
            Kind::Watch(_, outcome) => outcome.is_some(),
        }
    }
}

type Wake<'w> = std::pin::Pin<Box<dyn Future<Output = Woken> + Send + 'w>>;

/// Register what wakes `slot`: its tail's commit notification, its group's
/// segment notifications and earliest lease deadline, or its watch's
/// observation until the deadline.
fn wakes_of<'w>(
    wakes: &mut FuturesUnordered<Wake<'w>>,
    i: usize,
    slot: &'w Slot<'_>,
    service: &'w crate::application::watch::WatchService,
    deadline: Instant,
) {
    let signal = |notified: tokio::sync::futures::Notified<'w>| -> Wake<'w> {
        Box::pin(async move {
            notified.await;
            Woken::Signal
        })
    };
    match &slot.kind {
        Kind::Tail(t) => {
            wakes.extend(t.handle().map(|h| signal(h.notify.notified())));
            if let Some(at) = t.poll_at() {
                wakes.push(Box::pin(async move {
                    tokio::time::sleep_until(at.min(deadline)).await;
                    Woken::Signal
                }));
            }
        }
        Kind::Group(g) => {
            wakes.extend(g.notified().into_iter().map(signal));
            if let Some(at) = g.lease_deadline() {
                wakes.push(Box::pin(async move {
                    tokio::time::sleep_until(at.min(deadline)).await;
                    Woken::Signal
                }));
            }
        }
        Kind::Watch(w, None) => {
            let left = deadline.saturating_duration_since(Instant::now());
            wakes.push(Box::pin(async move {
                Woken::Watch(i, w.observe(service, left).await)
            }));
        }
        Kind::Watch(_, Some(_)) => {}
    }
}

#[derive(serde::Serialize)]
struct Results<'a> {
    results: Vec<Entry<'a>>,
}

impl<'a> Entry<'a> {
    fn of(id: &'a str, answer: Answer) -> Self {
        match answer {
            Answer::Tail(t) => {
                let t = *t;
                Entry::Tail {
                    id,
                    records: t.records,
                    cursor: t.cursor,
                    up_to_date: t.up_to_date,
                    sealed: t.sealed,
                }
            }
            Answer::Group(g) => Entry::Group {
                id,
                backlog: g.backlog,
                messages: g.messages,
            },
            Answer::Watch(event) => Entry::Watch { id, event },
            Answer::Error(status, error) => Entry::Error { id, status, error },
        }
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum Entry<'a> {
    #[serde(rename_all = "camelCase")]
    Tail {
        id: &'a str,
        records: Box<RawValue>,
        cursor: String,
        up_to_date: bool,
        sealed: bool,
    },
    Group {
        id: &'a str,
        backlog: u64,
        messages: Box<RawValue>,
    },
    Watch {
        id: &'a str,
        event: serde_json::Value,
    },
    Error {
        id: &'a str,
        status: u16,
        error: serde_json::Value,
    },
}

/// A refusal as its subscription's answer: its status and the `error`
/// object its per-stream route would have answered.
async fn error_answer(refusal: Refusal) -> Answer {
    let status = refusal.status().as_u16();
    let bytes = axum::body::to_bytes(refusal.into_body(), 64 << 10)
        .await
        .unwrap_or_default();
    let error = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|mut v| v.get_mut("error").map(serde_json::Value::take))
        .unwrap_or_else(|| serde_json::json!({"code": "internal", "message": "unreadable refusal", "retryable": true}));
    Answer::Error(status, error)
}

/// A subscription whose stream this instance does not serve: retryable, so
/// a client waits on it per stream, where the fleet replays it.
fn not_owner() -> Response {
    perr(
        StatusCode::MISDIRECTED_REQUEST,
        "not_owner",
        "this instance does not serve the stream; wait on it per stream",
        None,
        true,
    )
}

fn unwatchable() -> Answer {
    Answer::Error(
        StatusCode::MISDIRECTED_REQUEST.as_u16(),
        serde_json::json!({
            "code": "not_owner",
            "message": "this instance does not serve the stream; wait on it per stream",
            "retryable": true,
        }),
    )
}

fn unsupported_format() -> Response {
    perr(
        StatusCode::BAD_REQUEST,
        "unsupported_format",
        "a live wait tails JSON streams; read this stream per stream",
        None,
        false,
    )
}

fn internal(message: &str) -> Response {
    perr(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        message,
        None,
        true,
    )
}

fn retry_unavailable(message: &str) -> Response {
    let mut response = perr(
        StatusCode::SERVICE_UNAVAILABLE,
        "ttl_renewal_unavailable",
        message,
        None,
        true,
    );
    response
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("1"));
    response
}

/// The live descriptor `sref` names, as a records read resolves it;
/// `Err` its refusal.
async fn live_descriptor(
    state: &AppState,
    sref: &crate::tenant::TenantStreamRef,
) -> Result<StreamDesc, Refusal> {
    let refusal = match state.registry.get(sref).await {
        Ok(Some(d)) if crate::application::creation::desc_alive(&d) && d.init.is_none() => {
            return Ok(d);
        }
        Ok(Some(d)) if crate::application::creation::desc_alive(&d) => perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            None,
            true,
        ),
        Ok(_) => perr(
            StatusCode::NOT_FOUND,
            "not_found",
            "stream not found",
            None,
            false,
        ),
        Err(e) => super::internal::DescriptorReadAnswer::descriptor_read_answer(&e),
    };
    Err(Box::new(refusal))
}

/// `key` checked against `desc`, as a records read checks its header.
fn stream_key(
    key: &str,
    desc: &StreamDesc,
) -> Result<(crate::crypto::StreamKey, [u8; 16]), Refusal> {
    let epoch = desc.epoch();
    match crate::crypto::StreamKey::from_b64(key) {
        Ok(k) if k.fingerprint(&epoch) == desc.key_fingerprint => Ok((k, epoch)),
        _ => Err(Box::new(crate::audit::tag(
            perr(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "encryption key mismatch",
                None,
                false,
            ),
            "wrong_key",
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_route_is_data_plane_and_nothing_else_under_projects_is() {
        assert!(super::data_plane("/v1/projects/p1/live:wait"));
        assert!(super::data_plane("/v1/streams/a"));
        assert!(super::data_plane("/v1/stream/a"));
        assert!(!super::data_plane("/v1/projects/p1/usage"));
        assert!(!super::data_plane("/v1/queues/q"));
    }
}
