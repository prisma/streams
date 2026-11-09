//! Forwarding a stream request to its stream's owner.
//!
//! A request that reaches an instance which does not own its stream's
//! shard is answered `409 not_ring_owner` with `Streams-Replay-To`, and a
//! router or the SDK replays it to the owner. When the fleet has published
//! the owner's URL, this layer sends the request there itself, once, and
//! answers with the owner's response, so a client that reaches any
//! instance of the fleet is served without a replay of its own. Waiting
//! reads and live feeds are forwarded like any other request: the forward
//! holds a connection on both instances while it waits.
//!
//! - **Fleet-off cells pay nothing.** The layer takes part only on stream
//!   routes of an instance whose peer table is not empty ([`engaged`]); a
//!   standalone cell has none, so its requests reach their handlers as they
//!   arrived, at the cost of one read of the empty table.
//! - **One hop.** The forwarded request carries `Streams-Forwarded`, and an
//!   instance never forwards a request that carries it: a second owner
//!   bounce is answered with its `409`, so ownership churn cannot build a
//!   relay cycle. A client that sends the header itself only opts out of
//!   forwarding.
//! - **The body is not buffered here.** The handler reads the request
//!   body through [`Deferred`], which takes it at the handler's first read.
//!   A handler that answers `409` without reading it leaves it in place,
//!   and the body is then streamed to the owner as it arrives, so
//!   admission, memory lines and the shed before a body is read all apply
//!   at the owner as they would at a first hop. [`Deferred`] keeps the
//!   frames it hands the handler, up to [`KEPT_BYTES`]: a body the handler
//!   read whole within that bound, as an extractor does before the handler
//!   runs, is forwarded from those frames, which share the handler's bytes.
//!   A handler that read a larger body, or part of one, keeps its `409`,
//!   and the client replays it. Every route that takes a body today (raw
//!   and product appends and creates) reads it whole before it resolves
//!   ownership, so a forward sends at most the 64 KiB already read, from
//!   memory: a client's upload is never inside the forward's answer bound.
//! - **What a failure means.** The owner's unreachable socket answers the
//!   original `409`, so the client can still replay, and so does a read
//!   (`GET`, `HEAD`) that fails or times out after it was sent: it has no
//!   side effect. Any other forward that fails after the request was sent
//!   may have been applied by the owner, so it is answered
//!   `503 forward_failed` with `Retry-After: 1`, never with the
//!   `409` a client would replay as new. It is not marked `retryable`: the
//!   SDK then treats a plain append's outcome as unknown, which it is.
//! - **Bounded.** The owner has 1 s to accept the connection
//!   (`peer::FORWARD_CONNECT`, one transport per instance); past it
//!   nothing was sent and the client gets the `409`. Its answer must begin
//!   within its shard-open wait, the request's own wait and 5 s
//!   ([`answer_bound`]); past that, `503 forward_failed`. Forwards waiting
//!   for their owners at once hold at most a quarter of the in-flight cap
//!   (`admission::forwards`); past it the client gets the `409`, so an
//!   owner that never answers cannot take the non-owner's capacity.
//! - **What the client sees.** The owner's status, headers and body, with
//!   `Streams-Forwarded-To` naming the owner, so a client can address it
//!   next time.

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::Response;
use hyper::body::Frame;

use super::AppState;

/// Marks a forwarded request: its receiver serves it or answers its
/// `409`, and never forwards it again.
pub(super) const FORWARDED: &str = "streams-forwarded";

/// Names the owner that served a forwarded request.
const FORWARDED_TO: &str = "streams-forwarded-to";

/// The most body bytes a request keeps for a forward once its handler has
/// read them: 64 KiB, sharing the handler's bytes; at most 256 MiB at the
/// default 4,096 requests in flight, under the RSS shed line.
const KEPT_BYTES: usize = 64 << 10;

/// How long, past the owner's shard-open wait and the request's own wait, a
/// forward waits for the owner's answer to begin. Past it the owner may
/// have applied the request: `503 forward_failed`.
const ANSWER_MARGIN: Duration = Duration::from_secs(5);

/// The request body, until the handler or a forward takes it, and the
/// frames the handler read.
#[derive(Default)]
struct Slot {
    body: Option<Body>,
    kept: Vec<Bytes>,
    kept_bytes: usize,
    /// The handler read more than [`KEPT_BYTES`], or a frame that is not
    /// data, so the frames it read cannot be sent again.
    overflowed: bool,
    /// The handler read the body to its end.
    ended: bool,
}

impl Slot {
    /// What a forward can send: the unread body, or the frames of a body
    /// read whole within the bound.
    fn replay(&mut self) -> Option<Body> {
        if let Some(body) = self.body.take() {
            return Some(body);
        }
        if self.ended && !self.overflowed {
            let frames = std::mem::take(&mut self.kept);
            return Some(Body::from_stream(futures_util::stream::iter(
                frames.into_iter().map(Ok::<_, std::convert::Infallible>),
            )));
        }
        None
    }

    fn keep(&mut self, frame: &Frame<Bytes>) {
        match frame.data_ref() {
            Some(data) if !self.overflowed && self.kept_bytes + data.len() <= KEPT_BYTES => {
                self.kept_bytes += data.len();
                self.kept.push(data.clone());
            }
            _ => {
                self.overflowed = true;
                self.kept = Vec::new();
            }
        }
    }
}

/// A request a forward sends: its method, target, headers, and body.
struct Held {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
}

/// The middleware: run the request here, and forward it once to the owner
/// when this instance answers that another owns its stream without having
/// read its body.
pub(super) async fn to_owner(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if !engaged(&state.peer, &request) {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let (method, uri, headers) = (
        parts.method.clone(),
        parts.uri.clone(),
        parts.headers.clone(),
    );
    let deferred = Deferred::new(body);
    let slot = Arc::clone(&deferred.slot);
    let answer = next
        .run(Request::from_parts(parts, Body::new(deferred)))
        .await;
    if answer.status() != StatusCode::CONFLICT {
        return answer;
    }
    let Some((owner, base)) = super::replay_peer_url(&state, &answer) else {
        return answer;
    };
    let Some(body) = lock(&slot).replay() else {
        return answer;
    };
    let Some(_waiting) = state.admission.enter_forward() else {
        return answer;
    };
    let held = Held {
        method,
        uri,
        headers,
        body,
    };
    forward(&state, held, (&owner, &base), answer).await
}

/// Whether the layer takes part in serving `request`: a request on a stream
/// route (`/v1/stream*`; the multiplexed wait answers each subscription
/// itself and is never forwarded), not forwarded already, on an instance
/// the fleet has given peers. A fleet-off cell, which has none, passes
/// every request to its handler untouched: no copies of its head, no
/// deferred body, no kept frames.
fn engaged(peers: &crate::peer::PeerClient, request: &Request) -> bool {
    request.uri().path().starts_with("/v1/stream")
        && !request.headers().contains_key(FORWARDED)
        && peers.has_peers()
}

fn lock(slot: &Mutex<Slot>) -> MutexGuard<'_, Slot> {
    // Each section moves the body or one frame in or out of the slot, and
    // nothing in it panics, so a poisoned slot is as the last section left
    // it.
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The handler's request body: the original, taken from the slot at the
/// first read, so that a handler which answers without reading leaves it
/// for a forward.
struct Deferred {
    slot: Arc<Mutex<Slot>>,
    taken: Option<Body>,
}

impl Deferred {
    fn new(body: Body) -> Self {
        Self {
            slot: Arc::new(Mutex::new(Slot {
                body: Some(body),
                ..Slot::default()
            })),
            taken: None,
        }
    }
}

impl hyper::body::Body for Deferred {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = &mut *self;
        if this.taken.is_none() {
            this.taken = lock(&this.slot).body.take();
        }
        let Some(body) = this.taken.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = Pin::new(body).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => lock(&this.slot).keep(frame),
            Poll::Ready(None) => lock(&this.slot).ended = true,
            Poll::Ready(Some(Err(_))) => lock(&this.slot).overflowed = true,
            Poll::Pending => {}
        }
        polled
    }
}

/// Send `held` to the owner `(name, base URL)`, once. `refused` is this
/// instance's own `409`, answered when the owner cannot be reached.
async fn forward(
    state: &AppState,
    held: Held,
    (owner, base): (&str, &str),
    refused: Response,
) -> Response {
    let Some(client) = state.peer.forward_client() else {
        return refused;
    };
    let open_wait = Duration::from_millis(state.config.shard.open_wait_ms);
    let bound = answer_bound(&held.uri, open_wait);
    let read = matches!(held.method, Method::GET | Method::HEAD);
    let target = held
        .uri
        .path_and_query()
        .map_or("/", |target| target.as_str());
    let mut request = client.request(held.method, format!("{base}{target}"));
    for (name, value) in &held.headers {
        if !hop_by_hop(name) {
            request = request.header(name, value);
        }
    }
    let request = request
        .header(FORWARDED, state.origin_marker.as_str())
        .body(reqwest::Body::wrap_stream(held.body.into_data_stream()));
    match tokio::time::timeout(bound, request.send()).await {
        Ok(Ok(answer)) => relayed(answer, owner),
        Ok(Err(error)) if error.is_connect() => {
            tracing::warn!(%owner, "forward to the stream's owner could not connect: {error}");
            refused
        }
        Ok(Err(error)) => {
            tracing::warn!(%owner, "forward to the stream's owner failed: {error}");
            unanswered(read, refused)
        }
        Err(_) => {
            tracing::warn!(%owner, "the stream's owner did not answer a forward within {bound:?}");
            unanswered(read, refused)
        }
    }
}

/// The answer to a forward the owner did not answer after it was sent: a
/// read (`GET`, `HEAD`) has no side effect, so its `409` is safe to replay;
/// anything else the owner may have applied, so the client is told to
/// retry, never to replay it as new.
fn unanswered(read: bool, refused: Response) -> Response {
    if read { refused } else { failed() }
}

/// A forward that failed after it was sent: the owner may have applied it,
/// so the client is told to retry, never to replay it as new.
fn failed() -> Response {
    let mut failed = super::err_resp(
        StatusCode::SERVICE_UNAVAILABLE,
        "forward_failed",
        "the stream's owner did not answer the forwarded request; retry",
    );
    failed
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    failed
}

/// How long the owner may take to begin its answer to a request for `uri`:
/// its shard-open wait, the request's own wait, and the margin.
fn answer_bound(uri: &Uri, open_wait: Duration) -> Duration {
    open_wait + requested_wait(uri) + ANSWER_MARGIN
}

/// The wait a request asks of its owner: a long-poll's `timeout` (raw) or
/// `waitMs`, a watch's `timeoutMs`, at most the longest any route grants
/// (25 s), and that longest when a waiting request names none (a consumer
/// pull carries its wait in its body). Any other request waits for
/// nothing, a live feed included: its owner answers the head at once.
fn requested_wait(uri: &Uri) -> Duration {
    let path = uri.path();
    let query = uri.query().unwrap_or("");
    let waits = query.split('&').any(|pair| pair == "live=long-poll")
        || path
            .rsplit_once(':')
            .is_some_and(|(_, verb)| matches!(verb, "long-poll" | "pull"))
        || (path.contains("/watches/") && path.contains("/keys/"));
    if !waits {
        return Duration::ZERO;
    }
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(name, value)| match name {
            "waitMs" | "timeoutMs" => value.parse().ok().map(Duration::from_millis),
            "timeout" => super::parse_duration(value),
            _ => None,
        })
        .map_or(super::MAX_LONG_POLL, |wait| wait.min(super::MAX_LONG_POLL))
}

/// The owner's answer as this instance's response, its body streamed.
fn relayed(answer: reqwest::Response, owner: &str) -> Response {
    let status = answer.status();
    let mut headers = HeaderMap::with_capacity(answer.headers().len() + 1);
    for (name, value) in answer.headers() {
        if !hop_by_hop(name) {
            headers.append(name.clone(), value.clone());
        }
    }
    if let Ok(owner) = HeaderValue::from_str(owner) {
        headers.insert(FORWARDED_TO, owner);
    }
    let mut response = Response::new(Body::from_stream(answer.bytes_stream()));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// The headers that describe one connection, not the request (RFC 9110
/// §7.6.1, with the proxy credentials and the host the client addressed):
/// never carried across a forward. `content-length` is carried: the body
/// passes through unchanged, and its declared length is what the receiver
/// checks.
fn hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "proxy-connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

#[cfg(test)]
mod tests {
    use super::{ANSWER_MARGIN, Body, Duration, FORWARDED, HeaderName, Request, answer_bound};
    use super::{engaged, hop_by_hop};

    /// The layer takes part only in a stream request not forwarded already,
    /// and only on an instance the fleet gave peers: a fleet-off
    /// cell passes every request to its handler untouched, with no copies
    /// and no deferred body.
    #[test]
    fn only_stream_requests_on_an_instance_with_peers_engage() {
        let request = |target: &str, forwarded: bool| {
            let mut request = Request::builder().uri(target);
            if forwarded {
                request = request.header(FORWARDED, "inst-c");
            }
            request.body(Body::empty()).expect("a request")
        };
        let targets = [
            ("/v1/stream/s", false),
            ("/v1/streams/a/records", false),
            ("/v1/containers/p/live:wait", false),
            ("/v1/stream/s", true),
            ("/v1/queues/q/messages", false),
            ("/health", false),
        ];
        let engaging = |peers: &crate::peer::PeerClient| -> Vec<bool> {
            targets
                .iter()
                .map(|(target, forwarded)| engaged(peers, &request(target, *forwarded)))
                .collect()
        };
        let peers = crate::peer::PeerClient::new(None, None);
        assert_eq!(engaging(&peers), [false; 6], "a fleet-off cell");
        peers.set_peer("inst-b", "http://inst-b:1");
        assert_eq!(
            engaging(&peers),
            [true, true, false, false, false, false],
            "an instance with a peer"
        );
    }

    /// A forward waits for the owner's shard open, the request's own wait
    /// (at most 25 s, all of it when the request names none), and 5 s.
    #[test]
    fn the_answer_bound_adds_the_requests_own_wait() {
        let open = Duration::from_secs(10);
        let bound = |target: &str| answer_bound(&target.parse().expect("a target"), open);
        let secs = |s: u64| open + ANSWER_MARGIN + Duration::from_secs(s);
        let millis = |ms: u64| open + ANSWER_MARGIN + Duration::from_millis(ms);
        let cases = [
            ("/v1/stream/s", secs(0)),
            ("/v1/stream/s?offset=-1&timeout=7s", secs(0)),
            ("/v1/stream/s?offset=-1&live=sse", secs(0)),
            ("/v1/stream/s?offset=-1&live=long-poll&timeout=7s", secs(7)),
            (
                "/v1/stream/s?offset=-1&live=long-poll&timeout=250ms",
                millis(250),
            ),
            ("/v1/stream/s?offset=-1&live=long-poll&timeout=2m", secs(25)),
            ("/v1/stream/s?offset=-1&live=long-poll", secs(25)),
            ("/v1/streams/a/records", secs(0)),
            ("/v1/streams/a/records:sse", secs(0)),
            ("/v1/streams/a/records:long-poll?waitMs=1500", millis(1500)),
            ("/v1/streams/a/records:long-poll?waitMs=99000", secs(25)),
            ("/v1/streams/a/consumers/c:pull", secs(25)),
            ("/v1/streams/a/watches/w/keys/k?timeoutMs=100", millis(100)),
            // A watch itself, or a collection named `keys`, waits for nothing.
            ("/v1/streams/a/watches/w?timeoutMs=100", secs(0)),
            ("/v1/streams/keys/k?timeoutMs=100", secs(0)),
        ];
        for (target, expected) in cases {
            assert_eq!(bound(target), expected, "{target}");
        }
    }

    /// RFC 9110 §7.6.1: the fields that describe one connection, and the
    /// host the client addressed, are never carried across a forward; the
    /// body's length and the request's credentials are.
    #[test]
    fn only_connection_fields_and_the_host_are_hop_by_hop() {
        let dropped = [
            "connection",
            "proxy-connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
            "host",
        ];
        let carried = [
            "content-length",
            "content-type",
            "authorization",
            "stream-encryption-key",
            "prisma-encryption-key",
            "streams-replay-to",
            "stream-next-offset",
        ];
        let named = |names: &[&'static str]| -> Vec<(String, bool)> {
            names
                .iter()
                .map(|name| (name.to_string(), hop_by_hop(&HeaderName::from_static(name))))
                .collect()
        };
        assert_eq!(
            named(&dropped),
            dropped.map(|name| (name.to_string(), true)).to_vec()
        );
        assert_eq!(
            named(&carried),
            carried.map(|name| (name.to_string(), false)).to_vec()
        );
    }
}
