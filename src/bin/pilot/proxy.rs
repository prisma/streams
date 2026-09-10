//! Pilot proxy routing, replay and response attribution.

use super::{
    Lb, UpStat, collection_name, eject_ms, name_hash, now_ms, pick, shard_for, tracing_warn_once,
};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::TryStreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl Lb {
    #[expect(
        clippy::unwrap_used,
        reason = "Pilot proxy routing owns the fleet snapshot; active names, topology and overrides must be coherent; recovering a poisoned publication could route requests through partial state"
    )]
    fn select(&self, stream: Option<&str>, now: u64) -> usize {
        let (active, shard, override_to) = {
            let f = self.fleet.lock().unwrap();
            let shard = stream.map(|st| shard_for(&f.topology, &name_hash(st)));
            let mut active = if f.active.is_empty() {
                vec!["streams-1".to_string()]
            } else {
                f.active.clone()
            };
            // Locally ejected ordinals (an unmarked platform response within
            // the eject window) are removed from the routing set NOW —
            // round-19 MF4: heartbeat-dark detection takes ~30 s, and every
            // request routed there in the meantime is a client-visible
            // failure. Never eject the last candidate: some upstream must
            // remain so a fully-ejected fleet still produces a real answer
            // (and its own retryable error) rather than a routing panic.

            let live: Vec<String> = active
                .iter()
                .filter(|n| {
                    n.strip_prefix("streams-")
                        .and_then(|o| o.parse::<usize>().ok())
                        .and_then(|o| o.checked_sub(1))
                        .and_then(|i| self.stats.get(i))
                        .map(|st| st.eject_until_ms.load(Ordering::Relaxed) <= now)
                        .unwrap_or(true)
                })
                .cloned()
                .collect();
            if !live.is_empty() {
                active = live;
            }
            let ov = shard.as_deref().and_then(|sh| f.overrides.get(sh)).cloned();
            (active, shard, ov)
        };
        // Ownership mirrors the servers' effective_owner: a rebalancer
        // override whose target is active wins; otherwise rendezvous over
        // instance NAMES from the live-filtered active set — the identical
        // computation the servers run for their R2 check. Nameless
        // (registry-scoped) requests pin to the first active.
        let chosen: &str = match (&override_to, &shard) {
            (Some(t), _) if active.iter().any(|a| a == t) => t,
            (_, Some(sh)) => &active[pick(sh, &active)],
            _ => &active[0],
        };
        chosen
            .strip_prefix("streams-")
            .and_then(|n| n.parse::<usize>().ok())
            .and_then(|n| n.checked_sub(1))
            .filter(|n| *n < self.stats.len())
            .unwrap_or(0)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Pilot proxy forwarding owns the upstream URL snapshot; its ordinal indices correspond to the fixed stats vector; recovering a poisoned URL publication could send a request to stale partial state"
    )]
    fn upstream_url(&self, index: usize) -> String {
        self.upstreams.read().unwrap()[index].clone()
    }
}

/// The buffered wire request reused unchanged across ownership replays.
struct ForwardRequest {
    target: String,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
}

impl ForwardRequest {
    async fn buffer(request: Request) -> Result<Self, axum::Error> {
        let (parts, body) = request.into_parts();
        let mut headers = HeaderMap::new();
        for (name, value) in &parts.headers {
            if !matches!(
                name.as_str(),
                "host" | "connection" | "content-length" | "transfer-encoding"
            ) {
                headers.insert(name.clone(), value.clone());
            }
        }
        let query = parts
            .uri
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let target = format!("{}{query}", parts.uri.path());
        // Match the service's 32 MiB request limit on every replay.
        let body = axum::body::to_bytes(body, 32 * 1024 * 1024).await?;
        Ok(Self {
            target,
            method: parts.method,
            headers,
            body,
        })
    }

    async fn send(
        &self,
        lb: &Lb,
        http: &reqwest::Client,
        index: usize,
    ) -> reqwest::Result<reqwest::Response> {
        http.request(
            self.method.clone(),
            format!("{}{}", lb.upstream_url(index), self.target),
        )
        .headers(self.headers.clone())
        .body(self.body.clone())
        .send()
        .await
    }
}

fn replay_target(response: &reqwest::Response, count: usize) -> Option<usize> {
    if response.status() != StatusCode::CONFLICT {
        return None;
    }
    response
        .headers()
        .get("streams-replay-to")
        .and_then(|value| value.to_str().ok())
        .and_then(|name| name.strip_prefix("streams-"))
        .and_then(|ordinal| ordinal.parse::<usize>().ok())
        .and_then(|ordinal| ordinal.checked_sub(1))
        .filter(|index| *index < count)
}

impl UpStat {
    fn eject_for(&self, now: u64, duration: u64) {
        self.eject_until_ms
            .store(now.saturating_add(duration), Ordering::Relaxed);
    }

    fn record_served(&self, micros: u64, idle_ms: u64) {
        self.reqs.fetch_add(1, Ordering::Relaxed);
        self.window.fetch_add(1, Ordering::Relaxed);
        self.last_us.store(micros, Ordering::Relaxed);
        let previous = self.ewma_us.load(Ordering::Relaxed);
        let next = if previous == 0 {
            micros
        } else {
            u64::try_from((u128::from(previous) * 9 + u128::from(micros)) / 10).unwrap_or(u64::MAX)
        };
        self.ewma_us.store(next, Ordering::Relaxed);
        if idle_ms > 8000 && micros > 1_500_000 {
            self.cold_starts.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE,
     [("retry-after", "1"), ("cache-control", "no-store")],
     axum::Json(serde_json::json!({"error": {
         "code": "upstream_unavailable", "message": "the serving instance is unavailable; retry", "retryable": true,
     }}))).into_response()
}

fn relay(response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let mut out = Response::new(Body::from_stream(
        response.bytes_stream().map_err(std::io::Error::other),
    ));
    *out.status_mut() = status;
    for (name, value) in &headers {
        if !matches!(name.as_str(), "connection" | "transfer-encoding") {
            out.headers_mut().append(name.clone(), value.clone());
        }
    }
    out
}

pub(super) async fn proxy(State(lb): State<Arc<Lb>>, request: Request) -> Response {
    let path = request.uri().path();
    let stream = collection_name(path);
    if stream.is_none()
        && !(path == "/v1/streams"
            || path == "/v1/segments"
            || path.starts_with("/v1/segments/")
            || path == "/health"
            || path.starts_with("/v1/debug"))
    {
        return (StatusCode::NOT_FOUND, "lb: not a stream route").into_response();
    }
    let mut current = lb.select(stream.as_deref(), now_ms());
    let Ok(request) = ForwardRequest::buffer(request).await else {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    };
    let started = Instant::now();
    let http = lb.http.get();
    let mut response = request.send(&lb, &http, current).await;
    let mut follows = 0;
    while let Some(target) = response
        .as_ref()
        .ok()
        .and_then(|r| replay_target(r, lb.stats.len()))
    {
        lb.stats[current]
            .last_seen_ms
            .store(now_ms(), Ordering::Relaxed);
        lb.stats[current].replays.fetch_add(1, Ordering::Relaxed);
        if follows >= 2 {
            break;
        }
        if follows == 1 {
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
        current = target;
        follows += 1;
        response = request.send(&lb, &http, current).await;
    }
    let stat = &lb.stats[current];
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let now = now_ms();
    let idle_ms = now.saturating_sub(stat.last_seen_ms.load(Ordering::Relaxed));
    stat.last_seen_ms.store(now, Ordering::Relaxed);
    match response {
        Ok(response) if response.headers().contains_key("prisma-streams-origin") => {
            stat.record_served(micros, idle_ms);
            relay(response)
        }
        Ok(_) => {
            stat.unmarked.fetch_add(1, Ordering::Relaxed);
            stat.eject_for(now_ms(), eject_ms());
            unavailable()
        }
        Err(error) => {
            stat.errs.fetch_add(1, Ordering::Relaxed);
            stat.eject_for(now_ms(), eject_ms());
            tracing_warn_once(&error);
            unavailable()
        }
    }
}

#[cfg(test)]
#[path = "proxy/tests.rs"]
mod tests;
