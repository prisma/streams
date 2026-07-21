//! edgesim: a minimal CDN edge simulator for GET request coalescing +
//! Cache-Control caching — the two behaviors the collapsible touch-wait
//! design relies on (Cloudflare/Fastly-style). Used by livebench to measure
//! origin-load reduction locally.
//!
//! Semantics:
//! - GET only. The cache/coalesce key is the full path+query (URL).
//! - Identical concurrent URLs collapse into ONE upstream request; the
//!   response fans out to every collapsed client.
//! - Responses with `max-age>0` are cached until expiry; `no-store` are not.
//! - `GET /_edge/stats` reports downstream vs upstream request counts.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use clap::Parser;
use tokio::sync::oneshot;

#[derive(Parser, Debug)]
#[command(name = "edgesim")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9600")]
    listen: String,
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    origin: String,
}

#[derive(Clone)]
struct CachedResp {
    status: u16,
    content_type: String,
    cache_control: String,
    body: Bytes,
}

struct AppState {
    origin: String,
    http: reqwest::Client,
    cache: Mutex<HashMap<String, (CachedResp, Instant)>>,
    in_flight: Mutex<HashMap<String, Vec<oneshot::Sender<CachedResp>>>>,
    downstream: AtomicU64,
    upstream: AtomicU64,
    cache_hits: AtomicU64,
    coalesced: AtomicU64,
}

fn max_age(cache_control: &str) -> u64 {
    cache_control
        .split(',')
        .filter_map(|p| p.trim().strip_prefix("max-age="))
        .filter_map(|v| v.parse().ok())
        .next()
        .unwrap_or(0)
}

fn respond(r: &CachedResp) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_GATEWAY))
        .header(header::CONTENT_TYPE, r.content_type.clone())
        .header(header::CACHE_CONTROL, r.cache_control.clone())
        .body(Body::from(r.body.clone()))
        .unwrap()
}

async fn handle(State(state): State<Arc<AppState>>, method: Method, uri: Uri) -> Response {
    let url = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string());

    if url == "/_edge/stats" {
        return (
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::json!({
                "downstream": state.downstream.load(Ordering::Relaxed),
                "upstream": state.upstream.load(Ordering::Relaxed),
                "cache_hits": state.cache_hits.load(Ordering::Relaxed),
                "coalesced": state.coalesced.load(Ordering::Relaxed),
            })
            .to_string(),
        )
            .into_response();
    }
    if method != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    state.downstream.fetch_add(1, Ordering::Relaxed);

    // Cache.
    if let Some((resp, expiry)) = state.cache.lock().unwrap().get(&url).cloned() {
        if Instant::now() < expiry {
            state.cache_hits.fetch_add(1, Ordering::Relaxed);
            return respond(&resp);
        }
    }

    // Coalesce: join an in-flight upstream request if one exists.
    let rx = {
        let mut inflight = state.in_flight.lock().unwrap();
        if let Some(waiters) = inflight.get_mut(&url) {
            state.coalesced.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            Some(rx)
        } else {
            inflight.insert(url.clone(), Vec::new());
            None
        }
    };
    if let Some(rx) = rx {
        return match rx.await {
            Ok(resp) => respond(&resp),
            Err(_) => StatusCode::BAD_GATEWAY.into_response(),
        };
    }

    // Leader: fetch from origin, fan out, maybe cache.
    state.upstream.fetch_add(1, Ordering::Relaxed);
    let result = state
        .http
        .get(format!("{}{}", state.origin, url))
        .send()
        .await;
    let resp = match result {
        Ok(r) => {
            let status = r.status().as_u16();
            let content_type = r
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let cache_control = r
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("no-store")
                .to_string();
            let body = r.bytes().await.unwrap_or_default();
            CachedResp {
                status,
                content_type,
                cache_control,
                body,
            }
        }
        Err(_) => CachedResp {
            status: 502,
            content_type: "application/json".into(),
            cache_control: "no-store".into(),
            body: Bytes::from_static(b"{\"error\":{\"code\":\"upstream\"}}"),
        },
    };
    let ttl = max_age(&resp.cache_control);
    if resp.status == 200 && ttl > 0 {
        state.cache.lock().unwrap().insert(
            url.clone(),
            (resp.clone(), Instant::now() + Duration::from_secs(ttl)),
        );
    }
    let waiters = state
        .in_flight
        .lock()
        .unwrap()
        .remove(&url)
        .unwrap_or_default();
    for tx in waiters {
        let _ = tx.send(resp.clone());
    }
    respond(&resp)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let state = Arc::new(AppState {
        origin: args.origin.clone(),
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(1024)
            .timeout(Duration::from_secs(40))
            .build()?,
        cache: Mutex::new(HashMap::new()),
        in_flight: Mutex::new(HashMap::new()),
        downstream: AtomicU64::new(0),
        upstream: AtomicU64::new(0),
        cache_hits: AtomicU64::new(0),
        coalesced: AtomicU64::new(0),
    });
    let app = Router::new().fallback(handle).with_state(state);
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    eprintln!("edgesim on {} -> {}", args.listen, args.origin);
    axum::serve(listener, app).await?;
    Ok(())
}
