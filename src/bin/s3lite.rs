//! s3lite: a minimal in-memory S3-compatible server with injected latency.
//!
//! Implements enough of the S3 REST API for both SlateDB (via the
//! `object_store` AWS client: conditional PUTs, range GETs, ListObjectsV2,
//! multipart uploads, batch delete) and the existing Prisma Streams R2 client
//! (plain PUT/GET/HEAD/DELETE/ListV2). Authorization headers are ignored.
//!
//! Every S3 operation sleeps `--latency-ms` (default 25) before executing to
//! emulate object-store round-trip latency. `GET /_s3lite/stats` (no latency)
//! reports op counts for PUT-amplification comparisons.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "s3lite")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9500")]
    listen: String,
    /// Injected latency per S3 operation, in milliseconds.
    #[arg(long, default_value_t = 25)]
    latency_ms: u64,
    /// Bench mode: PUTs whose key contains this substring AND ends in
    /// ".sst" are acknowledged but their bodies dropped (metadata kept,
    /// GET returns 500). Lets an in-memory emulator absorb an unbounded
    /// history tier during sustained runs. Never used in correctness tests.
    #[arg(long)]
    discard_substr: Option<String>,
}

#[derive(Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    last_modified: chrono::DateTime<chrono::Utc>,
    /// Original body length (equals data.len() unless discarded).
    orig_len: u64,
    discarded: bool,
}

#[derive(Default)]
struct Stats {
    put: AtomicU64,
    get: AtomicU64,
    head: AtomicU64,
    delete: AtomicU64,
    list: AtomicU64,
    multipart: AtomicU64,
    put_bytes: AtomicU64,
    get_bytes: AtomicU64,
    /// Physical-request ledger for cost A/Bs: (tier, kind, op) →
    /// per-status-bucket counts, cumulative since process start. `GET
    /// /_s3lite/stats2` renders it with a Class A/B/free rollup at
    /// public-Tigris-shaped billing rules (PUT/LIST/multipart billable
    /// Class A on 2xx; GET/HEAD billable Class B on 2xx; 304/404/412,
    /// deletes, and errors free).
    detailed: Mutex<HashMap<(&'static str, &'static str, &'static str), [u64; 6]>>,
}

const STATUS_BUCKETS: [&str; 6] = ["2xx", "304", "404", "412", "4xx", "5xx"];

fn status_index(status: StatusCode) -> usize {
    match status.as_u16() {
        304 => 1,
        404 => 2,
        412 => 3,
        s if (200..300).contains(&s) => 0,
        s if (400..500).contains(&s) => 4,
        _ => 5,
    }
}

/// Which tier of the system a key belongs to, from the fully-prefixed
/// object key (bucket/PATH_PREFIX/...). Substrings mirror
/// `store_timing::classify`, split further by tier: the shard log lives
/// under `shards/`, per-stream history under `streams/`.
fn tier_class(method: &Method, key: &str, query: &HashMap<String, String>) -> &'static str {
    if key.is_empty() && *method == Method::GET {
        // bucket-level list: classify by the prefix= it scans
        return match query.get("prefix") {
            Some(p) if p.contains("history2") => "hist",
            Some(p) if p.contains("shards/") => "shard",
            Some(p) if p.contains("streams/") => "hist",
            Some(p) if p.contains("fleet") || p.contains("routers") => "fleet",
            Some(p) if p.contains("registry") => "registry",
            _ => "other",
        };
    }
    // history2 lives UNDER the shard prefix (ownership travels with the
    // shard) — classify it as history, checked before the shards/ match.
    let tier = if key.contains("history2/") {
        "hist"
    } else if key.contains("shards/") {
        "shard"
    } else if key.contains("streams/") {
        "hist"
    } else if key.contains("fleet/") || key.contains("routers/") {
        "fleet"
    } else if key.contains("registry/") || key.ends_with("topology.json") {
        "registry"
    } else {
        "other"
    };
    tier
}

/// Object kind within the tier — the second classification axis.
fn kind_class(key: &str) -> &'static str {
    if key.contains("/wal/") || key.starts_with("wal/") {
        "wal"
    } else if key.contains("compaction") {
        "compactions"
    } else if key.contains("manifest") {
        "manifest"
    } else if key.contains("/compacted/") || key.ends_with(".sst") {
        "sst"
    } else {
        "meta"
    }
}

fn op_name(method: &Method, key_empty: bool, query: &HashMap<String, String>) -> &'static str {
    match (method.clone(), key_empty) {
        (Method::GET, true) => "list",
        (Method::POST, true) => "delete", // batch delete
        (Method::POST, false) | (Method::PUT, false)
            if query.contains_key("uploads") || query.contains_key("uploadId") =>
        {
            "multipart"
        }
        (Method::PUT, _) => "put",
        (Method::GET, false) => "get",
        (Method::HEAD, _) => "head",
        (Method::DELETE, _) => "delete",
        _ => "other",
    }
}

/// Billing rollup per (op, status bucket): 'A' = Class A, 'B' = Class B,
/// 'f' = free. Mirrors public Tigris pricing shape: writes and lists are
/// Class A when successful; reads Class B when they return data;
/// conditional/absent/failed responses and every delete are free.
fn billing(op: &'static str, status_idx: usize) -> char {
    if status_idx != 0 {
        return 'f';
    }
    match op {
        "put" | "multipart" | "list" => 'A',
        "get" | "head" => 'B',
        _ => 'f', // delete, other
    }
}

struct AppState {
    latency: Duration,
    discard_substr: Option<String>,
    objects: Mutex<BTreeMap<String, StoredObject>>,
    uploads: Mutex<HashMap<String, BTreeMap<u32, Bytes>>>,
    etag_counter: AtomicU64,
    upload_counter: AtomicU64,
    stats: Stats,
}

impl AppState {
    fn next_etag(&self) -> String {
        format!(
            "\"e{:016x}\"",
            self.etag_counter.fetch_add(1, Ordering::Relaxed)
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let state = Arc::new(AppState {
        latency: Duration::from_millis(args.latency_ms),
        discard_substr: args.discard_substr.clone(),
        objects: Mutex::new(BTreeMap::new()),
        uploads: Mutex::new(HashMap::new()),
        etag_counter: AtomicU64::new(1),
        upload_counter: AtomicU64::new(1),
        stats: Stats::default(),
    });

    let app = Router::new().fallback(handle).with_state(state);
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    eprintln!(
        "s3lite listening on {} (latency {}ms per op)",
        args.listen, args.latency_ms
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn query_map(uri: &Uri) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(q) = uri.query() {
        for pair in q.split('&') {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            out.insert(percent_decode(k, true), percent_decode(v, true));
        }
    }
    out
}

/// Percent-decode. `plus_is_space` applies to query strings only; in object
/// key paths a literal '+' stays '+'.
fn percent_decode(s: &str, plus_is_space: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' && plus_is_space {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn handle(
    State(state): State<Arc<AppState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let path = uri.path().to_string();

    // Stats endpoints bypass latency injection.
    if path == "/_s3lite/stats" {
        let s = &state.stats;
        let body = serde_json::json!({
            "put": s.put.load(Ordering::Relaxed),
            "get": s.get.load(Ordering::Relaxed),
            "head": s.head.load(Ordering::Relaxed),
            "delete": s.delete.load(Ordering::Relaxed),
            "list": s.list.load(Ordering::Relaxed),
            "multipart": s.multipart.load(Ordering::Relaxed),
            "put_bytes": s.put_bytes.load(Ordering::Relaxed),
            "get_bytes": s.get_bytes.load(Ordering::Relaxed),
            "objects": state.objects.lock().unwrap().len(),
        });
        return (
            [(header::CONTENT_TYPE, "application/json")],
            body.to_string(),
        )
            .into_response();
    }
    if path == "/_s3lite/stats2" {
        let detailed = state.stats.detailed.lock().unwrap();
        let mut cells = serde_json::Map::new();
        let (mut class_a, mut class_b, mut free) = (0u64, 0u64, 0u64);
        let mut rollup: HashMap<&'static str, [u64; 3]> = HashMap::new();
        let mut keys: Vec<_> = detailed.keys().collect();
        keys.sort();
        for k in keys {
            let (tier, kind, op) = *k;
            let counts = &detailed[k];
            let mut cell = serde_json::Map::new();
            for (i, bucket) in STATUS_BUCKETS.iter().enumerate() {
                if counts[i] > 0 {
                    cell.insert((*bucket).into(), counts[i].into());
                }
                let r = rollup.entry(tier).or_default();
                match billing(op, i) {
                    'A' => {
                        class_a += counts[i];
                        r[0] += counts[i];
                    }
                    'B' => {
                        class_b += counts[i];
                        r[1] += counts[i];
                    }
                    _ => {
                        free += counts[i];
                        r[2] += counts[i];
                    }
                }
            }
            cells.insert(format!("{tier}/{kind}/{op}"), cell.into());
        }
        let by_tier: serde_json::Map<String, serde_json::Value> = rollup
            .into_iter()
            .map(|(t, [a, b, f])| {
                (
                    t.to_string(),
                    serde_json::json!({"class_a": a, "class_b": b, "free": f}),
                )
            })
            .collect();
        // Live-object census: what the bucket holds RIGHT NOW, by
        // tier/kind — the direct gauge for GC retention (request cells
        // alone can't show what was never deleted).
        let mut live: HashMap<(&'static str, &'static str), u64> = HashMap::new();
        {
            let objects = state.objects.lock().unwrap();
            let no_query = HashMap::new();
            for key in objects.keys() {
                let k = key.splitn(2, '/').nth(1).unwrap_or(key);
                let tier = tier_class(&Method::PUT, k, &no_query);
                *live.entry((tier, kind_class(k))).or_default() += 1;
            }
        }
        let mut live_map = serde_json::Map::new();
        let mut live_keys: Vec<_> = live.keys().copied().collect();
        live_keys.sort();
        for (tier, kind) in live_keys {
            live_map.insert(format!("{tier}/{kind}"), live[&(tier, kind)].into());
        }
        let body = serde_json::json!({
            "cells": cells,
            "by_tier": by_tier,
            "total": {"class_a": class_a, "class_b": class_b, "free": free},
            "live_objects": live_map,
        });
        return (
            [(header::CONTENT_TYPE, "application/json")],
            body.to_string(),
        )
            .into_response();
    }

    tokio::time::sleep(state.latency).await;

    let query = query_map(&uri);
    // Path: /{bucket} or /{bucket}/{key...}
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), percent_decode(k, false)),
        None => (trimmed.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let full_key = format!("{bucket}/{key}");

    let tier = tier_class(&method, &key, &query);
    let kind = kind_class(&key);
    let op = op_name(&method, key.is_empty(), &query);
    let resp = dispatch(&state, method, &bucket, &key, &full_key, &query, headers, body).await;
    {
        let mut detailed = state.stats.detailed.lock().unwrap();
        detailed.entry((tier, kind, op)).or_default()[status_index(resp.status())] += 1;
    }
    resp
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    state: &Arc<AppState>,
    method: Method,
    bucket: &str,
    key: &str,
    full_key: &str,
    query: &HashMap<String, String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match (method.clone(), key.is_empty()) {
        // ---- bucket-level ----
        (Method::GET, true) => list_objects(&state, &bucket, &query),
        (Method::POST, true) if query.contains_key("delete") => {
            batch_delete(&state, &bucket, body).await
        }
        (Method::HEAD, true) => StatusCode::OK.into_response(),
        (Method::PUT, true) => StatusCode::OK.into_response(), // create bucket

        // ---- object-level ----
        (Method::POST, false) if query.contains_key("uploads") => {
            state.stats.multipart.fetch_add(1, Ordering::Relaxed);
            let id = format!(
                "u{:x}",
                state.upload_counter.fetch_add(1, Ordering::Relaxed)
            );
            state
                .uploads
                .lock()
                .unwrap()
                .insert(format!("{full_key}:{id}"), BTreeMap::new());
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                xml_escape(&bucket),
                xml_escape(&key),
                id
            );
            ([(header::CONTENT_TYPE, "application/xml")], xml).into_response()
        }
        (Method::POST, false) if query.contains_key("uploadId") => {
            complete_multipart(&state, &bucket, &key, &full_key, &query).await
        }
        (Method::PUT, false) if query.contains_key("uploadId") => {
            state.stats.multipart.fetch_add(1, Ordering::Relaxed);
            let part: u32 = query
                .get("partNumber")
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            let upload_id = query.get("uploadId").cloned().unwrap_or_default();
            let data = match axum::body::to_bytes(body, usize::MAX).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            let mut uploads = state.uploads.lock().unwrap();
            let Some(parts) = uploads.get_mut(&format!("{full_key}:{upload_id}")) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload", "upload not found");
            };
            parts.insert(part, data);
            let etag = state.next_etag();
            ([(header::ETAG, etag)], "").into_response()
        }
        (Method::DELETE, false) if query.contains_key("uploadId") => {
            let upload_id = query.get("uploadId").cloned().unwrap_or_default();
            state
                .uploads
                .lock()
                .unwrap()
                .remove(&format!("{full_key}:{upload_id}"));
            StatusCode::NO_CONTENT.into_response()
        }
        (Method::PUT, false) => put_object(&state, &full_key, &headers, body).await,
        (Method::GET, false) => get_object(&state, &full_key, &headers, false),
        (Method::HEAD, false) => get_object(&state, &full_key, &headers, true),
        (Method::DELETE, false) => {
            state.stats.delete.fetch_add(1, Ordering::Relaxed);
            state.objects.lock().unwrap().remove(full_key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn s3_error(status: StatusCode, code: &str, message: &str) -> Response {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
        code, message
    );
    (status, [(header::CONTENT_TYPE, "application/xml")], xml).into_response()
}

async fn put_object(
    state: &Arc<AppState>,
    full_key: &str,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    state.stats.put.fetch_add(1, Ordering::Relaxed);
    let data = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    state
        .stats
        .put_bytes
        .fetch_add(data.len() as u64, Ordering::Relaxed);

    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok());
    let if_match = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok());

    let mut objects = state.objects.lock().unwrap();
    let existing = objects.get(full_key);
    if if_none_match == Some("*") && existing.is_some() {
        return s3_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "object already exists",
        );
    }
    if let Some(expected) = if_match {
        match existing {
            Some(obj) if obj.etag == expected.trim() => {}
            _ => {
                return s3_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "etag mismatch",
                );
            }
        }
    }
    let etag = state.next_etag();
    let orig_len = data.len() as u64;
    let discard = state
        .discard_substr
        .as_deref()
        .map(|sub| full_key.contains(sub) && full_key.ends_with(".sst"))
        .unwrap_or(false);
    objects.insert(
        full_key.to_string(),
        StoredObject {
            data: if discard { Bytes::new() } else { data },
            etag: etag.clone(),
            last_modified: chrono::Utc::now(),
            orig_len,
            discarded: discard,
        },
    );
    ([(header::ETAG, etag)], "").into_response()
}

fn get_object(
    state: &Arc<AppState>,
    full_key: &str,
    headers: &HeaderMap,
    head_only: bool,
) -> Response {
    if head_only {
        state.stats.head.fetch_add(1, Ordering::Relaxed);
    } else {
        state.stats.get.fetch_add(1, Ordering::Relaxed);
    }
    let objects = state.objects.lock().unwrap();
    let Some(obj) = objects.get(full_key) else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchKey", "key not found");
    };
    if obj.discarded && !head_only {
        return s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "DiscardedObject",
            "body was discarded by --discard-substr",
        );
    };
    // Conditional GET: If-None-Match -> 304 (real S3 semantics; the
    // segmap TTL refresh depends on this being cheap).
    if let Some(inm) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if inm == obj.etag || inm.split(',').any(|t| t.trim() == obj.etag) {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, obj.etag.clone())
                .body(Body::empty())
                .unwrap();
        }
    }
    let total = if obj.discarded {
        obj.orig_len
    } else {
        obj.data.len() as u64
    };
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|r| parse_range(r, total));

    let (status, slice, content_range) = match range {
        Some((start, end)) => {
            let s = obj.data.slice(start as usize..(end + 1) as usize);
            (
                StatusCode::PARTIAL_CONTENT,
                s,
                Some(format!("bytes {start}-{end}/{total}")),
            )
        }
        None => (StatusCode::OK, obj.data.clone(), None),
    };
    if !head_only {
        state
            .stats
            .get_bytes
            .fetch_add(slice.len() as u64, Ordering::Relaxed);
    }

    let mut builder = Response::builder()
        .status(status)
        .header(header::ETAG, obj.etag.clone())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::LAST_MODIFIED,
            obj.last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        )
        .header(header::CONTENT_LENGTH, slice.len());
    if let Some(cr) = content_range {
        builder = builder.header(header::CONTENT_RANGE, cr);
    }
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(slice)
    };
    builder.body(body).unwrap()
}

fn parse_range(raw: &str, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    let spec = raw.strip_prefix("bytes=")?;
    let mut it = spec.splitn(2, '-');
    let start_s = it.next()?;
    let end_s = it.next()?;
    if start_s.is_empty() {
        // suffix range: bytes=-N
        let n: u64 = end_s.parse().ok()?;
        let n = n.min(total);
        return Some((total - n, total - 1));
    }
    let start: u64 = start_s.parse().ok()?;
    if start >= total {
        return None;
    }
    let end = if end_s.is_empty() {
        total - 1
    } else {
        end_s.parse::<u64>().ok()?.min(total - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

fn list_objects(state: &Arc<AppState>, bucket: &str, query: &HashMap<String, String>) -> Response {
    state.stats.list.fetch_add(1, Ordering::Relaxed);
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").cloned();
    let max_keys: usize = query
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let start_after = query
        .get("continuation-token")
        .or_else(|| query.get("start-after"))
        .cloned();

    let bucket_prefix = format!("{bucket}/");
    let full_prefix = format!("{bucket_prefix}{prefix}");

    let objects = state.objects.lock().unwrap();
    let mut contents: Vec<(String, &StoredObject)> = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut truncated = false;
    let mut next_token: Option<String> = None;

    for (k, obj) in objects.range(full_prefix.clone()..) {
        if !k.starts_with(&full_prefix) {
            break;
        }
        let rel = &k[bucket_prefix.len()..];
        if let Some(tok) = &start_after {
            if rel <= tok.as_str() {
                continue;
            }
        }
        if let Some(delim) = &delimiter {
            let after_prefix = &rel[prefix.len()..];
            if let Some(pos) = after_prefix.find(delim.as_str()) {
                let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delim);
                if common_prefixes.last() != Some(&cp) {
                    if contents.len() + common_prefixes.len() >= max_keys {
                        truncated = true;
                        next_token = Some(rel.to_string());
                        break;
                    }
                    common_prefixes.push(cp);
                }
                continue;
            }
        }
        if contents.len() + common_prefixes.len() >= max_keys {
            truncated = true;
            next_token = Some(
                contents
                    .last()
                    .map(|(k, _)| k.clone())
                    .unwrap_or_else(|| rel.to_string()),
            );
            break;
        }
        contents.push((rel.to_string(), obj));
    }

    let mut xml = String::with_capacity(1024);
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&prefix)));
    xml.push_str(&format!(
        "<KeyCount>{}</KeyCount>",
        contents.len() + common_prefixes.len()
    ));
    xml.push_str(&format!("<MaxKeys>{max_keys}</MaxKeys>"));
    xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if let Some(tok) = next_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(&tok)
        ));
    }
    for (k, obj) in &contents {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(k),
            obj.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            xml_escape(&obj.etag),
            obj.data.len()
        ));
    }
    for cp in &common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(cp)
        ));
    }
    xml.push_str("</ListBucketResult>");
    ([(header::CONTENT_TYPE, "application/xml")], xml).into_response()
}

async fn batch_delete(state: &Arc<AppState>, bucket: &str, body: Body) -> Response {
    state.stats.delete.fetch_add(1, Ordering::Relaxed);
    let data = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let text = String::from_utf8_lossy(&data);
    let mut deleted = Vec::new();
    let mut rest = text.as_ref();
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        let Some(end) = after.find("</Key>") else {
            break;
        };
        let key = &after[..end];
        let key = key
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&");
        state
            .objects
            .lock()
            .unwrap()
            .remove(&format!("{bucket}/{key}"));
        deleted.push(key.to_string());
        rest = &after[end + 6..];
    }
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    for k in &deleted {
        xml.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(k)));
    }
    xml.push_str("</DeleteResult>");
    ([(header::CONTENT_TYPE, "application/xml")], xml).into_response()
}

async fn complete_multipart(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    full_key: &str,
    query: &HashMap<String, String>,
) -> Response {
    state.stats.multipart.fetch_add(1, Ordering::Relaxed);
    let upload_id = query.get("uploadId").cloned().unwrap_or_default();
    let parts = state
        .uploads
        .lock()
        .unwrap()
        .remove(&format!("{full_key}:{upload_id}"));
    let Some(parts) = parts else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload", "upload not found");
    };
    let mut data = Vec::new();
    for (_, part) in parts {
        data.extend_from_slice(&part);
    }
    state
        .stats
        .put_bytes
        .fetch_add(data.len() as u64, Ordering::Relaxed);
    let etag = state.next_etag();
    let orig_len = data.len() as u64;
    let discard = state
        .discard_substr
        .as_deref()
        .map(|sub| full_key.contains(sub) && full_key.ends_with(".sst"))
        .unwrap_or(false);
    state.objects.lock().unwrap().insert(
        full_key.to_string(),
        StoredObject {
            data: if discard {
                Bytes::new()
            } else {
                Bytes::from(data)
            },
            etag: etag.clone(),
            last_modified: chrono::Utc::now(),
            orig_len,
            discarded: discard,
        },
    );
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Location>http://s3lite/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(&etag)
    );
    ([(header::CONTENT_TYPE, "application/xml")], xml).into_response()
}
