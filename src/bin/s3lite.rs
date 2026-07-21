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

    // Stats endpoint bypasses latency injection.
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
            state.objects.lock().unwrap().remove(&full_key);
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
