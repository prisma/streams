//! `GET {collection}:scan`, the snapshot export page: resolves the
//! collection, decodes the frozen scan cursor and frames one page of
//! decrypted records (docs/refactor/WIRE-MATRIX.md §2.6). The entry
//! admits the read-byte quota; this render site debits the framed page.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;

use super::{
    READ_MAX_BYTES_CAP, SCAN_DEFAULT_BYTES, SCAN_TTL_MS, debit_read_bytes, perr, product_key,
    q_num, render_product_read_failure, strict_query,
};
use crate::http::AppState;

#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once each typed cursor verdict is answered and the page is debited; mapping either into a substitute response would report a wire status the handler never decided"
)]
#[expect(
    clippy::too_many_lines,
    reason = "product_scan; the scan resolves the collection, answers each typed cursor verdict, pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"
)]
pub(super) async fn product_scan(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    query: &str,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Response {
    let Some(key_b64) = product_key(&headers) else {
        return perr(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            None,
            false,
        );
    };
    let q = match strict_query(query, &["cursor", "maxBytes"]) {
        Ok(q) => q,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) if crate::http::desc_alive(&d) => {
            if crate::http::initializing(&d) {
                return perr(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                );
            }
            d
        }
        Ok(_) => {
            return perr(
                StatusCode::NOT_FOUND,
                "not_found",
                "stream not found",
                None,
                false,
            );
        }
        Err(e) => {
            return perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
                None,
                true,
            );
        }
    };
    let (skey, epoch) = match crate::http::check_key(Some(&key_b64), &desc) {
        crate::http::KeyCheck::Ok(k, e) => (k, e),
        crate::http::KeyCheck::Wrong => {
            return crate::audit::tag(
                perr(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "encryption key mismatch",
                    None,
                    false,
                ),
                "wrong_key",
            );
        }
        _ => {
            return perr(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Prisma-Encryption-Key required",
                None,
                false,
            );
        }
    };
    let now = crate::shard::now_ms();

    let sc = match q
        .get("cursor")
        .map(String::as_str)
        .filter(|c| !c.is_empty())
    {
        Some(c) => {
            match crate::product_cursor::ScanCursor::decode(c, &desc.project_id, &skey, &epoch, now)
            {
                Ok(sc) => Some(sc),
                Err(crate::product_cursor::ScanCursorError::Expired) => {
                    return perr(
                        StatusCode::GONE,
                        "scan_expired",
                        "scan snapshot expired; start a new scan",
                        None,
                        false,
                    );
                }
                Err(crate::product_cursor::ScanCursorError::WrongKind) => {
                    return perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_cursor",
                        "cursor is not a scan cursor",
                        None,
                        false,
                    );
                }
                Err(crate::product_cursor::ScanCursorError::Invalid) => {
                    return perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_cursor",
                        "invalid scan cursor",
                        None,
                        false,
                    );
                }
            }
        }
        None => None,
    };

    let max = match q_num::<usize>(&q, "maxBytes", "invalid_max_bytes") {
        Ok(v) => v
            .map(|v| v.clamp(4096, READ_MAX_BYTES_CAP))
            .unwrap_or(SCAN_DEFAULT_BYTES),
        Err(r) => return r,
    };
    let outcome = match state
        .read_service()
        .execute_scan(crate::application::read_scan::ScanCommand {
            descriptor: desc.clone(),
            key: skey.clone(),
            cursor: sc,
            max_bytes: max,
            now_ms: now,
            lifetime_ms: SCAN_TTL_MS,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => return render_product_read_failure(error),
    };
    let is_json = desc.is_json();
    let mut body = Vec::with_capacity(4096);
    body.push(b'[');
    for (index, record) in outcome.records.iter().enumerate() {
        if index > 0 {
            body.push(b',');
        }
        body.extend_from_slice(b"{\"routingKey\":");
        body.extend_from_slice(
            serde_json::to_string(&record.rkey)
                .expect("string serialization")
                .as_bytes(),
        );
        if is_json {
            body.extend_from_slice(b",\"value\":");
            body.extend_from_slice(&record.payload);
        } else {
            use base64::Engine;
            body.extend_from_slice(b",\"valueB64\":\"");
            body.extend_from_slice(
                base64::engine::general_purpose::STANDARD
                    .encode(&record.payload)
                    .as_bytes(),
            );
            body.push(b'"');
        }
        body.push(b'}');
    }
    body.push(b']');
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(cursor) = outcome.continuation {
        response = response.header(
            "Prisma-Next-Scan-Cursor",
            cursor.encode(&desc.project_id, &skey),
        );
    } else {
        response = response.header("Prisma-Scan-Complete", "true");
    }
    crate::billing::meter_read(
        &state,
        &desc,
        outcome
            .records
            .iter()
            .map(|record| record.payload.len() as u64)
            .sum(),
        outcome.records.len() as u64,
    );
    // The page is decided: charge the framed bytes the transport will send.
    debit_read_bytes(&state, principal, body.len());
    response.body(Body::from(body)).unwrap()
}
