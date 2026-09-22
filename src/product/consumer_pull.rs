//! `POST {collection}/consumers/{consumer}:pull`, the queue delivery
//! page: authorizes the consumer, activates it and frames one batch of
//! leased messages (docs/refactor/WIRE-MATRIX.md §2.15).
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use serde_json::json;

use super::{consumer_failure_response, consumer_key, json_ok, perr};
use crate::http::AppState;

/// DLQ transition (spec §2.8): append the DLQ record to the configured
/// dead-letter stream with a producer identity derived from the message
/// id (crash-idempotent), and only after that is durable, ack the
/// source lease. No dead-letter stream configured -> the poison is
/// dropped by acking directly.
#[expect(
    clippy::too_many_arguments,
    reason = "product_consumer_pull; the parameters are the request's typed context parts, not tunables; a bundle struct for this single call site would only rename the same positional list"
)]
// mt-lint: allow(name-param-shared-core): verbatim move out of product.rs (ceilinged); the fix commit hands the resolved TenantStreamRef in from product_entry
pub(super) async fn product_consumer_pull(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    access: crate::application::consumer::ConsumerAccess<'_>,
) -> Response {
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            // mt-lint: allow(stream-ref-construction): verbatim move out of product.rs (ceilinged); the fix commit resolves the identity at the entry
            &tenant.stream_ref(&name),
            cname.clone(),
            &key,
            &access,
            crate::tenant::Scope::ConsumersPull,
        )
        .await
    {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let context = match service.active(context).await {
        Ok(c) => c,
        Err(e) => return consumer_failure_response(e),
    };
    let doc = if body.is_empty() {
        crate::application::consumer::PullInput::default()
    } else {
        match serde_json::from_slice::<crate::application::consumer::PullInput>(&body) {
            Ok(d) => d,
            Err(e) => {
                return perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_body",
                    &format!("pull request: {e}"),
                    None,
                    false,
                );
            }
        }
    };
    match crate::application::consumer::pull(context, doc).await {
        Ok(out) => {
            if !out.messages.is_empty() {
                crate::billing::meter_pull(
                    &state,
                    &out.descriptor,
                    out.payload_bytes,
                    out.messages.len() as u64,
                );
            }
            json_ok(&json!({"messages":out.messages,"backlog":out.backlog}))
        }
        Err(e) => consumer_failure_response(e),
    }
}
