//! `POST {collection}/consumers/{consumer}:pull`, the queue delivery
//! page: authorizes the consumer, activates it and frames one batch of
//! leased messages (docs/refactor/WIRE-MATRIX.md §2.15). A batch is
//! decrypted record payload leaving the cell, so it draws on the
//! project's read-byte bucket like every page route: admitted at entry
//! while the bucket is not in debt, debited by the framed body served.
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use super::{check_read_quota, consumer_failure_response, consumer_key, debit_read_bytes, perr};
use crate::application::consumer::ConsumerAccess;
use crate::http::AppState;

/// The pull reply, serialised directly: through a `Value` each message's
/// stored record text would be parsed and re-encoded. The fields are in the
/// order the reply has always carried them.
#[derive(serde::Serialize)]
struct PullBody<'a> {
    backlog: u64,
    messages: &'a [crate::application::consumer::DeliveryMessage],
}

/// The read-quota admission precedes every other decision (key,
/// authorization, activation): a project in read debt is refused before
/// a lease is taken, so a refused pull never leases a message it will
/// not deliver.
#[expect(
    clippy::too_many_arguments,
    reason = "product_consumer_pull; the parameters are the request's typed context parts (resolved stream identity, consumer, headers, body, access), not tunables, and the reply serialises the stored records without a Value; a bundle struct for this single call site would only rename the same positional list"
)]
pub(super) async fn product_consumer_pull(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    cname: String,
    headers: HeaderMap,
    body: Bytes,
    access: ConsumerAccess<'_>,
) -> Response {
    let principal = match &access {
        ConsumerAccess::Account(p) => Some(*p),
        ConsumerAccess::Deployment => None,
    };
    if let Some(refusal) = check_read_quota(&state, principal) {
        return refusal;
    }
    let key = match consumer_key(&headers) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let service = state.consumer_service();
    let context = match service
        .authorize(
            &sref,
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
            let batch = match serde_json::to_string(&PullBody {
                backlog: out.backlog,
                messages: &out.messages,
            }) {
                Ok(batch) => batch,
                Err(e) => {
                    let message = e.to_string();
                    return perr(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &message,
                        None,
                        true,
                    );
                }
            };
            debit_read_bytes(&state, principal, batch.len());
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                batch,
            )
                .into_response()
        }
        Err(e) => consumer_failure_response(e),
    }
}
