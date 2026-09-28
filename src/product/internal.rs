//! The fleet-internal receivers' registry prelude. A peer relays on behalf
//! of an incarnation it has already bound, so what it hears back must say
//! which of two different things happened: the name has no descriptor (the
//! incarnation is gone; the sender may cut over) or this instance could not
//! read its registry (nothing is known; the sender must try again). One 404
//! for both let a registry blip read as "gone" — the sealed-span SSE sender
//! made it a fatal `IncarnationChanged` cutoff and disconnected every
//! subscriber of a feed whose stream still existed (review item 30).
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;

use super::perr;
use crate::registry::{Registry, StreamDesc};
use crate::tenant::TenantStreamRef;

/// 404 only for a positive `None`: an unreadable registry answers nothing
/// about the incarnation, so it is 503 `temporarily_unavailable`,
/// retryable. No liveness gating here: the incarnation check that follows
/// binds the request, and a dead descriptor whose epoch matches is still
/// the one the sender addressed.
#[expect(
    clippy::result_large_err,
    reason = "internal_desc; the three receivers answer the wire response this prelude decided, unchanged; a compact error would be rendered into that same response at each call site"
)]
pub(super) async fn internal_desc(
    registry: &Registry,
    sref: &TenantStreamRef,
) -> Result<StreamDesc, Response> {
    match registry.get(sref).await {
        Ok(Some(desc)) => Ok(desc),
        Ok(None) => Err(perr(
            StatusCode::NOT_FOUND,
            "not_found",
            "stream",
            None,
            false,
        )),
        Err(error) => Err(perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            &error.to_string(),
            None,
            true,
        )),
    }
}

/// How a product handler answers its own descriptor read that failed. A
/// stored descriptor that does not decode or validate is corruption and fails
/// closed: 500, not retryable, since a retry reads the same bytes. Any other
/// failure is the store's, before anything was read or written for the
/// request, so the client retries: 503 `temporarily_unavailable` with
/// `Retry-After: 1`, as an append's first read answers (edge changes #58 and
/// #64).
pub(super) trait DescriptorReadAnswer {
    fn descriptor_read_answer(&self) -> Response;
}

impl DescriptorReadAnswer for object_store::Error {
    fn descriptor_read_answer(&self) -> Response {
        if crate::registry::cache::is_corrupt_descriptor(self) {
            return perr(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &self.to_string(),
                None,
                false,
            );
        }
        let mut response = perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "stream metadata could not be read; retry",
            None,
            true,
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        response
    }
}
