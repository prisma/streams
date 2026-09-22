//! The fleet-internal receivers' registry prelude. A peer relays on behalf
//! of an incarnation it has already bound, so what it hears back must say
//! which of two different things happened: the name has no descriptor (the
//! incarnation is gone; the sender may cut over) or this instance could not
//! read its registry (nothing is known; the sender must try again). One 404
//! for both let a registry blip read as "gone" — the sealed-span SSE sender
//! made it `FatalSpanCutoff(IncarnationChanged)` and disconnected every
//! subscriber of a feed whose stream still existed (review item 30).
use axum::http::StatusCode;
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
