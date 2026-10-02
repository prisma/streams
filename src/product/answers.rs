//! The product's JSON success answers that no cache may store and no handler
//! module of their own builds: the settle's (WIRE-MATRIX §2.16), the watch
//! definitions' (§2.17) and the watch wait's (§2.18). `json_ok`
//! (`src/product.rs`) builds the same 200 without `Cache-Control` for the
//! answers the matrix lists without it: the two usage routes (§2.19, §2.21)
//! and the fleet-internal receivers.
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

/// 200 with `value` as JSON and `Cache-Control: no-store`: the settle's
/// outcome and the watch definitions.
pub(super) fn json_ok_no_store(value: &serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        value.to_string(),
    )
        .into_response()
}

/// A watch wait's observation: `json_ok_no_store`'s answer that also keeps
/// the request's URL, which may carry the wait's capability (`?cap=`), out of
/// any `Referer` (`Referrer-Policy: no-referrer`).
pub(super) fn watch_observation_response(value: &serde_json::Value) -> Response {
    (
        [(header::REFERRER_POLICY, "no-referrer")],
        json_ok_no_store(value),
    )
        .into_response()
}
