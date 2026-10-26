//! The product's JSON success answers that no cache may store and no handler
//! module of their own builds: the settle's (WIRE-MATRIX §2.16), the watch
//! definitions' (§2.17) and the watch wait's (§2.18). `json_ok`
//! (`src/product.rs`) builds the same 200 without `Cache-Control` for the
//! answers the matrix lists without it: the two usage routes (§2.19, §2.21)
//! and the fleet-internal receivers. `retry_unavailable` gives every product
//! 503 `temporarily_unavailable` its retry hint. `watch_event` is the watch
//! wait's body, which a multiplexed wait's watch entry carries too (§2.24).
use axum::http::{HeaderValue, StatusCode, header};
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

/// A product 503 `temporarily_unavailable` asks for a retry after one second
/// unless its answer already states a delay (edge changes #64 and #178): the
/// SDK rides through a restart only on a 503 that carries `Retry-After`
/// (#177). Any other answer passes unchanged.
pub(super) fn retry_unavailable(code: &str, mut response: Response) -> Response {
    if code == "temporarily_unavailable" && response.status() == StatusCode::SERVICE_UNAVAILABLE {
        let headers = response.headers_mut();
        headers
            .entry(header::RETRY_AFTER)
            .or_insert(HeaderValue::from_static("1"));
    }
    response
}

/// The watch wait's body for `observation` (WIRE-MATRIX §2.18): touched
/// (`changed`, or `resync` when the touch is unproven), resynchronised, or
/// timed out, each with the journal's cursor to wait from next.
pub(super) fn watch_event(
    observation: crate::application::watch::Observation,
) -> serde_json::Value {
    use crate::application::watch::Observation;
    use serde_json::json;
    match observation {
        Observation::Touched {
            cursor,
            proven,
            stream_cursor,
        } => json!({
            "invalidated": true, "reason": if proven { "changed" } else { "resync" },
            "cursor": cursor, "streamCursor": stream_cursor,
        }),
        Observation::Stale { cursor } => {
            json!({"invalidated": true, "reason": "resync", "cursor": cursor})
        }
        Observation::Timeout {
            cursor,
            stream_cursor,
        } => json!({"invalidated": false, "cursor": cursor, "streamCursor": stream_cursor}),
    }
}

#[cfg(test)]
mod tests {
    use axum::response::Response;

    use crate::application::append::{AppendCode, AppendFailure, FailureClass};
    use crate::application::read::ReadFailure;
    use crate::application::read_remote::RemoteSpanError;
    use crate::shard_directory::ResolveError;

    async fn wire(response: Response) -> (u16, Option<String>, String) {
        let status = response.status().as_u16();
        let retry = response.headers().get("retry-after");
        let retry = retry.map(|v| v.to_str().unwrap().to_string());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await;
        let body: serde_json::Value = serde_json::from_slice(&body.unwrap()).unwrap();
        (
            status,
            retry,
            body["error"]["code"].as_str().unwrap().to_string(),
        )
    }

    /// Edge change #178's header is for a 503 `temporarily_unavailable`
    /// alone: a 503 with any other code (`outcome_unknown` above all, which
    /// the SDK must never resend) and a `temporarily_unavailable` under any
    /// other status pass unchanged.
    #[test]
    fn only_a_503_temporarily_unavailable_gains_retry_after() {
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        let answer = |status: StatusCode| (status, "{}").into_response();
        let retry = |r: &Response| {
            r.headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap().to_string())
        };
        let cases = [
            (
                "temporarily_unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                Some("1"),
            ),
            ("outcome_unknown", StatusCode::SERVICE_UNAVAILABLE, None),
            (
                "temporarily_unavailable",
                StatusCode::INTERNAL_SERVER_ERROR,
                None,
            ),
            ("outcome_unknown", StatusCode::INTERNAL_SERVER_ERROR, None),
        ];
        for (code, status, want) in cases {
            let got = super::retry_unavailable(code, answer(status));
            assert_eq!(
                (code, status.as_u16(), retry(&got).as_deref()),
                (code, status.as_u16(), want)
            );
        }
    }

    /// Edge change #178: every product read 503 `temporarily_unavailable`
    /// (a shard opening or failing to open here, a peer read that failed)
    /// carries `Retry-After: 1`, so the SDK's restart ride-through (#177)
    /// covers reads as it covers appends.
    #[tokio::test]
    async fn a_read_temporarily_unavailable_asks_for_a_retry_after_one_second() {
        let opening = ResolveError::Opening {
            prefix: "000".into(),
            code: "shard_opening",
            retry_after_secs: 3,
        };
        let open_failed = ResolveError::OpenFailed {
            prefix: "000".into(),
            error: "x".into(),
        };
        let remote = RemoteSpanError::Transport("reset".into());
        for failure in [
            ReadFailure::Resolve(opening),
            ReadFailure::Resolve(open_failed),
            ReadFailure::Remote(remote),
        ] {
            let response = super::super::render_product_read_failure(failure);
            let want = (503, Some("1".to_string()), "temporarily_unavailable".into());
            assert_eq!(wire(response).await, want);
        }
        // Another retryable 503 is not given a delay it never stated.
        let response = super::super::render_product_read_failure(ReadFailure::Creating);
        assert_eq!(wire(response).await, (503, None, "creating".into()));
    }

    /// An append refused `temporarily_unavailable` with no delay of its own
    /// asks for one second; a stated delay is kept.
    #[tokio::test]
    async fn an_append_temporarily_unavailable_asks_for_a_retry() {
        let bare = || AppendFailure::new(FailureClass::Unavailable, AppendCode::Internal, "x");
        let response = super::super::render_product_append_error(bare());
        let want = (503, Some("1".to_string()), "temporarily_unavailable".into());
        assert_eq!(wire(response).await, want);
        let response = super::super::render_product_append_error(bare().retry(5));
        let want = (503, Some("5".to_string()), "temporarily_unavailable".into());
        assert_eq!(wire(response).await, want);
    }
}
