//! Fleet-safe system-stream append (round-21 blocker 5): local first,
//! one relay hop to the ring owner. Moved verbatim out of billing.rs;
//! the typed create-vs-relay decision lands in the commit that follows.

use super::is_reserved_stream;

/// Append to a reserved system stream from ANY fleet member: local
/// first; on an ownership 409 the body relays ONCE to the owner's
/// fleet-internal telemetry endpoint, authenticated with
/// FLEET_INTERNAL_TOKEN and carrying the system key. Ambiguity is
/// safe end to end because every record downstream deduplicates by
/// deterministic id / source sequence.
// mt-lint: allow(name-param-shared-core): system ledger under the system project; names are crate constants (_usage, _ops_*), never customer input
pub(crate) async fn system_append(
    state: &std::sync::Arc<crate::http::AppState>,
    stream: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    use axum::http::{HeaderMap, HeaderValue};
    debug_assert!(is_reserved_stream(stream));
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|_| "bad system key".to_string())?,
    );
    hdrs.insert("content-type", HeaderValue::from_static("application/json"));
    // Local attempt (create lazily on 404).
    let mut r = crate::http::append(
        state.clone(),
        // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
        crate::tenant::system_project().stream_ref(stream),
        hdrs.clone(),
        axum::body::Body::from(body.clone()),
        None,
        None,
        None,
    )
    .await;
    if r.status() == axum::http::StatusCode::NOT_FOUND {
        let c = crate::http::create_stream(
            state.clone(),
            crate::tenant::system_project(),
            stream.to_string(),
            hdrs.clone(),
            bytes::Bytes::new(),
        )
        .await;
        let cst = c.status().as_u16();
        if !(cst == 200 || cst == 201 || cst == 409) {
            // A create refused by ownership relays below with the body.
            if crate::http::replay_peer_url(state, &c).is_none() {
                return Err(format!("system create {stream}: {cst}"));
            }
            r = c;
        } else {
            r = crate::http::append(
                state.clone(),
                // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
                crate::tenant::system_project().stream_ref(stream),
                hdrs.clone(),
                axum::body::Body::from(body.clone()),
                None,
                None,
                None,
            )
            .await;
        }
    }
    if r.status().is_success() {
        return Ok(());
    }
    // Ownership bounce: relay once to the owner.
    if let Some((_, base)) = crate::http::replay_peer_url(state, &r) {
        let mk = |bearer: Option<&str>| {
            let mut req = crate::http::peer_client()
                .post(format!(
                    "{base}/v1/internal/telemetry-append/{}",
                    crate::http::encode_stream_name_path(stream)
                ))
                .timeout(std::time::Duration::from_secs(20))
                .header("stream-encryption-key", key)
                .header("content-type", "application/json")
                .body(body.clone());
            if let Some(t) = bearer {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            req
        };
        match state.peer.send(mk).await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => return Err(format!("telemetry relay {stream}: {}", resp.status())),
            Err(e) => return Err(format!("telemetry relay {stream}: {e}")),
        }
    }
    Err(format!("system append {stream}: {}", r.status()))
}
