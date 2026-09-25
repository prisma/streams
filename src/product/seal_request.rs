//! The collection seal's request body (Stage 8 §7.2): parsed once, before
//! any claim, and a final record authorized as the append it is.
use super::{AppState, auth_failure_response, enforce_customer, perr};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;

/// `{final, routingKey}`. `final` is tri-state: absent (a plain seal), a
/// present `null` (a final record whose value is null) or a value, held as
/// the record the seal stores.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SealDoc {
    // A PRESENT `null` is the record `null`: a plain Option drops the valid
    // JSON null the SDK sends whenever T admits it.
    #[serde(default, deserialize_with = "stored_final")]
    r#final: Option<Bytes>,
    #[serde(default)]
    routing_key: Option<String>,
}

impl SealDoc {
    /// The final record as the seal stores it, `null` included, or `None`
    /// for a plain seal.
    pub(super) fn final_record(&self) -> Option<Bytes> {
        self.r#final.clone()
    }

    /// The final record's routing key, empty when absent.
    pub(super) fn routing_key(&self) -> &str {
        self.routing_key.as_deref().unwrap_or_default()
    }
}

/// Parses a non-empty seal body. A final record, present even as `null`,
/// is an append of one record: in enforce mode the caller must hold
/// `streams.records.append` as well as the `streams.lifecycle.manage` the
/// gate already checked (owner decision, second external review), refused
/// here, before any seal claim or durable step, exactly as the gate
/// refuses a missing scope. A body without `final` appends nothing and
/// needs only the lifecycle scope. The gate's verified principal does not
/// reach this handler (passing it would grow `product_entry`'s excepted
/// scope), so the bearer is verified again; a caller the gate admitted
/// and whose authority has since lapsed is refused.
pub(super) fn seal_request(
    state: &AppState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<SealDoc, Box<Response>> {
    let doc: SealDoc = serde_json::from_slice(body).map_err(|e| {
        Box::new(perr(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            &format!("seal request: {e}"),
            None,
            false,
        ))
    })?;
    if doc.r#final.is_some() && state.auth.mode == crate::auth::AuthMode::Enforce {
        let principal = enforce_customer(state, headers).map_err(Box::new)?;
        if let Err(e) = principal.require(crate::tenant::Scope::RecordsAppend) {
            return Err(Box::new(crate::audit::tag_project(
                auth_failure_response(&e),
                &principal.project_id,
            )));
        }
    }
    Ok(doc)
}

/// An ABSENT final is `None`; a present one, `null` included, is the record
/// the seal stores: the client's own text, validated and without whitespace
/// (`creation::json_record`), never re-serialised.
fn stored_final<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Bytes>, D::Error> {
    use serde::Deserialize;
    let text = Box::<serde_json::value::RawValue>::deserialize(d)?;
    crate::application::creation::json_record(text.get().as_bytes())
        .map(Some)
        .map_err(serde::de::Error::custom)
}
