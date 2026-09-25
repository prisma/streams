//! The collection seal's request body (Stage 8 §7.2): parsed once, before
//! any claim, and a final record authorized as the append it is.
use super::{auth_failure_response, enforce_customer, perr};
use crate::http::AppState;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

/// `{final, routingKey}`. `final` is tri-state: absent (a plain seal), a
/// present `null` (a final record whose value is null) or a value.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SealDoc {
    // Double Option: serde collapses a PRESENT `null` into `None`, so
    // `{"final": null}` silently became a seal with no final record,
    // dropping a perfectly valid JSON null that the SDK sends whenever T
    // admits it. The outer layer is presence, the inner is the value.
    #[serde(default, deserialize_with = "deserialize_some")]
    #[expect(
        clippy::option_option,
        reason = "final; absent, null and a value are three distinct wire states the seal contract names; a tri-state enum would restate serde's own null handling"
    )]
    r#final: Option<Option<serde_json::Value>>,
    #[serde(default)]
    routing_key: Option<String>,
}

impl SealDoc {
    /// The final record, `null` included, or `None` for a plain seal.
    pub(super) fn final_record(&self) -> Option<serde_json::Value> {
        self.r#final.clone().map(Option::unwrap_or_default)
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

/// Distinguishes an ABSENT field from one present as `null`.
fn deserialize_some<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}
