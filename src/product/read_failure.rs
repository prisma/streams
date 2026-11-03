//! A product read's failure, as the product surface answers it: a typed
//! status, code and retry hint per failure, the replay header for a
//! non-owner, and the refusal a read's first sealed chunk met after its
//! wait (`admission::park::read_refused`, edge #240) in place of a storage
//! failure.
use axum::http::StatusCode;
use axum::response::Response;

use super::answers::retry_unavailable;
use super::perr;

pub(crate) fn render_product_read_failure(
    error: crate::application::read::ReadFailure,
) -> Response {
    if matches!(error, crate::application::read::ReadFailure::Storage(_))
        && let Some(refusal) = crate::admission::park::read_refused()
    {
        return super::read_memory::refusal_response(refusal);
    }
    failure_response(error)
}

/// The answer to `error`, by its kind.
fn failure_response(error: crate::application::read::ReadFailure) -> Response {
    use crate::application::read::ReadFailure as E;
    let (status, code, message, retry, owner) = match error {
        E::Missing | E::Gone => (
            StatusCode::NOT_FOUND,
            "not_found",
            "stream not found",
            false,
            None,
        ),
        E::Creating => (
            StatusCode::SERVICE_UNAVAILABLE,
            "creating",
            "stream is still being created; retry",
            true,
            None,
        ),
        E::MissingKey => (
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Prisma-Encryption-Key required",
            false,
            None,
        ),
        E::WrongKey => (
            StatusCode::FORBIDDEN,
            "wrong_key",
            "encryption key mismatch",
            false,
            None,
        ),
        E::InvalidCursor => (
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "cursor is outside this stream's readable lineage",
            false,
            None,
        ),
        E::ChangedIncarnation => (
            StatusCode::CONFLICT,
            "target_mismatch",
            "stream incarnation changed",
            false,
            None,
        ),
        E::CursorBeyondTail | E::HistoryReplaced(_) => (
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
            false,
            None,
        ),
        E::KeylessLive => (
            StatusCode::BAD_REQUEST,
            "keyless_live",
            "live reads require a routing key",
            false,
            None,
        ),
        E::AppliedFork => (
            StatusCode::BAD_REQUEST,
            "deliver_unsupported_fork",
            "deliver=applied is not supported on forked streams",
            false,
            None,
        ),
        E::Resolve(crate::shard_directory::ResolveError::NotOwner { owner, .. }) => (
            StatusCode::CONFLICT,
            "not_ring_owner",
            "stream is owned by another instance; retry",
            true,
            Some(owner),
        ),
        E::Resolve(_) | E::Remote(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "read service unavailable; retry",
            true,
            None,
        ),
        E::Storage(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "read failed",
            true,
            None,
        ),
    };
    let mut response = retry_unavailable(code, perr(status, code, message, None, retry));
    if let Some(owner) = owner
        && let Ok(owner) = axum::http::HeaderValue::from_str(&owner)
    {
        response.headers_mut().insert("streams-replay-to", owner);
    }
    if code == "wrong_key" {
        response = crate::audit::tag(response, "wrong_key");
    }
    response
}

#[cfg(test)]
mod tests {
    use crate::application::read::ReadFailure;

    /// Only a wrong key's answer is tagged for the audit journal as a
    /// `wrong_key` denial; any other failure carries no tag.
    #[test]
    fn only_a_wrong_key_is_tagged_for_the_audit() {
        let tag = |error| {
            super::failure_response(error)
                .extensions()
                .get::<crate::audit::DenialTag>()
                .map(|t| t.code)
        };
        assert_eq!(tag(ReadFailure::WrongKey), Some("wrong_key"));
        assert_eq!(tag(ReadFailure::MissingKey), None);
        assert_eq!(tag(ReadFailure::Missing), None);
    }
}
