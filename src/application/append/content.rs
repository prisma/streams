use super::{AppendCode, AppendCommand, AppendFailure, FailureClass, fail};
use crate::registry::StreamDesc;
use bytes::Bytes;

pub(super) struct ContentPlan {
    pub(super) entries: Vec<Bytes>,
    pub(super) deferred: Option<crate::shard::DeferredErr>,
}

#[expect(
    clippy::excessive_nesting,
    reason = "parse_content; the parser nests the deferred-versus-immediate verdict inside each content-type and body check so a producer's duplicate decision can still be recorded; flattening it would separate the verdict from the check that produced it"
)]
pub(super) fn parse_content(
    usage: &crate::usage::UsageService,
    desc: &StreamDesc,
    command: &AppendCommand,
    record_ceiling: usize,
    has_producer: bool,
) -> Result<ContentPlan, AppendFailure> {
    let body = &command.body;
    let close = command.close;
    let close_only = close && body.is_empty();
    // Content-Type: required on POST with a body; must match the stream's
    // configured media type (case-insensitive; parameters ignored). A
    // close-only POST ignores content type entirely. With producer headers
    // the mismatch is deferred so duplicates still return 204.
    let ct = &command.content_type;
    let mut deferred: Option<crate::shard::DeferredErr> = None;
    if !close_only {
        match ct {
            None => {
                if has_producer {
                    deferred = Some(crate::shard::DeferredErr::BadBody(
                        "missing Content-Type".into(),
                    ));
                } else {
                    return fail(
                        FailureClass::Invalid,
                        AppendCode::MissingContentType,
                        "Content-Type required",
                    );
                }
            }
            Some(c) => {
                if crate::registry::media_type(c) != crate::registry::media_type(&desc.content_type)
                {
                    if has_producer {
                        deferred = Some(crate::shard::DeferredErr::CtMismatch);
                    } else {
                        return fail(
                            FailureClass::Conflict,
                            AppendCode::ContentTypeMismatch,
                            "content type mismatch",
                        );
                    }
                }
            }
        }
    }

    // Body -> entries (batching rules); errors deferred with producers.
    let mut entries: Vec<Bytes> = Vec::new();
    if !close_only && deferred.is_none() {
        let refusal;
        (entries, refusal) = stored_records(desc, body, record_ceiling);
        if let Some(refusal) = refusal {
            if !has_producer {
                return Err(refusal);
            }
            deferred = Some(crate::shard::DeferredErr::BadBody(refusal.message));
        }
    }

    // Anything larger than a FRESH bucket can never be admitted, so it is a
    // permanent 413 for every content append — a 429 would name a wait no
    // wait can honour — and it is decided BEFORE the lifecycle intent, or a
    // close would leave the collection sealing forever, owing a record the
    // limiter will always refuse. A deferred verdict outranks it: the shard
    // still answers a duplicate producer request 204.
    if deferred.is_none()
        && let Some(kind) = usage.permanently_unadmittable(body.len() as u64, entries.len() as u64)
    {
        return fail(
            FailureClass::Invalid,
            AppendCode::PayloadTooLarge,
            &format!("request exceeds the per-stream ingest {kind} capacity"),
        );
    }
    Ok(ContentPlan { entries, deferred })
}

/// The records a content append of `body` stores in `desc`, and the append's
/// refusal of them: a JSON collection stores each array element re-encoded,
/// any other collection the body itself, and no record may exceed the
/// per-record ceiling. Records over the ceiling are still returned, since a
/// producer's deferred refusal carries them to the committer. A product seal
/// checks its final record's wire body here before publishing its intent.
pub(crate) fn stored_records(
    desc: &StreamDesc,
    body: &Bytes,
    record_ceiling: usize,
) -> (Vec<Bytes>, Option<AppendFailure>) {
    let invalid = |code, message: String| AppendFailure::new(FailureClass::Invalid, code, message);
    let records = if body.is_empty() {
        Err(invalid(AppendCode::EmptyBody, "empty body".into()))
    } else if desc.is_json() {
        crate::application::creation::json_entries(body, false)
            .map_err(|message| invalid(AppendCode::InvalidJson, message))
    } else {
        Ok(vec![body.clone()])
    };
    match records {
        Err(refusal) => (Vec::new(), Some(refusal)),
        Ok(records) => {
            let over = crate::application::creation::over_record_ceiling(record_ceiling, &records);
            let refusal = over.map(|over| {
                let message = format!(
                    "record of {over} bytes exceeds the per-record ceiling \
                     (MAX_RECORD_PAYLOAD_BYTES)"
                );
                invalid(AppendCode::RecordTooLarge, message)
            });
            (records, refusal)
        }
    }
}
