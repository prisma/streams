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
        if body.is_empty() {
            if has_producer {
                deferred = Some(crate::shard::DeferredErr::BadBody("empty body".into()));
            } else {
                return fail(FailureClass::Invalid, AppendCode::EmptyBody, "empty body");
            }
        } else if desc.is_json() {
            match crate::application::creation::json_entries(body, false) {
                Ok(v) => entries = v,
                Err(m) => {
                    if has_producer {
                        deferred = Some(crate::shard::DeferredErr::BadBody(m));
                    } else {
                        return fail(FailureClass::Invalid, AppendCode::InvalidJson, &m);
                    }
                }
            }
        } else {
            entries = vec![body.clone()];
        }
        if deferred.is_none()
            && let Some(over) =
                crate::application::creation::over_record_ceiling(record_ceiling, &entries)
        {
            let m = format!(
                "record of {over} bytes exceeds the per-record ceiling \
                 (MAX_RECORD_PAYLOAD_BYTES)"
            );
            if has_producer {
                deferred = Some(crate::shard::DeferredErr::BadBody(m));
            } else {
                return fail(FailureClass::Invalid, AppendCode::RecordTooLarge, &m);
            }
        }
    }

    let close_carries_content = !entries.is_empty();
    // A body larger than the ingest bucket's CAPACITY can never be
    // admitted — that is a permanent 413, and it must be decided BEFORE
    // the lifecycle intent, or the collection is left sealing forever
    // owing a record the limiter will always refuse.
    if close && close_carries_content && deferred.is_none() {
        // Bytes AND records: a batched close with more records than the
        // record bucket can ever hold is just as permanently refused as
        // an oversized body, and publishing an intent for it stranded
        // the collection at 429 forever.
        if let Some(kind) = usage.permanently_unadmittable(body.len() as u64, entries.len() as u64)
        {
            return fail(
                FailureClass::Invalid,
                AppendCode::PayloadTooLarge,
                &format!("request exceeds the per-stream ingest {kind} capacity"),
            );
        }
    }
    Ok(ContentPlan { entries, deferred })
}
