use super::{AppendCode, AppendFailure, FailureClass, fail};
use crate::registry::StreamDesc;
use std::sync::Arc;

#[expect(
    clippy::fn_params_excessive_bools,
    clippy::too_many_arguments,
    reason = "admit_usage; admission takes the request's parts and its two independent flags as the handler resolved them; a request struct or an enum would restate the request"
)]
pub(super) fn admit_usage(
    usage: &crate::usage::UsageService,
    desc: &StreamDesc,
    close_only: bool,
    valid_content: bool,
    stored_bytes: usize,
    record_count: usize,
) -> Result<Arc<crate::usage::Counters>, AppendFailure> {
    let name_hash = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    let counters = if !close_only && valid_content {
        match usage.admit_append(&name_hash, stored_bytes as u64, record_count as u64) {
            Err(hit) => {
                // Every refusal here is transient: parse_content has already
                // answered 413 for anything larger than a fresh bucket, so
                // the wait this names is one the bucket will honour.
                crate::usage::note_limit_refusal(&hit);
                return fail(
                    FailureClass::Capacity,
                    AppendCode::RateLimited(hit.code()),
                    &hit.message(usage.limits()),
                )
                .map_err(|e| e.retry(hit.retry_ms().div_ceil(1000).max(1)));
            }
            Ok(c) => {
                c.requests
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                c.records
                    .fetch_add(record_count as u64, std::sync::atomic::Ordering::Relaxed);
                c.bytes_in
                    .fetch_add(stored_bytes as u64, std::sync::atomic::Ordering::Relaxed);
                c
            }
        }
    } else {
        // Close-only / deferred-error requests skip admission; a single
        // resolve here still beats the old two-site double resolve.
        usage.counters(&name_hash)
    };

    Ok(counters)
}
