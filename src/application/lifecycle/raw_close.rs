//! Completion of the raw protocol's close after the committer's durable verdict.
use super::{FinalDisposition, LifecycleService, SealError, final_err_disposition};
use crate::registry::StreamDesc;
use crate::shard::{AppendAck, AppendErr};

pub(crate) struct RawClose<'a> {
    pub(crate) operation: &'a str,
    pub(crate) generation: Option<u64>,
    pub(crate) carries_content: bool,
    pub(crate) resumes_owed_final: bool,
}

/// A raw final and the product final share the same claim/mark/seal machinery.
/// Only this owner decides whether a durable or ambiguous verdict releases debt.
pub(crate) async fn complete_raw_close(
    service: &LifecycleService,
    descriptor: &StreamDesc,
    close: RawClose<'_>,
    verdict: &Result<AppendAck, AppendErr>,
) -> Result<(), SealError> {
    let stream = descriptor.sref();
    let name = stream.name().as_str();
    let release = || async {
        if let Some(generation) = close.generation
            && let Err(error) = super::abandon_seal_intent(
                service,
                &stream,
                close.operation,
                &descriptor.stream_epoch,
                generation,
            )
            .await
        {
            tracing::error!(stream = %name, %error, "releasing a refused raw close claim");
        }
    };
    let ack = match verdict {
        Ok(ack) => ack,
        Err(error) => {
            if close.carries_content
                && final_err_disposition(error) == FinalDisposition::DefinitivelyRejected
            {
                release().await;
            }
            return Ok(());
        }
    };
    // A spent producer tuple cannot deliver a new final record. Its original
    // acknowledgement remains the response, and only this exact claim releases.
    if ack.duplicate && !ack.closed {
        release().await;
    }
    if !ack.closed {
        return Ok(());
    }
    let owns_final = close.resumes_owed_final || (close.carries_content && !ack.duplicate);
    #[cfg(test)]
    if owns_final {
        crate::failpoints::pause_close_before_mark(name).await;
        if crate::failpoints::should_stop_before_mark_committed(name) {
            return Err(SealError::Resumable(
                "failpoint: stopped before marking the final durable".into(),
            ));
        }
    }
    if owns_final {
        super::mark_final_committed(service, &stream, close.operation, &descriptor.stream_epoch,
            close.generation.ok_or(SealError::InvalidClaim)?).await.map_err(|error| {
                SealError::Resumable(format!("the final record is durable but the seal could not be recorded: {error}; retry the close"))
            })?;
    }
    // Plain closes adopt the standing generation. Exact final retries retain
    // theirs, so neither can publish over a newer claim or another incarnation.
    super::run_seal(
        service,
        &stream,
        owns_final.then(|| close.operation.to_string()),
        &descriptor.stream_epoch,
        if owns_final { close.generation } else { None },
    )
    .await
    .map_err(|error| {
        SealError::Resumable(format!(
            "the collection seal did not complete: {error}; retry the close"
        ))
    })
}
