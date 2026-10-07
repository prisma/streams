//! Request body buffering (round 13 and shared cells H3). A body is
//! buffered whole before its handler runs, each arriving chunk charged to
//! its project's buffered-body pressure (the queued-byte counter starts too
//! late: the buffering window itself must be accounted). The caller drops
//! the returned guard at the queued-append transfer point, so the two
//! charges never overlap.
//!
//! Under a project memory line (`PROJECT_MEMORY_PRESSURE_BYTES`), a chunk
//! that would take the project's estimated pressure past the line while
//! the project holds other bytes refuses the body: uploads admitted
//! together (each passed the write gate at the pressure before any of them
//! buffered) share their project's line as its reads do, and a body alone
//! is buffered whole.

use std::sync::Arc;

use axum::body::Body;
use bytes::Bytes;

use crate::quota::{BufferedBodyGuard, ProjectAdmission};

/// Why a body was not buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyRefusal {
    /// It passed `limit`, or its stream failed.
    TooLarge,
    /// Its project's estimated pressure would have passed the line.
    MemoryPressure,
}

/// Buffer `body` up to `limit` bytes, charging `adm`'s buffered-body
/// pressure chunk by chunk under its project's memory `line` (0: none).
pub(crate) async fn buffer_within(
    body: Body,
    limit: usize,
    adm: Option<Arc<ProjectAdmission>>,
    line: u64,
) -> Result<(Bytes, Option<BufferedBodyGuard>), BodyRefusal> {
    use futures_util::StreamExt;
    let mut guard = adm.map(|a| BufferedBodyGuard::reserve(a, 0));
    let mut buf: Vec<u8> = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(c) = chunk else {
            return Err(BodyRefusal::TooLarge);
        };
        if buf.len() + c.len() > limit {
            return Err(BodyRefusal::TooLarge);
        }
        if let Some(g) = guard.as_mut()
            && !g.try_grow(c.len() as u64, line)
        {
            return Err(BodyRefusal::MemoryPressure);
        }
        buf.extend_from_slice(&c);
    }
    Ok((Bytes::from(buf), guard))
}

#[cfg(test)]
mod tests;
