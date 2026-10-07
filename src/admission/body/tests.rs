#![cfg(test)]

use std::sync::Arc;

use axum::body::Body;
use bytes::Bytes;

use super::{BodyRefusal, buffer_within};
use crate::project_policy::ProjectQuotas;
use crate::quota::{BufferedBodyGuard, ProjectAdmission, QuotaRegistry};
use crate::tenant::ProjectId;

const KIB: usize = 1 << 10;

/// A project's admission entry, tracked and idle.
fn project(registry: &QuotaRegistry) -> Arc<ProjectAdmission> {
    let id = ProjectId::new("proj-body").unwrap();
    drop(registry.admit(&id, &ProjectQuotas::default(), 0).unwrap());
    registry.pressure_handle(&id).unwrap()
}

/// A body arriving in `chunks` chunks of 1 KiB.
fn chunked(chunks: usize) -> Body {
    let parts = (0..chunks).map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; KIB])));
    Body::from_stream(futures_util::stream::iter(parts))
}

/// A body alone is buffered whole past its project's line, and its guard
/// charges exactly its bytes until it drops; beside 6 KiB the project's
/// other request holds, a body is refused at the chunk that would take
/// the project past its 8 KiB line, leaving only those 6 KiB charged.
#[tokio::test]
async fn a_body_is_buffered_alone_past_the_line_and_refused_beside_other_bytes() {
    let registry = QuotaRegistry::default();
    let entry = project(&registry);
    let line = 8 * 1024;
    let (bytes, guard) = buffer_within(chunked(16), 64 * KIB, Some(entry.clone()), line)
        .await
        .unwrap();
    assert_eq!(
        (bytes.len(), entry.estimated_pressure_bytes()),
        (16 * KIB, 16 * 1024)
    );
    drop(guard);
    assert_eq!(entry.estimated_pressure_bytes(), 0);
    let other = BufferedBodyGuard::reserve(entry.clone(), 6 * 1024);
    let refused = buffer_within(chunked(4), 64 * KIB, Some(entry.clone()), line).await;
    assert_eq!(refused.err(), Some(BodyRefusal::MemoryPressure));
    assert_eq!(entry.estimated_pressure_bytes(), 6 * 1024);
    let fits = buffer_within(chunked(2), 64 * KIB, Some(entry.clone()), line).await;
    assert_eq!(
        fits.map(|(b, _)| b.len()).ok(),
        Some(2 * KIB),
        "to the line"
    );
    drop(other);
    assert_eq!(entry.estimated_pressure_bytes(), 0);
}

/// Without a line a body grows to the limit beside anything; one byte past
/// the limit is refused as too large, and so is a body whose stream fails;
/// a body with no project buffers uncharged.
#[tokio::test]
async fn the_limit_bounds_a_body_and_no_line_is_no_line() {
    let registry = QuotaRegistry::default();
    let entry = project(&registry);
    let other = BufferedBodyGuard::reserve(entry.clone(), 1 << 20);
    let (bytes, _guard) = buffer_within(chunked(8), 8 * KIB, Some(entry.clone()), 0)
        .await
        .unwrap();
    assert_eq!(bytes.len(), 8 * KIB);
    let over = buffer_within(chunked(9), 8 * KIB, Some(entry.clone()), 0).await;
    assert_eq!(over.err(), Some(BodyRefusal::TooLarge));
    let failing = futures_util::stream::iter([Err::<Bytes, _>(std::io::Error::other("reset"))]);
    let failed = buffer_within(Body::from_stream(failing), 8 * KIB, None, 0).await;
    assert_eq!(failed.err(), Some(BodyRefusal::TooLarge));
    let (bytes, guard) = buffer_within(chunked(3), 8 * KIB, None, 1).await.unwrap();
    assert_eq!((bytes.len(), guard.is_none()), (3 * KIB, true));
    drop(other);
}
