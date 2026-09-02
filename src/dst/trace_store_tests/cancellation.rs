//! Async cancellation is RAII-safe.

#![allow(unused_imports)]
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use super::super::{TraceEventKind, TraceOutcome, TraceStore};
use super::*;
use crate::dst::{ObjClass, StoreOp};

/// Required cancellation test 1: a point-operation future polled to
/// `Pending` and then dropped records `Cancelled`, leaves no active
/// entry, and reset succeeds. Pre-3.2.1 the plain id never reached
/// `finish()` and the trace was poisoned forever.
#[tokio::test]
async fn dropping_a_pending_point_operation_records_cancelled() {
    use futures_util::FutureExt;
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/1.sst");
    // now_or_never polls exactly once, observes Pending, and DROPS the
    // future — the async-cancellation shape.
    assert!(
        s.get_opts(&p, GetOptions::default())
            .now_or_never()
            .is_none()
    );
    assert!(
        s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
            .now_or_never()
            .is_none()
    );
    assert!(s.list_with_delimiter(Some(&p)).now_or_never().is_none());
    assert!(
        s.copy_opts(&p, &ObjPath::from("a/2.sst"), CopyOptions::default())
            .now_or_never()
            .is_none()
    );
    let evs = s.events();
    assert_eq!(evs.len(), 4, "{evs:?}");
    assert!(
        evs.iter().all(|e| e.outcome == TraceOutcome::Cancelled),
        "every cancelled operation is recorded Cancelled, never left Pending: {evs:?}"
    );
    s.reset(); // acceptance 5: reset succeeds after every cancellation
    assert!(s.events().is_empty());
}

/// Required cancellation tests 2+3: a multipart part future dropped
/// UNPOLLED (the guard was captured at dispatch) and a multipart
/// complete polled to `Pending` then dropped both record `Cancelled`;
/// reset works afterwards.
#[tokio::test]
async fn dropping_multipart_futures_records_cancelled() {
    use futures_util::FutureExt;
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/big.sst");
    let mut up = s
        .put_multipart_opts(&p, PutMultipartOptions::default())
        .await
        .unwrap();
    // Unpolled: put_part begins at dispatch, so the guard exists inside
    // the returned future even before the first poll.
    let fut = up.put_part(PutPayload::from(vec![1u8; 8]));
    drop(fut);
    // Polled to Pending, then dropped.
    assert!(
        up.put_part(PutPayload::from(vec![2u8; 8]))
            .now_or_never()
            .is_none()
    );
    assert!(up.complete().now_or_never().is_none());
    let evs = s.events();
    assert_eq!(evs.len(), 4, "open + two parts + complete: {evs:?}");
    assert_eq!(evs[0].detail.as_deref(), Some("multipart-open"));
    assert_eq!(evs[0].outcome, TraceOutcome::Ok);
    assert!(
        evs[1..]
            .iter()
            .all(|e| e.outcome == TraceOutcome::Cancelled),
        "{evs:?}"
    );
    s.reset();
    assert!(s.events().is_empty());
}

/// Required cancellation test 7 (acceptance 6): normal completion
/// retires exactly once — the guard's later drop cannot overwrite a
/// recorded outcome with Cancelled.
#[tokio::test]
async fn completed_operation_is_never_overwritten_by_cancellation() {
    let s = TraceStore::verbatim(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].outcome, TraceOutcome::Ok, "{evs:?}");
    s.reset();
    assert!(s.events().is_empty());
}

/// An UNPOLLED point-operation future records nothing at all: with
/// async_trait the body (including `begin_operation`) runs on first
/// poll, so a never-started operation is absent from the trace — the
/// trace records operations that STARTED. Reset stays clean.
#[tokio::test]
async fn unpolled_point_operation_records_nothing() {
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/1.sst");
    let fut = s.get_opts(&p, GetOptions::default());
    drop(fut);
    assert!(s.events().is_empty(), "never polled => never dispatched");
    s.reset();
}
