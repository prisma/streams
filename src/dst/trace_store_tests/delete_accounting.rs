//! delete_stream: exactly-once delegation + the typed operation ledger.

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

/// Three inputs, three scripted results: one inner call, everything
/// passes through, input and result both traced.
#[tokio::test]
async fn delete_stream_delegates_exactly_once_and_traces_both_sides() {
    let (sp, calls, _consumed) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
        Ok(ObjPath::from("a/3.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one inner delete_stream"
    );
    assert_eq!(out.len(), 3, "every scripted result passes through");
    assert!(out.iter().all(|r| r.is_ok()));
    let evs = s.events();
    let inputs = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteInput)
        .count();
    let results = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteResult)
        .count();
    assert_eq!((inputs, results), (3, 3), "both sides traced: {evs:?}");
    // PR 3.2: the operation ledger counts ATTEMPTED deletes — the
    // Ok inputs the inner store consumed — exactly once each, never
    // the diagnostic result observations on top.
    let c = s.operation_counts();
    assert!(
        c.contains(&(StoreOp::Delete, ObjClass::Sst, 3)),
        "3 attempted deletes, not 6 observations: {c:?}"
    );
}

/// The inner store returns FEWER results than inputs (batching stores
/// coalesce): the client sees exactly what the store returned — the
/// trace layer must not invent completions.
#[tokio::test]
async fn delete_stream_never_fabricates_results() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 1, "no manufactured Ok for unreturned inputs");

    // And the extreme: empty output stays empty.
    let (sp2, _, _) = spy(vec![]);
    let s2 = TraceStore::verbatim(sp2);
    let out2: Vec<_> = s2
        .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
        .collect()
        .await;
    assert!(out2.is_empty(), "empty output must remain empty");
}

/// Input errors pass through untouched (and are traced as such).
#[tokio::test]
async fn delete_stream_preserves_input_errors() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/2.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Err(delete_err()),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 2);
    assert!(out[0].is_ok());
    assert!(out[1].is_err(), "the input error must surface unchanged");
    let evs = s.events();
    let err_inputs = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteInput && e.outcome != TraceOutcome::Ok)
        .count();
    assert_eq!(err_inputs, 1, "the input error is traced: {evs:?}");
    // PR 3.2: the error input is an observation, NOT an attempted
    // delete — only the two Ok inputs count in the ledger.
    let c = s.operation_counts();
    assert!(
        c.contains(&(StoreOp::Delete, ObjClass::Sst, 2)),
        "input errors never count as operations: {c:?}"
    );
}

/// Inner failures pass through unchanged (and are traced as such).
#[tokio::test]
async fn delete_stream_preserves_inner_failures() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst")), Err(delete_err())]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 2);
    assert!(out[0].is_ok());
    assert!(out[1].is_err(), "the inner failure must surface unchanged");
}

/// Dropping the consumer mid-stream does not trigger extra delegated
/// calls — the one invocation happened at dispatch, and nothing
/// retries or replays behind the client's back.
#[tokio::test]
async fn delete_stream_dropped_consumer_triggers_no_extra_calls() {
    let (sp, calls, consumed) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
        Ok(ObjPath::from("a/3.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let mut stream = s.delete_stream(
        futures_util::stream::iter(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
            Ok(ObjPath::from("a/3.sst")),
        ])
        .boxed(),
    );
    let first = stream.next().await;
    assert!(matches!(first, Some(Ok(_))));
    drop(stream);
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "no replay on drop"
    );
    assert!(
        consumed.load(std::sync::atomic::Ordering::Relaxed) < 3,
        "the abandoned input stream stops being consumed"
    );
}

/// Acceptance 8 (the double-count finding): one ordinary successful
/// delete = ONE attempted operation in the ledger, even though the
/// diagnostic trace holds both the input and the result observation.
#[tokio::test]
async fn ordinary_delete_counts_once_in_the_operation_ledger() {
    let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
        .collect()
        .await;
    assert_eq!(out.len(), 1);
    let evs = s.events();
    assert_eq!(evs.len(), 2, "input + result observations: {evs:?}");
    assert_eq!(evs[0].kind, TraceEventKind::DeleteInput);
    assert_eq!(evs[1].kind, TraceEventKind::DeleteResult);
    let c = s.operation_counts();
    assert_eq!(
        c,
        vec![(StoreOp::Delete, ObjClass::Sst, 1)],
        "one delete attempted, not two: {c:?}"
    );
}
