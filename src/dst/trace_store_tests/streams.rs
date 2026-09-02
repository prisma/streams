//! Stream lifetime vs observed outcome; reset refusal semantics.

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

/// reset() refuses to run while an operation is in flight — the
/// alternative is silent misattribution of the late completion.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_in_flight() {
    use futures_util::StreamExt;
    let s = TraceStore::new(mem());
    let mut stream = s.list(Some(&ObjPath::from("anything")));
    drop(stream.next());
    // The list event is still Pending (stream abandoned but not dropped),
    // so in_flight > 0 and reset must refuse.
    let _ = stream;
    // Force the guard NOT to run: leak the stream.
    std::mem::forget(stream);
    s.reset();
}

/// After completions land, reset is clean and the id space keeps
/// moving: a later event cannot be mistaken for an earlier one.
#[tokio::test]
async fn reset_after_completion_is_clean_and_ids_stay_monotonic() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.reset();
    assert!(s.events().is_empty());
    s.put_opts(&p, PutPayload::from(vec![2u8; 4]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert!(
        evs[0].seq > 0,
        "ids are monotonic across reset, not re-based: {:?}",
        evs[0].seq
    );
}

/// A list stream the consumer abandons is recorded Cancelled (and
/// its lifetime retired, so reset still works) — not left Pending
/// forever and not silently treated as successful.
#[tokio::test]
async fn abandoned_list_stream_is_marked_cancelled() {
    let s = TraceStore::new(mem());
    {
        let _stream = s.list(Some(&ObjPath::from("nothing-here")));
    }
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::List);
    assert_eq!(evs[0].outcome, TraceOutcome::Cancelled, "{evs:?}");
    // The lifetime was retired: reset works.
    s.reset();
    assert!(s.events().is_empty());
}

/// Acceptance 5-6 / required test 1: a list stream that yielded an
/// error is still ALIVE, so reset must refuse. Pre-3.2 the item
/// error retired the operation and the documented "refuses while
/// anything is in flight" claim was false for exactly this case.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_an_errored_list_stream_is_still_open() {
    use futures_util::StreamExt;
    let spy = list_spy_with_one_object().await;
    let s = TraceStore::verbatim(spy);
    let mut stream = s.list(Some(&ObjPath::from("a")));
    let first = stream.next().await;
    assert!(matches!(first, Some(Err(_))), "scripted error first");
    // The stream can keep serving items; its lifetime is open.
    s.reset();
}

/// Required test 2: a list that errors, then serves another item,
/// then completes — the first fact (the error) is the recorded
/// outcome, exhaustion retires the lifetime exactly once, and reset
/// then works.
#[tokio::test]
async fn errored_list_stream_serves_on_and_retires_at_exhaustion() {
    use futures_util::StreamExt;
    let spy = list_spy_with_one_object().await;
    let s = TraceStore::verbatim(spy);
    let out: Vec<_> = s.list(Some(&ObjPath::from("a"))).collect().await;
    assert_eq!(out.len(), 2);
    assert!(out[0].is_err() && out[1].is_ok(), "error then live item");
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(
        evs[0].outcome,
        TraceOutcome::Error,
        "first fact wins; exhaustion does not overwrite it: {evs:?}"
    );
    s.reset(); // exhaustion retired the lifetime
    assert!(s.events().is_empty());
}

/// Required test 3: an unconsumed delete stream is an ACTIVE
/// operation — reset must refuse while it is alive. Pre-3.2 delete
/// streams had no lifetime at all and reset would clear the trace
/// under a stream still appending observations.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_a_delete_stream_is_active() {
    let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let _stream =
        s.delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed());
    s.reset();
}

/// Required test 4: dropping a delete stream midway retires its
/// lifetime exactly once — reset works afterwards, repeatedly.
#[tokio::test]
async fn dropped_delete_stream_retires_exactly_once() {
    let (sp, _, _) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let mut stream = s.delete_stream(
        futures_util::stream::iter(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
        ])
        .boxed(),
    );
    let first = stream.next().await;
    assert!(matches!(first, Some(Ok(_))));
    drop(stream);
    s.reset();
    assert!(s.events().is_empty());
    s.reset(); // idempotent at quiescence: nothing double-retired
}
