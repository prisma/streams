//! Ordering, byte counts, redaction, hashing, outcomes, multipart.

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

/// The trace is in id (= trace-lock acquisition) order, so a
/// before/after refactor diff compares equal iff the client issued the
/// same operations in the same order. For a single sequential client
/// with no streams, ids happen to be dense from 0 — but the CONTRACT
/// is unique + strictly increasing, not dense (see `StoreTraceEvent`).
#[tokio::test]
async fn events_are_ordered_by_dispatch_seq() {
    let s = TraceStore::verbatim(mem());
    let pa = ObjPath::from("shards/x/wal/1.sst");
    let pb = ObjPath::from("shards/x/wal/2.sst");
    s.put_opts(&pa, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.put_opts(&pb, PutPayload::from(vec![2u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.get_opts(&pa, GetOptions::default()).await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 3);
    for w in evs.windows(2) {
        assert!(w[0].seq < w[1].seq, "ids strictly increase in trace order");
    }
    assert_eq!(evs[0].op, StoreOp::Put);
    assert_eq!(evs[0].path, pa.as_ref());
    assert_eq!(evs[1].op, StoreOp::Put);
    assert_eq!(evs[1].path, pb.as_ref());
    assert_eq!(evs[2].op, StoreOp::Get);
    assert!(evs.iter().all(|e| e.outcome == TraceOutcome::Ok));

    // The operation ledger and reset.
    let c = s.operation_counts();
    assert!(c.contains(&(StoreOp::Put, ObjClass::Wal, 2)), "{c:?}");
    assert!(c.contains(&(StoreOp::Get, ObjClass::Wal, 1)), "{c:?}");
    s.reset();
    assert!(s.events().is_empty());
}

#[tokio::test]
async fn put_records_byte_count() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![7u8; 1234]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::Put);
    assert_eq!(evs[0].class, ObjClass::Wal);
    assert_eq!(evs[0].bytes, Some(1234));
}

#[tokio::test]
async fn ranged_get_records_span() {
    let inner = mem();
    let p = ObjPath::from("a/b/1.sst");
    inner
        .put_opts(&p, PutPayload::from(vec![0u8; 16]), PutOptions::default())
        .await
        .unwrap();
    let s = TraceStore::new(inner);

    let bounded = GetOptions {
        range: Some(object_store::GetRange::Bounded(2..7)),
        ..Default::default()
    };
    s.get_opts(&p, bounded).await.unwrap();
    let suffix = GetOptions {
        range: Some(object_store::GetRange::Suffix(4)),
        ..Default::default()
    };
    s.get_opts(&p, suffix).await.unwrap();
    s.get_opts(&p, GetOptions::default()).await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 3);
    assert_eq!(evs[0].bytes, Some(5), "bounded range records its span");
    assert_eq!(evs[1].bytes, Some(4), "suffix range records its span");
    assert_eq!(evs[2].bytes, None, "a full get has no span");
}

/// Redaction is the default: no tenant-derived segment survives, the
/// path still classifies identically, and payload bytes are never
/// retained.
#[tokio::test]
async fn redaction_hashes_paths_and_never_stores_payload_bytes_by_default() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("acme-corp/shards/secret-stream-name/wal/00000042.sst");
    s.put_opts(&p, PutPayload::from(vec![0xabu8; 8]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    let e = &evs[0];
    assert_ne!(e.path, p.as_ref());
    for leaked in ["acme-corp", "secret-stream-name", "00000042"] {
        assert!(
            !e.path.contains(leaked),
            "redacted path must not contain {leaked}: {}",
            e.path
        );
    }
    // Classification is preserved, both in the event and on re-classify.
    assert_eq!(e.class, ObjClass::Wal);
    assert_eq!(ObjClass::of(&e.path), ObjClass::Wal, "{}", e.path);
    assert!(e.path.ends_with(".sst"), "extension survives: {}", e.path);
    assert!(
        e.path.contains("/wal/"),
        "structural segments survive: {}",
        e.path
    );
    // Payload bytes are never retained by default.
    assert_eq!(e.content_hash, None);

    // The manifest marker survives redaction too.
    let s2 = TraceStore::new(mem());
    let mp = ObjPath::from("tenant9/shards/root-3/manifest-00001.json");
    s2.put_opts(&mp, PutPayload::from(vec![0u8; 1]), PutOptions::default())
        .await
        .unwrap();
    let e2 = &s2.events()[0];
    assert_eq!(e2.class, ObjClass::Manifest);
    assert_eq!(ObjClass::of(&e2.path), ObjClass::Manifest, "{}", e2.path);
    assert!(!e2.path.contains("tenant9"));
    assert!(!e2.path.contains("root-3"));
}

#[tokio::test]
async fn hash_on_mode_records_content_hashes() {
    let s = TraceStore::with_options(mem(), true, true);
    let payload = b"stream-payload-bytes".to_vec();
    let p = ObjPath::from("shards/x/wal/9.sst");
    s.put_opts(&p, PutPayload::from(payload.clone()), PutOptions::default())
        .await
        .unwrap();
    let want = {
        use sha2::Digest;
        let d = sha2::Sha256::digest(&payload);
        crate::crypto::hex(&d[..8])
    };
    assert_eq!(want.len(), 16, "16 hex chars, not payload bytes");
    let e = &s.events()[0];
    assert_eq!(e.content_hash.as_deref(), Some(want.as_str()));
}

#[tokio::test]
async fn outcome_is_recorded_on_error() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/nope.sst");
    let res = s.get_opts(&p, GetOptions::default()).await;
    assert!(res.is_err(), "get of a nonexistent object must fail");
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::Get);
    assert_eq!(evs[0].outcome, TraceOutcome::NotFound);
}

/// The session is Put events all the way down: open, one per part
/// with byte counts, and a complete that notes how many parts landed.
#[tokio::test]
async fn multipart_session_is_traced() {
    let s = TraceStore::verbatim(mem());
    let p = ObjPath::from("shards/x/wal/big.sst");
    let mut up = s
        .put_multipart_opts(&p, PutMultipartOptions::default())
        .await
        .unwrap();
    up.put_part(PutPayload::from(vec![1u8; 10])).await.unwrap();
    up.put_part(PutPayload::from(vec![2u8; 20])).await.unwrap();
    up.complete().await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 4);
    assert_eq!(evs[0].detail.as_deref(), Some("multipart-open"));
    assert_eq!(evs[0].bytes, None);
    assert_eq!(evs[1].detail.as_deref(), Some("part"));
    assert_eq!(evs[1].bytes, Some(10));
    assert_eq!(evs[2].detail.as_deref(), Some("part"));
    assert_eq!(evs[2].bytes, Some(20));
    assert_eq!(evs[3].detail.as_deref(), Some("complete parts=2"));
    assert!(
        evs.iter()
            .all(|e| e.op == StoreOp::Put && e.outcome == TraceOutcome::Ok)
    );
}
