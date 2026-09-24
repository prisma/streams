//! Ambiguous replies at the HTTP layer (s3lite only). The production S3
//! client retries a conditional PUT answered 5xx (and, for updates, 409)
//! with its original precondition. A 5xx that arrives after the store
//! applied the PUT therefore comes back as `AlreadyExists` or
//! `Precondition`, the answers that otherwise mean "not written". These
//! cases pin what that does to each caller; docs/PROVIDER-CONTRACT.md
//! records the consequences as findings against ASM-OBJSTORE-CAS.
#![cfg(test)]

use axum::http::header::{IF_MATCH, IF_NONE_MATCH};
use bytes::Bytes;
use object_store::{ObjectStoreExt, PutPayload};

use super::Backend;
use super::registry_cases::{E1, E2, bump, dead, descriptor, registry, sref, stored};
use super::s3lite_harness::{HttpFault, HttpFaults};
use super::slatedb_cases::{bounded_put, is_fenced, open};
use super::store_cases::{create, update};

pub(super) async fn run(b: &Backend, faults: &HttpFaults) {
    failures_before_commit_are_retried_transparently(b, faults).await;
    committed_puts_come_back_refused(b, faults).await;
    registry_misreads_its_own_committed_writes(b, faults).await;
    slatedb_reports_its_own_committed_wal_as_fenced(b, faults).await;
    assert_eq!(
        faults.fired(),
        10,
        "{}: an armed HTTP fault never fired",
        b.name
    );
}

async fn stored_bytes(b: &Backend, path: &object_store::path::Path) -> Bytes {
    b.ops.get(path).await.unwrap().bytes().await.unwrap()
}

/// A 5xx before the store saw the PUT is retried by the client and lands
/// once; callers see an ordinary success.
async fn failures_before_commit_are_retried_transparently(b: &Backend, faults: &HttpFaults) {
    let path = b.path("http/transient");
    faults.arm(HttpFault::FailBeforeCommit, IF_NONE_MATCH, "http/transient");
    b.ops
        .put_opts(&path, PutPayload::from_static(b"first"), create())
        .await
        .expect("a create retried after a 5xx");
    let etag = b.ops.head(&path).await.unwrap().e_tag.unwrap();
    faults.arm(HttpFault::FailBeforeCommit, IF_MATCH, "http/transient");
    b.ops
        .put_opts(&path, PutPayload::from_static(b"second"), update(&etag))
        .await
        .expect("an update retried after a 5xx");
    assert_eq!(stored_bytes(b, &path).await, Bytes::from_static(b"second"));

    let reg = registry(b.ops.clone());
    let s = sref(b, "http-transient");
    reg.create(descriptor(b, "http-transient", E1, false))
        .await
        .unwrap();
    faults.arm(HttpFault::FailBeforeCommit, IF_MATCH, "");
    let mutated = reg.mutate_incarnation(&s, E1, bump).await;
    assert!(matches!(
        mutated,
        Ok(crate::registry::MutationResult::Applied(1))
    ));
    assert_eq!(stored(&reg, &s).await.seal_gen_counter, 1);
}

/// The raw fact: a conditional PUT the store applied, answered 5xx, is
/// re-sent by the client and refused, so the caller receives
/// `AlreadyExists` or `Precondition` for its own committed write.
async fn committed_puts_come_back_refused(b: &Backend, faults: &HttpFaults) {
    let path = b.path("http/committed");
    faults.arm(
        HttpFault::LoseReplyAfterCommit,
        IF_NONE_MATCH,
        "http/committed",
    );
    let created = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"mine"), create())
        .await;
    assert!(
        matches!(created, Err(object_store::Error::AlreadyExists { .. })),
        "{}: {created:?}",
        b.name
    );
    assert_eq!(stored_bytes(b, &path).await, Bytes::from_static(b"mine"));
    let etag = b.ops.head(&path).await.unwrap().e_tag.unwrap();
    faults.arm(HttpFault::LoseReplyAfterCommit, IF_MATCH, "http/committed");
    let updated = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"mine too"), update(&etag))
        .await;
    assert!(
        matches!(updated, Err(object_store::Error::Precondition { .. })),
        "{}: {updated:?}",
        b.name
    );
    assert_eq!(
        stored_bytes(b, &path).await,
        Bytes::from_static(b"mine too")
    );
}

/// The registry treats those refusals as "not written". FINDING: one
/// `mutate_incarnation` call applies its non-idempotent decision twice
/// (the counter moves 1 -> 3 and the call reports `Applied(3)`), and
/// `create` and `recreate` report a lost race to their own committed
/// descriptor.
async fn registry_misreads_its_own_committed_writes(b: &Backend, faults: &HttpFaults) {
    let reg = registry(b.ops.clone());
    let s = sref(b, "http-update");
    reg.create(descriptor(b, "http-update", E1, false))
        .await
        .unwrap();
    faults.arm(HttpFault::FailBeforeCommit, IF_MATCH, "");
    reg.mutate_incarnation(&s, E1, bump).await.unwrap();
    faults.arm(HttpFault::LoseReplyAfterCommit, IF_MATCH, "");
    let mutated = reg.mutate_incarnation(&s, E1, bump).await;
    assert!(
        matches!(mutated, Ok(crate::registry::MutationResult::Applied(3))),
        "{}: {mutated:?}",
        b.name
    );
    assert_eq!(stored(&reg, &s).await.seal_gen_counter, 3, "{}", b.name);

    faults.arm(HttpFault::LoseReplyAfterCommit, IF_NONE_MATCH, "");
    let (won, current) = reg
        .create(descriptor(b, "http-create", E1, false))
        .await
        .unwrap();
    assert!(!won && current.stream_epoch == E1, "{}", b.name);

    let gone = sref(b, "http-recreate");
    reg.create(descriptor(b, "http-recreate", E1, true))
        .await
        .unwrap();
    faults.arm(HttpFault::LoseReplyAfterCommit, IF_MATCH, "");
    let (won, current) = reg
        .recreate(&gone, descriptor(b, "http-recreate", E2, false), dead)
        .await
        .unwrap();
    assert!(!won && current.stream_epoch == E2, "{}", b.name);
}

/// SlateDB's retrying store sees `AlreadyExists` for its own WAL PUT. With
/// no put-id metadata on s3lite it reports `Fenced`; the batch is durable.
async fn slatedb_reports_its_own_committed_wal_as_fenced(b: &Backend, faults: &HttpFaults) {
    let path = b.slate_path("http-wal");
    let writer = open(b, b.shard.clone(), &path).await;
    faults.arm(HttpFault::LoseReplyAfterCommit, IF_NONE_MATCH, "/wal/");
    let written = bounded_put(&writer, b"k", b"v").await;
    assert!(
        written.as_ref().is_err_and(is_fenced),
        "{}: {written:?}",
        b.name
    );
    writer.close().await.ok();
    let reopened = open(b, b.shard.clone(), &path).await;
    assert_eq!(
        reopened.get(b"k").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    reopened.close().await.unwrap();
}
