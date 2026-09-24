//! Ambiguous replies at the HTTP layer (s3lite only), below the production
//! S3 client. The client sends a conditional PUT exactly once
//! (`src/bootstrap/s3_store.rs`, finding F1 in docs/PROVIDER-CONTRACT.md):
//! a 5xx, before or after the store applied the PUT, reaches the caller as
//! an error that is neither `AlreadyExists` nor `Precondition`, so those two
//! keep meaning "not written". Requests without a precondition are still
//! retried by the client.
#![cfg(test)]

use axum::http::header::{IF_MATCH, IF_NONE_MATCH};
use bytes::Bytes;
use object_store::{ObjectStoreExt, PutPayload};

use super::Backend;
use super::registry_cases::{E1, E2, bump, dead, descriptor, registry, sref, stored};
use super::s3lite_harness::{HttpFault, HttpFaults};
use super::slatedb_cases::{bounded_put, is_fenced, open};
use super::store_cases::{create, update};
use crate::registry::{MutationError, MutationResult};

pub(super) async fn run(b: &Backend, faults: &HttpFaults) {
    the_store_is_the_split_client(b).await;
    unconditional_puts_are_retried(b, faults).await;
    conditional_puts_are_sent_once(b, faults).await;
    committed_conditional_puts_are_errors_not_refusals(b, faults).await;
    mutate_incarnation_applies_once(b, faults).await;
    create_never_loses_a_race_to_itself(b, faults).await;
    recreate_never_declines_against_itself(b, faults).await;
    slatedb_wal_puts_answered_5xx(b, faults).await;
    assert_eq!(
        faults.fired(),
        11,
        "{}: an armed HTTP fault never fired",
        b.name
    );
}

async fn stored_bytes(b: &Backend, path: &object_store::path::Path) -> Bytes {
    b.ops.get(path).await.unwrap().bytes().await.unwrap()
}

/// Neither a refusal nor a success: the answer a caller must treat as
/// possibly committed.
fn is_ambiguous<T>(result: &object_store::Result<T>) -> bool {
    !matches!(
        result,
        Ok(_)
            | Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. })
    )
}

/// The server's store is the S3 client pair of `s3_store.rs`. It has no
/// copy-if-not-exists, so a conditional copy is refused before any request
/// rather than falling back to an unconditional one.
async fn the_store_is_the_split_client(b: &Backend) {
    let name = b.ops.to_string();
    assert!(
        name.contains("AmazonS3(provider-contract)"),
        "{}: {name}",
        b.name
    );
    let from = b.path("http/copy-source");
    b.ops
        .put(&from, PutPayload::from_static(b"source"))
        .await
        .unwrap();
    let to = b.path("http/copy-target");
    let copied = b
        .ops
        .copy_opts(
            &from,
            &to,
            object_store::CopyOptions {
                mode: object_store::CopyMode::Create,
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(copied, Err(object_store::Error::NotSupported { .. })),
        "{}: {copied:?}",
        b.name
    );
    assert!(
        matches!(
            b.ops.head(&to).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{}",
        b.name
    );
}

/// A PUT without a precondition is safe to repeat, so the client still
/// retries it: a 5xx is invisible to the caller.
async fn unconditional_puts_are_retried(b: &Backend, faults: &HttpFaults) {
    let path = b.path("http/overwrite");
    faults.arm(HttpFault::FailBeforeCommit, None, "http/overwrite");
    b.ops
        .put(&path, PutPayload::from_static(b"overwritten"))
        .await
        .expect("an unconditional PUT retried after a 5xx");
    assert_eq!(
        stored_bytes(b, &path).await,
        Bytes::from_static(b"overwritten")
    );
}

/// A 5xx before the store saw the conditional PUT is not retried: the
/// caller sees an error and nothing was written.
async fn conditional_puts_are_sent_once(b: &Backend, faults: &HttpFaults) {
    let path = b.path("http/transient");
    faults.arm(
        HttpFault::FailBeforeCommit,
        Some(IF_NONE_MATCH),
        "http/transient",
    );
    let created = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"first"), create())
        .await;
    assert!(is_ambiguous(&created), "{}: {created:?}", b.name);
    assert!(
        matches!(
            b.ops.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{}: a refused dispatch wrote",
        b.name
    );
    b.ops
        .put_opts(&path, PutPayload::from_static(b"first"), create())
        .await
        .expect("the caller's own retry of the create");
    let etag = b.ops.head(&path).await.unwrap().e_tag.unwrap();
    faults.arm(
        HttpFault::FailBeforeCommit,
        Some(IF_MATCH),
        "http/transient",
    );
    let updated = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"second"), update(&etag))
        .await;
    assert!(is_ambiguous(&updated), "{}: {updated:?}", b.name);
    assert_eq!(stored_bytes(b, &path).await, Bytes::from_static(b"first"));
}

/// A conditional PUT the store applied, answered 5xx, is reported as an
/// error, never as `AlreadyExists` or `Precondition` for the caller's own
/// write.
async fn committed_conditional_puts_are_errors_not_refusals(b: &Backend, faults: &HttpFaults) {
    let path = b.path("http/committed");
    faults.arm(
        HttpFault::LoseReplyAfterCommit,
        Some(IF_NONE_MATCH),
        "http/committed",
    );
    let created = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"mine"), create())
        .await;
    assert!(is_ambiguous(&created), "{}: {created:?}", b.name);
    assert_eq!(stored_bytes(b, &path).await, Bytes::from_static(b"mine"));
    let etag = b.ops.head(&path).await.unwrap().e_tag.unwrap();
    faults.arm(
        HttpFault::LoseReplyAfterCommit,
        Some(IF_MATCH),
        "http/committed",
    );
    let updated = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"mine too"), update(&etag))
        .await;
    assert!(is_ambiguous(&updated), "{}: {updated:?}", b.name);
    assert_eq!(
        stored_bytes(b, &path).await,
        Bytes::from_static(b"mine too")
    );
}

/// One non-idempotent `mutate_incarnation` applies its decision at most
/// once. A 5xx after or before the store applied it is
/// `AmbiguousCompletion`, and the counter moved by exactly what landed.
/// (Before the fix the client's retry of the landed PUT was refused, the
/// registry re-decided against its own write, and one call moved the
/// counter 0 -> 2 and reported `Applied(2)`.)
async fn mutate_incarnation_applies_once(b: &Backend, faults: &HttpFaults) {
    let reg = registry(b.ops.clone());
    let s = sref(b, "http-update");
    reg.create(descriptor(b, "http-update", E1, false))
        .await
        .unwrap();
    for (fault, counter) in [
        (HttpFault::LoseReplyAfterCommit, 1),
        (HttpFault::FailBeforeCommit, 1),
    ] {
        faults.arm(fault, Some(IF_MATCH), "");
        let mutated = reg.mutate_incarnation(&s, E1, bump).await;
        assert!(
            matches!(mutated, Err(MutationError::AmbiguousCompletion(_))),
            "{}: {fault:?}: {mutated:?}",
            b.name
        );
        assert_eq!(
            stored(&reg, &s).await.seal_gen_counter,
            counter,
            "{}: {fault:?}",
            b.name
        );
    }
    let next = reg.mutate_incarnation(&s, E1, bump).await;
    assert!(
        matches!(next, Ok(MutationResult::Applied(2))),
        "{}: {next:?}",
        b.name
    );
}

/// A create whose PUT landed but was answered 5xx is an error, not a lost
/// race to its own descriptor. (Before the fix it answered
/// `(false, own descriptor)`.)
async fn create_never_loses_a_race_to_itself(b: &Backend, faults: &HttpFaults) {
    let reg = registry(b.ops.clone());
    faults.arm(HttpFault::LoseReplyAfterCommit, Some(IF_NONE_MATCH), "");
    let created = reg.create(descriptor(b, "http-create", E1, false)).await;
    assert!(created.is_err(), "{}: {created:?}", b.name);
    assert_eq!(
        stored(&reg, &sref(b, "http-create")).await.stream_epoch,
        E1,
        "{}",
        b.name
    );
}

/// A recreate whose PUT landed but was answered 5xx is an error, not a
/// decline against its own new incarnation. (Before the fix it answered
/// `(false, own incarnation)`.)
async fn recreate_never_declines_against_itself(b: &Backend, faults: &HttpFaults) {
    let reg = registry(b.ops.clone());
    let gone = sref(b, "http-recreate");
    reg.create(descriptor(b, "http-recreate", E1, true))
        .await
        .unwrap();
    faults.arm(HttpFault::LoseReplyAfterCommit, Some(IF_MATCH), "");
    let recreated = reg
        .recreate(&gone, descriptor(b, "http-recreate", E2, false), dead)
        .await;
    assert!(recreated.is_err(), "{}: {recreated:?}", b.name);
    assert_eq!(stored(&reg, &gone).await.stream_epoch, E2, "{}", b.name);
}

/// SlateDB retries a failed WAL PUT itself. A 5xx before the store saw it
/// is retried and lands. One after the store applied it is the lost-reply
/// case: SlateDB's retry finds the path taken and, as s3lite returns no
/// put-id metadata, reports `Fenced` (with metadata it reports one success,
/// `slatedb_cases::ambiguous_wal_puts`). Both batches are durable.
async fn slatedb_wal_puts_answered_5xx(b: &Backend, faults: &HttpFaults) {
    let path = b.slate_path("http-wal");
    let writer = open(b, b.shard.clone(), &path).await;
    faults.arm(HttpFault::FailBeforeCommit, Some(IF_NONE_MATCH), "/wal/");
    bounded_put(&writer, b"retried", b"landed").await.unwrap();
    faults.arm(
        HttpFault::LoseReplyAfterCommit,
        Some(IF_NONE_MATCH),
        "/wal/",
    );
    let written = bounded_put(&writer, b"lost", b"landed").await;
    assert!(
        written.as_ref().is_err_and(is_fenced),
        "{}: {written:?}",
        b.name
    );
    writer.close().await.ok();
    let reopened = open(b, b.shard.clone(), &path).await;
    for key in [&b"retried"[..], b"lost"] {
        assert_eq!(
            reopened.get(key).await.unwrap(),
            Some(Bytes::from_static(b"landed")),
            "{}: {} is not durable",
            b.name,
            String::from_utf8_lossy(key)
        );
    }
    reopened.close().await.unwrap();
}
