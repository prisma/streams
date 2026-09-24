//! The registry's use of conditional writes through the store under test:
//! missing ETags refuse to write, ambiguous replies are reported and never
//! retried, a changed incarnation writes nothing, and racing registries
//! (separate caches, as separate processes) lose no update.
#![cfg(test)]

use std::sync::Arc;

use futures_util::future::join_all;
use object_store::ObjectStore;

use super::faults::{Conditional, FaultyStore, PutFault};
use super::{Backend, conclusive};
use crate::registry::{
    LAYOUT_VERSION, Mutation, MutationError, MutationResult, PersistedDescriptor, Registry,
    StreamDesc,
};
use crate::tenant::{CanonicalStreamName, CellId, ProjectId, TenantStreamRef};

pub(super) const E1: &str = "00000000000000000000000000000001";
pub(super) const E2: &str = "00000000000000000000000000000002";
const E3: &str = "00000000000000000000000000000003";
const MUTATORS: usize = 4;
const RACE_ROUNDS: usize = 3;

pub(super) fn registry(store: Arc<dyn ObjectStore>) -> Registry {
    Registry::new(store, &CellId::new("provider-contract").unwrap())
}

pub(super) fn sref(b: &Backend, name: &str) -> TenantStreamRef {
    TenantStreamRef::new(
        ProjectId::new(&b.project).unwrap(),
        CanonicalStreamName::new(name).unwrap(),
    )
}

pub(super) fn descriptor(
    b: &Backend,
    name: &str,
    epoch: &str,
    deleted: bool,
) -> PersistedDescriptor {
    PersistedDescriptor {
        seal_gen_counter: 0,
        account_id: None,
        project_id: ProjectId::new(&b.project).unwrap(),
        name: name.into(),
        stream_epoch: epoch.into(),
        key_fingerprint: "fp".into(),
        created_ms: 1,
        expires_at_ms: None,
        deleted,
        content_type: "application/json".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        layout_version: LAYOUT_VERSION,
    }
}

/// A non-idempotent decision: every applied call advances the counter by
/// one and reports the value it wrote.
pub(super) fn bump(current: &StreamDesc) -> Mutation<u64> {
    let mut next = current.to_persisted();
    next.seal_gen_counter += 1;
    let written = next.seal_gen_counter;
    Mutation::Write(next, written)
}

pub(super) fn dead(current: &StreamDesc) -> bool {
    current.deleted
}

pub(super) async fn stored(reg: &Registry, sref: &TenantStreamRef) -> StreamDesc {
    reg.invalidate(sref);
    reg.get(sref)
        .await
        .expect("read the descriptor")
        .expect("the descriptor exists")
}

pub(super) async fn run(b: &Backend) {
    missing_etag_refuses_to_write(b).await;
    ambiguous_updates_are_reported_not_retried(b).await;
    ambiguous_creates_and_recreates(b).await;
    changed_incarnation_writes_nothing(b).await;
    racing_mutators_lose_no_update(b).await;
    for round in 0..RACE_ROUNDS {
        conclusive(b, "recreate race", |attempt| {
            recreate_race(b, format!("recreate-race-{round}-{attempt}"))
        })
        .await;
    }
}

/// A read without an ETag cannot become a write: `mutate_incarnation`
/// reports the missing token and `recreate` fails, both before any PUT.
async fn missing_etag_refuses_to_write(b: &Backend) {
    let faulty = FaultyStore::new(b.ops.clone());
    let reg = registry(faulty.clone());
    let live = sref(b, "missing-etag");
    let gone = sref(b, "missing-etag-dead");
    reg.create(descriptor(b, "missing-etag", E1, false))
        .await
        .unwrap();
    reg.create(descriptor(b, "missing-etag-dead", E1, true))
        .await
        .unwrap();
    let before = faulty.dispatched();
    faulty.strip_etags(true);
    let mutated = reg.mutate_incarnation(&live, E1, bump).await;
    assert!(
        matches!(mutated, Err(MutationError::MissingConditionalToken(_))),
        "{}: {mutated:?}",
        b.name
    );
    let recreated = reg
        .recreate(&gone, descriptor(b, "missing-etag-dead", E2, false), dead)
        .await;
    assert!(recreated.is_err(), "{}: {recreated:?}", b.name);
    faulty.strip_etags(false);
    assert_eq!(
        faulty.dispatched(),
        before,
        "{}: PUT without a token",
        b.name
    );
    assert_eq!(stored(&reg, &live).await.seal_gen_counter, 0);
    assert_eq!(stored(&reg, &gone).await.stream_epoch, E1);
}

/// A lost reply to an applied update and a failed dispatch both surface
/// as `AmbiguousCompletion`, each after exactly one PUT; only the first
/// changed the descriptor.
async fn ambiguous_updates_are_reported_not_retried(b: &Backend) {
    let faulty = FaultyStore::new(b.ops.clone());
    let reg = registry(faulty.clone());
    let s = sref(b, "ambiguous-update");
    reg.create(descriptor(b, "ambiguous-update", E1, false))
        .await
        .unwrap();
    for (fault, dispatched, counter) in [
        (PutFault::LoseReply, 1, 1),
        (PutFault::FailBeforeDispatch, 0, 1),
    ] {
        let before = faulty.dispatched();
        faulty.arm(fault, Conditional::Update, "");
        let mutated = reg.mutate_incarnation(&s, E1, bump).await;
        assert!(
            matches!(mutated, Err(MutationError::AmbiguousCompletion(_))),
            "{}: {fault:?}: {mutated:?}",
            b.name
        );
        assert_eq!(
            faulty.fired(),
            if fault == PutFault::LoseReply { 1 } else { 2 }
        );
        assert_eq!(
            faulty.dispatched() - before,
            dispatched,
            "{}: {fault:?}",
            b.name
        );
        assert_eq!(
            stored(&reg, &s).await.seal_gen_counter,
            counter,
            "{}: {fault:?}",
            b.name
        );
    }
}

/// A create whose reply is lost is an error, never a success; the caller's
/// retry then finds its own landed descriptor as a lost race, not a second
/// creation. A recreate's lost reply is likewise an error, and a retry
/// declines against the incarnation that landed. Failed dispatches write
/// nothing.
async fn ambiguous_creates_and_recreates(b: &Backend) {
    let faulty = FaultyStore::new(b.ops.clone());
    let reg = registry(faulty.clone());
    faulty.arm(PutFault::LoseReply, Conditional::Create, "");
    let created = reg.create(descriptor(b, "lost-create", E1, false)).await;
    assert!(created.is_err(), "{}: lost create: {created:?}", b.name);
    let (won, current) = reg
        .create(descriptor(b, "lost-create", E2, false))
        .await
        .unwrap();
    assert!(!won, "{}: a landed create was created twice", b.name);
    assert_eq!(current.stream_epoch, E1);

    faulty.arm(PutFault::FailBeforeDispatch, Conditional::Create, "");
    let failed = reg.create(descriptor(b, "failed-create", E1, false)).await;
    assert!(failed.is_err(), "{}: failed create: {failed:?}", b.name);
    let (won, current) = reg
        .create(descriptor(b, "failed-create", E2, false))
        .await
        .unwrap();
    assert!(won, "{}: a failed dispatch left a descriptor", b.name);
    assert_eq!(current.stream_epoch, E2);

    let lost = sref(b, "lost-recreate");
    reg.create(descriptor(b, "lost-recreate", E1, true))
        .await
        .unwrap();
    faulty.arm(PutFault::LoseReply, Conditional::Update, "");
    let recreated = reg
        .recreate(&lost, descriptor(b, "lost-recreate", E2, false), dead)
        .await;
    assert!(
        recreated.is_err(),
        "{}: lost recreate: {recreated:?}",
        b.name
    );
    assert_eq!(stored(&reg, &lost).await.stream_epoch, E2);
    let (won, current) = reg
        .recreate(&lost, descriptor(b, "lost-recreate", E3, false), dead)
        .await
        .unwrap();
    assert!(!won, "{}: a landed recreate was applied twice", b.name);
    assert_eq!(current.stream_epoch, E2);

    let failed = sref(b, "failed-recreate");
    reg.create(descriptor(b, "failed-recreate", E1, true))
        .await
        .unwrap();
    faulty.arm(PutFault::FailBeforeDispatch, Conditional::Update, "");
    let recreated = reg
        .recreate(&failed, descriptor(b, "failed-recreate", E2, false), dead)
        .await;
    assert!(
        recreated.is_err(),
        "{}: failed recreate: {recreated:?}",
        b.name
    );
    let current = stored(&reg, &failed).await;
    assert!(current.deleted && current.stream_epoch == E1, "{}", b.name);
    assert_eq!(faulty.fired(), 4);
}

/// A mutator bound to a replaced incarnation is told so and writes nothing.
async fn changed_incarnation_writes_nothing(b: &Backend) {
    let faulty = FaultyStore::new(b.ops.clone());
    let reg = registry(faulty.clone());
    let s = sref(b, "replaced");
    reg.create(descriptor(b, "replaced", E1, true))
        .await
        .unwrap();
    let (won, _) = reg
        .recreate(&s, descriptor(b, "replaced", E2, false), dead)
        .await
        .unwrap();
    assert!(won);
    let before = faulty.dispatched();
    let stale = reg.mutate_incarnation(&s, E1, bump).await;
    assert!(
        matches!(stale, Ok(MutationResult::IncarnationChanged)),
        "{}: {stale:?}",
        b.name
    );
    assert_eq!(faulty.dispatched(), before, "{}", b.name);
    assert_eq!(stored(&reg, &s).await.seal_gen_counter, 0);
}

/// Registries with separate caches race non-idempotent mutations of one
/// descriptor. Each outcome is applied (with the value it wrote), a
/// conflict after the bounded retries, or, on a real provider only,
/// ambiguous. No applied value repeats and the stored counter equals the
/// number applied (plus at most the ambiguous ones).
async fn racing_mutators_lose_no_update(b: &Backend) {
    let registries: Vec<Registry> = (0..MUTATORS).map(|_| registry(b.ops.clone())).collect();
    let s = sref(b, "mutator-race");
    registries[0]
        .create(descriptor(b, "mutator-race", E1, false))
        .await
        .unwrap();
    let (mut applied, mut conflicts, mut ambiguous) = (Vec::new(), 0u64, 0u64);
    for _ in 0..RACE_ROUNDS {
        let results = join_all(
            registries
                .iter()
                .map(|reg| reg.mutate_incarnation(&s, E1, bump)),
        )
        .await;
        for result in results {
            match result {
                Ok(MutationResult::Applied(written)) => applied.push(written),
                Err(MutationError::Conflict(_)) => conflicts += 1,
                Err(MutationError::AmbiguousCompletion(_) | MutationError::ReadUnavailable(_))
                    if !b.local =>
                {
                    ambiguous += 1;
                }
                other => panic!("{}: racing mutator: {other:?}", b.name),
            }
        }
    }
    let counter = stored(&registries[0], &s).await.seal_gen_counter;
    applied.sort_unstable();
    let distinct = {
        let mut values = applied.clone();
        values.dedup();
        values.len()
    };
    assert_eq!(distinct, applied.len(), "{}: {applied:?}", b.name);
    assert!(!applied.is_empty(), "{}: no mutator applied", b.name);
    let applied_count = applied.len() as u64;
    assert!(
        counter >= applied_count && counter <= applied_count + ambiguous,
        "{}: stored {counter}, applied {applied:?}, ambiguous {ambiguous}",
        b.name
    );
    if ambiguous == 0 {
        assert_eq!(applied, (1..=counter).collect::<Vec<_>>(), "{}", b.name);
    }
    eprintln!(
        "{}: mutator race: {} applied, {conflicts} conflicts, {ambiguous} ambiguous",
        b.name,
        applied.len()
    );
}

/// Racing recreators of one dead incarnation: exactly one installs its
/// epoch; every other observes the winner instead of overwriting it.
async fn recreate_race(b: &Backend, name: String) -> Option<()> {
    let registries: Vec<Registry> = (0..MUTATORS).map(|_| registry(b.ops.clone())).collect();
    let s = sref(b, &name);
    registries[0]
        .create(descriptor(b, &name, E1, true))
        .await
        .ok()?;
    let epochs: Vec<String> = (2..2 + MUTATORS).map(|i| format!("{i:032x}")).collect();
    let results = join_all(
        registries
            .iter()
            .zip(&epochs)
            .map(|(reg, epoch)| reg.recreate(&s, descriptor(b, &name, epoch, false), dead)),
    )
    .await;
    if results.iter().any(Result::is_err) {
        assert!(!b.local, "{}: recreate race: {results:?}", b.name);
        return None;
    }
    let outcomes: Vec<(bool, String)> = results
        .into_iter()
        .flatten()
        .map(|(won, current)| (won, current.stream_epoch.clone()))
        .collect();
    let winners: Vec<&(bool, String)> = outcomes.iter().filter(|(won, _)| *won).collect();
    assert_eq!(winners.len(), 1, "{}: recreators {outcomes:?}", b.name);
    let winner = &winners[0].1;
    assert!(
        outcomes.iter().all(|(_, epoch)| epoch == winner),
        "{}: a loser saw another incarnation: {outcomes:?}",
        b.name
    );
    assert_eq!(&stored(&registries[0], &s).await.stream_epoch, winner);
    Some(())
}
