//! The raw conditional-write contract (ASM-OBJSTORE-CAS (a) and (b)):
//! competing creates and updates, stale and fabricated preconditions,
//! updates of missing objects, ETag presence and stability, and the user
//! metadata SlateDB's retrying store relies on.
#![cfg(test)]

use std::borrow::Cow;
use std::collections::HashMap;

use bytes::Bytes;
use futures_util::future::join_all;
use object_store::path::Path as ObjPath;
use object_store::{
    Attribute, Attributes, GetOptions, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};

use super::{Backend, Observations, conclusive};

const CREATORS: usize = 8;
const WRITERS: usize = 4;
const RACE_ROUNDS: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Written,
    AlreadyExists,
    Precondition,
    /// Anything else: on a real provider, possibly applied.
    Other,
}

fn outcome<T>(result: &object_store::Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Written,
        Err(object_store::Error::AlreadyExists { .. }) => Outcome::AlreadyExists,
        Err(object_store::Error::Precondition { .. }) => Outcome::Precondition,
        Err(_) => Outcome::Other,
    }
}

/// Every (ETag, content) pair the run observed: an ETag may repeat only for
/// byte-identical content.
#[derive(Default)]
struct EtagLedger(HashMap<String, Bytes>);

impl EtagLedger {
    fn record(&mut self, b: &Backend, etag: &str, content: &Bytes) {
        if let Some(earlier) = self.0.insert(etag.into(), content.clone()) {
            assert_eq!(
                &earlier, content,
                "{}: ETag {etag} named two different contents",
                b.name
            );
        }
    }
}

pub(super) fn create() -> PutOptions {
    PutOptions::from(PutMode::Create)
}

pub(super) fn update(etag: &str) -> PutOptions {
    PutOptions::from(PutMode::Update(UpdateVersion {
        e_tag: Some(etag.into()),
        version: None,
    }))
}

/// The stored content and the ETag a GET reports for it.
async fn read(b: &Backend, path: &ObjPath) -> (Option<String>, Bytes) {
    let got = b.ops.get(path).await.expect("read back a written object");
    let etag = got.meta.e_tag.clone();
    (etag, got.bytes().await.expect("read the object body"))
}

async fn read_etag(b: &Backend, path: &ObjPath) -> (String, Bytes) {
    let (etag, body) = read(b, path).await;
    let etag = etag.filter(|e| !e.is_empty());
    (
        etag.unwrap_or_else(|| panic!("{}: GET of {path} returned no ETag", b.name)),
        body,
    )
}

pub(super) async fn run(b: &Backend) -> Observations {
    let mut ledger = EtagLedger::default();
    for round in 0..RACE_ROUNDS {
        let label = format!("create race {round}");
        let (etag, body) = conclusive(b, &label, |attempt| {
            create_race(b, format!("create-race/{round}-{attempt}"))
        })
        .await;
        ledger.record(b, &etag, &body);
        let label = format!("update race {round}");
        let (etag, body) = conclusive(b, &label, |attempt| {
            update_race(b, format!("update-race/{round}-{attempt}"))
        })
        .await;
        ledger.record(b, &etag, &body);
    }
    refused_updates(b).await;
    let etag_repeats_for_identical_content = etag_stability(b, &mut ledger).await;
    Observations {
        etag_repeats_for_identical_content,
        metadata_round_trip: metadata(b).await,
    }
}

/// N concurrent creators of one path: exactly one is written, every other
/// answers `AlreadyExists`, and the stored content is the winner's. A later
/// create is refused and changes nothing.
async fn create_race(b: &Backend, name: String) -> Option<(String, Bytes)> {
    let path = b.path(&name);
    let payloads: Vec<Bytes> = (0..CREATORS)
        .map(|i| Bytes::from(format!("{name} creator {i}")))
        .collect();
    let results = join_all(
        payloads
            .iter()
            .map(|p| b.ops.put_opts(&path, PutPayload::from(p.clone()), create())),
    )
    .await;
    let outcomes: Vec<Outcome> = results.iter().map(outcome).collect();
    if outcomes.contains(&Outcome::Other) {
        return None;
    }
    let winners: Vec<usize> = (0..CREATORS)
        .filter(|&i| outcomes[i] == Outcome::Written)
        .collect();
    assert_eq!(winners.len(), 1, "{}: creators {outcomes:?}", b.name);
    let losers = outcomes
        .iter()
        .filter(|&&o| o == Outcome::AlreadyExists)
        .count();
    assert_eq!(losers, CREATORS - 1, "{}: creators {outcomes:?}", b.name);
    let winner = winners[0];
    let (etag, body) = read_etag(b, &path).await;
    assert_eq!(body, payloads[winner], "{}: stored content", b.name);
    if let Ok(put) = &results[winner]
        && let Some(put_etag) = &put.e_tag
    {
        assert_eq!(put_etag, &etag, "{}: PUT and GET ETags", b.name);
    }
    let late = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"late"), create())
        .await;
    if outcome(&late) == Outcome::Other {
        return None;
    }
    assert_eq!(outcome(&late), Outcome::AlreadyExists, "{}", b.name);
    assert_eq!(read_etag(b, &path).await, (etag.clone(), body.clone()));
    Some((etag, body))
}

/// Writers holding the same ETag: exactly one update commits, the others
/// answer `Precondition`; the stale ETag is then refused and changes
/// nothing.
async fn update_race(b: &Backend, name: String) -> Option<(String, Bytes)> {
    let path = b.path(&name);
    let first = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"v0"), create())
        .await;
    match outcome(&first) {
        Outcome::Written => {}
        Outcome::Other => return None,
        other => panic!("{}: fresh create answered {other:?}", b.name),
    }
    let (held, _) = read_etag(b, &path).await;
    let payloads: Vec<Bytes> = (0..WRITERS)
        .map(|i| Bytes::from(format!("{name} writer {i}")))
        .collect();
    let results = join_all(payloads.iter().map(|p| {
        b.ops
            .put_opts(&path, PutPayload::from(p.clone()), update(&held))
    }))
    .await;
    let outcomes: Vec<Outcome> = results.iter().map(outcome).collect();
    if outcomes.contains(&Outcome::Other) {
        return None;
    }
    let winners: Vec<usize> = (0..WRITERS)
        .filter(|&i| outcomes[i] == Outcome::Written)
        .collect();
    assert_eq!(winners.len(), 1, "{}: writers {outcomes:?}", b.name);
    let refused = outcomes
        .iter()
        .filter(|&&o| o == Outcome::Precondition)
        .count();
    assert_eq!(refused, WRITERS - 1, "{}: writers {outcomes:?}", b.name);
    let (etag, body) = read_etag(b, &path).await;
    assert_eq!(body, payloads[winners[0]], "{}: stored content", b.name);
    assert_ne!(etag, held, "{}: a committed update kept its ETag", b.name);
    if let Ok(put) = &results[winners[0]]
        && let Some(put_etag) = &put.e_tag
    {
        assert_eq!(put_etag, &etag, "{}: PUT and GET ETags", b.name);
    }
    let stale = b
        .ops
        .put_opts(&path, PutPayload::from_static(b"stale"), update(&held))
        .await;
    if outcome(&stale) == Outcome::Other {
        return None;
    }
    assert_eq!(outcome(&stale), Outcome::Precondition, "{}", b.name);
    assert_eq!(read_etag(b, &path).await, (etag.clone(), body.clone()));
    Some((etag, body))
}

/// An update of a missing object and one with an ETag the store never
/// issued are refused, and neither writes.
async fn refused_updates(b: &Backend) {
    let missing = b.path("update/missing");
    let ghost = b
        .ops
        .put_opts(&missing, PutPayload::from_static(b"ghost"), update("\"0\""))
        .await;
    assert_eq!(
        outcome(&ghost),
        Outcome::Precondition,
        "{}: {ghost:?}",
        b.name
    );
    assert!(
        matches!(
            b.ops.head(&missing).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{}: an update of a missing object created it",
        b.name
    );
    let path = b.path("update/fabricated");
    b.ops
        .put_opts(&path, PutPayload::from_static(b"real"), create())
        .await
        .expect("create the fabricated-ETag target");
    let before = read_etag(b, &path).await;
    let fabricated = b
        .ops
        .put_opts(
            &path,
            PutPayload::from_static(b"forged"),
            update("\"never-issued\""),
        )
        .await;
    assert_eq!(outcome(&fabricated), Outcome::Precondition, "{}", b.name);
    assert_eq!(read_etag(b, &path).await, before, "{}", b.name);
}

/// Distinct contents never share an ETag, repeated reads of one version
/// report one ETag, and whether re-writing identical bytes reproduces an
/// ETag is recorded (the ABA it allows is over identical bytes only).
async fn etag_stability(b: &Backend, ledger: &mut EtagLedger) -> bool {
    let path = b.path("etag/object");
    let first = Bytes::from_static(b"content A");
    let second = Bytes::from_static(b"content B");
    b.ops
        .put_opts(&path, PutPayload::from(first.clone()), create())
        .await
        .expect("create the ETag object");
    let (e_first, body) = read_etag(b, &path).await;
    assert_eq!(read_etag(b, &path).await.0, e_first, "{}: re-read", b.name);
    ledger.record(b, &e_first, &body);
    b.ops
        .put_opts(&path, PutPayload::from(second.clone()), update(&e_first))
        .await
        .expect("update to the second content");
    let (e_second, body) = read_etag(b, &path).await;
    assert_eq!(body, second);
    assert_ne!(e_second, e_first, "{}: two contents, one ETag", b.name);
    ledger.record(b, &e_second, &body);
    b.ops
        .put_opts(&path, PutPayload::from(first.clone()), update(&e_second))
        .await
        .expect("update back to the first content");
    let (e_again, body) = read_etag(b, &path).await;
    assert_eq!(body, first);
    assert_ne!(e_again, e_second, "{}: two contents, one ETag", b.name);
    ledger.record(b, &e_again, &body);
    e_again == e_first
}

/// Every read of an existing object carries an ETag (without one the
/// registry cannot write at all), and reports whether user metadata written
/// with a PUT comes back on HEAD.
async fn metadata(b: &Backend) -> bool {
    let path = b.path("metadata/object");
    let key = Attribute::Metadata(Cow::Borrowed("slatedbputid"));
    let value = format!("contract-{}", b.project);
    let mut attributes = Attributes::new();
    attributes.insert(key.clone(), value.clone().into());
    b.ops
        .put_opts(
            &path,
            PutPayload::from_static(b"metadata"),
            PutOptions {
                mode: PutMode::Create,
                attributes,
                ..Default::default()
            },
        )
        .await
        .expect("create the metadata object");
    let head = b.ops.head(&path).await.expect("HEAD the metadata object");
    assert!(
        head.e_tag.as_deref().is_some_and(|e| !e.is_empty()),
        "{}: HEAD returned no ETag",
        b.name
    );
    read_etag(b, &path).await;
    let probe = b
        .ops
        .get_opts(
            &path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .expect("HEAD with attributes");
    probe
        .attributes
        .get(&key)
        .is_some_and(|v| v.as_ref() == value)
}
