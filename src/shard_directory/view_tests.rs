#![cfg(test)]
//! A fleet instance opens nothing before its first ring view (edge change #263,
//! GATE-DECISION-f3-fleet-flag conditions 3a and 3b): `ShardDirectory::open_viewed`, the one
//! guarded open of `resolve` and `open_or_wait`.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::directory_tests::{directory, directory_of, hash_in};
use super::{Adoption, ResolveError};
use crate::ownership::OwnershipService;
use crate::sharddir::OpenOutcome;

/// **A fleet instance opens nothing before its first view** (edge change #263,
/// GATE-DECISION-f3-fleet-flag condition 3a). A fleet ownership service with no published
/// view: the request path's `resolve` and the fleet's `open_or_wait` both answer the
/// retryable `no_ring_view` refusal, and the opener is never called. Once a view names this
/// instance, each of them starts an open (here the scripted opener fails it).
#[tokio::test]
async fn a_fleet_instance_opens_nothing_before_its_first_view() {
    let fleet = OwnershipService::of_fleet("a", true);
    let (dir, ownership, calls) = directory_of(fleet, || anyhow::bail!("scripted"));
    let refused = dir.resolve(&hash_in(&dir, "00"), Adoption::External).await;
    let waited = match dir.open_or_wait("01", Duration::from_secs(1)).await {
        OpenOutcome::Wait {
            code,
            retry_after_secs,
        } => Some((code, retry_after_secs)),
        _ => None,
    };
    let refusal = ResolveError::Opening {
        prefix: "00".to_string(),
        code: crate::ownership::NO_VIEW,
        retry_after_secs: 1,
    };
    let no_view = Some((crate::ownership::NO_VIEW, 1));
    assert_eq!((refused.err(), waited), (Some(refusal), no_view));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "the opener is never called"
    );
    assert_eq!(dir.open_stats()["started"], 0, "{}", dir.open_stats());
    ownership.set_view(vec!["a".to_string()], std::collections::HashMap::new());
    let opened = dir
        .resolve(&hash_in(&dir, "00"), Adoption::External)
        .await
        .err();
    assert!(
        matches!(opened, Some(ResolveError::OpenFailed { .. })),
        "{opened:?}"
    );
    let opened = dir.open_or_wait("01", Duration::from_secs(1)).await;
    assert!(
        matches!(opened, OpenOutcome::Failed(_)),
        "the fleet's open starts"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "each path called the opener once"
    );
}

/// **Fleet off, an empty view opens as before** (condition 3b): a service named "streams",
/// the default name, built for no fleet, opens on both paths with no view published.
#[tokio::test]
async fn fleet_off_an_empty_view_opens_as_before() {
    let (dir, ownership, calls) = directory("streams", || anyhow::bail!("scripted"));
    assert!(ownership.ring_active().is_empty());
    let opened = dir
        .resolve(&hash_in(&dir, "00"), Adoption::External)
        .await
        .err();
    assert!(
        matches!(opened, Some(ResolveError::OpenFailed { .. })),
        "{opened:?}"
    );
    let opened = dir.open_or_wait("01", Duration::from_secs(1)).await;
    assert!(
        matches!(opened, OpenOutcome::Failed(_)),
        "the fleet's open starts"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "each path called the opener once"
    );
}
