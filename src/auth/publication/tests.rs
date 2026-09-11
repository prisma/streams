use std::collections::HashMap;
use std::sync::{Arc, Barrier};

use super::{AuthService, HighWater, JwksKey, JwksSnapshot, feed_fp};
use crate::auth::AuthMode;
use crate::project_policy::{
    CredentialGrant, CredentialStatus, GrantSnapshot, PolicySnapshot, ProjectPolicy, ProjectQuotas,
    ProjectStatus,
};
use crate::tenant::{ProjectId, ScopeSet, StreamGrant, WorkspaceId};

const WORKERS: u64 = 8;
const ROUNDS: u64 = 128;

#[derive(Clone, Copy, Debug)]
enum Feed {
    Keys,
    Policies,
    Grants,
}

impl Feed {
    fn publish(self, service: &AuthService, version: u64) -> Result<(), &'static str> {
        match self {
            Self::Keys => service.publish_jwks(JwksSnapshot {
                feed_version: version,
                ..JwksSnapshot::empty()
            }),
            Self::Policies => service.publish_policies(PolicySnapshot {
                feed_version: version,
                ..PolicySnapshot::empty()
            }),
            Self::Grants => service.publish_grants(GrantSnapshot {
                feed_version: version,
                ..GrantSnapshot::empty()
            }),
        }
    }

    fn version(self, service: &AuthService) -> u64 {
        match self {
            Self::Keys => service.jwks.load().feed_version,
            Self::Policies => service.projects.load().feed_version,
            Self::Grants => service.credentials.load().feed_version,
        }
    }
}

fn service() -> AuthService {
    AuthService::new(
        AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap()
}

/// Scoped threads always join before returning, including a worker assertion
/// failure. The barrier starts real simultaneous callers of the production API.
fn contend(feed: Feed) {
    let service = service();
    let start = Barrier::new(usize::try_from(WORKERS).unwrap());
    std::thread::scope(|scope| {
        for worker in 0..WORKERS {
            let service = &service;
            let start = &start;
            scope.spawn(move || publish_rounds(feed, service, start, worker));
        }
    });
    assert_eq!(feed.version(&service), WORKERS * ROUNDS);
    assert_eq!(
        *service.generation_watch().borrow(),
        service.auth_generation()
    );
}

fn publish_rounds(feed: Feed, service: &AuthService, start: &Barrier, worker: u64) {
    start.wait();
    for round in 0..ROUNDS {
        let version = round * WORKERS + worker + 1;
        if feed.publish(service, version).is_ok() {
            assert!(feed.version(service) >= version, "{feed:?} moved backward");
        }
    }
}

#[test]
fn concurrent_jwks_publications_preserve_snapshot_order() {
    contend(Feed::Keys);
}

#[test]
fn concurrent_policy_publications_preserve_snapshot_order() {
    contend(Feed::Policies);
}

#[test]
fn concurrent_grant_publications_preserve_snapshot_order() {
    contend(Feed::Grants);
}

fn policy(version: u64) -> ProjectPolicy {
    ProjectPolicy {
        project_id: ProjectId::new("proj-publish").unwrap(),
        workspace_id: WorkspaceId::new("ws_publish").unwrap(),
        cell_id: Arc::from("test-cell"),
        project_policy_version: version,
        ownership_version: version,
        status: ProjectStatus::Active,
        quotas: ProjectQuotas::default(),
    }
}

fn grant(version: u64) -> CredentialGrant {
    CredentialGrant {
        credential_id: Arc::from("cred-publish"),
        project_id: ProjectId::new("proj-publish").unwrap(),
        grant_version: version,
        status: CredentialStatus::Active,
        scopes: ScopeSet::parse("streams.records.read").0,
        grant: StreamGrant::All,
        expires_at: None,
    }
}

fn keys(version: u64, fingerprint: u8) -> JwksSnapshot {
    let public = include_bytes!("../../dst/fixtures/mt-test-rsa.pub.pem");
    JwksSnapshot {
        keys: HashMap::from([(
            "key-publish".into(),
            JwksKey {
                alg: jsonwebtoken::Algorithm::RS256,
                key: jsonwebtoken::DecodingKey::from_rsa_pem(public).unwrap(),
                fp: [fingerprint; 32],
            },
        )]),
        fetched_at_unix: 100,
        feed_version: version,
    }
}

fn history(service: &AuthService) -> [u8; 32] {
    feed_fp(&*service.high_water.lock().unwrap())
}

#[test]
fn refused_policy_changes_neither_snapshot_history_nor_generation() {
    let service = service();
    let original = policy(7);
    service
        .publish_policies(PolicySnapshot {
            projects: HashMap::from([(original.project_id.clone(), original.clone())]),
            fetched_at_unix: 100,
            feed_version: 10,
        })
        .unwrap();
    let before = service.projects.load_full();
    let history_before = history(&service);
    let generation = service.auth_generation();
    let mut changed = original;
    changed.status = ProjectStatus::Suspended;
    assert_eq!(
        service.publish_policies(PolicySnapshot {
            projects: HashMap::from([(changed.project_id.clone(), changed)]),
            fetched_at_unix: 200,
            feed_version: 11,
        }),
        Err("same project version with different content")
    );
    assert!(Arc::ptr_eq(&before, &service.projects.load_full()));
    assert_eq!(history(&service), history_before);
    assert_eq!(service.auth_generation(), generation);
    assert_eq!(*service.generation_watch().borrow(), generation);
}

#[test]
fn refused_grant_changes_neither_snapshot_history_nor_generation() {
    let service = service();
    let original = grant(7);
    service
        .publish_grants(GrantSnapshot {
            credentials: HashMap::from([(original.credential_id.clone(), original.clone())]),
            fetched_at_unix: 100,
            feed_version: 10,
        })
        .unwrap();
    let before = service.credentials.load_full();
    let history_before = history(&service);
    let generation = service.auth_generation();
    let mut changed = original;
    changed.status = CredentialStatus::Revoked;
    assert_eq!(
        service.publish_grants(GrantSnapshot {
            credentials: HashMap::from([(changed.credential_id.clone(), changed)]),
            fetched_at_unix: 200,
            feed_version: 11,
        }),
        Err("same grant_version with different content")
    );
    assert!(Arc::ptr_eq(&before, &service.credentials.load_full()));
    assert_eq!(history(&service), history_before);
    assert_eq!(service.auth_generation(), generation);
    assert_eq!(*service.generation_watch().borrow(), generation);
}

#[test]
fn refused_key_rebinding_changes_neither_snapshot_history_nor_generation() {
    let service = service();
    service.publish_jwks(keys(10, 1)).unwrap();
    let before = service.jwks.load_full();
    let history_before = history(&service);
    let generation = service.auth_generation();
    assert_eq!(
        service.publish_jwks(keys(11, 2)),
        Err("kid rebound to different key material")
    );
    assert!(Arc::ptr_eq(&before, &service.jwks.load_full()));
    assert_eq!(history(&service), history_before);
    assert_eq!(service.auth_generation(), generation);
    assert_eq!(*service.generation_watch().borrow(), generation);
}

#[test]
fn partial_history_poison_refuses_every_feed_publisher() {
    let service = service();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut history = service.high_water.lock().unwrap();
        history.policy_gen = Some((10, [1; 32]));
        panic!("simulated interruption between history and snapshot publication");
    }));
    assert!(poisoned.is_err());
    for feed in [Feed::Keys, Feed::Policies, Feed::Grants] {
        let attempt =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| feed.publish(&service, 11)));
        assert!(
            attempt.is_err(),
            "{feed:?} must not recover partial history"
        );
        assert_eq!(feed.version(&service), 0);
    }
    assert_eq!(service.auth_generation(), 0);
    assert_eq!(*service.generation_watch().borrow(), 0);
}

#[test]
fn omitted_records_require_new_versions_and_keep_one_fifo_entry_per_identity() {
    let mut history = HighWater::default();
    let original = policy(10);
    let credential = grant(10);
    history.remember_project(&original.project_id, &original);
    history.remember_credential(&credential.credential_id, &credential);
    history
        .projects
        .get_mut(&original.project_id)
        .unwrap()
        .omitted_at = Some((10, 10));
    history
        .credentials
        .get_mut(&credential.credential_id)
        .unwrap()
        .omitted_at = Some(10);
    assert!(
        history.projects[&original.project_id]
            .check(&original)
            .is_err()
    );
    assert!(
        history.credentials[&credential.credential_id]
            .check(&credential)
            .is_err()
    );
    let next_project = policy(11);
    let next_credential = grant(11);
    history.projects[&original.project_id]
        .check(&next_project)
        .unwrap();
    history.credentials[&credential.credential_id]
        .check(&next_credential)
        .unwrap();
    history.remember_project(&next_project.project_id, &next_project);
    history.remember_credential(&next_credential.credential_id, &next_credential);
    assert!(history.projects[&original.project_id].omitted_at.is_none());
    assert!(
        history.credentials[&credential.credential_id]
            .omitted_at
            .is_none()
    );
    assert_eq!(history.p_order.len(), 1);
    assert_eq!(history.c_order.len(), 1);
}
