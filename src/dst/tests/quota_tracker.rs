//! The project tracker's size is the instance's `CONTAINER_TRACKER_MAX`
//! (tuning item 3b): an instance booted with a larger tracker admits more
//! distinct projects than the default 32,768 before it refuses one
//! `container_tracker_capacity`.

use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

/// A verified principal of project `proj-<i>` with no quotas of its own.
fn principal(i: usize) -> crate::auth::RequestPrincipal {
    crate::auth::RequestPrincipal {
        project_id: crate::tenant::ProjectId::new(&format!("proj-{i}")).unwrap(),
        quotas: crate::project_policy::ProjectQuotas::default(),
        credential_id: format!("c-{i}").into(),
        ownership_version: 1,
        grant_version: 1,
        scopes: crate::tenant::ScopeSet::default(),
        grant: crate::tenant::StreamGrant::All,
        expires_at: i64::MAX,
    }
}

/// Booted with `CONTAINER_TRACKER_MAX=65536`, project admission tracks
/// 40,000 distinct projects at once, each still holding its admission:
/// every one is admitted and the tracker holds exactly 40,000 entries.
/// At the default, project 32,769 was refused `container_tracker_capacity`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_larger_tracker_admits_more_projects_than_the_default() {
    let options = HttpRigOptions {
        cli: |cli| cli.project_tracker_max = 65_536,
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let mut held = Vec::with_capacity(40_000);
    let mut refused = Vec::new();
    for i in 0..40_000 {
        match crate::product::project_admission(&rig.state, Some(&principal(i))) {
            Ok(guard) => held.push(guard),
            Err(response) => refused.push((i, response.status().as_u16())),
        }
    }
    assert_eq!(
        refused.first(),
        None,
        "the first refused project and its status"
    );
    assert_eq!((held.len(), rig.state.quotas.stats().0), (40_000, 40_000));
    rig.shutdown().await;
}
