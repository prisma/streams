#![cfg(test)]

use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use hyper::body::Body as _;

use super::{READ_MEMORY_WAIT, ReadHold, ReadRefusal};
use crate::admission::{AdmissionController, AdmissionKnobs, SubscriptionCapacity};
use crate::project_policy::ProjectQuotas;
use crate::quota::QuotaRegistry;
use crate::tenant::ProjectId;

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// A controller whose read memory is a quarter of `rss_shed_mb`.
fn ctl(rss_shed_mb: u64) -> AdmissionController {
    AdmissionController::new(AdmissionKnobs {
        max_inflight: 0,
        per_stream_cap: 0,
        rss_shed_mb,
        project_memory_pressure_bytes: 0,
        project_memory_release_pct: 75,
        subscriptions: SubscriptionCapacity {
            effective: 0,
            configured: 0,
        },
        record_ceiling_bytes: 0,
    })
}

fn project(registry: &QuotaRegistry) -> ProjectId {
    let id = ProjectId::new("proj-a").unwrap();
    drop(registry.admit(&id, &ProjectQuotas::default(), 0).unwrap());
    id
}

/// The instance's read memory is a quarter of the RSS shed line, and none
/// without one.
#[test]
fn the_read_memory_is_a_quarter_of_the_shed_line() {
    assert_eq!(ctl(500).read_memory(), (0, 125 * MIB));
    assert_eq!(ctl(0).read_memory(), (0, 0));
}

/// A hold's two ledgers move together: the reservation, its release while
/// the request waits, the first rendered page replacing it exactly, a
/// second page adding to it, and the drop releasing everything.
#[tokio::test]
async fn a_hold_reserves_releases_while_waiting_and_settles_to_its_pages() {
    let ctl = ctl(500);
    let registry = QuotaRegistry::default();
    let id = project(&registry);
    let entry = registry.pressure_handle(&id).unwrap();
    let ledgers = || (ctl.read_memory().0, entry.estimated_pressure_bytes());
    let hold = ReadHold::reserve(&ctl, registry.read_bytes(&id), 0, 8 * MIB)
        .await
        .unwrap();
    assert_eq!(ledgers(), (8 * MIB, 8 * MIB), "reserved");
    hold.unreserve();
    assert_eq!(ledgers(), (0, 0), "waiting");
    hold.settle(1_000);
    assert_eq!(ledgers(), (1_000, 1_000), "the first page");
    hold.unreserve();
    assert_eq!(ledgers(), (1_000, 1_000), "a rendered page stays held");
    hold.settle(500);
    assert_eq!(ledgers(), (1_500, 1_500), "a second page");
    drop(hold);
    assert_eq!(ledgers(), (0, 0), "dropped");
}

/// Over a 1 MiB budget, a 768 KiB read waits while another holds 512 KiB
/// and is admitted when it releases; a read alone is admitted whatever its
/// size.
#[tokio::test(start_paused = true)]
async fn a_read_waits_for_the_instance_memory_another_read_releases() {
    let ctl = ctl(4);
    assert_eq!(ctl.read_memory(), (0, MIB));
    let first = ReadHold::reserve(&ctl, None, 0, 512 * KIB).await.unwrap();
    let release = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(first);
    };
    let started = tokio::time::Instant::now();
    let (second, ()) =
        futures_util::future::join(ReadHold::reserve(&ctl, None, 0, 768 * KIB), release).await;
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    assert_eq!(ctl.read_memory().0, 768 * KIB);
    drop(second);
    let alone = ReadHold::reserve(&ctl, None, 0, 8 * MIB).await.unwrap();
    assert_eq!(ctl.read_memory().0, 8 * MIB, "alone, over the budget");
    drop(alone);
}

/// A read that finds either ledger full for the whole wait is refused for
/// that ledger, with nothing reserved in either: the instance's memory
/// held by another read, or its project's line held by its own. A refusal
/// at the line is one of the project's memory sheds.
#[tokio::test(start_paused = true)]
async fn a_read_is_refused_for_the_ledger_that_stayed_full_for_the_wait() {
    let ctl = ctl(4);
    let registry = QuotaRegistry::default();
    let id = project(&registry);
    let entry = registry.pressure_handle(&id).unwrap();
    let ledgers = || (ctl.read_memory().0, entry.estimated_pressure_bytes());
    let held = ReadHold::reserve(&ctl, None, 0, 900 * KIB).await.unwrap();
    let started = tokio::time::Instant::now();
    let refused = ReadHold::reserve(&ctl, registry.read_bytes(&id), 0, 200 * KIB).await;
    assert_eq!(refused.err(), Some(ReadRefusal::Instance));
    assert_eq!(started.elapsed(), READ_MEMORY_WAIT);
    assert_eq!(ledgers(), (900 * KIB, 0));
    drop(held);
    let line = MIB;
    let own = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 768 * KIB)
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let refused = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 512 * KIB).await;
    assert_eq!(refused.err(), Some(ReadRefusal::Project));
    assert_eq!(started.elapsed(), READ_MEMORY_WAIT);
    assert_eq!(ledgers(), (768 * KIB, 768 * KIB));
    let shed = &registry.memory_pressure_json(1, 8)["project_memory_shed_total"];
    assert_eq!(shed.as_u64(), Some(1));
    drop(own);
}

/// A read waiting at its project's line is admitted when the project's own
/// read releases, and only then.
#[tokio::test(start_paused = true)]
async fn a_read_waits_at_its_projects_line_for_its_own_reads() {
    let ctl = ctl(500);
    let registry = QuotaRegistry::default();
    let id = project(&registry);
    let line = 12 * MIB;
    let first = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 8 * MIB)
        .await
        .unwrap();
    let release = async {
        tokio::time::sleep(Duration::from_millis(700)).await;
        drop(first);
    };
    let started = tokio::time::Instant::now();
    let (second, ()) = futures_util::future::join(
        ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 8 * MIB),
        release,
    )
    .await;
    assert!(second.is_ok());
    assert_eq!(started.elapsed(), Duration::from_millis(700));
    assert_eq!(ctl.read_memory().0, 8 * MIB);
}

/// A woken wait takes its admission reservation again before it renders:
/// at once while it fits, when a release makes room while it waits, and
/// not at all when no room comes by its deadline, which takes nothing and
/// counts one memory shed when the project's line refused it. A rendered
/// hold, and one no admission reserved, take nothing.
#[tokio::test(start_paused = true)]
async fn a_woken_wait_takes_its_reservation_again_or_ends_at_its_deadline() {
    let ctl = ctl(500);
    let line = 12 * MIB;
    ctl.set_project_memory_pressure_bytes(line);
    let registry = QuotaRegistry::default();
    let id = project(&registry);
    let entry = registry.pressure_handle(&id).unwrap();
    let ledgers = || (ctl.read_memory().0, entry.estimated_pressure_bytes());
    let woken = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 8 * MIB)
        .await
        .unwrap();
    woken.unreserve();
    assert!(woken.resume(tokio::time::Instant::now()).await, "room");
    assert_eq!(ledgers(), (8 * MIB, 8 * MIB), "taken again");

    woken.unreserve();
    let other = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 8 * MIB)
        .await
        .unwrap();
    let release = async {
        tokio::time::sleep(Duration::from_millis(400)).await;
        drop(other);
    };
    let started = tokio::time::Instant::now();
    let deadline = started + READ_MEMORY_WAIT;
    let (resumed, ()) = futures_util::future::join(woken.resume(deadline), release).await;
    assert_eq!(
        (resumed, started.elapsed()),
        (true, Duration::from_millis(400))
    );
    assert_eq!(ledgers(), (8 * MIB, 8 * MIB), "taken when room came");

    woken.unreserve();
    let other = ReadHold::reserve(&ctl, registry.read_bytes(&id), line, 8 * MIB)
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(1_500);
    assert!(!woken.resume(deadline).await, "no room by the deadline");
    assert_eq!(started.elapsed(), Duration::from_millis(1_500));
    assert_eq!(ledgers(), (8 * MIB, 8 * MIB), "nothing taken");
    let shed = &registry.memory_pressure_json(1, 8)["project_memory_shed_total"];
    assert_eq!(shed.as_u64(), Some(1));
    drop(other);

    woken.settle(1_000);
    assert!(woken.resume(tokio::time::Instant::now()).await);
    assert_eq!(ledgers(), (1_000, 1_000), "a rendered hold takes nothing");
    let unreserved = ReadHold::unreserved(&ctl, registry.read_bytes(&id));
    assert!(unreserved.resume(tokio::time::Instant::now()).await);
    assert_eq!(ledgers(), (1_000, 1_000), "nor one no admission reserved");
}

/// A rendered page's hold rides its body: the body keeps its exact length,
/// yields frames of at most 64 KiB, and releases the hold only when it is
/// dropped. A hold that rendered nothing is released when the response is
/// answered.
#[tokio::test]
async fn a_rendered_hold_rides_its_body_and_an_unrendered_one_is_released() {
    let ctl = ctl(500);
    let page = Bytes::from(vec![7u8; 200 * 1024]);
    let hold = ReadHold::reserve(&ctl, None, 0, 8 * MIB).await.unwrap();
    hold.settle(page.len() as u64);
    let response = hold.attach(Response::new(Body::from(page.clone())));
    let mut body = response.into_body();
    assert_eq!(body.size_hint().exact(), Some(200 * 1024));
    let mut frames = Vec::new();
    while let Some(frame) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
    {
        frames.push(frame.unwrap().into_data().unwrap());
        assert_eq!(ctl.read_memory().0, 200 * KIB, "held while it streams");
    }
    let sizes: Vec<usize> = frames.iter().map(Bytes::len).collect();
    assert_eq!(sizes, vec![65_536, 65_536, 65_536, 8_192]);
    assert_eq!(frames.concat(), page.to_vec());
    drop(body);
    assert_eq!(ctl.read_memory().0, 0, "released with the body");
    let unrendered = ReadHold::reserve(&ctl, None, 0, 8 * MIB).await.unwrap();
    let response = unrendered.attach(Response::new(Body::from("refused")));
    assert_eq!(ctl.read_memory().0, 0, "a refusal holds nothing");
    drop(response);
}

/// Capacity review C5: the shared-cell profile's memory line
/// (`deploy/profiles/shared-cell.env`, `PROJECT_MEMORY_PRESSURE_BYTES` =
/// 16,384,000) is below two default page budgets (2 x 8 MiB = 16,777,216),
/// so one project's second concurrent read without `maxBytes` waits for
/// its first and is refused after 2 s while the first still runs (a cold
/// read): a compliant project reads one default page at a time.
#[tokio::test(start_paused = true)]
#[ignore = "red until the owner decides capacity review C5: the shared-cell line admits one default read per project"]
async fn the_shared_cell_line_admits_two_default_reads_of_one_project_at_once() {
    const PROFILE_LINE: u64 = 16_384_000;
    let ctl = ctl(500);
    let registry = QuotaRegistry::default();
    let id = project(&registry);
    let first = ReadHold::reserve(&ctl, registry.read_bytes(&id), PROFILE_LINE, 8 * MIB).await;
    assert!(first.is_ok(), "the first default read");
    let second = ReadHold::reserve(&ctl, registry.read_bytes(&id), PROFILE_LINE, 8 * MIB)
        .await
        .map(drop);
    assert_eq!(second, Ok(()), "the second default read of one project");
    drop(first);
}
