//! Idle release (Q0): an instance that has seen no data-plane request for two
//! minutes, with nothing left to absorb, releases its block caches and its
//! postings slices; health and operator requests do not hold it back, and the
//! next read returns the same records and starts a new quiet period.

use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::runtime::ManualClock;
use std::time::Duration;

const TICK: Duration = Duration::from_secs(2);

/// Real time between two manual ticks, for the loop to read each one.
const SPACING: Duration = Duration::from_millis(50);

/// Advances the manual clock `ticks` loop ticks, `SPACING` apart.
async fn tick(clock: &ManualClock, ticks: u32) {
    for _ in 0..ticks {
        clock.advance(TICK);
        tokio::time::sleep(SPACING).await;
    }
}

/// Advances the manual clock one loop tick at a time until `done` holds,
/// giving the loop up to 2 s of real time after each tick, at most `ticks`
/// ticks: a loaded host delays the loop's reading, never the outcome.
/// Whether it held.
async fn tick_until(clock: &ManualClock, ticks: u32, done: impl Fn() -> bool) -> bool {
    for _ in 0..ticks {
        clock.advance(TICK);
        for _ in 0..40 {
            if done() {
                return true;
            }
            tokio::time::sleep(SPACING).await;
        }
    }
    false
}

/// Both streams' records, as a reader sees them.
async fn records(addr: std::net::SocketAddr) -> Vec<Vec<u8>> {
    let mut all = Vec::new();
    for stream in ["idle-a", "idle-b"] {
        let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{stream}"), &[], b"").await;
        assert_eq!(status, 200, "read {stream}");
        all.push(body);
    }
    all
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_instance_releases_its_caches_and_the_next_read_refills_them() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr, clock) = (rig.state.clone(), rig.addr, rig.clock.clone());
    let ct = [("content-type", "application/json")];
    for stream in ["idle-a", "idle-b"] {
        let path = format!("/v1/stream/{stream}");
        let (status, _, _) = hreq(addr, "PUT", &path, &ct, b"").await;
        assert!(status == 200 || status == 201, "create {stream}: {status}");
        for n in 0..8 {
            let body = format!("[{{\"n\":{n},\"pad\":\"{}\"}}]", "x".repeat(2048));
            let (status, _, _) = hreq(addr, "POST", &path, &ct, body.as_bytes()).await;
            assert!(status == 200 || status == 204, "append {stream}: {status}");
        }
    }
    let runtime = &state.runtime;
    let absorbed = tokio::time::timeout(Duration::from_secs(30), async {
        while crate::backpressure::snapshot(&state.shards).unabsorbed_bytes_instance > 0
            || runtime.history.cache.inserted_bytes() == 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    absorbed
        .await
        .expect("the absorber flushed both streams into history, cached on write");
    let before = records(addr).await;
    assert_ne!(
        runtime.postings.stats()["entries"],
        0,
        "the absorber installed postings"
    );

    crate::bootstrap::idle::start(&state, &rig.tasks);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let released = || runtime.history.cache.releases();
    // Health and operator requests every 30 s do not count as arrivals:
    // nothing is released by 118 s, and the release follows.
    for ticks in [15, 15, 15, 14] {
        for path in ["/health", "/v1/debug/load"] {
            let (status, _, _) = hreq(addr, "GET", path, &[], b"").await;
            assert_eq!(status, 200, "{path}");
        }
        tick(&clock, ticks).await;
        assert_eq!(released(), 0, "released before the quiet period ended");
    }
    assert!(tick_until(&clock, 30, || released() > 0).await, "released");
    assert_eq!(released(), 1);
    assert_eq!(runtime.shared_cache.releases(), 1);
    assert_eq!(runtime.telemetry.cache.releases(), 1);
    assert_eq!(runtime.history.cache.inserted_bytes(), 0);
    assert_eq!(runtime.postings.stats()["entries"], 0);

    assert_eq!(records(addr).await, before, "a read after the release");
    tick(&clock, 58).await;
    assert_eq!(released(), 1, "the read started a new quiet period");
    assert!(
        tick_until(&clock, 30, || released() > 1).await,
        "released again"
    );
    rig.shutdown().await;
}
