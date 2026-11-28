//! Item 40, the owner's brownout requirement, in both boot orders (GATE-DECISION-s22-join-2026-11-20
//! Addendum 26): a slow store stretches every fleet pass to several tick periods, and that is not a
//! stuck controller. The heartbeat keeps its own cadence, the progress deadline is derived from the
//! budgets a pass may use, so neither instance ever leaves the other's ring. Its window starts once
//! §22's join has listed both and every pin has returned: at 600 ms an operation a planned move's
//! commit fits its hold (Addendum 26 (a)), so the pin the first boot wrote is returned.

use super::super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::super::fixture_runtime::RigRuntime;
use super::super::fixture_storage::mem;
use super::{HeldDocument, seed_ring_of_two, settled};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// Member `instance` (incarnation `incarnation`) over `store`, its fleet started.
async fn member(store: &Arc<dyn ObjectStore>, (incarnation, instance): (u64, &str)) -> HttpRig {
    let rig = http_rig_build(
        mem(),
        RigRuntime::incarnation(incarnation),
        HttpRigOptions {
            fleet_store: Some(store.clone()),
            instance: Some(instance.into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

/// The two members over `store`, `first` booted and listed with its own boot id (read on `inner`,
/// the store without the brownout) before the other boots: the boot order the case names, not left
/// to chance.
async fn in_order(
    store: &Arc<dyn ObjectStore>,
    inner: &Arc<dyn ObjectStore>,
    first: &str,
) -> Vec<HttpRig> {
    let both: [(u64, &str); 2] = [(0, "streams-1"), (1, "streams-2")];
    let (early, late): (Vec<_>, Vec<_>) = both.into_iter().partition(|(_, name)| *name == first);
    let a = member(store, early[0]).await;
    let boot = a.state.runtime.identity.boot_id.clone();
    let t0 = tokio::time::Instant::now();
    loop {
        let read = inner.get(&Path::from("fleet/overrides.json")).await;
        let doc: Option<crate::fleet::Overrides> = match read {
            Ok(got) => serde_json::from_slice(&got.bytes().await.unwrap()).ok(),
            Err(_) => None,
        };
        let members = doc.and_then(|doc| doc.members).unwrap_or_default();
        if members
            .iter()
            .any(|m| m.name == first && m.boot_id.as_deref() == Some(boot.as_str()))
        {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "{first} is listed first"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let b = member(store, late[0]).await;
    vec![a, b]
}

/// The tick's membership passes a rig completes before its heartbeat reads must show (Addendum 72).
const FIRST_PASSES: u32 = 2;

/// Fails, by state, once a rig's tick has completed its first passes with no heartbeat read
/// counted (Addendum 72): every pass follows its tick's heartbeat read, however slow the store, so
/// a late read only delays the pass, and passes with none begun mean the tick reads none. Never
/// returns.
async fn reads_beats(rigs: &[HttpRig]) {
    loop {
        for (index, rig) in rigs.iter().enumerate() {
            let standing = rig.state.fleet.standing();
            let (passes, reads) = (standing.join_passes(), standing.dead().reads());
            assert!(
                passes < FIRST_PASSES || reads > 0,
                "rig {index}'s tick completed {passes} passes with no heartbeat read counted"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The brownout scenario with `first` booted first.
async fn browned_out(first: &str) {
    let inner = mem();
    seed_ring_of_two(&inner, &[]).await;
    let store: Arc<dyn ObjectStore> = Arc::new(HeldDocument {
        delay: Duration::from_millis(600),
        ..HeldDocument::held(inner.clone(), "", false)
    });
    let rigs = in_order(&store, &inner, first).await;
    tokio::select! {
        () = super::super::fixture_members::all_joined(&inner, &rigs, Duration::from_secs(180)) => {}
        () = reads_beats(&rigs) => {}
    }
    let ring = vec!["streams-1".to_string(), "streams-2".to_string()];
    let both = || {
        rigs.iter()
            .all(|rig| rig.state.ownership.ring_active() == ring)
    };
    settled(Duration::from_secs(16), both).await;
    assert!(
        both(),
        "both instances must converge on the two-member ring"
    );
    let mut beats = std::collections::BTreeMap::new();
    let window = tokio::time::Instant::now() + Duration::from_secs(12);
    while tokio::time::Instant::now() < window {
        assert!(both(), "a brownout must not churn the ring");
        for instance in ["streams-1", "streams-2"] {
            let path = Path::from(format!("fleet/{instance}.json"));
            let bytes = inner.get(&path).await.unwrap().bytes().await.unwrap();
            let beat: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            beats.insert(
                (instance, beat["ts_ms"].as_i64()),
                beat["progress_age_ms"].as_u64(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for instance in ["streams-1", "streams-2"] {
        let ages: Vec<Option<u64>> = beats
            .iter()
            .filter(|((name, _), _)| *name == instance)
            .map(|(_, age)| *age)
            .collect();
        assert!(
            ages.len() >= 5,
            "{instance} must keep its heartbeat cadence through the brownout: {} beats in 12 s",
            ages.len()
        );
        let passes = ages.windows(2).filter(|pair| pair[1] < pair[0]).count();
        assert!(
            passes >= 1,
            "{instance}'s controller must keep completing passes: {ages:?}"
        );
    }
    for rig in &rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// The brownout with `streams-1` booted first: it lists both and holds `""`, its own over the ring
/// of two, so no pin is written and `streams-2` joins at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_store_brownout_slows_the_tick_without_churning_the_ring_streams_1_first() {
    browned_out("streams-1").await;
}

/// The brownout with `streams-2` booted first: it holds `""`, which the ring of two gives
/// `streams-1`, so it pins it; `streams-1` joins covered, and the pin's return, a planned move,
/// commits within its hold at 600 ms an operation. Red with the commit bounded by a fixed 400 ms
/// (the handoff as on d9d60b947): every return ends `Kept` (`release_ms=40x end=Kept`) and the
/// join's wait runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_store_brownout_slows_the_tick_without_churning_the_ring_streams_2_first() {
    browned_out("streams-2").await;
}
