//! Runtime sweep.

use super::fixture_failpoints::sweep_lock;
use super::fixture_http::http_rig_cold_absorb;
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use std::sync::Arc;

/// Drain billing debt until the sweep's own probes read clean, so the
/// retention decision under test is the MAINTENANCE one.
#[expect(
    clippy::let_underscore_must_use,
    reason = "drain_billing_clean; the fixture drains billing debt until its own probes read clean; each pass's result is irrelevant to the cleanliness it polls for"
)]
async fn drain_billing_clean(state: &Arc<crate::http::AppState>, prefixes: &[&str]) {
    for _ in 0..200 {
        let _ = crate::billing::drain_once(state).await;
        let mut clean = true;
        for p in prefixes {
            let Some(e) = state.shards.open(p) else {
                continue;
            };
            let dirty = e
                .usage_dirty_scan()
                .await
                .map(|d| !d.is_empty())
                .unwrap_or(true);
            let finals = e
                .usage_month_finals()
                .await
                .map(|f| !f.is_empty())
                .unwrap_or(true);
            if dirty || finals {
                clean = false;
            }
        }
        if clean {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("billing debt never drained clean");
}

/// R27-2a: a restart leaves durable maintenance backlog on a shard no
/// customer touches. The sweep must open it, see the backlog, KEEP the
/// engine resident (its absorber is what drains cold debt), and close
/// it on a later sweep once the ledger reaches zero. Before this fix
/// the sweep closed it as "debt-free" after checking only billing rows,
/// and the backlog sat until customer traffic happened to reopen it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_shard_maintenance_debt_survives_the_sweep_and_drains() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig_cold_absorb(store, vec!["00".into()]).await;
    let ct = [("content-type", "application/json")];
    // Layout 4: routes are project-qualified, so the name that lands
    // in the pinned "00" shard is found by probing, not hardcoded.
    let name = (0..256)
        .map(|i| format!("cold-{i}"))
        .find(|n| {
            crate::registry::hash_bits(
                &crate::crypto::RouteHash::for_stream(&state.deployment.raw_adapter_sref(n)).0,
            )
            .starts_with("00")
        })
        .expect("a cold name landing in shard 00");
    let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    drain_billing_clean(&state, &["00"]).await;
    let engine = state.shards.open("00").unwrap();
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert!(ledger > 0, "backlog must exist before the cold phase");

    // Go cold through the production retirement protocol (remove +
    // holdoff + close as ONE step).
    state.shards.retire(
        "00",
        crate::shard_directory::RetirementReason::SweepEviction,
        |_, _| true,
    );
    state.shards.clear_holdoff("00"); // the fixture reopens at once
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Sweep: reopens the owned shard, sees maintenance debt with clean
    // billing, and must keep it resident.
    crate::billing::sweep_owned_outboxes(&state).await;
    let kept = state.shards.open("00");
    let kept = kept.expect("sweep must keep a maintenance-indebted shard resident");
    assert_eq!(
        kept.maintenance_snapshot().unabsorbed_frame_bytes,
        ledger,
        "the reopened engine restored the durable ledger"
    );

    // The debt drains (deterministically, via an exact retirement — the
    // absorber path is proven elsewhere; the subject here is the sweep
    // policy)...
    // Retire via the durable dirty index — the engine's own record of
    // which stream carries the backlog (identity-derivation-free).
    let dirty = kept.scan_dirty_streams().await.unwrap();
    assert_eq!(dirty.len(), 1, "exactly one indebted stream expected");
    let hash = dirty[0].0;
    let tail = kept.tail_fields(&hash).await.unwrap().unwrap();
    kept.submit_absorbed(hash, tail.next, tail.unabsorbed_bytes)
        .await;
    let mut drained = false;
    for _ in 0..400 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if kept.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
    }
    assert!(drained, "retirement never drained the kept engine");

    // ...and the NEXT sweep closes the now debt-free shard.
    crate::billing::sweep_owned_outboxes(&state).await;
    assert!(
        !state.shards.is_open("00"),
        "a drained sweep-opened shard must close"
    );
}

/// R27-2b: many cold indebted shards must not all stay resident — the
/// bound (SWEEP_MAINT_RESIDENT=2) holds, and the rotation gives a
/// DIFFERENT residency window on the next sweep so no shard starves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_residency_bound_rotates_over_many_indebted_shards() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    // Streams covering every physical shard.
    let mut covered: std::collections::HashSet<String> = Default::default();
    for i in 0..64 {
        if covered.len() == 4 {
            break;
        }
        let name = format!("cold-m{i}");
        let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        assert!(st == 200 || st == 201);
        let desc = state
            .registry
            .get(&state.deployment.raw_adapter_sref(&name))
            .await
            .unwrap()
            .unwrap();
        let seg = desc.resolve_segment("");
        let p = state.shards.prefix_for(&seg.shard_route);
        if covered.insert(p) {
            let (st, _, _) = hreq(
                addr,
                "POST",
                &format!("/v1/stream/{name}"),
                &ct,
                br#"[{"n":1}]"#,
            )
            .await;
            assert!(st == 200 || st == 204);
        }
    }
    assert_eq!(covered.len(), 4, "could not cover all four shards");
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;

    // All four go cold with durable backlog.
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    assert_eq!(engines.len(), 4);
    for e in engines {
        assert!(e.maintenance_snapshot().unabsorbed_frame_bytes > 0);
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let resident = |state: &Arc<crate::http::AppState>| -> Vec<String> {
        let mut v: Vec<String> = state.shards.held_prefixes();
        v.sort();
        v
    };
    crate::billing::sweep_owned_outboxes(&state).await;
    let kept1 = resident(&state);
    assert_eq!(
        kept1.len(),
        2,
        "residency bound must hold: got {kept1:?} (bound 2 of 4 indebted)"
    );
    // R28: residents hold their slot for SWEEP_RESIDENT_QUANTUM sweeps
    // (default 4) so each gets real drain time, then expire so the
    // rotation admits the others. Over quantum+2 further sweeps the
    // bound must hold EVERY time and the window must both move and
    // eventually cover all four indebted shards.
    let mut seen: std::collections::BTreeSet<String> = kept1.iter().cloned().collect();
    let mut moved = false;
    for i in 0..12 {
        crate::billing::sweep_owned_outboxes(&state).await;
        let kept = resident(&state);
        assert!(kept.len() <= 2, "bound must hold on sweep {i}: {kept:?}");
        if kept != kept1 {
            moved = true;
        }
        seen.extend(kept.into_iter());
    }
    assert!(moved, "residency window never moved off {kept1:?}");
    assert_eq!(
        seen.len(),
        4,
        "every indebted shard must get a residency turn: saw {seen:?}"
    );
}

/// R28: the scheduler's PEAK concurrently-held engine count must never
/// exceed the budget — the R27-2 version opened every owned shard to
/// discover debt and only bounded the post-sweep survivors, which
/// reproduced the cross-DB residency overlap during the sweep itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_peak_open_never_exceeds_the_budget() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    let mut covered: std::collections::HashSet<String> = Default::default();
    for i in 0..64 {
        if covered.len() == 4 {
            break;
        }
        let name = format!("peak-m{i}");
        let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        assert!(st == 200 || st == 201);
        let desc = state
            .registry
            .get(&state.deployment.raw_adapter_sref(&name))
            .await
            .unwrap()
            .unwrap();
        let seg = desc.resolve_segment("");
        let p = state.shards.prefix_for(&seg.shard_route);
        if covered.insert(p) {
            let (st, _, _) = hreq(
                addr,
                "POST",
                &format!("/v1/stream/{name}"),
                &ct,
                br#"[{"n":1}]"#,
            )
            .await;
            assert!(st == 200 || st == 204);
        }
    }
    assert_eq!(covered.len(), 4);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    crate::billing::sweep_open_peak_reset(&state);
    for _ in 0..8 {
        crate::billing::sweep_owned_outboxes(&state).await;
    }
    assert!(
        crate::billing::sweep_open_peak(&state) <= 2,
        "peak scheduler-held engines {} exceeded budget 2",
        crate::billing::sweep_open_peak(&state)
    );
}

/// R28: a customer request that races into a sweep-opened engine
/// revokes the scheduler's custody — the engine must NOT be closed
/// out from under in-flight traffic. engine_for bumps the touch
/// counter inside the shards-map read guard; the close re-checks
/// after taking the write lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn customer_race_into_a_sweep_opened_engine_prevents_its_close() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    // One stream with durable backlog on one shard.
    let name = "race-m0".to_string();
    let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
    assert!(st == 200 || st == 201);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(&name))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let prefix = state.shards.prefix_for(&seg.shard_route);
    let (st, _, _) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    // Cold: close every engine, then let ONE sweep re-open the debtor.
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    crate::billing::sweep_owned_outboxes(&state).await;
    assert!(
        state.shards.is_open(&prefix),
        "sweep must have re-opened the indebted shard {prefix}"
    );
    // Customer traffic adopts the engine (append -> engine_for touch).
    let (st, _, _) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":2}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "append through the sweep-opened engine: {st}"
    );
    // Many further sweeps (past budget churn AND the residence
    // quantum): the adopted engine must survive them all.
    for _ in 0..8 {
        crate::billing::sweep_owned_outboxes(&state).await;
    }
    assert!(
        state.shards.is_open(&prefix),
        "adopted engine was closed out from under customer traffic"
    );
    // And it still serves: the stream reads back.
    let (st, _, _) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(st, 200, "adopted engine must keep serving reads");
}

/// R29 custody: an engine with ANY external history — including a
/// customer who resolved it before the sweep could probe (the pre-mark
/// window), or who coalesced into the sweep's own in-flight open —
/// makes custody installation DECLINE. The scheduler never closes an
/// engine it could not take custody of.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custody_declines_on_prior_external_use() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/pre-mark-1", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/pre-mark-1", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("pre-mark-1"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let prefix = state.shards.prefix_for(&seg.shard_route);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    // Cold-close, then reproduce the reviewer's ordering EXACTLY: the
    // open completes (engine publicly visible), a customer resolves it
    // (external stamp), and only THEN would the sweep mark it.
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let crate::sharddir::OpenOutcome::Ready(engine) = state
        .shards
        .open_or_wait(&prefix, std::time::Duration::from_secs(20))
        .await
    else {
        panic!("open failed");
    };
    // The customer resolution (external): same stamping path a
    // coalesced get_or_open Ready takes.
    let _ = state.engine_for(&seg.shard_route).await.unwrap();
    // Sweep now audits/discovers: custody must DECLINE and the engine
    // must survive any number of sweeps.
    for _ in 0..6 {
        crate::billing::sweep_owned_outboxes(&state).await;
    }
    assert!(
        state.shards.is_open(&prefix),
        "engine with prior external use must never be sweep-closed"
    );
    assert_eq!(
        engine
            .sweep_custody
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "custody must not be installed over external history"
    );
}

/// R29 custody: INTERNAL resolutions (tombstone walk, scaler) never
/// stamp adoption — a sweep-held engine touched by maintenance stays
/// under scheduler control and still closes when its debt drains or
/// its quantum expires, instead of leaking out of the rotation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_touch_does_not_leak_an_engine_from_the_rotation() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/itouch-1", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/itouch-1", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("itouch-1"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let prefix = state.shards.prefix_for(&seg.shard_route);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // One sweep opens + retains the indebted shard under custody.
    crate::billing::sweep_owned_outboxes(&state).await;
    let engine = state
        .shards
        .open(&prefix)
        .expect("sweep must retain the indebted shard");
    let custody0 = engine
        .sweep_custody
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_ne!(custody0, 0, "sweep must hold custody of its resident");
    // Maintenance-style internal touches: must NOT revoke custody.
    for _ in 0..3 {
        let _ = state.engine_for_quiet(&seg.shard_route).await.unwrap();
    }
    assert_eq!(
        engine
            .sweep_custody
            .load(std::sync::atomic::Ordering::Relaxed),
        custody0,
        "internal resolution must not revoke scheduler custody"
    );
    // Let the debt drain while resident, then sweeps must CLOSE it —
    // proof the engine never left the scheduler's rotation.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        crate::billing::sweep_owned_outboxes(&state).await;
        if !state.shards.is_open(&prefix) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sweep never reclaimed the internally-touched resident"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// R29: the tombstone walk must consume the SAME scheduler budget
/// BEFORE opening. Terminal (TTL-expired) descriptors spanning every
/// physical shard used to open one engine per route for the whole
/// page and only bound afterwards; now over-budget routes defer to
/// the next sweep and the peak gauge covers walk opens too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_walk_peak_residency_stays_under_the_budget() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json"), ("stream-ttl", "1")];
    // TTL streams with data covering all four shards.
    let mut covered: std::collections::HashSet<String> = Default::default();
    let mut names = Vec::new();
    for i in 0..64 {
        if covered.len() == 4 {
            break;
        }
        let name = format!("tomb-m{i}");
        let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        assert!(st == 200 || st == 201);
        let desc = state
            .registry
            .get(&state.deployment.raw_adapter_sref(&name))
            .await
            .unwrap()
            .unwrap();
        let seg = desc.resolve_segment("");
        let p = state.shards.prefix_for(&seg.shard_route);
        if covered.insert(p) {
            let (st, _, _) = hreq(
                addr,
                "POST",
                &format!("/v1/stream/{name}"),
                &ct,
                br#"[{"n":1}]"#,
            )
            .await;
            assert!(st == 200 || st == 204);
            names.push(name);
        }
    }
    assert_eq!(covered.len(), 4);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    // Cold-close everything, let the TTL lapse so every descriptor is
    // terminal, and force fresh registry reads.
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    for n in &names {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref(n));
    }
    crate::billing::sweep_open_peak_reset(&state);
    // Several sweeps: the walk pages terminal descriptors on all four
    // shards; deferred routes get their turn on later sweeps.
    for _ in 0..8 {
        crate::billing::sweep_owned_outboxes(&state).await;
    }
    assert!(
        crate::billing::sweep_open_peak(&state) <= 2,
        "walk drove scheduler-held engines to {} (budget 2)",
        crate::billing::sweep_open_peak(&state)
    );
}

/// R30: walk FAIRNESS, not just bounded peak. Two shards carry pinned
/// maintenance debt (absorber paused) and occupy the residency budget;
/// TTL-expired streams on the OTHER two shards still get their billing
/// closures within bounded sweeps — the walk's continuation cursor
/// resumes at the deferred page instead of restarting at the first
/// descriptor, and the residence quantum rotates the occupants out.
#[expect(
    clippy::too_many_lines,
    reason = "walk fairness scenario; two shards' pinned debt, the occupied budget and the alternating walk form one causal sequence; helper phases would hide which shard the walk starved"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "tombstone_walk_fairness_under_occupied_budget; the fixture nests the pin request inside the first-seen check of the shard walk; flattening it would separate the pin from the shard it occupies"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_walk_fairness_under_occupied_budget() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    let ttl = [("content-type", "application/json"), ("stream-ttl", "1")];
    // Pin maintenance debt on two shards; TTL victims on the others.
    let mut pinned: std::collections::HashSet<String> = Default::default();
    let mut expired_shards: std::collections::HashSet<String> = Default::default();
    let mut expired_names = Vec::new();
    for i in 0..96 {
        if pinned.len() == 2 && expired_shards.len() == 2 {
            break;
        }
        let name = format!("fair-m{i}");
        let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        assert!(st == 200 || st == 201);
        let desc = state
            .registry
            .get(&state.deployment.raw_adapter_sref(&name))
            .await
            .unwrap()
            .unwrap();
        let seg = desc.resolve_segment("");
        let p = state.shards.prefix_for(&seg.shard_route);
        if pinned.len() < 2 && !expired_shards.contains(&p) {
            if pinned.insert(p.clone()) {
                let (st, _, _) = hreq(
                    addr,
                    "POST",
                    &format!("/v1/stream/{name}"),
                    &ct,
                    br#"[{"n":1}]"#,
                )
                .await;
                assert!(st == 200 || st == 204);
            }
            continue;
        }
        if !pinned.contains(&p) && expired_shards.len() < 2 && expired_shards.insert(p.clone()) {
            // Recreate WITH a TTL so the descriptor turns terminal.
            let tname = format!("fair-t{i}");
            let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{tname}"), &ttl, b"").await;
            assert!(st == 200 || st == 201);
            let (st, _, _) = hreq(
                addr,
                "POST",
                &format!("/v1/stream/{tname}"),
                &ttl,
                br#"[{"n":1}]"#,
            )
            .await;
            assert!(st == 200 || st == 204);
            expired_names.push(tname);
        }
    }
    assert_eq!(pinned.len(), 2);
    assert_eq!(expired_shards.len(), 2);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    // Pin the maintenance debt: paused absorbers never retire it.
    state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    for n in &expired_names {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref(n));
    }
    let submits0 = WALK_CLOSE_SUBMITS_SNAPSHOT();
    // Bounded sweeps: quantum rotation + walk cursor must reach BOTH
    // expired shards even while pinned debt keeps re-occupying slots.
    let mut ok = false;
    for _ in 0..14 {
        crate::billing::sweep_owned_outboxes(&state).await;
        if WALK_CLOSE_SUBMITS_SNAPSHOT() >= submits0 + 2 {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    state
        .runtime
        .history
        .paused
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(
        ok,
        "expired shards starved: walk close submits {} -> {}",
        submits0,
        WALK_CLOSE_SUBMITS_SNAPSHOT()
    );
}

#[allow(
    non_snake_case,
    reason = "WALK_CLOSE_SUBMITS_SNAPSHOT; the snapshot is spelled as the constant it stands in for so the assertion reads as the invariant it pins; a snake-case name would hide that it is a fixed value"
)]
fn WALK_CLOSE_SUBMITS_SNAPSHOT() -> u64 {
    crate::billing::WALK_CLOSE_SUBMITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// R30: a custody-revoked close must be INVISIBLE — the identical
/// engine Arc stays in the map with no empty-slot window (the close
/// holds one write guard through remove -> CAS -> reinsert), so no
/// second open can start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_close_keeps_the_identical_engine_with_no_new_open() {
    let _serial = sweep_lock().lock().await;
    let store = mem();
    let prefixes = vec![
        "00".to_string(),
        "01".to_string(),
        "10".to_string(),
        "11".to_string(),
    ];
    let (state, addr) = http_rig_cold_absorb(store, prefixes.clone()).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/noslot-1", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/noslot-1", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("noslot-1"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let prefix = state.shards.prefix_for(&seg.shard_route);
    let pref_refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    drain_billing_clean(&state, &pref_refs).await;
    // Cold-close, reopen via the gate, take custody, then let a
    // customer adopt (revoke) — the exact adopted-close shape.
    let engines: Vec<_> = prefixes
        .iter()
        .filter_map(|p| {
            match state.shards.retire(
                p,
                crate::shard_directory::RetirementReason::Shutdown,
                |_, _| true,
            ) {
                crate::shard_directory::RetireOutcome::Retired(e) => Some(e),
                _ => None,
            }
        })
        .collect();
    // PR 6.1.1-B: retirement arms the production anti-flap holdoff. The
    // fixture wants the NEXT sweep to rediscover these shards at once —
    // in production that is simply a later request, after the holdoff —
    // so the test says so instead of waiting it out.
    for p in &prefixes {
        state.shards.clear_holdoff(p);
    }
    drop(engines); // retirement already closed them
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    crate::billing::sweep_owned_outboxes(&state).await;
    let engine = state
        .shards
        .open(&prefix)
        .expect("sweep must retain the indebted shard");
    assert_ne!(
        engine
            .sweep_custody
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    // Customer adoption revokes custody.
    let _ = state.engine_for(&seg.shard_route).await.unwrap();
    crate::billing::sweep_owned_outboxes(&state).await;
    let now = state
        .shards
        .open(&prefix)
        .expect("adopted engine must remain resident");
    assert!(
        std::sync::Arc::ptr_eq(&engine, &now),
        "the SAME engine must survive a revoked close — a replacement          means an empty-slot window existed"
    );
    // Ptr-identity IS the no-second-open proof for this prefix: the
    // gate's open task unconditionally inserts its fresh engine, so any
    // second open would have REPLACED the map entry. (A global
    // opens-started check is wrong here — the same sweep legitimately
    // discovery-opens the other cold shards.)
    assert_eq!(
        now.sweep_custody.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "scheduler must have dropped its claim"
    );
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/noslot-1", &[], b"").await;
    assert_eq!(st, 200, "adopted engine keeps serving");
}
