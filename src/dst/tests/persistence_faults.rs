//! Persistence faults.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_livefeed::lf_connect;
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{
    FaultPlan, FaultProfile, FaultStore, ObjClass, OpLog, StoreOp, Workload, drain_observed, mech,
};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// I1+I2+I3 for a single writer under the full fault set — errors, lost
/// responses and realistic latency, not latency alone.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn acked_records_survive_store_faults() {
    for seed in [1u64, 7, 99] {
        let inner = mem();
        // Errors and ambiguity on the WAL (the ack path); manifest and SST
        // get latency only, so compaction still makes progress.
        let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 40))
            .with_class(ObjClass::Wal, FaultPlan::new(12, 8, 40));
        let store = FaultStore::new(inner.clone(), seed, profile);
        let cov = store.coverage();
        let engine = open_engine(store.clone(), &format!("dst-faults-{seed}")).await;
        let key = skey();
        let hash = [3u8; 16];

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.run(&engine, hash, &key, &["a", "b", "c"], 12, false, &mut log)
            .await;
        assert!(log.total_acked() > 0, "seed {seed}: nothing acked");

        let _ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: {e}\ncoverage={:?}", cov.snapshot());
        }
        if let Err(e) = cov.require(&[
            mech::STORE_LATENCY,
            mech::STORE_ERROR,
            mech::STORE_LOST_RESPONSE,
            mech::APPEND_ACKED,
        ]) {
            panic!("seed {seed}: {e}");
        }
    }
}

/// Characterisation, not aspiration: **object-store faults do not reach the
/// client as failures.** SlateDB retries them, so a store that is flaky but
/// eventually available makes appends *slow*, never failed or ambiguous.
///
/// This is worth pinning because it defines the ambiguity surface. If it
/// ever fails, retries have stopped somewhere and clients can now see
/// unknown outcomes from plain store flakiness — which changes what the
/// producer-idempotence contract has to cover. It is also the mechanism
/// that made the eu-central-1 wedge invisible until client timeouts fired:
/// nothing failed, everything just took longer than anyone would wait
/// (docs/SOAK-REGIONS.md).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn store_errors_surface_as_latency_not_as_failed_appends() {
    let inner = mem();
    // 95 % of WAL writes fail before dispatch.
    let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 20))
        .with_class(ObjClass::Wal, FaultPlan::new(95, 0, 5));
    let store = FaultStore::new(inner.clone(), 5, profile);
    let cov = store.coverage();
    let engine = open_engine(store.clone(), "dst-flaky").await;
    let key = skey();
    let hash = [12u8; 16];

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.max_attempts = 1; // no client-side retry: this is about the server
    for _ in 0..20 {
        w.append(&engine, hash, &key, "p", false, &mut log).await;
    }

    assert!(
        store.injected_errors() > 100,
        "expected a storm of injected errors, got {}",
        store.injected_errors()
    );
    assert_eq!(
        log.total_acked(),
        20,
        "every append should still have been acknowledged; unknown={} rejected={}",
        log.unknown.len(),
        log.rejected.len()
    );
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    log.audit(&observed).expect("audit");
}

/// **I4, asserted.** The previous version of this test built a "ghost"
/// ledger of writes attempted through the fenced owner and then never
/// looked at it, so an old owner that acknowledged every one of them would
/// still have passed. The assertion is the test.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_fenced_owner_acknowledges_nothing() {
    for seed in [2u64, 13] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 0, 25));
        let cov = store.coverage();
        let key = skey();
        let hash = [5u8; 16];
        let prefix = format!("dst-fence-{seed}");

        let a = open_engine(store.clone(), &prefix).await;
        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        w.run(&a, hash, &key, &["x", "y"], 8, false, &mut log).await;
        let before = log.total_acked();
        assert!(before > 0, "seed {seed}: nothing acked pre-handoff");

        // The move: a new owner opens the same shard log, fencing A.
        let b = open_engine(store.clone(), &prefix).await;

        // Everything A acknowledges from here is an I4 violation.
        let mut ghost = OpLog::default();
        let mut gw = Workload::new(cov.clone());
        gw.max_attempts = 1; // a fenced owner gets one shot, not three
        gw.run(&a, hash, &key, &["x", "y"], 5, false, &mut ghost)
            .await;

        assert_eq!(
            ghost.total_acked(),
            0,
            "I4 violated (seed {seed}): the fenced owner acknowledged {} write(s) \
             after the new owner opened",
            ghost.total_acked()
        );
        assert!(
            a.is_closed(),
            "seed {seed}: the fenced owner never observed that it lost the shard, \
             so its background tasks are still live"
        );
        cov.hit(mech::OLD_OWNER_FENCED);

        // I1 across the move: what A acked is still readable through B.
        w.run(&b, hash, &key, &["x", "y"], 8, false, &mut log).await;
        let _ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&b, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: after handoff (pre-handoff acks={before}): {e}");
        }
    }
}

/// The dangerous window the previous test could not reach: an append that
/// is **already in flight** when the new owner opens.
///
/// The store gate parks the WAL PUT — after the engine has staged the
/// batch, before durability, therefore before any acknowledgment — which
/// is the `after_db_write_before_durable_ack` fault point without adding a
/// hook to production code. The contract: that request either acks and is
/// readable through the new owner, or fails; it may not ack and vanish.
#[expect(
    clippy::disallowed_methods,
    reason = "handoff fixture; the in-flight append task is joined before its outcome is reconciled with the successor's log; it must run concurrently with the engine handoff"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handoff_with_an_append_in_flight_resolves_safely() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 31, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [6u8; 16];
    let prefix = "dst-inflight".to_string();

    let a = open_engine(store.clone(), &prefix).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["k"], 4, false, &mut log).await;
    let settled = log.total_acked();
    assert!(settled > 0, "nothing acked before the handoff");

    // Park the next WAL write, then start an append that will block in it.
    // Park exactly one WAL write: the in-flight append's. The new
    // owner's open writes to the WAL too, and an unbounded hold would
    // park the handoff itself.
    let engaged = store.hold_class(StoreOp::Put, ObjClass::Wal, 1);
    let a2 = a.clone();
    let key2 = key.clone();
    let cov2 = cov.clone();
    let inflight = tokio::spawn(async move {
        let mut log = OpLog::default();
        let mut w = Workload::new(cov2);
        w.max_attempts = 1;
        let outcome = w.append(&a2, hash, &key2, "k", false, &mut log).await;
        (outcome, log)
    });

    // Wait for the append to actually park in the WAL write.
    for _ in 0..2000 {
        if engaged.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(
        engaged.load(Ordering::SeqCst) > 0,
        "no append ever parked in the WAL write — the scenario is vacuous"
    );
    cov.hit(mech::IN_FLIGHT_AT_FENCE);

    // Hand the shard over while that request is stuck mid-write.
    let b = open_engine(store.clone(), &prefix).await;
    store.release_hold();

    let (outcome, inflight_log) = inflight.await.expect("in-flight task panicked");

    // Whatever happened, it must not be "acknowledged but unreadable".
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&b, hash, &key, &cov).await;
    for (rk, attempts) in &inflight_log.acked {
        log.acked.entry(rk.clone()).or_default().extend(attempts);
    }
    log.rejected.extend(inflight_log.rejected.iter().copied());
    if let Err(e) = log.audit(&observed) {
        panic!("in-flight handoff ({outcome:?}): {e}");
    }
    if let Err(e) = cov.require(&[mech::IN_FLIGHT_AT_FENCE]) {
        panic!("{e}");
    }
}

/// One cursor-resuming livefeed subscriber: it reconnects from its last
/// control cursor until stopped, records every control cursor and delivered
/// `q` in arrival order, and reports its first up-to-date cursor once.
struct CursorSubscriber {
    addr: std::net::SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
    delivered: Arc<std::sync::Mutex<std::collections::HashSet<u64>>>,
    events: Arc<std::sync::Mutex<Vec<String>>>,
    ready: Option<tokio::sync::oneshot::Sender<String>>,
    cursor: Option<String>,
}

impl CursorSubscriber {
    async fn run(mut self) {
        let mut reconnects = 0u32;
        while !self.stop.load(std::sync::atomic::Ordering::Relaxed) {
            let q = match &self.cursor {
                Some(c) => format!("?cursor={}", c.replace('=', "%3D")),
                None => "?cursor=now".to_string(),
            };
            self.events.lock().unwrap().push(format!("RECONNECT {q}"));
            let sck = lf_connect(self.addr, "race", &q).await;
            self.drain(sck).await;
            reconnects += 1;
            if reconnects > 100_000 {
                break;
            }
        }
    }

    /// Reads one connection until EOF, an error, a 5 s idle gap or the stop
    /// flag, observing every complete line as it arrives.
    async fn drain(&mut self, mut sck: tokio::net::TcpStream) {
        use tokio::io::AsyncReadExt;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let read =
                tokio::time::timeout(std::time::Duration::from_secs(5), sck.read(&mut chunk)).await;
            let Ok(Ok(n @ 1..)) = read else {
                return; // EOF, error or idle
            };
            buf.extend_from_slice(&chunk[..n]);
            let Some(cut) = buf.iter().rposition(|&b| b == b'\n').map(|i| i + 1) else {
                continue;
            };
            let complete = String::from_utf8_lossy(&buf[..cut]).to_string();
            buf.drain(..cut);
            self.observe(&complete);
            if self.stop.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
        }
    }

    /// Logs every control cursor and delivered record in arrival order and
    /// reports readiness once the feed is up to date.
    fn observe(&mut self, complete: &str) {
        for c in control_cursors(complete) {
            self.events.lock().unwrap().push(format!("CTL {c}"));
            self.cursor = Some(c);
        }
        if complete.contains("\"upToDate\":true")
            && let Some(tx) = self.ready.take()
        {
            tx.send(self.cursor.clone().expect("initial control has a cursor"))
                .expect("the test awaits the subscriber's readiness");
        }
        for v in delivered_records(complete) {
            self.delivered.lock().unwrap().insert(v);
            self.events.lock().unwrap().push(format!("REC {v}"));
        }
    }
}

/// Every `nextCursor` value in the complete lines, in arrival order.
fn control_cursors(complete: &str) -> Vec<String> {
    let mut cursors = Vec::new();
    let mut cp = 0usize;
    while let Some(p) = complete[cp..].find("\"nextCursor\":\"") {
        let s2 = cp + p + 14;
        let Some(e) = complete[s2..].find('"') else {
            break;
        };
        cursors.push(complete[s2..s2 + e].to_string());
        cp = s2 + e;
    }
    cursors
}

/// Every delivered `q` in the complete lines, in arrival order.
fn delivered_records(complete: &str) -> Vec<u64> {
    let mut records = Vec::new();
    let mut at = 0usize;
    while let Some(p) = complete[at..].find("\"q\":") {
        let s2 = at + p + 4;
        let digits: String = complete[s2..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(v) = digits.parse::<u64>() {
            records.push(v);
        }
        at = s2;
    }
    records
}

/// Round-13 CODE-RED repro hunt (field A1v2): 11 acked+DURABLE records
/// were never delivered — one per noisy stream, each at the instant
/// the feed first hit its project retention cap and cut both parked
/// subscribers (~0.3% of cut events). This leg drives the same
/// convergence hard: a shared feed under a tiny retention budget
/// (constant ProjectOver -> clear_ring + floor=head -> cuts),
/// continuous appends, the absorb boundary swinging (pause flag
/// toggles), store latency injected, and two cursor-resuming
/// subscribers reconciled EXACTLY against the acked set.
#[expect(
    clippy::disallowed_methods,
    reason = "cut-resume fixture; both cursor subscribers and the absorb-boundary swinger are stopped through the shared flag and joined with their dispositions checked; the subscribers must read concurrently with the appends and the swinging boundary"
)]
#[expect(
    clippy::too_many_lines,
    reason = "cut-resume scenario; two resuming subscribers, a swinging absorb boundary, the serial appends and the exact per-subscriber reconciliation form one causal sequence; helper phases would hide which reconnect skipped a durable record"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "cut_resume_never_skips_a_durable_record; the fixture nests the gap diagnostics inside the per-subscriber failure report; flattening them would separate the context lines from the gap they explain"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_resume_never_skips_a_durable_record() {
    let _l = gap_lock().lock().await; // shared failpoint schedule
    let store: Arc<dyn ObjectStore> =
        Arc::new(FaultStore::uniform(mem(), 41, FaultPlan::new(0, 0, 25)));
    let (state, addr) = http_rig(store).await;
    // NOTE: TEST_ASSERT_KEYED_DENSE stays DISARMED in-suite — it is a
    // process-global bisect lever and the parallel suite runs
    // legitimate sparse keyed lanes concurrently (arming it here
    // failed five unrelated tests). The leg's protection is the exact
    // reconciliation below, which is what caught the field loss.
    state.livefeed.budget().set_max_for_test(64 * 1024); // project cap 16 KiB
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/race",
        &[ekey],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Two cursor-resuming subscribers with exact q bitmaps.
    let stopf = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bitmaps: Vec<Arc<std::sync::Mutex<std::collections::HashSet<u64>>>> = (0..2)
        .map(|_| Arc::new(std::sync::Mutex::new(Default::default())))
        .collect();
    let events: Vec<Arc<std::sync::Mutex<Vec<String>>>> = (0..2)
        .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
        .collect();
    let mut subtasks = Vec::new();
    let mut ready_receivers = Vec::new();
    for (bm, ev) in bitmaps.iter().cloned().zip(events.iter().cloned()) {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        ready_receivers.push(ready_rx);
        let subscriber = CursorSubscriber {
            addr,
            stop: stopf.clone(),
            delivered: bm,
            events: ev,
            ready: Some(ready_tx),
            cursor: None,
        };
        subtasks.push(tokio::spawn(subscriber.run()));
    }
    // cursor=now is sampled by the server, not when the tasks spawn.
    // Both subscriptions must observe the empty stream before q=0 is
    // appended; elapsed startup time cannot establish that precondition.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("race"))
        .await
        .unwrap()
        .unwrap();
    for (i, ready) in ready_receivers.into_iter().enumerate() {
        let cursor = tokio::time::timeout(std::time::Duration::from_secs(30), ready)
            .await
            .expect("subscriber must reach its initial upToDate control")
            .expect("subscriber must retain its readiness sender");
        let position = crate::product_cursor::KeyCursor::decode(
            &cursor,
            &desc.project_id,
            &skey(),
            &desc.epoch_bytes().unwrap(),
            &crate::crypto::stream_hash(""),
        )
        .expect("initial cursor must authenticate for this stream");
        assert_eq!(position.offset, 0, "sub{i} must start before q=0");
    }

    // Absorb-boundary swinger: WAL->history retirement races reads.
    let stop2 = stopf.clone();
    let swinger_resources = state.runtime.history.clone();
    let swinger = tokio::spawn(async move {
        let mut on = false;
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            on = !on;
            swinger_resources.paused.store(on, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(47)).await;
        }
        swinger_resources.paused.store(false, Ordering::Relaxed);
    });

    // Continuous appends: 1 KiB records, serial, as fast as the rig
    // commits them.
    let pad = "y".repeat(900);
    let mut acked: std::collections::HashSet<u64> = Default::default();
    for q in 0..1_200u64 {
        let body = format!(r#"{{"q":{q},"pad":"{pad}"}}"#);
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/race/records",
            &[ekey, ct],
            body.as_bytes(),
        )
        .await;
        if st == 200 || st == 201 {
            acked.insert(q);
        }
    }
    // Settle: both subscribers reach the acked frontier.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let maxq = *acked.iter().max().unwrap();
    while std::time::Instant::now() < deadline {
        if bitmaps.iter().all(|b| b.lock().unwrap().contains(&maxq)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    stopf.store(true, std::sync::atomic::Ordering::Relaxed);
    swinger
        .await
        .expect("the absorb-boundary swinger completed");
    for t in subtasks {
        tokio::time::timeout(std::time::Duration::from_secs(10), t)
            .await
            .expect("a subscriber stopped within ten seconds of the stop flag")
            .expect("a subscriber task panicked");
    }
    state.runtime.history.paused.store(false, Ordering::Relaxed);

    let cuts = crate::sse::auth::sse_stats::FEED_LAG_DISCONNECTS
        .load(std::sync::atomic::Ordering::Relaxed);
    for (i, bm) in bitmaps.iter().enumerate() {
        let got = bm.lock().unwrap();
        let mut missing: Vec<u64> = acked.iter().filter(|q| !got.contains(q)).copied().collect();
        missing.sort_unstable();
        if !missing.is_empty() {
            let evs = events[i].lock().unwrap();
            let first = missing[0];
            // The last delivered record BEFORE the gap and the events
            // around it tell us which cursor the client resumed with.
            let mut ctx_lines: Vec<&String> = Vec::new();
            for (j, e) in evs.iter().enumerate() {
                if e == &format!("REC {}", first.saturating_sub(1))
                    || e == &format!("REC {}", missing[missing.len() - 1] + 1)
                {
                    let lo = j.saturating_sub(6);
                    let hi = (j + 7).min(evs.len());
                    ctx_lines.extend(&evs[lo..hi]);
                    ctx_lines.push(&evs[j]); // marker dup ok
                }
            }
            eprintln!("== sub{i} events around the gap:");
            for e in ctx_lines.iter().take(40) {
                eprintln!("  {e}");
            }
            // Count reconnects/controls near the end for context.
            let recon_n = evs.iter().filter(|e| e.starts_with("RECONNECT")).count();
            eprintln!("  (total events {}, reconnects {recon_n})", evs.len());
        }
        assert!(
            missing.is_empty(),
            "sub{i}: {} acked records never delivered through {cuts} cuts: {:?}",
            missing.len(),
            &missing[..missing.len().min(20)]
        );
    }
    engine_shutdown(&state).await;
}
