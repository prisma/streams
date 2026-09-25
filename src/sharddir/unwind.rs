//! The opener's unwind boundary: a panicking open is a FAILED open.
//!
//! The open task owns the single-flight outcome for its prefix (see the
//! module doc of `sharddir`). Until this boundary existed, a panic inside
//! the opener killed that task together with its result sender: the
//! in-flight marker outlived it, every later `get_or_open` subscribed to
//! the dead channel and failed with "shard open task vanished" (no
//! strike, no holdoff, no health record) and shutdown counted the phantom
//! open until its grace ran out. The production route is real:
//! `bootstrap::on_slatedb_rt` panics when the SlateDB open task panics,
//! because its handoff channel is answered only by an open that completes.

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use super::{EngineIncarnation, OpenFn};
use crate::shard::ShardEngine;

/// Wrap `opener` so that a panic (while it produces its future or while
/// that future is polled) surfaces as `Err`, which `get_or_open` handles
/// exactly like any failed open: strike, holdoff, health, marker cleared.
pub(super) fn unwind_proof(opener: OpenFn) -> OpenFn {
    let opener: Arc<OpenFn> = Arc::new(opener);
    Box::new(
        move |prefix: String,
              incarnation: EngineIncarnation|
              -> BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>> {
            let opener = opener.clone();
            Box::pin(async move {
                // The closure call runs inside the boundary too: a sync
                // panic while building the future is the same failure.
                let attempt = async move {
                    let open = opener(prefix, incarnation);
                    open.await
                };
                match std::panic::AssertUnwindSafe(attempt).catch_unwind().await {
                    Ok(opened) => opened,
                    Err(payload) => Err(anyhow::anyhow!(
                        "shard open panicked: {}",
                        panic_text(payload.as_ref())
                    )),
                }
            })
        },
    )
}

/// The payload as text: `panic!` literals and formatted messages are the
/// two payload types the standard hook produces.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use crate::sharddir::{EngineIncarnation, OpenFn, OpenGate, OpenOutcome};

    /// A real engine over an in-memory store (the fixture in
    /// dst/tests/fixture_storage.rs is private to that subtree).
    async fn open_engine(prefix: &str) -> Arc<crate::shard::ShardEngine> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::builder(prefix, store.clone())
            .build()
            .await
            .expect("open db");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        crate::shard::ShardEngine::start(
            prefix.to_string(),
            Arc::new(db),
            store,
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            maintenance,
        )
    }

    /// The injected fault: the first call panics with `what`, later
    /// calls pass. The second call is the recovery under test.
    fn panic_on_first(calls: &AtomicUsize, what: &str) {
        assert_ne!(calls.fetch_add(1, Ordering::SeqCst), 0, "{what}");
    }

    /// Panics inside its future on the first call, opens normally after.
    fn panic_once_opener(calls: Arc<AtomicUsize>) -> OpenFn {
        Box::new(move |prefix: String, _inc: EngineIncarnation| {
            let calls = calls.clone();
            Box::pin(async move {
                panic_on_first(&calls, "scripted opener panic");
                Ok(open_engine(&prefix).await)
            })
        })
    }

    /// Panics while BUILDING its future on the first call (a synchronous
    /// panic inside the closure), opens normally after.
    fn sync_panic_once_opener(calls: Arc<AtomicUsize>) -> OpenFn {
        Box::new(move |prefix: String, _inc: EngineIncarnation| {
            panic_on_first(&calls, "scripted sync opener panic");
            Box::pin(async move { Ok(open_engine(&prefix).await) })
        })
    }

    fn describe(outcome: &OpenOutcome) -> String {
        match outcome {
            OpenOutcome::Ready(_) => "Ready".to_string(),
            OpenOutcome::Wait { code, .. } => format!("Wait({code})"),
            OpenOutcome::Failed(msg) => format!("Failed({msg})"),
        }
    }

    /// A panicking opener is a FAILED open, not a vanished one: the
    /// in-flight marker clears, the strike and holdoff arm, health records
    /// the prefix, no phantom open is pending at shutdown, and the next
    /// attempt consults the opener again and installs its engine.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_panicking_opener_fails_its_open_and_the_next_attempt_installs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let shards = Arc::new(RwLock::new(HashMap::new()));
        let gate = OpenGate::new(
            shards.clone(),
            panic_once_opener(calls.clone()),
            Duration::from_secs(30),
        );

        let first = gate.get_or_open("dst-panic", Duration::from_secs(30)).await;
        let OpenOutcome::Failed(msg) = first else {
            panic!("expected Failed, got {}", describe(&first));
        };
        assert!(
            msg.contains("scripted opener panic"),
            "a panicking opener must fail its open with the panic, got: {msg}"
        );
        let opens = gate.stats_json();
        assert_eq!(opens["failed"], 1, "the panic is one failed open: {opens}");
        assert_eq!(opens["completed"], 0, "{opens}");
        assert_eq!(
            opens["in_flight"], 0,
            "a panicked open is not in flight: {opens}"
        );
        assert_eq!(
            gate.shutdown_pending().1,
            0,
            "a phantom open is pending: the in-flight marker outlived its task"
        );
        assert_eq!(
            gate.inner.health.0.lock().unwrap().failed.len(),
            1,
            "health must record the failed prefix"
        );
        {
            let st = gate.inner.st.lock().unwrap();
            let g = st.get("dst-panic").expect("gate state for the prefix");
            assert!(
                g.inflight.is_none(),
                "the in-flight marker outlived its task"
            );
            assert_eq!(g.strikes, 1, "a panicked open is a strike");
            assert!(
                g.holdoff_until.is_some(),
                "a panicked open arms the holdoff"
            );
        }

        // The holdoff is the ordinary failed-open holdoff...
        match gate.get_or_open("dst-panic", Duration::from_secs(1)).await {
            OpenOutcome::Wait { code, .. } => assert_eq!(code, "shard_moving"),
            other => panic!("expected the failed-open holdoff, got {}", describe(&other)),
        }
        // ...and once it passes, the opener runs again and installs. (The
        // holdoff clock is std::time; the test-only reset stands in for it,
        // exactly as the flap test documents.)
        gate.clear_holdoff("dst-panic");
        let engine = match gate.get_or_open("dst-panic", Duration::from_secs(30)).await {
            OpenOutcome::Ready(engine) => engine,
            other => panic!("expected Ready after the holdoff, got {}", describe(&other)),
        };
        assert!(!engine.is_closed(), "the recovery engine must be live");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the second attempt consulted the opener"
        );
        assert!(
            !shards.read().unwrap().is_empty(),
            "the recovery engine is installed"
        );
        engine.begin_close();
    }

    /// A panic while the opener BUILDS its future is the same failed open.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_synchronous_opener_panic_is_the_same_failed_open() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = OpenGate::new(
            Arc::new(RwLock::new(HashMap::new())),
            sync_panic_once_opener(calls.clone()),
            Duration::from_secs(30),
        );
        let first = gate
            .get_or_open("dst-sync-panic", Duration::from_secs(30))
            .await;
        let OpenOutcome::Failed(msg) = first else {
            panic!("expected Failed, got {}", describe(&first));
        };
        assert!(msg.contains("scripted sync opener panic"), "got: {msg}");
        assert_eq!(gate.shutdown_pending().1, 0, "no phantom open pending");
        gate.clear_holdoff("dst-sync-panic");
        let engine = match gate
            .get_or_open("dst-sync-panic", Duration::from_secs(30))
            .await
        {
            OpenOutcome::Ready(engine) => engine,
            other => panic!("expected Ready after the holdoff, got {}", describe(&other)),
        };
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        engine.begin_close();
    }
}
