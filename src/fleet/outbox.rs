//! Durable fleet event outbox coordinator. Cancellation retains the CAS source
//! until append succeeds; clearing after that point cannot lose the event.
use super::{Desired, FleetDocument, FleetRepository, Overrides};
use crate::tenant::CellId;

pub(super) async fn drain_events<F, Fut>(
    repository: &FleetRepository,
    cell: &CellId,
    mut append: F,
) -> Result<usize, String>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    if !repository.enabled() {
        return Ok(0);
    }
    let mut emitted = 0usize;
    for doc in [FleetDocument::Desired, FleetDocument::Overrides] {
        let Some((bytes, version)) = repository.read_doc(doc).await else {
            continue;
        };
        let pending: Vec<crate::ops::OpsEvent> = if doc == FleetDocument::Desired {
            serde_json::from_slice::<Desired>(&bytes)
                .map(|d| d.pending_events)
                .unwrap_or_default()
        } else {
            serde_json::from_slice::<Overrides>(&bytes)
                .map(|o| o.pending_events)
                .unwrap_or_default()
        };
        if pending.is_empty() {
            continue;
        }
        let ids: std::collections::HashSet<String> =
            pending.iter().map(|e| e.event_id.clone()).collect();
        // Round-21 ordering fix: the events reach `_ops_events`
        // DURABLY before the CAS outbox is cleared — a crash between
        // the two re-emits deterministic ids the rollup deduplicates,
        // never loses the transition.
        let mut stamped = pending.clone();
        for ev in &mut stamped {
            if ev.cell.is_empty() {
                ev.cell = cell.as_str().to_string();
            }
        }
        let body = match serde_json::to_vec(&stamped) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if append(body).await.is_err() {
            continue; // outbox stays; retry next tick
        }
        emitted += ids.len();
        // Clear EXACTLY the drained ids under CAS; concurrent writers'
        // new events survive.
        let cleared: Vec<u8> = if doc == FleetDocument::Desired {
            let Ok(mut d) = serde_json::from_slice::<Desired>(&bytes) else {
                continue;
            };
            d.pending_events.retain(|e| !ids.contains(&e.event_id));
            serde_json::to_vec(&d).unwrap_or_default()
        } else {
            let Ok(mut o) = serde_json::from_slice::<Overrides>(&bytes) else {
                continue;
            };
            o.pending_events.retain(|e| !ids.contains(&e.event_id));
            serde_json::to_vec(&o).unwrap_or_default()
        };
        let _ = repository
            .replace_document(doc, cleared, Some(version))
            .await;
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    async fn seeded(repository: &FleetRepository) {
        let document = Desired {
            count: 2,
            reason: "test".into(),
            epoch: 1,
            computed_at_ms: 1,
            pending_events: vec![crate::ops::OpsEvent::new(
                "scale_out",
                "fixed-event-id".into(),
            )],
        };
        assert!(
            repository
                .replace_document(
                    FleetDocument::Desired,
                    serde_json::to_vec(&document).unwrap(),
                    None
                )
                .await
        );
    }
    async fn pending(repository: &FleetRepository) -> Vec<String> {
        let (body, _) = repository.read_doc(FleetDocument::Desired).await.unwrap();
        serde_json::from_slice::<Desired>(&body)
            .unwrap()
            .pending_events
            .into_iter()
            .map(|event| event.event_id)
            .collect()
    }
    #[tokio::test]
    async fn cancelled_fleet_append_keeps_durable_outbox_and_same_retry_id() {
        let repository =
            FleetRepository::new(Some(Arc::new(object_store::memory::InMemory::new())));
        seeded(&repository).await;
        let cell = CellId::new("cell-test").unwrap();
        let entered = tokio::sync::Notify::new();
        let seen = Mutex::new(Vec::new());
        let mut drain = Box::pin(drain_events(&repository, &cell, |body| {
            let entered = &entered;
            let seen = &seen;
            async move {
                let events: Vec<crate::ops::OpsEvent> = serde_json::from_slice(&body).unwrap();
                assert_eq!(events[0].cell, "cell-test");
                seen.lock().unwrap().push(events[0].event_id.clone());
                entered.notify_one();
                std::future::pending::<Result<(), String>>().await
            }
        }));
        tokio::select! {
            _ = &mut drain => panic!("append is held"),
            _ = entered.notified() => {}
        }
        drop(drain);
        assert_eq!(pending(&repository).await, ["fixed-event-id"]);
        assert_eq!(
            drain_events(&repository, &cell, |body| {
                let seen = &seen;
                async move {
                    let events: Vec<crate::ops::OpsEvent> = serde_json::from_slice(&body).unwrap();
                    seen.lock().unwrap().push(events[0].event_id.clone());
                    Ok(())
                }
            })
            .await
            .unwrap(),
            1
        );
        assert_eq!(*seen.lock().unwrap(), ["fixed-event-id", "fixed-event-id"]);
        assert!(pending(&repository).await.is_empty());
    }
    #[tokio::test]
    async fn cancelled_fleet_clear_follows_append_and_retains_retry_source() {
        use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
        let store = FaultStore::uniform(
            Arc::new(object_store::memory::InMemory::new()),
            79,
            FaultPlan::CLEAN,
        );
        let repository = FleetRepository::new(Some(store.clone()));
        seeded(&repository).await;
        let cell = CellId::new("cell-test").unwrap();
        let appended = AtomicUsize::new(0);
        let held = store.hold_class(StoreOp::Put, ObjClass::Fleet, 1);
        let mut drain = Box::pin(drain_events(&repository, &cell, |_| async {
            appended.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::select! {
                _ = &mut drain => panic!("CAS clear is held"),
                _ = async { while held.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {}
            }
        }).await.expect("entered the actual ObjectStore CAS clear");
        assert_eq!(
            appended.load(Ordering::SeqCst),
            1,
            "append succeeded before CAS clear was attempted"
        );
        drop(drain);
        store.release_hold();
        assert_eq!(pending(&repository).await, ["fixed-event-id"]);
        assert_eq!(
            drain_events(&repository, &cell, |_| async { Ok(()) })
                .await
                .unwrap(),
            1
        );
        assert!(pending(&repository).await.is_empty());
    }
}
