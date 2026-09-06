use super::*;
const LIMIT: usize = 64 * 1024;
fn event(id: &str, size: usize) -> OpsEvent {
    OpsEvent::new("test", id.into()).fields(serde_json::json!({"payload": "x".repeat(size)}))
}
fn gap(n: u64) -> OpsEvent {
    OpsEvent::new("telemetry_gap", "gap".into()).fields(serde_json::json!({"dropped": n}))
}
fn take(service: &OpsService) -> Result<PendingOps<'_>, String> {
    PendingOps::take(
        &service.queue,
        &service.dropped,
        &service.gap,
        LIMIT,
        gap,
        |event| {
            event.cell = "test-cell".into();
        },
    )
}

#[tokio::test]
async fn byte_bounded_ops_batches_preserve_unselected_and_cancelled_order() {
    let service = OpsService::new();
    service
        .queue
        .lock()
        .unwrap()
        .queue
        .extend((0..900).map(|i| event(&format!("event-{i:04}"), 256)));
    let batch = take(&service).unwrap();
    let selected = batch.events.len();
    assert!(
        selected > 0 && selected < 512,
        "byte budget must bind before row cap"
    );
    assert!(batch.body.len() <= LIMIT);
    {
        let queued = service.queue.lock().unwrap();
        assert_eq!(queued.queue.len(), 900 - selected);
        assert_eq!(
            queued.queue.front().unwrap().event_id,
            format!("event-{selected:04}")
        );
        let next_size = serde_json::to_vec(queued.queue.front().unwrap())
            .unwrap()
            .len();
        assert!(
            batch.body.len() + next_size + 1 > LIMIT,
            "largest fitting prefix"
        );
    }
    let entered = tokio::sync::Notify::new();
    let entered_sink = &entered;
    let mut pending = Box::pin(persist_ops_batch(batch, |body| async move {
        assert!(body.len() <= LIMIT);
        entered_sink.notify_one();
        std::future::pending::<Result<(), String>>().await
    }));
    tokio::select! {
        _ = &mut pending => panic!("append sink remains held"),
        _ = entered.notified() => {}
    }
    service
        .queue
        .lock()
        .unwrap()
        .queue
        .push_back(event("newer", 256));
    drop(pending);
    let mut received = Vec::new();
    let mut pages = 0;
    for _ in 0..32 {
        let batch = take(&service).unwrap();
        if batch.events.is_empty() {
            break;
        }
        let received = &mut received;
        persist_ops_batch(batch, |body| async move {
            assert!(body.len() <= LIMIT);
            let rows: Vec<OpsEvent> = serde_json::from_slice(&body).unwrap();
            received.extend(rows.into_iter().map(|event| event.event_id));
            Ok(())
        })
        .await
        .unwrap();
        pages += 1;
    }
    let mut expected: Vec<_> = (0..900).map(|i| format!("event-{i:04}")).collect();
    expected.push("newer".into());
    assert_eq!(received, expected);
    assert!(pages > 1);
    assert!(service.queue.lock().unwrap().queue.is_empty());
    assert_eq!(service.dropped(), 0);
    assert_eq!(service.gap.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn oversized_ops_event_becomes_visible_gap_without_wedging_followers() {
    let service = OpsService::new();
    service
        .queue
        .lock()
        .unwrap()
        .queue
        .extend([event("oversized", LIMIT * 2), event("next", 256)]);
    assert!(take(&service).err().unwrap().contains("oversized"));
    assert_eq!(service.dropped(), 1);
    assert_eq!(service.gap.load(Ordering::Relaxed), 1);
    assert_eq!(service.queue.lock().unwrap().queue.len(), 1);
    let batch = take(&service).unwrap();
    assert_eq!(batch.events.len(), 2);
    persist_ops_batch(batch, |body| async move {
        assert!(body.len() <= LIMIT);
        let rows: Vec<OpsEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(rows[0].event_id, "gap");
        assert_eq!(rows[0].fields["dropped"], serde_json::json!(1));
        assert_eq!(rows[1].event_id, "next");
        Ok(())
    })
    .await
    .unwrap();
    assert!(service.queue.lock().unwrap().queue.is_empty());
    assert_eq!(service.gap.load(Ordering::Relaxed), 0);
    assert_eq!(service.dropped(), 1);
}
