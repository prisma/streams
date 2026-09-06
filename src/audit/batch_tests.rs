use super::*;
const LIMIT: usize = 64 * 1024;
fn event(id: &str, size: usize) -> AuditEvent {
    AuditEvent {
        v: 1,
        event_id: id.into(),
        event_time_ms: 1,
        cell: "test-cell".into(),
        code: "denied".into(),
        project_id: None,
        route: "x".repeat(size),
        method: "GET".into(),
        status: 403,
        dropped: None,
    }
}
fn gap(n: u64) -> AuditEvent {
    {
        let mut event = event("gap", 0);
        event.dropped = Some(n);
        event
    }
}
fn take(service: &AuditJournal) -> Result<PendingAudit<'_>, String> {
    PendingAudit::take(&service.queue, &service.dropped, &service.gap, LIMIT, gap)
}

#[tokio::test]
async fn byte_bounded_audit_batches_preserve_unselected_and_cancelled_order() {
    let service = AuditJournal::new();
    service
        .queue
        .lock()
        .unwrap()
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
        assert_eq!(queued.len(), 900 - selected);
        assert_eq!(
            queued.front().unwrap().event_id,
            format!("event-{selected:04}")
        );
        let next_size = serde_json::to_vec(queued.front().unwrap()).unwrap().len();
        assert!(
            batch.body.len() + next_size + 1 > LIMIT,
            "largest fitting prefix"
        );
    }
    let entered = tokio::sync::Notify::new();
    let entered_sink = &entered;
    let mut pending = Box::pin(persist_audit_batch(batch, |body| async move {
        assert!(body.len() <= LIMIT);
        entered_sink.notify_one();
        std::future::pending::<Result<(), String>>().await
    }));
    tokio::select! {
        _ = &mut pending => panic!("append sink remains held"),
        _ = entered.notified() => {}
    }
    service.queue.lock().unwrap().push_back(event("newer", 256));
    drop(pending);
    let mut received = Vec::new();
    let mut pages = 0;
    for _ in 0..32 {
        let batch = take(&service).unwrap();
        if batch.events.is_empty() {
            break;
        }
        let received = &mut received;
        persist_audit_batch(batch, |body| async move {
            assert!(body.len() <= LIMIT);
            let rows: Vec<AuditEvent> = serde_json::from_slice(&body).unwrap();
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
    assert!(service.queue.lock().unwrap().is_empty());
    assert_eq!(service.dropped(), 0);
    assert_eq!(service.gap.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn oversized_audit_event_becomes_visible_gap_without_wedging_followers() {
    let service = AuditJournal::new();
    service
        .queue
        .lock()
        .unwrap()
        .extend([event("oversized", LIMIT * 2), event("next", 256)]);
    assert!(take(&service).err().unwrap().contains("oversized"));
    assert_eq!(service.dropped(), 1);
    assert_eq!(service.gap.load(Ordering::Relaxed), 1);
    assert_eq!(service.queue.lock().unwrap().len(), 1);
    let batch = take(&service).unwrap();
    assert_eq!(batch.events.len(), 2);
    persist_audit_batch(batch, |body| async move {
        assert!(body.len() <= LIMIT);
        let rows: Vec<AuditEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(rows[0].event_id, "gap");
        assert_eq!(serde_json::json!(rows[0].dropped), serde_json::json!(1));
        assert_eq!(rows[1].event_id, "next");
        Ok(())
    })
    .await
    .unwrap();
    assert!(service.queue.lock().unwrap().is_empty());
    assert_eq!(service.gap.load(Ordering::Relaxed), 0);
    assert_eq!(service.dropped(), 1);
}
