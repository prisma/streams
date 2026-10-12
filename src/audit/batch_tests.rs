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
        count: None,
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

fn aggregate(code: &'static str, status: u16, count: u64) -> AuditEvent {
    AuditEvent {
        code: code.into(),
        route: String::new(),
        status,
        count: Some(count),
        ..event("aggregate", 0)
    }
}
fn queued(service: &AuditJournal) -> Vec<(String, u16, Option<u64>)> {
    let queue = service.queue.lock().unwrap();
    queue
        .iter()
        .map(|e| (e.code.clone(), e.status, e.count))
        .collect()
}

#[test]
fn unverified_denials_past_the_allowance_drain_as_one_aggregate_per_code_and_status() {
    let service = AuditJournal::new();
    for _ in 0..UNVERIFIED_EVENTS_PER_DRAIN {
        assert!(
            !service.count_unverified("malformed", 401),
            "a single event"
        );
    }
    for _ in 0..3 {
        assert!(service.count_unverified("malformed", 401));
    }
    assert!(service.count_unverified("watch_unauthorized", 403));
    assert!(service.count_unverified("malformed", 400));
    assert!(
        service.queue.lock().unwrap().is_empty(),
        "counted, not queued"
    );
    service.queue_unverified(aggregate);
    let expected = [
        ("malformed".to_string(), 400, Some(1)),
        ("malformed".to_string(), 401, Some(3)),
        ("watch_unauthorized".to_string(), 403, Some(1)),
    ];
    assert_eq!(queued(&service), expected);
    // The drain leaves no count behind; the allowance stays spent until
    // its batch is appended (a_drain_whose_append_fails_reopens_no_allowance).
    service.queue_unverified(aggregate);
    assert_eq!(queued(&service), expected);
    assert!(service.count_unverified("malformed", 401), "still spent");
    assert_eq!(service.dropped(), 0);
    assert_eq!(service.gap.load(Ordering::Relaxed), 0);
}

#[test]
fn an_aggregate_carries_its_count_and_other_events_serialize_as_before() {
    let plain = serde_json::to_string(&event("e", 1)).unwrap();
    assert_eq!(
        plain,
        r#"{"v":1,"event_id":"e","event_time_ms":1,"cell":"test-cell","code":"denied","route":"x","method":"GET","status":403}"#
    );
    let counted = serde_json::to_string(&aggregate("malformed", 401, 7)).unwrap();
    assert_eq!(
        counted,
        r#"{"v":1,"event_id":"aggregate","event_time_ms":1,"cell":"test-cell","code":"malformed","route":"","method":"GET","status":401,"count":7}"#
    );
    let read: AuditEvent = serde_json::from_str(&counted).unwrap();
    assert_eq!(read.count, Some(7));
    let read: AuditEvent = serde_json::from_str(&plain).unwrap();
    assert_eq!(read.count, None);
}

#[test]
fn an_aggregate_the_full_queue_refuses_is_lost_as_the_denials_it_counts() {
    let service = AuditJournal::new();
    let full = (0..AUDIT_QUEUE_CAP).map(|i| event(&format!("queued-{i}"), 0));
    service.queue.lock().unwrap().extend(full);
    for _ in 0..UNVERIFIED_EVENTS_PER_DRAIN + 5 {
        service.count_unverified("malformed", 401);
    }
    service.queue_unverified(aggregate);
    assert_eq!(service.queue.lock().unwrap().len(), AUDIT_QUEUE_CAP);
    assert_eq!(service.dropped(), 5);
    assert_eq!(service.gap.load(Ordering::Relaxed), 5);
    // A cancelled batch that cannot requeue its aggregate loses as many.
    let cancelled = AuditJournal::new();
    cancelled
        .queue
        .lock()
        .unwrap()
        .push_back(event("pending", 0));
    let mut batch = take(&cancelled).unwrap();
    batch.events.push(aggregate("malformed", 401, 9));
    let newer = (0..AUDIT_QUEUE_CAP).map(|i| event(&format!("newer-{i}"), 0));
    cancelled.queue.lock().unwrap().extend(newer);
    drop(batch);
    assert_eq!(cancelled.queue.lock().unwrap().len(), AUDIT_QUEUE_CAP);
    assert_eq!(
        cancelled.dropped(),
        9 + 1,
        "the aggregate's nine, then pending"
    );
    assert_eq!(cancelled.gap.load(Ordering::Relaxed), 9 + 1);
}

/// Queues `n` unverified denials as `observe_denial` does: a single event
/// each while the drain's allowance lasts, a count past it.
fn deny(service: &AuditJournal, n: u64) {
    for _ in 0..n {
        if !service.count_unverified("malformed", 401) {
            service.queue.lock().unwrap().push_back(event("single", 0));
        }
    }
}

/// The allowance of single unverified events reopens only once a drain's
/// batch is durably appended (or the drain found nothing to take): a drain
/// whose append fails requeues its batch and reopens nothing, so while
/// `_audit_events` appends fail an anonymous flood adds one aggregate per
/// (code, status) and drain attempt to the queue, never another allowance
/// of single events. The first drain that appends reopens it.
#[tokio::test]
async fn a_drain_whose_append_fails_reopens_no_allowance() {
    let allowance = usize::try_from(UNVERIFIED_EVENTS_PER_DRAIN).unwrap();
    let service = AuditJournal::new();
    deny(&service, UNVERIFIED_EVENTS_PER_DRAIN + 5);
    for attempt in 1..=3 {
        service.queue_unverified(aggregate);
        let batch = take(&service).unwrap();
        let down = |_| async { Err::<(), String>("store down".into()) };
        assert!(service.append_batch(batch, down).await.is_err());
        deny(&service, UNVERIFIED_EVENTS_PER_DRAIN);
        let held = service.queue.lock().unwrap().len();
        assert_eq!(
            held,
            allowance + attempt,
            "queued after failed drain {attempt}"
        );
    }
    service.queue_unverified(aggregate);
    let batch = take(&service).unwrap();
    let appended = service.append_batch(batch, |_| async { Ok(()) }).await;
    assert_eq!(
        appended,
        Ok(allowance + 4),
        "the singles and four aggregates"
    );
    deny(&service, 1);
    assert_eq!(queued(&service), [("denied".to_string(), 403, None)]);
    assert_eq!(
        (service.dropped(), service.gap.load(Ordering::Relaxed)),
        (0, 0)
    );
}
