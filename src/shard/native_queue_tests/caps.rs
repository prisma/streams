//! The per-queue caps (S3b-DESIGN §8.1; SCALING-DECISIONS Q12 as revised):
//! `maxQueuedBytes` and `maxQueuedMessages` are optional guards with no
//! default; open deliveries (leased and retrying), failed deliveries and
//! scheduled messages each have a cap of 1,000,000 by default, clamped to
//! 1,000-10,000,000 and fixed at creation; a scheduled page rides inline
//! only while the queue's inline bytes stay under 64 MiB. No settle, fail,
//! cancel or expiry is ever refused by them.
#![cfg(test)]
use super::{HASH, claim, clocked, edit_state, msg, op, send_op, set_clock, settle, state};
use crate::native_queue::rows::decode_scheduled;
use crate::native_queue::{
    NqOut, NqRefusal, QueueConfig, SCHEDULED_INLINE_BUDGET_BYTES, SendMessage, SettleKind, keys,
};

fn capped(cap: u64) -> QueueConfig {
    QueueConfig {
        max_open_deliveries: cap,
        max_failed_messages: cap,
        max_scheduled_messages: cap,
        ..QueueConfig::default()
    }
}

fn scheduled(body: &str, at: i64) -> SendMessage {
    SendMessage {
        deliver_at: Some(at),
        ..msg(body)
    }
}

#[test]
fn the_queued_guards_have_no_default_and_the_echo_omits_them() {
    let cfg = QueueConfig::default();
    assert_eq!((cfg.max_queued_bytes, cfg.max_queued_messages), (None, None));
    let echo = serde_json::to_value(&cfg).unwrap();
    assert_eq!(echo.get("maxQueuedBytes"), None);
    assert_eq!(echo.get("maxQueuedMessages"), None);
    assert_eq!(
        (
            &echo["maxOpenDeliveries"],
            &echo["maxFailedMessages"],
            &echo["maxScheduledMessages"]
        ),
        (
            &serde_json::json!(1_000_000),
            &serde_json::json!(1_000_000),
            &serde_json::json!(1_000_000)
        )
    );
    let set: QueueConfig = serde_json::from_value(serde_json::json!({ "maxQueuedBytes": 5 })).unwrap();
    assert_eq!(set.max_queued_bytes, Some(5));
}

#[test]
fn the_caps_are_clamped_to_a_thousand_through_ten_million() {
    for (cap, ok) in [(999, false), (1_000, true), (10_000_000, true), (10_000_001, false)] {
        for which in 0..3 {
            let mut cfg = QueueConfig::default();
            *[
                &mut cfg.max_open_deliveries,
                &mut cfg.max_failed_messages,
                &mut cfg.max_scheduled_messages,
            ][which] = cap;
            assert_eq!(cfg.validate().is_ok(), ok, "cap {which} at {cap}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_leases_nothing_new_at_the_open_delivery_cap() {
    let (e, db, _c) = clocked("nq-cap-open", capped(1_000)).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    // Retries the cap counts beside the leases (no rows behind them).
    edit_state(&db, |s| s.retrying = 999).await;
    assert_eq!(claim(&e, 1).await.len(), 1, "999 + 1: the cap itself");
    assert!(claim(&e, 1).await.is_empty(), "1,000 open: nothing new");
    let s = state(&db).await;
    assert_eq!((s.leased, s.frontier), (1, 1), "the claim moved nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_leases_nothing_new_at_the_failed_cap_and_a_fail_is_never_refused() {
    let (e, db, _c) = clocked("nq-cap-failed", capped(1_000)).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    edit_state(&db, |s| s.failed = 999).await;
    let d = claim(&e, 1).await;
    assert_eq!(d.len(), 1, "999 failed of 1,000");
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    assert_eq!(state(&db).await.failed, 1_000, "the fail at the cap is taken");
    assert!(claim(&e, 1).await.is_empty(), "1,000 failed: nothing new");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_scheduled_send_past_the_scheduled_cap_is_refused_and_an_immediate_one_never() {
    let (e, db, c) = clocked("nq-cap-scheduled", capped(1_000)).await;
    edit_state(&db, |s| s.scheduled = 999).await;
    let later = c + 60_000;
    assert_eq!(
        op(&e, send_op(vec![scheduled("a", later), scheduled("b", later)]))
            .await
            .unwrap_err(),
        NqRefusal::ScheduledFull
    );
    let s = state(&db).await;
    assert_eq!((s.counters.sent, s.scheduled), (0, 999), "nothing stored");
    op(&e, send_op(vec![scheduled("a", later)])).await.unwrap();
    assert_eq!(state(&db).await.scheduled, 1_000, "the cap itself");
    let NqOut::Sent(sent) = op(&e, send_op(vec![msg("now")])).await.unwrap() else {
        panic!()
    };
    assert_eq!(sent.len(), 1, "an immediate send is never refused by it");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_scheduled_page_rides_inline_only_under_the_inline_budget() {
    let (e, db, c) = clocked("nq-cap-inline", QueueConfig::default()).await;
    let at = c + 60_000;
    edit_state(&db, |s| s.inline_bytes = SCHEDULED_INLINE_BUDGET_BYTES - 10).await;
    op(&e, send_op(vec![scheduled("over the budget", at)]))
        .await
        .unwrap();
    let entry = db
        .get(keys::by_time(&HASH, keys::AVAIL, at, 0))
        .await
        .unwrap()
        .unwrap();
    assert!(decode_scheduled(at, &entry).unwrap().1.is_empty(), "not inline");
    assert_eq!(state(&db).await.inline_bytes, SCHEDULED_INLINE_BUDGET_BYTES - 10);
    edit_state(&db, |s| s.inline_bytes = 0).await;
    op(&e, send_op(vec![scheduled("under it", at)])).await.unwrap();
    let page = super::stored(&db, 1).await;
    assert_eq!(state(&db).await.inline_bytes, page, "counted inline");
    set_clock(&db, at).await;
    let d = claim(&e, 2).await;
    let acks = d.iter().map(|v| (v.offset, v.lease_gen, SettleKind::Ack));
    op(&e, settle(acks.collect())).await.unwrap();
    assert_eq!(state(&db).await.inline_bytes, 0, "back when it ends");
}
