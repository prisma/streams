//! Codec and arithmetic tests for the native queue vocabulary.
use super::keys::{lease_token, message_id, parse_lease_token, parse_message_id};
use super::rows::{Cause, DState, DeliveryRow, MetaRow, StateRow};
use super::*;

#[test]
fn message_ids_and_lease_tokens_round_trip_and_refuse_another_incarnation() {
    let hash = [7u8; 16];
    let other = [8u8; 16];
    for off in [0u64, 1, 42, u64::MAX] {
        let id = message_id(&hash, off);
        assert_eq!(parse_message_id(&hash, &id), Some(off));
        assert_eq!(parse_message_id(&other, &id), None);
        let tok = Token {
            offset: off,
            lease_gen: 9,
        };
        let s = lease_token(&hash, tok);
        assert_eq!(parse_lease_token(&hash, &s), Some(tok));
        assert_eq!(parse_lease_token(&other, &s), None);
    }
    assert_eq!(parse_message_id(&hash, "msg_!!"), None);
    assert_eq!(parse_lease_token(&hash, "msg_00"), None);
}

#[test]
fn rows_round_trip_exactly() {
    let meta = MetaRow {
        enqueued_at: 1,
        available_at: 2,
        retention_end: 3,
        hard_expires_at: 4,
        bytes: 5,
        open: 1,
        cancelled: true,
        taken: true,
        dedup_tag: Some([6; 16]),
    };
    assert_eq!(MetaRow::decode(&meta.encode()).unwrap(), meta);
    let row = DeliveryRow {
        state: DState::Failed,
        cycle: 2,
        cycle_deadline: 10,
        attempts: 3,
        releases: 1,
        lease_gen: 4,
        lease_deadline: 11,
        due_at: 12,
        first_delivered_at: 13,
        failed_at: 14,
        failure_expires_at: 15,
        cause: Some(Cause::Exhausted),
        code: Some("provider_down".into()),
    };
    assert_eq!(DeliveryRow::decode(&row.encode()).unwrap(), row);
    let mut truncated = row.encode();
    truncated.pop();
    assert!(DeliveryRow::decode(&truncated).is_err());
    let st = StateRow {
        v: 1,
        clock: 77,
        ..StateRow::default()
    };
    assert_eq!(StateRow::decode(&st.encode()).unwrap(), st);
}

#[test]
fn backoff_grows_by_the_multiplier_and_caps_at_max() {
    let b = Backoff {
        initial_ms: 1_000,
        max_ms: 10_000,
        multiplier: 2,
    };
    let one = JITTER_SCALE;
    assert_eq!(backoff_ms(&b, 1, one), 1_000);
    assert_eq!(backoff_ms(&b, 3, one), 4_000);
    assert_eq!(backoff_ms(&b, 30, one), 10_000);
    assert_eq!(backoff_ms(&b, 3, one / 2), 2_000);
    assert_eq!(backoff_ms(&b, 3, 0), 0);
}

#[test]
fn configuration_clamps_are_enforced() {
    assert!(QueueConfig::default().validate().is_ok());
    let bad = QueueConfig {
        retention_ms: 15 * DAY_MS,
        ..QueueConfig::default()
    };
    assert!(bad.validate().is_err());
    let mut g = QueueConfig::default();
    g.group.lease_ms = 500;
    assert!(g.validate().is_err());
    let d = QueueConfig {
        dedup_window_ms: 5 * DAY_MS,
        ..QueueConfig::default()
    };
    assert!(
        d.validate().is_err(),
        "the dedup window may not exceed retention"
    );
}

#[test]
fn queue_scans_cache_their_blocks() {
    // A queue scan that does not cache re-reads every sorted run's block
    // from the bucket on every claim and sweep (the first 1x pricing run:
    // 1M extra GETs in 90 minutes).
    assert!(scan_options().cache_blocks);
    assert!(matches!(
        scan_options().order,
        slatedb::IterationOrder::Ascending
    ));
}
