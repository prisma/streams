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
        clock: 77,
        ..StateRow::default()
    };
    assert_eq!(StateRow::decode(&st.encode()).unwrap(), st);
}

/// A state row with every field set and pairwise-distinct values among the
/// fields of one width (an order swap of two equal fields would round-trip
/// unseen): 13-digit times, 7-digit counts, one `i64::MAX` floor.
fn populated_state() -> StateRow {
    let mut s = StateRow::default();
    s.cfg.retention_ms = 1_760_000_000_001;
    s.cfg.failure_retention_ms = 1_760_000_000_002;
    s.cfg.max_queued_bytes = 1_000_001;
    s.cfg.max_queued_messages = 1_000_002;
    s.cfg.dedup_window_ms = 1_760_000_000_003;
    s.cfg.max_dedup_entries = 1_000_003;
    s.cfg.group.max_attempts = 11;
    s.cfg.group.backoff.initial_ms = 1_760_000_000_004;
    s.cfg.group.backoff.max_ms = 1_760_000_000_005;
    s.cfg.group.backoff.multiplier = 12;
    s.cfg.group.lease_ms = 1_760_000_000_006;
    s.cfg.group.max_in_flight = 13;
    s.cfg.group.max_batch = 14;
    s.clock = 1_760_000_000_007;
    s.cursor_at = 1_760_000_000_008;
    s.cursor_off = 1_000_004;
    s.cursor_started = true;
    s.queued_messages = 1_000_005;
    s.queued_bytes = 1_000_006;
    s.leased = 1_000_007;
    s.retrying = 1_000_008;
    s.failed = 1_000_009;
    s.dedup_entries = 1_000_010;
    s.gap_count = 1_000_011;
    s.counters = rows::Counters {
        sent: 2_000_001,
        delivered: 2_000_002,
        acked: 2_000_003,
        retried: 2_000_004,
        failed: 2_000_005,
        expired: 2_000_006,
        discarded: 2_000_007,
        cancelled: 2_000_008,
        redriven: 2_000_009,
        stale: 2_000_010,
        duplicates: 2_000_011,
    };
    s.debt_bytes = 3_000_001;
    s.debt_oldest_at = 1_760_000_000_009;
    s.debt_generation = 3_000_002;
    s.wakeup_url = Some("https://example.com/hooks/queue-wakeup-1".into());
    s.wakeup_name = "stripe-webhooks-production-0001".into();
    s.wakeup_version = 15;
    s.last_useful_claim_at = 1_760_000_000_010;
    s.last_notified_at = 1_760_000_000_011;
    s.notify_backoff_ms = 4_000_001;
    s.jitter = 5_000_001;
    s.floors = rows::Floors {
        avail: 1_760_000_000_012,
        leases: 1_760_000_000_013,
        retries: i64::MAX,
        fail_expiry: 1_760_000_000_014,
        dedup_expiry: 1_760_000_000_015,
        retry_cycle: 1_760_000_000_016,
    };
    s.roster_at = Some(1_760_000_000_017);
    s
}

#[test]
fn the_state_row_is_a_fixed_binary_layout_that_round_trips_exactly() {
    let row = populated_state();
    assert_eq!(row.wakeup_url.as_deref().map(str::len), Some(40));
    assert_eq!(row.wakeup_name.len(), 31);
    let raw = row.encode();
    assert_eq!(
        raw.len(),
        456,
        "4 version + 88 cfg + 24 clock/cursor + 1 flags + 56 counts + 88 counters \
         + 24 debt + (4 + 40) url + (4 + 31) name + 4 wakeup version \
         + 24 notify + 8 jitter + 48 floors + 8 roster = 456"
    );
    assert_eq!(raw[..4], [3, 0, 0, 0], "the layout's version leads the row");
    assert_eq!(
        raw[116], 0b1_1001,
        "flags after version, cfg, clock and cursor: cursor started, roster, url"
    );
    assert_eq!(StateRow::decode(&raw).unwrap(), row);
    for n in 0..raw.len() {
        assert!(StateRow::decode(&raw[..n]).is_err(), "a {n}-byte prefix");
    }
    let mut trailing = raw.clone();
    trailing.push(0);
    assert_eq!(
        StateRow::decode(&trailing),
        Err("queue row: trailing bytes".to_owned())
    );
    let mut v2 = raw.clone();
    v2[..4].copy_from_slice(&2u32.to_le_bytes());
    assert_eq!(
        StateRow::decode(&v2),
        Err("queue state row: version 2".to_owned())
    );
    let mut not_utf8 = raw.clone();
    not_utf8[285 + 4 + 40 + 4] = 0xFF;
    assert_eq!(
        StateRow::decode(&not_utf8),
        Err("queue row: not UTF-8".to_owned()),
        "the name's first byte, after the debt and the URL"
    );
    let mut unknown_flag = raw;
    unknown_flag[116] |= 0b10_0000;
    assert_eq!(
        StateRow::decode(&unknown_flag),
        Err("queue state row: flags 0x39".to_owned())
    );
}

#[test]
fn state_row_flags_and_optional_fields_round_trip_distinctly() {
    let base = populated_state();
    let mut variants = Vec::new();
    for (url, roster) in [(None, None), (Some(String::new()), Some(0))] {
        let mut s = base.clone();
        s.cursor_started = false;
        s.wakeup_url = url;
        s.roster_at = roster;
        variants.push(s);
    }
    let mut paused = variants[0].clone();
    paused.paused = true;
    let mut deleting = variants[0].clone();
    deleting.deleting = true;
    variants.extend([paused, deleting]);
    let encoded: Vec<Vec<u8>> = variants.iter().map(StateRow::encode).collect();
    let flags: Vec<u8> = encoded.iter().map(|raw| raw[116]).collect();
    assert_eq!(flags, [0, 0b1_1000, 0b10, 0b100]);
    let lens: Vec<usize> = encoded.iter().map(Vec::len).collect();
    assert_eq!(lens, [373 + 31, 373 + 31 + 4 + 8, 404, 404]);
    for (s, raw) in variants.iter().zip(&encoded) {
        assert_eq!(&StateRow::decode(raw).unwrap(), s);
    }
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
