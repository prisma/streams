#![cfg(test)]
use super::{UsageCounters, decode_static, encode_static};
use crate::billing::SegmentBillingMetaV1;

fn meta() -> SegmentBillingMetaV1 {
    SegmentBillingMetaV1 {
        v: 1,
        account_id: "acct".into(),
        project_id: "proj".into(),
        stream_id: "sid".into(),
        stream_name: "orders".into(),
        segment_id: 3,
        usage_version: 41,
        ingest_payload_bytes_total: 1 << 40,
        ingest_records_total: 9,
        owned_frame_bytes_current: 300,
        storage_accounted_through_ms: 1_700_000_000_123,
        month_year: 2026,
        month_month: 10,
        month_ingest_payload_bytes: 77,
        month_ingest_records: 2,
        month_storage_byte_ms: (u128::from(u64::MAX) * 3).to_string(),
        retained_by_forks: true,
    }
}

/// The JSON of a meta, every field compared.
fn json(m: &SegmentBillingMetaV1) -> serde_json::Value {
    serde_json::to_value(m).unwrap()
}

#[test]
fn a_meta_is_its_static_row_and_its_counters() {
    for byte_ms in ["", "0", "12", &(u128::MAX).to_string()] {
        let mut m = meta();
        m.month_storage_byte_ms = byte_ms.to_owned();
        let counters = UsageCounters::of(&m).unwrap();
        let mut stored = Vec::new();
        counters.encode_into(&mut stored);
        let mut back = decode_static(&encode_static(&m)).unwrap();
        UsageCounters::decode(&stored).unwrap().apply(&mut back);
        assert_eq!(json(&back), json(&m), "byte-time {byte_ms:?}");
    }
}

#[test]
fn the_static_row_holds_no_counter_and_its_golden_bytes_are_pinned() {
    let m = meta();
    let mut other = m.clone();
    other.usage_version = 1;
    other.owned_frame_bytes_current = 0;
    other.month_storage_byte_ms = String::new();
    assert_eq!(encode_static(&m), encode_static(&other));
    let row = encode_static(&m);
    assert_eq!(
        crate::crypto::hex(&row),
        // version, v, segment, year, month, retained, then four names
        "01 0100 03000000 ea070000 0a 01 0461636374 0470726f6a 03736964 066f7264657273"
            .replace(' ', "")
    );
}

#[test]
fn counters_golden_bytes_are_pinned() {
    let mut out = Vec::new();
    UsageCounters {
        usage_version: 1,
        ingest_payload_bytes_total: 300,
        ingest_records_total: 1,
        owned_frame_bytes_current: 0,
        storage_accounted_through_ms: -1,
        month_ingest_payload_bytes: 128,
        month_ingest_records: 0,
        month_storage_byte_ms: Some(1),
    }
    .encode_into(&mut out);
    assert_eq!(crate::crypto::hex(&out), "4601ac02010001800100010100");
}

#[test]
fn malformed_rows_are_refused() {
    let row = encode_static(&meta());
    for end in 0..row.len() {
        assert!(decode_static(&row[..end]).is_none(), "a {end}-byte prefix");
    }
    let mut trailing = row.clone();
    trailing.push(0);
    assert!(decode_static(&trailing).is_none());
    let mut version = row.clone();
    version[0] = 2;
    assert!(decode_static(&version).is_none());
    let mut flag = row;
    flag[12] = 2;
    assert!(
        decode_static(&flag).is_none(),
        "the retention flag is 0 or 1"
    );
    let mut stored = Vec::new();
    UsageCounters::of(&meta()).unwrap().encode_into(&mut stored);
    for end in 0..stored.len() {
        assert!(
            UsageCounters::decode(&stored[..end]).is_none(),
            "a {end}-byte prefix"
        );
    }
    stored.push(0);
    assert!(UsageCounters::decode(&stored).is_none());
    assert_eq!(UsageCounters::decode(b"F\0\0\0\0\0\0\0\x02"), None);
    let mut long = meta();
    long.stream_name = "x".repeat(usize::from(u16::MAX) + 1);
    let name = decode_static(&encode_static(&long)).unwrap().stream_name;
    assert_eq!(name.len(), long.stream_name.len());
    long.month_storage_byte_ms = "-1".into();
    assert_eq!(UsageCounters::of(&long), None);
}
