#![cfg(test)]
//! Layout-4 storage golden tests.
//!
//! These pin the EXACT on-disk / on-wire encodings — byte-for-byte
//! against hand-derived literals — so a codec refactor can move code
//! without moving bytes. Every expected value was derived by reading
//! the encoder source (field order, tag bytes, endianness) and, for the
//! MAC'd/HKDF values, recomputed independently from RFC 5869 / HMAC /
//! Crockford-base32 definitions — then confirmed by running the suite.
//! A failure here means the storage format changed; that is a deliberate
//! act, never something to "fix" by pasting the new actual value without
//! a layout-version bump.

/// Shared fixtures: fixed hashes/timestamps keep every golden stable.
const H: [u8; 16] = [0x11; 16]; // stream/segment hash
const K: [u8; 16] = [0x22; 16]; // routing-key hash
/// Big-endian offsets use asymmetric patterns so an endianness flip or
/// a field-order swap changes the literal.
const OFF: u64 = 0x0102_0304_0506_0708;
const OFF2: u64 = 0x1112_1314_1516_1718;

fn hex(b: &[u8]) -> String {
    crate::crypto::hex(b)
}

mod shard_keys {
    use super::*;
    use crate::shard::*;

    #[test]
    fn golden_layout4_tail_key_bytes() {
        // <hash16> 't'
        assert_eq!(
            hex(&tail_key(&H)),
            concat!("11111111111111111111111111111111", "74")
        );
    }

    #[test]
    fn golden_layout4_record_key_bytes_be_offset() {
        // <hash16> 'r' <offset u64 BIG-endian> — BE keeps keys
        // offset-ordered inside the hash range.
        assert_eq!(
            hex(&record_key(&H, OFF)),
            concat!("11111111111111111111111111111111", "72", "0102030405060708")
        );
        assert_eq!(record_key(&H, OFF).len(), 25);
    }

    #[test]
    fn golden_layout4_seq_key_bytes() {
        // <hash16> 's' <key_hash16>
        assert_eq!(
            hex(&seq_key(&H, &K)),
            concat!(
                "11111111111111111111111111111111",
                "73",
                "22222222222222222222222222222222"
            )
        );
    }

    #[test]
    fn golden_layout4_producer_key_bytes() {
        // <hash16> 'q' <key_hash16> <producer id UTF-8, no separator>
        assert_eq!(
            hex(&producer_key(&H, &K, "prod-9")),
            concat!(
                "11111111111111111111111111111111",
                "71",
                "22222222222222222222222222222222",
                "70726f642d39" // "prod-9"
            )
        );
    }

    #[test]
    fn golden_layout4_dirty_key_sentinel_bytes() {
        // <0xFF*16 sentinel> 'D' <hash16>
        assert_eq!(
            hex(&dirty_key(&H)),
            concat!(
                "ffffffffffffffffffffffffffffffff",
                "44",
                "11111111111111111111111111111111"
            )
        );
    }

    #[test]
    fn golden_layout4_shard_maint_key_bytes() {
        // <0xFF*16 sentinel> 'M'
        assert_eq!(
            hex(&shard_maint_key()),
            concat!("ffffffffffffffffffffffffffffffff", "4d")
        );
    }
}

mod tail_codec {
    use super::*;
    use crate::shard::{TailFields, decode_tail_for_tests, encode_tail_for_tests};

    /// Fully-populated v3 tail. Each numeric field carries a distinct
    /// ascending byte pattern so field-order or endianness regressions
    /// move the golden bytes.
    fn full_tail() -> TailFields {
        TailFields {
            next: 0x0807_0605_0403_0201,
            ts: 0x1817_1615_1413_1211,
            logical: 0x2827_2625_2423_2221,
            absorbed: 0x3837_3635_3433_3231,
            trimmed: 0x4847_4645_4443_4241,
            seq: Some("seq-7".into()),
            closed: true,
            history_v2: true,
            route: [
                0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
                0xAE, 0xAF,
            ],
            trim_safe_to: 0x5857_5655_5453_5251,
            unabsorbed_bytes: 0x6867_6665_6463_6261,
        }
    }

    /// v3 layout:
    /// [ver=3][next u64 LE][ts i64 LE][logical u64 LE][absorbed u64 LE]
    /// [trimmed u64 LE][flags u8][seq_len u16 LE][seq][route 16]
    /// [trim_safe_to u64 LE][unabsorbed_bytes u64 LE]
    const FULL_V3_HEX: &str = concat!(
        "03",                               // ver
        "0102030405060708",                 // next LE
        "1112131415161718",                 // ts LE
        "2122232425262728",                 // logical LE
        "3132333435363738",                 // absorbed LE
        "4142434445464748",                 // trimmed LE
        "03",                               // flags: bit0 closed | bit1 history_v2
        "0500",                             // seq_len LE = 5
        "7365712d37",                       // "seq-7"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf", // route16
        "5152535455565758",                 // trim_safe_to LE
        "6162636465666768",                 // unabsorbed_bytes LE
    );

    fn assert_full_fields(t: &TailFields) {
        assert_eq!(t.next, 0x0807_0605_0403_0201);
        assert_eq!(t.ts, 0x1817_1615_1413_1211);
        assert_eq!(t.logical, 0x2827_2625_2423_2221);
        assert_eq!(t.absorbed, 0x3837_3635_3433_3231);
        assert_eq!(t.trimmed, 0x4847_4645_4443_4241);
        assert_eq!(t.seq.as_deref(), Some("seq-7"));
        assert!(t.closed);
        assert!(t.history_v2);
        assert_eq!(t.route[0], 0xA0);
        assert_eq!(t.route[15], 0xAF);
        assert_eq!(t.trim_safe_to, 0x5857_5655_5453_5251);
        assert_eq!(t.unabsorbed_bytes, 0x6867_6665_6463_6261);
    }

    #[test]
    fn golden_layout4_tail_v3_full_bytes() {
        let v = encode_tail_for_tests(&full_tail());
        assert_eq!(v.len(), 81);
        assert_eq!(hex(&v), FULL_V3_HEX);
    }

    #[test]
    fn golden_layout4_tail_v3_minimal_bytes() {
        // Default fields: seq=None encodes as seq_len=0, flags=0, and
        // the extension block is zeroed — 0x03 then 75 zero bytes.
        let v = encode_tail_for_tests(&TailFields::default());
        assert_eq!(v.len(), 76);
        // 0x03 followed by exactly 75 zero bytes (5 zeroed u64 lanes,
        // flags=0, seq_len=0, zeroed route16/trim_safe_to/unabsorbed).
        assert_eq!(
            hex(&v),
            concat!(
                "03",
                "00000000000000000000000000000000000000000000000000",
                "00000000000000000000000000000000000000000000000000",
                "00000000000000000000000000000000000000000000000000",
            )
        );
    }

    #[test]
    fn golden_layout4_tail_v3_roundtrip_fields() {
        let d = decode_tail_for_tests(&encode_tail_for_tests(&full_tail())).expect("v3 decodes");
        assert_full_fields(&d);
    }

    /// v2 rows have NO flags byte: seq_len sits at offset 41, and the
    /// decoder reports flags=0 (open, legacy per-stream history).
    fn v2_row() -> Vec<u8> {
        let mut v = vec![2u8];
        v.extend_from_slice(&0x0807_0605_0403_0201u64.to_le_bytes()); // next
        v.extend_from_slice(&0x1817_1615_1413_1211i64.to_le_bytes()); // ts
        v.extend_from_slice(&0x2827_2625_2423_2221u64.to_le_bytes()); // logical
        v.extend_from_slice(&0x3837_3635_3433_3231u64.to_le_bytes()); // absorbed
        v.extend_from_slice(&0x4847_4645_4443_4241u64.to_le_bytes()); // trimmed
        v.extend_from_slice(&5u16.to_le_bytes()); // seq_len at offset 41
        v.extend_from_slice(b"seq-7");
        v
    }

    #[test]
    fn golden_layout4_tail_v2_decode_no_flags_byte() {
        // A bare v2 row ends after the seq bytes: the extension fields
        // default to zero.
        let d = decode_tail_for_tests(&v2_row()).expect("v2 decodes");
        assert_eq!(d.next, 0x0807_0605_0403_0201);
        assert_eq!(d.ts, 0x1817_1615_1413_1211);
        assert_eq!(d.logical, 0x2827_2625_2423_2221);
        assert_eq!(d.absorbed, 0x3837_3635_3433_3231);
        assert_eq!(d.trimmed, 0x4847_4645_4443_4241);
        assert_eq!(d.seq.as_deref(), Some("seq-7"));
        assert!(!d.closed, "v2 rows have no flags byte -> open");
        assert!(!d.history_v2, "v2 rows predate the v2-history bit");
        assert_eq!(d.route, [0u8; 16]);
        assert_eq!(d.trim_safe_to, 0);
        assert_eq!(d.unabsorbed_bytes, 0);
    }

    #[test]
    fn golden_layout4_tail_v2_decode_with_trailing_extensions() {
        // The decoder reads route/trim_safe_to/unabsorbed_bytes from a
        // v2 row that carries them (same trailing layout as v3).
        let mut v = v2_row();
        v.extend_from_slice(&[0xA0u8; 16]);
        v.extend_from_slice(&0x5857_5655_5453_5251u64.to_le_bytes());
        v.extend_from_slice(&0x6867_6665_6463_6261u64.to_le_bytes());
        let d = decode_tail_for_tests(&v).expect("v2 + extensions decodes");
        assert_eq!(d.route, [0xA0u8; 16]);
        assert_eq!(d.trim_safe_to, 0x5857_5655_5453_5251);
        assert_eq!(d.unabsorbed_bytes, 0x6867_6665_6463_6261);
        assert!(!d.closed && !d.history_v2);
    }

    #[test]
    fn golden_layout4_tail_decode_tolerates_trailing_bytes() {
        // Forward evolution: unknown trailing bytes are ignored.
        let mut v = encode_tail_for_tests(&full_tail());
        v.extend_from_slice(&[0xEE; 7]);
        let d = decode_tail_for_tests(&v).expect("trailing bytes tolerated");
        assert_full_fields(&d);
    }

    #[test]
    fn golden_layout4_tail_decode_rejects_short_and_bad_version() {
        // Shorter than the fixed v3 prefix (44 bytes): None, not a panic.
        assert!(decode_tail_for_tests(&[3u8; 43]).is_none());
        assert!(decode_tail_for_tests(&[]).is_none());
        // Version bytes outside {2, 3} are refused even at full length.
        assert!(decode_tail_for_tests(&[4u8; 64]).is_none());
        assert!(decode_tail_for_tests(&[0u8; 64]).is_none());
        assert!(decode_tail_for_tests(&[1u8; 64]).is_none());
        // A seq_len overrunning the buffer is None, not a panic.
        let mut v = encode_tail_for_tests(&TailFields::default());
        v.truncate(44);
        v[42] = 0xFF; // seq_len = 65535
        v[43] = 0xFF;
        assert!(decode_tail_for_tests(&v).is_none());
    }
}

mod dirty_value {
    use crate::shard::{StreamMaintenance, decode_dirty_value, dirty_value_for_tests};

    #[test]
    fn golden_layout4_dirty_value_32byte_le() {
        // v2 row: absorbed, next, unabsorbed_bytes, oldest_unabsorbed_ms
        // — four little-endian u64/i64 lanes, 32 bytes total.
        let m = StreamMaintenance {
            absorbed: 0x0807_0605_0403_0201,
            next: 0x1817_1615_1413_1211,
            unabsorbed_bytes: 0x2827_2625_2423_2221,
            oldest_unabsorbed_ms: 0x3837_3635_3433_3231,
        };
        let v = dirty_value_for_tests(&m);
        assert_eq!(
            crate::crypto::hex(&v),
            concat!(
                "0102030405060708", // absorbed LE
                "1112131415161718", // next LE
                "2122232425262728", // unabsorbed_bytes LE
                "3132333435363738", // oldest_unabsorbed_ms LE
            )
        );
        let d = decode_dirty_value(&v).expect("v2 row decodes");
        assert_eq!(d, m);
    }

    #[test]
    fn golden_layout4_dirty_value_v1_16byte_decode() {
        // v1 rows (absorbed, next only) decode with zero gauge/age and
        // remain valid "has outstanding maintenance" markers.
        let v1 = crate::crypto::unhex("01020304050607081112131415161718").unwrap();
        let d = decode_dirty_value(&v1).expect("v1 row decodes");
        assert_eq!(d.absorbed, 0x0807_0605_0403_0201);
        assert_eq!(d.next, 0x1817_1615_1413_1211);
        assert_eq!(d.unabsorbed_bytes, 0);
        assert_eq!(d.oldest_unabsorbed_ms, 0);
        // Anything shorter than one v1 row is rejected, never a panic.
        assert!(decode_dirty_value(&v1[..15]).is_none());
    }
}

mod queue_keys {
    use super::*;
    use crate::queue::{ack_key, config_key, cursor_key, fence_key, lease_key};

    // <hash16> <tag> <consumer UTF-8> 0x00 <cgen u64 BE> [<off u64 BE>]
    // cgen = OFF, off = OFF2.

    #[test]
    fn golden_layout4_cursor_key_bytes() {
        assert_eq!(
            hex(&cursor_key(&H, "c1", OFF)),
            concat!(
                "11111111111111111111111111111111",
                "63",               // 'c'
                "6331",             // "c1"
                "00",               // name/generation separator
                "0102030405060708"  // cgen BE
            )
        );
    }

    #[test]
    fn golden_layout4_lease_key_bytes() {
        assert_eq!(
            hex(&lease_key(&H, "c1", OFF, OFF2)),
            concat!(
                "11111111111111111111111111111111",
                "6c", // 'l'
                "6331",
                "00",
                "0102030405060708", // cgen BE
                "1112131415161718"  // off BE
            )
        );
    }

    #[test]
    fn golden_layout4_ack_key_bytes() {
        assert_eq!(
            hex(&ack_key(&H, "c1", OFF, OFF2)),
            concat!(
                "11111111111111111111111111111111",
                "78", // 'x'
                "6331",
                "00",
                "0102030405060708",
                "1112131415161718"
            )
        );
    }

    #[test]
    fn golden_layout4_config_key_bytes() {
        // Parent-identity row: no NUL separator, no generation.
        assert_eq!(
            hex(&config_key(&H, "c1")),
            concat!(
                "11111111111111111111111111111111",
                "43", // 'C'
                "6331"
            )
        );
    }

    #[test]
    fn golden_layout4_fence_key_bytes() {
        assert_eq!(
            hex(&fence_key(&H, "c1")),
            concat!(
                "11111111111111111111111111111111",
                "46", // 'F'
                "6331"
            )
        );
    }
}

mod billing {
    use super::*;
    use crate::billing::{
        SegmentBillingMetaV1, billing_meta_key, usage_dirty_key, usage_month_final_key,
    };

    #[test]
    fn golden_layout4_billing_meta_key_bytes() {
        // <seg-hash16> 'B'
        assert_eq!(
            hex(&billing_meta_key(&H)),
            concat!("11111111111111111111111111111111", "42")
        );
    }

    #[test]
    fn golden_layout4_usage_dirty_key_bytes() {
        // <0xFF*16 sentinel> 'U' <seg-hash16>
        assert_eq!(
            hex(&usage_dirty_key(&H)),
            concat!(
                "ffffffffffffffffffffffffffffffff",
                "55",
                "11111111111111111111111111111111"
            )
        );
    }

    #[test]
    fn golden_layout4_usage_month_final_key_ascii_month() {
        // <0xFF*16 sentinel> 'V' <seg-hash16> <"YYYY-MM" ASCII> — the
        // month is DECIMAL TEXT ("2026-08"), not a binary i32/u32 pair,
        // so there is no endianness; pin the zero-padded format.
        assert_eq!(
            hex(&usage_month_final_key(&H, 2026, 8)),
            concat!(
                "ffffffffffffffffffffffffffffffff",
                "56",
                "11111111111111111111111111111111",
                "323032362d3038" // "2026-08"
            )
        );
    }

    /// The billing-meta VALUE codec is serde_json over
    /// SegmentBillingMetaV1; every field always serializes (no skips).
    #[test]
    fn golden_layout4_billing_meta_value_json() {
        let m = SegmentBillingMetaV1 {
            v: 1,
            account_id: "acct-1".into(),
            project_id: "proj-test".into(),
            stream_id: "sid".into(),
            stream_name: "orders".into(),
            segment_id: 2,
            usage_version: 9,
            ingest_payload_bytes_total: 1000,
            ingest_records_total: 10,
            owned_frame_bytes_current: 4096,
            storage_accounted_through_ms: 1_786_000_000_000,
            month_year: 2026,
            month_month: 8,
            month_ingest_payload_bytes: 500,
            month_ingest_records: 5,
            month_storage_byte_ms: "12345678901234567890".into(), // u128 as string
            retained_by_forks: true,
        };
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(
            json,
            r#"{"v":1,"account_id":"acct-1","project_id":"proj-test","stream_id":"sid","stream_name":"orders","segment_id":2,"usage_version":9,"ingest_payload_bytes_total":1000,"ingest_records_total":10,"owned_frame_bytes_current":4096,"storage_accounted_through_ms":1786000000000,"month_year":2026,"month_month":8,"month_ingest_payload_bytes":500,"month_ingest_records":5,"month_storage_byte_ms":"12345678901234567890","retained_by_forks":true}"#
        );
    }

    #[test]
    fn golden_layout4_billing_meta_value_default_json_and_decode() {
        // Every field carries #[serde(default)]: "{}" decodes to the
        // zero row, and the zero row serializes with all keys present.
        let d: SegmentBillingMetaV1 = serde_json::from_str("{}").unwrap();
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            r#"{"v":0,"account_id":"","project_id":"","stream_id":"","stream_name":"","segment_id":0,"usage_version":0,"ingest_payload_bytes_total":0,"ingest_records_total":0,"owned_frame_bytes_current":0,"storage_accounted_through_ms":0,"month_year":0,"month_month":0,"month_ingest_payload_bytes":0,"month_ingest_records":0,"month_storage_byte_ms":"","retained_by_forks":false}"#
        );
    }
}

mod history_postings {
    use super::*;
    use crate::crypto::{RouteHash, RoutingKeyHash, SegmentHash};

    #[test]
    fn golden_layout4_hist2_record_key_bytes() {
        // <route16> <inc16> 'r' <offset u64 BE>
        assert_eq!(
            hex(&crate::history::hist2_record_key(
                RouteHash(H),
                SegmentHash(K),
                OFF
            )),
            concat!(
                "11111111111111111111111111111111",
                "22222222222222222222222222222222",
                "72",
                "0102030405060708"
            )
        );
    }

    #[test]
    fn golden_layout4_postings_key_bytes() {
        // <route16> <inc16> 'p' <rk_hash16> <bucket BE8> <page_first BE8>
        assert_eq!(
            hex(&crate::postings::postings_key(
                RouteHash(H),
                SegmentHash(K),
                &RoutingKeyHash([0x33; 16]),
                OFF,
                OFF2
            )),
            concat!(
                "11111111111111111111111111111111",
                "22222222222222222222222222222222",
                "70",
                "33333333333333333333333333333333",
                "0102030405060708",
                "1112131415161718"
            )
        );
    }

    #[test]
    fn golden_layout4_rk_hash_sha256_prefix() {
        // rk_hash is the first 16 bytes of SHA-256 over the routing key
        // (independently computed: sha256("golden-key")[..16]).
        assert_eq!(
            hex(&crate::postings::rk_hash("golden-key").0),
            "62518b7ee254c5426695f549d536d62c"
        );
    }
}

mod descriptor_json {
    use crate::registry::{
        ForkRef, InitState, SealIntent, SealState, StreamDesc, WatchDefinition, decode_desc,
    };
    use crate::segmap::SegmentMap;
    use crate::tenant::ProjectId;

    fn proj() -> ProjectId {
        ProjectId::new("proj-test").unwrap()
    }

    /// Every skip-able field populated, every always-on field distinct.
    fn full_desc() -> StreamDesc {
        StreamDesc {
            name: "orders".into(),
            account_id: Some("acct-1".into()),
            project_id: proj(),
            stream_epoch: "00112233445566778899aabbccddeeff".into(),
            key_fingerprint: "fp-abc".into(),
            created_ms: 1_700_000_000_123,
            expires_at_ms: Some(1_800_000_000_000),
            deleted: true,
            soft_deleted: true,
            logical_close_ms: Some(1_700_100_000_000),
            forked_from: Some(ForkRef {
                source: "parent".into(),
                source_epoch: "ffeeddccbbaa99887766554433221100".into(),
                fork_offset: 42,
                fork_sub: 3,
                fork_id: "fork-1".into(),
            }),
            fork_children: vec!["child-1".into(), "child-2".into()],
            init: Some(InitState {
                request_hash: "rh-1".into(),
                key_fingerprint: "kfp-1".into(),
                claimed_ms: 5,
            }),
            content_type: "application/json".into(),
            ttl_secs: Some(3600),
            segments: Some(SegmentMap::initial("shard-0", 1_700_000_000_000)),
            sealed: true,
            seal_gen_counter: 7,
            sealing: Some(SealState {
                operation_id: "op-1".into(),
                intent: SealIntent::Final {
                    routing_key: "rk".into(),
                    request_hash: "rq".into(),
                    final_committed: true,
                },
                claimed_ms: 9,
                claim_generation: 2,
            }),
            seal_op: Some("op-0".into()),
            watch_definitions: vec![WatchDefinition {
                name: "w1".into(),
                fields: vec!["/a".into(), "/b".into()],
            }],
            parent_ref_pending: true,
            watch_sig_key: Some("wsk-1".into()),
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    const FULL_JSON: &str = r#"{"name":"orders","account_id":"acct-1","project_id":"proj-test","stream_epoch":"00112233445566778899aabbccddeeff","key_fingerprint":"fp-abc","created_ms":1700000000123,"expires_at_ms":1800000000000,"deleted":true,"soft_deleted":true,"logical_close_ms":1700100000000,"forked_from":{"source":"parent","source_epoch":"ffeeddccbbaa99887766554433221100","fork_offset":42,"fork_sub":3,"fork_id":"fork-1"},"fork_children":["child-1","child-2"],"init":{"request_hash":"rh-1","key_fingerprint":"kfp-1","claimed_ms":5},"content_type":"application/json","ttl_secs":3600,"segments":{"version":1,"next_seg_id":1,"segments":[{"seg_id":0,"lo":0,"hi":18446744073709551615,"shard_prefix":"shard-0","route_hash":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"created_ms":1700000000000,"predecessors":[],"sealed_ms":null,"sealed_next_offset":null}]},"sealed":true,"seal_gen_counter":7,"sealing":{"operation_id":"op-1","intent":{"kind":"final","routing_key":"rk","request_hash":"rq","final_committed":true},"claimed_ms":9,"claim_generation":2},"seal_op":"op-0","watch_definitions":[{"name":"w1","fields":["/a","/b"]}],"parent_ref_pending":true,"watch_sig_key":"wsk-1","layout_version":4}"#;

    /// All optional/empty/defaultable fields at their defaults.
    fn minimal_desc() -> StreamDesc {
        StreamDesc {
            name: "min".into(),
            account_id: None,
            project_id: proj(),
            stream_epoch: "00000000000000000000000000000001".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            content_type: "application/octet-stream".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            seal_gen_counter: 0,
            sealing: None,
            seal_op: None,
            watch_definitions: Vec::new(),
            parent_ref_pending: false,
            watch_sig_key: None,
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    /// NOTE, derived from the serde attributes: `deleted`, `expires_at_ms`,
    /// `ttl_secs`, `sealed`, `seal_gen_counter`, `layout_version` and
    /// `content_type` have NO skip_serializing_if — a minimal descriptor
    /// still carries them (`expires_at_ms`/`ttl_secs` as explicit nulls,
    /// `deleted: false` present). The skip-able set is: account_id,
    /// soft_deleted, logical_close_ms, forked_from, fork_children, init,
    /// segments, sealing, seal_op, watch_definitions, parent_ref_pending,
    /// watch_sig_key.
    const MINIMAL_JSON: &str = r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1,"expires_at_ms":null,"deleted":false,"content_type":"application/octet-stream","ttl_secs":null,"sealed":false,"seal_gen_counter":0,"layout_version":4}"#;

    #[test]
    fn golden_layout4_descriptor_full_json() {
        let d = full_desc();
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, FULL_JSON);
        // The golden JSON decodes through the PRODUCTION fail-closed
        // path and re-serializes byte-identically (field order pinned
        // both directions).
        let back = decode_desc(FULL_JSON.as_bytes(), None).expect("layout-4 desc decodes");
        assert_eq!(serde_json::to_string(&back).unwrap(), FULL_JSON);
    }

    #[test]
    fn golden_layout4_descriptor_minimal_json() {
        assert_eq!(
            serde_json::to_string(&minimal_desc()).unwrap(),
            MINIMAL_JSON
        );
    }

    #[test]
    fn golden_layout4_descriptor_decode_defaults() {
        // Only the mandatory fields present (name, project_id,
        // stream_epoch, key_fingerprint, created_ms): every other field
        // takes its documented serde default. layout_version defaults to
        // 0 — precisely what the layout gate then refuses.
        let d: StreamDesc = serde_json::from_str(
            r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1}"#,
        )
        .unwrap();
        assert_eq!(d.account_id, None);
        assert_eq!(d.expires_at_ms, None);
        assert!(!d.deleted);
        assert!(!d.soft_deleted);
        assert_eq!(d.logical_close_ms, None);
        assert_eq!(d.forked_from, None);
        assert!(d.fork_children.is_empty());
        assert_eq!(d.init, None);
        assert_eq!(d.content_type, "application/octet-stream");
        assert_eq!(d.ttl_secs, None);
        assert!(d.segments.is_none());
        assert!(!d.sealed);
        assert_eq!(d.seal_gen_counter, 0);
        assert_eq!(d.sealing, None);
        assert_eq!(d.seal_op, None);
        assert!(d.watch_definitions.is_empty());
        assert!(!d.parent_ref_pending);
        assert_eq!(d.watch_sig_key, None);
        assert_eq!(d.layout_version, 0);
    }

    #[test]
    fn golden_layout4_descriptor_foreign_layout_rejected() {
        // The production fail-closed path (registry::decode_desc): any
        // layout_version != 4 is refused as unsupported_storage_layout.
        for bad in [
            r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1,"layout_version":3}"#,
            r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1,"layout_version":5}"#,
            // No layout_version at all: serde default 0, also refused.
            r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1}"#,
        ] {
            let err = decode_desc(bad.as_bytes(), None).expect_err("foreign layout refused");
            let msg = err.to_string();
            assert!(
                msg.contains("unsupported_storage_layout"),
                "wrong refusal: {msg}"
            );
        }
        // The precise operator-facing text for the explicit-version case.
        let err = decode_desc(
            br#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1,"layout_version":3}"#,
            None,
        )
        .expect_err("layout 3 refused");
        assert!(err.to_string().contains("has layout 3"));
    }

    #[test]
    fn golden_layout4_descriptor_corrupt_json_errors_never_panics() {
        // Garbage and truncation are parse errors, never panics.
        let garbage = decode_desc(b"{\"name\":", None).expect_err("garbage refused");
        assert!(garbage.to_string().contains("descriptor parse"));
        let truncated = &FULL_JSON.as_bytes()[..FULL_JSON.len() / 2];
        assert!(decode_desc(truncated, None).is_err());
        assert!(decode_desc(b"", None).is_err());
        assert!(decode_desc(b"[]", None).is_err());
    }
}

mod cursors {
    use crate::crypto::StreamKey;
    use crate::offsets::{Offset, encode_ep};
    use crate::product_cursor::KeyCursor;
    use crate::tenant::ProjectId;

    fn proj() -> ProjectId {
        ProjectId::new("proj-test").unwrap()
    }

    /// Product key cursor: base64url(payload || mac16), payload =
    /// [0x12 kind][epoch 16][key_hash 16][seg_id u32 LE][offset u64 LE],
    /// mac16 = HMAC-SHA256(mac_key, payload)[..16] where
    /// mac_key = HMAC-SHA256(HKDF-SHA256(salt=epoch, ikm=stream_key,
    /// info="\0product-cursor-v2\0\0"+0u32LE), project_id).
    /// Expected string below was recomputed independently from those
    /// primitives (RFC 5869 HKDF, HMAC-SHA256, base64url-no-pad).
    #[test]
    fn golden_layout4_key_cursor_string_with_mac() {
        let c = KeyCursor {
            epoch: [1; 16],
            key_hash: [2; 16],
            seg_id: 7,
            offset: 4242,
        };
        let s = c.encode(&proj(), &StreamKey([9u8; 32]));
        assert_eq!(
            s,
            "EgEBAQEBAQEBAQEBAQEBAQECAgICAgICAgICAgICAgICBwAAAJIQAAAAAAAA79N5ZF0cdhYyslfZAoUlLw"
        );
        // The golden string still authenticates and decodes to exactly
        // the encoded fields (the literal is payload-consistent).
        let d = KeyCursor::decode(&s, &proj(), &StreamKey([9u8; 32]), &[1; 16], &[2; 16])
            .expect("golden cursor verifies");
        assert_eq!(d, c);
        // Pin the payload prefix separately from the MAC: decode the
        // base64 and check the unsigned bytes directly.
        use base64::Engine;
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&s)
            .unwrap();
        assert_eq!(raw.len(), 1 + 16 + 16 + 4 + 8 + 16);
        assert_eq!(
            crate::crypto::hex(&raw[..45]),
            concat!(
                "12",                               // KIND_KEY_V2
                "01010101010101010101010101010101", // epoch
                "02020202020202020202020202020202", // key_hash
                "07000000",                         // seg_id LE
                "9210000000000000",                 // offset 4242 LE
            )
        );
    }

    /// Raw-surface cursor: the Durable Streams offset token — 26-char
    /// Crockford base32 over a big-endian 128-bit tuple (epoch u32,
    /// rawSeq = offset+1 split hi/lo, in_block u32), padded to 130 bits.
    /// Expected strings recomputed from the alphabet
    /// "0123456789ABCDEFGHJKMNPQRSTVWXYZ" and the shift schedule.
    #[test]
    fn golden_layout4_raw_offset_tokens() {
        assert_eq!(Offset::START.encode(), "00000000000000000000000000");
        assert_eq!(Offset(Some(0)).encode(), "0000000000000000000G000000");
        assert_eq!(Offset(Some(41)).encode(), "000000000000000000N0000000");
        assert_eq!(
            Offset(Some(u64::MAX - 1)).encode(),
            "0000007ZZZZZZZZZZZZG000000"
        );
        // "-1" is the wire form of start-of-stream; every token parses
        // back to exactly its offset.
        assert_eq!(Offset::parse("-1").unwrap(), Offset::START);
        assert_eq!(
            Offset::parse("0000000000000000000G000000").unwrap(),
            Offset(Some(0))
        );
        assert_eq!(
            Offset::parse("0000007ZZZZZZZZZZZZG000000").unwrap(),
            Offset(Some(u64::MAX - 1))
        );
    }

    #[test]
    fn golden_layout4_raw_epoch_offset_token() {
        // Per-key streams put the segment ordinal in the epoch lane.
        assert_eq!(encode_ep(3, Offset(Some(5))), "000000R0000000000030000000");
        assert_eq!(
            crate::offsets::parse_ep("000000R0000000000030000000").unwrap(),
            (3, Offset(Some(5)))
        );
    }
}

mod capability {
    /// wait_sig_key = HKDF-SHA256(salt=stream_epoch, ikm=touch_token)
    ///                  .expand("wait-sig-v1", 32)
    /// — the verifier persisted in the registry for signed
    /// watch-observation URLs. Expected hex recomputed independently
    /// from RFC 5869.
    #[test]
    fn golden_layout4_wait_sig_key_derivation() {
        let token = [0x77u8; 32];
        let epoch = [0xEEu8; 16];
        let out = crate::crypto::wait_sig_key(&token, &epoch);
        assert_eq!(
            crate::crypto::hex(&out),
            "8ba1362434fa72f4d2c6b6ea0d5e7ee227f02008f1b2e2158eb8213fb6e3b314"
        );
    }
}
