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
            concat!("53", "11111111111111111111111111111111", "74")
        );
    }

    #[test]
    fn golden_layout6_pending_key_bytes() {
        // 'S' <hash16> 'P': the stream's unsealed pages, one row
        assert_eq!(
            hex(&crate::spill::pending::pending_key(&H)),
            concat!("53", "11111111111111111111111111111111", "50")
        );
    }

    #[test]
    fn golden_layout4_seq_key_bytes() {
        // <hash16> 's' <key_hash16>
        assert_eq!(
            hex(&seq_key(&H, &K)),
            concat!(
                "53", // layout 6 stream segment tag
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
                "53", // layout 6 stream segment tag
                "11111111111111111111111111111111",
                "71",
                "22222222222222222222222222222222",
                "70726f642d39" // "prod-9"
            )
        );
    }

    #[test]
    fn golden_layout4_seal_fence_key_bytes() {
        // <hash16> 'G' -> u64 LE: the durable seal fence (TLA-002-F1)
        assert_eq!(
            hex(&seal_fence_key(&H)),
            concat!("53", "11111111111111111111111111111111", "47")
        );
    }
}

mod tail_codec {
    use super::*;
    use crate::shard::{TailFields, decode_tail_for_tests, encode_tail_for_tests};

    /// A fully populated v4 tail. Each numeric field carries a distinct
    /// ascending byte pattern so field-order or endianness regressions
    /// move the golden bytes.
    fn full_tail() -> TailFields {
        TailFields {
            next: 0x0807_0605_0403_0201,
            ts: 0x1817_1615_1413_1211,
            logical: 0x2827_2625_2423_2221,
            absorbed: 0x3837_3635_3433_3231,
            seq: Some("seq-7".into()),
            closed: true,
            route: [
                0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
                0xAE, 0xAF,
            ],
            unabsorbed_bytes: 0x6867_6665_6463_6261,
            usage: None,
            billing: crate::shard::StaticBilling::Unread,
        }
    }

    /// v4 layout (layout 6):
    /// [ver=4][next u64 LE][ts i64 LE][logical u64 LE][absorbed u64 LE]
    /// [flags u8][seq_len u16 LE][seq][route 16][unabsorbed_bytes u64 LE]
    const FULL_V4_HEX: &str = concat!(
        "04",                               // ver
        "0102030405060708",                 // next LE
        "1112131415161718",                 // ts LE
        "2122232425262728",                 // logical LE
        "3132333435363738",                 // absorbed (sealed boundary) LE
        "01",                               // flags: bit0 closed
        "0500",                             // seq_len LE = 5
        "7365712d37",                       // "seq-7"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf", // route16
        "6162636465666768",                 // unabsorbed_bytes LE
    );

    fn assert_full_fields(t: &TailFields) {
        assert_eq!(t.next, 0x0807_0605_0403_0201);
        assert_eq!(t.ts, 0x1817_1615_1413_1211);
        assert_eq!(t.logical, 0x2827_2625_2423_2221);
        assert_eq!(t.absorbed, 0x3837_3635_3433_3231);
        assert_eq!(t.seq.as_deref(), Some("seq-7"));
        assert!(t.closed);
        assert_eq!(t.route[0], 0xA0);
        assert_eq!(t.route[15], 0xAF);
        assert_eq!(t.unabsorbed_bytes, 0x6867_6665_6463_6261);
    }

    #[test]
    fn golden_layout6_tail_v4_full_bytes() {
        let v = encode_tail_for_tests(&full_tail());
        assert_eq!(v.len(), 65);
        assert_eq!(hex(&v), FULL_V4_HEX);
    }

    #[test]
    fn golden_layout6_tail_v4_minimal_bytes() {
        // Default fields: 0x04, then four zero u64 lanes, flags 0,
        // seq_len 0, a zero route and a zero unsealed count: 60 bytes.
        let v = encode_tail_for_tests(&TailFields::default());
        assert_eq!(v.len(), 60);
        assert_eq!(hex(&v), format!("04{}", "00".repeat(59)));
    }

    #[test]
    fn golden_layout6_tail_v4_roundtrip_fields() {
        let d = decode_tail_for_tests(&encode_tail_for_tests(&full_tail())).expect("v4 decodes");
        assert_full_fields(&d);
    }

    #[test]
    fn golden_layout6_tail_trailing_bytes_are_usage_counters_or_refused() {
        // Layout 6 (f3): after the complete known suffix only a billed
        // segment's usage counters follow; any other trailing bytes are
        // corruption.
        let mut v = encode_tail_for_tests(&full_tail());
        v.extend_from_slice(&[0xEE; 7]);
        assert!(
            decode_tail_for_tests(&v).is_none(),
            "trailing bytes refused"
        );
        let mut billed = full_tail();
        billed.usage = Some(crate::billing::stored_meta::UsageCounters {
            usage_version: 2,
            ingest_payload_bytes_total: 300,
            ingest_records_total: 1,
            owned_frame_bytes_current: 380,
            storage_accounted_through_ms: 1_700_000_000_000,
            month_ingest_payload_bytes: 300,
            month_ingest_records: 1,
            month_storage_byte_ms: None,
        });
        let v = encode_tail_for_tests(&billed);
        assert_eq!(
            hex(&v[65..]),
            concat!(
                "46",
                "02",
                "ac02",
                "01",
                "fc02",
                "80a0abfef962",
                "ac02",
                "01",
                "00"
            )
        );
        let d = decode_tail_for_tests(&v).expect("counters decode");
        assert_full_fields(&d);
        assert_eq!(d.usage, billed.usage);
        assert!(
            decode_tail_for_tests(&v[..v.len() - 1]).is_none(),
            "partial counters refused"
        );
    }

    #[test]
    fn golden_layout6_tail_decode_rejects_short_bad_version_and_unknown_flags() {
        // Shorter than the fixed v4 prefix (60 bytes): None, not a panic.
        let minimal = encode_tail_for_tests(&TailFields::default());
        assert!(decode_tail_for_tests(&minimal[..59]).is_none());
        assert!(decode_tail_for_tests(&[]).is_none());
        // Every version byte but 4 is refused at full length, the earlier
        // layouts' 2 and 3 included: no legacy decoder.
        for ver in [0u8, 1, 2, 3, 5, 0xFF] {
            let mut v = minimal.clone();
            v[0] = ver;
            assert!(decode_tail_for_tests(&v).is_none(), "version {ver}");
        }
        // A flag bit other than closed is refused (bit 1 was history v2).
        let mut v = minimal.clone();
        v[33] = 0x02;
        assert!(decode_tail_for_tests(&v).is_none());
        // A seq_len overrunning the buffer is None, not a panic.
        let mut v = minimal;
        v[34] = 0xFF; // seq_len = 65535
        v[35] = 0xFF;
        assert!(decode_tail_for_tests(&v).is_none());
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
                "53", // layout 6 stream segment tag
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
                "53", // layout 6 stream segment tag
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
                "53", // layout 6 stream segment tag
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
                "53", // layout 6 stream segment tag
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
                "53", // layout 6 stream segment tag
                "11111111111111111111111111111111",
                "46", // 'F'
                "6331"
            )
        );
    }
}

mod billing {
    use super::*;
    use crate::billing::stored_meta::{UsageCounters, decode_static, encode_static};
    use crate::billing::{SegmentBillingMetaV1, billing_meta_key, usage_month_final_key};

    #[test]
    fn golden_layout4_billing_meta_key_bytes() {
        // <seg-hash16> 'B'
        assert_eq!(
            hex(&billing_meta_key(&H)),
            concat!("53", "11111111111111111111111111111111", "42")
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
                "53", // layout 6 stream segment tag
                "ffffffffffffffffffffffffffffffff",
                "56",
                "11111111111111111111111111111111",
                "323032362d3038" // "2026-08"
            )
        );
    }

    /// The billing meta is stored in two halves (layout 6, f3): its static
    /// row under `billing_meta_key`, and its usage counters on the tail.
    #[test]
    fn golden_layout6_billing_meta_static_row_and_counters_bytes() {
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
        let row = encode_static(&m);
        assert_eq!(
            hex(&row),
            concat!(
                "01",                   // static row version
                "0100",                 // v LE
                "02000000",             // segment_id LE
                "ea070000",             // month_year LE
                "08",                   // month (varint)
                "01",                   // flags: retained by forks
                "06616363742d31",       // "acct-1"
                "0970726f6a2d74657374", // "proj-test"
                "03736964",             // "sid"
                "066f7264657273",       // "orders"
            )
        );
        let mut counters = Vec::new();
        UsageCounters::of(&m).unwrap().encode_into(&mut counters);
        assert_eq!(
            hex(&counters),
            concat!(
                "46",                   // 'F'
                "09",                   // usage_version
                "e807",                 // ingest bytes 1000
                "0a",                   // ingest records
                "8020",                 // gauge 4096
                "8090aadefa67",         // storage clock (zigzag)
                "f403",                 // month bytes 500
                "05",                   // month records
                "01",                   // month byte-time present
                "d295fcd8ceb1aaaaab01", // low 64 bits
                "00",                   // high 64 bits
            )
        );
        let mut back = decode_static(&row).unwrap();
        UsageCounters::decode(&counters).unwrap().apply(&mut back);
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            serde_json::to_value(&m).unwrap()
        );
    }
}

mod routing_key_hash {
    use super::*;

    #[test]
    fn golden_layout4_rk_hash_sha256_prefix() {
        // A routing key's hash is the first 16 bytes of SHA-256 over the
        // key (independently computed: sha256("golden-key")[..16]); the
        // seal directory's key summaries are built from it.
        assert_eq!(
            hex(&crate::crypto::RoutingKeyHash::of("golden-key").0),
            "62518b7ee254c5426695f549d536d62c"
        );
    }
}

mod descriptor_json {
    use crate::registry::{
        ForkRef, InitState, SealIntent, SealState, WatchDefinition, decode_desc,
    };
    use crate::segmap::SegmentMap;
    use crate::tenant::ProjectId;

    fn proj() -> ProjectId {
        ProjectId::new("proj-test").unwrap()
    }

    /// Every skip-able field populated, every always-on field distinct.
    fn full_desc() -> crate::registry::PersistedDescriptor {
        crate::registry::PersistedDescriptor {
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

    const FULL_JSON: &str = r#"{"name":"orders","account_id":"acct-1","project_id":"proj-test","stream_epoch":"00112233445566778899aabbccddeeff","key_fingerprint":"fp-abc","created_ms":1700000000123,"expires_at_ms":1800000000000,"deleted":true,"soft_deleted":true,"logical_close_ms":1700100000000,"forked_from":{"source":"parent","source_epoch":"ffeeddccbbaa99887766554433221100","fork_offset":42,"fork_sub":3,"fork_id":"fork-1"},"fork_children":["child-1","child-2"],"init":{"request_hash":"rh-1","key_fingerprint":"kfp-1","claimed_ms":5},"content_type":"application/json","ttl_secs":3600,"segments":{"version":1,"next_seg_id":1,"segments":[{"seg_id":0,"lo":0,"hi":18446744073709551615,"shard_prefix":"shard-0","route_hash":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"created_ms":1700000000000,"predecessors":[],"sealed_ms":null,"sealed_next_offset":null}]},"sealed":true,"seal_gen_counter":7,"sealing":{"operation_id":"op-1","intent":{"kind":"final","routing_key":"rk","request_hash":"rq","final_committed":true},"claimed_ms":9,"claim_generation":2},"seal_op":"op-0","watch_definitions":[{"name":"w1","fields":["/a","/b"]}],"parent_ref_pending":true,"watch_sig_key":"wsk-1","layout_version":6}"#;

    /// All optional/empty/defaultable fields at their defaults.
    fn minimal_desc() -> crate::registry::PersistedDescriptor {
        crate::registry::PersistedDescriptor {
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
    const MINIMAL_JSON: &str = r#"{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1,"expires_at_ms":null,"deleted":false,"content_type":"application/octet-stream","ttl_secs":null,"sealed":false,"seal_gen_counter":0,"layout_version":6}"#;

    #[test]
    fn golden_layout6_descriptor_full_json() {
        let d = full_desc();
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, FULL_JSON);
        // The golden JSON decodes through the PRODUCTION fail-closed
        // path and re-serializes byte-identically (field order pinned
        // both directions).
        // DTO wire preservation is independent from serving validity: this
        // synthetic every-field fixture intentionally combines incompatible
        // lifecycle flags, so it cannot become serving state.
        let back: crate::registry::PersistedDescriptor = serde_json::from_str(FULL_JSON).unwrap();
        assert!(decode_desc(FULL_JSON.as_bytes(), None).is_err());
        assert_eq!(serde_json::to_string(&back).unwrap(), FULL_JSON);
    }

    #[test]
    fn golden_layout6_descriptor_minimal_json() {
        assert_eq!(
            serde_json::to_string(&minimal_desc()).unwrap(),
            MINIMAL_JSON
        );
    }

    #[test]
    fn golden_layout6_descriptor_decode_defaults() {
        // Only the mandatory fields present (name, project_id,
        // stream_epoch, key_fingerprint, created_ms): every other field
        // takes its documented serde default. layout_version defaults to
        // 0 — precisely what the layout gate then refuses.
        let d: crate::registry::PersistedDescriptor = serde_json::from_str(
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
    fn golden_layout6_descriptor_foreign_layout_rejected() {
        // The production fail-closed path (registry::decode_desc): any
        // layout_version != 6 is refused as unsupported_storage_layout,
        // layout 5 (the unsegmented shard log, the layout before 6) included.
        let with_layout = |layout: &str| {
            format!(
                r#"{{"name":"min","project_id":"proj-test","stream_epoch":"00000000000000000000000000000001","key_fingerprint":"fp","created_ms":1{layout}}}"#
            )
        };
        for bad in [
            with_layout(r#","layout_version":3"#),
            with_layout(r#","layout_version":4"#),
            with_layout(r#","layout_version":5"#),
            with_layout(r#","layout_version":7"#),
            // No layout_version at all: serde default 0, also refused.
            with_layout(""),
        ] {
            let err = decode_desc(bad.as_bytes(), None).expect_err("foreign layout refused");
            let msg = err.to_string();
            assert!(
                msg.contains("unsupported_storage_layout"),
                "wrong refusal: {msg}"
            );
        }
        // The precise operator-facing text for the layout before 6.
        let err = decode_desc(with_layout(r#","layout_version":5"#).as_bytes(), None)
            .expect_err("layout 5 refused");
        assert!(
            err.to_string()
                .contains("has layout 5 (this binary reads only 6)"),
            "wrong refusal: {err}"
        );
    }

    #[test]
    fn golden_layout6_descriptor_corrupt_json_errors_never_panics() {
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
    use crate::offsets::{encode, parse};
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

    /// Product session cursor (TLA-018-F3): base64url(payload || mac16),
    /// payload = [0x13 kind][epoch 16][key_hash 16][seg_id u32 LE]
    /// [offset u64 LE][history 16][recover u64 LE][from u64 LE][digest 16],
    /// MAC exactly as the v2 key cursor. Expected string recomputed
    /// independently from those primitives (RFC 5869 HKDF, HMAC-SHA256,
    /// base64url-no-pad).
    #[test]
    fn golden_layout4_session_cursor_string_with_mac() {
        use crate::product_cursor::{ReadCursor, SessionCursor};
        let c = SessionCursor {
            position: KeyCursor {
                epoch: [1; 16],
                key_hash: [2; 16],
                seg_id: 7,
                offset: 4242,
            },
            history: [3; 16],
            recover: 4200,
            from: 4100,
            digest: [4; 16],
        };
        let s = c.encode(&proj(), &StreamKey([9u8; 32]));
        assert_eq!(
            s,
            concat!(
                "EwEBAQEBAQEBAQEBAQEBAQECAgICAgICAgICAgICAgICBwAAAJIQAAAAAAAAAwMDAwMDAwMDAwMDAwMDA2gQ",
                "AAAAAAAABBAAAAAAAAAEBAQEBAQEBAQEBAQEBAQEG7l8nbKDJwsgWeTzJ3-rZQ"
            )
        );
        let d = ReadCursor::decode(&s, &proj(), &StreamKey([9u8; 32]), &[1; 16], &[2; 16])
            .expect("golden session cursor verifies");
        assert_eq!(d, ReadCursor::Session(c));
        use base64::Engine;
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&s)
            .unwrap();
        assert_eq!(raw.len(), 93 + 16);
        assert_eq!(
            crate::crypto::hex(&raw[45..93]),
            concat!(
                "03030303030303030303030303030303", // history
                "6810000000000000",                 // recover 4200 LE
                "0410000000000000",                 // from 4100 LE
                "04040404040404040404040404040404", // digest
            )
        );
        // A v2 key cursor still decodes, as a durable position.
        let v2 =
            "EgEBAQEBAQEBAQEBAQEBAQECAgICAgICAgICAgICAgICBwAAAJIQAAAAAAAA79N5ZF0cdhYyslfZAoUlLw";
        assert!(matches!(
            ReadCursor::decode(v2, &proj(), &StreamKey([9u8; 32]), &[1; 16], &[2; 16]),
            Ok(ReadCursor::Durable(KeyCursor { offset: 4242, .. }))
        ));
    }

    /// Raw-surface cursor: the Durable Streams offset token — 26-char
    /// Crockford base32 over a big-endian 128-bit tuple (epoch u32,
    /// rawSeq = next (the old offset+1) split hi/lo, in_block u32), padded to 130 bits.
    /// Expected strings recomputed from the alphabet
    /// "0123456789ABCDEFGHJKMNPQRSTVWXYZ" and the shift schedule.
    #[test]
    fn golden_layout4_raw_offset_tokens() {
        assert_eq!(encode(0, 0), "00000000000000000000000000");
        assert_eq!(encode(0, 1), "0000000000000000000G000000");
        assert_eq!(encode(0, 42), "000000000000000000N0000000");
        assert_eq!(encode(0, u64::MAX), "0000007ZZZZZZZZZZZZG000000");
        // "-1" is the wire form of start-of-stream; every token parses
        // back to exactly its position.
        assert_eq!(parse("-1"), Ok((0, 0)));
        assert_eq!(parse("0000000000000000000G000000"), Ok((0, 1)));
        assert_eq!(parse("0000007ZZZZZZZZZZZZG000000"), Ok((0, u64::MAX)));
    }

    #[test]
    fn golden_layout4_raw_epoch_offset_token() {
        // Per-key streams put the segment ordinal in the epoch lane.
        assert_eq!(encode(3, 6), "000000R0000000000030000000");
        assert_eq!(parse("000000R0000000000030000000"), Ok((3, 6)));
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
