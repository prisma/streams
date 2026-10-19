#![cfg(test)]

use super::{READ_BATCH_ROW_BYTES, READ_ROUND_BYTES, ReadEmission, split_rows};
use crate::billing::{BillingIdentity, MeterSource, ReadBatch, ReadRow, UsagePayload};

fn identity(account: &str, name: &str) -> BillingIdentity {
    BillingIdentity {
        account_id: account.into(),
        project_id: "p".into(),
        stream_id: "0".repeat(32),
        stream_name: name.into(),
    }
}

fn max_row(identity: BillingIdentity) -> ReadRow {
    ReadRow {
        identity,
        read_payload_bytes: u64::MAX,
        read_records: u64::MAX,
        read_operations: u64::MAX,
        queue_operations: u64::MAX,
        append_requests: u64::MAX,
    }
}

/// An identity whose read row is bounded at exactly 512 bytes.
fn wide(stream: u32) -> BillingIdentity {
    identity("account", &format!("{stream:0>210}"))
}

fn batch(rows: u32) -> ReadBatch {
    ReadBatch {
        source: MeterSource {
            cell: "cell".into(),
            instance: "instance".into(),
            boot: "boot".into(),
        },
        seq: 9,
        from_ms: 1_000,
        to_ms: 2_000,
        rows: (0..rows).map(|i| max_row(wide(i))).collect(),
    }
}

fn record_bytes(envelope: &crate::billing::UsageEnvelope) -> usize {
    serde_json::to_vec(envelope).unwrap().len()
}

#[test]
fn a_read_row_bound_is_the_fixed_row_plus_each_escaped_identity_string() {
    let empty = BillingIdentity {
        account_id: String::new(),
        project_id: String::new(),
        stream_id: String::new(),
        stream_name: String::new(),
    };
    let encoded = serde_json::to_vec(&max_row(empty.clone())).unwrap().len();
    assert_eq!(empty.read_row_bound(), encoded + 1);
    // A space and a non-ASCII character are not escaped; a quote, a
    // backslash, a newline and a unit separator are, in at most six bytes.
    let plain = identity("a b", "ü");
    assert_eq!(plain.read_row_bound(), encoded + 1 + 3 + 1 + 32 + 2);
    let escaped = identity("a\"b\\", "x\n\u{1f}");
    assert_eq!(
        escaped.read_row_bound(),
        encoded + 1 + 4 + 1 + 32 + 3 + 4 * 5
    );
}

#[test]
fn a_read_row_bound_covers_its_encoding_for_every_byte_json_escapes() {
    for byte in 0u8..0x80 {
        let text = char::from(byte).to_string().repeat(512);
        let id = identity(&text, &text);
        let encoded = serde_json::to_vec(&max_row(id.clone())).unwrap().len();
        assert!(id.read_row_bound() > encoded, "byte {byte:#x}");
    }
}

#[test]
fn rows_split_into_ordered_runs_that_fill_the_bound_exactly() {
    let rows = batch(300).rows;
    let names = rows.iter().map(|r| r.identity.stream_name.clone());
    let names = names.collect::<Vec<_>>();
    let runs = split_rows(rows);
    assert_eq!(
        runs.iter().map(Vec::len).collect::<Vec<_>>(),
        [128, 128, 44]
    );
    let split = runs
        .iter()
        .flatten()
        .map(|r| r.identity.stream_name.clone());
    assert!(split.eq(names));
    assert_eq!(128 * wide(0).read_row_bound(), READ_BATCH_ROW_BYTES);
    assert!(split_rows(Vec::new()).is_empty());
}

#[test]
fn a_batch_over_the_ceiling_emits_runs_under_its_own_source() {
    let rb = batch(300);
    let whole = ReadEmission {
        cell: "cell",
        ceiling: 0,
    }
    .envelopes(&rb);
    assert_eq!(whole.len(), 1, "a zero ceiling is unlimited");
    let exact = record_bytes(&whole[0]);
    assert!(exact > READ_BATCH_ROW_BYTES * 2);
    let at = ReadEmission {
        cell: "cell",
        ceiling: exact,
    };
    let one = at.envelopes(&rb);
    assert_eq!((one.len(), one[0].event_id.as_str()), (1, "read/boot/9"));
    let over = ReadEmission {
        cell: "cell",
        ceiling: exact - 1,
    };
    let runs = over.envelopes(&rb);
    let mut rows = 0;
    for (i, envelope) in runs.iter().enumerate() {
        assert_eq!(envelope.event_id, format!("read/boot/9/{i}"));
        assert!(record_bytes(envelope) < READ_BATCH_ROW_BYTES + 1_024);
        let UsagePayload::ReadBatch(piece) = &envelope.payload else {
            panic!("a read envelope carries a read batch");
        };
        assert_eq!(
            (piece.source.boot.as_str(), piece.seq),
            ("boot/9", i as u64)
        );
        assert_eq!(
            (piece.from_ms, piece.to_ms, envelope.event_time_ms),
            (1_000, 2_000, 2_000)
        );
        assert_eq!(piece.source.instance, "instance");
        rows += piece.rows.len();
    }
    assert_eq!((runs.len(), rows), (3, 300));
    let again = over.envelopes(&rb);
    let ids = |envelopes: &[crate::billing::UsageEnvelope]| {
        serde_json::to_value(envelopes.iter().map(|e| &e.payload).collect::<Vec<_>>()).unwrap()
    };
    assert_eq!(ids(&again), ids(&runs), "every emission is the same runs");
}

#[test]
fn a_round_admits_batches_up_to_its_budget_and_always_one() {
    let reads = ReadEmission {
        cell: "cell",
        ceiling: 0,
    };
    let rb = batch(10);
    let size = super::super::encoded_size(&reads.envelopes(&rb)[0]);
    let (mut envelopes, mut body) = (Vec::new(), 0);
    assert!(reads.admit(&rb, &mut envelopes, &mut body, 1));
    assert_eq!((envelopes.len(), body), (1, size));
    assert!(!reads.admit(&rb, &mut envelopes, &mut body, 2 * size - 1));
    assert_eq!((envelopes.len(), body), (1, size));
    assert!(reads.admit(&rb, &mut envelopes, &mut body, 2 * size));
    assert_eq!((envelopes.len(), body), (2, 2 * size));
}

#[test]
fn a_round_takes_read_batches_up_to_four_mebibytes() {
    let reads = ReadEmission {
        cell: "cell",
        ceiling: 0,
    };
    let rb = batch(100);
    let size = super::super::encoded_size(&reads.envelopes(&rb)[0]);
    let (mut envelopes, mut body) = (Vec::new(), 0);
    while reads.admit(&rb, &mut envelopes, &mut body, READ_ROUND_BYTES) {}
    assert_eq!(envelopes.len(), (4 << 20) / size);
}
