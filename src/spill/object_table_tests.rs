#![cfg(test)]
//! A segment object's chunk table, decoded from its tail: exactly the
//! encoded table from a tail of any length that holds it, a pointer to the
//! table when the tail starts after it, and every damaged byte refused.

use bytes::Bytes;

use super::object::{ChunkInput, TABLE_ENTRY_LEN, TRAILER_LEN, encode};
use super::object_table::{TAIL_BYTES, Table, decode_table};

fn chunks(n: usize) -> Vec<ChunkInput> {
    (0..n)
        .map(|i| {
            let first = 100 * i as u64;
            let mut hash = [0u8; 16];
            hash[8..].copy_from_slice(&(i as u64).to_be_bytes());
            ChunkInput {
                hash,
                first,
                max_ts: i as i64,
                pages: (0..3)
                    .map(|p| {
                        (
                            first + p,
                            Bytes::from(vec![
                                u8::try_from((i + 1) % 256).unwrap();
                                40 + usize::try_from(p).unwrap()
                            ]),
                        )
                    })
                    .collect(),
            }
        })
        .collect()
}

#[test]
fn a_tail_decodes_to_exactly_the_encoded_table() {
    let enc = encode(&chunks(5)).unwrap();
    let body = enc.to_bytes();
    let len = body.len() as u64;
    let want: Vec<_> = enc.chunks.iter().map(|c| c.entry).collect();
    let table = TRAILER_LEN + TABLE_ENTRY_LEN * 5;
    for tail in [
        table,
        table + 1,
        body.len(),
        usize::try_from(TAIL_BYTES).unwrap().min(body.len()),
    ] {
        let got = decode_table(&body.slice(body.len() - tail..), len).unwrap();
        assert_eq!(got, Table::Chunks(want.clone()), "a tail of {tail} B");
    }
    let short = decode_table(&body.slice(body.len() - table + 1..), len).unwrap();
    let table_off = len - table as u64;
    assert_eq!(
        short,
        Table::StartsAt(table_off),
        "a tail that starts inside the table"
    );
}

#[test]
fn every_damaged_byte_of_the_table_or_trailer_is_refused() {
    let enc = encode(&chunks(3)).unwrap();
    let body = enc.to_bytes().to_vec();
    let len = body.len() as u64;
    let tail = TRAILER_LEN + TABLE_ENTRY_LEN * 3;
    for at in body.len() - tail..body.len() {
        let mut bad = body.clone();
        bad[at] ^= 0x01;
        let got = decode_table(&Bytes::from(bad).slice(body.len() - tail..), len);
        assert!(
            !matches!(got, Ok(Table::Chunks(_))),
            "byte {} of the tail flipped decoded: {got:?}",
            at - (body.len() - tail)
        );
    }
}

/// One tail GET covers the table of a seal's most chunks (1,024) and no
/// more: a table of 1,025 chunks starts before it.
#[test]
fn the_tail_holds_a_table_of_1024_chunks_exactly() {
    let tail = usize::try_from(TAIL_BYTES).unwrap();
    for (n, fits) in [(1024, true), (1025, false)] {
        let body = encode(&chunks(n)).unwrap().to_bytes();
        let len = body.len() as u64;
        let got = decode_table(&body.slice(body.len() - tail..), len).unwrap();
        assert_eq!(matches!(got, Table::Chunks(_)), fits, "{n} chunks: {got:?}");
    }
}

/// Rewrite the trailer's table CRC and its own CRC after an edit, so only
/// the edited field can refuse the tail.
fn reseal(body: &mut [u8], count: usize) {
    let trailer_at = body.len() - TRAILER_LEN;
    let table_at = trailer_at - TABLE_ENTRY_LEN * count;
    let table_crc = super::crc32c::crc32c(&body[table_at..trailer_at]);
    body[trailer_at + 16..trailer_at + 20].copy_from_slice(&table_crc.to_le_bytes());
    let crc = super::crc32c::crc32c(&body[trailer_at..trailer_at + 20]);
    body[trailer_at + 20..trailer_at + 24].copy_from_slice(&crc.to_le_bytes());
}

/// A trailer with flags set, or a table row whose first offset passes its
/// last, is refused even under valid CRCs.
#[test]
fn a_flagged_trailer_or_a_reversed_row_is_refused_under_valid_crcs() {
    let body = encode(&chunks(3)).unwrap().to_bytes().to_vec();
    let len = body.len() as u64;
    let tail = TRAILER_LEN + TABLE_ENTRY_LEN * 3;
    let decode = |b: Vec<u8>| decode_table(&Bytes::from(b).slice(body.len() - tail..), len);
    let mut plain = body.clone();
    reseal(&mut plain, 3);
    assert!(
        matches!(decode(plain), Ok(Table::Chunks(_))),
        "a reseal alone decodes"
    );
    let mut flagged = body.clone();
    let flags_at = body.len() - TRAILER_LEN + 14;
    flagged[flags_at] = 1;
    reseal(&mut flagged, 3);
    assert!(decode(flagged).is_err(), "flags set");
    let mut reversed = body.clone();
    let row = body.len() - tail + TABLE_ENTRY_LEN;
    let last = u64::from_le_bytes(reversed[row + 24..row + 32].try_into().unwrap());
    reversed[row + 16..row + 24].copy_from_slice(&(last + 1).to_le_bytes());
    reseal(&mut reversed, 3);
    assert!(decode(reversed).is_err(), "first past last");
}
