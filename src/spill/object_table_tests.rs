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
            ChunkInput {
                hash: [u8::try_from(i).unwrap(); 16],
                first,
                max_ts: i as i64,
                pages: (0..3)
                    .map(|p| {
                        (
                            first + p,
                            Bytes::from(vec![
                                u8::try_from(i + 1).unwrap();
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
