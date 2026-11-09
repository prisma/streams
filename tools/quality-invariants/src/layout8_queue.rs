//! Layout 8's consumer-row keys, byte for byte (S3a-DESIGN §2.1): every row a stream's consumer owns
//! starts with the stream's hash, then the stream family byte `'S'`, then the row's tag. The keys are
//! built in `src/queue.rs` (a by-path include of this harness), and pinned here, beside it, so the
//! `queue` mutation owner's tests read the bytes each builder writes.

use crate::queue::{ack_key, config_key, cursor_key, fence_key, lease_key, state_prefix};

const HASH: [u8; 16] = [0xA5; 16];

fn key(tag: &[u8], rest: &[u8]) -> Vec<u8> {
    [HASH.as_slice(), b"S", tag, rest].concat()
}

#[test]
fn every_consumer_key_is_the_streams_hash_then_its_family_then_its_tag() {
    assert_eq!(config_key(&HASH, "worker"), key(b"C", b"worker"));
    assert_eq!(fence_key(&HASH, "worker"), key(b"F", b"worker"));
    assert_eq!(state_prefix(&HASH, b'c', "worker"), key(b"c", b"worker\0"));
    let generation = 7u64.to_be_bytes();
    assert_eq!(
        cursor_key(&HASH, "worker", 7),
        key(b"c", &[b"worker\0".as_slice(), &generation].concat())
    );
    let offset = 9u64.to_be_bytes();
    let row = [b"worker\0".as_slice(), &generation, &offset].concat();
    assert_eq!(lease_key(&HASH, "worker", 7, 9), key(b"l", &row));
    assert_eq!(ack_key(&HASH, "worker", 7, 9), key(b"x", &row));
}

#[test]
fn a_consumer_names_its_own_config_and_fence_rows() {
    assert_ne!(config_key(&HASH, "a"), config_key(&HASH, "b"));
    assert_ne!(fence_key(&HASH, "a"), fence_key(&[0x5A; 16], "a"));
    assert_ne!(config_key(&HASH, "a"), fence_key(&HASH, "a"));
}
