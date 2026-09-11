//! Frame codec fixtures: key fingerprints, capability signatures and
//! the v3/v4 frame round trips.
#![cfg(test)]
use super::*;

fn key() -> StreamKey {
    StreamKey([7u8; 32])
}

#[test]
fn round_trip_and_fresh_invocation_nonce() {
    let epoch = [3u8; 16];
    let hash = stream_hash("s1");
    let sub = derive_subkey(&key(), &epoch, "chat-42", 0);
    let h = FrameHeader {
        offset: 12345,
        ts_ms: 1_751_900_000_000,
        key_version: 0,
        routing_key: "chat-42".into(),
    };
    let f1 = encrypt_frame(&sub, &hash, &h, b"hello world", FrameCompression::Disabled);
    let f2 = encrypt_frame(&sub, &hash, &h, b"hello world", FrameCompression::Disabled);
    assert_ne!(f1, f2, "new invocations must use fresh nonces");

    let dec = decode_frame(&f1).unwrap();
    assert_eq!(dec.header.offset, 12345);
    assert_eq!(dec.header.routing_key, "chat-42");
    let pt = decrypt_frame(&sub, &hash, &dec, &f1).unwrap();
    assert_eq!(pt, b"hello world");

    // wrong subkey fails
    let bad = derive_subkey(&key(), &epoch, "chat-43", 0);
    assert!(decrypt_frame(&bad, &hash, &dec, &f1).is_err());
    // different epoch => different subkey (fail-closed on stream re-creation)
    let sub2 = derive_subkey(&key(), &[4u8; 16], "chat-42", 0);
    assert_ne!(sub, sub2);
}

#[test]
fn fingerprint_stable() {
    let epoch = [3u8; 16];
    assert_eq!(key().fingerprint(&epoch), key().fingerprint(&epoch));
    assert_ne!(key().fingerprint(&epoch), key().fingerprint(&[4u8; 16]));
}
