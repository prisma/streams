//! Compressed frame round trips and their size bounds.
#![cfg(test)]
use super::*;

fn sub() -> [u8; KEY_LEN] {
    [9u8; KEY_LEN]
}

fn hdr(offset: u64) -> FrameHeader {
    FrameHeader {
        offset,
        ts_ms: 1_753_000_000_000,
        key_version: 0,
        routing_key: "rk".into(),
    }
}

#[test]
fn new_compressed_frame_round_trip() {
    let payload = vec![b'x'; 4096];
    let hash = stream_hash("s");
    // The writer side honors the explicit policy: ZstdLevel1 emits a
    // v5 frame for a compressible payload and decodes to the original.
    let on = FrameCipher::new(&sub(), &hash, FrameCompression::ZstdLevel1);
    let h = hdr(7);
    let frame = on.encrypt(
        &hash,
        h.offset,
        h.ts_ms,
        h.key_version,
        &h.routing_key,
        &payload,
    );
    let dec = decode_frame(&frame).expect("v3 decodes");
    assert_eq!(dec.ver, FRAME_VER_Z);
    let pt = decrypt_frame(&sub(), &hash, &dec, &frame).expect("decrypts");
    assert_eq!(pt, payload);
    let frame2 = on.encrypt(
        &hash,
        h.offset,
        h.ts_ms,
        h.key_version,
        &h.routing_key,
        &payload,
    );
    assert_ne!(frame, frame2, "compressed frames also get fresh nonces");
    // Disabled policy on the same payload stays uncompressed v4.
    let off = FrameCipher::new(&sub(), &hash, FrameCompression::Disabled);
    let h2 = hdr(8);
    let frame3 = off.encrypt(
        &hash,
        h2.offset,
        h2.ts_ms,
        h2.key_version,
        &h2.routing_key,
        &payload,
    );
    assert_eq!(decode_frame(&frame3).unwrap().ver, FRAME_VER);

    // The decode/decrypt path also handles a hand-built v3 frame
    // (wire-shape pin, independent of the writer policy).
    let cipher = Aes256Gcm::new((&sub()).into());
    let z = zstd::bulk::compress(&payload, 1).unwrap();
    assert!(z.len() < payload.len());
    let h = hdr(7);
    let rk = h.routing_key.as_bytes();
    let mut header = Vec::new();
    header.push(LEGACY_FRAME_VER_Z);
    header.extend_from_slice(&h.offset.to_be_bytes());
    header.extend_from_slice(&h.ts_ms.to_be_bytes());
    header.extend_from_slice(&h.key_version.to_be_bytes());
    header.extend_from_slice(&u16::try_from(rk.len()).unwrap().to_be_bytes());
    header.extend_from_slice(rk);
    let nonce = nonce_for_offset(h.offset);
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &z[..],
                aad: &aad(&hash, &header),
            },
        )
        .unwrap();
    let mut frame = header;
    frame.extend_from_slice(&u32::try_from(ct.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&ct);

    let dec = decode_frame(&frame).expect("v3 decodes");
    assert_eq!(dec.ver, LEGACY_FRAME_VER_Z);
    let pt = decrypt_frame(&sub(), &hash, &dec, &frame).expect("decrypts");
    assert_eq!(pt, payload);
}

#[test]
fn new_uncompressed_frame_round_trip() {
    let hash = stream_hash("s");
    let cipher = FrameCipher::new(&sub(), &hash, FrameCompression::Disabled);
    let h = hdr(9);
    let frame = cipher.encrypt(
        &hash,
        h.offset,
        h.ts_ms,
        h.key_version,
        &h.routing_key,
        b"tiny",
    );
    let dec = decode_frame(&frame).expect("v2 decodes");
    assert_eq!(dec.ver, FRAME_VER);
    assert_eq!(decrypt_frame(&sub(), &hash, &dec, &frame).unwrap(), b"tiny");
}

#[test]
fn unknown_version_rejected() {
    let hash = stream_hash("s");
    let cipher = FrameCipher::new(&sub(), &hash, FrameCompression::Disabled);
    let mut frame = cipher.encrypt(&hash, 1, 0, 0, "rk", b"data");
    frame[0] = 9;
    assert!(decode_frame(&frame).is_none());
    // Flipping v2 -> v3 must fail the AAD tag, not decompress garbage.
    frame[0] = FRAME_VER_Z;
    let dec = decode_frame(&frame).unwrap();
    assert!(decrypt_frame(&sub(), &hash, &dec, &frame).is_err());
}
