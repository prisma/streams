//! R01: retained legacy frames and new versions read together.
#![cfg(test)]
use super::*;

#[test]
fn r01_rfc8452_aes256_empty_plaintext_vector() {
    // RFC 8452 Appendix C.2, first AES-256-GCM-SIV test vector.
    let mut key = [0; 32];
    key[0] = 1;
    let mut nonce = [0; 12];
    nonce[0] = 3;
    let cipher = Aes256GcmSiv::new((&key).into());
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: b"", aad: b"" })
        .unwrap();
    assert_eq!(hex(&ct), "07f5f4169bbf55a8400cd47ea6fd400f");
}

#[test]
fn r01_segments_and_reused_offsets_have_safe_invocation_domains() {
    let sub = derive_subkey(&StreamKey([7; 32]), &[8; 16], "customer-1", 0);
    let parent = [1; 16];
    let successor = [2; 16];
    assert_ne!(
        segment_frame_key(&sub, &parent),
        segment_frame_key(&sub, &successor)
    );
    let a = FrameCipher::new(&sub, &parent, FrameCompression::Disabled);
    let b = FrameCipher::new(&sub, &successor, FrameCompression::Disabled);
    let p1 = [b'A'; 16];
    let p2 = [b'B'; 16];
    // Forced identical nonce proves the vetted construction protects
    // rollback and even RNG-repetition cases; ordinary writes use OsRng.
    let f1 = a.encrypt_with_nonce(&parent, 0, 1000, 0, "customer-1", &p1, [0; 12]);
    let sibling = b.encrypt_with_nonce(&successor, 0, 1000, 0, "customer-1", &p2, [0; 12]);
    let rollback = a.encrypt_with_nonce(&parent, 0, 1000, 0, "customer-1", &p2, [0; 12]);
    let c1 = decode_frame(&f1).unwrap();
    for (segment, frame) in [(successor, sibling), (parent, rollback)] {
        let decoded = decode_frame(&frame).unwrap();
        let ciphertext_xor: Vec<_> = c1.ciphertext[..16]
            .iter()
            .zip(&decoded.ciphertext[..16])
            .map(|(a, b)| a ^ b)
            .collect();
        assert_ne!(ciphertext_xor, vec![3; 16], "no repeated GCM keystream");
        assert_eq!(decrypt_frame(&sub, &segment, &decoded, &frame).unwrap(), p2);
    }
    let retry = f1.clone();
    assert_eq!(retry, f1, "retransmit encoded durable bytes exactly");
    let recreated_writer = FrameCipher::new(&sub, &parent, FrameCompression::Disabled);
    let fresh = recreated_writer.encrypt(&parent, 0, 1000, 0, "customer-1", &p2);
    assert_ne!(
        &fresh[c1.header_len - 12..c1.header_len],
        &f1[c1.header_len - 12..c1.header_len]
    );
    assert_eq!(
        decrypt_frame(&sub, &parent, &decode_frame(&fresh).unwrap(), &fresh).unwrap(),
        p2
    );
}

#[test]
fn r01_retained_legacy_frames_and_new_versions_read_together() {
    let sub = [7; 32];
    let segment = [8; 16];
    for (version, compressed) in [(LEGACY_FRAME_VER, false), (LEGACY_FRAME_VER_Z, true)] {
        let payload = vec![b'x'; 1024];
        let message = if compressed {
            zstd::bulk::compress(&payload, 1).unwrap()
        } else {
            payload.clone()
        };
        let mut header = vec![version];
        header.extend_from_slice(&0u64.to_be_bytes());
        header.extend_from_slice(&1000i64.to_be_bytes());
        header.extend_from_slice(&0u32.to_be_bytes());
        header.extend_from_slice(&1u16.to_be_bytes());
        header.push(b'k');
        let ct = Aes256Gcm::new((&sub).into())
            .encrypt(
                Nonce::from_slice(&nonce_for_offset(0)),
                Payload {
                    msg: &message,
                    aad: &aad(&segment, &header),
                },
            )
            .unwrap();
        let mut frame = header;
        frame.extend_from_slice(&u32::try_from(ct.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(&ct);
        assert_eq!(
            decrypt_frame(&sub, &segment, &decode_frame(&frame).unwrap(), &frame).unwrap(),
            payload
        );
        let replacement =
            FrameCipher::new(&sub, &segment, FrameCompression::from_enabled(compressed))
                .encrypt(&segment, 0, 1000, 0, "k", &payload);
        assert_eq!(
            decode_frame(&replacement).unwrap().ver,
            if compressed { FRAME_VER_Z } else { FRAME_VER }
        );
        assert_eq!(
            decrypt_frame(
                &sub,
                &segment,
                &decode_frame(&replacement).unwrap(),
                &replacement
            )
            .unwrap(),
            payload
        );
        let mut forged = replacement.clone();
        forged[24] ^= 1;
        assert!(
            decrypt_frame(&sub, &segment, &decode_frame(&forged).unwrap(), &forged).is_err(),
            "nonce is authenticated"
        );
    }
}

#[test]
fn r01_frame_golden_header_and_ciphertext() {
    let cipher = FrameCipher::new(&[7; 32], &[8; 16], FrameCompression::Disabled);
    let frame = cipher.encrypt_with_nonce(&[8; 16], 0, 1000, 0, "k", b"hello", [3; 12]);
    assert_eq!(
        hex(&frame[..36]),
        "04000000000000000000000000000003e80000000000016b030303030303030303030303"
    );
    assert_eq!(
        hex(&frame),
        "04000000000000000000000000000003e80000000000016b03030303030303030303030300000015d484209bdfe8375c81dbafa669e2b57200b50f9346"
    );
    assert_eq!(
        decrypt_frame(&[7; 32], &[8; 16], &decode_frame(&frame).unwrap(), &frame).unwrap(),
        b"hello"
    );
}
