use super::*;
#[test]
fn e03_one_decryptor_preserves_mixed_legacy_new_frames_and_decoded_bounds() {
    let subkey = [7; 32];
    let segment = [8; 16];
    let decoder = FrameDecryptor::new(&subkey, &segment);
    let payload = vec![b'x'; 1024];
    let mut frames = Vec::new();
    for (version, compressed) in [(LEGACY_FRAME_VER, false), (LEGACY_FRAME_VER_Z, true)] {
        let message = if compressed {
            zstd::bulk::compress(&payload, 1).unwrap()
        } else {
            payload.clone()
        };
        let mut header = vec![version];
        header.extend(0u64.to_be_bytes());
        header.extend(1000i64.to_be_bytes());
        header.extend(0u32.to_be_bytes());
        header.extend(1u16.to_be_bytes());
        header.push(b'k');
        let ciphertext = Aes256Gcm::new((&subkey).into())
            .encrypt(
                Nonce::from_slice(&nonce_for_offset(0)),
                Payload {
                    msg: &message,
                    aad: &aad(&segment, &header),
                },
            )
            .unwrap();
        header.extend((ciphertext.len() as u32).to_be_bytes());
        header.extend(ciphertext);
        frames.push(header);
        frames.push(
            FrameCipher::new(
                &subkey,
                &segment,
                FrameCompression::from_enabled(compressed),
            )
            .encrypt(&segment, 0, 1000, 0, "k", &payload),
        );
    }
    for _ in 0..3 {
        for raw in &frames {
            let frame = decode_frame(raw).unwrap();
            assert_eq!(
                decoder.decrypt(&frame, raw, 1024).unwrap().unwrap(),
                payload
            );
            assert!(decoder.decrypt(&frame, raw, 1023).unwrap().is_none());
            let mut tampered = raw.clone();
            *tampered.last_mut().unwrap() ^= 1;
            assert!(
                decoder
                    .decrypt(&decode_frame(&tampered).unwrap(), &tampered, 1024)
                    .is_err()
            );
        }
    }
    assert!(decoder.current.get().is_some());
    assert!(decoder.legacy.get().is_some());
}
