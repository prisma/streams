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

#[test]
fn o2_in_place_append_reuses_storage_and_rolls_back_failed_authentication() {
    let subkey = [7; 32];
    let segment = [8; 16];
    let decoder = FrameDecryptor::new(&subkey, &segment);
    let encoder = FrameCipher::new(&subkey, &segment, FrameCompression::Disabled);
    let mut plaintext = Vec::with_capacity(64 * 1024);
    let mut auth = Vec::with_capacity(128);
    let (payload_ptr, auth_ptr) = (plaintext.as_ptr(), auth.as_ptr());
    for offset in 0..64 {
        let payload = vec![offset as u8; 1024];
        let raw = encoder.encrypt(&segment, offset, 123, 0, "lane", &payload);
        let frame = decode_frame(&raw).unwrap();
        let range = decoder
            .decrypt_append(&frame, &raw, 1024, &mut plaintext, &mut auth)
            .unwrap()
            .unwrap();
        assert_eq!(range, offset as usize * 1024..(offset as usize + 1) * 1024);
        assert_eq!(&plaintext[range], payload);
        assert_eq!(plaintext.as_ptr(), payload_ptr);
        assert_eq!(auth.as_ptr(), auth_ptr);
    }
    let before = plaintext.clone();
    let mut raw = encoder.encrypt(&segment, 64, 123, 0, "lane", b"never publish");
    *raw.last_mut().unwrap() ^= 1;
    assert!(
        decoder
            .decrypt_append(
                &decode_frame(&raw).unwrap(),
                &raw,
                1024,
                &mut plaintext,
                &mut auth
            )
            .is_err()
    );
    assert_eq!(plaintext, before);
    let raw = encoder.encrypt(&segment, 65, 123, 0, "lane", b"withheld");
    assert!(
        decoder
            .decrypt_append(
                &decode_frame(&raw).unwrap(),
                &raw,
                1,
                &mut plaintext,
                &mut auth
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(plaintext, before);
}
