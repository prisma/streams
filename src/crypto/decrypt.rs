use super::*;
/// Immutable key/segment binding. Cached only within one bounded read page;
/// lazy schedules preserve legacy/new mixed-frame decoding without extra work.
pub(crate) struct FrameDecryptor {
    subkey: [u8; KEY_LEN],
    segment: [u8; 16],
    legacy: std::sync::OnceLock<Aes256Gcm>,
    current: std::sync::OnceLock<Aes256GcmSiv>,
}
impl FrameDecryptor {
    pub(crate) fn new(subkey: &[u8; KEY_LEN], segment: &[u8; 16]) -> Self {
        Self {
            subkey: *subkey,
            segment: *segment,
            legacy: Default::default(),
            current: Default::default(),
        }
    }
    /// Append authenticated plaintext to caller-owned bounded page storage.
    /// No slice is published until AEAD and page admission both succeed.
    pub(crate) fn decrypt_append(
        &self,
        frame: &DecodedFrame<'_>,
        raw: &[u8],
        limit: usize,
        plaintext: &mut Vec<u8>,
        auth: &mut Vec<u8>,
    ) -> Result<Option<Decrypted>, String> {
        use aes_gcm::aead::AeadInPlace;
        if raw.len() > MAX_ENCODED_FRAME || frame.ciphertext.len() > MAX_RECORD_PLAINTEXT + 16 {
            return Err("encoded record exceeds the record bound".into());
        }
        let len = frame
            .ciphertext
            .len()
            .checked_sub(16)
            .ok_or_else(|| "invalid frame tag".to_string())?;
        // Keep the existing bounded decompressor and authenticate an oversized
        // candidate before withholding it. Neither path can grow the page past
        // its admission limit or publish tentative output.
        if matches!(frame.ver, FRAME_VER_Z | LEGACY_FRAME_VER_Z)
            || len > limit.min(MAX_RECORD_PLAINTEXT)
        {
            return self
                .decrypt(frame, raw, limit)
                .map(|candidate| candidate.map(Decrypted::Owned));
        }
        auth.clear();
        auth.extend_from_slice(&self.segment);
        auth.extend_from_slice(&raw[..frame.header_len]);
        let start = plaintext.len();
        plaintext.reserve(len);
        plaintext.extend_from_slice(&frame.ciphertext[..len]);
        let tag = aes_gcm::Tag::from_slice(&frame.ciphertext[len..]);
        let result = match frame.ver {
            LEGACY_FRAME_VER => {
                let cipher = self
                    .legacy
                    .get_or_init(|| Aes256Gcm::new((&self.subkey).into()));
                cipher.decrypt_in_place_detached(
                    Nonce::from_slice(&nonce_for_offset(frame.header.offset)),
                    auth,
                    &mut plaintext[start..],
                    tag,
                )
            }
            FRAME_VER => {
                let cipher = self.current.get_or_init(|| {
                    Aes256GcmSiv::new((&segment_frame_key(&self.subkey, &self.segment)).into())
                });
                cipher.decrypt_in_place_detached(
                    Nonce::from_slice(&raw[frame.header_len - 12..frame.header_len]),
                    auth,
                    &mut plaintext[start..],
                    tag,
                )
            }
            _ => {
                plaintext.truncate(start);
                return Err("unsupported frame version".into());
            }
        };
        if result.is_err() {
            plaintext.truncate(start);
            return Err("decryption failed (wrong key or tampered record)".into());
        }
        Ok(Some(Decrypted::Appended(start..plaintext.len())))
    }

    pub(crate) fn decrypt(
        &self,
        frame: &DecodedFrame<'_>,
        raw: &[u8],
        limit: usize,
    ) -> Result<Option<Vec<u8>>, String> {
        if raw.len() > MAX_ENCODED_FRAME || frame.ciphertext.len() > MAX_RECORD_PLAINTEXT + 16 {
            return Err("encoded record exceeds the record bound".into());
        }
        let limit = limit.min(MAX_RECORD_PLAINTEXT);
        let payload = Payload {
            msg: frame.ciphertext,
            aad: &aad(&self.segment, &raw[..frame.header_len]),
        };
        let pt = match frame.ver {
            LEGACY_FRAME_VER | LEGACY_FRAME_VER_Z => {
                let cipher = self
                    .legacy
                    .get_or_init(|| Aes256Gcm::new((&self.subkey).into()));
                let nonce = nonce_for_offset(frame.header.offset);
                cipher.decrypt(Nonce::from_slice(&nonce), payload)
            }
            FRAME_VER | FRAME_VER_Z => {
                let cipher = self.current.get_or_init(|| {
                    Aes256GcmSiv::new((&segment_frame_key(&self.subkey, &self.segment)).into())
                });
                let nonce = raw
                    .get(frame.header_len - 12..frame.header_len)
                    .ok_or_else(|| "invalid frame nonce".to_string())?;
                cipher.decrypt(Nonce::from_slice(nonce), payload)
            }
            _ => return Err("unsupported frame version".into()),
        }
        .map_err(|_| "decryption failed (wrong key or tampered record)".to_string())?;
        if frame.ver == FRAME_VER_Z || frame.ver == LEGACY_FRAME_VER_Z {
            // Version byte is AAD-bound, so reaching here means the frame was
            // genuinely written compressed.
            use std::io::Read;
            let mut decoder = zstd::stream::read::Decoder::new(&pt[..])
                .map_err(|e| format!("frame decompression failed: {e}"))?;
            decoder
                .window_log_max(25)
                .map_err(|e| format!("frame window limit: {e}"))?;
            let mut decoded = Vec::new();
            decoder
                .take(limit as u64 + 1)
                .read_to_end(&mut decoded)
                .map_err(|e| format!("frame decompression failed: {e}"))?;
            return Ok((decoded.len() <= limit).then_some(decoded));
        }
        Ok((pt.len() <= limit).then_some(pt))
    }
}

#[cfg(test)]
#[path = "decrypt/tests.rs"]
mod tests;
