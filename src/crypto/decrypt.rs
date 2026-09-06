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
