//! Page-local key material; retain at most 64 expanded cipher schedules.
use crate::crypto::{DecodedFrame, FrameDecryptor, StreamKey, derive_subkey};
use std::collections::HashMap;
const MAX_CIPHERS: usize = 64;
struct KeyEntry {
    subkey: [u8; 32],
    cipher: Option<Box<FrameDecryptor>>,
}
pub(super) struct ReadKeys<'a> {
    key: &'a StreamKey,
    epoch: &'a [u8; 16],
    segment: [u8; 16],
    // mt-lint: allow(name-keyed-map): routing keys within this fixed StreamKey, epoch,
    // and physical segment are data lanes, never unqualified stream identities.
    entries: HashMap<u32, HashMap<String, KeyEntry>>,
    cached: usize,
}
impl<'a> ReadKeys<'a> {
    pub(super) fn new(key: &'a StreamKey, epoch: &'a [u8; 16], segment: [u8; 16]) -> Self {
        Self {
            key,
            epoch,
            segment,
            entries: HashMap::new(),
            cached: 0,
        }
    }
    #[cfg(test)]
    pub(super) fn decrypt(
        &mut self,
        frame: &DecodedFrame<'_>,
        raw: &[u8],
        limit: usize,
    ) -> Result<Option<Vec<u8>>, String> {
        let mut plaintext = Vec::new();
        self.decrypt_append(frame, raw, limit, &mut plaintext, &mut Vec::new())
            .map(|decoded| {
                decoded.map(|value| match value {
                    crate::crypto::Decrypted::Appended(_) => plaintext,
                    crate::crypto::Decrypted::Owned(bytes) => bytes,
                })
            })
    }
    pub fn decrypt_append(
        &mut self,
        frame: &DecodedFrame<'_>,
        raw: &[u8],
        limit: usize,
        plaintext: &mut Vec<u8>,
        auth: &mut Vec<u8>,
    ) -> Result<Option<crate::crypto::Decrypted>, String> {
        let lanes = self.entries.entry(frame.header.key_version).or_default();
        let entry = if let Some(entry) = lanes.get(frame.header.routing_key) {
            entry
        } else {
            let subkey = derive_subkey(
                self.key,
                self.epoch,
                frame.header.routing_key,
                frame.header.key_version,
            );
            let cipher = if self.cached < MAX_CIPHERS {
                self.cached += 1;
                Some(Box::new(FrameDecryptor::new(&subkey, &self.segment)))
            } else {
                None
            };
            lanes
                .entry(frame.header.routing_key.to_owned())
                .or_insert(KeyEntry { subkey, cipher })
        };
        match &entry.cipher {
            Some(cipher) => cipher.decrypt_append(frame, raw, limit, plaintext, auth),
            None => FrameDecryptor::new(&entry.subkey, &self.segment)
                .decrypt_append(frame, raw, limit, plaintext, auth),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn e03_page_cipher_cache_bounds_unique_lanes_and_fences_segment_epoch_and_version() {
        let key = StreamKey([3; 32]);
        let epoch = [4; 16];
        let hash = [5; 16];
        let mut keys = ReadKeys::new(&key, &epoch, hash);
        let mut encoded = Vec::new();
        for index in 0..128 {
            let lane = format!("lane-{index}");
            let version = (index % 2 + 1) as u32;
            let subkey = derive_subkey(&key, &epoch, &lane, version);
            let raw = crate::crypto::FrameCipher::new(
                &subkey,
                &hash,
                crate::crypto::FrameCompression::Disabled,
            )
            .encrypt(&hash, index, 123, version, &lane, b"same payload");
            let frame = crate::crypto::decode_frame(&raw).unwrap();
            assert_eq!(
                keys.decrypt(&frame, &raw, 12).unwrap().unwrap(),
                b"same payload"
            );
            assert!(keys.decrypt(&frame, &raw, 11).unwrap().is_none());
            encoded.push(raw);
        }
        assert_eq!(keys.entries.values().map(HashMap::len).sum::<usize>(), 128);
        assert_eq!(keys.cached, MAX_CIPHERS);
        assert_eq!(
            keys.entries
                .values()
                .flat_map(HashMap::values)
                .filter(|entry| entry.cipher.is_some())
                .count(),
            MAX_CIPHERS
        );
        for raw in encoded.iter().rev() {
            let frame = crate::crypto::decode_frame(raw).unwrap();
            assert_eq!(
                keys.decrypt(&frame, raw, 12).unwrap().unwrap(),
                b"same payload"
            );
        }
        assert_eq!(
            keys.cached, MAX_CIPHERS,
            "overflow must not retain more schedules"
        );
        let frame = crate::crypto::decode_frame(&encoded[0]).unwrap();
        assert!(
            ReadKeys::new(&key, &epoch, [6; 16])
                .decrypt(&frame, &encoded[0], 12)
                .is_err()
        );
        assert!(
            ReadKeys::new(&key, &[7; 16], hash)
                .decrypt(&frame, &encoded[0], 12)
                .is_err()
        );
        let mut changed = encoded[0].clone();
        changed[17..21].copy_from_slice(&2u32.to_be_bytes());
        let frame = crate::crypto::decode_frame(&changed).unwrap();
        assert!(keys.decrypt(&frame, &changed, 12).is_err());
        println!(
            "expanded cipher bytes <= {} per segment page",
            MAX_CIPHERS * std::mem::size_of::<FrameDecryptor>()
        );
    }
}
