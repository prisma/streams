//! Signed, versioned product cursors (spec Stage 6 + appendix §5).
//!
//! Three distinct token classes exist on the wire — protocol offsets
//! (raw route only), KEY cursors, and SCAN cursors — and each carries
//! an explicit kind byte so the wrong endpoint rejects it instead of
//! misreading it. Product cursors are authenticated: the MAC key is
//! HKDF-derived from the stream's ENCRYPTION key and epoch, so a
//! client can only mint or edit cursors for streams whose key it
//! already holds (the existing authorization boundary), and a cursor
//! can never be edited to cross a stream, tenant, routing key, or
//! snapshot bound.
//!
//! Encoding: base64url(payload || mac16). Payload layout is
//! fixed-width little-endian; the kind+version byte leads.

use crate::crypto::StreamKey;

pub const KIND_KEY_V1: u8 = 0x11;
pub const KIND_SCAN_V1: u8 = 0x21;

const MAC_LEN: usize = 16;

/// One routing-key read position (spec Stage 6 §2.3): the lineage
/// position and the consumed segment-local offset, bound to the stream
/// incarnation and the exact routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyCursor {
    /// Stream incarnation (epoch bytes) the cursor belongs to.
    pub epoch: [u8; 16],
    /// Routing-key hash — the cursor is valid for exactly this key.
    pub key_hash: [u8; 16],
    /// Segment the position lives in.
    pub seg_id: u32,
    /// Segment-local consumed-through offset (next read starts here).
    pub offset: u64,
}

fn mac_key(key: &StreamKey, epoch: &[u8; 16]) -> [u8; 32] {
    crate::crypto::derive_subkey(key, epoch, "\u{0}product-cursor\u{0}", 0)
}

fn mac16(k: &[u8; 32], payload: &[u8]) -> [u8; MAC_LEN] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(k).expect("hmac key");
    m.update(payload);
    let full = m.finalize().into_bytes();
    let mut out = [0u8; MAC_LEN];
    out.copy_from_slice(&full[..MAC_LEN]);
    out
}

fn b64(v: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v)
}

fn unb64(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .ok()
}

impl KeyCursor {
    pub fn encode(&self, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(1 + 16 + 16 + 4 + 8 + MAC_LEN);
        p.push(KIND_KEY_V1);
        p.extend_from_slice(&self.epoch);
        p.extend_from_slice(&self.key_hash);
        p.extend_from_slice(&self.seg_id.to_le_bytes());
        p.extend_from_slice(&self.offset.to_le_bytes());
        let mac = mac16(&mac_key(key, &self.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }

    /// Decode + authenticate. `expect_epoch`/`expect_key_hash` bind the
    /// cursor to the REQUESTED stream incarnation and routing key: a
    /// cursor for any other stream or key is invalid_cursor, never a
    /// silent cross-read.
    pub fn decode(
        s: &str,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
        expect_key_hash: &[u8; 16],
    ) -> Result<KeyCursor, &'static str> {
        let raw = unb64(s).ok_or("invalid_cursor")?;
        if raw.len() != 1 + 16 + 16 + 4 + 8 + MAC_LEN {
            return Err("invalid_cursor");
        }
        if raw[0] != KIND_KEY_V1 {
            // A scan cursor (or protocol offset) on a key endpoint is a
            // DIFFERENT token class, rejected explicitly.
            return Err("wrong_cursor_kind");
        }
        let (payload, mac) = raw.split_at(raw.len() - MAC_LEN);
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[1..17]);
        let want = mac16(&mac_key(key, &epoch), payload);
        // Constant-time-ish compare (16 bytes; not secret-dependent
        // branching on contents).
        if mac
            .iter()
            .zip(want.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            != 0
        {
            return Err("invalid_cursor");
        }
        let mut key_hash = [0u8; 16];
        key_hash.copy_from_slice(&payload[17..33]);
        let seg_id = u32::from_le_bytes(payload[33..37].try_into().unwrap());
        let offset = u64::from_le_bytes(payload[37..45].try_into().unwrap());
        if &epoch != expect_epoch || &key_hash != expect_key_hash {
            return Err("invalid_cursor");
        }
        Ok(KeyCursor {
            epoch,
            key_hash,
            seg_id,
            offset,
        })
    }
}

/// Snapshot-bounded scan cursor (spec Stage 6 §5.3): the whole snapshot
/// is embedded so creating a scan adds no control-plane request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanCursor {
    pub epoch: [u8; 16],
    pub map_version: u64,
    /// (segment id, end_exclusive) captured at snapshot creation, in
    /// traversal order.
    pub segments: Vec<(u32, u64)>,
    pub current_index: u32,
    pub current_offset: u64,
    /// Unix ms after which the cursor is 410 scan_expired.
    pub expires_at_ms: i64,
}

pub const SCAN_CURSOR_MAX: usize = 16 * 1024;

impl ScanCursor {
    pub fn encode(&self, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(64 + self.segments.len() * 12);
        p.push(KIND_SCAN_V1);
        p.extend_from_slice(&self.epoch);
        p.extend_from_slice(&self.map_version.to_le_bytes());
        p.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for (id, end) in &self.segments {
            p.extend_from_slice(&id.to_le_bytes());
            p.extend_from_slice(&end.to_le_bytes());
        }
        p.extend_from_slice(&self.current_index.to_le_bytes());
        p.extend_from_slice(&self.current_offset.to_le_bytes());
        p.extend_from_slice(&self.expires_at_ms.to_le_bytes());
        let mac = mac16(&mac_key(key, &self.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }

    pub fn decode(
        s: &str,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
        now_ms: i64,
    ) -> Result<ScanCursor, &'static str> {
        if s.len() > SCAN_CURSOR_MAX * 4 / 3 + 4 {
            return Err("invalid_cursor");
        }
        let raw = unb64(s).ok_or("invalid_cursor")?;
        if raw.len() < 1 + 16 + 8 + 4 + 4 + 8 + 8 + MAC_LEN {
            return Err("invalid_cursor");
        }
        if raw[0] != KIND_SCAN_V1 {
            return Err("wrong_cursor_kind");
        }
        let (payload, mac) = raw.split_at(raw.len() - MAC_LEN);
        let mut epoch = [0u8; 16];
        epoch.copy_from_slice(&payload[1..17]);
        let want = mac16(&mac_key(key, &epoch), payload);
        if mac
            .iter()
            .zip(want.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            != 0
        {
            return Err("invalid_cursor");
        }
        let map_version = u64::from_le_bytes(payload[17..25].try_into().unwrap());
        let n = u32::from_le_bytes(payload[25..29].try_into().unwrap()) as usize;
        let need = 29 + n * 12 + 4 + 8 + 8;
        if payload.len() != need || n > 4096 {
            return Err("invalid_cursor");
        }
        let mut segments = Vec::with_capacity(n);
        let mut at = 29;
        for _ in 0..n {
            let id = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap());
            let end = u64::from_le_bytes(payload[at + 4..at + 12].try_into().unwrap());
            segments.push((id, end));
            at += 12;
        }
        let current_index = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap());
        let current_offset = u64::from_le_bytes(payload[at + 4..at + 12].try_into().unwrap());
        let expires_at_ms = i64::from_le_bytes(payload[at + 12..at + 20].try_into().unwrap());
        if &epoch != expect_epoch {
            return Err("invalid_cursor");
        }
        if now_ms > expires_at_ms {
            return Err("scan_expired");
        }
        Ok(ScanCursor {
            epoch,
            map_version,
            segments,
            current_index,
            current_offset,
            expires_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> StreamKey {
        StreamKey([9u8; 32])
    }

    #[test]
    fn key_cursor_roundtrip_and_binding() {
        let c = KeyCursor {
            epoch: [1; 16],
            key_hash: [2; 16],
            seg_id: 7,
            offset: 4242,
        };
        let s = c.encode(&key());
        assert_eq!(
            KeyCursor::decode(&s, &key(), &[1; 16], &[2; 16]).unwrap(),
            c
        );
        // Wrong stream incarnation / wrong routing key / wrong stream key.
        assert!(KeyCursor::decode(&s, &key(), &[9; 16], &[2; 16]).is_err());
        assert!(KeyCursor::decode(&s, &key(), &[1; 16], &[3; 16]).is_err());
        assert!(KeyCursor::decode(&s, &StreamKey([8u8; 32]), &[1; 16], &[2; 16]).is_err());
        // Tampered byte.
        let mut raw = s.into_bytes();
        let mid = raw.len() / 2;
        raw[mid] = if raw[mid] == b'A' { b'B' } else { b'A' };
        let t = String::from_utf8(raw).unwrap();
        assert!(KeyCursor::decode(&t, &key(), &[1; 16], &[2; 16]).is_err());
    }

    #[test]
    fn scan_cursor_roundtrip_expiry_and_kind() {
        let c = ScanCursor {
            epoch: [1; 16],
            map_version: 3,
            segments: vec![(0, 100), (1, 50), (2, 75)],
            current_index: 1,
            current_offset: 10,
            expires_at_ms: 1_000_000,
        };
        let s = c.encode(&key());
        assert_eq!(
            ScanCursor::decode(&s, &key(), &[1; 16], 999_999).unwrap(),
            c
        );
        assert_eq!(
            ScanCursor::decode(&s, &key(), &[1; 16], 1_000_001).unwrap_err(),
            "scan_expired"
        );
        // A key cursor on the scan decoder (and vice versa) is a
        // DIFFERENT token class.
        let kc = KeyCursor {
            epoch: [1; 16],
            key_hash: [2; 16],
            seg_id: 0,
            offset: 0,
        }
        .encode(&key());
        assert_eq!(
            ScanCursor::decode(&kc, &key(), &[1; 16], 0).unwrap_err(),
            "wrong_cursor_kind"
        );
        assert_eq!(
            KeyCursor::decode(&s, &key(), &[1; 16], &[2; 16]).unwrap_err(),
            "wrong_cursor_kind"
        );
    }
}
