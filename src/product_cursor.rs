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

#[path = "product_cursor/decode.rs"]
mod decode;

pub(crate) const KIND_KEY_V2: u8 = 0x12;
pub(crate) const KIND_SCAN_V2: u8 = 0x22;
pub(crate) const KIND_MSG_V2: u8 = 0x32;
pub(crate) const KIND_LEASE_V2: u8 = 0x42;

const MAC_LEN: usize = 16;

/// One routing-key read position (spec Stage 6 §2.3): the lineage
/// position and the consumed segment-local offset, bound to the stream
/// incarnation and the exact routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyCursor {
    /// Stream incarnation (epoch bytes) the cursor belongs to.
    pub epoch: [u8; 16],
    /// Routing-key hash — the cursor is valid for exactly this key.
    pub key_hash: [u8; 16],
    /// Segment the position lives in.
    pub seg_id: u32,
    /// Segment-local consumed-through offset (next read starts here).
    pub offset: u64,
}

/// v2 (review item 3): the subkey binds the PROJECT as well as the
/// stream key and epoch — a cursor minted for one project's stream
/// can never authenticate against another project's same-named,
/// same-keyed stream.
#[expect(
    clippy::expect_used,
    reason = "mac_key; HMAC-SHA256 accepts a key of any length, so construction from the derived key cannot fail; a fallible path would add a branch no key reaches"
)]
fn mac_key(project: &crate::tenant::ProjectId, key: &StreamKey, epoch: &[u8; 16]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let base = crate::crypto::derive_subkey(key, epoch, "\u{0}product-cursor-v2\u{0}", 0);
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(&base).expect("hmac key");
    m.update(project.as_str().as_bytes());
    let full = m.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&full);
    out
}

#[expect(
    clippy::expect_used,
    reason = "mac16; HMAC-SHA256 accepts a key of any length, so construction from the derived key cannot fail; a fallible path would add a branch no key reaches"
)]
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
    pub(crate) fn encode(&self, project: &crate::tenant::ProjectId, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(1 + 16 + 16 + 4 + 8 + MAC_LEN);
        p.push(KIND_KEY_V2);
        p.extend_from_slice(&self.epoch);
        p.extend_from_slice(&self.key_hash);
        p.extend_from_slice(&self.seg_id.to_le_bytes());
        p.extend_from_slice(&self.offset.to_le_bytes());
        let mac = mac16(&mac_key(project, key, &self.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }
}

/// Snapshot-bounded scan cursor (spec Stage 6 §5.3): the whole snapshot
/// is embedded so creating a scan adds no control-plane request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanCursor {
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

pub(crate) const SCAN_CURSOR_MAX: usize = 16 * 1024;

/// Catalog cursor (review item 3): versioned and PROJECT-BOUND — a
/// listing cursor from one project replayed under another is
/// invalid_cursor, never a silent reposition. Signed with the
/// deployment cursor key when one is configured (fleets set
/// STREAMS_CURSOR_KEY so page walks verify across instances; a
/// keyless single instance still gets the binding, and cursors from a
/// differently-configured process fail closed on shape).
pub(crate) const KIND_CATALOG_V1: u8 = 0x51;

pub(crate) struct CatalogCursor {
    pub project: crate::tenant::ProjectId,
    pub last_name: String,
}

impl CatalogCursor {
    pub(crate) fn encode(&self, key: Option<&[u8; 32]>) -> String {
        let pb = self.project.as_str().as_bytes();
        let mut p = Vec::with_capacity(1 + 2 + pb.len() + self.last_name.len() + MAC_LEN);
        p.push(KIND_CATALOG_V1);
        p.extend_from_slice(&u16::try_from(pb.len()).unwrap_or(u16::MAX).to_le_bytes());
        p.extend_from_slice(pb);
        p.extend_from_slice(self.last_name.as_bytes());
        if let Some(k) = key {
            let mac = mac16(k, &p);
            p.extend_from_slice(&mac);
        }
        b64(&p)
    }
}

impl ScanCursor {
    pub(crate) fn encode(&self, project: &crate::tenant::ProjectId, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(64 + self.segments.len() * 12);
        p.push(KIND_SCAN_V2);
        p.extend_from_slice(&self.epoch);
        p.extend_from_slice(&self.map_version.to_le_bytes());
        p.extend_from_slice(
            &u32::try_from(self.segments.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for (id, end) in &self.segments {
            p.extend_from_slice(&id.to_le_bytes());
            p.extend_from_slice(&end.to_le_bytes());
        }
        p.extend_from_slice(&self.current_index.to_le_bytes());
        p.extend_from_slice(&self.current_offset.to_le_bytes());
        p.extend_from_slice(&self.expires_at_ms.to_le_bytes());
        let mac = mac16(&mac_key(project, key, &self.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }
}

/// Opaque consumer message identity (spec Stage 2 §2.4): stream
/// incarnation + routing-key hash + segment + offset, MAC'd like every
/// product token. Clients never see internal offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageId {
    pub epoch: [u8; 16],
    pub key_hash: [u8; 16],
    pub seg_id: u32,
    pub offset: u64,
}

impl MessageId {
    pub(crate) fn encode(&self, project: &crate::tenant::ProjectId, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(1 + 16 + 16 + 4 + 8 + MAC_LEN);
        p.push(KIND_MSG_V2);
        p.extend_from_slice(&self.epoch);
        p.extend_from_slice(&self.key_hash);
        p.extend_from_slice(&self.seg_id.to_le_bytes());
        p.extend_from_slice(&self.offset.to_le_bytes());
        let mac = mac16(&mac_key(project, key, &self.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }
}

/// Generation-fenced lease token (spec Stage 2 §2.7): the message
/// identity plus the lease generation and deadline, unforgeable
/// without the stream key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaseToken {
    pub msg: MessageId,
    pub lease_gen: u32,
    /// The CONSUMER generation this lease was granted under (round
    /// 16). A settle presenting a token from a deleted generation is
    /// counted stale, never applied — even if the consumer name has
    /// since been recreated.
    pub consumer_gen: u64,
    pub deadline_ms: i64,
}

impl LeaseToken {
    pub(crate) fn encode(&self, project: &crate::tenant::ProjectId, key: &StreamKey) -> String {
        let mut p = Vec::with_capacity(1 + 16 + 16 + 4 + 8 + 4 + 8 + 8 + MAC_LEN);
        p.push(KIND_LEASE_V2);
        p.extend_from_slice(&self.msg.epoch);
        p.extend_from_slice(&self.msg.key_hash);
        p.extend_from_slice(&self.msg.seg_id.to_le_bytes());
        p.extend_from_slice(&self.msg.offset.to_le_bytes());
        p.extend_from_slice(&self.lease_gen.to_le_bytes());
        p.extend_from_slice(&self.consumer_gen.to_le_bytes());
        p.extend_from_slice(&self.deadline_ms.to_le_bytes());
        let mac = mac16(&mac_key(project, key, &self.msg.epoch), &p);
        p.extend_from_slice(&mac);
        b64(&p)
    }
}

#[cfg(test)]
mod tests {
    fn tp() -> crate::tenant::ProjectId {
        crate::tenant::ProjectId::new("proj-test").unwrap()
    }

    fn other() -> crate::tenant::ProjectId {
        crate::tenant::ProjectId::new("proj-other").unwrap()
    }

    /// Review item 3 red tests: cursors are PROJECT-BOUND.
    #[test]
    fn cursors_never_cross_projects() {
        // Same stream key, same epoch, same routing key — only the
        // project differs. The v2 subkey makes the MAC fail.
        let c = KeyCursor {
            epoch: [1; 16],
            key_hash: [2; 16],
            seg_id: 0,
            offset: 7,
        };
        let s = c.encode(&tp(), &key());
        assert!(KeyCursor::decode(&s, &tp(), &key(), &[1; 16], &[2; 16]).is_ok());
        assert!(
            KeyCursor::decode(&s, &other(), &key(), &[1; 16], &[2; 16]).is_err(),
            "a cursor minted under one project must not verify under another"
        );

        // Catalog cursor: bound without a key, signed with one.
        let cc = CatalogCursor {
            project: tp(),
            last_name: "orders".into(),
        };
        let unsigned = cc.encode(None);
        assert_eq!(
            CatalogCursor::decode(&unsigned, &tp(), None).as_deref(),
            Some("orders")
        );
        assert!(
            CatalogCursor::decode(&unsigned, &other(), None).is_none(),
            "project binding holds even unsigned"
        );
        let k = [7u8; 32];
        let signed = cc.encode(Some(&k));
        assert_eq!(
            CatalogCursor::decode(&signed, &tp(), Some(&k)).as_deref(),
            Some("orders")
        );
        // Tampered project inside a signed cursor: MAC fails.
        let mut raw = {
            use base64::Engine;
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(signed.as_bytes())
                .unwrap()
        };
        raw[4] ^= 1;
        let tampered = {
            use base64::Engine;
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&raw)
        };
        assert!(CatalogCursor::decode(&tampered, &tp(), Some(&k)).is_none());
        // The retired bare-base64 cursor form is invalid.
        let legacy = {
            use base64::Engine;
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"orders")
        };
        assert!(CatalogCursor::decode(&legacy, &tp(), None).is_none());
    }

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
        let s = c.encode(&tp(), &key());
        assert_eq!(
            KeyCursor::decode(&s, &tp(), &key(), &[1; 16], &[2; 16]).unwrap(),
            c
        );
        // Wrong stream incarnation / wrong routing key / wrong stream key.
        assert!(KeyCursor::decode(&s, &tp(), &key(), &[9; 16], &[2; 16]).is_err());
        assert!(KeyCursor::decode(&s, &tp(), &key(), &[1; 16], &[3; 16]).is_err());
        assert!(KeyCursor::decode(&s, &tp(), &StreamKey([8u8; 32]), &[1; 16], &[2; 16]).is_err());
        // Tampered byte.
        let mut raw = s.into_bytes();
        let mid = raw.len() / 2;
        raw[mid] = if raw[mid] == b'A' { b'B' } else { b'A' };
        let t = String::from_utf8(raw).unwrap();
        assert!(KeyCursor::decode(&t, &tp(), &key(), &[1; 16], &[2; 16]).is_err());
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
        let s = c.encode(&tp(), &key());
        assert_eq!(
            ScanCursor::decode(&s, &tp(), &key(), &[1; 16], 999_999).unwrap(),
            c
        );
        assert_eq!(
            ScanCursor::decode(&s, &tp(), &key(), &[1; 16], 1_000_001).unwrap_err(),
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
        .encode(&tp(), &key());
        assert_eq!(
            ScanCursor::decode(&kc, &tp(), &key(), &[1; 16], 0).unwrap_err(),
            "wrong_cursor_kind"
        );
        assert_eq!(
            KeyCursor::decode(&s, &tp(), &key(), &[1; 16], &[2; 16]).unwrap_err(),
            "wrong_cursor_kind"
        );
    }

    #[test]
    fn message_and_lease_tokens_roundtrip_and_fence() {
        let m = MessageId {
            epoch: [1; 16],
            key_hash: [2; 16],
            seg_id: 3,
            offset: 44,
        };
        let ms = m.encode(&tp(), &key());
        assert_eq!(MessageId::decode(&ms, &tp(), &key(), &[1; 16]).unwrap(), m);
        assert!(MessageId::decode(&ms, &tp(), &key(), &[9; 16]).is_err());
        assert!(MessageId::decode(&ms, &tp(), &StreamKey([8u8; 32]), &[1; 16]).is_err());
        let lt = LeaseToken {
            msg: m,
            lease_gen: 7,
            consumer_gen: 3,
            deadline_ms: 123_456,
        };
        let ls = lt.encode(&tp(), &key());
        assert_eq!(
            LeaseToken::decode(&ls, &tp(), &key(), &[1; 16]).unwrap(),
            lt
        );
        // Cross-kind: a lease token is not a message id, a message id
        // is not a cursor, and vice versa.
        assert_eq!(
            MessageId::decode(&ls, &tp(), &key(), &[1; 16]).unwrap_err(),
            "wrong_token_kind"
        );
        assert_eq!(
            LeaseToken::decode(&ms, &tp(), &key(), &[1; 16]).unwrap_err(),
            "wrong_token_kind"
        );
        assert_eq!(
            KeyCursor::decode(&ms, &tp(), &key(), &[1; 16], &[2; 16]).unwrap_err(),
            "wrong_cursor_kind"
        );
    }
}

#[cfg(test)]
#[path = "product_cursor/regressions.rs"]
mod regressions;
