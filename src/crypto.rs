//! §3.7 encryption envelope.
//!
//! subkey = HKDF-SHA256(ikm = streamKey, salt = streamEpoch,
//!                      info = routingKey ‖ 0x00 ‖ keyVersion_le)
//! Record payloads: AES-256-GCM under the subkey with a deterministic nonce
//! derived from the record offset (unique per (streamKey, epoch, version,
//! routingKey) by G3), and the frame header bound as AAD. Deterministic
//! encryption makes re-encryption of the same plaintext byte-identical, so
//! chunk responses are byte-immutable regardless of serving tier.
//!
//! Wire/storage frame (shard log value == wire bytes):
//!   [ver u8 = 2][offset u64 BE][ts_ms i64 BE][key_version u32 BE]
//!   [rk_len u16 BE][routing key][ct_len u32 BE][ciphertext (payload+16B tag)]

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const FRAME_VER: u8 = 2;
pub const KEY_LEN: usize = 32;
pub const EPOCH_LEN: usize = 16;

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct StreamKey(pub [u8; KEY_LEN]);

impl StreamKey {
    pub fn from_b64(s: &str) -> Result<StreamKey, String> {
        use base64::Engine;
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s.trim())
            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(s.trim()))
            .map_err(|e| format!("invalid key encoding: {e}"))?;
        let arr: [u8; KEY_LEN] = raw
            .try_into()
            .map_err(|_| "stream key must be 32 bytes".to_string())?;
        Ok(StreamKey(arr))
    }

    #[allow(dead_code)] // used by the streams-keys binary via this shared module
    pub fn to_b64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }

    /// One-way fingerprint stored in the registry at creation so a wrong key
    /// on later requests is rejected with 403 instead of poisoning the
    /// stream. Bound to the stream epoch.
    pub fn fingerprint(&self, stream_epoch: &[u8; EPOCH_LEN]) -> String {
        let hk = Hkdf::<Sha256>::new(Some(stream_epoch), &self.0);
        let mut out = [0u8; 16];
        hk.expand(b"streams-key-fingerprint-v1", &mut out)
            .expect("hkdf expand");
        hex(&out)
    }
}

/// Touch capability token (PROFILES.md §6): authorizes /touch/* observation
/// without granting payload decryption. Derived, never stored — the registry
/// keeps only its fingerprint.
pub fn touch_token(key: &StreamKey, stream_epoch: &[u8; EPOCH_LEN]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(stream_epoch), &key.0);
    let mut out = [0u8; 32];
    hk.expand(b"touch-capability-v1", &mut out)
        .expect("hkdf expand");
    out
}

pub fn touch_token_fingerprint(token: &[u8; 32]) -> String {
    let digest = Sha256::digest(token);
    hex(&digest[..16])
}

/// URL-signing key, scoped below the touch token: derived by clients from
/// the token, stored by the registry so the origin can verify wait-URL
/// signatures without holding the token itself. Registry exposure grants at
/// most observation-forging — never decryption.
pub fn wait_sig_key(token: &[u8; 32], stream_epoch: &[u8; EPOCH_LEN]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(stream_epoch), token);
    let mut out = [0u8; 32];
    hk.expand(b"wait-sig-v1", &mut out).expect("hkdf expand");
    out
}

/// Capability signature embedded in collapsible wait URLs:
/// GET /touch/key/{watchKeyHex}?sig={this}. Constant per (token, key) so a
/// cohort sharing the token collapses at the CDN, while URL possession is
/// the observation capability (cache hits never consult origin auth).
pub fn wait_url_sig(sig_key: &[u8; 32], watch_key_hex: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(sig_key).expect("hmac key");
    mac.update(b"wait-url\0");
    mac.update(watch_key_hex.as_bytes());
    let out = mac.finalize().into_bytes();
    hex(&out[..8])
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok())
        .collect()
}

pub fn stream_hash(name: &str) -> [u8; 16] {
    let digest = Sha256::digest(name.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

pub fn derive_subkey(
    key: &StreamKey,
    stream_epoch: &[u8; EPOCH_LEN],
    routing_key: &str,
    key_version: u32,
) -> [u8; KEY_LEN] {
    let hk = Hkdf::<Sha256>::new(Some(stream_epoch), &key.0);
    let mut info = Vec::with_capacity(routing_key.len() + 5);
    info.extend_from_slice(routing_key.as_bytes());
    info.push(0);
    info.extend_from_slice(&key_version.to_le_bytes());
    let mut out = [0u8; KEY_LEN];
    hk.expand(&info, &mut out).expect("hkdf expand");
    out
}

fn nonce_for_offset(offset: u64) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[4..].copy_from_slice(&offset.to_be_bytes());
    n
}

pub struct FrameHeader {
    pub offset: u64,
    pub ts_ms: i64,
    pub key_version: u32,
    pub routing_key: String,
}

fn aad(stream_hash: &[u8], header: &[u8]) -> Vec<u8> {
    let mut a = Vec::with_capacity(stream_hash.len() + header.len());
    a.extend_from_slice(stream_hash);
    a.extend_from_slice(header);
    a
}

/// Encrypt one record into its wire/storage frame. Deterministic: identical
/// (subkey, header, plaintext) always yields identical bytes.
pub fn encrypt_frame(
    subkey: &[u8; KEY_LEN],
    stream_hash: &[u8],
    h: &FrameHeader,
    plaintext: &[u8],
) -> Vec<u8> {
    FrameCipher::new(subkey).encrypt(
        stream_hash,
        h.offset,
        h.ts_ms,
        h.key_version,
        &h.routing_key,
        plaintext,
    )
}

/// A reusable frame cipher: the AES key schedule is built ONCE and reused
/// for every record of a request/batch. Rebuilding the cipher per frame
/// (the old encrypt_frame path) costs ~2-3 us/record - measurable at 50k
/// events/s and pure waste inside the serial committer loop.
pub struct FrameCipher {
    cipher: Aes256Gcm,
}

impl FrameCipher {
    pub fn new(subkey: &[u8; KEY_LEN]) -> FrameCipher {
        FrameCipher {
            cipher: Aes256Gcm::new(subkey.into()),
        }
    }

    /// Encrypt one record into its wire/storage frame without re-deriving
    /// the key schedule and without cloning the routing key.
    pub fn encrypt(
        &self,
        stream_hash: &[u8],
        offset: u64,
        ts_ms: i64,
        key_version: u32,
        routing_key: &str,
        plaintext: &[u8],
    ) -> Vec<u8> {
        let rk = routing_key.as_bytes();
        let mut header = Vec::with_capacity(23 + rk.len());
        header.push(FRAME_VER);
        header.extend_from_slice(&offset.to_be_bytes());
        header.extend_from_slice(&ts_ms.to_be_bytes());
        header.extend_from_slice(&key_version.to_be_bytes());
        header.extend_from_slice(&(rk.len() as u16).to_be_bytes());
        header.extend_from_slice(rk);
        let nonce = nonce_for_offset(offset);
        let ct = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad(stream_hash, &header),
                },
            )
            .expect("aes-gcm encrypt");
        let mut frame = header;
        frame.reserve(4 + ct.len());
        frame.extend_from_slice(&(ct.len() as u32).to_be_bytes());
        frame.extend_from_slice(&ct);
        frame
    }
}

pub struct DecodedFrame<'a> {
    pub header: FrameHeader,
    pub header_len: usize,
    pub ciphertext: &'a [u8],
}

/// Parse a frame without decrypting (routing key and offsets are metadata).
pub fn decode_frame(buf: &[u8]) -> Option<DecodedFrame<'_>> {
    if buf.len() < 27 || buf[0] != FRAME_VER {
        return None;
    }
    let offset = u64::from_be_bytes(buf[1..9].try_into().ok()?);
    let ts_ms = i64::from_be_bytes(buf[9..17].try_into().ok()?);
    let key_version = u32::from_be_bytes(buf[17..21].try_into().ok()?);
    let rk_len = u16::from_be_bytes(buf[21..23].try_into().ok()?) as usize;
    let header_len = 23 + rk_len;
    let routing_key = String::from_utf8(buf.get(23..header_len)?.to_vec()).ok()?;
    let ct_len_at = header_len.checked_add(4)?;
    let ct_len = u32::from_be_bytes(buf.get(header_len..ct_len_at)?.try_into().ok()?) as usize;
    let ct_end = ct_len_at.checked_add(ct_len)?;
    // There is one canonical frame encoding. Ignoring an unauthenticated
    // suffix would let distinct byte strings decode to the same record and
    // would break the immutable-ciphertext/cache contract.
    if ct_end != buf.len() {
        return None;
    }
    let ciphertext = buf.get(ct_len_at..ct_end)?;
    Some(DecodedFrame {
        header: FrameHeader {
            offset,
            ts_ms,
            key_version,
            routing_key,
        },
        header_len,
        ciphertext,
    })
}

pub fn decrypt_frame(
    subkey: &[u8; KEY_LEN],
    stream_hash: &[u8],
    frame: &DecodedFrame<'_>,
    raw: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new(subkey.into());
    let nonce = nonce_for_offset(frame.header.offset);
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: frame.ciphertext,
                aad: &aad(stream_hash, &raw[..frame.header_len]),
            },
        )
        .map_err(|_| "decryption failed (wrong key or tampered record)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> StreamKey {
        StreamKey([7u8; 32])
    }

    #[test]
    fn round_trip_and_determinism() {
        let epoch = [3u8; 16];
        let hash = stream_hash("s1");
        let sub = derive_subkey(&key(), &epoch, "chat-42", 0);
        let h = FrameHeader {
            offset: 12345,
            ts_ms: 1_751_900_000_000,
            key_version: 0,
            routing_key: "chat-42".into(),
        };
        let f1 = encrypt_frame(&sub, &hash, &h, b"hello world");
        let f2 = encrypt_frame(&sub, &hash, &h, b"hello world");
        assert_eq!(f1, f2, "deterministic re-encryption must be byte-identical");

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

        let mut suffixed = f1.clone();
        suffixed.push(0);
        assert!(
            decode_frame(&suffixed).is_none(),
            "trailing bytes are not a canonical frame"
        );
    }

    #[test]
    fn fingerprint_stable() {
        let epoch = [3u8; 16];
        assert_eq!(key().fingerprint(&epoch), key().fingerprint(&epoch));
        assert_ne!(key().fingerprint(&epoch), key().fingerprint(&[4u8; 16]));
    }
}
