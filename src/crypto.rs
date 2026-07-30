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
//!   [ver u8][offset u64 BE][ts_ms i64 BE][key_version u32 BE]
//!   [rk_len u16 BE][routing key][ct_len u32 BE][ciphertext (payload+16B tag)]
//!
//! ver = 2: ciphertext decrypts to the record payload as-is.
//! ver = 3: payload was zstd-compressed BEFORE encryption (ciphertext never
//! compresses, so this is the only place compression can live — it shrinks
//! every downstream copy: WAL, L0, compaction, absorber reads, history).
//! The version byte sits in the AAD-bound header, so it cannot be flipped
//! without failing the tag. Writers emit v3 only when FRAME_COMPRESS is on
//! AND compression actually wins; readers accept both unconditionally.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};

pub const FRAME_VER: u8 = 2;
/// Frame whose plaintext is zstd-compressed (compress-then-encrypt).
pub const FRAME_VER_Z: u8 = 3;
pub const KEY_LEN: usize = 32;
pub const EPOCH_LEN: usize = 16;

/// Payloads below this size skip the compression attempt (zstd overhead
/// dominates, and the attempt itself costs CPU in the serial committer).
const COMPRESS_MIN_BYTES: usize = 256;

/// FRAME_COMPRESS=1 turns on compress-then-encrypt for newly written
/// frames. Read-side support is unconditional, so this can be flipped per
/// deployment without migration.
pub fn frame_compress_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("FRAME_COMPRESS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

#[derive(Clone)]
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
    if s.len() % 2 != 0 {
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

/// Shard-routing identity: `stream_hash(name)`. Keys the usage counters
/// and prefixes the history-v2 keyspace (range-splittable by route).
///
/// Distinct type from [`SegmentHash`] on purpose: the absorb-lag
/// observable was broken for its entire life by joining a name-hash
/// table against an engine-hash table of the same bare `[u8; 16]` shape
/// (docs/COST-WIDE2.md §4), and the v2 keyspace places the two hashes
/// adjacently where a silent swap would corrupt every key.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(transparent)]
pub struct RouteHash(pub [u8; 16]);

impl RouteHash {
    pub fn of(name: &str) -> Self {
        RouteHash(stream_hash(name))
    }
}

/// Storage/segment incarnation identity (the engine hash): keys engine
/// segments, the absorber lag map, and the incarnation slot of
/// history-v2 keys. NOT derivable from the stream name — per-key
/// streams mint one per touched segment. See [`RouteHash`] for why the
/// two are distinct types.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(transparent)]
pub struct SegmentHash(pub [u8; 16]);

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

fn header_bytes(h: &FrameHeader) -> Vec<u8> {
    let rk = h.routing_key.as_bytes();
    let mut buf = Vec::with_capacity(17 + rk.len());
    buf.push(FRAME_VER);
    buf.extend_from_slice(&h.offset.to_be_bytes());
    buf.extend_from_slice(&h.ts_ms.to_be_bytes());
    buf.extend_from_slice(&h.key_version.to_be_bytes());
    buf.extend_from_slice(&(rk.len() as u16).to_be_bytes());
    buf.extend_from_slice(rk);
    buf
}

fn aad(stream_hash: &[u8; 16], header: &[u8]) -> Vec<u8> {
    let mut a = Vec::with_capacity(16 + header.len());
    a.extend_from_slice(stream_hash);
    a.extend_from_slice(header);
    a
}

/// Encrypt one record into its wire/storage frame. Deterministic: identical
/// (subkey, header, plaintext) always yields identical bytes.
pub fn encrypt_frame(
    subkey: &[u8; KEY_LEN],
    stream_hash: &[u8; 16],
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
        stream_hash: &[u8; 16],
        offset: u64,
        ts_ms: i64,
        key_version: u32,
        routing_key: &str,
        plaintext: &[u8],
    ) -> Vec<u8> {
        // Compress-then-encrypt: attempted only when enabled and the
        // payload is big enough; kept only when it actually shrinks.
        // zstd at a fixed level is deterministic, preserving the
        // byte-identical re-encryption property within a deployment.
        let mut ver = FRAME_VER;
        let mut compressed: Option<Vec<u8>> = None;
        if frame_compress_enabled() && plaintext.len() >= COMPRESS_MIN_BYTES {
            if let Ok(z) = zstd::bulk::compress(plaintext, 1) {
                if z.len() < plaintext.len() {
                    ver = FRAME_VER_Z;
                    compressed = Some(z);
                }
            }
        }
        let msg: &[u8] = compressed.as_deref().unwrap_or(plaintext);
        let rk = routing_key.as_bytes();
        let mut header = Vec::with_capacity(23 + rk.len());
        header.push(ver);
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
                    msg,
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
    /// Frame version byte (FRAME_VER or FRAME_VER_Z); decides whether the
    /// decrypted payload needs zstd decompression.
    pub ver: u8,
}

/// Parse a frame without decrypting (routing key and offsets are metadata).
pub fn decode_frame(buf: &[u8]) -> Option<DecodedFrame<'_>> {
    if buf.len() < 27 || !(buf[0] == FRAME_VER || buf[0] == FRAME_VER_Z) {
        return None;
    }
    let offset = u64::from_be_bytes(buf[1..9].try_into().ok()?);
    let ts_ms = i64::from_be_bytes(buf[9..17].try_into().ok()?);
    let key_version = u32::from_be_bytes(buf[17..21].try_into().ok()?);
    let rk_len = u16::from_be_bytes(buf[21..23].try_into().ok()?) as usize;
    let header_len = 23 + rk_len;
    let routing_key = String::from_utf8(buf.get(23..header_len)?.to_vec()).ok()?;
    let ct_len = u32::from_be_bytes(buf.get(header_len..header_len + 4)?.try_into().ok()?) as usize;
    let ciphertext = buf.get(header_len + 4..header_len + 4 + ct_len)?;
    Some(DecodedFrame {
        header: FrameHeader {
            offset,
            ts_ms,
            key_version,
            routing_key,
        },
        header_len,
        ciphertext,
        ver: buf[0],
    })
}

pub fn decrypt_frame(
    subkey: &[u8; KEY_LEN],
    stream_hash: &[u8; 16],
    frame: &DecodedFrame<'_>,
    raw: &[u8],
) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new(subkey.into());
    let nonce = nonce_for_offset(frame.header.offset);
    let pt = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: frame.ciphertext,
                aad: &aad(stream_hash, &raw[..frame.header_len]),
            },
        )
        .map_err(|_| "decryption failed (wrong key or tampered record)".to_string())?;
    if frame.ver == FRAME_VER_Z {
        // Version byte is AAD-bound, so reaching here means the frame was
        // genuinely written compressed.
        return zstd::stream::decode_all(&pt[..])
            .map_err(|e| format!("frame decompression failed: {e}"));
    }
    Ok(pt)
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
    }

    #[test]
    fn fingerprint_stable() {
        let epoch = [3u8; 16];
        assert_eq!(key().fingerprint(&epoch), key().fingerprint(&epoch));
        assert_ne!(key().fingerprint(&epoch), key().fingerprint(&[4u8; 16]));
    }
}

#[cfg(test)]
mod compress_tests {
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
    fn v3_round_trip_compressible() {
        // Force-on for the test regardless of env.
        let payload = vec![b'x'; 4096];
        let cipher = FrameCipher::new(&sub());
        let hash = stream_hash("s");
        // encrypt() consults the env flag; emulate by calling the parts:
        // compress + encrypt via a v3-style frame produced with the flag on.
        // Instead of mutating process env (racy across tests), assert the
        // decode/decrypt path handles a hand-built v3 frame.
        let z = zstd::bulk::compress(&payload, 1).unwrap();
        assert!(z.len() < payload.len());
        let h = hdr(7);
        let rk = h.routing_key.as_bytes();
        let mut header = Vec::new();
        header.push(FRAME_VER_Z);
        header.extend_from_slice(&h.offset.to_be_bytes());
        header.extend_from_slice(&h.ts_ms.to_be_bytes());
        header.extend_from_slice(&h.key_version.to_be_bytes());
        header.extend_from_slice(&(rk.len() as u16).to_be_bytes());
        header.extend_from_slice(rk);
        let nonce = nonce_for_offset(h.offset);
        let ct = cipher
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &z[..],
                    aad: &aad(&hash, &header),
                },
            )
            .unwrap();
        let mut frame = header;
        frame.extend_from_slice(&(ct.len() as u32).to_be_bytes());
        frame.extend_from_slice(&ct);

        let dec = decode_frame(&frame).expect("v3 decodes");
        assert_eq!(dec.ver, FRAME_VER_Z);
        let pt = decrypt_frame(&sub(), &hash, &dec, &frame).expect("decrypts");
        assert_eq!(pt, payload);
    }

    #[test]
    fn v2_still_decodes_with_ver_field() {
        let cipher = FrameCipher::new(&sub());
        let hash = stream_hash("s");
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
        let cipher = FrameCipher::new(&sub());
        let hash = stream_hash("s");
        let mut frame = cipher.encrypt(&hash, 1, 0, 0, "rk", b"data");
        frame[0] = 9;
        assert!(decode_frame(&frame).is_none());
        // Flipping v2 -> v3 must fail the AAD tag, not decompress garbage.
        frame[0] = FRAME_VER_Z;
        let dec = decode_frame(&frame).unwrap();
        assert!(decrypt_frame(&sub(), &hash, &dec, &frame).is_err());
    }
}
