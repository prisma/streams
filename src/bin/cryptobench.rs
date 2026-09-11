// Microbench: is the encrypt hook a throughput factor?
use std::time::Instant;

#[path = "../crypto.rs"]
#[allow(
    dead_code,
    reason = "crypto benchmark module selection; the benchmark measures frame operations from the canonical service module; token and routing entry points remain compiled but unused here"
)]
mod crypto;
#[path = "../tenant.rs"]
#[allow(
    dead_code,
    reason = "benchmark module selection; crypto shares canonical tenant types with the service; the binary does not expose every service identity operation"
)]
mod tenant;
use crypto::{
    FrameCompression, FrameHeader, StreamKey, decode_frame, decrypt_frame, derive_subkey,
    encrypt_frame,
};

// This tool measures the v2 (uncompressed) encrypt hook; compression is
// an explicit choice of the caller, so pin it here rather than reading
// any ambient configuration.
const COMPRESSION: FrameCompression = FrameCompression::Disabled;

#[expect(
    clippy::expect_used,
    reason = "main; the bench's key, frames and authentication inputs are its own fixtures, so decoding and verifying them cannot fail; a fallible bench would only restate the panic at startup"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "main; the iteration count is a bench argument far below usize on a 64-bit target; a checked conversion would only restate the pointer width the bench assumes"
)]
fn main() {
    use base64::Engine;
    let kb64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7u8; 32]);
    let key = StreamKey::from_b64(&kb64).expect("key");
    let epoch = [9u8; 16];
    let sub = derive_subkey(&key, &epoch, "rk", 0);
    let hash = [3u8; 16];

    for size in [256usize, 4096, 65536, 1048576] {
        let plain = vec![0x42u8; size];
        let n: u64 = match size {
            256 => 200_000,
            4096 => 100_000,
            65536 => 20_000,
            _ => 2_000,
        };
        // warmup
        for i in 0..1000u64 {
            let h = FrameHeader {
                offset: i,
                ts_ms: 1,
                key_version: 0,
                routing_key: "rk".into(),
            };
            let _ = encrypt_frame(&sub, &hash, &h, &plain, COMPRESSION);
        }
        let t0 = Instant::now();
        let mut sink = 0usize;
        for i in 0..n {
            let h = FrameHeader {
                offset: i,
                ts_ms: 1,
                key_version: 0,
                routing_key: "rk".into(),
            };
            let f = encrypt_frame(&sub, &hash, &h, &plain, COMPRESSION);
            sink += f.len();
        }
        let dt = t0.elapsed().as_secs_f64();
        let per = dt / n as f64;
        println!(
            "encrypt {size:>7}B: {:>9.2} ops/s | {:>8.2} µs/op | {:>8.1} MB/s  (sink {sink})",
            n as f64 / dt,
            per * 1e6,
            (n as usize * size) as f64 / dt / 1e6
        );
        // decrypt
        let h = FrameHeader {
            offset: 0,
            ts_ms: 1,
            key_version: 0,
            routing_key: "rk".into(),
        };
        let frame = encrypt_frame(&sub, &hash, &h, &plain, COMPRESSION);
        let t0 = Instant::now();
        let mut sink2 = 0usize;
        for _ in 0..n {
            let dec = decode_frame(&frame).expect("decode");
            let out = decrypt_frame(&sub, &hash, &dec, &frame).expect("decrypt");
            sink2 += out.len();
        }
        let dt = t0.elapsed().as_secs_f64();
        println!(
            "decrypt {size:>7}B: {:>9.2} ops/s | {:>8.2} µs/op | {:>8.1} MB/s  (sink {sink2})",
            n as f64 / dt,
            (dt / n as f64) * 1e6,
            (n as usize * size) as f64 / dt / 1e6
        );
        // Page-style reuse: one bound schedule and private plaintext/AAD
        // storage. Keep the existing fresh-decrypt measurement above intact.
        let decoder = crypto::FrameDecryptor::new(&sub, &hash);
        let decoded = decode_frame(&frame).expect("decode");
        let mut plaintext = Vec::with_capacity(size);
        let mut auth = Vec::with_capacity(16 + decoded.header_len);
        decoder
            .decrypt_append(&decoded, &frame, size, &mut plaintext, &mut auth)
            .expect("authenticate")
            .expect("fits");
        assert_eq!(plaintext, plain);
        let t0 = Instant::now();
        for _ in 0..n {
            plaintext.clear();
            let range = decoder
                .decrypt_append(&decoded, &frame, size, &mut plaintext, &mut auth)
                .expect("authenticate")
                .expect("fits");
            match range {
                crypto::Decrypted::Appended(range) => {
                    std::hint::black_box(&plaintext[range]);
                }
                crypto::Decrypted::Owned(bytes) => {
                    std::hint::black_box(&bytes);
                }
            }
        }
        let dt = t0.elapsed().as_secs_f64();
        println!(
            "decrypt reuse {size:>7}B: {:>9.2} ops/s | {:>8.2} µs/op | {:>8.1} MB/s",
            n as f64 / dt,
            dt / n as f64 * 1e6,
            (n as usize * size) as f64 / dt / 1e6
        );
    }
}
