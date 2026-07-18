// Microbench: is the encrypt hook a throughput factor?
use std::time::Instant;

#[path = "../crypto.rs"]
mod crypto;

use crypto::{FrameHeader, StreamKey, derive_subkey, encrypt_frame, decrypt_frame, decode_frame};

fn main() {
    use base64::Engine;
    let kb64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7u8; 32]);
    let key = StreamKey::from_b64(&kb64).expect("key");
    let epoch = [9u8; 16];
    let sub = derive_subkey(&key, &epoch, "rk", 0);
    let hash = [3u8; 16];

    for size in [256usize, 4096, 65536, 1048576] {
        let plain = vec![0x42u8; size];
        let n: u64 = match size { 256 => 200_000, 4096 => 100_000, 65536 => 20_000, _ => 2_000 };
        // warmup
        for i in 0..1000u64 {
            let h = FrameHeader { offset: i, ts_ms: 1, key_version: 0, routing_key: "rk".into() };
            let _ = encrypt_frame(&sub, &hash, &h, &plain);
        }
        let t0 = Instant::now();
        let mut sink = 0usize;
        for i in 0..n {
            let h = FrameHeader { offset: i, ts_ms: 1, key_version: 0, routing_key: "rk".into() };
            let f = encrypt_frame(&sub, &hash, &h, &plain);
            sink += f.len();
        }
        let dt = t0.elapsed().as_secs_f64();
        let per = dt / n as f64;
        println!(
            "encrypt {size:>7}B: {:>9.2} ops/s | {:>8.2} µs/op | {:>8.1} MB/s  (sink {sink})",
            n as f64 / dt, per * 1e6, (n as usize * size) as f64 / dt / 1e6
        );
        // decrypt
        let h = FrameHeader { offset: 0, ts_ms: 1, key_version: 0, routing_key: "rk".into() };
        let frame = encrypt_frame(&sub, &hash, &h, &plain);
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
            n as f64 / dt, (dt / n as f64) * 1e6, (n as usize * size) as f64 / dt / 1e6
        );
    }
}
