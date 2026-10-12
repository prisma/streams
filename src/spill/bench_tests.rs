//! The encode bench (SPIKE-PLAN M2): a 6 MiB, 200-chunk object must encode
//! in at most 5 ms of CPU on the M1 Ultra. Run alone, in release:
//!
//! ```text
//! cargo test --locked --release --lib -- --ignored --nocapture spill::bench_
//! ```
//!
//! CPU is this thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`), so other work
//! on the host inflates it only through cache and frequency effects; wall
//! time is reported beside it. The encode is everything a seal does to
//! build its object from ring pages: offsets checked, headers and
//! directories written, every byte CRC'd, the footer built, the body sliced.
//! The page bytes are opaque random bytes (CRC cost does not depend on
//! content).
use std::time::Instant;

use rand::{SeedableRng, rngs::StdRng};

use super::object::{ChunkInput, decode, encode};
use super::test_pages::{opaque_object, pool};

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: clock_gettime writes one timespec into the valid `ts`.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0);
    (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
}

struct Stats {
    cpu_ns: Vec<u64>,
    wall_ns: Vec<u64>,
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1e6
}

fn measure(iters: usize, mut f: impl FnMut()) -> Stats {
    for _ in 0..iters / 10 + 3 {
        f();
    }
    let mut s = Stats { cpu_ns: Vec::with_capacity(iters), wall_ns: Vec::with_capacity(iters) };
    for _ in 0..iters {
        let (c0, w0) = (thread_cpu_ns(), Instant::now());
        f();
        s.cpu_ns.push(thread_cpu_ns() - c0);
        s.wall_ns.push(w0.elapsed().as_nanos() as u64);
    }
    s.cpu_ns.sort_unstable();
    s.wall_ns.sort_unstable();
    s
}

fn report(label: &str, bytes: usize, s: &Stats) -> f64 {
    let med = pct(&s.cpu_ns, 0.5);
    println!(
        "L6M2 {label}: object {bytes} B; CPU ms min {:.3} p50 {:.3} p90 {:.3} max {:.3}; wall ms p50 {:.3} p90 {:.3}; {:.0} MB/s at the CPU p50",
        pct(&s.cpu_ns, 0.0),
        med,
        pct(&s.cpu_ns, 0.9),
        pct(&s.cpu_ns, 1.0),
        pct(&s.wall_ns, 0.5),
        pct(&s.wall_ns, 0.9),
        bytes as f64 / (med / 1e3) / 1e6,
    );
    med
}

fn bench_encode(label: &str, chunks: &[ChunkInput], iters: usize) -> f64 {
    let len = encode(chunks).unwrap().len;
    let pages: usize = chunks.iter().map(|c| c.pages.len()).sum();
    println!("L6M2 {label}: {} chunks, {pages} pages", chunks.len());
    let s = measure(iters, || {
        let enc = encode(chunks).unwrap();
        std::hint::black_box(&enc);
    });
    report(label, len, &s)
}

#[test]
#[ignore = "L6 spike bench: run in release, alone"]
fn encode_6_mib_200_chunks_within_5_ms_cpu() {
    let mut rng = StdRng::seed_from_u64(0xBE7C);
    let pool = pool(&mut rng, 8 << 20);
    println!(
        "L6M2 host: {} cores; build: {}",
        std::thread::available_parallelism().map_or(0, |n| n.get()),
        if cfg!(debug_assertions) { "debug" } else { "release" }
    );
    // The bar: 6 MiB, 200 chunks of ~31 KiB, pages of ~10 KiB with 10
    // records (the b64rand 1 KiB × 10 shape).
    let headline = opaque_object(&mut rng, &pool, 200, 6 << 20, 10 * 1024, 10);
    let med = bench_encode("encode 6MiB/200 chunks/10KiB pages", &headline, 300);
    // Sensitivity (reported, no bar): single 1 KiB records (6,144 pages);
    // 300 B singles (~21,000 pages); 10,000 chunks; a 64 KiB-page object.
    let singles = opaque_object(&mut rng, &pool, 200, 6 << 20, 1024, 1);
    bench_encode("encode 6MiB/200 chunks/1KiB 1-record pages", &singles, 200);
    let tiny = opaque_object(&mut rng, &pool, 200, 6 << 20, 300, 1);
    bench_encode("encode 6MiB/200 chunks/300B 1-record pages", &tiny, 100);
    let many = opaque_object(&mut rng, &pool, 10_000, 6 << 20, 10 * 1024, 10);
    bench_encode("encode 6MiB/10000 chunks", &many, 100);
    let big = opaque_object(&mut rng, &pool, 8, 6 << 20, 64 * 1024, 64);
    bench_encode("encode 6MiB/8 chunks/64KiB pages", &big, 200);
    // The verifying decode of the headline object (what a whole-object
    // reader or a consolidation pays), and the PUT payload build.
    let body = encode(&headline).unwrap().to_bytes();
    let s = measure(200, || {
        std::hint::black_box(decode(&body).unwrap());
    });
    report("decode 6MiB/200 chunks (verify every CRC)", body.len(), &s);
    let enc = encode(&headline).unwrap();
    let s = measure(300, || {
        std::hint::black_box(enc.payload());
    });
    report("payload() of the encoded headline object", enc.len, &s);
    let s = measure(300, || {
        std::hint::black_box(super::crc32c::crc32c(&body));
    });
    report("crc32c over the 6 MiB body alone", body.len(), &s);
    assert!(med <= 5.0, "encode p50 {med:.3} ms of CPU exceeds the 5 ms bar");
}
