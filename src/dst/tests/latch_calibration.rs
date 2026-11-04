//! The write latch's calibration (edge change #248, pressure model 3): what a
//! tiny dirty stream and a clean resident handle hold, measured on the
//! process's allocator. `PRESSURE_DIRTY_STREAM_WEIGHT_BYTES` is the larger of
//! the dirty slope and the +8,000 batch's average, rounded up to 4 KiB (past
//! the boundary when within 10% of it) and never below 8 KiB; `HANDLE_MAX_RESIDENT` in the 1 GiB profiles is held to
//! the budget sum's 47 MB of clean handles. Run alone, in release, on Linux in a 1 GiB
//! container (the weight calibrated on Linux aarch64 release, memory-layout
//! equivalent; confirmed on AMD x86_64 Compute by the habanero run): it prints the figures and the
//! two rules' answers; it asserts only that it measured something.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;

/// Bytes the process's allocator holds in use.
fn heap_in_use() -> u64 {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: a null zone asks for every zone's totals; the statistics
        // struct is plain data, zeroed before the call fills it.
        let stats = unsafe {
            let mut s: libc::malloc_statistics_t = std::mem::zeroed();
            libc::malloc_zone_statistics(std::ptr::null_mut(), &mut s);
            s
        };
        stats.size_in_use as u64
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: mallinfo2 takes no arguments and returns plain data
        // summed over every arena.
        let info = unsafe { libc::mallinfo2() };
        (info.uordblks + info.hblkhd) as u64
    }
    #[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
    {
        0
    }
}

/// Streams `from..to` made dirty: each created, then one 9-byte record.
async fn touch(addr: std::net::SocketAddr, from: u64, to: u64) {
    let ct = [("content-type", "application/json")];
    for i in from..to {
        let path = format!("/v1/stream/cal{i:06}");
        assert_eq!(hreq(addr, "PUT", &path, &ct, b"").await.0, 201);
        let (st, _, _) = hreq(addr, "POST", &path, &ct, br#"[{"n":1}]"#).await;
        assert_eq!(st, 204);
    }
}

/// Every engine sealed until a seal has nothing left, then flushed: the
/// handles stay resident, clean.
async fn seal_all(state: &crate::http::AppState) {
    for engine in state.shards.engines() {
        while engine.seal_requested().await.unwrap().streams > 0 {}
        engine.flush_memtable().await.unwrap();
    }
}

fn resident(state: &crate::http::AppState) -> u64 {
    let engines = state.shards.engines();
    engines.iter().map(|e| e.resident_streams() as u64).sum()
}

/// The least-squares slope of `points` (x, y).
fn slope(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    let (sx, sy) = points
        .iter()
        .fold((0.0, 0.0), |(a, b), (x, y)| (a + x, b + y));
    let (mx, my) = (sx / n, sy / n);
    let num: f64 = points.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
    let den: f64 = points.iter().map(|(x, _)| (x - mx) * (x - mx)).sum();
    num / den
}

/// The weight rule: the larger figure rounded up to 4 KiB, at least 8 KiB,
/// and past the next boundary when it lies within 10% below one (the lead's
/// rule of 2026-11-04): the figure plus 10% rounded up.
fn weight_of(slope: f64, average: f64) -> u64 {
    let target = slope.max(average) * 1.1;
    let mut weight = 8_192u64;
    while (weight as f64) < target {
        weight += 4_096;
    }
    weight
}

/// The cap rule: 65,536 handles, or the largest power of two whose clean
/// handles hold at most the budget sum's handle line, 47 MB.
fn cap_of(clean: f64) -> u64 {
    let mut cap = 65_536u64;
    while cap > 1 && cap as f64 * clean > 47_000_000.0 {
        cap /= 2;
    }
    cap
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "calibration: run alone, in release, on the deployed class"]
async fn a_dirty_and_a_clean_stream_footprint() {
    let dir = std::env::temp_dir().join(format!("latch-calibration-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = object_store::local::LocalFileSystem::new_with_prefix(&dir).unwrap();
    let (state, addr) = http_rig(std::sync::Arc::new(store)).await;
    touch(addr, 0, 500).await;
    seal_all(&state).await;
    let (base, base_resident) = (heap_in_use(), resident(&state));
    let (mut at, mut dirty, mut clean, mut grown) = (500u64, Vec::new(), Vec::new(), 0u64);
    let mut last_average = 0.0;
    for n in [2_000u64, 4_000, 8_000] {
        let before = heap_in_use();
        touch(addr, at, at + n).await;
        let after = heap_in_use();
        at += n;
        grown += after - before;
        last_average = (after - before) as f64 / n as f64;
        dirty.push(((at - 500) as f64, grown as f64));
        seal_all(&state).await;
        let (sealed, handles) = (heap_in_use(), resident(&state));
        clean.push((
            (handles - base_resident) as f64,
            sealed as f64 - base as f64,
        ));
        println!(
            "LATCH_CALIBRATION batch +{n}: dirty {} B a stream, heap {after} B, clean heap {sealed} B, \
             resident {handles}, rss {} B",
            (after - before) / n,
            crate::fleet::rss_bytes()
        );
    }
    let (dirty_slope, clean_slope) = (slope(&dirty), slope(&clean));
    let (weight, cap) = (weight_of(dirty_slope, last_average), cap_of(clean_slope));
    println!(
        "LATCH_CALIBRATION dirty slope {dirty_slope:.0} B, +8,000 average {last_average:.0} B, \
         weight {weight} B; clean slope {clean_slope:.0} B a handle, HANDLE_MAX_RESIDENT {cap}"
    );
    assert!(dirty_slope > 0.0 && weight >= 8_192, "nothing was measured");
    engine_shutdown(&state).await;
    if let Err(e) = std::fs::remove_dir_all(&dir) {
        println!("LATCH_CALIBRATION left {}: {e}", dir.display());
    }
}

#[cfg(test)]
mod rules {
    use super::{cap_of, slope, weight_of};

    /// The two rules as edge change #248 states them.
    #[test]
    fn the_weight_and_cap_rules_are_the_ratified_ones() {
        assert_eq!(weight_of(4_700.0, 5_806.0), 8_192);
        assert_eq!(weight_of(7_800.0, 0.0), 12_288);
        assert_eq!(weight_of(8_193.0, 0.0), 12_288);
        assert_eq!(weight_of(11_200.0, 0.0), 16_384);
        assert_eq!(cap_of(700.0), 65_536);
        assert_eq!(cap_of(1_500.0), 16_384);
        assert_eq!(cap_of(5_687.0), 8_192);
        assert_eq!(slope(&[(0.0, 1.0), (1.0, 3.0), (2.0, 5.0)]), 2.0);
    }
}
