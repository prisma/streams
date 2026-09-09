#![no_main]
// Compile the actual private production owners. No copied decoder, stand-in
// crypto types or verification-only public service API. The unused sibling
// entry points are intentionally not fuzz targets.
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "postings fuzz target; actual private crypto source supplies nominal key types; exporting or copying those owners would weaken the boundary"
)]
#[path = "../../src/crypto.rs"]
mod crypto;
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "postings fuzz target; compile the entire actual index owner; unused planner entry points are verified by their ordinary unit tests"
)]
#[path = "../../src/postings.rs"]
mod postings;
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "postings fuzz target; actual tenant source preserves nominal identity dependencies; unused service entry points are outside this decoder target"
)]
#[path = "../../src/tenant.rs"]
mod tenant;

libfuzzer_sys::fuzz_target!(|raw: &[u8]| {
    if let Some(page) = postings::decode_page(raw) {
        assert!(!page.runs.is_empty());
        assert!(page.last_offset_exclusive > page.first_offset);
        let absolute = postings::decode_page_abs(page.first_offset, raw);
        if let Some(runs) = absolute {
            assert!(postings::ValidatedRuns::new(runs).is_some());
        }
    }
});
