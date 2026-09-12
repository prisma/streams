//! Test-only codec and fault-injection helpers for the shard engine.
#![cfg(test)]
use super::*;

/// Test-only: encode a tail then STRIP the trailing exact-gauge field,
/// producing the pre-gauge layout older builds wrote. DST uses this to
/// prove the R26-4 open-time repair; production code never writes it.
pub(crate) fn encode_tail_without_gauge_for_tests(t: &TailFields) -> Vec<u8> {
    let mut v = encode_tail(t);
    v.truncate(v.len() - 8);
    v
}

pub(crate) fn decode_tail_for_tests(v: &[u8]) -> Option<TailFields> {
    decode_tail(v)
}

/// Test-only: the production tail encoder, exposed so golden tests can
/// pin the exact v3 byte layout without going through a shard engine.
/// Test-only: the production dirty-row encoder, exposed so golden tests
/// can pin the exact 32-byte LE layout.
pub(crate) fn encode_tail_for_tests(t: &TailFields) -> Vec<u8> {
    encode_tail(t)
}

pub(crate) fn dirty_value_for_tests(m: &StreamMaintenance) -> [u8; 32] {
    dirty_value(m)
}

/// Test-only fault injection for the durable dirty-index scan, keyed by
/// shard prefix so concurrent tests cannot poison each other. The
/// object-store fault substrate cannot reach this path deterministically
/// (SlateDB retries store faults internally), and the absorber's
/// scan-retry loop is exactly the code under test.
pub(super) fn dirty_scan_faults() -> &'static Mutex<HashMap<String, u32>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u32>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Arrange for the next `n` dirty-index scans on `prefix` to fail.
pub(crate) fn inject_dirty_scan_faults(prefix: &str, n: u32) {
    dirty_scan_faults()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), n);
}
