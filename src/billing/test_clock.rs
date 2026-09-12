//! The injectable billing clock the month-sensitive tests share.
#![cfg(test)]

pub(crate) static BILLING_CLOCK_OVERRIDE: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// Serializes month-sensitive tests: the clock-injecting test takes
/// write; tests asserting real-now months take read.
pub(crate) fn billing_clock_lock() -> &'static tokio::sync::RwLock<()> {
    static L: std::sync::OnceLock<tokio::sync::RwLock<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::RwLock::new(()))
}
