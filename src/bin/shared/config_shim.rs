//! Config shim for side binaries that `#[path]`-include `../crypto.rs`.
//!
//! Since WP-01 PR 3, `crypto.rs` reads FRAME_COMPRESS from the
//! library's centralized `crate::config::current()` (an `AppConfig`
//! installed at bootstrap). Side bins compile `crypto.rs` standalone
//! and never install an AppConfig, so they include this module as
//! `mod config` (same trick as the shared `tenant` module) — it
//! preserves the pre-WP-01 standalone behavior exactly: FRAME_COMPRESS
//! parsed once from the process environment ("1" or case-insensitive
//! "true", default false).

use std::sync::Arc;

pub struct AppConfig {
    pub crypto: CryptoConfig,
}

pub struct CryptoConfig {
    pub frame_compress: bool,
}

pub fn current() -> Arc<AppConfig> {
    static C: std::sync::OnceLock<Arc<AppConfig>> = std::sync::OnceLock::new();
    C.get_or_init(|| {
        Arc::new(AppConfig {
            crypto: CryptoConfig {
                frame_compress: std::env::var("FRAME_COMPRESS")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
            },
        })
    })
    .clone()
}
