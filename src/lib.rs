#![recursion_limit = "512"]
//! Durable Streams server on SlateDB — library crate root (WP-01/PR 2).
//!
//! All production modules live here so the binary (`src/main.rs`) is a
//! thin composition root and tests/simulator can drive production code
//! through public module ports. Binary-only concerns (allocator, tracing
//! init, runtime construction, pre-runtime environment checks) stay in
//! the binary; bootstrap/configuration live in [`bootstrap`].

pub mod audit;
pub mod auth;
pub mod auth_feed;
pub mod backpressure;
pub mod billing;
pub mod bootstrap;
pub mod config;
pub mod crypto;
#[cfg(test)]
mod dst;
pub mod failpoints;
pub mod fleet;
mod golden_tests;
pub mod history;
pub mod http;
#[cfg(test)]
mod mt_lint;
pub mod offsets;
pub mod operator;
pub mod ops;
pub mod postings;
pub mod postings_cache;
pub mod product;
pub mod product_cursor;
pub mod project_policy;
pub mod protocol_pin;
pub mod queue;
pub mod quota;
pub mod registry;
pub mod rollup;
pub mod scaler3;
pub mod segmap;
pub mod shard;
pub mod sharddir;
pub mod sketch;
pub mod sse;
pub mod store_timing;
pub mod tenant;
pub mod touch;
pub mod touch_keys;
pub mod usage;

// Interim re-exports (WP-01): these helpers moved from the old crate-root
// main.rs into `bootstrap`; other modules still reach them via `crate::`.
// PR 3 (config model) gives them a canonical home.
pub use bootstrap::{
    compactor_profile_json, on_slatedb_rt, production_settings_families, resolved_compactor_options,
};

/// Default metadata-poll cadences, shared with the DST idle-cost pin
/// (`idle_engine_store_traffic_is_bounded_by_the_poll_cadence`). Every
/// manifest/compactions poll is a live probe-GET against Tigris — a 404
/// that costs ~200-240 ms of Tigris-internal work (docs/TIGRIS-404-COST.md)
/// — so these cadences are a cost posture, not just a freshness knob.
/// Deploy scripts intentionally do NOT override them.
pub const DEFAULT_MANIFEST_POLL_MS: u64 = 2000;
pub const DEFAULT_COMPACTOR_POLL_MS: u64 = 2500;
