#![recursion_limit = "512"]
//! Durable Streams server on SlateDB — library crate root (WP-01).
//!
//! All production modules are PRIVATE: the library exposes exactly one
//! deliberate facade (below). The binary (`src/main.rs`) is a thin
//! composition root; in-crate tests drive production code through
//! crate-internal paths, so nothing about the internal module layout is
//! downstream-visible API (PR 3.1: a `pub mod` here would both leak
//! implementation details and silence dead-code analysis).
//!
//! The facade is the whole supported surface:
//!
//! ```text
//! let cli = CliArgs::parse();                        // clap
//! let parsed = ServerConfig::load(cli, &ProcessEnvironment);
//! let config = parsed.validate()?;                   // ValidatedServerConfig
//! run(config).await                                  // preflight, owners, serve
//! ```

mod audit;
mod auth;
mod auth_feed;
mod backpressure;
mod billing;
mod bootstrap;
mod config;
mod crypto;
#[cfg(test)]
mod dst;
mod failpoints;
mod fleet;
mod golden_tests;
mod history;
mod http;
#[cfg(test)]
mod mt_lint;
mod offsets;
mod operator;
mod ops;
mod postings;
mod postings_cache;
mod product;
mod product_cursor;
mod project_policy;
mod protocol_pin;
mod queue;
mod quota;
mod registry;
mod rollup;
mod scaler3;
mod segmap;
mod shard;
mod sharddir;
mod sketch;
mod sse;
mod store_timing;
mod tenant;
mod touch;
mod touch_keys;
mod usage;

/// The deliberate library facade: the CLI surface, the environment
/// source, the owned configuration graph (parsed and validated as two
/// distinct types), and the one runtime entry point. Nothing else is
/// public API.
pub use config::validation::{ConfigError, ValidatedServerConfig};
pub use config::{CliArgs, Environment, ProcessEnvironment, ServerConfig};

/// Build the runtime owners and serve until shutdown. Called once by
/// the binary composition root with the PROVEN configuration —
/// [`ServerConfig::validate`] is the only way to construct the
/// argument, so validation precedes every startup side effect by type,
/// not by convention. `run` is process-singleton in the current
/// transitional posture (see `bootstrap::run`); a second invocation in
/// one process fails loudly.
pub async fn run(config: ValidatedServerConfig) -> anyhow::Result<()> {
    bootstrap::run(config).await
}

/// Default metadata-poll cadences, shared with the DST idle-cost pin
/// (`idle_engine_store_traffic_is_bounded_by_the_poll_cadence`). Every
/// manifest/compactions poll is a live probe-GET against Tigris — a 404
/// that costs ~200-240 ms of Tigris-internal work (docs/TIGRIS-404-COST.md)
/// — so these cadences are a cost posture, not just a freshness knob.
/// Deploy scripts intentionally do NOT override them.
pub(crate) const DEFAULT_MANIFEST_POLL_MS: u64 = 2000;
pub(crate) const DEFAULT_COMPACTOR_POLL_MS: u64 = 2500;
