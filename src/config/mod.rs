//! Centralized process configuration (WP-01, corrected in PR 3.1).
//!
//! [`ServerConfig`] is the ONE parsed, validated, immutable
//! configuration graph: the parsed CLI ([`cli::CliArgs`]) plus every
//! environment knob, read exactly once from an explicit
//! [`environment::Environment`] source at the composition root. It is
//! then handed to owners at construction. There is deliberately no
//! process-global config slot (`install`/`current` are gone): two
//! runtime instances in one process hold two independent values.
//!
//! Layout:
//! - [`cli`]: the 84-flag command-line surface (clap DTO);
//! - [`environment`]: the environment source trait + process/map impls;
//! - [`model`]: `ServerConfig` and the 13 knob sub-configs;
//! - [`load`]: environment parsing (defaults + overlay);
//! - [`summary`]: the explicit redacted diagnostics projection.

pub mod cli;
pub mod environment;
pub mod load;
pub mod model;
pub mod summary;

#[cfg(test)]
mod tests;

pub use cli::CliArgs;
#[cfg(test)]
pub use environment::MapEnvironment;
pub use environment::{Environment, ProcessEnvironment};
pub use model::{
    AdmissionConfig, BillingConfig, EngineConfig, FleetConfig, HistoryConfig, HttpConfig,
    RuntimeConfig, ScaleConfig, ServerConfig, ShardRuntimeConfig, SseConfig, StorageConfig,
};
