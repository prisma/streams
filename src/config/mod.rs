//! Centralized process configuration (WP-01, corrected in PR 3.1).
//!
//! [`ServerConfig`] is the ONE parsed, immutable configuration graph:
//! the parsed CLI ([`cli::CliArgs`]) plus every environment knob, read
//! exactly once from an explicit [`environment::Environment`] source
//! at the composition root, then PROVEN by `validate()` (PR 3.2:
//! `bootstrap::ValidatedServerConfig` is the only type `run()`
//! accepts). It is then handed to owners at construction. There is
//! deliberately no process-global config slot (`install`/`current`
//! are gone). Independent RuntimeCaps owners construct their caches,
//! journals and admission policies from these values. The executable's
//! `run()` entry starts physical process infrastructure only once.
//!
//! Layout:
//! - [`cli`]: the 84-flag command-line surface (clap DTO);
//! - [`environment`]: the environment source trait + process/map impls;
//! - [`model`]: `ServerConfig` and the 13 knob sub-configs;
//! - [`load`]: environment parsing (defaults + overlay);
//! - [`summary`]: the explicit redacted diagnostics projection.

pub(crate) mod cli;
pub(crate) mod environment;
pub(crate) mod load;
pub(crate) mod model;
pub(crate) mod notice;
pub(crate) mod profile;
pub(crate) mod summary;
pub(crate) mod validation;

#[cfg(test)]
mod tests;

pub use cli::CliArgs;
#[cfg(test)]
pub(crate) use environment::MapEnvironment;
pub use environment::{Environment, ProcessEnvironment};
pub use model::ServerConfig;
pub(crate) use model::{
    AdmissionConfig, BillingConfig, EngineConfig, FleetConfig, HistoryConfig, HttpConfig,
    ScaleConfig, ShardRuntimeConfig, SseConfig, StorageConfig,
};

#[cfg(test)]
mod certification_tests;
