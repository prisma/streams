//! Object-store provider contract (docs/PROVIDER-CONTRACT.md): the
//! conditional-write behaviour ASM-OBJSTORE-CAS and ASM-SLATEDB-FENCE
//! state, checked against a store built exactly as the server builds it
//! ([`crate::config::ServerConfig::store_for`]: the S3 client, its timing
//! connector and wrapper, the path prefix) and then through the registry
//! and SlateDB code that relies on it.
//!
//! Three runners share one suite. The in-memory store and the repository's
//! s3lite emulator run in every test build; s3lite is served in-process
//! from its own source and reached through the production client over
//! HTTP. A real provider runs only when explicitly configured
//! (`real_provider_meets_the_provider_contract`); passing it is
//! qualification evidence for that endpoint, bucket and configuration at
//! that time, not a property of the code.
#![cfg(test)]

mod faults;
mod http_cases;
mod registry_cases;
mod s3lite_harness;
mod slatedb_cases;
mod store_cases;

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

use crate::config::{CliArgs, Environment, ProcessEnvironment, ServerConfig};

/// The one opt-in for the real-provider runner.
const OPT_IN: &str = "STREAMS_PROVIDER_CONTRACT";

/// One store under test and the run-unique names every case writes under.
pub(super) struct Backend {
    name: &'static str,
    /// The registry's store role (production: the ops store).
    ops: Arc<dyn ObjectStore>,
    /// SlateDB's store role (production: the shard store).
    shard: Arc<dyn ObjectStore>,
    /// The production SlateDB settings for this configuration.
    settings: slatedb::config::Settings,
    /// Run-unique key root for raw objects and SlateDB paths.
    root: String,
    /// Run-unique project id: registry descriptors live under it.
    project: String,
    /// A local backend has no transport errors, so any is a failure; on a
    /// real provider a transport error makes a round inconclusive.
    local: bool,
}

/// Provider facts the suite observes rather than requires.
#[derive(Debug, Clone, Copy)]
struct Observations {
    /// Re-writing byte-identical content reproduced an earlier ETag
    /// (content-derived ETags, as S3's MD5 ETags are).
    etag_repeats_for_identical_content: bool,
    /// A PUT's user metadata comes back on HEAD. SlateDB's retrying store
    /// uses it to recognise its own landed conditional PUT after a lost
    /// reply; without it that PUT reports `Fenced`.
    metadata_round_trip: bool,
}

impl Backend {
    fn new(
        name: &'static str,
        ops: Arc<dyn ObjectStore>,
        shard: Arc<dyn ObjectStore>,
        config: &ServerConfig,
        local: bool,
    ) -> Self {
        let run = format!("{:016x}", rand::random::<u64>());
        Backend {
            name,
            ops,
            shard,
            settings: crate::config::validation::shard_settings(&config.cli, &config.engine),
            root: format!("provider-contract/{run}"),
            project: format!("contract-{run}"),
            local,
        }
    }

    fn path(&self, suffix: &str) -> ObjPath {
        ObjPath::from(format!("{}/{suffix}", self.root))
    }

    fn slate_path(&self, case: &str) -> String {
        format!("{}/slatedb/{case}", self.root)
    }

    /// Best-effort removal of everything this run wrote.
    async fn cleanup(&self) {
        let registry = format!(
            "registry/v4/projects/{}/",
            crate::crypto::hex(self.project.as_bytes())
        );
        for (store, prefix) in [
            (&self.ops, self.root.clone()),
            (&self.shard, self.root.clone()),
            (&self.ops, registry),
        ] {
            let listed: Vec<ObjPath> = store
                .list(Some(&ObjPath::from(prefix)))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await
                .unwrap_or_default();
            let locations = futures_util::stream::iter(listed.into_iter().map(Ok)).boxed();
            let failures = store
                .delete_stream(locations)
                .filter(|deleted| std::future::ready(deleted.is_err()))
                .count()
                .await;
            if failures > 0 {
                eprintln!("{}: cleanup left {failures} objects", self.name);
            }
        }
    }
}

/// Retry a round that a transport error made inconclusive, a bounded
/// number of times; `round` gets the attempt number for fresh names.
async fn conclusive<T, F, Fut>(b: &Backend, label: &str, mut round: F) -> T
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    const MAX_ATTEMPTS: u32 = 4;
    for attempt in 1..=MAX_ATTEMPTS {
        if let Some(outcome) = round(attempt).await {
            return outcome;
        }
        assert!(!b.local, "{}: {label}: transport error", b.name);
        eprintln!("{}: {label}: attempt {attempt} inconclusive", b.name);
    }
    panic!(
        "{}: {label}: {MAX_ATTEMPTS} attempts were inconclusive",
        b.name
    );
}

async fn run_contract(b: &Backend) -> Observations {
    let observations = store_cases::run(b).await;
    registry_cases::run(b).await;
    slatedb_cases::run(b, observations.metadata_round_trip).await;
    b.cleanup().await;
    eprintln!("{}: provider contract passed; {observations:?}", b.name);
    observations
}

/// The server's configuration with every flag at its default, pointed at
/// `endpoint`.
fn local_config(endpoint: &str) -> ServerConfig {
    let mut cli = CliArgs::deterministic();
    cli.s3_endpoint = endpoint.into();
    cli.bucket = "provider-contract".into();
    ServerConfig::with_knob_defaults(cli)
}

fn production_stores(config: &ServerConfig) -> (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) {
    let resources = Arc::new(crate::store_timing::StoreResources::new(&config.storage));
    let ops = config
        .store_for(&config.cli.ops_bucket, &resources)
        .expect("build the ops store as the server does");
    let shard = config
        .store_for(&config.cli.shard_bucket, &resources)
        .expect("build the shard store as the server does");
    (ops, shard)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_memory_store_meets_the_provider_contract() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let config = local_config("http://127.0.0.1:1");
    let b = Backend::new("in-memory", store.clone(), store, &config, true);
    let observed = run_contract(&b).await;
    assert!(observed.metadata_round_trip, "InMemory keeps attributes");
    assert!(!observed.etag_repeats_for_identical_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3lite_through_the_production_client_meets_the_provider_contract() {
    let s3lite = s3lite_harness::S3lite::bind().await;
    let faults = s3lite.faults();
    let config = local_config(&s3lite.endpoint());
    let (ops, shard) = production_stores(&config);
    let b = Backend::new("s3lite", ops, shard, &config, true);
    let observed = s3lite
        .serve_while(async {
            let observed = run_contract(&b).await;
            http_cases::run(&b, &faults).await;
            observed
        })
        .await;
    assert!(
        !observed.metadata_round_trip,
        "s3lite stores no user metadata; the SlateDB lost-reply case relies on knowing that"
    );
    assert!(!observed.etag_repeats_for_identical_content);
}

/// The real-provider runner (docs/PROVIDER-CONTRACT.md). It reads the
/// server's own environment (`SLATE_S3_*`, `PATH_PREFIX`, the storage
/// knobs) through the server's parser and builds the stores with the
/// server's builder, so it qualifies the configuration it is given.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "qualifies a real object store; needs STREAMS_PROVIDER_CONTRACT=1 and a dedicated bucket/prefix (docs/PROVIDER-CONTRACT.md)"]
async fn real_provider_meets_the_provider_contract() {
    use clap::Parser;
    let env = ProcessEnvironment;
    if env.get(OPT_IN).as_deref() != Some("1") {
        eprintln!(
            "SKIPPED: the real-provider contract runs only with {OPT_IN}=1 \
             (docs/PROVIDER-CONTRACT.md); nothing was qualified"
        );
        return;
    }
    for required in ["SLATE_S3_ENDPOINT", "SLATE_S3_BUCKET", "PATH_PREFIX"] {
        assert!(
            env.get(required).is_some_and(|value| !value.is_empty()),
            "{OPT_IN}=1 requires {required}: the run writes under a dedicated bucket and prefix"
        );
    }
    let cli = CliArgs::try_parse_from(["streams-slate"])
        .expect("parse the server command line from the environment");
    let config = ServerConfig::load(cli, &env);
    eprintln!(
        "qualifying endpoint {} bucket {} prefix {:?} region {}",
        config.cli.s3_endpoint, config.cli.bucket, config.cli.path_prefix, config.cli.region
    );
    let (ops, shard) = production_stores(&config);
    let b = Backend::new("real-provider", ops, shard, &config, false);
    run_contract(&b).await;
}
