//! Offline, idempotent cutover administration for managed cells.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use streams_slate::registry::Registry;

#[derive(Debug, Parser)]
#[command(name = "streams-cell-admin")]
struct Args {
    /// The sole cell present in cells.json during the placement backfill.
    #[arg(long, env = "CELL_ID")]
    cell_id: String,
    /// Mutate descriptors. Without this flag the command is an audit only.
    #[arg(long, default_value_t = false)]
    apply: bool,
    /// Required with --apply: no serving process may create streams during
    /// the bounded global descriptor scan.
    #[arg(long, default_value_t = false)]
    confirm_serving_quiesced: bool,
    /// Explicit upper bound for this migration wave.
    #[arg(long, default_value_t = 100_000)]
    max_descriptors: usize,
    #[arg(long, env = "REGISTRY_S3_ENDPOINT")]
    s3_endpoint: String,
    #[arg(long, env = "REGISTRY_S3_BUCKET")]
    s3_bucket: String,
    #[arg(long, env = "REGISTRY_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "REGISTRY_S3_ACCESS_KEY_ID")]
    s3_access_key_id: String,
    #[arg(long, env = "REGISTRY_S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: String,
    #[arg(long, env = "REGISTRY_S3_ALLOW_HTTP", default_value_t = false)]
    s3_allow_http: bool,
    #[arg(long, env = "REGISTRY_PATH_PREFIX")]
    path_prefix: String,
}

fn store(args: &Args) -> anyhow::Result<Arc<dyn ObjectStore>> {
    anyhow::ensure!(
        !args.path_prefix.is_empty() && !args.path_prefix.starts_with("cells/"),
        "the global registry prefix must be non-empty and outside cells/"
    );
    let raw: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(&args.s3_endpoint)
            .with_bucket_name(&args.s3_bucket)
            .with_region(&args.s3_region)
            .with_access_key_id(&args.s3_access_key_id)
            .with_secret_access_key(&args.s3_secret_access_key)
            .with_allow_http(args.s3_allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .context("build registry object store")?,
    );
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        raw,
        args.path_prefix.as_str(),
    )))
}

async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(
        !args.apply || args.confirm_serving_quiesced,
        "--apply requires --confirm-serving-quiesced"
    );
    let registry = Registry::new(store(&args)?);
    let operation = registry
        .migrate_single_cell_descriptors(&args.cell_id, args.max_descriptors, args.apply)
        .await
        .context("audit or migrate stream placements")?;
    if args.apply {
        let post_audit = registry
            .migrate_single_cell_descriptors(&args.cell_id, args.max_descriptors, false)
            .await
            .context("verify completed stream placement migration")?;
        anyhow::ensure!(
            post_audit.pending_placements == 0 && post_audit.pending_indices == 0,
            "post-migration audit found incomplete placement state"
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "operation": operation,
                "post_audit": post_audit,
            }))?
        );
    } else {
        println!("{}", serde_json::to_string_pretty(&operation)?);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(Args::parse()).await
}
