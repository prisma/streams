//! Restartable, conflict-detecting merge of managed-cell registry closures.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

#[derive(Debug, Parser)]
#[command(name = "streams-registry-restore")]
struct Args {
    #[arg(long, default_value = "latest")]
    snapshot_id: String,
    #[arg(long, env = "BACKUP_S3_ENDPOINT")]
    backup_endpoint: String,
    #[arg(long, env = "BACKUP_S3_BUCKET")]
    backup_bucket: String,
    #[arg(long, env = "BACKUP_S3_REGION", default_value = "us-east-1")]
    backup_region: String,
    #[arg(long, env = "BACKUP_S3_ACCESS_KEY_ID")]
    backup_access_key_id: String,
    #[arg(long, env = "BACKUP_S3_SECRET_ACCESS_KEY")]
    backup_secret_access_key: String,
    #[arg(long, env = "BACKUP_PATH_PREFIX")]
    backup_prefix: String,

    #[arg(long, env = "RESTORE_REGISTRY_S3_ENDPOINT")]
    target_endpoint: String,
    #[arg(long, env = "RESTORE_REGISTRY_S3_BUCKET")]
    target_bucket: String,
    #[arg(long, env = "RESTORE_REGISTRY_S3_REGION", default_value = "us-east-1")]
    target_region: String,
    #[arg(long, env = "RESTORE_REGISTRY_S3_ACCESS_KEY_ID")]
    target_access_key_id: String,
    #[arg(long, env = "RESTORE_REGISTRY_S3_SECRET_ACCESS_KEY")]
    target_secret_access_key: String,
    #[arg(long, env = "RESTORE_REGISTRY_PATH_PREFIX")]
    target_prefix: String,
    #[arg(long, default_value_t = false)]
    allow_http: bool,

    /// Required acknowledgement: the registry target is offline. It may be
    /// non-empty from previously merged cell points.
    #[arg(long, default_value_t = false)]
    confirm_registry_offline: bool,
}

fn s3_store(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    prefix: &str,
    allow_http: bool,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    anyhow::ensure!(!prefix.is_empty(), "object-store prefix must be non-empty");
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(allow_http)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(allow_http)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()
        .context("build object store")?;
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store, prefix,
    )))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.confirm_registry_offline,
        "--confirm-registry-offline is required"
    );
    anyhow::ensure!(
        !args.target_prefix.starts_with("cells/"),
        "registry restore prefix must be outside cells/"
    );
    let backup = s3_store(
        &args.backup_endpoint,
        &args.backup_bucket,
        &args.backup_region,
        &args.backup_access_key_id,
        &args.backup_secret_access_key,
        &args.backup_prefix,
        args.allow_http,
    )?;
    let target = s3_store(
        &args.target_endpoint,
        &args.target_bucket,
        &args.target_region,
        &args.target_access_key_id,
        &args.target_secret_access_key,
        &args.target_prefix,
        args.allow_http,
    )?;
    let snapshot_id = if args.snapshot_id == "latest" {
        streams_slate::backup::latest_snapshot_id(backup.clone()).await?
    } else {
        args.snapshot_id
    };
    let merged =
        streams_slate::backup::merge_registry_snapshot(backup, &snapshot_id, target).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "snapshot_id": snapshot_id,
            "merged_registry_objects": merged,
        }))?
    );
    Ok(())
}
