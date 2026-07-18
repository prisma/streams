use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

#[derive(Debug, Parser)]
#[command(
    name = "streams-restore",
    about = "Restore a complete Prisma Streams recovery snapshot into empty offline targets"
)]
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
    backup_prefix: Option<String>,

    #[arg(long, env = "RESTORE_S3_ENDPOINT")]
    target_endpoint: String,
    #[arg(long, env = "RESTORE_S3_BUCKET")]
    target_bucket: String,
    #[arg(long, env = "RESTORE_OPS_BUCKET")]
    target_ops_bucket: Option<String>,
    #[arg(long, env = "RESTORE_SHARD_BUCKET")]
    target_shard_bucket: Option<String>,
    #[arg(long, env = "RESTORE_DATA_BUCKET")]
    target_data_bucket: Option<String>,
    #[arg(long, env = "RESTORE_S3_REGION", default_value = "us-east-1")]
    target_region: String,
    #[arg(long, env = "RESTORE_S3_ACCESS_KEY_ID")]
    target_access_key_id: String,
    #[arg(long, env = "RESTORE_S3_SECRET_ACCESS_KEY")]
    target_secret_access_key: String,
    #[arg(long, env = "RESTORE_PATH_PREFIX")]
    target_prefix: Option<String>,

    /// Required acknowledgement: targets are offline and disposable if this
    /// command fails. Restore itself independently verifies that each is empty.
    #[arg(long, default_value_t = false)]
    confirm_offline_empty_targets: bool,
}

fn s3_store(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    prefix: Option<&str>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()
        .context("build object store")?;
    Ok(match prefix {
        Some(prefix) => {
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix)) as Arc<dyn ObjectStore>
        }
        None => Arc::new(store),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.confirm_offline_empty_targets,
        "pass --confirm-offline-empty-targets after taking the target service offline"
    );
    let backup = s3_store(
        &args.backup_endpoint,
        &args.backup_bucket,
        &args.backup_region,
        &args.backup_access_key_id,
        &args.backup_secret_access_key,
        args.backup_prefix.as_deref(),
    )?;
    let roles = [
        (
            "ops",
            args.target_ops_bucket
                .as_deref()
                .unwrap_or(&args.target_bucket),
        ),
        (
            "shard",
            args.target_shard_bucket
                .as_deref()
                .unwrap_or(&args.target_bucket),
        ),
        (
            "data",
            args.target_data_bucket
                .as_deref()
                .unwrap_or(&args.target_bucket),
        ),
    ];
    let mut by_bucket = HashMap::new();
    let mut targets = HashMap::new();
    for (role, bucket) in roles {
        let store = match by_bucket.get(bucket) {
            Some(store) => Arc::clone(store),
            None => {
                let store = s3_store(
                    &args.target_endpoint,
                    bucket,
                    &args.target_region,
                    &args.target_access_key_id,
                    &args.target_secret_access_key,
                    args.target_prefix.as_deref(),
                )?;
                by_bucket.insert(bucket.to_string(), store.clone());
                store
            }
        };
        targets.insert(role.to_string(), store);
    }

    let snapshot_id = if args.snapshot_id == "latest" {
        streams_slate::backup::latest_snapshot_id(backup.clone()).await?
    } else {
        args.snapshot_id
    };
    let restored = streams_slate::backup::restore_snapshot(backup, &snapshot_id, &targets).await?;
    println!(
        "{}",
        serde_json::json!({"snapshot_id": snapshot_id, "restored_objects": restored})
    );
    Ok(())
}
