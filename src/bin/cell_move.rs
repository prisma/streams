//! Fenced, restartable movement of one stream between managed cells.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use streams_slate::cell_move::{CellStores, cleanup_stream_source, move_stream};
use streams_slate::registry::Registry;

#[derive(Debug, Parser)]
#[command(name = "streams-cell-move")]
struct Args {
    #[arg(long)]
    customer_id: String,
    #[arg(long)]
    stream: String,
    #[arg(long)]
    source_cell: String,
    #[arg(long)]
    target_cell: String,
    /// Stable retry identity. Generated and printed when omitted; retain it
    /// if the command exits before reporting completion.
    #[arg(long)]
    operation_id: Option<String>,
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    max_object_bytes: u64,
    #[arg(long, default_value_t = false)]
    allow_http: bool,
    /// Acknowledges that the target stream incarnation is non-authoritative
    /// and may be replaced while the descriptor remains pinned to source.
    #[arg(long, default_value_t = false)]
    confirm_target_stream_replaceable: bool,
    /// Reclaim the obsolete source copy of an already completed move. This is
    /// a separate retention phase; it never initiates or repeats a move.
    #[arg(long, default_value_t = false)]
    cleanup_source: bool,
    #[arg(long, default_value_t = 7 * 24 * 60 * 60)]
    min_source_retention_secs: u64,
    /// Hermetic-test-only escape hatch for the provider-clock retention wait.
    #[arg(long, default_value_t = false)]
    allow_zero_retention: bool,
    #[arg(long, default_value_t = false)]
    confirm_source_cleanup: bool,

    #[arg(long, env = "REGISTRY_S3_ENDPOINT")]
    registry_endpoint: String,
    #[arg(long, env = "REGISTRY_S3_BUCKET")]
    registry_bucket: String,
    #[arg(long, env = "REGISTRY_S3_REGION", default_value = "us-east-1")]
    registry_region: String,
    #[arg(long, env = "REGISTRY_S3_ACCESS_KEY_ID")]
    registry_access_key_id: String,
    #[arg(long, env = "REGISTRY_S3_SECRET_ACCESS_KEY")]
    registry_secret_access_key: String,
    #[arg(long, env = "REGISTRY_PATH_PREFIX")]
    registry_prefix: String,

    #[arg(long, env = "SOURCE_S3_ENDPOINT")]
    source_endpoint: String,
    #[arg(long, env = "SOURCE_S3_BUCKET")]
    source_bucket: String,
    #[arg(long, env = "SOURCE_OPS_S3_BUCKET")]
    source_ops_bucket: Option<String>,
    #[arg(long, env = "SOURCE_SHARD_S3_BUCKET")]
    source_shard_bucket: Option<String>,
    #[arg(long, env = "SOURCE_DATA_S3_BUCKET")]
    source_data_bucket: Option<String>,
    #[arg(long, env = "SOURCE_S3_REGION", default_value = "us-east-1")]
    source_region: String,
    #[arg(long, env = "SOURCE_S3_ACCESS_KEY_ID")]
    source_access_key_id: String,
    #[arg(long, env = "SOURCE_S3_SECRET_ACCESS_KEY")]
    source_secret_access_key: String,
    #[arg(long, env = "SOURCE_PATH_PREFIX")]
    source_prefix: String,
    /// Full fleet coordination prefix for the source cell. The mover requires
    /// a fresh aggregate proving every live member supports this protocol.
    #[arg(long, env = "SOURCE_FLEET_PREFIX")]
    source_fleet_prefix: String,

    #[arg(long, env = "TARGET_S3_ENDPOINT")]
    target_endpoint: String,
    #[arg(long, env = "TARGET_S3_BUCKET")]
    target_bucket: String,
    #[arg(long, env = "TARGET_OPS_S3_BUCKET")]
    target_ops_bucket: Option<String>,
    #[arg(long, env = "TARGET_SHARD_S3_BUCKET")]
    target_shard_bucket: Option<String>,
    #[arg(long, env = "TARGET_DATA_S3_BUCKET")]
    target_data_bucket: Option<String>,
    #[arg(long, env = "TARGET_S3_REGION", default_value = "us-east-1")]
    target_region: String,
    #[arg(long, env = "TARGET_S3_ACCESS_KEY_ID")]
    target_access_key_id: String,
    #[arg(long, env = "TARGET_S3_SECRET_ACCESS_KEY")]
    target_secret_access_key: String,
    #[arg(long, env = "TARGET_PATH_PREFIX")]
    target_prefix: String,
    /// Full fleet coordination prefix for the target cell.
    #[arg(long, env = "TARGET_FLEET_PREFIX")]
    target_fleet_prefix: String,

    #[arg(long, env = "TARGET_BACKUP_S3_ENDPOINT")]
    target_backup_endpoint: Option<String>,
    #[arg(long, env = "TARGET_BACKUP_S3_BUCKET")]
    target_backup_bucket: Option<String>,
    #[arg(long, env = "TARGET_BACKUP_S3_REGION", default_value = "us-east-1")]
    target_backup_region: String,
    #[arg(long, env = "TARGET_BACKUP_S3_ACCESS_KEY_ID")]
    target_backup_access_key_id: Option<String>,
    #[arg(long, env = "TARGET_BACKUP_S3_SECRET_ACCESS_KEY")]
    target_backup_secret_access_key: Option<String>,
    #[arg(long, env = "TARGET_BACKUP_PATH_PREFIX")]
    target_backup_prefix: Option<String>,
}

struct StoreAuth<'a> {
    endpoint: &'a str,
    region: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    prefix: &'a str,
    allow_http: bool,
}

fn store(auth: &StoreAuth<'_>, bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    store_at_prefix(auth, bucket, auth.prefix)
}

fn store_at_prefix(
    auth: &StoreAuth<'_>,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let raw: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(auth.endpoint)
            .with_bucket_name(bucket)
            .with_region(auth.region)
            .with_access_key_id(auth.access_key)
            .with_secret_access_key(auth.secret_key)
            .with_allow_http(auth.allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .context("build cell move object store")?,
    );
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        raw, prefix,
    )))
}

fn cell_stores(
    auth: &StoreAuth<'_>,
    default_bucket: &str,
    ops_bucket: Option<&str>,
    shard_bucket: Option<&str>,
    data_bucket: Option<&str>,
    fleet_prefix: &str,
) -> anyhow::Result<CellStores> {
    Ok(CellStores {
        ops: store(auth, ops_bucket.unwrap_or(default_bucket))?,
        shard: store(auth, shard_bucket.unwrap_or(default_bucket))?,
        data: store(auth, data_bucket.unwrap_or(default_bucket))?,
        fleet: store_at_prefix(auth, default_bucket, fleet_prefix)?,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.cleanup_source || args.confirm_target_stream_replaceable,
        "--confirm-target-stream-replaceable is required for a move"
    );
    anyhow::ensure!(
        args.source_prefix == streams_slate::cells::cell_prefix(&args.source_cell)
            && args.target_prefix == streams_slate::cells::cell_prefix(&args.target_cell)
            && args.source_prefix != args.target_prefix,
        "source/target prefixes must be their distinct canonical cells/<id> prefixes"
    );
    anyhow::ensure!(
        args.source_fleet_prefix
            .strip_prefix(&args.source_prefix)
            .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
            && args
                .target_fleet_prefix
                .strip_prefix(&args.target_prefix)
                .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1),
        "source/target fleet prefixes must be non-empty descendants of their cell prefixes"
    );
    anyhow::ensure!(
        !args.registry_prefix.is_empty() && !args.registry_prefix.starts_with("cells/"),
        "global registry prefix must be non-empty and outside cells/"
    );
    let registry_auth = StoreAuth {
        endpoint: &args.registry_endpoint,
        region: &args.registry_region,
        access_key: &args.registry_access_key_id,
        secret_key: &args.registry_secret_access_key,
        prefix: &args.registry_prefix,
        allow_http: args.allow_http,
    };
    let source_auth = StoreAuth {
        endpoint: &args.source_endpoint,
        region: &args.source_region,
        access_key: &args.source_access_key_id,
        secret_key: &args.source_secret_access_key,
        prefix: &args.source_prefix,
        allow_http: args.allow_http,
    };
    let target_auth = StoreAuth {
        endpoint: &args.target_endpoint,
        region: &args.target_region,
        access_key: &args.target_access_key_id,
        secret_key: &args.target_secret_access_key,
        prefix: &args.target_prefix,
        allow_http: args.allow_http,
    };
    let registry = Registry::new(store(&registry_auth, &args.registry_bucket)?);
    let source = cell_stores(
        &source_auth,
        &args.source_bucket,
        args.source_ops_bucket.as_deref(),
        args.source_shard_bucket.as_deref(),
        args.source_data_bucket.as_deref(),
        &args.source_fleet_prefix,
    )?;
    let target = cell_stores(
        &target_auth,
        &args.target_bucket,
        args.target_ops_bucket.as_deref(),
        args.target_shard_bucket.as_deref(),
        args.target_data_bucket.as_deref(),
        &args.target_fleet_prefix,
    )?;
    let operation_id = match args.operation_id {
        Some(operation_id) => operation_id,
        None if args.cleanup_source => {
            anyhow::bail!("--operation-id is required for source cleanup")
        }
        None => uuid::Uuid::new_v4().simple().to_string(),
    };
    if args.cleanup_source {
        anyhow::ensure!(
            args.confirm_source_cleanup,
            "--confirm-source-cleanup is required"
        );
        anyhow::ensure!(
            (args.allow_zero_retention && args.min_source_retention_secs == 0)
                || (60 * 60..=365 * 24 * 60 * 60).contains(&args.min_source_retention_secs),
            "source retention must be 1 hour to 365 days (zero is test-only)"
        );
        let backup_endpoint = args
            .target_backup_endpoint
            .as_deref()
            .context("--target-backup-endpoint is required for source cleanup")?;
        let backup_bucket = args
            .target_backup_bucket
            .as_deref()
            .context("--target-backup-bucket is required for source cleanup")?;
        let backup_access_key = args
            .target_backup_access_key_id
            .as_deref()
            .context("--target-backup-access-key-id is required for source cleanup")?;
        let backup_secret_key = args
            .target_backup_secret_access_key
            .as_deref()
            .context("--target-backup-secret-access-key is required for source cleanup")?;
        let backup_prefix = args
            .target_backup_prefix
            .as_deref()
            .context("--target-backup-prefix is required for source cleanup")?;
        anyhow::ensure!(
            !backup_prefix.is_empty(),
            "target backup prefix must be non-empty"
        );
        let backup_auth = StoreAuth {
            endpoint: backup_endpoint,
            region: &args.target_backup_region,
            access_key: backup_access_key,
            secret_key: backup_secret_key,
            prefix: backup_prefix,
            allow_http: args.allow_http,
        };
        let report = cleanup_stream_source(
            &registry,
            &args.customer_id,
            &args.stream,
            &args.source_cell,
            &args.target_cell,
            &operation_id,
            &source,
            &target,
            store(&backup_auth, backup_bucket)?,
            Duration::from_secs(args.min_source_retention_secs),
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        let report = move_stream(
            &registry,
            &args.customer_id,
            &args.stream,
            &args.source_cell,
            &args.target_cell,
            &operation_id,
            &source,
            &target,
            args.max_object_bytes,
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    Ok(())
}
