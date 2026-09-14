//! Executable provider requirements through the same object_store client as the server.
//! Each run creates a fresh sub-prefix and retains its tiny objects as evidence.

use std::sync::Arc;

use anyhow::{Context, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

pub(super) async fn run(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    concurrency: usize,
) -> anyhow::Result<()> {
    let root = conditional_writes(store.clone(), prefix, concurrency).await?;
    consistency(&store, &root).await?;
    println!(
        "PROVIDER_CONTRACT_OK {}",
        serde_json::json!({
            "prefix": root.to_string(), "concurrency": concurrency,
            "checks": ["single-create-winner", "single-update-winner", "winner-exact-bytes", "stale-update-rejected", "overwrite-read-head-range-list", "delete-get-head-list"]
        })
    );
    Ok(())
}

pub(super) async fn conditional_writes(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    concurrency: usize,
) -> anyhow::Result<Path> {
    ensure!(
        (2..=64).contains(&concurrency),
        "conditional race concurrency must be 2..=64"
    );
    let root = Path::from(format!("{prefix}/contract-{:032x}", rand::random::<u128>()));
    let created = conditional_race(
        &store,
        &root.clone().join("create"),
        concurrency,
        PutMode::Create,
    )
    .await?;
    println!(
        "conditional create: {} winner bytes verified",
        created.len()
    );
    let key = root.clone().join("update");
    let original = store
        .put_opts(
            &key,
            PutPayload::from_static(b"original"),
            PutMode::Create.into(),
        )
        .await?;
    let version = UpdateVersion {
        e_tag: original.e_tag,
        version: original.version,
    };
    ensure!(
        version.e_tag.is_some() || version.version.is_some(),
        "provider returned no conditional update identity"
    );
    let updated =
        conditional_race(&store, &key, concurrency, PutMode::Update(version.clone())).await?;
    let stale = store
        .put_opts(
            &key,
            PutPayload::from_static(b"stale"),
            PutMode::Update(version).into(),
        )
        .await;
    ensure!(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "stale update did not fail with Precondition: {stale:?}"
    );
    ensure!(
        store.get(&key).await?.bytes().await? == updated,
        "rejected stale update changed winner bytes"
    );
    Ok(root)
}

async fn conditional_race(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    concurrency: usize,
    mode: PutMode,
) -> anyhow::Result<Bytes> {
    let updates = matches!(mode, PutMode::Update(_));
    let results: Vec<_> = stream::iter(0..concurrency)
        .map(|index| {
            let options = PutOptions::from(mode.clone());
            async move {
                let bytes = Bytes::from(format!("candidate-{index}"));
                let result = store
                    .put_opts(key, PutPayload::from_bytes(bytes.clone()), options)
                    .await;
                (bytes, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let mut winner = None;
    for (bytes, result) in results {
        match result {
            Ok(_) => ensure!(
                winner.replace(bytes).is_none(),
                "conditional race accepted multiple winners"
            ),
            Err(object_store::Error::Precondition { .. }) if updates => (),
            Err(object_store::Error::AlreadyExists { .. }) if !updates => (),
            Err(error) => {
                return Err(error).context("conditional race returned an unexpected loser outcome");
            }
        }
    }
    let expected = winner.context("conditional race accepted no winner")?;
    ensure!(
        store.get(key).await?.bytes().await? == expected,
        "conditional race persisted bytes other than its winner"
    );
    Ok(expected)
}

async fn consistency(store: &Arc<dyn ObjectStore>, root: &Path) -> anyhow::Result<()> {
    let key = root.clone().join("visibility");
    for generation in 0..4 {
        let value = Bytes::from(format!(
            "generation-{generation}-{}",
            "exact-payload".repeat(generation + 1)
        ));
        let mode = if generation == 0 {
            PutMode::Create
        } else {
            PutMode::Overwrite
        };
        store
            .put_opts(&key, PutPayload::from_bytes(value.clone()), mode.into())
            .await?;
        ensure!(
            store.get(&key).await?.bytes().await? == value,
            "GET returned stale or corrupted bytes after PUT"
        );
        let head = store.head(&key).await?;
        ensure!(
            head.size == u64::try_from(value.len())?,
            "HEAD returned stale object size"
        );
        ensure!(
            store.get_range(&key, 0..12).await? == value.slice(0..12),
            "range GET returned different bytes"
        );
        let listed: Vec<_> = store.list(Some(root)).try_collect().await?;
        let matching: Vec<_> = listed.iter().filter(|item| item.location == key).collect();
        ensure!(
            matching.len() == 1,
            "LIST omitted or duplicated acknowledged object"
        );
        ensure!(
            matching[0].size == head.size,
            "LIST returned stale object size"
        );
    }
    store.delete(&key).await?;
    ensure!(
        matches!(
            store.get(&key).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "GET found successfully deleted object"
    );
    ensure!(
        matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "HEAD found successfully deleted object"
    );
    let listed: Vec<_> = store.list(Some(root)).try_collect().await?;
    ensure!(
        listed.iter().all(|item| item.location != key),
        "LIST retained successfully deleted object"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run;
    use std::sync::Arc;

    #[tokio::test]
    async fn provider_contract_accepts_real_memory_store() {
        run(
            Arc::new(object_store::memory::InMemory::new()),
            "fixture",
            8,
        )
        .await
        .unwrap();
    }
}
