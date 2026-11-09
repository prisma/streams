//! The topology's writes (S4-DESIGN §6.2 steps 0.2 and 4.1): every one a CAS on the ETag of the
//! object read, never an unconditional PUT, so a concurrent writer's change is never lost.

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use super::model::Topology;

const TOPOLOGY_PATH: &str = "topology.json";

/// Why a topology write did not land.
#[derive(Debug)]
pub(crate) enum WriteError {
    /// The object changed since it was read: re-read and decide again.
    Moved,
    Store(object_store::Error),
}

/// The stored topology and its ETag.
pub(crate) async fn read(store: &dyn ObjectStore) -> Result<(Topology, String), WriteError> {
    let got = store
        .get(&ObjPath::from(TOPOLOGY_PATH))
        .await
        .map_err(WriteError::Store)?;
    let etag = got.meta.e_tag.clone().unwrap_or_default();
    let raw = got.bytes().await.map_err(WriteError::Store)?;
    let topology = serde_json::from_slice(&raw).map_err(|e| {
        WriteError::Store(object_store::Error::Generic {
            store: "topology",
            source: format!("corrupt topology object: {e}").into(),
        })
    })?;
    Ok((topology, etag))
}

/// Write `next` over the object at `etag`: its new ETag, or `Moved` when the object changed.
pub(crate) async fn replace(
    store: &dyn ObjectStore,
    etag: &str,
    next: &Topology,
) -> Result<String, WriteError> {
    if etag.is_empty() {
        return Err(WriteError::Moved);
    }
    let raw = serde_json::to_vec(next).map_err(|e| {
        WriteError::Store(object_store::Error::Generic {
            store: "topology",
            source: Box::new(e),
        })
    })?;
    let mode = PutMode::Update(UpdateVersion {
        e_tag: Some(etag.to_string()),
        version: None,
    });
    let put = store
        .put_opts(
            &ObjPath::from(TOPOLOGY_PATH),
            PutPayload::from(raw),
            PutOptions::from(mode),
        )
        .await;
    match put {
        Ok(put) => put.e_tag.filter(|e| !e.is_empty()).ok_or(WriteError::Moved),
        Err(object_store::Error::Precondition { .. }) => Err(WriteError::Moved),
        Err(e) => Err(WriteError::Store(e)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::ObjectStore;

    use super::{WriteError, read, replace};

    /// **A write lands only over the object it read.** A cell's topology is read, its homes
    /// reserved and written over its ETag; a second write over the old ETag is `Moved` and
    /// changes nothing; the stored topology is the reserved one.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_topology_write_lands_only_over_the_object_it_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let missing = read(&*store).await;
        assert!(
            matches!(&missing, Err(WriteError::Store(e)) if matches!(e, object_store::Error::NotFound { .. })),
            "{missing:?}"
        );
        let initial = crate::config::validation::InitialShards::new(2)
            .unwrap()
            .topology(1 << 20);
        crate::registry::load_or_init_topology(
            &store,
            crate::config::validation::InitialShards::new(2).unwrap(),
            1 << 20,
        )
        .await
        .unwrap();
        let (stored, etag) = read(&*store).await.unwrap();
        assert_eq!(stored, initial);
        let (reserved, _) = stored.reserve_homes();
        let moved_to = replace(&*store, &etag, &reserved).await.unwrap();
        assert_ne!(moved_to, etag);
        let stale = replace(&*store, &etag, &stored).await;
        assert!(matches!(stale, Err(WriteError::Moved)), "{stale:?}");
        assert!(matches!(
            replace(&*store, "", &stored).await,
            Err(WriteError::Moved)
        ));
        assert_eq!(read(&*store).await.unwrap(), (reserved, moved_to));
    }
}
