//! The fleet coordination repository (PR 6.1-D, completed by 6.1.1-C):
//! ONE per-runtime owner of the cell's coordination documents — this
//! instance's heartbeat, the live heartbeat set, router reports, the
//! desired-count and overrides documents with their CAS versions, the
//! published URL map, and the event outboxes the telemetry drainer
//! clears. The fleet loop, the drainer and the operator surface all go
//! through it, so a runtime cannot end up reading one store while
//! writing another: the store handle itself never leaves this module.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path as ObjPath,
};

use super::{Desired, Heartbeat, Overrides};

/// Coordination documents this instance may address. Naming them here
/// keeps the layout in one place instead of spelling paths at every
/// call site.
const DESIRED_DOC: &str = "fleet/desired.json";
const OVERRIDES_DOC: &str = "fleet/overrides.json";
const URLS_DOC: &str = "fleet/urls.json";

#[derive(Clone, Default)]
pub struct FleetRepository {
    store: Option<Arc<dyn ObjectStore>>,
}

impl FleetRepository {
    pub fn new(store: Option<Arc<dyn ObjectStore>>) -> Self {
        Self { store }
    }

    /// Whether this runtime participates in fleet coordination.
    pub fn enabled(&self) -> bool {
        self.store.is_some()
    }

    // -- this instance's own heartbeat ---------------------------------

    pub async fn publish_heartbeat(&self, instance: &str, hb: &Heartbeat) -> anyhow::Result<()> {
        let Some(store) = self.store.as_ref() else {
            return Ok(());
        };
        let path = ObjPath::from(format!("fleet/{instance}.json"));
        store
            .put(&path, PutPayload::from(serde_json::to_vec(hb)?))
            .await?;
        Ok(())
    }

    /// Complete bounded populations. An incomplete/invalid snapshot is
    /// an error, so the controller retains its prior ownership view.
    pub async fn read_heartbeat_set(&self) -> anyhow::Result<Vec<Heartbeat>> {
        self.read_population("fleet", true).await
    }

    pub async fn read_router_reports(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        self.read_population("routers", false).await
    }

    async fn read_population<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
        heartbeat: bool,
    ) -> anyhow::Result<Vec<T>> {
        const MAX_OBJECTS: usize = 4096;
        const MAX_OBJECT_BYTES: usize = 128 * 1024;
        const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
        let Some(store) = self.store.as_ref() else {
            return Ok(Vec::new());
        };
        let pass = async {
            let mut paths = Vec::new();
            let mut listed = 0usize;
            let prefix = ObjPath::from(prefix);
            let mut listing = store.list(Some(&prefix));
            while let Some(meta) = listing.try_next().await? {
                listed += 1;
                anyhow::ensure!(
                    listed <= MAX_OBJECTS,
                    "fleet population exceeds {MAX_OBJECTS} objects"
                );
                let loc = meta.location.as_ref();
                if heartbeat
                    && (!loc.ends_with(".json")
                        || ["desired.json", "overrides.json", "urls.json"]
                            .iter()
                            .any(|n| loc.ends_with(n)))
                {
                    continue;
                }
                anyhow::ensure!(
                    meta.size <= MAX_OBJECT_BYTES as u64,
                    "fleet document too large: {loc}"
                );
                paths.push(meta.location);
            }
            paths.sort();
            let mut reads = futures_util::stream::iter(paths)
                .map(|path| async move {
                    let result = store.get(&path).await?;
                    let mut chunks = result.into_stream();
                    let mut raw = Vec::new();
                    while let Some(chunk) = chunks.try_next().await? {
                        anyhow::ensure!(
                            raw.len().saturating_add(chunk.len()) <= MAX_OBJECT_BYTES,
                            "fleet document exceeds byte budget: {path}"
                        );
                        raw.extend_from_slice(&chunk);
                    }
                    Ok::<_, anyhow::Error>(raw)
                })
                .buffered(8);
            let mut bytes = 0usize;
            let mut out = Vec::new();
            while let Some(raw) = reads.try_next().await? {
                bytes += raw.len();
                anyhow::ensure!(
                    bytes <= MAX_TOTAL_BYTES,
                    "fleet population exceeds byte budget"
                );
                out.push(serde_json::from_slice(&raw)?);
            }
            Ok(out)
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), pass)
            .await
            .map_err(|_| anyhow::anyhow!("fleet population read timed out"))?
    }

    // -- coordination documents ----------------------------------------

    /// The desired-count document and the version a CAS must present.
    pub async fn read_desired_state(&self) -> (Option<Desired>, Option<UpdateVersion>) {
        match self.read_typed::<Desired>(DESIRED_DOC).await {
            Some((doc, version)) => (doc, Some(version)),
            None => (None, None),
        }
    }

    /// The overrides document (absent reads as empty) and its version.
    pub async fn read_overrides(&self) -> (Overrides, Option<UpdateVersion>) {
        match self.read_typed::<Overrides>(OVERRIDES_DOC).await {
            Some((doc, version)) => (doc.unwrap_or_default(), Some(version)),
            None => (Overrides::default(), None),
        }
    }

    /// The platform-published instance -> base URL map, exactly as
    /// stored; the caller applies its own trust policy to each URL.
    pub async fn read_published_urls(&self) -> Option<std::collections::HashMap<String, String>> {
        let (doc, _) = self
            .read_typed::<std::collections::HashMap<String, String>>(URLS_DOC)
            .await?;
        doc
    }

    /// CAS-replace a coordination document: `Some(version)` updates that
    /// exact version, `None` creates. Returns whether it committed.
    pub async fn replace_document(
        &self,
        doc: FleetDocument,
        body: Vec<u8>,
        version: Option<UpdateVersion>,
    ) -> bool {
        let Some(store) = self.store.as_ref() else {
            return false;
        };
        let mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        store
            .put_opts(
                &ObjPath::from(doc.path()),
                PutPayload::from(body),
                PutOptions::from(mode),
            )
            .await
            .is_ok()
    }

    /// One document's bytes with the version a CAS clear needs — the
    /// event drainer's read.
    pub(crate) async fn read_doc(&self, doc: FleetDocument) -> Option<(Bytes, UpdateVersion)> {
        let store = self.store.as_ref()?;
        let got = store.get(&ObjPath::from(doc.path())).await.ok()?;
        let version = UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let raw = got.bytes().await.ok()?;
        Some((raw, version))
    }

    /// What the operator surface shows about the cell: the live
    /// heartbeat set and the desired-count document.
    pub async fn operator_snapshot(
        &self,
    ) -> (Option<Vec<serde_json::Value>>, Option<serde_json::Value>) {
        if !self.enabled() {
            return (None, None);
        }
        let heartbeats = self.read_heartbeat_set().await.ok().map(|items| {
            items
                .into_iter()
                .filter_map(|hb| serde_json::to_value(hb).ok())
                .collect()
        });
        let desired = self
            .read_typed::<serde_json::Value>(DESIRED_DOC)
            .await
            .and_then(|(doc, _)| doc);
        (heartbeats, desired)
    }

    async fn read_typed<T: serde::de::DeserializeOwned>(
        &self,
        doc: &str,
    ) -> Option<(Option<T>, UpdateVersion)> {
        let store = self.store.as_ref()?;
        match store.get(&ObjPath::from(doc)).await {
            Ok(r) => {
                let version = UpdateVersion {
                    e_tag: r.meta.e_tag.clone(),
                    version: r.meta.version.clone(),
                };
                let raw = r.bytes().await.unwrap_or_default();
                Some((serde_json::from_slice::<T>(&raw).ok(), version))
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => {
                tracing::warn!(document = doc, "fleet document read failed: {e}");
                None
            }
        }
    }
}

/// The coordination documents a runtime may write. A closed set, so a
/// caller cannot invent a path the repository does not own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetDocument {
    Desired,
    Overrides,
}

impl FleetDocument {
    pub(crate) fn path(self) -> &'static str {
        match self {
            FleetDocument::Desired => DESIRED_DOC,
            FleetDocument::Overrides => OVERRIDES_DOC,
        }
    }
}

#[cfg(test)]
mod population_tests {
    use super::*;

    #[tokio::test]
    async fn population_reads_are_ordered_and_corruption_is_not_empty_membership() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let repository = FleetRepository::new(Some(store.clone()));
        for n in [3, 1, 2] {
            store
                .put(
                    &ObjPath::from(format!("routers/{n}.json")),
                    PutPayload::from(format!("{{\"n\":{n}}}")),
                )
                .await
                .unwrap();
        }
        let rows = repository.read_router_reports().await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r["n"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        store
            .put(
                &ObjPath::from("routers/2.json"),
                PutPayload::from("invalid".to_string()),
            )
            .await
            .unwrap();
        assert!(repository.read_router_reports().await.is_err());
    }

    #[tokio::test]
    async fn population_object_and_byte_budgets_fail_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let repository = FleetRepository::new(Some(store.clone()));
        store
            .put(
                &ObjPath::from("routers/large"),
                PutPayload::from(vec![b' '; 128 * 1024 + 1]),
            )
            .await
            .unwrap();
        assert!(
            repository
                .read_router_reports()
                .await
                .unwrap_err()
                .to_string()
                .contains("large")
        );
        store.delete(&ObjPath::from("routers/large")).await.unwrap();
        for n in 0..4097 {
            store
                .put(
                    &ObjPath::from(format!("routers/{n:04}")),
                    PutPayload::from("{}".to_string()),
                )
                .await
                .unwrap();
        }
        assert!(
            repository
                .read_router_reports()
                .await
                .unwrap_err()
                .to_string()
                .contains("4096")
        );
    }
}
