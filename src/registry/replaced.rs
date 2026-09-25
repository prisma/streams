//! Closure debts of replaced incarnations (second external review of
//! 9813d1cb; a billing release blocker). Recreating a name over a dead
//! incarnation replaced its descriptor, and with it the last durable record
//! that its storage gauges still had to close: the tombstone walk closes only
//! the terminal descriptors it can still find, so an idle-expired
//! incarnation replaced before the walk reached it kept its gauge, and every
//! month close carried that storage on.
//!
//! `recreate` therefore records the incarnation it is about to replace, in
//! its own object, BEFORE the conditional write that replaces it: the
//! obligation is durable before the only record of it can disappear. A
//! recreation that then loses its race wrote a debt for an incarnation that
//! may still be stored; the settlement pass judges each debt against the
//! stored descriptor, never the debt alone, and acts only once the name
//! holds another incarnation (or none). Debts of one incarnation keep the
//! latest persisted close instant: its expiry only moves forward and a
//! deletion's stamp comes after it, so a slower, staler writer never moves
//! it back.
use super::{PersistedDescriptor, Registry, StreamDesc};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

/// The root every closure debt lives under, apart from the descriptors the
/// catalog scans (an unknown object under the projects root fails a scan).
const REPLACED_ROOT: &str = "registry/v4/replaced/";

/// One replaced incarnation's closure debt: its descriptor as the
/// recreation found it, the persisted instant its storage closes at, and
/// the segments whose owners already found nothing left open.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplacedIncarnation {
    pub descriptor: PersistedDescriptor,
    pub close_ms: i64,
    #[serde(default)]
    pub settled: Vec<u32>,
}

impl ReplacedIncarnation {
    /// The replaced incarnation, validated as the registry validates any
    /// descriptor.
    pub(crate) fn incarnation(&self) -> Result<StreamDesc, object_store::Error> {
        StreamDesc::try_from(self.descriptor.clone())
    }

    /// Every segment the incarnation had.
    pub(crate) fn segments(&self) -> Result<Vec<u32>, object_store::Error> {
        let desc = self.incarnation()?;
        Ok(desc.segments.as_ref().map_or_else(
            || vec![0],
            |m| m.segments.iter().map(|s| s.seg_id).collect(),
        ))
    }
}

/// A debt as the settlement pass reads it, with the key it lives under.
pub(crate) struct DebtEntry {
    pub key: String,
    pub debt: ReplacedIncarnation,
}

/// The persisted instant a dead incarnation's storage closes at: its
/// deletion stamp, else its expiry. A replaceable incarnation is deleted or
/// expired, so it always has one.
fn close_instant(desc: &StreamDesc) -> Option<i64> {
    desc.logical_close_ms.or(desc.expires_at_ms)
}

fn debt_path(desc: &StreamDesc) -> ObjPath {
    let sref = desc.sref();
    ObjPath::from(format!(
        "{REPLACED_ROOT}{}/{}/{}.json",
        crate::crypto::hex(sref.project_id().as_bytes()),
        crate::crypto::hex(sref.name().as_str().as_bytes()),
        crate::crypto::hex(desc.stream_epoch.as_bytes()),
    ))
}

fn debt_error(reason: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "registry",
        source: reason.into(),
    }
}

impl Registry {
    /// A debt and the version it was read at, or `None` when there is none.
    async fn read_debt(
        &self,
        path: &ObjPath,
    ) -> Result<Option<(UpdateVersion, ReplacedIncarnation)>, object_store::Error> {
        let got = match self.store.get(path).await {
            Ok(got) => got,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let version = UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let debt = serde_json::from_slice(&got.bytes().await?)
            .map_err(|_| debt_error("undecodable closure debt"))?;
        Ok(Some((version, debt)))
    }

    /// Writes `debt` under `mode`; `false` when another writer won the race.
    async fn write_debt(
        &self,
        path: &ObjPath,
        debt: &ReplacedIncarnation,
        mode: PutMode,
    ) -> Result<bool, object_store::Error> {
        let body = serde_json::to_vec(debt).map_err(|_| debt_error("closure debt encoding"))?;
        let put = self
            .store
            .put_opts(path, PutPayload::from(body), PutOptions::from(mode))
            .await;
        match put {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Durably records that `dead` is about to be replaced, before the write
    /// that replaces it. Idempotent; keeps the later close instant.
    pub(crate) async fn record_replaced(
        &self,
        dead: &StreamDesc,
    ) -> Result<(), object_store::Error> {
        let Some(close_ms) = close_instant(dead) else {
            return Ok(());
        };
        let path = debt_path(dead);
        for _ in 0..5 {
            let (mode, settled) = match self.read_debt(&path).await? {
                Some((_, known)) if known.close_ms >= close_ms => return Ok(()),
                Some((version, known)) => (PutMode::Update(version), known.settled),
                None => (PutMode::Create, Vec::new()),
            };
            let debt = ReplacedIncarnation {
                descriptor: dead.to_persisted(),
                close_ms,
                settled,
            };
            if self.write_debt(&path, &debt, mode).await? {
                return Ok(());
            }
        }
        Err(debt_error("closure debt retries exhausted"))
    }

    /// Up to `limit` closure debts of the cell after key `after` (from the
    /// start when `None`), in key order. Only that many keys are listed.
    pub(crate) async fn replaced_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DebtEntry>, object_store::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        let prefix = ObjPath::from(REPLACED_ROOT.trim_end_matches('/'));
        let listing = match after {
            Some(key) => self
                .store
                .list_with_offset(Some(&prefix), &ObjPath::from(key)),
            None => self.store.list(Some(&prefix)),
        };
        let mut keys: Vec<ObjPath> = listing
            .map_ok(|meta| meta.location)
            .take(limit)
            .try_collect()
            .await?;
        keys.sort();
        let mut page = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, debt)) = self.read_debt(&key).await? {
                page.push(DebtEntry {
                    key: key.to_string(),
                    debt,
                });
            }
        }
        Ok(page)
    }

    /// Records that segment `seg_id`'s owner found nothing of the debt left
    /// open, and removes the debt once every segment has. Returns whether
    /// the debt is gone.
    pub(crate) async fn settle_replaced(
        &self,
        key: &str,
        seg_id: u32,
    ) -> Result<bool, object_store::Error> {
        let path = ObjPath::from(key);
        for _ in 0..5 {
            let Some((version, mut debt)) = self.read_debt(&path).await? else {
                return Ok(true);
            };
            if !debt.settled.contains(&seg_id) {
                debt.settled.push(seg_id);
            }
            if debt.segments()?.iter().all(|s| debt.settled.contains(s)) {
                self.store.delete(&path).await?;
                return Ok(true);
            }
            if self
                .write_debt(&path, &debt, PutMode::Update(version))
                .await?
            {
                return Ok(false);
            }
        }
        Err(debt_error("closure debt retries exhausted"))
    }

    /// Removes a debt the stored descriptor proves spurious: the recreation
    /// that wrote it lost to a renewal, so nothing was replaced.
    pub(crate) async fn drop_replaced(&self, key: &str) -> Result<(), object_store::Error> {
        match self.store.delete(&ObjPath::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }
}
