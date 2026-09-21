//! Catalog pages: one bounded, budgeted scan of descriptor objects, read
//! through the same decoder the registry serves from. The project catalog
//! (§10.3) scans one project's prefix; the reconciliation page scans every
//! project in the cell. Both bind each descriptor to the identity its OWN
//! object key names.
use super::*;

/// The last listing entry a page consumed: its object key and the identity
/// that key names. Each entry point continues from the half it owns.
type Consumed = (ObjPath, crate::tenant::TenantStreamRef);

/// The identity a descriptor key names, for exactly the keys `desc_path`
/// mints. Anything else under the projects root (a foreign object,
/// non-canonical hex, an id or name the validated constructors refuse) is
/// `None`, and the scan fails closed on it rather than skip a descriptor
/// nobody could then close.
fn sref_of_key(cell: &str, key: &ObjPath) -> Option<crate::tenant::TenantStreamRef> {
    let text = key.as_ref();
    if !text.is_ascii() {
        // `unhex` slices by byte index.
        return None;
    }
    let (project_hex, name_hex) = text
        .strip_prefix(PROJECTS_ROOT)?
        .strip_suffix(".json")?
        .split_once("/streams/")?;
    let project = String::from_utf8(crate::crypto::unhex(project_hex)?).ok()?;
    let name = String::from_utf8(crate::crypto::unhex(name_hex)?).ok()?;
    let sref = crate::tenant::TenantStreamRef::new(
        crate::tenant::ProjectId::new(&project).ok()?,
        crate::tenant::CanonicalStreamName::new(&name).ok()?,
    );
    (desc_path(cell, &sref) == *key).then_some(sref)
}

impl Registry {
    /// §10.3: one project's ACTIVE descriptors, continued by stream name.
    pub(super) async fn project_page(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage, object_store::Error> {
        let root = project_streams_prefix(project);
        let offset = after.map(|n| ObjPath::from(format!("{root}{}.json", hex(n.as_bytes()))));
        let (streams, last, exhausted) = self
            .catalog_page(&root, offset.as_ref(), limit, false)
            .await?;
        Ok(CatalogPage {
            streams,
            next_after: last.map(|(_, sref)| sref.name().as_str().to_owned()),
            exhausted,
        })
    }

    /// Terminal-closure reconciliation: EVERY descriptor of EVERY project in
    /// this cell — tombstoned, expired and half-built included — continued by
    /// the last consumed object key. Billing closes are owed per segment, not
    /// per tenant, so this page takes no project and cannot be rooted at one.
    /// Never a customer surface: §10.3 isolation is `project_page`'s prefix.
    pub(crate) async fn reconciliation_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage, object_store::Error> {
        let offset = after.map(ObjPath::from);
        let (streams, last, exhausted) = self
            .catalog_page(PROJECTS_ROOT, offset.as_ref(), limit, true)
            .await?;
        Ok(CatalogPage {
            streams,
            next_after: last.map(|(key, _)| key.to_string()),
            exhausted,
        })
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "Registry::catalog_page; the page walk nests the byte, size and decode guards inside each fetched descriptor's read; flattening them would separate the guards from the descriptor they bound"
    )]
    async fn catalog_page(
        &self,
        root: &str,
        offset: Option<&ObjPath>,
        limit: usize,
        include_inactive: bool,
    ) -> Result<(Vec<StreamDesc>, Option<Consumed>, bool), object_store::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        if limit == 0 {
            return Err(catalog_error("catalog limit must be positive"));
        }
        let limit = limit.min(1000);
        let max_scan = limit.saturating_mul(8) + 64;
        const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
        const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
        let prefix = ObjPath::from(root.trim_end_matches('/'));
        let listing = match offset {
            Some(o) => self.store.list_with_offset(Some(&prefix), o),
            None => self.store.list(Some(&prefix)),
        };
        let pass = async {
            let mut reads = listing
                .take(max_scan)
                .map(|meta| async move {
                    let meta = meta?;
                    let sref = sref_of_key(&self.cell, &meta.location)
                        .ok_or_else(|| catalog_error("non-canonical catalog key"))?;
                    if meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                        return Err(catalog_error("descriptor exceeds catalog byte budget"));
                    }
                    let raw = match self.store.get(&meta.location).await {
                        Ok(result) => {
                            let mut chunks = result.into_stream();
                            let mut raw = Vec::new();
                            while let Some(chunk) = chunks.try_next().await? {
                                if raw.len().saturating_add(chunk.len()) > MAX_DESCRIPTOR_BYTES {
                                    return Err(catalog_error(
                                        "descriptor exceeds catalog byte budget",
                                    ));
                                }
                                raw.extend_from_slice(&chunk);
                            }
                            raw
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            return Ok(((meta.location, sref), None, 0));
                        }
                        Err(error) => return Err(error),
                    };
                    if raw.len() > MAX_DESCRIPTOR_BYTES {
                        return Err(catalog_error("descriptor exceeds catalog byte budget"));
                    }
                    let desc = decode_desc(&raw, Some(&sref)).map_err(|error| {
                        catalog_error(&format!(
                            "catalog: undecodable descriptor at {}: {error}",
                            meta.location
                        ))
                    })?;
                    Ok(((meta.location, sref), Some(desc), raw.len()))
                })
                .buffered(8);
            let (mut out, mut last, mut scanned, mut bytes) = (Vec::new(), None, 0usize, 0usize);
            let now = crate::shard::now_ms();
            let mut exhausted = false;
            while out.len() < limit {
                let Some((consumed, desc, size)) = reads.try_next().await? else {
                    exhausted = scanned < max_scan;
                    break;
                };
                if bytes.saturating_add(size) > MAX_PAGE_BYTES {
                    break;
                }
                bytes += size;
                scanned += 1;
                // Advance only through consumed provider results. Prefetched
                // results beyond the output/byte limit are retried next page.
                last = Some(consumed);
                if let Some(desc) = desc {
                    let active = !desc.deleted
                        && !desc.soft_deleted
                        && desc.init.is_none()
                        && !desc.expires_at_ms.is_some_and(|expires| now >= expires);
                    if include_inactive || active {
                        out.push(desc);
                    }
                }
            }
            Ok((out, last, exhausted))
        };
        tokio::time::timeout(Duration::from_secs(10), pass)
            .await
            .map_err(|_| catalog_error("catalog page deadline exceeded"))?
    }
}

#[cfg(test)]
mod tests;
