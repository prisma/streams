//! The project catalog page: one bounded, budgeted listing of a project's
//! descriptors, read through the same decoder the registry serves from.
use super::*;

impl Registry {
    #[expect(
        clippy::excessive_nesting,
        reason = "Registry::catalog_page; the page walk nests the byte, size and decode guards inside each fetched descriptor's read; flattening them would separate the guards from the descriptor they bound"
    )]
    pub(super) async fn catalog_page(
        &self,
        project: &crate::tenant::ProjectId,
        after: Option<&str>,
        limit: usize,
        include_inactive: bool,
    ) -> Result<CatalogPage, object_store::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        if limit == 0 {
            return Err(catalog_error("catalog limit must be positive"));
        }
        let limit = limit.min(1000);
        let max_scan = limit.saturating_mul(8) + 64;
        const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
        const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
        let root = project_streams_prefix(project);
        let prefix = ObjPath::from(root.trim_end_matches('/'));
        let offset = after.map(|n| ObjPath::from(format!("{root}{}.json", hex(n.as_bytes()))));
        let listing = match &offset {
            Some(o) => self.store.list_with_offset(Some(&prefix), o),
            None => self.store.list(Some(&prefix)),
        };
        let pass = async {
            let mut reads = listing
                .take(max_scan)
                .map(|meta| async move {
                    let meta = meta?;
                    let name = name_from_desc_path(&meta.location)
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
                        Err(object_store::Error::NotFound { .. }) => return Ok((name, None, 0)),
                        Err(error) => return Err(error),
                    };
                    if raw.len() > MAX_DESCRIPTOR_BYTES {
                        return Err(catalog_error("descriptor exceeds catalog byte budget"));
                    }
                    let canonical = crate::tenant::CanonicalStreamName::new(&name)
                        .map_err(|_| catalog_error("non-canonical catalog name"))?;
                    let expect = crate::tenant::TenantStreamRef::new(project.clone(), canonical);
                    let desc = decode_desc(&raw, Some(&expect)).map_err(|error| {
                        catalog_error(&format!(
                            "catalog: undecodable descriptor at {}: {error}",
                            meta.location
                        ))
                    })?;
                    Ok((name, Some(desc), raw.len()))
                })
                .buffered(8);
            let (mut out, mut last_name, mut scanned, mut bytes) =
                (Vec::new(), None, 0usize, 0usize);
            let now = crate::shard::now_ms();
            let mut exhausted = false;
            while out.len() < limit {
                let Some((name, desc, size)) = reads.try_next().await? else {
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
                last_name = Some(name);
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
            Ok(CatalogPage {
                streams: out,
                next_after: last_name,
                exhausted,
            })
        };
        tokio::time::timeout(Duration::from_secs(10), pass)
            .await
            .map_err(|_| catalog_error("catalog page deadline exceeded"))?
    }
}
