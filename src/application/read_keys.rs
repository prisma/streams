//! Page-local key material: one page cipher per (key version, routing key)
//! lane of the segment a read serves, at most 64 expanded schedules.
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{CheckedPage, OpenedPage, PageCipher};
use std::collections::HashMap;
const MAX_CIPHERS: usize = 64;
pub(super) struct ReadKeys<'a> {
    key: &'a StreamKey,
    epoch: &'a [u8; 16],
    segment: [u8; 16],
    // mt-lint: allow(name-keyed-map): routing keys within this fixed StreamKey, epoch,
    // and physical segment are data lanes, never unqualified stream identities.
    pages: HashMap<u32, HashMap<String, PageCipher>>,
    pages_cached: usize,
}
impl<'a> ReadKeys<'a> {
    pub(super) fn new(key: &'a StreamKey, epoch: &'a [u8; 16], segment: [u8; 16]) -> Self {
        Self {
            key,
            epoch,
            segment,
            pages: HashMap::new(),
            pages_cached: 0,
        }
    }
    /// Open an admitted page of this segment under its lane's page key. The
    /// whole page authenticates and parses before any record is returned.
    /// A lane past the cache bound derives its cipher for this page only.
    pub(crate) fn open_page(&mut self, page: &CheckedPage) -> Result<OpenedPage, String> {
        let refused = |error| {
            format!(
                "stored page [{}, {}] did not open: {error:?}",
                page.first(),
                page.last()
            )
        };
        let lanes = self.pages.entry(page.key_version()).or_default();
        if let Some(cipher) = lanes.get(page.routing_key()) {
            return cipher.open(page).map_err(refused);
        }
        let subkey = derive_subkey(self.key, self.epoch, page.routing_key(), page.key_version());
        let cipher = PageCipher::new(&subkey, &self.segment);
        let opened = cipher.open(page).map_err(refused);
        if self.pages_cached < MAX_CIPHERS {
            self.pages_cached += 1;
            lanes.insert(page.routing_key().to_owned(), cipher);
        }
        opened
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto_page::PageLane;

    /// One sealed single-record page of `lane` at version `version`, under
    /// the page key of (`key`, `epoch`, `segment`), admitted as the ring
    /// admits it.
    fn page(
        key: &StreamKey,
        epoch: &[u8; 16],
        segment: &[u8; 16],
        lane: &str,
        version: u32,
    ) -> CheckedPage {
        let subkey = derive_subkey(key, epoch, lane, version);
        let lane = PageLane {
            ts_ms: 123,
            key_version: version,
            routing_key: lane,
        };
        let sealed = PageCipher::new(&subkey, segment)
            .seal(&lane, 7, &[b"same payload"])
            .unwrap();
        CheckedPage::admit(bytes::Bytes::from(sealed.bytes), sealed.last).unwrap()
    }

    fn payload(keys: &mut ReadKeys<'_>, page: &CheckedPage) -> Result<Vec<u8>, String> {
        let opened = keys.open_page(page)?;
        Ok(opened
            .records()
            .flat_map(|record| record.payload.to_vec())
            .collect())
    }

    /// 128 lanes over two key versions each open their own pages; the cache
    /// keeps exactly 64 page ciphers and the lanes past the bound still
    /// open. A page key is bound to its segment, its stream epoch and the
    /// key version: a reader of another segment or epoch, or a page whose
    /// clear key version was changed, does not open it.
    #[test]
    fn e03_page_cipher_cache_bounds_unique_lanes_and_fences_segment_epoch_and_version() {
        let key = StreamKey([3; 32]);
        let epoch = [4; 16];
        let segment = [5; 16];
        let mut keys = ReadKeys::new(&key, &epoch, segment);
        let mut pages = Vec::new();
        for version in [1, 2] {
            for index in 0..64 {
                let lane = format!("lane-{index}");
                pages.push(page(&key, &epoch, &segment, &lane, version));
            }
        }
        for page in &pages {
            assert_eq!(payload(&mut keys, page).unwrap(), b"same payload");
        }
        let cached: usize = keys.pages.values().map(HashMap::len).sum();
        assert_eq!((cached, keys.pages_cached), (MAX_CIPHERS, MAX_CIPHERS));
        for page in pages.iter().rev() {
            assert_eq!(payload(&mut keys, page).unwrap(), b"same payload");
        }
        let cached: usize = keys.pages.values().map(HashMap::len).sum();
        assert_eq!(
            (cached, keys.pages_cached),
            (MAX_CIPHERS, MAX_CIPHERS),
            "lanes past the bound must not retain more schedules"
        );
        let first = &pages[0];
        assert!(payload(&mut ReadKeys::new(&key, &epoch, [6; 16]), first).is_err());
        assert!(payload(&mut ReadKeys::new(&key, &[7; 16], segment), first).is_err());
        let mut changed = first.raw().to_vec();
        let at = 1 + 8 + 1 + 8;
        changed[at..at + 4].copy_from_slice(&2u32.to_be_bytes());
        let changed = CheckedPage::admit(bytes::Bytes::from(changed), first.last()).unwrap();
        assert_eq!(changed.key_version(), 2);
        assert!(payload(&mut keys, &changed).is_err());
    }
}
