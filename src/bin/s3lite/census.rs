//! The emulator's storage census for cost runs: what the bucket holds right
//! now, in bytes (`GET /_s3lite/stats2` `live_bytes`), and the bytes sent
//! to it. The parts of a multipart upload that is neither completed nor
//! aborted are stored, and billed, before they become an object: they are
//! counted under their target's tier as `<tier>/in_progress_multipart`,
//! and a part's body counts into `put_bytes` when it is uploaded, whether
//! or not the upload is ever completed (an aborted upload's parts were
//! sent all the same).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::Method;

use super::{AppState, kind_class, tier_class};

/// The kind under which open multipart uploads are counted.
const IN_PROGRESS: &str = "in_progress_multipart";

impl AppState {
    /// Live-bytes census: the count and original byte length of every object
    /// the bucket holds, and of every open multipart upload's parts (one
    /// entry per upload), per tier/kind and in total. A poisoned map reports
    /// no partial census.
    pub(super) fn live_bytes(&self) -> serde_json::Value {
        let (Ok(objects), Ok(uploads)) = (self.objects.lock(), self.uploads.lock()) else {
            return serde_json::json!({"poisoned": true});
        };
        let no_query = HashMap::new();
        let class = |full_key: &str| {
            let key = full_key.split_once('/').map_or(full_key, |x| x.1);
            (tier_class(&Method::PUT, key, &no_query), kind_class(key))
        };
        let mut cells: BTreeMap<(&str, &str), [u64; 2]> = BTreeMap::new();
        for (key, object) in objects.iter() {
            let cell = cells.entry(class(key)).or_default();
            cell[0] += 1;
            cell[1] += object.orig_len as u64;
        }
        for (key, parts) in uploads.iter() {
            // "{bucket}/{key}:{upload id}"
            let target = key.rsplit_once(':').map_or(key.as_str(), |x| x.0);
            let cell = cells.entry((class(target).0, IN_PROGRESS)).or_default();
            cell[0] += 1;
            cell[1] += parts.values().map(|part| part.len() as u64).sum::<u64>();
        }
        drop((objects, uploads));
        let census =
            |[objects, bytes]: [u64; 2]| serde_json::json!({"objects": objects, "bytes": bytes});
        let total = cells
            .values()
            .fold([0, 0], |[n, b], [cn, cb]| [n + cn, b + cb]);
        let cells: serde_json::Map<_, _> = cells
            .into_iter()
            .map(|((tier, kind), counts)| (format!("{tier}/{kind}"), census(counts)))
            .collect();
        serde_json::json!({"cells": cells, "total": census(total)})
    }

    /// `GET /_s3lite/stats2`: the request ledger and both live censuses.
    pub(super) fn stats2(&self) -> serde_json::Value {
        let mut body = self.stats.detailed_snapshot();
        body["live_objects"] = self.live_objects();
        body["live_bytes"] = self.live_bytes();
        body
    }

    /// An UploadPart body, read once and counted into `put_bytes` as it is
    /// uploaded. A body that fails to read is handed on failing, so the
    /// part handler answers it as before.
    pub(super) async fn counted_part(&self, body: Body) -> Body {
        match axum::body::to_bytes(body, usize::MAX).await {
            Ok(data) => {
                let sent = data.len() as u64;
                self.stats.put_bytes.fetch_add(sent, Ordering::Relaxed);
                Body::from(data)
            }
            Err(error) => Body::from_stream(futures_util::stream::once(async move {
                Err::<bytes::Bytes, _>(std::io::Error::other(error.to_string()))
            })),
        }
    }
}
