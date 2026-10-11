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

use axum::http::Method;

use super::{AppState, kind_class, tier_class};

/// The kind under which open multipart uploads are counted.
const IN_PROGRESS: &str = "in_progress_multipart";

impl AppState {
    /// Live-bytes census: the count and original byte length of every object
    /// the bucket holds, and of every open multipart upload's parts (one
    /// entry per upload), per tier/kind and in total. A poisoned map reports
    /// no partial census.
    #[cfg_attr(not(test), expect(dead_code, reason = "L6 spike: tests read the census alone"))]
    pub(super) fn live_bytes(&self) -> serde_json::Value {
        self.live_and_put_bytes().0
    }

    /// L6 spike: the live census and the byte ledger read under the objects,
    /// uploads and byte-ledger locks at once (the order every writer takes
    /// them in), so `put_bytes.total` equals the sum of its cells and the
    /// live total equals put minus out in every snapshot.
    pub(super) fn live_and_put_bytes(&self) -> (serde_json::Value, serde_json::Value) {
        let poisoned = || serde_json::json!({"poisoned": true});
        let (Ok(objects), Ok(uploads)) = (self.objects.lock(), self.uploads.lock()) else {
            return (poisoned(), poisoned());
        };
        let Ok(ledger) = self.stats.bytes.lock() else {
            return (poisoned(), poisoned());
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
        let total_put = self.stats.put_bytes.load(Ordering::Relaxed);
        let put_doc = put_bytes_doc(&ledger, total_put, &cells);
        drop((ledger, objects, uploads));
        let census =
            |[objects, bytes]: [u64; 2]| serde_json::json!({"objects": objects, "bytes": bytes});
        let total = cells
            .values()
            .fold([0, 0], |[n, b], [cn, cb]| [n + cn, b + cb]);
        let cells: serde_json::Map<_, _> = cells
            .into_iter()
            .map(|((tier, kind), counts)| (format!("{tier}/{kind}"), census(counts)))
            .collect();
        (
            serde_json::json!({"cells": cells, "total": census(total)}),
            put_doc,
        )
    }

    /// `GET /_s3lite/stats2`: the request ledger, both live censuses and
    /// (L6 spike) the byte ledger.
    pub(super) fn stats2(&self) -> serde_json::Value {
        let mut body = self.stats.detailed_snapshot();
        body["live_objects"] = self.live_objects();
        let (live, put) = self.live_and_put_bytes();
        body["live_bytes"] = live;
        body["put_bytes"] = put;
        body
    }
}

/// L6 spike: the byte ledger as JSON, with its two reconciliations: the
/// cells sum to the global counter, and per tier the live bytes equal the
/// bytes put minus the bytes that left (rejected, replaced, deleted,
/// aborted).
fn put_bytes_doc(
    ledger: &super::ByteLedger,
    total_put: u64,
    live: &BTreeMap<(&str, &str), [u64; 2]>,
) -> serde_json::Value {
    let mut tiers: BTreeMap<&str, [u64; 3]> = BTreeMap::new(); // put, out, live
    let mut cells = serde_json::Map::new();
    let mut cells_sum = 0u64;
    for (class, n) in &ledger.put {
        cells.insert(format!("{}/{}/{}", class.tier, class.kind, class.op), (*n).into());
        cells_sum += n;
        tiers.entry(class.tier).or_default()[0] += n;
    }
    let mut out = serde_json::Map::new();
    let mut out_sum = 0u64;
    for ((tier, kind), counts) in &ledger.out {
        let named: serde_json::Map<String, serde_json::Value> = super::OUT_NAMES
            .iter()
            .zip(counts)
            .map(|(name, n)| ((*name).to_string(), (*n).into()))
            .collect();
        out.insert(format!("{tier}/{kind}"), named.into());
        let sum: u64 = counts.iter().sum();
        out_sum += sum;
        tiers.entry(tier).or_default()[1] += sum;
    }
    let mut live_sum = 0u64;
    for ((tier, _), [_, bytes]) in live {
        tiers.entry(tier).or_default()[2] += bytes;
        live_sum += bytes;
    }
    let tiers_ok = tiers
        .values()
        .all(|[put, out, live]| put.checked_sub(*out) == Some(*live));
    let by_tier: serde_json::Map<String, serde_json::Value> = tiers
        .into_iter()
        .map(|(tier, [put, out, live])| {
            (
                tier.to_string(),
                serde_json::json!({"put": put, "out": out, "live": live}),
            )
        })
        .collect();
    serde_json::json!({
        "total": total_put,
        "cells": cells,
        "cells_sum": cells_sum,
        "out": out,
        "out_sum": out_sum,
        "live_sum": live_sum,
        "by_tier": by_tier,
        "check": {
            "cells_sum_eq_total": cells_sum == total_put,
            "live_eq_put_minus_out": total_put.checked_sub(out_sum) == Some(live_sum),
            "tiers_reconcile": tiers_ok,
        },
    })
}
