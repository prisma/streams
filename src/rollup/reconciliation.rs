//! Recompute invoice meters per container, then consume those
//! totals as their served aggregates are checked. Any entries left over
//! have no aggregate. Storage is intentionally outside this comparison.

use super::totals::InvoiceMeters;
use super::{AggRow, MonthRow, ReconcileReport, UsageRollup, k_month_prefix};
use std::collections::BTreeMap;

struct Reconciliation {
    report: ReconcileReport,
    // mt-lint: allow(name-keyed-map): the container identifier is the complete invoice comparison key; stream names are never keys here
    computed: BTreeMap<String, InvoiceMeters>,
}

impl Reconciliation {
    fn stream(&mut self, key: &[u8], value: &[u8]) {
        let key = std::str::from_utf8(key).unwrap_or("");
        // month/{month}/{container}/{stream_id}
        let parts: Vec<&str> = key.splitn(4, '/').collect();
        let [_, _, container, _] = parts.as_slice() else {
            self.report
                .mismatches
                .push(format!("unparseable month key: {key}"));
            return;
        };
        let Ok(row) = serde_json::from_slice::<MonthRow>(value) else {
            self.report
                .mismatches
                .push(format!("undecodable month row: {key}"));
            return;
        };
        self.report.stream_rows += 1;
        self.computed
            .entry((*container).to_owned())
            .or_default()
            .add(&row.invoice_meters());
    }

    fn project(&mut self, key: &[u8], value: &[u8]) {
        let key = std::str::from_utf8(key).unwrap_or("");
        // container/{month}/{container}
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let [_, _, container] = parts.as_slice() else {
            self.report
                .mismatches
                .push(format!("unparseable container key: {key}"));
            return;
        };
        let Ok(aggregate) = serde_json::from_slice::<AggRow>(value) else {
            self.report
                .mismatches
                .push(format!("undecodable container aggregate: {key}"));
            return;
        };
        self.report.projects += 1;
        let served = aggregate.invoice_meters();
        match self.computed.remove(*container) {
            None => self
                .report
                .mismatches
                .push(format!("aggregate without stream rows: {container}")),
            Some(computed) if computed != served => self.report.mismatches.push(format!(
                "totals disagree for {container}: streams={computed:?} aggregate={served:?}"
            )),
            Some(_) => {}
        }
    }

    fn finish(mut self) -> ReconcileReport {
        for container in self.computed.keys() {
            self.report
                .mismatches
                .push(format!("stream rows without an aggregate: {container}"));
        }
        self.report.ok = self.report.mismatches.is_empty();
        self.report
    }
}

impl UsageRollup {
    /// Reconcile every container separately. Stream and container totals
    /// include corrections; stream
    /// totals use the same frozen invoice base as the customer response.
    /// Storage is excluded because provisional extrapolation differs.
    pub(crate) async fn reconcile_month(&self, month: &str) -> anyhow::Result<ReconcileReport> {
        let mut reconciliation = Reconciliation {
            report: ReconcileReport {
                month: month.to_owned(),
                ..Default::default()
            },
            computed: BTreeMap::new(),
        };
        let prefix = k_month_prefix(month);
        let mut streams = self
            .db
            .scan_prefix_with_options(&prefix[..], .., &crate::rollup::telemetry_scan())
            .await?;
        while let Some(row) = streams.next().await? {
            reconciliation.stream(&row.key, &row.value);
        }
        let prefix = format!("container/{month}/").into_bytes();
        let mut projects = self
            .db
            .scan_prefix_with_options(&prefix[..], .., &crate::rollup::telemetry_scan())
            .await?;
        while let Some(row) = projects.next().await? {
            reconciliation.project(&row.key, &row.value);
        }
        Ok(reconciliation.finish())
    }
}

#[cfg(test)]
mod tests;
