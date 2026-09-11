//! Recompute invoice meters per account and project, then consume those
//! totals as their served aggregates are checked. Any entries left over
//! have no aggregate. Storage is intentionally outside this comparison.

use super::totals::InvoiceMeters;
use super::{AggRow, MonthRow, ReconcileReport, UsageRollup, k_month_prefix};
use std::collections::BTreeMap;

struct Reconciliation {
    report: ReconcileReport,
    // mt-lint: allow(name-keyed-map): account and project identifiers form the complete invoice comparison key; stream names are never keys here
    computed: BTreeMap<(String, String), InvoiceMeters>,
}

impl Reconciliation {
    fn stream(&mut self, key: &[u8], value: &[u8]) {
        let key = std::str::from_utf8(key).unwrap_or("");
        // month/{month}/{account}/{project}/{stream_id}
        let parts: Vec<&str> = key.splitn(5, '/').collect();
        let [_, _, account, project, _] = parts.as_slice() else {
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
        if !row.account_id.is_empty() && row.account_id != *account {
            self.report.mismatches.push(format!(
                "row account drift at {key}: key={account} row={}",
                row.account_id
            ));
        }
        self.computed
            .entry(((*account).to_owned(), (*project).to_owned()))
            .or_default()
            .add(&row.invoice_meters());
    }

    fn project(&mut self, key: &[u8], value: &[u8]) {
        let key = std::str::from_utf8(key).unwrap_or("");
        // project/{month}/{account}/{project}
        let parts: Vec<&str> = key.splitn(4, '/').collect();
        let [_, _, account, project] = parts.as_slice() else {
            self.report
                .mismatches
                .push(format!("unparseable project key: {key}"));
            return;
        };
        let Ok(aggregate) = serde_json::from_slice::<AggRow>(value) else {
            self.report
                .mismatches
                .push(format!("undecodable project aggregate: {key}"));
            return;
        };
        self.report.projects += 1;
        let served = aggregate.invoice_meters();
        match self
            .computed
            .remove(&((*account).to_owned(), (*project).to_owned()))
        {
            None => self.report.mismatches.push(format!(
                "aggregate without stream rows: {account}/{project}"
            )),
            Some(computed) if computed != served => self.report.mismatches.push(format!(
                "totals disagree for {account}/{project}: streams={computed:?} aggregate={served:?}"
            )),
            Some(_) => {}
        }
    }

    fn finish(mut self) -> ReconcileReport {
        for (account, project) in self.computed.keys() {
            self.report.mismatches.push(format!(
                "stream rows without an aggregate: {account}/{project}"
            ));
        }
        self.report.ok = self.report.mismatches.is_empty();
        self.report
    }
}

impl UsageRollup {
    /// Reconcile every account/project separately, including ownership
    /// transfers. Stream and project totals include corrections; stream
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
        let mut streams = self.db.scan_prefix(&prefix[..], ..).await?;
        while let Some(row) = streams.next().await? {
            reconciliation.stream(&row.key, &row.value);
        }
        let prefix = format!("project/{month}/").into_bytes();
        let mut projects = self.db.scan_prefix(&prefix[..], ..).await?;
        while let Some(row) = projects.next().await? {
            reconciliation.project(&row.key, &row.value);
        }
        Ok(reconciliation.finish())
    }
}

#[cfg(test)]
mod tests;
