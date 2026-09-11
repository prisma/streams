//! Validated sweep dimensions and immutable request shapes.
use anyhow::Context;
use bytes::Bytes;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verb {
    Append,
    Health,
    Sleep(u64),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Point {
    pub(super) sweep: &'static str,
    pub(super) event_bytes: usize,
    pub(super) batch: usize,
}

pub(super) struct Config {
    pub(super) target: String,
    pub(super) auth: String,
    pub(super) key: String,
    pub(super) stream: String,
    pub(super) verb: Verb,
    pub(super) warmup: Duration,
    pub(super) measure: Duration,
    pub(super) drain: Duration,
    pub(super) max_workers: NonZeroUsize,
    pub(super) inflight_bytes: usize,
    pub(super) plan: Vec<Point>,
    pub(super) port: u16,
}

impl Config {
    pub(super) fn load(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let target = lookup("TARGET").context("TARGET required")?;
        let parsed = reqwest::Url::parse(&target).context("invalid TARGET URL")?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
            "TARGET must be an HTTP URL"
        );
        let auth = lookup("AUTH_TOKEN").context("AUTH_TOKEN required")?;
        let key = lookup("STREAM_KEY").context("STREAM_KEY required")?;
        let verb = match lookup("BENCH_VERB").as_deref().unwrap_or("append") {
            "append" => Verb::Append,
            "health" => Verb::Health,
            "sleep" => Verb::Sleep(number(&lookup, "SLEEP_MS", "100")?),
            _ => anyhow::bail!("BENCH_VERB must be append, health or sleep"),
        };
        let fixed: NonZeroUsize = number(&lookup, "FIXED_SIZE", "256")?;
        let sizes = ladder(
            &lookup,
            "SIZES",
            "64;256;1024;4096;16384;65536;262144;1048576",
        )?;
        let batches = ladder(&lookup, "BATCHES", "1;4;16;64;256;1024;4096")?;
        let mut plan: Vec<_> = sizes
            .into_iter()
            .map(|event_bytes| Point {
                sweep: "size",
                event_bytes,
                batch: 1,
            })
            .collect();
        plan.extend(batches.into_iter().map(|batch| Point {
            sweep: "batch",
            event_bytes: fixed.get(),
            batch,
        }));
        let inflight_mb: NonZeroUsize = number(&lookup, "MAX_INFLIGHT_MB", "2")?;
        let inflight_bytes = inflight_mb
            .get()
            .checked_mul(1024 * 1024)
            .context("MAX_INFLIGHT_MB overflows")?;
        let measure = Duration::from_secs(number(&lookup, "MEASURE_SECS", "40")?);
        anyhow::ensure!(!measure.is_zero(), "MEASURE_SECS must be positive");
        let drain = Duration::from_secs(number(&lookup, "INTER_POINT_SECS", "20")?);
        drain
            .checked_mul(3)
            .context("INTER_POINT_SECS overflows collapsed drain")?;
        Ok(Self {
            target: target.trim_end_matches('/').to_string(),
            auth,
            key,
            verb,
            plan,
            inflight_bytes,
            measure,
            drain,
            stream: lookup("BENCH_STREAM").unwrap_or_else(|| "bench-ordered".into()),
            warmup: Duration::from_secs(number(&lookup, "WARMUP_SECS", "8")?),
            max_workers: number(&lookup, "MAX_WORKERS", "1024")?,
            port: number(&lookup, "PORT", "8080")?,
        })
    }

    pub(super) fn concurrency(&self, point: Point, body_bytes: usize) -> usize {
        let max = self.max_workers.get();
        match self.verb {
            Verb::Append => (self.inflight_bytes / body_bytes.max(1)).clamp(4.min(max), max),
            Verb::Health | Verb::Sleep(_) => point.batch.clamp(1, max),
        }
    }

    pub(super) fn url(&self) -> String {
        match self.verb {
            Verb::Append => format!("{}/v1/stream/{}", self.target, self.stream),
            Verb::Health => format!("{}/health", self.target),
            Verb::Sleep(ms) => format!("{}/v1/debug/sleep?ms={ms}", self.target),
        }
    }
}

impl Point {
    pub(super) fn body(self, verb: Verb) -> anyhow::Result<Bytes> {
        if verb != Verb::Append {
            return Ok(Bytes::new());
        }
        let padding = self.event_bytes.saturating_sub(8).max(1);
        let record_len = padding.checked_add(8).context("event size overflows")?;
        let capacity = record_len
            .checked_add(1)
            .and_then(|n| n.checked_mul(self.batch))
            .and_then(|n| n.checked_add(1))
            .context("batch body size overflows")?;
        let mut body = String::new();
        body.try_reserve_exact(capacity)
            .context("cannot reserve benchmark body")?;
        body.push('[');
        for index in 0..self.batch {
            if index > 0 {
                body.push(',');
            }
            body.push_str("{\"p\":\"");
            body.extend(std::iter::repeat_n('x', padding));
            body.push_str("\"}");
        }
        body.push(']');
        Ok(Bytes::from(body))
    }
}

fn number<T: FromStr>(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    fallback: &str,
) -> anyhow::Result<T> {
    lookup(key)
        .as_deref()
        .unwrap_or(fallback)
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid {key}"))
}

fn ladder(
    lookup: &impl Fn(&str) -> Option<String>,
    key: &str,
    fallback: &str,
) -> anyhow::Result<Vec<usize>> {
    lookup(key)
        .as_deref()
        .unwrap_or(fallback)
        .split(';')
        .map(|part| {
            part.trim()
                .parse::<NonZeroUsize>()
                .map(NonZeroUsize::get)
                .map_err(|_| anyhow::anyhow!("invalid {key} dimension"))
        })
        .collect()
}
