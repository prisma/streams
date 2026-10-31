//! Step 0 of the S-gap plan: every database the process opens is
//! attributed by path, the telemetry databases included, with its reads
//! by kind and its object-store requests by component.
use std::sync::Arc;

use object_store::ObjectStore;

use super::metrics::attribution;

fn cfg() -> crate::config::ServerConfig {
    crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
}

#[tokio::test(crate = "crate::test_rt")]
async fn the_read_spool_and_the_usage_rollup_are_attributed_by_path() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cfg = cfg();
    let spool = crate::billing::ReadSpool::open(store.clone(), "attrib-a", "solo", &cfg)
        .await
        .unwrap();
    let rollup = crate::rollup::UsageRollup::open(store.clone(), "attrib-a", &cfg)
        .await
        .unwrap();
    assert!(rollup.cursor().await.unwrap().is_none());
    let a = attribution();
    let spool_path = "attrib-a/telemetry/read-spool/solo";
    let rollup_path = format!("attrib-a/{}", crate::rollup::ROLLUP_PATH);
    for path in [spool_path, rollup_path.as_str()] {
        let db = a
            .get(path)
            .unwrap_or_else(|| panic!("{path} is not attributed: {a}"));
        let gets = db
            .as_object()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with("slatedb.db.request_count{op=get"))
            .map(|(_, v)| v.as_u64().unwrap())
            .sum::<u64>();
        assert!(gets >= 1, "{path}: no point read counted: {db}");
    }
    drop(spool);
}

/// S-gap step 0: the usage rollup's call sites are attributed under its path:
/// the raw-ops retention sweep counts as `gc`, month close as `month_close`,
/// a rollup step's cursor read as `rollup_step` (`ops_rollup_step` for the
/// ops cursor), a pending-artifact scan as `outbox`.
#[tokio::test(crate = "crate::test_rt")]
async fn the_rollups_call_sites_are_attributed_under_its_path() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let rollup = crate::rollup::UsageRollup::open(store, "attrib-b", &cfg())
        .await
        .unwrap();
    rollup.sweep_ops_raw(0, 10).await.unwrap();
    rollup.close_months_due(0).await.unwrap();
    rollup.cursor().await.unwrap();
    rollup.ops_cursor().await.unwrap();
    rollup.pending_artifacts(64).await.unwrap();
    let a = attribution();
    let path = format!("attrib-b/{}", crate::rollup::ROLLUP_PATH);
    let db = a
        .get(&path)
        .unwrap_or_else(|| panic!("{path} is not attributed: {a}"));
    for site in [
        "gc",
        "month_close",
        "rollup_step",
        "ops_rollup_step",
        "outbox",
    ] {
        let key = format!("prisma_streams.call_site.calls{{site={site}}}");
        assert_eq!(
            db.get(&key).and_then(serde_json::Value::as_u64),
            Some(1),
            "{key}: {db}"
        );
    }
}

/// S-gap step 0: a call site's GETs are exactly its database's main-store
/// reader GETs while it runs. Reads before or after it, a compactor's GETs,
/// the WAL's and HEADs never count; a call site that ran beside another of
/// the same database counts under `overlap`, its GETs and the other's.
#[test]
fn a_call_site_counts_exactly_the_reader_gets_while_it_runs() {
    let m = super::metrics::for_log("tests/call-site-exact");
    let recorder = m.recorder();
    let get = |component: &str, store: &str, api: &str, n: u64| {
        let labels = [
            ("component", component),
            ("store_type", store),
            ("op", "get"),
            ("api", api),
        ];
        recorder
            .register_counter("slatedb.object_store.request_count", "test", &labels)
            .increment(n);
    };
    get("db", "main", "get", 5);
    {
        let _a = m.call_site("site-a");
        get("db", "main", "get", 7);
        get("compactor", "main", "get", 11);
        get("db", "wal", "get", 13);
        get("db", "main", "head", 17);
    }
    get("db", "main", "get", 19);
    assert_eq!(m.call_site_counts("site-a"), (1, 7));
    {
        let a = m.call_site("site-a");
        get("db", "main", "get", 2);
        let b = m.call_site("site-b");
        get("db", "main", "get", 3);
        drop(a);
        get("db", "main", "get", 4);
        drop(b);
    }
    assert_eq!(m.call_site_counts("site-a"), (1, 7));
    assert_eq!(m.call_site_counts("site-b"), (0, 0));
    assert_eq!(m.call_site_counts("overlap"), (2, 5 + 7));
    {
        let _b = m.call_site("site-b");
        get("db", "main", "get", 23);
    }
    assert_eq!(
        m.call_site_counts("site-b"),
        (1, 23),
        "a later call is alone"
    );
}
