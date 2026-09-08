//! R08-A: all three canonical history scan paths validate the same rows.
use super::{
    execute_postings_plan, hist2_record_key, read_history2_keyed_envelope, read_history2_scan,
};
use crate::crypto::{FrameCipher, FrameCompression, RouteHash, SegmentHash};
use crate::postings::AbsRun;
use slatedb::Db;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_history_scan_envelope_and_postings_reject_corrupt_rows() {
    let route = RouteHash([7; 16]);
    let inc = SegmentHash([8; 16]);
    for label in ["short", "offset", "frame"] {
        let db = Arc::new(
            Db::builder(
                format!("r08a-history-{label}"),
                Arc::new(object_store::memory::InMemory::new()),
            )
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
        );
        let mut key = hist2_record_key(route, inc, 1);
        let value = match label {
            "short" => {
                key.remove(33);
                FrameCipher::new(&[7; 32], &inc.0, FrameCompression::Disabled).encrypt(
                    &inc.0,
                    1,
                    1,
                    1,
                    "other",
                    b"retained",
                )
            }
            "offset" => FrameCipher::new(&[7; 32], &inc.0, FrameCompression::Disabled).encrypt(
                &inc.0,
                2,
                1,
                1,
                "other",
                b"retained",
            ),
            _ => b"broken".to_vec(),
        };
        db.put(&key, value.clone())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        assert!(
            read_history2_scan(&db, route, inc, 0, 512, 1024)
                .await
                .is_err(),
            "{label}: whole scan"
        );
        assert!(
            read_history2_keyed_envelope(&db, route, inc, "wanted", 0, 512, 1024)
                .await
                .is_err(),
            "{label}: filtered envelope"
        );
        let runs = vec![AbsRun {
            start: 0,
            count: 512,
            matching_bytes: 512,
            gap_bytes_before: 0,
        }];
        assert!(
            execute_postings_plan(
                &db,
                route,
                inc,
                "wanted",
                crate::postings::RunWindow::new(
                    crate::postings::ValidatedRuns::new(runs).unwrap(),
                    0,
                    512
                ),
                512,
                512,
                1024
            )
            .await
            .is_err(),
            "{label}: postings span"
        );
        assert_eq!(db.get(&key).await.unwrap().unwrap().as_ref(), value);
        db.close().await.unwrap();
    }
}
