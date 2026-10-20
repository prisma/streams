#![cfg(test)]

use axum::http::Method;

use super::{appends, query_on_cell};

/// A shared cell's projects (`PROJECT_SHARE_K`).
const SHARED: u64 = 8;

#[test]
fn a_shared_cells_records_read_without_max_bytes_is_served_one_mib() {
    let get = &Method::GET;
    assert_eq!(
        query_on_cell(SHARED, get, "orders/records", ""),
        "maxBytes=1048576"
    );
    assert_eq!(
        query_on_cell(SHARED, get, "orders/records:long-poll", "offset=5"),
        "offset=5&maxBytes=1048576"
    );
    assert_eq!(
        query_on_cell(2, get, "orders/records", ""),
        "maxBytes=1048576",
        "two projects make a shared cell"
    );
}

#[test]
fn a_query_is_kept_as_sent_off_a_shared_cells_default_records_read() {
    let get = &Method::GET;
    assert_eq!(
        query_on_cell(1, get, "orders/records", "offset=5"),
        "offset=5",
        "a dedicated cell"
    );
    assert_eq!(
        query_on_cell(SHARED, get, "orders/records", "maxBytes=4096"),
        "maxBytes=4096",
        "a read that names its page"
    );
    assert_eq!(
        query_on_cell(SHARED, &Method::POST, "orders/records", ""),
        "",
        "an append"
    );
    for (path, query) in [
        ("orders/records:sse", "offset=5"),
        ("orders", ""),
        ("orders:scan", ""),
    ] {
        assert_eq!(query_on_cell(SHARED, get, path, query), query, "{path}");
    }
}

#[test]
fn only_a_records_append_single_or_batch_appends() {
    assert!(appends(&Method::POST, "orders/records"));
    assert!(appends(&Method::POST, "orders/records:batch"));
    for (method, path) in [
        (Method::GET, "orders/records"),
        (Method::PUT, "orders"),
        (Method::POST, "orders:seal"),
        (Method::POST, "orders/consumers/c1:pull"),
        (Method::POST, "orders/nowhere"),
    ] {
        assert!(!appends(&method, path), "{method} {path}");
    }
}
