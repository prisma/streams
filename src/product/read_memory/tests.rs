#![cfg(test)]

use axum::http::Method;

use super::{appends, carries_body, pull_coverage, query_on_cell};

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

/// Only a `POST` or a `PUT` has its body buffered under the project's
/// memory line, and so only they can be shed before their body is read
/// (fair shedding's blame or the RSS write gate); every other method's
/// body is dropped unpolled and the request goes on to its handler.
#[test]
fn only_a_post_or_a_put_carries_a_body() {
    assert!(carries_body(&Method::POST));
    assert!(carries_body(&Method::PUT));
    for method in [
        Method::GET,
        Method::HEAD,
        Method::DELETE,
        Method::PATCH,
        Method::OPTIONS,
    ] {
        assert!(!carries_body(&method), "{method}");
    }
}

/// A consumer pull reserves the shared cell's 1 MiB page on a shared cell
/// and a dedicated cell's 4 MiB coverage on a dedicated one; no other
/// request reserves a coverage.
#[test]
fn a_pull_reserves_one_mib_on_a_shared_cell_and_four_on_a_dedicated_one() {
    let (post, pull) = (&Method::POST, "orders/consumers/g:pull");
    assert_eq!(pull_coverage(SHARED, post, pull), Some(1 << 20));
    assert_eq!(pull_coverage(2, post, pull), Some(1 << 20), "two projects");
    assert_eq!(pull_coverage(1, post, pull), Some(4 << 20), "dedicated");
    assert_eq!(pull_coverage(1, &Method::GET, pull), None);
    assert_eq!(
        pull_coverage(SHARED, post, "orders/consumers/g:settle"),
        None
    );
    assert_eq!(pull_coverage(SHARED, post, "orders/records"), None);
}
