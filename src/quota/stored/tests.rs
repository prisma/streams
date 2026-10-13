//! The published stored figures (stored-cap B3).
#![cfg(test)]

use super::{StoredFigure, StoredTable};
use crate::tenant::ProjectId;

fn figure(project: &str, bytes: u64, written: u64) -> StoredFigure {
    StoredFigure {
        project: ProjectId::new(project).unwrap(),
        bytes,
        written,
    }
}

fn sorted(table: &StoredTable) -> Vec<StoredFigure> {
    let mut figures = table.figures();
    figures.sort_by(|a, b| a.project.as_str().cmp(b.project.as_str()));
    figures
}

/// A table nothing published into holds no figure and no time.
#[test]
fn an_unpublished_table_is_empty_and_undated() {
    let table = StoredTable::default();
    assert_eq!(table.figures(), Vec::new());
    assert_eq!(table.published_ms(), 0);
}

/// A publication replaces each named project's figures, adds a project it
/// names first, keeps every other project's, and dates the table; every
/// clone of the table sees it.
#[test]
fn a_publication_replaces_each_named_projects_figures() {
    let table = StoredTable::default();
    let reader = table.clone();
    table.publish(&[figure("p", 10, 10), figure("q", 3, 3)], 5);
    assert_eq!(sorted(&reader), [figure("p", 10, 10), figure("q", 3, 3)]);
    assert_eq!(reader.published_ms(), 5);
    table.publish(&[figure("p", 4, 12), figure("r", 1, 1)], 7);
    let expected = [figure("p", 4, 12), figure("q", 3, 3), figure("r", 1, 1)];
    assert_eq!(sorted(&reader), expected);
    assert_eq!(reader.published_ms(), 7);
}

/// A page that changed no figure still dates the table: its figures are
/// current through that page.
#[test]
fn an_empty_publication_only_dates_the_table() {
    let table = StoredTable::default();
    table.publish(&[figure("p", 10, 10)], 5);
    table.publish(&[], 9);
    assert_eq!(table.figures(), [figure("p", 10, 10)]);
    assert_eq!(table.published_ms(), 9);
}
