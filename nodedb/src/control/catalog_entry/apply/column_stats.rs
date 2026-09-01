// SPDX-License-Identifier: BUSL-1.1

//! Apply ANALYZE column-statistics rows to `SystemCatalog` redb.
//!
//! Writes only. ANALYZE scans every vShard, so the numbers describe the whole
//! collection and each node stores the same figures. Apply runs the
//! unvalidated catalog path: a rejection here diverges a follower from a
//! statement the leader already accepted.
//!
//! One entry carries a collection's whole column set and lands in one
//! transaction. A planner reads every column or none.

use crate::control::security::catalog::column_stats::StoredColumnStats;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutColumnStats` entry. A re-delivery rewrites the same rows.
pub fn put_rows(rows: &[StoredColumnStats], catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_column_stats_batch(rows).map_err(|e| {
        let collection = rows.first().map_or("", |r| r.collection.as_str());
        catalog_err(
            &format!("put_column_stats '{collection}' ({} rows)", rows.len()),
            e,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};

    const DATABASE: u64 = 3;
    const TENANT: u64 = 7;
    const COLLECTION: &str = "documents";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn row(column: &str) -> StoredColumnStats {
        StoredColumnStats {
            database_id: DATABASE,
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            column: column.to_string(),
            row_count: 1200,
            null_count: 3,
            distinct_count: 900,
            min_value: Some("a".to_string()),
            max_value: Some("z".to_string()),
            avg_value_len: Some(12),
            analyzed_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn put_column_stats_roundtrips_through_codec() {
        let entry = CatalogEntry::PutColumnStats(Box::new(vec![row("title"), row("body")]));
        match decode(&encode(&entry).unwrap()).unwrap() {
            CatalogEntry::PutColumnStats(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].database_id, DATABASE);
                assert_eq!(rows[0].tenant_id, TENANT);
                assert_eq!(rows[0].collection, COLLECTION);
                assert_eq!(rows[0].column, "title");
                assert_eq!(rows[0].row_count, 1200);
                assert_eq!(rows[0].distinct_count, 900);
                assert_eq!(rows[1].column, "body");
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_every_column_stats_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutColumnStats(Box::new(vec![row("title"), row("body")])),
            &catalog,
        )
        .unwrap();

        let mut stored: Vec<String> = catalog
            .load_column_stats(DATABASE, TENANT, COLLECTION)
            .unwrap()
            .into_iter()
            .map(|s| s.column)
            .collect();
        stored.sort();
        assert_eq!(stored, vec!["body", "title"]);
    }
}
