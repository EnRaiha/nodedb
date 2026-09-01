// SPDX-License-Identifier: BUSL-1.1

//! Column statistics for query optimizer (ANALYZE).

use super::types::{COLUMN_STATS, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Per-column statistics collected by ANALYZE.
///
/// Stored in redb under `_system.column_stats` with key
/// `"{database_id}:{tenant_id}:{collection}:{column}"`.
/// Used by DataFusion's cost-based optimizer for cardinality estimation.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredColumnStats {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub column: String,
    /// Total number of rows in the collection at ANALYZE time.
    pub row_count: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Number of distinct values (estimated via HLL or exact for small sets).
    pub distinct_count: u64,
    /// Minimum value as string (for display and text-based comparison).
    pub min_value: Option<String>,
    /// Maximum value as string.
    pub max_value: Option<String>,
    /// Average value length in bytes (for variable-length types).
    pub avg_value_len: Option<u32>,
    /// Timestamp of last ANALYZE (epoch millis).
    pub analyzed_at: u64,
}

impl SystemCatalog {
    /// Store one column's statistics.
    pub fn put_column_stats(&self, stats: &StoredColumnStats) -> crate::Result<()> {
        self.put_column_stats_batch(std::slice::from_ref(stats))
    }

    /// Store every row in one transaction.
    ///
    /// ANALYZE writes a collection's columns through this call, so a planner
    /// reads either all of them or none. A partial set costs a query against
    /// a subset while reporting a whole collection.
    pub fn put_column_stats_batch(&self, stats: &[StoredColumnStats]) -> crate::Result<()> {
        if stats.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(COLUMN_STATS)
                .map_err(|e| catalog_err("open column_stats", e))?;
            for row in stats {
                let key = stats_key(row.database_id, row.tenant_id, &row.collection, &row.column);
                let bytes = zerompk::to_msgpack_vec(row)
                    .map_err(|e| catalog_err("serialize column_stats", e))?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert column_stats", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load all column statistics for one collection in one database.
    pub fn load_column_stats(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredColumnStats>> {
        let prefix = format!("{database_id}:{tenant_id}:{collection}:");
        let upper = prefix_upper_bound(database_id, tenant_id, collection);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLUMN_STATS)
            .map_err(|e| catalog_err("open column_stats", e))?;

        let mut stats = Vec::new();
        let range = table
            .range(prefix.as_str()..upper.as_str())
            .map_err(|e| catalog_err("range column_stats", e))?;
        for row in range {
            let (key, value) = row.map_err(|e| catalog_err("scan column_stats", e))?;
            match zerompk::from_msgpack::<StoredColumnStats>(value.value()) {
                Ok(decoded) => stats.push(decoded),
                Err(e) => tracing::warn!(
                    key = key.value(),
                    error = %e,
                    "skipping undecodable column_stats row"
                ),
            }
        }
        Ok(stats)
    }
}

fn stats_key(database_id: u64, tenant_id: u64, collection: &str, column: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}:{column}")
}

/// Exclusive upper bound for one collection's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key
/// sorts immediately past every column of the collection.
fn prefix_upper_bound(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample(database_id: u64, column: &str) -> StoredColumnStats {
        StoredColumnStats {
            database_id,
            tenant_id: 1,
            collection: "users".into(),
            column: column.into(),
            row_count: 10000,
            null_count: 50,
            distinct_count: 9500,
            min_value: Some("a@b.com".into()),
            max_value: Some("z@z.org".into()),
            avg_value_len: Some(20),
            analyzed_at: 1700000000000,
        }
    }

    #[test]
    fn put_and_load_stats() {
        let (_dir, cat) = make_catalog();
        cat.put_column_stats(&sample(2, "email")).unwrap();

        let loaded = cat.load_column_stats(2, 1, "users").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].column, "email");
        assert_eq!(loaded[0].row_count, 10000);
        assert_eq!(loaded[0].distinct_count, 9500);
    }

    #[test]
    fn stats_of_one_database_are_invisible_to_another() {
        let (_dir, cat) = make_catalog();
        let mut other = sample(3, "email");
        other.row_count = 42;
        cat.put_column_stats(&sample(2, "email")).unwrap();
        cat.put_column_stats(&other).unwrap();

        let first = cat.load_column_stats(2, 1, "users").unwrap();
        let second = cat.load_column_stats(3, 1, "users").unwrap();
        assert_eq!(first.len(), 1, "the key is scoped by database");
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].row_count, 10000);
        assert_eq!(second[0].row_count, 42);
    }

    #[test]
    fn a_batch_write_lands_every_column() {
        let (_dir, cat) = make_catalog();
        let rows = vec![sample(2, "email"), sample(2, "name"), sample(2, "age")];
        cat.put_column_stats_batch(&rows).unwrap();

        let mut loaded: Vec<String> = cat
            .load_column_stats(2, 1, "users")
            .unwrap()
            .into_iter()
            .map(|s| s.column)
            .collect();
        loaded.sort();
        assert_eq!(loaded, vec!["age", "email", "name"]);
    }

    #[test]
    fn the_range_excludes_a_collection_sharing_a_name_prefix() {
        let (_dir, cat) = make_catalog();
        let mut sibling = sample(2, "email");
        sibling.collection = "users_archive".into();
        cat.put_column_stats(&sample(2, "email")).unwrap();
        cat.put_column_stats(&sibling).unwrap();

        let loaded = cat.load_column_stats(2, 1, "users").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].collection, "users");
    }
}
