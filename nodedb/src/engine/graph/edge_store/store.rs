// SPDX-License-Identifier: BUSL-1.1

//! EdgeStore root — types, redb table definitions, open/close.
//!
//! Query paths (`get_edge`, `scan_*`, `put_edge_raw`) live in `scan.rs`.
//! Cascade (`delete_edges_for_node`) lives in `cascade.rs`. Bitemporal
//! write/read primitives live in `temporal/`.

use std::path::Path;
use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};

use super::stats::{GRAPH_STATS, GRAPH_STATS_LEGACY};

/// `(collection, src, label, dst)` — a base-edge identity (no version suffix).
pub(super) type BaseKey = (String, String, String, String);

/// Database- and tenant-qualified `BaseKey`. Used when scanning across tenants.
pub(super) type TenantBaseKey = (u64, u64, String, String, String, String);

/// Edge table: composite key `(db, tid, "collection\x00src\x00label\x00dst\x00{system_from:020}")` → value.
///
/// Value is either an `EdgeValuePayload` (zerompk fixarray-3) or a single-byte
/// sentinel (`TOMBSTONE_SENTINEL`, `GDPR_ERASURE_SENTINEL`).
pub(super) const EDGES: TableDefinition<(u64, u64, &str), &[u8]> = TableDefinition::new("edges_v2");

/// Reverse edge index: same versioned key shape as `EDGES` but with
/// `dst`/`src` swapped. Value is empty for live edges, or a sentinel for
/// soft-deleted / erased edges (symmetry with forward).
pub(super) const REVERSE_EDGES: TableDefinition<(u64, u64, &str), &[u8]> =
    TableDefinition::new("reverse_edges_v2");

/// Legacy (pre-database-scoping) `(tid, composite)` forward-edge table.
const EDGES_LEGACY: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("edges");

/// Legacy (pre-database-scoping) `(tid, composite)` reverse-edge table.
const REVERSE_EDGES_LEGACY: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("reverse_edges");

pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("{ctx}: {e}"),
    }
}

// Re-export shared Direction from nodedb-types.
pub use nodedb_types::graph::Direction;

/// Decoded edge record yielded by `EdgeStore::scan_all_edges_decoded`:
/// `(database, tenant, collection, src, label, dst, properties)`. Current-state
/// only — tombstoned and GDPR-erased edges are filtered out, and only the latest
/// non-sentinel version per base key is yielded.
pub type EdgeRecord = (
    DatabaseId,
    TenantId,
    String,
    String,
    String,
    String,
    Vec<u8>,
);

/// A single edge with its properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
}

/// redb-backed edge storage for the Knowledge Graph engine.
///
/// Keys are `(TenantId, versioned_composite_key)` tuples — tenant routing
/// is structural, not lexical. Each Data Plane core owns its own
/// `EdgeStore` instance; no cross-core sharing.
pub struct EdgeStore {
    pub(super) db: Arc<Database>,
}

impl EdgeStore {
    /// Open or create the edge store database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("begin_write", e))?;
        {
            let _ = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let _ = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            let _ = write_txn
                .open_table(GRAPH_STATS)
                .map_err(|e| redb_err("open graph_stats", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Rewrite legacy (pre-database-scoping) `edges` / `reverse_edges` /
    /// `graph_stats` rows into their database-scoped `_v2` companions,
    /// prepending [`DatabaseId::DEFAULT`] (0) as the new leading key
    /// component. Covers all three tables in one write transaction.
    ///
    /// * **No-op on fresh boot** — the legacy tables are absent or empty.
    /// * **Idempotent** — once any `_v2` table is non-empty the rewrite is
    ///   skipped, so re-running on every core startup is safe.
    /// * **Atomic** — every rewrite commits in a single write transaction.
    ///
    /// redb has no `drop_table`, so the legacy rows remain in place after a
    /// migration (orphaned, harmless); live paths only ever touch the `_v2`
    /// tables. Old data is preserved under `DatabaseId::DEFAULT`, so it stays
    /// readable as the default database.
    pub fn migrate_edges_v2(&self) -> crate::Result<()> {
        // Gather legacy rows for all three tables up front.
        let edges = collect_legacy(&self.db, EDGES_LEGACY, "migrate_edges_v2 (edges)")?;
        let rev = collect_legacy(&self.db, REVERSE_EDGES_LEGACY, "migrate_edges_v2 (reverse)")?;
        let stats = collect_legacy(&self.db, GRAPH_STATS_LEGACY, "migrate_edges_v2 (stats)")?;

        if edges.is_empty() && rev.is_empty() && stats.is_empty() {
            return Ok(());
        }

        // Skip if any v2 table is already populated (already migrated).
        if v2_nonempty(&self.db, EDGES, "migrate_edges_v2 (edges v2)")?
            || v2_nonempty(&self.db, REVERSE_EDGES, "migrate_edges_v2 (reverse v2)")?
            || v2_nonempty(&self.db, GRAPH_STATS, "migrate_edges_v2 (stats v2)")?
        {
            return Ok(());
        }

        let db_id = DatabaseId::DEFAULT.as_u64();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("migrate_edges_v2 begin_write", e))?;
        {
            let mut t = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("migrate_edges_v2 open edges_v2", e))?;
            for (tid, composite, value) in &edges {
                t.insert((db_id, *tid, composite.as_str()), value.as_slice())
                    .map_err(|e| redb_err("migrate_edges_v2 insert edge", e))?;
            }
        }
        {
            let mut t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("migrate_edges_v2 open reverse_v2", e))?;
            for (tid, composite, value) in &rev {
                t.insert((db_id, *tid, composite.as_str()), value.as_slice())
                    .map_err(|e| redb_err("migrate_edges_v2 insert reverse", e))?;
            }
        }
        {
            let mut t = write_txn
                .open_table(GRAPH_STATS)
                .map_err(|e| redb_err("migrate_edges_v2 open graph_stats_v2", e))?;
            for (tid, key, value) in &stats {
                t.insert((db_id, *tid, key.as_str()), value.as_slice())
                    .map_err(|e| redb_err("migrate_edges_v2 insert stat", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("migrate_edges_v2 commit", e))
    }
}

/// Read every `(tid, key, value)` row out of a legacy `(u64, &str)` table.
/// An absent legacy table yields an empty vector (fresh boot).
fn collect_legacy(
    db: &Database,
    legacy: TableDefinition<(u64, &str), &[u8]>,
    ctx: &str,
) -> crate::Result<Vec<(u64, String, Vec<u8>)>> {
    let txn = db.begin_read().map_err(|e| redb_err(ctx, e))?;
    match txn.open_table(legacy) {
        Ok(table) => {
            let iter = table.iter().map_err(|e| redb_err(ctx, e))?;
            let mut out = Vec::new();
            for entry in iter {
                let (k, v) = entry.map_err(|e| redb_err(ctx, e))?;
                let (tid, key) = k.value();
                out.push((tid, key.to_string(), v.value().to_vec()));
            }
            Ok(out)
        }
        Err(_) => Ok(Vec::new()),
    }
}

/// Whether a database-scoped `(u64, u64, &str)` v2 table already has rows.
fn v2_nonempty(
    db: &Database,
    v2: TableDefinition<(u64, u64, &str), &[u8]>,
    ctx: &str,
) -> crate::Result<bool> {
    let txn = db.begin_read().map_err(|e| redb_err(ctx, e))?;
    match txn.open_table(v2) {
        Ok(table) => Ok(!table.is_empty().map_err(|e| redb_err(ctx, e))?),
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_tables_in_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let _ = EdgeStore::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let _a = EdgeStore::open(&path).unwrap();
        drop(_a);
        let _b = EdgeStore::open(&path).unwrap();
    }
}
