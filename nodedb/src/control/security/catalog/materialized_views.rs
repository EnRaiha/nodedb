// SPDX-License-Identifier: BUSL-1.1

//! Materialized view metadata operations for the system catalog.

use super::types::{MATERIALIZED_VIEWS, StoredMaterializedView, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

impl SystemCatalog {
    /// Store a materialized view record.
    ///
    /// The key comes from the record, so the row can never land under a
    /// database the record does not name.
    pub fn put_materialized_view(&self, view: &StoredMaterializedView) -> crate::Result<()> {
        let key = view_key(view.database_id, view.tenant_id, &view.name);
        let bytes = zerompk::to_msgpack_vec(view)
            .map_err(|e| catalog_err("serialize materialized view", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(MATERIALIZED_VIEWS)
                .map_err(|e| catalog_err("open materialized_views", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert materialized_view", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a materialized view by name, with the calling connection's
    /// buffered transactional DDL merged in — a `CREATE MATERIALIZED VIEW`
    /// this same transaction has buffered but not yet committed resolves
    /// here too.
    pub fn get_materialized_view(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredMaterializedView>> {
        let committed = self.get_committed_materialized_view(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_materialized_view(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// Committed-only read, bypassing the transaction DDL overlay. The
    /// descriptor stamper reads through this — see
    /// `get_committed_collection` for why.
    pub fn get_committed_materialized_view(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredMaterializedView>> {
        let key = view_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(MATERIALIZED_VIEWS)
            .map_err(|e| catalog_err("open materialized_views", e))?;
        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes = guard.value();
                let view: StoredMaterializedView = zerompk::from_msgpack(bytes)
                    .map_err(|e| catalog_err("deserialize materialized view", e))?;
                Ok(Some(view))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get materialized_view", e)),
        }
    }

    /// Load every materialized view across all databases and tenants. Used by
    /// the startup integrity check and any cross-tenant audit.
    pub fn load_all_materialized_views(&self) -> crate::Result<Vec<StoredMaterializedView>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(MATERIALIZED_VIEWS)
            .map_err(|e| catalog_err("open materialized_views", e))?;
        let mut views = Vec::new();
        for entry in table.range(..).map_err(|e| catalog_err("range scan", e))? {
            let (_key, val) = entry.map_err(|e| catalog_err("read entry", e))?;
            let view: StoredMaterializedView = zerompk::from_msgpack(val.value())
                .map_err(|e| catalog_err("deser materialized_view", e))?;
            views.push(view);
        }
        Ok(views)
    }

    /// List every materialized view of one tenant in one database.
    ///
    /// The scan is bounded to the tenant's key range, so a node holding many
    /// databases reads only the rows it returns.
    pub fn list_materialized_views(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredMaterializedView>> {
        self.range_materialized_views(
            &format!("{database_id}:{tenant_id}:"),
            &tenant_upper_bound(database_id, tenant_id),
        )
    }

    /// List every materialized view of one database, across every tenant.
    pub fn list_materialized_views_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredMaterializedView>> {
        self.range_materialized_views(
            &format!("{database_id}:"),
            &database_upper_bound(database_id),
        )
    }

    /// Delete a materialized view by name.
    pub fn delete_materialized_view(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<()> {
        let key = view_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(MATERIALIZED_VIEWS)
                .map_err(|e| catalog_err("open materialized_views", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete materialized_view", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Decode every row in one key range.
    fn range_materialized_views(
        &self,
        lower: &str,
        upper: &str,
    ) -> crate::Result<Vec<StoredMaterializedView>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(MATERIALIZED_VIEWS)
            .map_err(|e| catalog_err("open materialized_views", e))?;
        let mut views = Vec::new();
        for entry in table
            .range(lower..upper)
            .map_err(|e| catalog_err("range scan", e))?
        {
            let (_key, val) = entry.map_err(|e| catalog_err("read entry", e))?;
            let view: StoredMaterializedView = zerompk::from_msgpack(val.value())
                .map_err(|e| catalog_err("deser materialized_view", e))?;
            views.push(view);
        }
        Ok(views)
    }
}

fn view_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

/// Exclusive upper bound for one database's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every tenant of the database.
fn database_upper_bound(database_id: u64) -> String {
    format!("{database_id};")
}

/// Exclusive upper bound for one tenant's key prefix.
fn tenant_upper_bound(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id};")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn view(database_id: u64, name: &str, source: &str) -> StoredMaterializedView {
        StoredMaterializedView {
            database_id,
            tenant_id: 1,
            name: name.into(),
            source: source.into(),
            query_sql: format!("SELECT * FROM {source}"),
            refresh_mode: "auto".into(),
            owner: "alice".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    /// Two databases of one tenant hold a same-named view. A delete in one
    /// leaves the other's row standing.
    #[test]
    fn views_of_one_database_survive_a_delete_in_another() {
        let (_dir, cat) = make_catalog();
        cat.put_materialized_view(&view(2, "mv_orders", "orders"))
            .unwrap();
        cat.put_materialized_view(&view(3, "mv_orders", "sales"))
            .unwrap();

        cat.delete_materialized_view(3, 1, "mv_orders").unwrap();

        let kept = cat
            .get_committed_materialized_view(2, 1, "mv_orders")
            .unwrap()
            .expect("the key is scoped by database");
        assert_eq!(kept.source, "orders");
        assert!(
            cat.get_committed_materialized_view(3, 1, "mv_orders")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn listing_a_tenant_excludes_another_database() {
        let (_dir, cat) = make_catalog();
        cat.put_materialized_view(&view(2, "mv_a", "orders"))
            .unwrap();
        cat.put_materialized_view(&view(2, "mv_b", "orders"))
            .unwrap();
        cat.put_materialized_view(&view(3, "mv_c", "orders"))
            .unwrap();

        assert_eq!(cat.list_materialized_views(2, 1).unwrap().len(), 2);
        assert_eq!(cat.list_materialized_views(3, 1).unwrap().len(), 1);
        assert_eq!(cat.load_all_materialized_views().unwrap().len(), 3);
    }

    #[test]
    fn listing_a_database_excludes_a_tenant_sharing_an_id_prefix() {
        let (_dir, cat) = make_catalog();
        let mut other_tenant = view(2, "mv_a", "orders");
        other_tenant.tenant_id = 11;
        cat.put_materialized_view(&view(2, "mv_a", "orders"))
            .unwrap();
        cat.put_materialized_view(&other_tenant).unwrap();

        assert_eq!(cat.list_materialized_views(2, 1).unwrap().len(), 1);
        assert_eq!(cat.list_materialized_views_in_database(2).unwrap().len(), 2);
    }
}
