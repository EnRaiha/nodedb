// SPDX-License-Identifier: BUSL-1.1

//! Materialized view metadata operations for the system catalog.

use super::types::{MATERIALIZED_VIEWS, StoredMaterializedView, SystemCatalog, catalog_err};
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Store a materialized view record.
    pub fn put_materialized_view(&self, view: &StoredMaterializedView) -> crate::Result<()> {
        let key = format!("{}:{}", view.tenant_id, view.name);
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

    /// Get a materialized view by name.
    pub fn get_materialized_view(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredMaterializedView>> {
        let key = format!("{tenant_id}:{name}");
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

    /// Load every materialized view across all tenants. Used by
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
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range scan", e))?
        {
            let (_key, val) = entry.map_err(|e| catalog_err("read entry", e))?;
            let view: StoredMaterializedView = zerompk::from_msgpack(val.value())
                .map_err(|e| catalog_err("deser materialized_view", e))?;
            views.push(view);
        }
        Ok(views)
    }

    /// List all materialized views for a tenant.
    pub fn list_materialized_views(
        &self,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredMaterializedView>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(MATERIALIZED_VIEWS)
            .map_err(|e| catalog_err("open materialized_views", e))?;
        let mut views = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range scan", e))?
        {
            let (key, val) = entry.map_err(|e| catalog_err("read entry", e))?;
            if key.value().starts_with(&prefix)
                && let Ok(view) = zerompk::from_msgpack::<StoredMaterializedView>(val.value())
            {
                views.push(view);
            }
        }
        Ok(views)
    }

    /// Delete a materialized view by name.
    pub fn delete_materialized_view(&self, tenant_id: u64, name: &str) -> crate::Result<()> {
        let key = format!("{tenant_id}:{name}");
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
}
