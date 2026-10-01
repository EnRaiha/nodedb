// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree definition changes the metadata applier committed but the
//! permission cache has not taken yet.
//!
//! The applier runs synchronously and cannot take the cache's async lock, so
//! it queues each change here. The planning view and the lease coverage
//! apply the queue before they read the cache. Changes apply in commit
//! order.

use std::sync::Mutex;

use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::security::permission_tree::{
    PermissionCache, PermissionTreeDef, SourceIndex, TreeKey,
};

/// One committed change to a collection's tree definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeDefChange {
    Register {
        key: TreeKey,
        def: PermissionTreeDef,
    },
    Unregister {
        key: TreeKey,
    },
}

impl TreeDefChange {
    /// The change a committed collection descriptor of any database makes.
    /// An inactive collection governs nothing.
    pub fn from_collection(stored: &StoredCollection) -> crate::Result<Self> {
        let key = TreeKey::new(stored.database_id, stored.tenant_id, stored.name.clone());
        let def = match (&stored.permission_tree_def, stored.is_active) {
            (Some(json), true) => json,
            _ => return Ok(Self::Unregister { key }),
        };
        let def: PermissionTreeDef =
            sonic_rs::from_str(def).map_err(|e| crate::Error::Serialization {
                format: "json".into(),
                detail: format!("PERMISSION_TREE of collection '{}': {e}", key.collection),
            })?;
        Ok(Self::Register { key, def })
    }

    /// Record this committed change in the source index, ahead of the cache.
    pub fn note_committed(&self, sources: &SourceIndex) {
        match self {
            Self::Register { key, def } => sources.note_committed(key, Some(def)),
            Self::Unregister { key } => sources.note_committed(key, None),
        }
    }

    /// Apply this change to `cache`.
    pub fn apply(self, cache: &mut PermissionCache) {
        match self {
            Self::Register { key, def } => cache.register_tree_def(key, def),
            Self::Unregister { key } => cache.unregister_tree_def(&key),
        }
    }
}

/// A cache holding the tree definition of every collection `catalog` stores,
/// in every database. Boot builds the cache with this; the edges and grants
/// load once the data groups replayed.
pub fn cache_from_catalog(catalog: &SystemCatalog) -> crate::Result<PermissionCache> {
    let mut cache = PermissionCache::new();
    load_tree_defs(&mut cache, catalog)?;
    Ok(cache)
}

/// Apply the tree definition of every collection `catalog` stores, in every
/// database, to `cache` in place. The cache keeps its source index, so an
/// authorization fence built on that index sees every definition loaded.
pub fn load_tree_defs(cache: &mut PermissionCache, catalog: &SystemCatalog) -> crate::Result<()> {
    for stored in &catalog.load_all_collections_across_databases()? {
        TreeDefChange::from_collection(stored)?.apply(cache);
    }
    Ok(())
}

/// The queue of committed changes not yet in the cache.
#[derive(Debug, Default)]
pub struct PendingTreeDefs {
    changes: Mutex<Vec<TreeDefChange>>,
}

impl PendingTreeDefs {
    /// Queue a change the applier committed.
    pub fn push(&self, change: TreeDefChange) {
        self.changes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(change);
    }

    /// Whether a change waits.
    pub fn is_empty(&self) -> bool {
        self.changes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// Apply every queued change to `cache`. The caller holds the cache's
    /// write lock, so two callers cannot apply out of order.
    pub fn apply_to(&self, cache: &mut PermissionCache) {
        let changes = std::mem::take(&mut *self.changes.lock().unwrap_or_else(|p| p.into_inner()));
        for change in changes {
            change.apply(cache);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DatabaseId;

    fn def() -> PermissionTreeDef {
        sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def")
    }

    fn key(collection: &str) -> TreeKey {
        TreeKey::new(DatabaseId::DEFAULT, 1, collection)
    }

    #[test]
    fn queued_changes_apply_in_commit_order() {
        let pending = PendingTreeDefs::default();
        pending.push(TreeDefChange::Register {
            key: key("docs"),
            def: def(),
        });
        pending.push(TreeDefChange::Unregister { key: key("docs") });
        pending.push(TreeDefChange::Register {
            key: key("notes"),
            def: def(),
        });
        assert!(!pending.is_empty());

        let mut cache = PermissionCache::new();
        pending.apply_to(&mut cache);

        assert!(pending.is_empty());
        assert!(cache.get_tree_def(&key("docs")).is_none());
        assert_eq!(cache.get_tree_def(&key("notes")), Some(&def()));
    }

    /// A tree on a named database's collection registers under that
    /// database, never the default one. Boot and the metadata applier both
    /// read collections through this path.
    #[test]
    fn a_named_database_collection_registers_its_tree() {
        let db = DatabaseId::new(7);
        let mut stored = StoredCollection::new(1, "docs", "alice");
        stored.database_id = db;
        stored.is_active = true;
        stored.permission_tree_def = Some(sonic_rs::to_string(&def()).expect("serialize tree def"));
        let change = TreeDefChange::from_collection(&stored).expect("parse tree def");
        assert_eq!(
            change,
            TreeDefChange::Register {
                key: TreeKey::new(db, 1, "docs"),
                def: def(),
            }
        );

        let mut cache = PermissionCache::new();
        change.apply(&mut cache);
        assert!(cache.get_tree_def(&TreeKey::new(db, 1, "docs")).is_some());
        assert!(cache.get_tree_def(&key("docs")).is_none());
    }

    /// Boot registers the tree of a named database's collection under that
    /// database.
    #[test]
    fn boot_loads_trees_of_every_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog =
            SystemCatalog::open(&dir.path().join("system.redb")).expect("open system catalog");
        let db = DatabaseId::new(7);
        let json = sonic_rs::to_string(&def()).expect("serialize tree def");
        for (database_id, name) in [(db, "docs"), (DatabaseId::DEFAULT, "notes")] {
            let mut stored = StoredCollection::stamped_for_test(1, name, "alice");
            stored.database_id = database_id;
            stored.is_active = true;
            stored.permission_tree_def = Some(json.clone());
            catalog
                .put_collection(database_id, &stored)
                .expect("put collection");
        }

        let cache = cache_from_catalog(&catalog).expect("load tree defs");

        assert_eq!(
            cache.get_tree_def(&TreeKey::new(db, 1, "docs")),
            Some(&def())
        );
        assert!(cache.get_tree_def(&key("docs")).is_none());
        assert_eq!(cache.get_tree_def(&key("notes")), Some(&def()));
        assert!(cache.get_tree_def(&TreeKey::new(db, 1, "notes")).is_none());
    }
}
