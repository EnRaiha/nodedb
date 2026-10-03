// SPDX-License-Identifier: BUSL-1.1

//! Keys of the permission-tree state.
//!
//! A tree governs one collection of one tenant in one database. Its
//! permission table and hierarchy rows live in that same database, so the
//! resource hierarchy and the grants are held per `(database, tenant)`.

use nodedb_types::id::VShardId;
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

/// The resource hierarchy and grants of one tenant in one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TreeScope {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
}

impl TreeScope {
    pub fn new(database_id: DatabaseId, tenant_id: u64) -> Self {
        Self {
            database_id,
            tenant_id,
        }
    }

    /// The key of `collection`, a bare catalog name in this scope.
    pub fn collection(self, collection: impl Into<String>) -> TreeKey {
        TreeKey {
            scope: self,
            collection: collection.into(),
        }
    }
}

/// One collection in a [`TreeScope`], by its bare catalog name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeKey {
    pub scope: TreeScope,
    pub collection: String,
}

impl TreeKey {
    pub fn new(database_id: DatabaseId, tenant_id: u64, collection: impl Into<String>) -> Self {
        TreeScope::new(database_id, tenant_id).collection(collection)
    }

    /// The key of `qualified`, a database-qualified name a plan or a write
    /// event carries for `database_id`.
    pub fn from_qualified(
        database_id: DatabaseId,
        tenant_id: u64,
        qualified: &str,
    ) -> crate::Result<Self> {
        let key = CollectionKey::from_qualified_str(database_id, qualified)?;
        Ok(Self::new(database_id, tenant_id, key.name()))
    }

    /// The name storage engines and plans key this collection by.
    pub fn qualified(&self) -> QualifiedCollection {
        QualifiedCollection::new(self.scope.database_id, &self.collection)
    }

    /// The vShard this collection homes to.
    pub fn vshard(&self) -> VShardId {
        CollectionKey::from_bare(self.scope.database_id, &self.collection).vshard()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_qualified_name_resolves_to_its_database_scope() {
        let key = TreeKey::from_qualified(DatabaseId::new(7), 1, "7/docs").expect("qualified");
        assert_eq!(key, TreeKey::new(DatabaseId::new(7), 1, "docs"));
        assert_eq!(key.qualified().as_str(), "7/docs");

        let key = TreeKey::from_qualified(DatabaseId::DEFAULT, 1, "docs").expect("bare");
        assert_eq!(key, TreeKey::new(DatabaseId::DEFAULT, 1, "docs"));
    }

    #[test]
    fn a_name_qualified_for_another_database_is_rejected() {
        assert!(TreeKey::from_qualified(DatabaseId::new(7), 1, "8/docs").is_err());
        assert!(TreeKey::from_qualified(DatabaseId::new(7), 1, "docs").is_err());
    }

    #[test]
    fn the_same_name_in_two_databases_is_two_keys() {
        let a = TreeKey::new(DatabaseId::new(7), 1, "docs");
        let b = TreeKey::new(DatabaseId::new(8), 1, "docs");
        assert_ne!(a, b);
        assert_ne!(a.qualified(), b.qualified());
    }
}
