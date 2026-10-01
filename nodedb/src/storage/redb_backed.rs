// SPDX-License-Identifier: BUSL-1.1

//! Access to the redb database behind a durable store.

/// A store kept in one redb database.
///
/// A physical base snapshot reads each store's image through this trait.
pub trait RedbBacked {
    /// The redb database that holds this store.
    fn redb_database(&self) -> &redb::Database;
}

impl RedbBacked for redb::Database {
    fn redb_database(&self) -> &redb::Database {
        self
    }
}

impl RedbBacked for nodedb_cluster::ClusterCatalog {
    fn redb_database(&self) -> &redb::Database {
        self.database()
    }
}
