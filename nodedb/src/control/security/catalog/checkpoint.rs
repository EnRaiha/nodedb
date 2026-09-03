// SPDX-License-Identifier: BUSL-1.1

//! Named checkpoint records persisted in the system catalog.
//!
//! Key format: `"{database_id}:{tenant_id}:{collection}:{doc_id}:{name}"`.
//!
//! The database segment scopes the row: `collection` and `doc_id` are both
//! database-relative, so a shared key lets one database read and drop the
//! checkpoints of another.

/// A named checkpoint: captures a version vector at a point in time.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct CheckpointRecord {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub doc_id: String,
    pub checkpoint_name: String,
    pub version_vector_json: String,
    pub created_by: String,
    pub created_at: u64,
}

impl CheckpointRecord {
    pub fn catalog_key(&self) -> String {
        checkpoint_key(
            self.database_id,
            self.tenant_id,
            &self.collection,
            &self.doc_id,
            &self.checkpoint_name,
        )
    }

    pub fn doc_prefix(database_id: u64, tenant_id: u64, collection: &str, doc_id: &str) -> String {
        format!("{database_id}:{tenant_id}:{collection}:{doc_id}:")
    }

    /// Exclusive upper bound for one document's key prefix.
    ///
    /// The prefix ends with `:`. The next byte after `:` is `;`, so this key
    /// sorts immediately past every checkpoint of the document.
    pub fn doc_upper_bound(
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        doc_id: &str,
    ) -> String {
        format!("{database_id}:{tenant_id}:{collection}:{doc_id};")
    }
}

fn checkpoint_key(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    checkpoint_name: &str,
) -> String {
    format!("{database_id}:{tenant_id}:{collection}:{doc_id}:{checkpoint_name}")
}

/// Key of one checkpoint row, for callers that hold the parts and not a record.
pub(super) fn key_of(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    checkpoint_name: &str,
) -> String {
    checkpoint_key(database_id, tenant_id, collection, doc_id, checkpoint_name)
}
