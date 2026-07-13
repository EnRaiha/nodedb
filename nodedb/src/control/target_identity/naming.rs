// SPDX-License-Identifier: BUSL-1.1

//! De-qualify a db-qualified collection name to the bare name the catalog
//! keys collections by.

use nodedb_types::DatabaseId;

/// Strip the `{database_id}/` qualifier from a db-qualified collection name
/// to recover the bare name the catalog keys collections by (the DEFAULT
/// database uses the bare name unqualified).
pub(crate) fn bare_collection_name(database_id: DatabaseId, qualified: &str) -> String {
    if database_id == DatabaseId::DEFAULT {
        return qualified.to_string();
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified
        .strip_prefix(&prefix)
        .unwrap_or(qualified)
        .to_string()
}
