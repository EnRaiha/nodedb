// SPDX-License-Identifier: BUSL-1.1

//! Apply ApiKey catalog entries to `SystemCatalog` redb.

use tracing::debug;

use crate::control::security::catalog::{StoredApiKey, SystemCatalog, catalog_err};

pub fn put(stored: &StoredApiKey, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_api_key(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_api_key '{}' (user '{}')",
                stored.key_id, stored.username
            ),
            e,
        )
    })
}

/// Load the key, flip `is_revoked`, write it back. A missing record on a fresh
/// follower is a no-op, matching the user / collection drop pattern.
pub fn revoke(key_id: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    let existing = catalog
        .get_api_key(key_id)
        .map_err(|e| catalog_err(&format!("revoke_api_key read of '{key_id}'"), e))?;
    let Some(mut stored) = existing else {
        debug!(
            key_id = %key_id,
            "catalog_entry: revoke on missing api_key (fresh follower)"
        );
        return Ok(());
    };
    stored.is_revoked = true;
    catalog
        .put_api_key(&stored)
        .map_err(|e| catalog_err(&format!("revoke_api_key write of '{key_id}'"), e))
}
