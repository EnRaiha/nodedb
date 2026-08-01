// SPDX-License-Identifier: BUSL-1.1

//! Apply streaming materialized-view catalog entries.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::event::streaming_mv::StreamingMvDef;
use crate::types::DatabaseId;

pub fn put(definition: &StreamingMvDef, catalog: &SystemCatalog) {
    if let Err(error) = catalog.put_streaming_mv(definition) {
        tracing::warn!(
            database = definition.database_id.as_u64(),
            tenant = definition.tenant_id,
            view = %definition.name,
            %error,
            "catalog_entry: put_streaming_materialized_view failed"
        );
    }
    super::owner::put(
        &StoredOwner {
            database_id: definition.database_id.as_u64(),
            object_type: object_type::STREAMING_MATERIALIZED_VIEW.to_string(),
            object_name: definition.name.clone(),
            tenant_id: definition.tenant_id,
            owner_username: definition.owner.clone(),
        },
        catalog,
    );
}

pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog.delete_streaming_mv(DatabaseId::new(database_id), tenant_id, name)?;
    super::owner::delete_parent_owner_in_database_checked(
        object_type::STREAMING_MATERIALIZED_VIEW,
        database_id,
        tenant_id,
        name,
        catalog,
    )?;
    Ok(())
}
