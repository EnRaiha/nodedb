// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION <name> DROP COLUMN <col>` — remove a column from a
//! strict-document collection's schema.
//!
//! Ported verbatim from the pgwire `ddl::collection::alter::drop_column`
//! handler; only the result type changed to the protocol-neutral
//! [`DdlResult`] / [`DdlError`]. The dropped-column bookkeeping
//! (`dropped_columns` push + version bump), primary-key guard, persist,
//! and audit are unchanged, as is the `ALTER COLLECTION` command tag.

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::strict_schema::{
    load_strict_collection, persist_schema_change, remove_field, write_schema_back,
};
use super::support::{err, status};

/// ALTER COLLECTION <name> DROP COLUMN <column_name>
pub(super) async fn alter_collection_drop_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    column_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let (coll, mut schema) =
        load_strict_collection(state, tenant_id.as_u64(), name, "DROP COLUMN")?;

    let idx = schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            err(
                "42703",
                format!("column '{column_name}' does not exist on '{name}'"),
            )
        })?;

    if schema.columns[idx].primary_key {
        return Err(err(
            "42601",
            format!("cannot drop primary key column '{column_name}'"),
        ));
    }

    let dropped_def = schema.columns.remove(idx);
    let new_version = schema.version.saturating_add(1);
    schema
        .dropped_columns
        .push(nodedb_types::columnar::DroppedColumn {
            def: dropped_def,
            position: idx,
            dropped_at_version: new_version,
        });
    schema.version = new_version;

    let mut updated = coll;
    write_schema_back(&mut updated, schema);
    remove_field(&mut updated, column_name);
    persist_schema_change(state, &updated).await?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER COLLECTION '{name}' DROP COLUMN '{column_name}'"),
    );

    Ok(status("ALTER COLLECTION"))
}
