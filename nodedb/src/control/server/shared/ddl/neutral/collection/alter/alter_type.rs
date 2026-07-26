// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION <name> ALTER COLUMN <col> TYPE <type>` — change a
//! column's declared type in a strict-document collection's schema.
//!
//! Ported verbatim from the pgwire `ddl::collection::alter::alter_type`
//! handler; only the result type changed to the protocol-neutral
//! [`DdlResult`] / [`DdlError`]. The same-discriminant gate (rejecting a
//! true type change that would require re-encoding), version bump, persist,
//! and audit are unchanged, as is the `ALTER COLLECTION` command tag.

use std::str::FromStr;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::strict_schema::{
    load_strict_collection, persist_schema_change, retype_field, write_schema_back,
};
use super::support::{err, status};

/// ALTER COLLECTION <name> ALTER COLUMN <column_name> TYPE <new_type>
pub(super) async fn alter_collection_alter_column_type(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    column_name: &str,
    new_type_str: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let new_type = nodedb_types::columnar::ColumnType::from_str(new_type_str)
        .map_err(|e| err("42601", format!("invalid type '{new_type_str}': {e}")))?;

    let (coll, mut schema) =
        load_strict_collection(state, tenant_id.as_u64(), name, "ALTER COLUMN TYPE")?;

    let col = schema
        .columns
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            err(
                "42703",
                format!("column '{column_name}' does not exist on '{name}'"),
            )
        })?;

    // Reject a true type change that would require re-encoding existing rows.
    if std::mem::discriminant(&col.column_type) != std::mem::discriminant(&new_type) {
        return Err(err(
            "0A000",
            format!(
                "cross-type change from {:?} to {:?} requires an online rewrite; \
                 only alias type changes (e.g. INT ↔ BIGINT) are supported today",
                col.column_type, new_type
            ),
        ));
    }
    col.column_type = new_type;
    schema.version = schema.version.saturating_add(1);

    let mut updated = coll;
    write_schema_back(&mut updated, schema);
    // The declared spelling, not the resolved `ColumnType`, is what carries
    // the integer width — see `retype_field`.
    retype_field(&mut updated, column_name, new_type_str);
    persist_schema_change(state, &updated).await?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER COLLECTION '{name}' ALTER COLUMN '{column_name}' TYPE {new_type_str}"),
    );

    Ok(status("ALTER COLLECTION"))
}
