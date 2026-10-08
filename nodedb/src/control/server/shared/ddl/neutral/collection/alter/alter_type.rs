// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION <name> ALTER COLUMN <col> TYPE <type>` — change a
//! column's declared type in a strict-document collection's schema.
//!
//! The result type is the protocol-neutral [`DdlResult`] / [`DdlError`]. The
//! full-equality gate rejects any type change that requires re-encoding
//! existing rows, including a parameter change (`VECTOR(384)` to
//! `VECTOR(768)`) that shares a discriminant with the current type. Version
//! bump, persist, and audit run here, and the command tag is
//! `ALTER COLLECTION`.

use std::str::FromStr;

use nodedb_types::DatabaseId;

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
    database_id: DatabaseId,
    name: &str,
    column_name: &str,
    new_type_str: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let new_type = nodedb_types::columnar::ColumnType::from_str(new_type_str)
        .map_err(|e| err(e.sqlstate(), format!("invalid type '{new_type_str}': {e}")))?;

    let (coll, mut schema) = load_strict_collection(
        state,
        database_id,
        tenant_id.as_u64(),
        name,
        "ALTER COLUMN TYPE",
    )?;

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

    // Reject a type change that needs every stored row re-encoded.
    //
    // An alias change resolves to the identical `ColumnType`: every integer
    // width parses to `Int64`, and only the declared spelling differs. A
    // parameter change — `VECTOR(384)` to `VECTOR(768)`, `DECIMAL(10,2)` to
    // `DECIMAL(28,10)` — shares the discriminant but rewrites every stored
    // value, so full equality is the gate.
    if col.column_type != new_type {
        return Err(err(
            "0A000",
            format!(
                "type change from {:?} to {:?} requires an online rewrite; \
                 only alias type changes (e.g. INT ↔ BIGINT) are supported today",
                col.column_type, new_type
            ),
        ));
    }
    // An alias change moves the declared numeric width. A narrower width
    // bounds values that existing rows can already exceed, so it needs every
    // stored row checked. Only an equal or wider width is accepted.
    let retyped = col.clone().with_declared_width(new_type_str);
    if narrows_declared_width(col, &retyped) {
        return Err(err(
            "0A000",
            format!(
                "type change from {} to {} narrows the column and requires every \
                 stored row checked; only a change to an equal or wider type is supported",
                col.declared_type_name(),
                retyped.declared_type_name()
            ),
        ));
    }
    *col = retyped;
    schema.version = schema.version.saturating_add(1);

    let mut updated = coll;
    write_schema_back(&mut updated, schema);
    // The catalog keeps the declared spelling the schema width came from.
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

/// Whether `to` declares a narrower integer or float width than `from`. An
/// absent width is the widest of its family.
fn narrows_declared_width(
    from: &nodedb_types::columnar::ColumnDef,
    to: &nodedb_types::columnar::ColumnDef,
) -> bool {
    use nodedb_types::columnar::{FloatWidth, IntWidth};
    let int = |c: &nodedb_types::columnar::ColumnDef| c.int_width.unwrap_or(IntWidth::I64);
    let float = |c: &nodedb_types::columnar::ColumnDef| c.float_width.unwrap_or(FloatWidth::F64);
    int(to) < int(from) || float(to) < float(from)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    use super::narrows_declared_width;

    fn int(declared: &str) -> ColumnDef {
        ColumnDef::nullable("n", ColumnType::Int64).with_declared_width(declared)
    }

    fn float(declared: &str) -> ColumnDef {
        ColumnDef::nullable("f", ColumnType::Float64).with_declared_width(declared)
    }

    #[test]
    fn only_a_narrower_width_is_a_narrowing() {
        assert!(narrows_declared_width(&int("BIGINT"), &int("SMALLINT")));
        assert!(narrows_declared_width(&int("INT"), &int("INT2")));
        assert!(narrows_declared_width(&float("DOUBLE"), &float("REAL")));
        assert!(!narrows_declared_width(&int("SMALLINT"), &int("BIGINT")));
        assert!(!narrows_declared_width(&int("INT"), &int("INTEGER")));
        assert!(!narrows_declared_width(&float("REAL"), &float("FLOAT8")));
    }
}
