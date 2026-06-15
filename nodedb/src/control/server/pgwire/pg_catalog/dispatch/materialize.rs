// SPDX-License-Identifier: BUSL-1.1

//! Materialize each referenced catalog relation and combine them (via joins)
//! into a single relation for the executor. Also builds the catalog resolver
//! for `::regclass` / `::regtype`.

use std::collections::HashMap;

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::oid::stable_collection_oid;
use crate::control::server::pgwire::pg_catalog::tables::{self, collections};
use crate::control::server::pgwire::pg_catalog::vquery::expr::{EvalCtx, eval, truthy};
use crate::control::server::pgwire::pg_catalog::vquery::select::{FromClause, JoinKind, JoinSpec};
use crate::control::server::pgwire::pg_catalog::vquery::value::VValue;
use crate::control::server::pgwire::pg_catalog::vquery::{CatalogResolver, VTable};
use crate::control::server::pgwire::pg_catalog::{
    audit_log, dropped_collections, l2_cleanup_queue,
};
use crate::control::state::SharedState;

/// Well-known PostgreSQL OIDs for the catalog tables themselves, so
/// `'pg_class'::regclass` resolves the way clients expect.
const SYSTEM_REL_OIDS: &[(&str, i64)] = &[
    ("pg_class", 1259),
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_namespace", 2615),
    ("pg_index", 2610),
    ("pg_authid", 1260),
    ("pg_database", 1262),
    ("pg_proc", 1255),
];

/// Build the name→OID resolver from the well-known catalog OIDs, the visible
/// user collections, and the built-in type table.
pub fn build_resolver(state: &SharedState, identity: &AuthenticatedIdentity) -> CatalogResolver {
    let mut rel_oids: HashMap<String, i64> = SYSTEM_REL_OIDS
        .iter()
        .map(|&(n, o)| (n.to_string(), o))
        .collect();
    for coll in collections::load_collections(state, identity) {
        rel_oids.insert(
            coll.name.to_ascii_lowercase(),
            stable_collection_oid(coll.tenant_id, &coll.name),
        );
    }
    CatalogResolver::new(rel_oids, tables::pg_type::type_oid_map())
}

async fn materialize_named(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<VTable> {
    Ok(match name {
        "pg_database" => tables::pg_database()?,
        "pg_namespace" => tables::pg_namespace()?,
        "pg_type" => tables::pg_type()?,
        "pg_class" => tables::pg_class(state, identity)?,
        "pg_attribute" => tables::pg_attribute(state, identity)?,
        "pg_index" => tables::pg_index(state, identity)?,
        "pg_authid" => tables::pg_authid(state, identity)?,
        "_system.audit_log" => audit_log::audit_log(state, identity)?,
        "_system.dropped_collections" => {
            dropped_collections::dropped_collections(state, identity).await?
        }
        "_system.l2_cleanup_queue" => l2_cleanup_queue::l2_cleanup_queue(state, identity)?,
        other => {
            return Err(user_error(format!(
                "virtual table query: unknown catalog relation `{other}`"
            )));
        }
    })
}

/// Materialize every relation in `from`, qualify its columns with the relation
/// alias, and fold them into a single combined table via the declared joins.
/// A missing base (`from` with no relations) is a no-FROM scalar SELECT and
/// yields a single empty row.
pub async fn build_combined(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    from: &FromClause,
    ctx: &EvalCtx<'_>,
) -> PgWireResult<VTable> {
    let Some(base) = &from.base else {
        return Ok(VTable::single_empty_row());
    };
    let base_tbl = materialize_named(state, identity, &base.table).await?;
    let mut combined = VTable {
        columns: base_tbl.with_qualifier(&base.alias),
        rows: base_tbl.rows,
    };
    for join in &from.joins {
        let right = materialize_named(state, identity, &join.rel.table).await?;
        combined = join_tables(combined, right, join, ctx)?;
    }
    Ok(combined)
}

fn join_tables(
    left: VTable,
    right: VTable,
    join: &JoinSpec,
    ctx: &EvalCtx<'_>,
) -> PgWireResult<VTable> {
    let right_cols = right.with_qualifier(&join.rel.alias);
    let left_width = left.columns.len();
    let right_width = right_cols.len();

    let mut columns = left.columns;
    columns.extend(right_cols);
    let schema = VTable {
        columns,
        rows: Vec::new(),
    };

    let mut out_rows: Vec<Vec<VValue>> = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];

    for l in &left.rows {
        let mut l_matched = false;
        for (ri, r) in right.rows.iter().enumerate() {
            let mut row = l.clone();
            row.extend_from_slice(r);
            if join_matches(join, &row, &schema, ctx)? {
                out_rows.push(row);
                l_matched = true;
                right_matched[ri] = true;
            }
        }
        if !l_matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
            let mut row = l.clone();
            row.extend(std::iter::repeat_n(VValue::Null, right_width));
            out_rows.push(row);
        }
    }

    if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
        for (ri, r) in right.rows.iter().enumerate() {
            if !right_matched[ri] {
                let mut row = vec![VValue::Null; left_width];
                row.extend_from_slice(r);
                out_rows.push(row);
            }
        }
    }

    Ok(VTable {
        columns: schema.columns,
        rows: out_rows,
    })
}

fn join_matches(
    join: &JoinSpec,
    row: &[VValue],
    schema: &VTable,
    ctx: &EvalCtx<'_>,
) -> PgWireResult<bool> {
    match &join.on {
        None => Ok(true), // CROSS JOIN
        Some(on) => {
            let v = eval(on, row, schema, ctx)
                .map_err(|e| user_error(format!("virtual table query: join condition: {e}")))?;
            Ok(truthy(&v))
        }
    }
}

fn user_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        message,
    )))
}
