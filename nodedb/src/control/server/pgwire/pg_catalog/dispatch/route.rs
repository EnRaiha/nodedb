// SPDX-License-Identifier: BUSL-1.1

//! pg_catalog query interception, routing decision, and evaluation entry points.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::vquery::select::VSelect;
use crate::control::server::pgwire::pg_catalog::vquery::{
    EvalCtx, encode, execute, parse_select_with_params,
};
use crate::control::state::SharedState;

use super::materialize::{build_combined, build_resolver};
use super::schema::{extract_pg_catalog_table, known_table};

/// Try to handle a SQL query as a pg_catalog virtual-table lookup. Returns
/// `Some(..)` if the query targets the catalog surface, `None` if it should
/// fall through to the normal planner.
pub async fn try_pg_catalog(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    try_pg_catalog_with_params(state, identity, sql, &[]).await
}

/// Same as [`try_pg_catalog`] but binds prepared-statement parameters into the
/// SQL before evaluation.
pub async fn try_pg_catalog_with_params(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    params: &[nodedb_sql::ParamValue],
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = sql.to_ascii_uppercase();
    let mentions_table = extract_pg_catalog_table(&upper).is_some();
    if !mentions_table && !references_scalar_feature(&upper) {
        return None;
    }

    let select = match parse_select_with_params(sql, params) {
        Ok(s) => s,
        // Only claim (and report errors for) a query that clearly targets a
        // known catalog relation; otherwise let the planner try.
        Err(e) if mentions_table => return Some(Err(catalog_error(&e.to_string()))),
        Err(_) => return None,
    };

    // Every relation in the FROM clause must be a known virtual table, else
    // this isn't ours (e.g. a join against a real user table).
    let relations = select.from.relations();
    for rel in &relations {
        known_table(&rel.table)?;
    }
    // A no-FROM SELECT is only ours if it uses a catalog cast/function.
    if relations.is_empty() && !references_scalar_feature(&upper) {
        return None;
    }

    Some(evaluate(state, identity, &select).await)
}

async fn evaluate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    select: &VSelect,
) -> PgWireResult<Vec<Response>> {
    let resolver = build_resolver(state, identity);
    let search_path = ["public".to_string()];
    let ctx = EvalCtx {
        resolver: &resolver,
        username: &identity.username,
        database: "nodedb",
        search_path: &search_path,
    };

    let combined = build_combined(state, identity, &select.from, &ctx).await?;
    let result = execute(select, combined, &ctx).map_err(|e| catalog_error(&e.to_string()))?;
    encode::encode(result)
}

/// True if the SQL uses a catalog cast or scalar function the evaluator owns —
/// used to claim no-FROM scalar selects like `SELECT 'x'::regclass::oid`.
fn references_scalar_feature(upper: &str) -> bool {
    const MARKERS: &[&str] = &[
        "::REGCLASS",
        "::REGTYPE",
        "REGCLASS",
        "REGTYPE",
        "CURRENT_SCHEMA",
        "CURRENT_DATABASE",
        "CURRENT_USER",
        "CURRENT_ROLE",
        "VERSION(",
    ];
    MARKERS.iter().any(|m| upper.contains(m))
}

fn catalog_error(detail: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!("virtual table query: {detail}"),
    )))
}
