// SPDX-License-Identifier: BUSL-1.1

//! NodeDbQueryParser — pgwire `QueryParser` implementation.
//!
//! Converts incoming SQL (from a Parse message) into a `ParsedStatement`
//! with inferred parameter types and result schema. Uses nodedb-sql for
//! schema resolution instead of DataFusion.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use crate::control::state::SharedState;

use super::statement::ParsedStatement;
use parser_schema::{
    count_placeholders, fields_from_projection, infer_result_fields, is_dsl_statement,
    parse_select_projection, result_fields_for_returning, substitute_placeholders_with_null,
};

#[path = "parser_schema.rs"]
mod parser_schema;

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via sqlparser, extracts placeholder types
/// from the catalog schema, and computes the result schema.
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
}

impl NodeDbQueryParser {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Infer parameter and result types using nodedb-sql catalog, scoped to
    /// the connecting user's tenant so a tenant-N user's Parse message
    /// resolves against tenant-N's catalog (not tenant 1).
    fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
        tenant_id: u64,
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        let catalog = crate::control::planner::catalog_adapter::OriginCatalog::new(
            Arc::clone(&self.state.credentials),
            tenant_id,
            crate::types::DatabaseId::DEFAULT,
            Some(Arc::clone(&self.state.retention_policy_registry)),
        );

        // Placeholder inference runs unconditionally so an unplannable
        // SQL string (e.g. `WHERE id = $1` where the planner needs bound
        // params to typecheck) still reports the right number of
        // parameter slots in Describe.
        let param_count = count_placeholders(sql);
        let mut param_types = vec![None; param_count.max(client_types.len())];
        for (i, ct) in client_types.iter().enumerate() {
            if let Some(t) = ct {
                param_types[i] = Some(t.clone());
            }
        }

        // Strip RETURNING from DML before passing to DataFusion. Retain the
        // parsed spec so we can build result fields for Describe.
        let (sql_stripped, returning_spec) =
            match crate::control::server::pgwire::handler::returning::strip_returning(sql) {
                Ok(pair) => pair,
                Err(_) => return (param_types, Vec::new()),
            };

        // Parse and plan to get collection info for result schema.
        //
        // The planner type-checks WHERE/projection expressions, which
        // fails on raw `$N` placeholders (no bound value to typecheck).
        // For schema inference we only need the collection + projection
        // structure, so substitute placeholders with NULL literals just
        // for this planning pass. Execution re-plans with real bound
        // values.
        let sql_for_inference = substitute_placeholders_with_null(&sql_stripped);
        let plans = match nodedb_sql::plan_sql(&sql_for_inference, &catalog) {
            Ok(p) => p,
            Err(_) => return (param_types, Vec::new()),
        };

        // When the original SQL had a RETURNING clause on a DML statement,
        // build result fields from the collection schema and the RETURNING spec.
        if let Some(spec) = returning_spec
            && let Some(fields) = result_fields_for_returning(&spec, plans.first(), &catalog)
        {
            return (param_types, fields);
        }

        // Infer result fields.
        //
        // Prefer the explicit SELECT list from sqlparser: the planner
        // drops it for PointGet/PointLookup variants, but the projection
        // the client sees must match what they wrote in the SQL. When
        // the list is `*` or missing, fall back to the plan's collection
        // schema.
        let result_fields = if let Some(projection) = parse_select_projection(&sql_for_inference) {
            fields_from_projection(&projection, plans.first(), &catalog)
        } else if let Some(plan) = plans.first() {
            infer_result_fields(plan, &catalog)
        } else {
            Vec::new()
        };

        (param_types, result_fields)
    }
}

#[async_trait]
impl QueryParser for NodeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Wire-streaming COPY shapes for backup/restore: bypass nodedb-sql
        // entirely. The Execute handler intercepts these via
        // `control::backup::detect`. Returning early avoids a fruitless
        // sqlparser pass on syntax it doesn't model.
        if crate::control::backup::detect(sql).is_some() {
            return Ok(ParsedStatement {
                sql: sql.to_owned(),
                param_types: Vec::new(),
                result_fields: Vec::new(),
                is_dsl: false,
                pg_catalog_table: None,
            });
        }

        // pg_catalog virtual tables: bypass the planner entirely — they
        // aren't real collections. Populate result_fields from the static
        // catalog schema so Describe can report column types before Bind.
        let upper = sql.to_uppercase();
        if let Some(table) =
            crate::control::server::pgwire::pg_catalog::extract_pg_catalog_table(&upper)
        {
            // Parse the SELECT now so Describe can report the *projected*
            // column schema rather than the full table schema. The
            // evaluator-produced result must match what Describe announces
            // — drivers (tokio-postgres, JDBC) decode the wire response
            // against the Describe shape and will panic on column-count
            // mismatch. Fall back to the full schema if parsing fails so
            // drivers that probe with malformed-but-permissive queries still
            // get a usable Describe.
            let result_fields =
                crate::control::server::pgwire::pg_catalog::pg_catalog_projected_schema(sql)
                    .or_else(|| {
                        crate::control::server::pgwire::pg_catalog::pg_catalog_schema(table)
                    })
                    .unwrap_or_default();
            let count = count_placeholders(sql).max(types.len());
            let param_types: Vec<Option<Type>> = (0..count)
                .map(|i| types.get(i).and_then(|t| t.clone()))
                .collect();
            return Ok(ParsedStatement {
                sql: sql.to_owned(),
                param_types,
                result_fields,
                is_dsl: false,
                pg_catalog_table: Some(table),
            });
        }

        // Resolve the connecting user's tenant from pgwire metadata so
        // parse-time catalog lookups are scoped to the right tenant.
        // Unknown users fall back to tenant 1 only during bootstrap
        // (credential store empty) — otherwise parse-time inference
        // returns empty field info, which is the safe default.
        let tenant_id = client
            .metadata()
            .get("user")
            .and_then(|u| {
                self.state
                    .credentials
                    .to_identity(u, crate::control::security::identity::AuthMethod::Trust)
                    .or_else(|| {
                        self.state.credentials.to_identity(
                            u,
                            crate::control::security::identity::AuthMethod::ScramSha256,
                        )
                    })
            })
            .map(|id| id.tenant_id.as_u64())
            .unwrap_or(1);
        let (param_types, result_fields) = self.try_infer_types(sql, types, tenant_id);

        // If type inference produced no result fields and the SQL matches a
        // known DSL prefix, mark the statement as a DSL passthrough. The
        // Execute handler will route it through the full DSL dispatcher
        // (same as the simple-query path) instead of `execute_planned_sql_with_params`.
        let is_dsl = result_fields.is_empty() && is_dsl_statement(sql);

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
            is_dsl,
            pg_catalog_table: None,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt
            .param_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_fields.clone())
    }
}
