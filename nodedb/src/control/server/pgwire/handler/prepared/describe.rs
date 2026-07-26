// SPDX-License-Identifier: BUSL-1.1

//! Describe overrides for prepared statements and portals.
//!
//! Provides real parameter types and result schemas instead of the
//! pgwire default (which delegates to QueryParser methods).

use pgwire::api::portal::Portal;
use pgwire::api::results::{DescribePortalResponse, DescribeResponse, DescribeStatementResponse};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use super::super::core::NodeDbPgHandler;
use super::statement::ParsedStatement;

impl NodeDbPgHandler {
    /// Describe a prepared statement: return parameter types + result columns.
    ///
    /// Called by pgwire when a Describe('S') message arrives. Returns
    /// ParameterDescription + RowDescription (or NoData for DML).
    pub(crate) async fn describe_statement_impl<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<ParsedStatement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = &target.statement;

        // Merge client-provided types with server-inferred types.
        // Client types (from Parse message) take precedence.
        let max_len = stmt.param_types.len().max(target.parameter_types.len());
        let param_types: Vec<Type> = (0..max_len)
            .map(|i| {
                // Client-provided type from the Parse message.
                let client = target.parameter_types.get(i).and_then(|t| t.clone());
                // Server-side type, resolved at Parse time from the SQL text
                // (see `NodeDbQueryParser::fill_inferred_param_types`).
                let server = stmt.param_types.get(i).and_then(|t| t.clone());
                client.or(server).unwrap_or(Type::UNKNOWN)
            })
            .collect();

        if stmt.result_fields.is_empty() {
            // DML statement (INSERT/UPDATE/DELETE) — no result columns.
            Ok(DescribeStatementResponse::new(param_types, vec![]))
        } else {
            Ok(DescribeStatementResponse::new(
                param_types,
                stmt.result_fields.clone(),
            ))
        }
    }

    /// Describe a portal: return result columns.
    ///
    /// Called by pgwire when a Describe('P') message arrives after Bind.
    /// At this point parameters are bound, so we can return the full result schema.
    pub(crate) async fn describe_portal_impl<C>(
        &self,
        _client: &mut C,
        target: &Portal<ParsedStatement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = &target.statement.statement;

        if stmt.result_fields.is_empty() {
            return Ok(DescribePortalResponse::no_data());
        }

        // The portal is bound, so the client's requested result-column formats
        // are known. Stamp the resolved (feature-downgraded) formats onto the
        // RowDescription so Describe advertises exactly what Execute encodes.
        let formats = super::result_format::resolve_result_formats(
            &stmt.result_fields,
            &target.result_column_format,
        );
        let fields = super::result_format::stamp_formats(&stmt.result_fields, &formats);
        Ok(DescribePortalResponse::new(fields))
    }
}
