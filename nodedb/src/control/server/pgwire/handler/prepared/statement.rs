// SPDX-License-Identifier: BUSL-1.1

//! ParsedStatement — the result of parsing SQL in the extended query protocol.
//!
//! Stored in pgwire's `StoredStatement<ParsedStatement>` after a Parse message.
//! Contains the original SQL text and pre-inferred parameter/result types.

use pgwire::api::results::FieldInfo;

/// A parsed SQL statement for the extended query protocol.
///
/// Created by `NodeDbQueryParser::parse_sql` during a Parse message.
/// The pgwire crate stores this as `StoredStatement<ParsedStatement>`.
/// On Bind + Execute, we re-plan the SQL with bound parameter values.
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    /// Original SQL text (may contain `$1`, `$2` placeholders).
    pub sql: String,
    /// Resolved parameter types, indexed by position: `param_types[0]` is
    /// the type of `$1`. A slot holds the type the client declared in its
    /// Parse message when it declared one, otherwise the type inferred from
    /// the SQL text by `nodedb_sql::infer_placeholder_types`.
    /// `None` means the position's type is unknown — Describe reports OID 0
    /// for it and the client sends the value in text format.
    pub param_types: Vec<Option<pgwire::api::Type>>,
    /// Result column schema inferred from the logical plan.
    /// Empty for DML statements (INSERT/UPDATE/DELETE).
    pub result_fields: Vec<FieldInfo>,
    /// True when the SQL is a DSL statement (SEARCH, GRAPH, MATCH, UPSERT INTO,
    /// etc.) that `plan_sql` cannot parse. The Execute handler routes these
    /// through the full DSL dispatcher instead of `execute_planned_sql_with_params`.
    pub is_dsl: bool,
}
