// SPDX-License-Identifier: Apache-2.0

//! Error types for the nodedb-sql crate.

/// Errors produced during SQL parsing, resolution, or planning.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum SqlError {
    #[error("parse error: {detail}")]
    Parse { detail: String },

    #[error("table not found: {name}")]
    UnknownTable { name: String },

    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("ambiguous column '{column}' — qualify with table name")]
    AmbiguousColumn { column: String },

    #[error("type mismatch: {detail}")]
    TypeMismatch { detail: String },

    #[error("unsupported: {detail}")]
    Unsupported { detail: String },

    #[error("invalid function call: {detail}")]
    InvalidFunction { detail: String },

    #[error("invalid window frame: {detail}")]
    InvalidWindowFrame { detail: String },

    #[error("missing required field '{field}' for {context}")]
    MissingField { field: String, context: String },

    /// A positional `INSERT`/`UPSERT` `VALUES` row (no explicit column
    /// list) supplied more values than the target collection has declared
    /// columns. Binding the overflow value(s) to a synthetic `col{N}` name
    /// would silently store them under an unaddressable column — the same
    /// failure mode as #202 — so this is rejected rather than guessed
    /// at.
    #[error(
        "INSERT/UPSERT into '{collection}': row has {given} value(s) but only \
         {declared} column(s) are declared; supply an explicit column list \
         or remove the extra value(s)"
    )]
    InsertColumnArityMismatch {
        collection: String,
        given: usize,
        declared: usize,
    },

    /// A positional `INSERT`/`UPSERT` into a Key-Value collection supplied
    /// no explicit column list. The KV path splits key/value by matching
    /// column *names* against `pk_col`/`"key"`/`"ttl"`, so without a column
    /// list there is no principled key/value split — binding positionally
    /// would silently write an empty-keyed, empty-valued row (all such rows
    /// collide). Rejected rather than guessed at, mirroring
    /// [`InsertColumnArityMismatch`].
    #[error(
        "INSERT/UPSERT into KV collection '{collection}': supply an explicit \
         column list (KV needs named key/value columns; a positional VALUES \
         row has no key to bind to)"
    )]
    PositionalKvInsertUnsupported { collection: String },

    /// A descriptor the planner depends on is being drained by
    /// an in-flight DDL. Callers (pgwire handlers) should retry
    /// the whole statement after a short backoff. Propagated
    /// from `SqlCatalogError::RetryableSchemaChanged`.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged { descriptor: String },

    /// Identifier is a NodeDB reserved keyword. Use a quoted identifier to bypass.
    #[error(
        "identifier '{name}' is reserved by NodeDB ({reason}); \
         use a quoted identifier (e.g., \"{name}\") to bypass"
    )]
    ReservedIdentifier { name: String, reason: &'static str },

    /// An unsupported SQL constraint was used in a DDL statement.
    ///
    /// Rendered as SQLSTATE `0A000` (feature_not_supported). The `feature` field
    /// names the constraint keyword and `hint` points to the NodeDB equivalent.
    #[error("unsupported constraint: {feature}; {hint}")]
    UnsupportedConstraint { feature: String, hint: String },

    /// Both a `WITH (engine='...')` clause and a trailing `ENGINE = ...`
    /// suffix were given on the same `CREATE COLLECTION` / `CREATE TABLE`
    /// statement, naming different engines.
    ///
    /// Rendered as SQLSTATE `0A000` (feature_not_supported), matching the
    /// existing duplicate-axis rejection precedent for `WITH (profile=...)`.
    #[error(
        "conflicting engine selection: WITH (engine='{with_engine}') and \
         ENGINE = {suffix_engine} name different engines; only one engine-selection \
         clause is allowed"
    )]
    ConflictingEngineClause {
        with_engine: String,
        suffix_engine: String,
    },

    /// WITH RECURSIVE used a set operator other than UNION or UNION ALL.
    ///
    /// Only `UNION` and `UNION ALL` are permitted in the recursive term of a
    /// `WITH RECURSIVE` CTE. `INTERSECT` and `EXCEPT` are rejected because
    /// they cannot guarantee termination in standard iterative evaluation.
    #[error(
        "WITH RECURSIVE: only UNION / UNION ALL are allowed in the recursive term; \
         {op} is not permitted"
    )]
    InvalidRecursiveSetOp { op: String },

    /// The recursive self-reference is absent, appears more than once, or
    /// appears inside a subquery, aggregate, or the nullable side of an outer join.
    #[error("WITH RECURSIVE: invalid self-reference to '{cte_name}' in recursive term: {reason}")]
    InvalidRecursiveSelfRef { cte_name: String, reason: String },

    /// The anchor SELECT produces a different number of columns than the
    /// column list declared on the CTE (or the recursive arm).
    #[error(
        "WITH RECURSIVE CTE '{cte_name}': anchor produces {anchor_cols} column(s) \
         but {declared_cols} were declared"
    )]
    RecursiveColumnMismatch {
        cte_name: String,
        anchor_cols: usize,
        declared_cols: usize,
    },

    /// The recursive CTE exceeded the configured `max_recursion_depth`.
    ///
    /// This is a runtime error produced by the executor, not the planner.
    #[error(
        "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
         add a stricter termination condition or raise max_recursion_depth"
    )]
    RecursionDepthExceeded { cte_name: String, max_depth: usize },

    /// Collection is soft-deleted (within retention window).
    /// Propagated from `SqlCatalogError::CollectionDeactivated`;
    /// the pgwire layer renders this as sqlstate 42P01 with an
    /// `UNDROP COLLECTION <name>` hint in the message.
    #[error(
        "collection '{name}' was dropped; \
         restore with `{undrop_hint}` before retention elapses \
         at {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        name: String,
        retention_expires_at_ns: u64,
        undrop_hint: String,
    },
}

impl From<crate::catalog::SqlCatalogError> for SqlError {
    fn from(e: crate::catalog::SqlCatalogError) -> Self {
        match e {
            crate::catalog::SqlCatalogError::RetryableSchemaChanged { descriptor } => {
                Self::RetryableSchemaChanged { descriptor }
            }
            crate::catalog::SqlCatalogError::CollectionDeactivated {
                name,
                retention_expires_at_ns,
            } => {
                let undrop_hint = format!("UNDROP COLLECTION {name}");
                Self::CollectionDeactivated {
                    name,
                    retention_expires_at_ns,
                    undrop_hint,
                }
            }
        }
    }
}

impl From<nodedb_query::expr_parse::ExprParseError> for SqlError {
    fn from(e: nodedb_query::expr_parse::ExprParseError) -> Self {
        Self::Parse {
            detail: e.to_string(),
        }
    }
}

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        Self::Parse {
            detail: e.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqlError>;
