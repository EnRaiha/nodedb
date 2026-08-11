// SPDX-License-Identifier: Apache-2.0

//! Collection DDL/DML statements.

use crate::ddl_ast::alter_ops::AlterCollectionOp;

#[derive(Debug, Clone, PartialEq)]
pub enum CollectionStmt {
    CreateCollection {
        name: String,
        if_not_exists: bool,
        /// Canonical engine name (e.g. `"kv"`, `"vector"`, `"document_strict"`).
        /// `None` means no `engine=` key was present.
        engine: Option<String>,
        /// `(col_name, col_type)` pairs from the parenthesised column list.
        columns: Vec<(String, String)>,
        /// Key-value pairs from the `WITH (...)` clause, excluding `engine=`.
        options: Vec<(String, String)>,
        /// Free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
        flags: Vec<String>,
        /// Raw interior of a `BALANCED ON (group_key = col, ...)` clause, or `None`.
        balanced_raw: Option<String>,
    },
    /// `CREATE TABLE <name> (<col_list>)` — Postgres-style strict-default DDL.
    /// Infers strict relational mode unless overridden via `WITH (engine='...')`.
    /// No column list → rejected with SQLSTATE `42601`.
    CreateTable {
        name: String,
        if_not_exists: bool,
        engine: Option<String>,
        columns: Vec<(String, String)>,
        options: Vec<(String, String)>,
        flags: Vec<String>,
        balanced_raw: Option<String>,
    },
    DropCollection {
        name: String,
        if_exists: bool,
        /// Skip the soft-delete step (requires superuser/tenant_admin).
        purge: bool,
        /// Recursively drop dependents (triggers, RLS, MVs, streams, schedules).
        cascade: bool,
        /// Like `cascade` but also drops schedules with `references_unknown = true`.
        cascade_force: bool,
    },
    /// `UNDROP COLLECTION <n>` — restore a soft-deleted collection within retention window.
    UndropCollection {
        name: String,
    },
    AlterCollection {
        name: String,
        operation: AlterCollectionOp,
    },
    DescribeCollection {
        name: String,
    },
    ShowCollections,

    // ── Index ────────────────────────────────────────────────────
    CreateIndex {
        unique: bool,
        index_name: Option<String>,
        collection: String,
        field: String,
        case_insensitive: bool,
        where_condition: Option<String>,
        /// `IF NOT EXISTS` — creating an index that already exists is a
        /// successful no-op instead of an error.
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        collection: Option<String>,
        if_exists: bool,
    },
    ShowIndexes {
        collection: Option<String>,
    },
    Reindex {
        collection: String,
        index_name: Option<String>,
        concurrent: bool,
    },

    // ── Sequence ─────────────────────────────────────────────────
    CreateSequence {
        name: String,
        if_not_exists: bool,
        start: Option<i64>,
        increment: Option<i64>,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
        cache: Option<i64>,
        /// Raw `FORMAT 'template'` string (quotes stripped), or `None`.
        format_template_raw: Option<String>,
        /// Raw `RESET YEARLY|MONTHLY|QUARTERLY|DAILY` token, or `None`.
        reset_period_raw: Option<String>,
        gap_free: bool,
        scope: Option<String>,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },
    AlterSequence {
        name: String,
        action: String,
        with_value: Option<String>,
    },
    DescribeSequence {
        name: String,
    },
    ShowSequences,
}
