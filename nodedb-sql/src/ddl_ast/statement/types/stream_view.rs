// SPDX-License-Identifier: Apache-2.0

//! Stream and view DDL/DML statements.

#[derive(Debug, Clone, PartialEq)]
pub enum StreamViewStmt {
    // ── Change stream ────────────────────────────────────────────
    CreateChangeStream {
        name: String,
        collection: String,
        with_clause_raw: String,
    },
    DropChangeStream {
        name: String,
        if_exists: bool,
    },
    AlterChangeStream {
        name: String,
        action: String,
    },
    ShowChangeStreams,

    // ── Consumer group ───────────────────────────────────────────
    CreateConsumerGroup {
        group_name: String,
        stream_name: String,
    },
    DropConsumerGroup {
        name: String,
        stream: String,
        if_exists: bool,
    },
    ShowConsumerGroups {
        stream: Option<String>,
    },

    // ── Materialized view ────────────────────────────────────────
    CreateMaterializedView {
        name: String,
        source: String,
        query_sql: String,
        refresh_mode: String,
    },
    DropMaterializedView {
        name: String,
        if_exists: bool,
    },
    ShowMaterializedViews,

    // ── Continuous aggregate ─────────────────────────────────────
    CreateContinuousAggregate {
        name: String,
        source: String,
        bucket_raw: String,
        aggregate_exprs_raw: String,
        group_by: Vec<String>,
        with_clause_raw: String,
    },
    DropContinuousAggregate {
        name: String,
        if_exists: bool,
    },
    /// `REFRESH CONTINUOUS AGGREGATE <name>`: fold every sealed source
    /// partition the aggregate lacks.
    RefreshContinuousAggregate {
        name: String,
    },
    ShowContinuousAggregates,
}
