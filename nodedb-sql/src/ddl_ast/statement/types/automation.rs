// SPDX-License-Identifier: Apache-2.0

//! Automation DDL/DML statements.

#[derive(Debug, Clone, PartialEq)]
pub enum AutomationStmt {
    // ── Trigger ──────────────────────────────────────────────────
    CreateTrigger {
        or_replace: bool,
        /// "ASYNC", "SYNC", or "DEFERRED".
        execution_mode: String,
        name: String,
        /// "BEFORE", "AFTER", or "INSTEAD OF".
        timing: String,
        events_insert: bool,
        events_update: bool,
        events_delete: bool,
        collection: String,
        /// "ROW" or "STATEMENT".
        granularity: String,
        when_condition: Option<String>,
        priority: i32,
        /// The `SECURITY` clause as written, or `None` when it was omitted.
        /// The parser does not pick a default — which modes are implementable
        /// is an engine question, so the DDL layer resolves it.
        security: Option<String>,
        body_sql: String,
    },
    DropTrigger {
        name: String,
        collection: String,
        if_exists: bool,
    },
    AlterTrigger {
        name: String,
        action: String,
        new_owner: Option<String>,
    },
    ShowTriggers {
        collection: Option<String>,
    },

    // ── Schedule ─────────────────────────────────────────────────
    CreateSchedule {
        name: String,
        cron_expr: String,
        body_sql: String,
        scope: String,
        missed_policy: String,
        allow_overlap: bool,
    },
    DropSchedule {
        name: String,
        if_exists: bool,
    },
    AlterSchedule {
        name: String,
        action: String,
        cron_expr: Option<String>,
    },
    ShowSchedules,
    ShowScheduleHistory {
        name: String,
    },

    // ── Alert ────────────────────────────────────────────────────
    CreateAlert {
        name: String,
        collection: String,
        where_filter: Option<String>,
        condition_raw: String,
        group_by: Vec<String>,
        window_raw: String,
        fire_after: u32,
        recover_after: u32,
        severity: String,
        notify_targets_raw: String,
    },
    DropAlert {
        name: String,
        if_exists: bool,
    },
    AlterAlert {
        name: String,
        action: String,
    },
    ShowAlerts,
    ShowAlertStatus {
        name: String,
    },
}
