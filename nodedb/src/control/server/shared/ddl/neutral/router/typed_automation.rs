// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `AutomationStmt`: triggers, schedules, and alerts.

use nodedb_sql::ddl_ast::statement::{AutomationStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::alert::{self, CreateAlertRequest};
use super::super::schedule::{self, CreateScheduleRequest};
use super::super::trigger;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        NodedbStatement::Automation(AutomationStmt::CreateTrigger {
            or_replace,
            execution_mode,
            name,
            timing,
            events_insert,
            events_update,
            events_delete,
            collection,
            granularity,
            when_condition,
            priority,
            security,
            body_sql,
        }) => Some(trigger::create_trigger(
            state,
            identity,
            trigger::create::CreateTriggerRequest {
                or_replace: *or_replace,
                execution_mode,
                name,
                timing,
                events_insert: *events_insert,
                events_update: *events_update,
                events_delete: *events_delete,
                collection,
                granularity,
                when_condition: when_condition.as_deref(),
                priority: *priority,
                security: security.as_deref(),
                body_sql,
            },
        )),

        NodedbStatement::Automation(AutomationStmt::AlterTrigger {
            name,
            action,
            new_owner,
        }) => Some(trigger::alter_trigger(
            state,
            identity,
            name,
            action,
            new_owner.as_deref(),
        )),

        NodedbStatement::Automation(AutomationStmt::DropTrigger {
            name, if_exists, ..
        }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing trigger returns the tag before the token handler runs
            // (and before any catalog-read error surfaces). The `if_exists:
            // false` case and the existing-trigger case fall through to
            // `drop_trigger`, which re-derives the name / IF EXISTS from `parts`
            // exactly as the pgwire schema string dispatch did.
            if *if_exists && !trigger::trigger_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP TRIGGER".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(trigger::drop_trigger(state, identity, &parts))
        }

        NodedbStatement::Automation(AutomationStmt::ShowTriggers { .. }) => {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(trigger::show_triggers(state, identity, database_id, &parts))
        }

        NodedbStatement::Automation(AutomationStmt::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            scope,
            missed_policy,
            allow_overlap,
        }) => Some(schedule::create_schedule(
            state,
            identity,
            database_id,
            &CreateScheduleRequest {
                name,
                cron_expr,
                body_sql,
                scope,
                missed_policy,
                allow_overlap: *allow_overlap,
            },
        )),

        NodedbStatement::Automation(AutomationStmt::AlterSchedule {
            name,
            action,
            cron_expr,
        }) => Some(schedule::alter_schedule(
            state,
            identity,
            database_id,
            name,
            action,
            cron_expr.as_deref(),
        )),

        NodedbStatement::Automation(AutomationStmt::DropSchedule { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing schedule returns the tag before the token handler runs
            // (and before the tenant-admin gate). The `if_exists: false` case and
            // the existing-schedule case fall through to `drop_schedule`, which
            // re-derives the name / IF EXISTS from `parts` exactly as the pgwire
            // admin string dispatch did.
            if *if_exists && !schedule::schedule_exists(state, identity, database_id, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP SCHEDULE".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(schedule::drop_schedule(
                state,
                identity,
                database_id,
                &parts,
            ))
        }

        NodedbStatement::Automation(AutomationStmt::CreateAlert {
            name,
            collection,
            where_filter,
            condition_raw,
            group_by,
            window_raw,
            fire_after,
            recover_after,
            severity,
            notify_targets_raw,
        }) => Some(alert::create_alert(
            state,
            identity,
            &CreateAlertRequest {
                name,
                collection,
                where_filter: where_filter.as_deref(),
                condition_raw,
                group_by,
                window_raw,
                fire_after: *fire_after,
                recover_after: *recover_after,
                severity,
                notify_targets_raw,
                database_id,
            },
        )),

        NodedbStatement::Automation(AutomationStmt::AlterAlert { name, action }) => Some(
            alert::alter_alert(state, identity, database_id, name, action),
        ),

        NodedbStatement::Automation(AutomationStmt::DropAlert { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing alert returns the tag before the token handler runs
            // (and before the tenant-admin gate). The `if_exists: false` case and
            // the existing-alert case fall through to `drop_alert`, which
            // re-derives the name from `parts[2]` exactly as the pgwire admin
            // string dispatch did.
            if *if_exists && !alert::alert_exists(state, identity, database_id, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP ALERT".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(alert::drop_alert(state, identity, database_id, &parts))
        }

        _ => None,
    }
}
