// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL router.
//!
//! [`try_dispatch`] recognizes the migrated families and routes to them; every
//! other statement returns `None` so the transitional pgwire delegation in the
//! parent [`super::super::dispatch`] handles it.

use nodedb_sql::ddl_ast::AlterCollectionOp;
use nodedb_sql::ddl_ast::statement::{
    AuthStmt, AutomationStmt, ClusterStmt, CollectionStmt, DatabaseStmt, GraphStmt,
    NodedbStatement, PolicyStmt, StreamViewStmt,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};
use super::alert::{self, CreateAlertRequest};
use super::apikey;
use super::auth_key;
use super::auth_user;
use super::blacklist;
use super::bulk;
use super::change_stream;
use super::cluster;
use super::collection;
use super::conflict_policy;
use super::constraint;
use super::consumer_group;
use super::continuous_agg;
use super::crdt_ops;
use super::custom_type;
use super::database;
use super::dsl;
use super::emergency_ddl;
use super::explain_ddl;
use super::function;
use super::grant;
use super::graph_ops;
use super::impersonation;
use super::inspect;
use super::inspect_audit;
use super::kv_atomic;
use super::kv_sorted_index;
use super::last_value;
use super::maintenance;
use super::match_ops;
use super::materialized_view;
use super::metering_ddl;
use super::observability;
use super::oidc;
use super::org_ddl;
use super::period_lock;
use super::permission_tree;
use super::procedure;
use super::query_functions;
use super::rate_gate;
use super::retention_policy;
use super::rls::{self, CreateRlsPolicyRequest};
use super::role;
use super::schedule::{self, CreateScheduleRequest};
use super::scope_ddl;
use super::scope_query_ddl;
use super::sequence::{self, CreateSequenceRequest};
use super::service_account;
use super::session_admin;
use super::spatial;
use super::stream_select;
use super::synonym_group;
use super::system_ddl;
use super::tenant;
use super::timeseries;
use super::topic;
use super::topic_subscribe;
use super::transfer;
use super::tree_ops;
use super::trigger;
use super::typeguard;
use super::user;
use super::version_history;
use super::weighted_pick;

/// Try to handle `sql` with a migrated protocol-neutral DDL family handler.
///
/// Returns `Some(result)` when a migrated family owns the statement, `None`
/// otherwise (non-migrated family, parse error, or a sub-case that today falls
/// through to the SQL planner) so the caller can fall back to the transitional
/// pgwire delegation.
pub async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // String-recognized user/role families. `DROP USER` parses into a typed
    // `AuthStmt::DropUser` that carries no `if_exists` flag (so it mishandles
    // `DROP USER IF EXISTS`), and `CREATE ROLE` / `DROP ROLE` do not parse into
    // any typed variant at all — the pgwire router dispatched all three from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the token-based `strip_if_exists` / `strip_if_not_exists` handling and the
    // syntax messages stay byte-identical.
    let upper = sql.to_uppercase();
    if upper.starts_with("DROP USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(user::drop_user(state, identity, &parts));
    }
    if upper.starts_with("CREATE ROLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(role::create_role(state, identity, &parts));
    }
    if upper.starts_with("DROP ROLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(role::drop_role(state, identity, &parts));
    }

    // Service accounts. These statements do not parse into any typed AST
    // variant — the pgwire router dispatched all three from the raw token
    // slice by string prefix. Replicate that exactly here, before the parse
    // gate, so the token-based `IF [NOT] EXISTS` stripping and syntax messages
    // stay byte-identical.
    if upper.starts_with("CREATE SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(service_account::create_service_account(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(service_account::drop_service_account(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("ALTER SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(service_account::alter_service_account_set_databases(
            state, identity, &parts,
        ));
    }

    // Auth-admin DDL families (API keys, auth-scoped API keys, auth user
    // management, blacklist). None of these parse into any typed AST variant —
    // the pgwire admin router dispatched all of them by string prefix from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the prefix recognition and syntax messages stay byte-identical. The
    // `BLACKLIST ` prefix intentionally precedes the (non-migrated) emergency
    // `BLACKLIST AUTH USERS WHERE` handler exactly as it did in the pgwire admin
    // router, so the shadowing behavior is unchanged.
    if upper.starts_with("CREATE API KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::create_api_key(state, identity, &parts));
    }
    if upper.starts_with("REVOKE API KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::revoke_api_key(state, identity, &parts));
    }
    if upper.starts_with("LIST API KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::list_api_keys(state, identity, &parts));
    }
    if upper.starts_with("SHOW API KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::list_api_keys(state, identity, &parts));
    }
    if upper.starts_with("CREATE AUTH KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::create_auth_key(state, identity, &parts));
    }
    if upper.starts_with("ROTATE AUTH KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::rotate_auth_key(state, identity, &parts));
    }
    if upper.starts_with("LIST AUTH KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::list_auth_keys(state, identity, &parts));
    }
    if upper.starts_with("DEACTIVATE AUTH USER ") || upper.starts_with("ALTER AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::handle_auth_user(state, identity, &parts));
    }
    if upper.starts_with("PURGE AUTH USERS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::purge_auth_users(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUTH USERS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::show_auth_users(state, identity, &parts));
    }
    if upper.starts_with("BLACKLIST ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(blacklist::handle_blacklist(state, identity, &parts));
    }
    if upper.starts_with("SHOW BLACKLIST") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(blacklist::show_blacklist(state, identity, &parts));
    }

    // Tenant management. `CREATE TENANT`, `DROP TENANT`, and `PURGE TENANT`
    // parse into no typed AST variant — the pgwire auth router dispatched all
    // three by string prefix from the raw token slice. Replicate that exactly
    // here, before the parse gate, so the `IF [NOT] EXISTS` stripping and
    // syntax messages stay byte-identical. `PURGE TENANT` dispatches an async
    // Data Plane meta op.
    //
    // `ALTER TENANT ` is ambiguous: `ALTER TENANT <id|name> SET QUOTA ...`
    // (this string form) and `ALTER TENANT <name> IN DATABASE <db> SET QUOTA
    // (...)` (a typed `DatabaseStmt::AlterTenant`, handled in the typed match
    // below) share the same prefix. The typed `ddl_ast` tenant parser only
    // recognizes the `IN DATABASE` form when `parts.len() >= 8` and tokens 3/4
    // are `IN`/`DATABASE`; replicate that exact partition here so the
    // `IN DATABASE` form always falls through to the typed arm instead of
    // being shadowed by this string handler.
    //
    // `SHOW TENANT USAGE` / `SHOW TENANT QUOTA` (bare, no `IN DATABASE`) are
    // NOT recognized here: the typed `ddl_ast` tenant parser never returns
    // `None` for `SHOW TENANT USAGE|QUOTA...` — every such input resolves to
    // either the typed `IN DATABASE` variant or a `42601` parse error. Their
    // pgwire string handlers were therefore confirmed dead code and deleted,
    // not migrated; adding a neutral string prefix for them would make that
    // dead code reachable and break parity.
    if upper.starts_with("CREATE TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::create_tenant(state, identity, &parts));
    }
    if upper.starts_with("ALTER TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let is_in_database_form = parts.len() >= 8
            && parts[3].eq_ignore_ascii_case("IN")
            && parts[4].eq_ignore_ascii_case("DATABASE");
        if !is_in_database_form {
            return Some(tenant::alter_tenant(state, identity, &parts));
        }
    }
    if upper.starts_with("DROP TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::drop_tenant(state, identity, &parts));
    }
    if upper.starts_with("PURGE TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::purge_tenant(state, identity, database_id, &parts).await);
    }

    // Emergency & incident response DDL. `EMERGENCY LOCKDOWN` / `EMERGENCY
    // UNLOCK` parse into no typed AST variant — the pgwire admin router
    // dispatched both by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and syntax
    // messages stay byte-identical. `BLACKLIST AUTH USERS WHERE …` is likewise
    // string-recognized, but the `BLACKLIST ` prefix above already claims it
    // (exactly as it shadowed the pgwire emergency handler, which ran only after
    // this neutral router). This guard is therefore intentionally kept after the
    // `BLACKLIST ` guard so `bulk_blacklist` remains unreachable — preserving the
    // dead-but-present state verbatim.
    if upper.starts_with("EMERGENCY LOCKDOWN") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::emergency_lockdown(state, identity, &parts));
    }
    if upper.starts_with("EMERGENCY UNLOCK") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::emergency_unlock(state, identity, &parts));
    }
    if upper.starts_with("BLACKLIST AUTH USERS WHERE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::bulk_blacklist(state, identity, &parts));
    }

    // System-level settings: `ALTER SYSTEM SET <field> = <value>`. Parses into
    // no typed AST variant — the pgwire auth router dispatched it by string
    // prefix from the raw token slice. Replicate that exactly here, before the
    // parse gate, so the prefix recognition and the `parts`-based field / value
    // extraction stay byte-identical.
    if upper.starts_with("ALTER SYSTEM ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(system_ddl::alter_system(state, identity, &parts));
    }

    // Stored procedures. None of `CREATE [OR REPLACE] PROCEDURE`, `DROP
    // PROCEDURE`, `SHOW PROCEDURES`, or `CALL <procedure>(...)` parse into any
    // typed AST variant — the pgwire router dispatched all of them by string
    // prefix from the raw SQL / token slice. Replicate that exactly here, before
    // the parse gate, so the prefix recognition and syntax messages stay
    // byte-identical.
    if upper.starts_with("CREATE OR REPLACE PROCEDURE ") || upper.starts_with("CREATE PROCEDURE ") {
        return Some(procedure::create_procedure(state, identity, sql));
    }
    if upper.starts_with("DROP PROCEDURE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(procedure::drop_procedure(state, identity, &parts));
    }
    if upper == "SHOW PROCEDURES" || upper.starts_with("SHOW PROCEDURES") {
        return Some(procedure::show_procedures(state, identity));
    }
    if upper.starts_with("CALL ") {
        return Some(procedure::call_procedure(state, identity, sql).await);
    }

    // User-defined functions. None of `CREATE [OR REPLACE] [AGGREGATE] FUNCTION`,
    // `DROP FUNCTION`, `ALTER FUNCTION`, or `SHOW FUNCTIONS` parse into any typed
    // AST variant — the pgwire router dispatched all of them by string prefix
    // from the raw SQL / token slice. Replicate that exactly here, before the
    // parse gate, so the prefix recognition, `LANGUAGE WASM` branch, and syntax
    // messages stay byte-identical.
    if upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
        || upper.starts_with("CREATE AGGREGATE FUNCTION ")
    {
        return Some(function::create_wasm_aggregate(state, identity, sql));
    }
    if upper.starts_with("CREATE OR REPLACE FUNCTION ") || upper.starts_with("CREATE FUNCTION ") {
        if upper.contains("LANGUAGE WASM") {
            return Some(function::create_wasm_function(state, identity, sql));
        }
        return Some(function::create_function(state, identity, sql));
    }
    if upper.starts_with("DROP FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::drop_function(state, identity, &parts));
    }
    if upper.starts_with("ALTER FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::alter_function(state, identity, &parts));
    }
    if upper == "SHOW FUNCTIONS" || upper.starts_with("SHOW FUNCTIONS") {
        return Some(function::show_functions(state, identity));
    }

    // Constraint DDL. `ALTER COLLECTION ... ADD CONSTRAINT` / `ADD TRANSITION
    // CHECK` do not parse into any typed AST variant (the `parse_alter_operation`
    // path returns `None` for them, so `ddl_ast::parse` yields `None`), and
    // `DROP CONSTRAINT` / `SHOW CONSTRAINTS ON` were dispatched by string prefix
    // from the pgwire collaborative router. Replicate that exactly here, before
    // the parse gate, so the prefix recognition and syntax messages stay
    // byte-identical. Guard ordering (TRANSITIONS before the general CHECK arm,
    // which excludes both TRANSITIONS and TRANSITION CHECK) is preserved verbatim.
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("TRANSITIONS")
    {
        return Some(constraint::add_state_constraint(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD TRANSITION CHECK") {
        return Some(constraint::add_transition_check(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("CHECK")
        && !upper.contains("TRANSITIONS")
        && !upper.contains("TRANSITION CHECK")
    {
        return Some(constraint::add_check_constraint(state, identity, sql));
    }
    if upper.starts_with("SHOW CONSTRAINTS ON ") {
        return Some(constraint::show_constraints(state, identity, sql));
    }
    if upper.starts_with("DROP CONSTRAINT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(constraint::drop_constraint(state, identity, &parts));
    }

    // Permission tree management. `ALTER COLLECTION … SET PERMISSION_TREE` and
    // `… DROP PERMISSION_TREE` do not parse into any typed AST variant (the
    // `parse_alter_operation` path returns `None` for both, so `ddl_ast::parse`
    // yields `None`) — the pgwire collaborative router dispatched both from the
    // raw SQL by string prefix + `contains`. Replicate that exactly here, before
    // the parse gate, so the recognition and syntax messages stay byte-identical.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET PERMISSION_TREE") {
        return Some(permission_tree::set_permission_tree(state, identity, sql).await);
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERMISSION_TREE") {
        return Some(permission_tree::drop_permission_tree(state, identity, sql).await);
    }

    // Period lock management. `ALTER COLLECTION … ADD PERIOD LOCK` and `… DROP
    // PERIOD LOCK` do not parse into any typed AST variant (the
    // `parse_alter_operation` path returns `None` for both, so `ddl_ast::parse`
    // yields `None`) — the pgwire collaborative router dispatched both from the
    // raw SQL by string prefix + `contains`. Replicate that exactly here, before
    // the parse gate, so the recognition and syntax messages stay byte-identical.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD PERIOD LOCK") {
        return Some(period_lock::add_period_lock(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERIOD LOCK") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(period_lock::drop_period_lock(state, identity, &parts));
    }

    // TYPEGUARD DDL. None of these statements are dispatched from a typed AST
    // variant — the pgwire router recognized all of them by string prefix from
    // the raw SQL (the `SHOW TYPEGUARD…` prefix does parse into a typed
    // `MiscStmt::ShowTypeGuards`, but the pgwire string dispatch claimed it
    // before the parse gate). Replicate that exactly here, before the parse
    // gate, so the prefix recognition and syntax messages stay byte-identical.
    if upper.starts_with("CREATE TYPEGUARD ") || upper.starts_with("CREATE OR REPLACE TYPEGUARD ") {
        return Some(typeguard::create_typeguard(state, identity, sql));
    }
    if upper.starts_with("ALTER TYPEGUARD ") {
        return Some(typeguard::alter_typeguard(state, identity, sql));
    }
    if upper.starts_with("DROP TYPEGUARD ") {
        return Some(typeguard::drop_typeguard(state, identity, sql));
    }
    if upper.starts_with("VALIDATE TYPEGUARD ON ") {
        return Some(typeguard::validate_typeguard(state, identity, sql).await);
    }
    if upper.starts_with("SHOW TYPEGUARD ON ") {
        return Some(typeguard::show_typeguard(state, identity, sql));
    }
    if upper == "SHOW TYPEGUARDS" || upper.starts_with("SHOW TYPEGUARDS") {
        return Some(typeguard::show_typeguards(state, identity, sql));
    }

    // Schedule SHOW. `SHOW SCHEDULE HISTORY <name>` parses into a typed
    // `AutomationStmt::ShowScheduleHistory` and `SHOW SCHEDULES` into
    // `AutomationStmt::ShowSchedules`, but the pgwire router dispatched both from
    // the raw token slice by string prefix (the `SHOW SCHEDULE` prefix also
    // captures the bare-singular `SHOW SCHEDULE` input, which parses into no
    // typed variant). Replicate that exactly here, before the parse gate, so the
    // prefix recognition and `parts.get(3)` name extraction stay byte-identical.
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(3).copied().unwrap_or("");
        return Some(schedule::show_schedule_history(state, identity, name));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(schedule::show_schedules(state, identity));
    }

    // Alert SHOW. `SHOW ALERT STATUS <name>` parses into a typed
    // `AutomationStmt::ShowAlertStatus` and `SHOW ALERTS` into
    // `AutomationStmt::ShowAlerts`, but the pgwire admin router dispatched both
    // from the raw token slice by string prefix (the `SHOW ALERT` prefix also
    // captures the bare-singular `SHOW ALERT` input, which parses into
    // `ShowAlerts`). Replicate that exactly here, before the parse gate, so the
    // prefix recognition (STATUS checked first) and the `parts.get(4)` name
    // extraction (name after `ON`) stay byte-identical.
    if upper.starts_with("SHOW ALERT STATUS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(4).copied().unwrap_or("");
        return Some(alert::show_alert_status(state, identity, database_id, name));
    }
    if upper.starts_with("SHOW ALERT") {
        return Some(alert::show_alerts(state, identity, database_id));
    }

    // Change streams: `SHOW CHANGE STREAM(S)`. This parses into a typed
    // `StreamViewStmt::ShowChangeStreams`, but the pgwire router dispatched it
    // from the raw SQL by string prefix (the `SHOW CHANGE STREAM` prefix, which
    // captures both the plural `SHOW CHANGE STREAMS` and the bare-singular
    // input). Replicate that exactly here, before the parse gate, so the prefix
    // recognition stays byte-identical.
    if upper.starts_with("SHOW CHANGE STREAM") {
        return Some(change_stream::show_change_streams(state, identity));
    }

    // Consumer groups: `SHOW CONSUMER GROUPS ON <stream>`, `SHOW PARTITIONS ON
    // <stream>`, and `COMMIT OFFSET(S) …`. The pgwire streaming router dispatched
    // all four by string prefix from the raw token slice. `SHOW CONSUMER GROUPS`
    // parses into a typed `StreamViewStmt::ShowConsumerGroups`, but the pgwire
    // string dispatch claimed it before any typed arm ran; `SHOW PARTITIONS` and
    // `COMMIT OFFSET(S)` parse into no typed variant at all. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based syntax messages stay byte-identical. (`SHOW PARTITIONS ` also
    // shadows the timeseries `show_partitions` handler exactly as the pgwire
    // streaming router — which ran before engine_ops — did.)
    if upper.starts_with("SHOW CONSUMER GROUPS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::show_consumer_groups(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::show_partitions(state, identity, &parts));
    }
    if upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::commit_offset(state, identity, &parts));
    }

    // Topics: `CREATE TOPIC`, `DROP TOPIC`, `SHOW TOPIC(S)`, and `PUBLISH TO`.
    // None of these parse into any typed AST variant — the pgwire streaming
    // router dispatched all four by string prefix from the raw token slice /
    // SQL. Replicate that exactly here, before the parse gate, so the prefix
    // recognition (including the trailing-space-less `SHOW TOPIC`, which
    // captures both `SHOW TOPICS` and the bare-singular input) and the
    // `parts`-based syntax messages stay byte-identical.
    if upper.starts_with("CREATE TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::create_topic(state, identity, &parts, sql));
    }
    if upper.starts_with("DROP TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::drop_topic(state, identity, &parts));
    }
    if upper.starts_with("SHOW TOPIC") {
        return Some(topic::show_topics(state, identity));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(topic::handle_publish(state, identity, sql).await);
    }

    // Stream consumption: `SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    // [PARTITION <p>] [LIMIT <n>]`. Parses into no typed AST variant — the
    // pgwire streaming router recognized it by string prefix from the raw
    // token slice. Replicate that exactly here, before the parse gate, so the
    // prefix recognition and the `parts`-based extraction stay byte-identical.
    if upper.starts_with("SELECT ")
        && upper.contains("FROM STREAM ")
        && upper.contains("CONSUMER GROUP")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(stream_select::select_from_stream(state, identity, &parts).await);
    }

    // Stream/Topic consumption: `SELECT * FROM TOPIC <name> CONSUMER GROUP
    // <group> [LIMIT <n>]`. Topics use "topic:<name>" buffer keys; the pgwire
    // streaming router rewrote the token slice (TOPIC → STREAM, name →
    // "topic:<name>") and delegated to the stream-consume handler. Replicate
    // that rewrite exactly here, before the parse gate.
    if upper.starts_with("SELECT ")
        && upper.contains("FROM TOPIC ")
        && upper.contains("CONSUMER GROUP")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 8
            || !parts[3].eq_ignore_ascii_case("TOPIC")
            || !parts[5].eq_ignore_ascii_case("CONSUMER")
            || !parts[6].eq_ignore_ascii_case("GROUP")
        {
            return Some(Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "expected SELECT * FROM TOPIC <topic> CONSUMER GROUP <group>".to_string(),
            }));
        }
        let prefixed_name = format!("topic:{}", parts[4].to_lowercase());
        let stream_keyword = "STREAM";
        let mut rewritten = Vec::with_capacity(parts.len());
        for (i, &p) in parts.iter().enumerate() {
            match i {
                3 => rewritten.push(stream_keyword),
                4 => rewritten.push(prefixed_name.as_str()),
                _ => rewritten.push(p),
            }
        }
        return Some(stream_select::select_from_stream(state, identity, &rewritten).await);
    }

    // Pub/Sub: `SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]` (legacy).
    // Parses into no typed AST variant — the pgwire collaborative router
    // recognized it by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition stays
    // byte-identical.
    if upper.starts_with("SUBSCRIBE TO ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic_subscribe::subscribe_to(state, identity, sql, &parts));
    }

    // Version history. None of `CREATE CHECKPOINT`, `DROP CHECKPOINT`, `SHOW
    // VERSIONS OF`, `SELECT … AT VERSION`, `SELECT DIFF(…)`, `RESTORE … SET
    // VERSION`, or `COMPACT HISTORY ON` parse into any typed AST variant — the
    // pgwire collaborative router dispatched all of them by string prefix from
    // the raw SQL. Replicate that exactly here, before the parse gate, so the
    // prefix recognition (including the `RESTORE … SET VERSION` guard that keeps
    // `RESTORE TENANT` / `RESTORE DATABASE` on the typed path) and syntax
    // messages stay byte-identical. Guard ordering mirrors the pgwire router.
    if upper.starts_with("CREATE CHECKPOINT ") {
        return Some(
            version_history::checkpoint::create_checkpoint(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("DROP CHECKPOINT ") {
        return Some(version_history::checkpoint::drop_checkpoint(
            state, identity, sql,
        ));
    }
    if upper.starts_with("SHOW VERSIONS OF ") {
        return Some(version_history::show_versions::show_versions(
            state, identity, sql,
        ));
    }
    if upper.contains("AT VERSION") && upper.starts_with("SELECT") {
        return Some(
            version_history::at_version::select_at_version(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("SELECT DIFF(") || upper.starts_with("SELECT DIFF (") {
        return Some(version_history::diff::select_diff(state, identity, database_id, sql).await);
    }
    if upper.starts_with("RESTORE ") && upper.contains("SET VERSION") {
        return Some(
            version_history::restore::restore_version(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("COMPACT HISTORY ON ") {
        return Some(
            version_history::compact::compact_history(state, identity, database_id, sql).await,
        );
    }

    // Maintenance: ANALYZE / COMPACT / SHOW STORAGE / SHOW COMPACTION STATUS.
    // These parse into typed `ClusterStmt` variants, but the pgwire router
    // dispatched all four by string prefix from the raw SQL / token slice (the
    // pgwire typed-AST path has no arm for them). Replicate that exactly here,
    // before the parse gate, so the prefix recognition (trailing space on
    // `ANALYZE ` / `COMPACT `, and the `SHOW COMPACTION STATUS` exact / prefix
    // forms) and the `parts`-based name extraction stay byte-identical. The
    // `COMPACT ` prefix is placed after the version-history `COMPACT HISTORY ON`
    // guard above, preserving that `COMPACT HISTORY ON …` routes to
    // version_history exactly as the pgwire dispatch (neutral-first) did.
    if upper.starts_with("ANALYZE ") {
        return Some(maintenance::handle_analyze(state, identity, sql).await);
    }
    if upper.starts_with("COMPACT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(maintenance::handle_compact(state, identity, &parts));
    }
    if upper.starts_with("SHOW STORAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(maintenance::handle_show_storage(state, identity, &parts));
    }
    if upper == "SHOW COMPACTION STATUS" || upper.starts_with("SHOW COMPACTION STATUS ") {
        return Some(maintenance::handle_show_compaction_status(state, identity));
    }

    // Cluster management & observability: SHOW CLUSTER, SHOW RAFT GROUPS,
    // SHOW RAFT GROUP <id>, SHOW MIGRATIONS, REBALANCE, SHOW PEER HEALTH,
    // SHOW NODES, SHOW NODE <id>, REMOVE NODE <id>, SHOW RANGES, SHOW
    // ROUTING, SHOW SCHEMA VERSION. All of these parse into typed
    // `ClusterStmt` variants, but the pgwire admin router dispatched them by
    // string prefix from the raw SQL / token slice (the pgwire typed-AST path
    // only had an arm for `ALTER RAFT GROUP`). Replicate that exactly here,
    // before the parse gate, so the prefix recognition (order matters: `SHOW
    // RAFT GROUPS` before `SHOW RAFT GROUP `) and the `parts`-based
    // extraction stay byte-identical. `ALTER RAFT GROUP` is dispatched via
    // the typed match below, exactly as the pgwire router did.
    if upper.starts_with("SHOW CLUSTER") {
        return Some(cluster::show_cluster(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUPS") {
        return Some(cluster::show_raft_groups(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::show_raft_group(state, identity, &parts));
    }
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(cluster::show_migrations(state, identity));
    }
    if upper.starts_with("REBALANCE") {
        return Some(cluster::rebalance(state, identity));
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(cluster::show_peer_health(state, identity));
    }
    if upper.starts_with("SHOW NODES") {
        return Some(cluster::show_nodes(state, identity));
    }
    if upper.starts_with("SHOW NODE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::show_node(state, identity, &parts));
    }
    if upper.starts_with("REMOVE NODE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::remove_node(state, identity, &parts));
    }
    if upper.starts_with("SHOW RANGES") {
        return Some(cluster::show_ranges(state, identity));
    }
    if upper.starts_with("SHOW ROUTING") {
        return Some(cluster::show_routing(state, identity));
    }
    if upper.starts_with("SHOW SCHEMA VERSION") {
        return Some(cluster::show_schema_version(state, identity));
    }

    // Vector index lifecycle: SHOW VECTOR INDEX / ALTER VECTOR INDEX. None of
    // these are dispatched from a typed AST arm — the pgwire engine_ops router
    // recognized all four by string prefix from the raw SQL. Replicate that
    // exactly here, before the parse gate, so the prefix recognition (and the
    // ` SEAL` / ` COMPACT` / ` SET ` sub-clause guards, checked in this order)
    // stays byte-identical.
    if upper.starts_with("SHOW VECTOR INDEX ") {
        return Some(maintenance::handle_show_vector_index(state, identity, sql).await);
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" SEAL") {
        return Some(maintenance::handle_alter_vector_index_seal(state, identity, sql).await);
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" COMPACT") {
        return Some(maintenance::handle_alter_vector_index_compact(state, identity, sql).await);
    }
    if upper.starts_with("ALTER VECTOR INDEX ") && upper.contains(" SET ") {
        return Some(maintenance::handle_alter_vector_index_set(state, identity, sql).await);
    }

    // Graph index and tree operations: CREATE GRAPH INDEX / TREE_SUM /
    // TREE_CHILDREN. None of these are dispatched from a typed AST arm — the
    // pgwire engine_ops router recognized all three by string prefix from the
    // raw SQL (the `SELECT TREE_SUM` / bare `TREE_SUM` and `SELECT
    // TREE_CHILDREN` / bare `TREE_CHILDREN` forms never parse into a typed DDL
    // AST). Replicate that exactly here, before the parse gate, so the prefix
    // recognition and syntax messages stay byte-identical.
    if upper.starts_with("CREATE GRAPH INDEX ") {
        return Some(tree_ops::create_graph_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT TREE_SUM") || upper.starts_with("TREE_SUM") {
        return Some(tree_ops::tree_sum(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT TREE_CHILDREN") || upper.starts_with("TREE_CHILDREN") {
        return Some(tree_ops::tree_children(state, identity, database_id, sql).await);
    }

    // Engine-ops SQL functions and DDL. None of these are dispatched from a
    // typed AST arm — the pgwire engine_ops router recognized all of them by
    // string prefix from the raw SQL (these keywords do not appear in the DDL
    // AST grammar, so `ddl_ast::parse` returns `None` for them). Replicate that
    // exactly here, before the parse gate, so the prefix recognition, guard
    // ordering, and syntax messages stay byte-identical. The three vector
    // model / metadata forms (`ALTER COLLECTION … SET VECTOR METADATA ON`,
    // `SHOW VECTOR MODELS`, `SELECT VECTOR_METADATA(…)`) remain on the
    // transitional pgwire path — they are handled by the not-yet-migrated
    // collection family — so they are intentionally not routed here.
    //
    // `CREATE TIMESERIES` / `ALTER TIMESERIES` / `REWRITE PARTITIONS` are
    // routed here, but `SHOW PARTITIONS ` is intentionally NOT — it is already
    // claimed by the consumer-group handler above (which ran before engine_ops
    // on the pgwire path too), so the timeseries `show_partitions` handler stays
    // shadowed exactly as it was.

    // Weighted random selection.
    if upper.contains("WEIGHTED_PICK(") || upper.contains("WEIGHTED_PICK (") {
        return Some(weighted_pick::weighted_pick(state, identity, sql).await);
    }

    // Rate gate / cooldown functions.
    if upper.starts_with("SELECT RATE_CHECK(") || upper.starts_with("SELECT RATE_CHECK (") {
        return Some(rate_gate::rate_check(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_REMAINING(") || upper.starts_with("SELECT RATE_REMAINING (") {
        return Some(rate_gate::rate_remaining(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_RESET(") || upper.starts_with("SELECT RATE_RESET (") {
        return Some(rate_gate::rate_reset(state, identity, sql).await);
    }

    // Atomic transfer functions.
    if upper.starts_with("SELECT TRANSFER(") || upper.starts_with("SELECT TRANSFER (") {
        return Some(transfer::transfer(state, identity, sql).await);
    }
    if upper.starts_with("SELECT TRANSFER_ITEM(") || upper.starts_with("SELECT TRANSFER_ITEM (") {
        return Some(transfer::transfer_item(state, identity, sql).await);
    }

    // Sorted index DDL.
    if upper.starts_with("CREATE SORTED INDEX ") {
        return Some(kv_sorted_index::create_sorted_index(state, identity, sql).await);
    }
    if upper.starts_with("DROP SORTED INDEX ") {
        return Some(kv_sorted_index::drop_sorted_index(state, identity, sql).await);
    }

    // Sorted index query functions.
    if upper.starts_with("SELECT RANK(") || upper.starts_with("SELECT RANK (") {
        return Some(kv_sorted_index::select_rank(state, identity, sql).await);
    }
    if upper.contains("TOPK(") || upper.contains("TOPK (") {
        return Some(kv_sorted_index::select_topk(state, identity, sql).await);
    }
    if upper.starts_with("SELECT SORTED_COUNT(") || upper.starts_with("SELECT SORTED_COUNT (") {
        return Some(kv_sorted_index::select_sorted_count(state, identity, sql).await);
    }
    // RANGE as a sorted index function (check it's not a standard SQL RANGE).
    if (upper.starts_with("SELECT * FROM RANGE(") || upper.starts_with("SELECT * FROM RANGE ("))
        && !upper.contains(" BETWEEN ")
    {
        return Some(kv_sorted_index::select_range(state, identity, sql).await);
    }

    // KV_INCR / KV_DECR / KV_INCR_FLOAT / KV_CAS / KV_GETSET — atomic KV operations.
    if upper.starts_with("SELECT KV_INCR(") || upper.starts_with("SELECT KV_INCR (") {
        return Some(kv_atomic::kv_incr(state, identity, sql, false).await);
    }
    if upper.starts_with("SELECT KV_DECR(") || upper.starts_with("SELECT KV_DECR (") {
        return Some(kv_atomic::kv_incr(state, identity, sql, true).await);
    }
    if upper.starts_with("SELECT KV_INCR_FLOAT(") || upper.starts_with("SELECT KV_INCR_FLOAT (") {
        return Some(kv_atomic::kv_incr_float(state, identity, sql).await);
    }
    if upper.starts_with("SELECT KV_CAS(") || upper.starts_with("SELECT KV_CAS (") {
        return Some(kv_atomic::kv_cas(state, identity, sql).await);
    }
    if upper.starts_with("SELECT KV_GETSET(") || upper.starts_with("SELECT KV_GETSET (") {
        return Some(kv_atomic::kv_getset(state, identity, sql).await);
    }

    // Timeseries: CREATE TIMESERIES, ALTER TIMESERIES, REWRITE PARTITIONS.
    // (SHOW PARTITIONS is shadowed by consumer_group above, as noted.)
    if upper.starts_with("CREATE TIMESERIES ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::create_timeseries(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper.starts_with("ALTER TIMESERIES ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::alter_timeseries(state, identity, &parts));
    }
    if upper.starts_with("REWRITE PARTITIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::rewrite_partitions(state, identity, &parts));
    }

    // Last-value cache queries.
    if upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUES('collection_name')
        if let Some(collection) = extract_last_values_arg(sql) {
            return Some(
                last_value::query_last_values(state, identity, database_id, &collection).await,
            );
        }
    }
    if upper.starts_with("SELECT LAST_VALUE(") && !upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUE('collection_name', series_id)
        if let Some((collection, series_id)) = extract_last_value_args(sql) {
            return Some(
                last_value::query_last_value(state, identity, database_id, &collection, series_id)
                    .await,
            );
        }
    }

    // Materialized views (HTAP). `REFRESH MATERIALIZED VIEW` parses into no typed
    // AST variant, and `SHOW MATERIALIZED VIEWS` parses into a typed
    // `StreamViewStmt::ShowMaterializedViews` but the pgwire admin router
    // dispatched it from the raw token slice by string prefix (the `SHOW
    // MATERIALIZED VIEW` prefix, trailing-space-less, captures both the plural
    // `SHOW MATERIALIZED VIEWS` and the bare-singular input). Replicate both here,
    // before the parse gate, so the prefix recognition and the `parts`-based name
    // extraction stay byte-identical. `CREATE` / `DROP MATERIALIZED VIEW` are
    // handled in the typed match below (they parse into typed StreamView variants).
    if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(materialized_view::refresh_materialized_view(state, identity, &parts).await);
    }
    if upper.starts_with("SHOW MATERIALIZED VIEW") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(materialized_view::show_materialized_views(
            state, identity, &parts,
        ));
    }

    // Continuous aggregates (timeseries). `SHOW CONTINUOUS AGGREGATES [FOR
    // <source>]` parses into a typed `StreamViewStmt::ShowContinuousAggregates`
    // but the pgwire admin router dispatched it from the raw token slice by
    // string prefix (the `SHOW CONTINUOUS AGGREGATE` prefix, trailing-space-less,
    // captures both the plural `SHOW CONTINUOUS AGGREGATES` and the bare-singular
    // input). Replicate that here, before the parse gate, so the prefix
    // recognition and the `parts`-based `FOR <source>` extraction stay
    // byte-identical. `CREATE` / `DROP CONTINUOUS AGGREGATE` are handled in the
    // typed match below (they parse into typed StreamView variants).
    if upper.starts_with("SHOW CONTINUOUS AGGREGATE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(
            continuous_agg::show_continuous_aggregates(state, identity, database_id, &parts).await,
        );
    }

    // Retention policies (timeseries). `SHOW RETENTION POLICIES` parses into a
    // typed `PolicyStmt::ShowRetentionPolicies`, but the pgwire admin router
    // dispatched it from the raw token slice by the `SHOW RETENTION POLIC`
    // prefix (trailing-space-less, captures both the plural `SHOW RETENTION
    // POLICIES` and the singular `SHOW RETENTION POLICY ON <collection>`).
    // Replicate that exactly here, before the parse gate, so the prefix
    // recognition and the `parts`-based `ON <collection>` filter stay
    // byte-identical. `CREATE` / `ALTER` / `DROP RETENTION POLICY` are handled in
    // the typed match below (they parse into typed Policy variants).
    if upper.starts_with("SHOW RETENTION POLIC") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(retention_policy::show_retention_policy(
            state,
            identity,
            database_id,
            &parts,
        ));
    }

    // DSL extensions (custom SQL-like surfaces). None of these are dispatched
    // from a typed AST arm — the pgwire dsl router recognized all six by string
    // prefix from the raw SQL. Replicate that exactly here, before the parse
    // gate, so the prefix recognition and syntax messages stay byte-identical.
    // `SEARCH ... USING FUSION` must precede the parse gate because it would
    // otherwise parse into a typed graph statement and be captured by the graph
    // dispatch below. `SEARCH ... USING VECTOR(...)` never reaches here — it is
    // preprocessor-rewritten to a canonical `SELECT ... vector_distance(...)`.
    if upper.starts_with("SEARCH ") && upper.contains("USING FUSION") {
        return Some(dsl::search_fusion(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE VECTOR INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::create_vector_index(state, identity, &parts).await);
    }
    if upper.starts_with("CREATE FULLTEXT INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::create_fulltext_index(state, identity, &parts));
    }
    if upper.starts_with("CREATE SEARCH INDEX ") {
        return Some(dsl::create_search_index(state, identity, sql));
    }
    if upper.starts_with("CREATE SPARSE INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::create_sparse_index(state, identity, &parts));
    }
    // CREATE SPATIAL INDEX — string-recognized (no typed AST variant); the pgwire
    // schema string router dispatched it from the raw token slice. Replicate that
    // exactly here, before the parse gate.
    if upper.starts_with("CREATE SPATIAL INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(spatial::create_spatial_index(state, identity, &parts));
    }
    if upper.starts_with("CRDT MERGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::crdt_merge(state, identity, database_id, &parts).await);
    }
    // `SELECT crdt_state(...)` / `SELECT crdt_apply(...)` CRDT DSL functions —
    // string-recognized (they parse into no typed DDL variant). The pgwire dsl
    // string router recognized both by prefix from the raw SQL; replicate that
    // exactly here, before the parse gate.
    if upper.starts_with("SELECT CRDT_STATE(") || upper.starts_with("SELECT CRDT_STATE (") {
        return Some(crdt_ops::crdt_state(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT CRDT_APPLY(") || upper.starts_with("SELECT CRDT_APPLY (") {
        return Some(crdt_ops::crdt_apply(state, identity, database_id, sql).await);
    }

    // Administrative introspection & audit: SHOW USERS, SHOW TENANTS, SHOW
    // ROLES, SHOW SESSION, EXPORT AUDIT, SHOW AUDIT IN DATABASE / WHERE / LOG,
    // SHOW GRANTS. `SHOW USERS`, `SHOW GRANTS`, and `SHOW AUDIT…` parse into
    // typed AST variants (`AuthStmt::ShowUsers` / `ShowGrants`,
    // `MiscStmt::ShowAuditLog`) and bare `SHOW TENANTS` into
    // `DatabaseStmt::ShowTenants`, but the pgwire typed-AST path has no arm for
    // any of them — they fell through to the admin/observability string router,
    // which dispatched them by prefix from the raw token slice. `SHOW ROLES`,
    // `SHOW SESSION`, and `EXPORT AUDIT` parse into no typed DDL variant.
    // Replicate the string dispatch exactly here, before the parse gate, so the
    // prefix recognition and the `parts`-based extraction stay byte-identical.
    // `SHOW SESSIONS` is excluded here (see the `session_admin::show_sessions`
    // arm above, which is now checked first) so the two never race. The
    // `TRUNCATE / DELETE / CLEAR AUDIT` guard stays on the transitional pgwire
    // path — it is not one of the migrated inspect handlers.
    if upper.starts_with("SHOW USERS") {
        return Some(inspect::show_users(state, identity));
    }
    // Exact-match only. Filtered forms (`SHOW TENANTS WITH NAME <name>`,
    // `SHOW TENANT <ident>`) are parsed into typed variants and routed through
    // the typed match below; a prefix match here would silently drop the filter
    // and list every tenant.
    if upper == "SHOW TENANTS" {
        return Some(inspect::show_tenants(state, identity));
    }
    if upper == "SHOW ROLES" || upper.starts_with("SHOW ROLES ") {
        return Some(inspect::show_roles(state, identity));
    }
    if upper.starts_with("SHOW SESSION") && !upper.starts_with("SHOW SESSIONS") {
        return Some(inspect::show_session(identity));
    }
    if upper.starts_with("EXPORT AUDIT") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::export_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT IN DATABASE") {
        // SHOW AUDIT IN DATABASE <name> [LIMIT <n>]
        // parts: ["SHOW", "AUDIT", "IN", "DATABASE", "<name>", ...]
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let db_name = if parts.len() >= 5 {
            parts[4]
        } else {
            return Some(Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "syntax: SHOW AUDIT IN DATABASE <name> [LIMIT <n>]".to_string(),
            }));
        };
        let limit = if parts.len() >= 7 && parts[5].eq_ignore_ascii_case("LIMIT") {
            parts[6].parse::<usize>().unwrap_or(100)
        } else {
            100
        };
        return Some(inspect_audit::show_audit_in_database(
            state, identity, db_name, limit,
        ));
    }
    if upper.starts_with("SHOW AUDIT WHERE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::show_audit_where(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::show_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW GRANTS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect::show_grants(state, identity, &parts));
    }

    // Impersonation & delegation: IMPERSONATE AUTH USER, STOP IMPERSONATION,
    // DELEGATE AUTH USER, REVOKE DELEGATION, SHOW DELEGATIONS. None of these
    // parse into any typed AST variant — the pgwire admin router dispatched
    // all five by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based extraction / syntax messages stay byte-identical.
    if upper.starts_with("IMPERSONATE AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::impersonate(state, identity, &parts));
    }
    if upper.starts_with("STOP IMPERSONATION") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::stop_impersonation(state, identity, &parts));
    }
    if upper.starts_with("DELEGATE AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::delegate(state, identity, &parts));
    }
    if upper.starts_with("REVOKE DELEGATION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::revoke_delegation(state, identity, &parts));
    }
    if upper.starts_with("SHOW DELEGATIONS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::show_delegations(state, identity, &parts));
    }

    // Session management: SHOW SESSIONS, KILL SESSION, KILL USER SESSIONS,
    // VERIFY AUDIT CHAIN. None of these parse into any typed AST variant —
    // the pgwire admin router dispatched all four by string prefix from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the prefix recognition and the `parts`-based extraction / syntax
    // messages stay byte-identical. `SHOW SESSIONS` is matched here (before
    // the observability `SHOW SESSION` prefix below), mirroring the pgwire
    // admin router's precedence over the pgwire observability router; the
    // `SHOW SESSION` guard below already excludes `SHOW SESSIONS` explicitly,
    // so the two never race regardless of which is checked first.
    if upper.starts_with("SHOW SESSIONS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::show_sessions(state, identity, &parts));
    }
    if upper.starts_with("KILL SESSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::kill_session(state, identity, &parts));
    }
    if upper.starts_with("KILL USER SESSIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::kill_user_sessions(state, identity, &parts));
    }
    if upper.starts_with("VERIFY AUDIT CHAIN") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::verify_audit_chain(state, identity, &parts));
    }

    // Administrative observability: SHOW SERVER STATS / SHOW STATS / SHOW
    // METRICS / SHOW MEMORY. None of these parse into a typed DDL AST variant —
    // the pgwire admin observability router recognized all four by the
    // exact-or-trailing-space prefix from the raw SQL. Replicate that exactly
    // here, before the parse gate, so the recognition (and the `SHOW SERVER
    // STATS` / `SHOW STATS` shared handler) stays byte-identical. `SHOW SERVER
    // STATS` is checked before `SHOW STATS` exactly as the pgwire router did.
    if upper == "SHOW SERVER STATS" || upper.starts_with("SHOW SERVER STATS ") {
        return Some(observability::show_server_stats(state, identity));
    }
    if upper == "SHOW STATS" || upper.starts_with("SHOW STATS ") {
        return Some(observability::show_server_stats(state, identity));
    }
    if upper == "SHOW METRICS" || upper.starts_with("SHOW METRICS ") {
        return Some(observability::show_metrics(state, identity));
    }
    if upper == "SHOW MEMORY" || upper.starts_with("SHOW MEMORY ") {
        return Some(observability::show_memory(state, identity));
    }

    // Permission / scope introspection: EXPLAIN PERMISSION / EXPLAIN SCOPE.
    // Neither parses into a typed DDL AST variant — the pgwire admin router
    // recognized both by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based extraction / syntax messages stay byte-identical. The
    // pgwire wire path reaches these full-`EXPLAIN …` statements through the
    // DDL dispatch (native / http always; pgwire only for the non-`EXPLAIN `
    // full-SQL dispatch), so recognizing them here preserves behavior; the
    // `EXPLAIN <query>` handler strips the leading `EXPLAIN ` and never yields a
    // `PERMISSION …` / `SCOPE …` prefix, so it is unaffected.
    if upper.starts_with("EXPLAIN PERMISSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(explain_ddl::explain_permission(state, identity, &parts));
    }
    if upper.starts_with("EXPLAIN SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(explain_ddl::explain_scope(state, identity, &parts));
    }

    // Usage metering: DEFINE METERING DIMENSION, SHOW USAGE FOR TENANT, EXPORT
    // USAGE, SHOW USAGE, SHOW QUOTA. None of these parse into a typed DDL AST
    // variant — the pgwire admin router recognized all five by string prefix
    // from the raw token slice. Replicate that exactly here, before the parse
    // gate, so the prefix recognition and the `parts`-based extraction / syntax
    // messages stay byte-identical. Guard ordering (SHOW USAGE FOR TENANT and
    // EXPORT USAGE before the broader SHOW USAGE) mirrors the pgwire router.
    if upper.starts_with("DEFINE METERING DIMENSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::define_dimension(state, identity, &parts));
    }
    if upper.starts_with("SHOW USAGE FOR TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_usage_for_tenant(state, identity, &parts));
    }
    if upper.starts_with("EXPORT USAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::export_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW USAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW QUOTA ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_quota(state, identity, &parts));
    }

    // Organization management. None of `CREATE ORG`, `ALTER ORG`, `DROP ORG`,
    // `SHOW ORGS`, or `SHOW MEMBERS OF ORG` parse into any typed AST variant —
    // the pgwire admin router dispatched all of them by string prefix from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the prefix recognition and the `parts`-based extraction / syntax messages
    // stay byte-identical.
    if upper.starts_with("CREATE ORG ")
        || upper.starts_with("ALTER ORG ")
        || upper.starts_with("DROP ORG ")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::handle_org(state, identity, &parts));
    }
    if upper.starts_with("SHOW ORGS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::show_orgs(state, identity, &parts));
    }
    if upper.starts_with("SHOW MEMBERS OF ORG") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::show_members(state, identity, &parts));
    }

    // Scope management: DEFINE / DROP / GRANT / REVOKE / ALTER / RENEW SCOPE,
    // SHOW MY SCOPES, SHOW SCOPES FOR, SHOW SCOPE GRANTS, SHOW SCOPE(S). None of
    // these parse into any typed AST variant — `GRANT SCOPE` / `REVOKE SCOPE`
    // are explicitly excluded from the typed grant parser (returning `None`),
    // and the rest have no grammar at all — so the pgwire admin router
    // dispatched all of them by string prefix from the raw token slice.
    // Replicate that exactly here, before the parse gate, so the prefix
    // recognition and the `parts`-based extraction / syntax messages stay
    // byte-identical. Guard ordering mirrors the pgwire admin router: `SHOW MY
    // SCOPES` and `SHOW SCOPES FOR ` are matched before the broader `SHOW SCOPE
    // GRANTS` / `SHOW SCOPE` pair (nothing between them in the pgwire router
    // claimed a scope input, so grouping them here is behavior-preserving), and
    // `SHOW SCOPE GRANTS` is checked before the `SHOW SCOPE` catch-all.
    if upper.starts_with("DEFINE SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::define_scope(state, identity, &parts));
    }
    if upper.starts_with("DROP SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::drop_scope(state, identity, &parts));
    }
    if upper.starts_with("GRANT SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::grant_scope(state, identity, &parts));
    }
    if upper.starts_with("REVOKE SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::revoke_scope(state, identity, &parts));
    }
    if upper.starts_with("ALTER SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::alter_scope(state, identity, &parts));
    }
    if upper.starts_with("SHOW MY SCOPES") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::show_my_scopes(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPES FOR ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::show_scopes_for(state, identity, &parts));
    }
    if upper.starts_with("RENEW SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::renew_scope(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPE GRANTS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::show_scope_grants(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::show_scopes(state, identity, &parts));
    }

    // Collection introspection: DESCRIBE <collection> / `\D <collection>`,
    // UNDROP COLLECTION|TABLE, SHOW COLLECTIONS, SHOW INDEXES|INDEX. All four
    // parse into typed `CollectionStmt` variants, but the pgwire schema string
    // router dispatched them by string prefix from the raw token slice, using
    // `parts`-based name / filter extraction and the `\D` alias that the typed
    // parser does not reproduce (`\D <coll>` never parses into
    // `DescribeCollection`; bare `\D` parses into `ShowCollections`; the
    // `SHOW INDEXES` typed `collection` field is `parts[2]`, not the handler's
    // `parts[3]` filter). Replicate the string dispatch exactly here, before the
    // parse gate, so the prefix recognition, `parts` extraction, and syntax
    // messages stay byte-identical. `DESCRIBE SEQUENCE` is excluded so it falls
    // through to the typed `DescribeSequence` arm (claimed by the sequence
    // family), exactly as it was before this block existed.
    if (upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE"))
        || upper.starts_with("\\D ")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::describe_collection(state, identity, &parts));
    }
    if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::undrop_collection(state, identity, &parts));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(collection::show_collections(state, identity));
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::show_indexes(state, identity, &parts));
    }

    // DROP INDEX <name>. Parses into a typed `CollectionStmt::DropIndex`, but
    // the pgwire schema string router dispatched it by string prefix from the
    // raw token slice (the pgwire typed guards / sync / async arms all returned
    // `None` for it), reading `parts[2]` for the name and handling IF EXISTS
    // inside `drop_index`. Replicate that exactly here, before the parse gate,
    // so the prefix recognition and `parts`-based name extraction stay
    // byte-identical.
    if upper.starts_with("DROP INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::drop_index(state, identity, &parts).await);
    }

    // Parse errors → let the pgwire path run, which re-parses and reproduces the
    // exact error handling for those inputs.
    //
    // Non-DDL statements (`None`) include the temporal / audit query functions —
    // `SELECT <FUNC>(...)` calls that never parse into a typed DDL AST. In the
    // pgwire router these were recognized by substring after the typed-AST parse
    // gate and the auth family; recognizing them here, in the `None` branch,
    // preserves that ordering exactly (any typed DDL whose body contains one of
    // the substrings is handled by the typed match above first). A non-match
    // returns `None` so the transitional pgwire delegation handles it unchanged.
    let stmt = match nodedb_sql::ddl_ast::parse(sql) {
        Some(Ok(stmt)) => stmt,
        Some(Err(_)) => return None,
        None => {
            // Bulk import: `COPY <collection> FROM STDIN [WITH (...)]`. The
            // file-path form (`COPY … FROM '<path>'`) parses into a typed
            // `MiscStmt::CopyFromFile` and stays on the transitional pgwire path;
            // the STDIN form parses into no typed variant (`ddl_ast::parse`
            // returns `None`) and reached the pgwire `dsl` string router, which
            // ran after the typed-AST parse gate. Recognizing it here in the
            // `None` branch preserves that ordering exactly — the file form never
            // reaches this arm, so it is not diverted from the typed handler.
            if upper.starts_with("COPY ") && upper.contains(" FROM ") {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                return Some(bulk::copy_from(state, identity, &parts).await);
            }
            return query_functions::try_dispatch(state, identity, sql).await;
        }
    };

    // `MATCH` pattern queries parse into `GraphStmt::MatchQuery`. The `match_ops`
    // handler re-parses the raw `sql` with the graph pattern compiler, so it is
    // dispatched here from the typed arm with the original SQL (matching the
    // pgwire `dsl` router's MatchQuery branch). It must precede the general graph
    // dispatch below, which does not own `MatchQuery`.
    if let NodedbStatement::Graph(GraphStmt::MatchQuery { .. }) = &stmt {
        return Some(match_ops::match_query(state, identity, database_id, sql).await);
    }

    // Graph-overlay statements (GRAPH INSERT/DELETE EDGE, GRAPH LABEL/UNLABEL,
    // GRAPH TRAVERSE/NEIGHBORS/PATH, GRAPH ALGO, GRAPH RAG FUSION, SHOW GRAPH
    // STATS) parse into typed `GraphStmt` variants. In the pgwire router these
    // were dispatched from the typed AST by the `dsl` string router (last).
    // Recognizing them here on the typed path preserves that: `dispatch_graph`
    // returns `Some` for the graph-overlay variants and `None` otherwise.
    if let NodedbStatement::Graph(_) = &stmt {
        return graph_ops::dispatch_graph(state, identity, database_id, stmt).await;
    }

    match &stmt {
        NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
            name,
            collection,
            with_clause_raw,
        }) => Some(change_stream::create_change_stream(
            state,
            identity,
            name,
            collection,
            with_clause_raw,
        )),

        NodedbStatement::StreamView(StreamViewStmt::AlterChangeStream { name, action }) => Some(
            change_stream::alter_change_stream(state, identity, name, action),
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropChangeStream { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing change stream returns the tag before the token
            // handler runs. The `if_exists: false` case and the existing-stream
            // case fall through to `drop_change_stream`, which re-derives the
            // name / IF EXISTS from `parts` exactly as the pgwire streaming
            // string dispatch did.
            if *if_exists && !change_stream::change_stream_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CHANGE STREAM".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(change_stream::drop_change_stream(state, identity, &parts))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateConsumerGroup {
            group_name,
            stream_name,
        }) => Some(consumer_group::create_consumer_group(
            state,
            identity,
            group_name,
            stream_name,
        )),

        NodedbStatement::StreamView(StreamViewStmt::DropConsumerGroup {
            name,
            stream,
            if_exists,
        }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing consumer group returns the tag before the token
            // handler runs. The `if_exists: false` case and the existing-group
            // case fall through to `drop_consumer_group`, which re-derives the
            // name / stream from `parts` exactly as the pgwire streaming string
            // dispatch did. The guard checks the in-memory group registry for the
            // identity tenant using the parsed name / stream verbatim.
            let tid = identity.tenant_id.as_u64();
            if *if_exists && state.group_registry.get(tid, stream, name).is_none() {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CONSUMER GROUP".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(consumer_group::drop_consumer_group(state, identity, &parts))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            refresh_mode,
        }) => Some(
            materialized_view::create_materialized_view(
                state,
                identity,
                name,
                source,
                query_sql,
                refresh_mode,
            )
            .await,
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropMaterializedView { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing materialized view returns the tag before the token
            // handler runs. The existence check reads the in-memory registry
            // (`mv_registry`) for the identity tenant exactly as the pgwire guard
            // did. The `if_exists: false` case and the existing-view case fall
            // through to `drop_materialized_view`, which re-derives the name / IF
            // EXISTS from `parts` (and runs its own catalog-based existence check)
            // exactly as the pgwire admin string dispatch did.
            if *if_exists && !materialized_view::materialized_view_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP MATERIALIZED VIEW".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(materialized_view::drop_materialized_view(
                state, identity, &parts,
            ))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            group_by,
            with_clause_raw,
        }) => Some(
            continuous_agg::create_continuous_aggregate(
                state,
                identity,
                &continuous_agg::CreateContinuousAggregateRequest {
                    name,
                    source,
                    bucket_raw,
                    aggregate_exprs_raw,
                    group_by,
                    with_clause_raw,
                    database_id,
                },
            )
            .await,
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropContinuousAggregate {
            name,
            if_exists,
        }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing continuous aggregate returns the tag before the token
            // handler runs. The existence check reads the in-memory registry
            // (`mv_registry`) for the identity tenant exactly as the pgwire guard
            // did. The `if_exists: false` case and the existing-aggregate case
            // fall through to `drop_continuous_aggregate`, which re-derives the
            // name from `parts[3]` exactly as the pgwire admin string dispatch
            // did.
            if *if_exists && !continuous_agg::continuous_aggregate_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CONTINUOUS AGGREGATE".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(
                continuous_agg::drop_continuous_aggregate(state, identity, database_id, &parts)
                    .await,
            )
        }

        // CREATE COLLECTION / CREATE TABLE. Migrated from the pgwire typed-AST
        // async router (`async_ops`) plus the `if_not_exists: true` guard
        // short-circuit that used to live in the pgwire `guards` module
        // (checked here, inline, before the create handler runs — same
        // ordering). `build_and_persist` (name/duplicate/engine validation,
        // schema construction, `StoredCollection` assembly, propose+apply,
        // SERIAL sequence auto-creation) and the `dispatch_register_by_name`
        // follow-up dispatch are preserved verbatim in `collection::create`.
        NodedbStatement::Collection(CollectionStmt::CreateCollection {
            name,
            if_not_exists,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        }) => {
            if *if_not_exists && collection_exists(state, identity, name, database_id) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "CREATE COLLECTION".to_string(),
                    rows_affected: None,
                }]));
            }
            let result = collection::create_collection(
                state,
                identity,
                &collection::CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
                database_id,
            )
            .await;
            let result = match result {
                Ok(resp) => {
                    collection::dispatch_register_by_name(state, identity, name, database_id)
                        .await
                        .map(|()| resp)
                        .map_err(|e| DdlError {
                            sqlstate: "XX000".to_string(),
                            message: e.to_string(),
                        })
                }
                Err(e) => Err(e),
            };
            Some(result)
        }

        NodedbStatement::Collection(CollectionStmt::CreateTable {
            name,
            // Both false (normal create) and true (IF NOT EXISTS — guard
            // already returned early if the collection existed) fall
            // through to the same create_table handler.
            if_not_exists: _,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        }) => {
            let result = collection::create_table(
                state,
                identity,
                &collection::CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
                database_id,
            )
            .await;
            let result = match result {
                Ok(resp) => {
                    collection::dispatch_register_by_name(state, identity, name, database_id)
                        .await
                        .map(|()| resp)
                        .map_err(|e| DdlError {
                            sqlstate: "XX000".to_string(),
                            message: e.to_string(),
                        })
                }
                Err(e) => Err(e),
            };
            Some(result)
        }

        // DROP { COLLECTION | TABLE } [IF EXISTS] <name> [PURGE] [CASCADE
        // [FORCE]] — parser folds both spellings into `DropCollection`.
        // Migrated from the pgwire typed-AST sync router (`sync_ops`). The
        // handler honours `if_exists` internally via its existence-check
        // matrix (no guard short-circuit); the catalog propose + single-node
        // fallback, cascade dependent enumeration, soft vs hard delete, the
        // implicit-sequence sweep, and the audit pair are preserved verbatim
        // in `collection::drop`.
        NodedbStatement::Collection(CollectionStmt::DropCollection {
            name,
            if_exists,
            purge,
            cascade,
            cascade_force,
        }) => Some(collection::drop_collection(
            state,
            identity,
            name,
            *if_exists,
            *purge,
            *cascade,
            *cascade_force,
        )),

        // CREATE [UNIQUE] INDEX [name] ON <collection> (<field>) [WHERE ...].
        // Migrated from the pgwire typed-AST async router (`async_ops`). The
        // two-phase Building→Ready backfill, peer fan-out, Register refresh,
        // and owner-ledger propose are preserved verbatim in `collection::index`.
        NodedbStatement::Collection(CollectionStmt::CreateIndex {
            unique,
            index_name,
            collection: coll,
            field,
            case_insensitive,
            where_condition,
        }) => Some(
            collection::create_index(
                state,
                identity,
                &collection::CreateIndexRequest {
                    is_unique: *unique,
                    index_name_opt: index_name.as_deref(),
                    collection: coll,
                    field,
                    case_insensitive: *case_insensitive,
                    where_condition: where_condition.as_deref(),
                },
            )
            .await,
        ),

        NodedbStatement::Collection(CollectionStmt::CreateSequence {
            name,
            if_not_exists,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            format_template_raw,
            reset_period_raw,
            gap_free,
            scope,
        }) => {
            // IF NOT EXISTS on a non-existing sequence falls through to the
            // planner today (the pgwire guard returned None and no create arm
            // matched `if_not_exists: true`). Replicate by returning None.
            let tenant_id = identity.tenant_id.as_u64();
            if *if_not_exists && !state.sequence_registry.exists(tenant_id, name) {
                return None;
            }
            Some(sequence::create_sequence(
                state,
                identity,
                &CreateSequenceRequest {
                    name,
                    if_not_exists: *if_not_exists,
                    start: *start,
                    increment: *increment,
                    min_value: *min_value,
                    max_value: *max_value,
                    cycle: *cycle,
                    cache: *cache,
                    format_template_raw: format_template_raw.as_deref(),
                    reset_period_raw: reset_period_raw.as_deref(),
                    gap_free: *gap_free,
                    scope: scope.as_deref(),
                },
            ))
        }

        NodedbStatement::Collection(CollectionStmt::AlterSequence {
            name,
            action,
            with_value,
        }) => Some(sequence::alter_sequence(
            state,
            identity,
            name,
            action,
            with_value.as_deref(),
        )),

        NodedbStatement::Collection(CollectionStmt::DropSequence { name, if_exists }) => {
            Some(sequence::drop_sequence(state, identity, name, *if_exists))
        }

        NodedbStatement::Collection(CollectionStmt::ShowSequences) => {
            Some(sequence::show_sequences(state, identity))
        }

        NodedbStatement::Collection(CollectionStmt::DescribeSequence { name }) => {
            Some(sequence::describe_sequence(state, identity, name))
        }

        // CRDT conflict-policy update: `ALTER COLLECTION <name> SET ON CONFLICT
        // <policy> FOR <kind>`. This parses into `CollectionStmt::AlterCollection`
        // with an `AlterCollectionOp::SetOnConflict` operation, dispatched from
        // the pgwire typed-AST ALTER-COLLECTION router (`dispatch_alter_collection`).
        // Only `SetOnConflict` is migrated; every other `AlterCollectionOp`
        // returns `None` so it falls through to the transitional pgwire path
        // unchanged.
        NodedbStatement::Collection(CollectionStmt::AlterCollection {
            name,
            operation:
                AlterCollectionOp::SetOnConflict {
                    policy,
                    constraint_kind,
                },
        }) => Some(
            conflict_policy::alter_set_on_conflict(
                state,
                identity,
                database_id,
                name,
                policy,
                constraint_kind,
            )
            .await,
        ),

        // `SHOW CONFLICT POLICY ON <collection>`. Parses into a typed
        // `PolicyStmt::ShowConflictPolicy` and was dispatched from the pgwire
        // typed-AST async router. The Data Plane `GetPolicy` read is preserved
        // verbatim in `conflict_policy`.
        NodedbStatement::Policy(PolicyStmt::ShowConflictPolicy { collection }) => Some(
            conflict_policy::show_conflict_policy(state, identity, database_id, collection).await,
        ),

        NodedbStatement::Collection(CollectionStmt::Reindex {
            collection,
            index_name,
            concurrent,
        }) => Some(
            maintenance::handle_reindex(
                state,
                identity,
                collection,
                index_name.as_deref(),
                *concurrent,
                database_id,
            )
            .await,
        ),

        NodedbStatement::Policy(PolicyStmt::CreateRlsPolicy {
            name,
            collection,
            policy_type,
            predicate_raw,
            is_restrictive,
            on_deny_raw,
            tenant_id_override,
        }) => Some(rls::create_rls_policy(
            state,
            identity,
            &CreateRlsPolicyRequest {
                name,
                collection,
                policy_type_raw: policy_type,
                predicate_raw,
                is_restrictive: *is_restrictive,
                on_deny_raw: on_deny_raw.as_deref(),
                tenant_id_override: *tenant_id_override,
            },
        )),

        NodedbStatement::Policy(PolicyStmt::DropRlsPolicy {
            name,
            collection,
            if_exists,
        }) => {
            // IF EXISTS on a non-existing policy short-circuits to the tag,
            // folded from the pgwire guard (which checks existence against the
            // identity tenant). The existing case and the non-IF-EXISTS case
            // fall through to the token-based handler, which re-derives the name
            // / collection / TENANT override from `parts` exactly as the pgwire
            // string dispatch did.
            let tid = identity.tenant_id.as_u64();
            if *if_exists && !state.rls.policy_exists(tid, collection, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP RLS POLICY".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(rls::drop_rls_policy(state, identity, &parts))
        }

        NodedbStatement::Policy(PolicyStmt::ShowRlsPolicies { .. }) => {
            // The AST recognizes the broader `SHOW RLS POLI…` prefix, but the
            // pgwire string dispatch only handled `SHOW RLS POLICIES` /
            // `SHOW RLS POLICY`; narrower inputs fell through to the planner.
            // Replicate that exact prefix guard by returning None otherwise.
            let upper = sql.to_uppercase();
            if !(upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY")) {
                return None;
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(rls::show_rls_policies(state, identity, &parts))
        }

        NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
            Some(custom_type::create_enum_type(state, identity, name, labels))
        }

        NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => Some(
            custom_type::create_composite_type(state, identity, name, fields),
        ),

        NodedbStatement::Policy(PolicyStmt::DropType { name, if_exists }) => {
            Some(custom_type::drop_type(state, identity, name, *if_exists))
        }

        NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue { type_name, label }) => Some(
            custom_type::alter_type_add_value(state, identity, type_name, label),
        ),

        NodedbStatement::Policy(PolicyStmt::ShowTypes) => {
            Some(custom_type::show_types(state, identity))
        }

        NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            password,
            role,
            tenant,
            if_not_exists,
        }) => Some(user::create_user(
            state,
            identity,
            username,
            password,
            role.as_deref(),
            tenant.as_ref(),
            *if_not_exists,
        )),

        NodedbStatement::Auth(AuthStmt::AlterUser { username, op }) => {
            Some(user::alter_user(state, identity, username, op))
        }

        NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) => {
            Some(role::alter_role_typed(state, identity, name, sub_op))
        }

        NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
            Some(grant::role::grant_role(state, identity, roles, grantee))
        }

        NodedbStatement::Auth(AuthStmt::RevokeRole { roles, grantee }) => {
            Some(grant::role::revoke_role(state, identity, roles, grantee))
        }

        NodedbStatement::Auth(AuthStmt::GrantPermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(grant::permission::grant_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokePermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(grant::permission::revoke_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::GrantDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(grant::database_permission::grant_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokeDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(grant::database_permission::revoke_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
            name,
            issuer,
            jwks_uri,
            audience,
            claim_mappings,
        }) => Some(oidc::create_oidc_provider(
            state,
            identity,
            name,
            issuer,
            jwks_uri,
            audience.as_deref(),
            claim_mappings,
        )),

        NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            name,
            claim_mappings,
        }) => Some(oidc::alter_oidc_provider_claim_mapping(
            state,
            identity,
            name,
            claim_mappings,
        )),

        NodedbStatement::Auth(AuthStmt::DropOidcProvider { name, if_exists }) => {
            Some(oidc::drop_oidc_provider(state, identity, name, *if_exists))
        }

        NodedbStatement::Auth(AuthStmt::ShowOidcProviders) => {
            Some(oidc::show_oidc_providers(state, identity))
        }

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
            *or_replace,
            execution_mode,
            name,
            timing,
            *events_insert,
            *events_update,
            *events_delete,
            collection,
            granularity,
            when_condition.as_deref(),
            *priority,
            security,
            body_sql,
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
            Some(trigger::show_triggers(state, identity, &parts))
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
            if *if_exists && !schedule::schedule_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP SCHEDULE".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(schedule::drop_schedule(state, identity, &parts))
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

        NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        }) => Some(
            retention_policy::create_retention_policy(
                state,
                identity,
                database_id,
                name,
                collection,
                body_raw,
                eval_interval_raw.as_deref(),
            )
            .await,
        ),

        NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
            name,
            action,
            set_key,
            set_value,
        }) => Some(retention_policy::alter_retention_policy(
            state,
            identity,
            database_id,
            name,
            action,
            set_key.as_deref(),
            set_value.as_deref(),
        )),

        NodedbStatement::Policy(PolicyStmt::DropRetentionPolicy { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing retention policy returns the tag before the token
            // handler runs (and before the tenant-admin gate). The existence
            // check reads the in-memory `retention_policy_registry` for the
            // identity tenant scoped to the session database, exactly as the
            // pgwire guard (`retention_policy_exists`) did. The `if_exists:
            // false` case and the existing-policy case fall through to
            // `drop_retention_policy`, which re-derives the name from `parts[3]`
            // exactly as the pgwire admin string dispatch did.
            let tid = identity.tenant_id.as_u64();
            if *if_exists
                && state
                    .retention_policy_registry
                    .get(database_id.as_u64(), tid, name)
                    .is_none()
            {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP RETENTION POLICY".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(
                retention_policy::drop_retention_policy(state, identity, database_id, &parts).await,
            )
        }

        NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => Some(
            synonym_group::create_synonym_group(state, identity, database_id, name, terms).await,
        ),

        NodedbStatement::Policy(PolicyStmt::DropSynonymGroup { name, if_exists }) => Some(
            synonym_group::drop_synonym_group(state, identity, database_id, name, *if_exists).await,
        ),

        NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups) => {
            Some(synonym_group::show_synonym_groups(state, identity))
        }

        NodedbStatement::Cluster(ClusterStmt::AlterRaftGroup {
            group_id,
            action,
            node_id,
        }) => Some(cluster::alter_raft_group(
            state, identity, group_id, action, node_id,
        )),

        // Database DDL family (CREATE / DROP / ALTER DATABASE, SHOW DATABASES /
        // QUOTA / USAGE / LINEAGE, CLONE / MIRROR / PROMOTE, BACKUP / RESTORE,
        // SHOW DATABASE MIRROR STATUS). Migrated from the pgwire typed-AST
        // database router (`database_ops`); all catalog / audit / gate side
        // effects are preserved verbatim in `database`.
        //
        // NOT here: `UseDatabase` (session-coupled, intercepted before the DDL
        // router). `AlterTenant` / `ShowTenantQuotaInDatabase` /
        // `ShowTenantUsageInDatabase` / `MoveTenant` are typed `DatabaseStmt`
        // variants too, but dispatch to the `tenant` family below, not `database`.
        NodedbStatement::Database(DatabaseStmt::CreateDatabase {
            name,
            if_not_exists,
            options,
        }) => Some(database::create::create_database(
            state,
            identity,
            name,
            *if_not_exists,
            options,
        )),

        NodedbStatement::Database(DatabaseStmt::DropDatabase {
            name,
            if_exists,
            cascade,
        }) => Some(database::drop::drop_database(
            state, identity, name, *if_exists, *cascade,
        )),

        NodedbStatement::Database(DatabaseStmt::AlterDatabase { name, operation }) => Some(
            database::alter::alter_database(state, identity, name, operation),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabases) => {
            Some(database::show::show_databases(state, identity))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseQuota { name }) => Some(
            database::show_quota::show_database_quota(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseUsage { name }) => Some(
            database::show_usage::show_database_usage(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseLineage { name }) => Some(
            database::show_lineage::show_database_lineage(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::CloneDatabase {
            new_name,
            source_name,
            as_of,
        }) => Some(database::clone::clone_database(
            state,
            identity,
            database::clone::CloneDatabaseParams {
                new_name,
                source_name,
                as_of,
            },
        )),

        NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
            local_name,
            source_cluster,
            source_database,
            mode,
        }) => Some(database::mirror::create::mirror_database(
            state,
            identity,
            local_name,
            source_cluster,
            source_database,
            *mode,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseMirrorStatus { name }) => Some(
            database::mirror::show::show_database_mirror_status(state, identity, name.as_deref()),
        ),

        NodedbStatement::Database(DatabaseStmt::BackupDatabase { name, .. }) => Some(
            database::backup_restore::backup_database(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::RestoreDatabase { name, .. }) => Some(
            database::backup_restore::restore_database(state, identity, name),
        ),

        // Tenant DDL family (`ALTER TENANT ... IN DATABASE ... SET QUOTA`,
        // `SHOW TENANT QUOTA|USAGE FOR ... IN DATABASE ...`). These parse into
        // typed `DatabaseStmt` variants and were dispatched from the pgwire
        // typed-AST database router (`database_ops`); all catalog / audit /
        // gate side effects are preserved verbatim in `tenant`.
        NodedbStatement::Database(DatabaseStmt::AlterTenant {
            name,
            database,
            operation,
        }) => Some(tenant::handle_alter_tenant_quota(
            state, identity, name, database, operation,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowTenantQuotaInDatabase { name, database }) => {
            Some(tenant::handle_show_tenant_quota_in_database(
                state, identity, name, database,
            ))
        }

        NodedbStatement::Database(DatabaseStmt::ShowTenantUsageInDatabase { name, database }) => {
            Some(tenant::handle_show_tenant_usage_in_database(
                state, identity, name, database,
            ))
        }

        // `MOVE TENANT <name> FROM <source_db> TO <target_db>` — async,
        // 5-phase re-parenting sequence. Parses into a typed `DatabaseStmt`
        // variant and was dispatched from the pgwire typed-AST async router
        // (`async_ops`); every phase (pre-flight, drain, snapshot, cutover,
        // resume), the journal, and the compensation paths are preserved
        // verbatim in `tenant::move_tenant`.
        NodedbStatement::Database(DatabaseStmt::MoveTenant {
            tenant_name,
            from_db,
            to_db,
        }) => Some(tenant::handle_move_tenant(state, identity, tenant_name, from_db, to_db).await),

        // Tenant introspection by identifier / name filter. These parse into
        // typed `DatabaseStmt` variants and were dispatched from the pgwire
        // typed-AST database router (`database_ops`). The credential / usage
        // reads are preserved verbatim in `inspect`.
        NodedbStatement::Database(DatabaseStmt::ShowTenantByIdentifier { ident }) => {
            Some(inspect::show_tenant_by_identifier(state, identity, ident))
        }

        NodedbStatement::Database(DatabaseStmt::ShowTenantsFilteredByName { name }) => Some(
            inspect::show_tenants_filtered_by_name(state, identity, name),
        ),

        // SHOW PERMISSIONS [ON <collection>] [FOR <grantee>]. Parses into a
        // typed `AuthStmt::ShowPermissions` and was dispatched from the pgwire
        // typed-AST sync router (`sync_ops`). The permission-store reads are
        // preserved verbatim in `inspect`.
        NodedbStatement::Auth(AuthStmt::ShowPermissions {
            on_collection,
            for_grantee,
        }) => Some(inspect::show_permissions(
            state,
            identity,
            on_collection.as_deref(),
            for_grantee.as_deref(),
        )),

        _ => None,
    }
}

/// Existence check backing the `CreateCollection` `if_not_exists: true`
/// short-circuit above.
///
/// Relocated verbatim from the pgwire `router::ast::exists::collection_exists`
/// helper (now deleted, along with the pgwire guard arms that were its only
/// callers).
fn collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database_id: DatabaseId,
) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_collection(database_id, tid, name), Ok(Some(_)))
}

/// Extract the single-quoted collection argument from `SELECT LAST_VALUES('coll')`.
///
/// Mirrors the pgwire router's `extract_quoted_arg(sql, "LAST_VALUES(")` exactly
/// so the parse behaviour stays byte-identical.
fn extract_last_values_arg(sql: &str) -> Option<String> {
    let prefix = "LAST_VALUES(";
    let upper = sql.to_uppercase();
    let pos = upper.find(prefix)?;
    let after = &sql[pos + prefix.len()..];
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract `('collection', series_id)` from a `SELECT LAST_VALUE(...)` call.
///
/// Mirrors the pgwire router's `extract_lv_args` exactly.
fn extract_last_value_args(sql: &str) -> Option<(String, u64)> {
    let upper = sql.to_uppercase();
    let pos = upper.find("LAST_VALUE(")?;
    let after = &sql[pos + 11..];
    let close = after.find(')')?;
    let inner = &after[..close];
    let parts: Vec<&str> = inner.splitn(2, ',').collect();
    if parts.len() != 2 {
        return None;
    }
    let collection = parts[0].trim().trim_matches('\'').to_string();
    let series_id: u64 = parts[1].trim().parse().ok()?;
    Some((collection, series_id))
}
