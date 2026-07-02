// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL router.
//!
//! [`try_dispatch`] recognizes the migrated families and routes to them; every
//! other statement returns `None` so the transitional pgwire delegation in the
//! parent [`super::super::dispatch`] handles it.

use nodedb_sql::ddl_ast::statement::{
    AuthStmt, AutomationStmt, ClusterStmt, CollectionStmt, DatabaseStmt, NodedbStatement,
    PolicyStmt, StreamViewStmt,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};
use super::alert::{self, CreateAlertRequest};
use super::change_stream;
use super::cluster;
use super::constraint;
use super::consumer_group;
use super::continuous_agg;
use super::custom_type;
use super::dsl;
use super::function;
use super::grant;
use super::graph_ops;
use super::inspect;
use super::inspect_audit;
use super::kv_atomic;
use super::kv_sorted_index;
use super::last_value;
use super::maintenance;
use super::materialized_view;
use super::oidc;
use super::procedure;
use super::query_functions;
use super::rate_gate;
use super::retention_policy;
use super::rls::{self, CreateRlsPolicyRequest};
use super::role;
use super::schedule::{self, CreateScheduleRequest};
use super::sequence::{self, CreateSequenceRequest};
use super::service_account;
use super::synonym_group;
use super::timeseries;
use super::topic;
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
    if upper.starts_with("CRDT MERGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::crdt_merge(state, identity, database_id, &parts).await);
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
    // `SHOW SESSIONS` is excluded because the admin router claimed it (via the
    // not-yet-migrated `session_ddl::show_sessions`) before the observability
    // `SHOW SESSION` prefix ran; the guard preserves that ordering. The
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
        None => return query_functions::try_dispatch(state, identity, sql).await,
    };

    // Graph-overlay statements (GRAPH INSERT/DELETE EDGE, GRAPH LABEL/UNLABEL,
    // GRAPH TRAVERSE/NEIGHBORS/PATH, GRAPH ALGO, GRAPH RAG FUSION, SHOW GRAPH
    // STATS) parse into typed `GraphStmt` variants. In the pgwire router these
    // were dispatched from the typed AST by the `dsl` string router (last),
    // after the `MATCH` pattern query was split off to `match_ops`. Recognizing
    // them here on the typed path preserves that: `dispatch_graph` returns
    // `Some` for the graph-overlay variants and `None` for `GraphStmt::MatchQuery`
    // (which stays on the pgwire `match_ops` path) so it falls through unchanged.
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
