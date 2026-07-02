// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL router.
//!
//! [`try_dispatch`] recognizes the migrated families and routes to them; every
//! other statement returns `None` so the transitional pgwire delegation in the
//! parent [`super::super::dispatch`] handles it.

use nodedb_sql::ddl_ast::statement::{
    AuthStmt, AutomationStmt, CollectionStmt, NodedbStatement, PolicyStmt, StreamViewStmt,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};
use super::alert::{self, CreateAlertRequest};
use super::change_stream;
use super::constraint;
use super::consumer_group;
use super::continuous_agg;
use super::custom_type;
use super::function;
use super::grant;
use super::materialized_view;
use super::oidc;
use super::procedure;
use super::query_functions;
use super::retention_policy;
use super::rls::{self, CreateRlsPolicyRequest};
use super::role;
use super::schedule::{self, CreateScheduleRequest};
use super::sequence::{self, CreateSequenceRequest};
use super::service_account;
use super::topic;
use super::trigger;
use super::typeguard;
use super::user;
use super::version_history;

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

        _ => None,
    }
}
