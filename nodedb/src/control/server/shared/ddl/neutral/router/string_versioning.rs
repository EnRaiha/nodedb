// SPDX-License-Identifier: BUSL-1.1

//! String-recognized versioning DDL arms: version history, maintenance,
//! cluster management, vector-index lifecycle, vector-model metadata, and
//! graph index / tree operations.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::cluster;
use super::super::collection;
use super::super::maintenance;
use super::super::tree_ops;
use super::super::version_history;

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
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
            state,
            identity,
            database_id,
            sql,
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
        if restore_forbidden_in_transaction(txn_ctx) {
            return Some(Err(DdlError {
                sqlstate: "25001".to_owned(),
                message: crate::Error::CrdtApplyForbiddenInTransaction.to_string(),
            }));
        }
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

    // Vector model metadata. None of these are dispatched from a typed AST arm —
    // `ALTER COLLECTION ... SET VECTOR METADATA ON` parses into no
    // `AlterCollectionOp` variant, and `SHOW VECTOR MODELS` / `SELECT
    // VECTOR_METADATA(...)` parse into no typed DDL AST at all. The pgwire
    // engine_ops router recognized all three by string prefix from the raw SQL.
    // Replicate that exactly here, before the parse gate, so the prefix
    // recognition (and the `ALTER COLLECTION ... SET VECTOR METADATA ON` guard
    // running before the typed `AlterCollection` parse handling) stays
    // byte-identical. The `SET VECTOR METADATA ON` guard precedes the typed
    // parse gate below, so it is never shadowed by the migrated typed
    // `AlterCollection` dispatch.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET VECTOR METADATA ON") {
        return Some(collection::handle_set_vector_metadata(
            state,
            identity,
            sql,
            database_id,
        ));
    }
    if upper.starts_with("SHOW VECTOR MODELS") {
        return Some(collection::handle_show_vector_models(state, identity));
    }
    if upper.starts_with("SELECT VECTOR_METADATA(") || upper.starts_with("SELECT VECTOR_METADATA (")
    {
        let inner = sql
            .find('(')
            .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
        if let Some(args_str) = inner {
            let args: Vec<&str> = args_str
                .split(',')
                .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
                .collect();
            if args.len() >= 2 && !args[0].is_empty() && !args[1].is_empty() {
                return Some(collection::handle_vector_metadata_query(
                    state,
                    identity,
                    &args[0].to_lowercase(),
                    &args[1].to_lowercase(),
                ));
            }
        }
        return Some(Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "usage: SELECT VECTOR_METADATA('collection', 'column')".to_string(),
        }));
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

    None
}

fn restore_forbidden_in_transaction(txn_ctx: &DmlTxnCtx<'_>) -> bool {
    txn_ctx.sessions.transaction_state(txn_ctx.session_id) != TransactionState::Idle
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{SessionId, SessionStore};

    #[test]
    fn restore_is_forbidden_in_active_or_failed_transactions() {
        let sessions = SessionStore::new();
        let addr = "127.0.0.1:5400".parse().expect("test address");
        sessions.ensure_session(addr);
        let ctx = DmlTxnCtx {
            sessions: &sessions,
            session_id: SessionId::from(&addr),
        };
        assert!(!restore_forbidden_in_transaction(&ctx));
        sessions
            .begin(addr, crate::types::Lsn::new(1), 0)
            .expect("begin");
        assert!(restore_forbidden_in_transaction(&ctx));
        sessions.fail_transaction(addr);
        assert!(restore_forbidden_in_transaction(&ctx));
    }
}
