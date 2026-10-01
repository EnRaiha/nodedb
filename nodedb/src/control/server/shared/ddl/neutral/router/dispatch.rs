// SPDX-License-Identifier: BUSL-1.1

//! DDL dispatch orchestrator.
//!
//! [`try_dispatch`] runs the string-recognized family clusters (in order,
//! return-on-first-match), then the typed parse gate, then the typed family
//! clusters; every other statement returns `None` so the transitional pgwire
//! delegation in `super::super::super::dispatch` handles it.

use nodedb_sql::ddl_ast::statement::{GraphStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::bulk;
use super::super::collection;
use super::super::graph_ops;
use super::super::match_ops;
use super::super::query_functions;

use super::{
    string_admin, string_engine_ops, string_introspection, string_schema, string_streaming,
    string_versioning, typed_auth, typed_automation, typed_collection, typed_database, typed_misc,
    typed_policy, typed_streamview,
};

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
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Trim so prefix-anchored arms (SELECT TOPK(, SELECT RANK(, …) still
    // match a statement that arrives with leading whitespace or a newline.
    let upper = sql.trim().to_uppercase();

    if let Some(r) = string_admin::try_string(state, identity, sql, &upper, database_id).await {
        return Some(r);
    }
    if let Some(r) = string_schema::try_string(state, identity, sql, &upper, database_id).await {
        return Some(r);
    }
    if let Some(r) =
        string_streaming::try_string(state, identity, sql, &upper, database_id, txn_ctx).await
    {
        return Some(r);
    }
    if let Some(r) =
        string_versioning::try_string(state, identity, sql, &upper, database_id, txn_ctx).await
    {
        return Some(r);
    }
    if let Some(r) =
        string_engine_ops::try_string(state, identity, sql, &upper, database_id, txn_ctx).await
    {
        return Some(r);
    }
    if let Some(r) =
        string_introspection::try_string(state, identity, sql, &upper, database_id).await
    {
        return Some(r);
    }

    // Parse errors surface as a typed `DdlError` here, with the SQLSTATE the
    // planner path gives the same `SqlError` (`UnsupportedConstraint` is
    // `0A000`, a parse error `42601`) and the parser's own `Display` text as
    // the message. This is the sole parse-error gate for the DDL router; the
    // GRAPH / MATCH / SHOW GRAPH STATS prefixed inputs reach their parse
    // error through this arm.
    //
    // Non-DDL statements (`None`) include the temporal / audit query functions —
    // `SELECT <FUNC>(...)` calls that never parse into a typed DDL AST. They are
    // recognized by substring after the typed-AST parse gate and the auth
    // family, in the `None` branch (any typed DDL whose body contains one of
    // the substrings is handled by the typed match above first). A non-match
    // returns `None` so the caller falls through to the SQL planner.
    let stmt = match nodedb_sql::ddl_ast::parse(sql) {
        Some(Ok(stmt)) => stmt,
        Some(Err(e)) => {
            // The SQLSTATE the planner path renders for the same error, so a
            // parse refusal answers one class wherever it is raised.
            let message = e.to_string();
            let (_, sqlstate, _) = crate::control::server::pgwire::types::error_to_sqlstate(
                &crate::control::planner::plan_error_map::map_plan_error(e, identity.tenant_id),
            );
            return Some(Err(DdlError::new(sqlstate, message)));
        }
        None => {
            // Bulk import: `COPY <collection> FROM STDIN [WITH (...)]`. The
            // file-path form (`COPY … FROM '<path>'`) parses into a typed
            // `MiscStmt::CopyFromFile`, handled by the typed arm above; the
            // STDIN form parses into no typed variant (`ddl_ast::parse`
            // returns `None`) and reached the pgwire `dsl` string router, which
            // ran after the typed-AST parse gate. Recognizing it here in the
            // `None` branch preserves that ordering exactly — the file form never
            // reaches this arm, so it is not diverted from the typed handler.
            if upper.starts_with("COPY ") && upper.contains(" FROM ") {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                return Some(bulk::copy_from(state, identity, &parts).await);
            }

            // INSERT INTO x { } — object literal syntax; intercept for
            // trigger/sequence handling. It runs after the typed-AST parse
            // gate, in the `None` branch.
            if sql
                .get(.."INSERT INTO ".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("INSERT INTO "))
            {
                let after_into = sql
                    .get("INSERT INTO ".len()..)
                    .unwrap_or_default()
                    .trim_start();
                let coll_end = after_into
                    .find(|c: char| c.is_whitespace())
                    .unwrap_or(after_into.len());
                if after_into
                    .get(coll_end..)
                    .unwrap_or_default()
                    .trim_start()
                    .starts_with('{')
                    && let Some(result) =
                        collection::insert_document(state, identity, database_id, sql, txn_ctx)
                            .await
                {
                    return Some(result);
                }
            }

            // UPSERT INTO — same as INSERT but merges into existing document
            // if it exists. Handles both (cols) VALUES (vals) and { } object
            // literal forms.
            if sql
                .get(.."UPSERT INTO ".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("UPSERT INTO "))
                && (upper.contains("VALUES") || {
                    let after_into = sql
                        .get("UPSERT INTO ".len()..)
                        .unwrap_or_default()
                        .trim_start();
                    let coll_end = after_into
                        .find(|c: char| c.is_whitespace())
                        .unwrap_or(after_into.len());
                    after_into
                        .get(coll_end..)
                        .unwrap_or_default()
                        .trim_start()
                        .starts_with('{')
                })
                && let Some(result) =
                    collection::upsert_document(state, identity, database_id, sql, txn_ctx).await
            {
                return Some(result);
            }

            return query_functions::try_dispatch(state, identity, database_id, sql).await;
        }
    };

    // `MATCH` pattern queries parse into `GraphStmt::MatchQuery`. The `match_ops`
    // handler re-parses the raw `sql` with the graph pattern compiler, so it is
    // dispatched here from the typed arm with the original SQL (matching the
    // pgwire `dsl` router's MatchQuery branch). It must precede the general graph
    // dispatch below, which does not own `MatchQuery`.
    if let NodedbStatement::Graph(GraphStmt::MatchQuery { .. }) = &stmt {
        // Read-your-own-writes: a MATCH inside an explicit transaction must
        // observe the session's staged edge writes/deletes, so resolve the
        // active `TxnId` (idle sessions → `None`, an autocommit read) and hand
        // it to the MATCH handler for the Data-Plane overlay merge — mirroring
        // the single-hop `GRAPH NEIGHBORS` path.
        let (txn_id, _) = txn_ctx.sessions.txn_identity(txn_ctx.session_id);
        let read = crate::control::server::graph_dispatch::GraphRead {
            txn_id,
            linearizable: txn_ctx.linearizable_reads(),
        };
        return Some(match_ops::match_query(state, identity, database_id, sql, read).await);
    }

    // Graph-overlay statements (GRAPH INSERT/DELETE EDGE, GRAPH LABEL/UNLABEL,
    // GRAPH TRAVERSE/NEIGHBORS/PATH, GRAPH ALGO, GRAPH RAG FUSION, SHOW GRAPH
    // STATS) parse into typed `GraphStmt` variants. `dispatch_graph` returns `Some` for the
    // graph-overlay variants and `None` otherwise.
    if let NodedbStatement::Graph(_) = &stmt {
        // The graph-overlay handlers thread the session's transaction context
        // through `txn_ctx`: single-hop reads (Neighbors/Hop) resolve the
        // active `TxnId` for read-your-own-writes overlay merge, and edge
        // writes (`GRAPH DELETE EDGE`) stage into the per-transaction
        // `GraphTxnOverlay` through the neutral gate when `InBlock`.
        return graph_ops::dispatch_graph(state, identity, database_id, stmt, txn_ctx).await;
    }

    if let Some(r) = typed_misc::try_typed(state, identity, sql, database_id, &stmt, txn_ctx).await
    {
        return Some(r);
    }
    if let Some(r) = typed_streamview::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }
    if let Some(r) = typed_collection::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }
    if let Some(r) = typed_policy::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }
    if let Some(r) = typed_auth::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }
    if let Some(r) = typed_automation::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }
    if let Some(r) = typed_database::try_typed(state, identity, sql, database_id, &stmt).await {
        return Some(r);
    }

    None
}
