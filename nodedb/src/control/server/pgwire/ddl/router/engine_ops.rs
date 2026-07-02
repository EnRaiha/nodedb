// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
    database_id: crate::types::DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // Vector index lifecycle (SHOW VECTOR INDEX, ALTER VECTOR INDEX SEAL /
    // COMPACT / SET) is served by the protocol-neutral DDL router; the pgwire
    // router no longer routes it.

    // Vector model metadata: ALTER COLLECTION ... SET VECTOR METADATA ON ...
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET VECTOR METADATA ON") {
        return Some(super::super::collection::handle_set_vector_metadata(
            state, identity, sql,
        ));
    }

    // SHOW VECTOR MODELS — catalog view.
    if upper.starts_with("SHOW VECTOR MODELS") || upper == "SHOW VECTOR MODELS" {
        return Some(super::super::collection::handle_show_vector_models(
            state, identity,
        ));
    }

    // SELECT VECTOR_METADATA('collection', 'column') — inline query.
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
                return Some(super::super::collection::handle_vector_metadata_query(
                    state,
                    identity,
                    &args[0].to_lowercase(),
                    &args[1].to_lowercase(),
                ));
            }
        }
        return Some(Err(super::super::super::types::sqlstate_error(
            "42601",
            "usage: SELECT VECTOR_METADATA('collection', 'column')",
        )));
    }

    // Weighted random selection.
    if upper.contains("WEIGHTED_PICK(") || upper.contains("WEIGHTED_PICK (") {
        return Some(super::super::weighted_pick::weighted_pick(state, identity, sql).await);
    }

    // Rate gate / cooldown functions.
    if upper.starts_with("SELECT RATE_CHECK(") || upper.starts_with("SELECT RATE_CHECK (") {
        return Some(super::super::rate_gate::rate_check(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_REMAINING(") || upper.starts_with("SELECT RATE_REMAINING (") {
        return Some(super::super::rate_gate::rate_remaining(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_RESET(") || upper.starts_with("SELECT RATE_RESET (") {
        return Some(super::super::rate_gate::rate_reset(state, identity, sql).await);
    }

    // Atomic transfer functions.
    if upper.starts_with("SELECT TRANSFER(") || upper.starts_with("SELECT TRANSFER (") {
        return Some(super::super::transfer::transfer(state, identity, sql).await);
    }
    if upper.starts_with("SELECT TRANSFER_ITEM(") || upper.starts_with("SELECT TRANSFER_ITEM (") {
        return Some(super::super::transfer::transfer_item(state, identity, sql).await);
    }

    // Sorted index DDL.
    if upper.starts_with("CREATE SORTED INDEX ") {
        return Some(
            super::super::kv_sorted_index::create_sorted_index(state, identity, sql).await,
        );
    }
    if upper.starts_with("DROP SORTED INDEX ") {
        return Some(super::super::kv_sorted_index::drop_sorted_index(state, identity, sql).await);
    }

    // Sorted index query functions.
    if upper.starts_with("SELECT RANK(") || upper.starts_with("SELECT RANK (") {
        return Some(super::super::kv_sorted_index::select_rank(state, identity, sql).await);
    }
    if upper.contains("TOPK(") || upper.contains("TOPK (") {
        return Some(super::super::kv_sorted_index::select_topk(state, identity, sql).await);
    }
    if upper.starts_with("SELECT SORTED_COUNT(") || upper.starts_with("SELECT SORTED_COUNT (") {
        return Some(
            super::super::kv_sorted_index::select_sorted_count(state, identity, sql).await,
        );
    }
    // RANGE as a sorted index function (check it's not a standard SQL RANGE).
    if (upper.starts_with("SELECT * FROM RANGE(") || upper.starts_with("SELECT * FROM RANGE ("))
        && !upper.contains(" BETWEEN ")
    {
        return Some(super::super::kv_sorted_index::select_range(state, identity, sql).await);
    }

    // KV_INCR / KV_DECR / KV_INCR_FLOAT / KV_CAS / KV_GETSET — atomic KV operations.
    if upper.starts_with("SELECT KV_INCR(") || upper.starts_with("SELECT KV_INCR (") {
        return Some(super::super::kv_atomic::kv_incr(state, identity, sql, false).await);
    }
    if upper.starts_with("SELECT KV_DECR(") || upper.starts_with("SELECT KV_DECR (") {
        return Some(super::super::kv_atomic::kv_incr(state, identity, sql, true).await);
    }
    if upper.starts_with("SELECT KV_INCR_FLOAT(") || upper.starts_with("SELECT KV_INCR_FLOAT (") {
        return Some(super::super::kv_atomic::kv_incr_float(state, identity, sql).await);
    }
    if upper.starts_with("SELECT KV_CAS(") || upper.starts_with("SELECT KV_CAS (") {
        return Some(super::super::kv_atomic::kv_cas(state, identity, sql).await);
    }
    if upper.starts_with("SELECT KV_GETSET(") || upper.starts_with("SELECT KV_GETSET (") {
        return Some(super::super::kv_atomic::kv_getset(state, identity, sql).await);
    }

    // Graph index and tree operations.
    if upper.starts_with("CREATE GRAPH INDEX ") {
        return Some(
            super::super::tree_ops::create_graph_index(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("SELECT TREE_SUM") || upper.starts_with("TREE_SUM") {
        return Some(super::super::tree_ops::tree_sum(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT TREE_CHILDREN") || upper.starts_with("TREE_CHILDREN") {
        return Some(
            super::super::tree_ops::tree_children(state, identity, database_id, sql).await,
        );
    }

    // Timeseries: CREATE TIMESERIES, SHOW PARTITIONS, ALTER TIMESERIES.
    if upper.starts_with("CREATE TIMESERIES ") {
        return Some(super::super::timeseries::create_timeseries(
            state,
            identity,
            parts,
            database_id,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        return Some(super::super::timeseries::show_partitions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("ALTER TIMESERIES ") {
        return Some(super::super::timeseries::alter_timeseries(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REWRITE PARTITIONS ") {
        return Some(super::super::timeseries::rewrite_partitions(
            state, identity, parts,
        ));
    }

    // Last-value cache queries.
    if upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUES('collection_name')
        if let Some(collection) = super::helpers::extract_quoted_arg(sql, "LAST_VALUES(") {
            return Some(
                super::super::last_value::query_last_values(
                    state,
                    identity,
                    database_id,
                    &collection,
                )
                .await,
            );
        }
    }
    if upper.starts_with("SELECT LAST_VALUE(") && !upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUE('collection_name', series_id)
        if let Some((collection, series_id)) = super::helpers::extract_lv_args(sql) {
            return Some(
                super::super::last_value::query_last_value(
                    state,
                    identity,
                    database_id,
                    &collection,
                    series_id,
                )
                .await,
            );
        }
    }

    None
}
