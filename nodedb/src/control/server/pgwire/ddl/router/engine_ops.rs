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
    _parts: &[&str],
    _database_id: crate::types::DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // Most engine-ops families (weighted pick, rate gate, atomic transfer,
    // sorted index, atomic KV, timeseries, last-value cache, vector index
    // lifecycle, graph/tree ops) are served by the protocol-neutral DDL router;
    // the pgwire router no longer routes them. Only the vector model / metadata
    // forms below remain here, because they are handled by the not-yet-migrated
    // collection family.

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

    None
}
