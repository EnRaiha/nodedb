// SPDX-License-Identifier: BUSL-1.1

//! `SEARCH <collection> USING FUSION(...)` DSL.
//!
//! Parsing is delegated to `nodedb_sql::ddl_ast::parse_search_using_fusion`
//! so this SQL surface shares the same quote- and bracket-aware tokenizer
//! and the same typed [`FusionParams`] extractor as `GRAPH RAG FUSION`.
//! Both surfaces dispatch through the protocol-neutral
//! [`crate::control::server::shared::ddl::neutral::graph_ops::rag_fusion::rag_fusion`],
//! so defaults and caps cannot drift between them. This pgwire surface wraps the
//! neutral `DdlResult` back into pgwire `Response`s via `ddl_results_to_pgwire`
//! (a byte-identical round-trip) until the `dsl` family migrates too.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::parse_search_using_fusion;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl_encode::ddl_results_to_pgwire;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::server::shared::ddl::neutral::graph_ops::rag_fusion::rag_fusion;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub async fn search_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, params) = parse_search_using_fusion(sql).ok_or_else(|| {
        sqlstate_error(
            "42601",
            "syntax: SEARCH <collection> USING FUSION(ARRAY[...] ...)",
        )
    })?;
    let result = rag_fusion(state, identity, database_id, collection, params).await;
    ddl_results_to_pgwire(result)
}
