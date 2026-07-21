// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane handler for `GraphOp::Stats` — reads the persistent O(1)
//! graph-stats counter table on this Data Plane core's `EdgeStore` and
//! returns a MessagePack-encoded `Vec<CollectionStats>`.

use nodedb_types::TenantId;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::stats::CollectionStats;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_stats(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: Option<&str>,
        as_of_ms: Option<i64>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            ?collection,
            ?as_of_ms,
            "graph stats"
        );
        let as_of = as_of_ms.map(nodedb_types::ms_to_ordinal_upper);
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let result: Vec<CollectionStats> = match collection {
            Some(name) => match self
                .edge_store
                .collection_stats(database_id, tenant, name, as_of)
            {
                Ok(s) => vec![s],
                Err(e) => return self.response_error(task, ErrorCode::from(e)),
            },
            None => match self.edge_store.tenant_stats(database_id, tenant, as_of) {
                Ok(v) => v,
                Err(e) => return self.response_error(task, ErrorCode::from(e)),
            },
        };

        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
