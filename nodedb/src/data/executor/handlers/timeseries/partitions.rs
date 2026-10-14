// SPDX-License-Identifier: BUSL-1.1

//! `TimeseriesOp::Partitions`: list or rewrite a timeseries collection's
//! partitions on the core that owns it.
//!
//! The core's registry is the partition manifest, so both actions read it
//! here and never on the Control Plane.

use nodedb_physical::physical_plan::PartitionsAction;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::partition_registry::PartitionListing;

impl CoreLoop {
    /// Run `action` over `collection`'s partitions.
    pub(in crate::data::executor) fn execute_ts_partitions(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        action: PartitionsAction,
    ) -> Response {
        let database_id = task.request.database_id;
        let tid = task.request.tenant_id;
        match action {
            PartitionsAction::List => {
                let listing = self
                    .list_ts_partitions(database_id, tid, collection)
                    .and_then(|rows| response_codec::encode_serde(&rows));
                match listing {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, e),
                }
            }
            PartitionsAction::Rewrite => {
                match self.rewrite_ts_partitions(database_id, tid, collection) {
                    Ok(rewritten) => self.response_affected(task, rewritten),
                    Err(e) => self.response_error(task, e),
                }
            }
        }
    }

    /// Every queryable partition of `collection`, oldest first.
    fn list_ts_partitions(
        &mut self,
        database_id: crate::types::DatabaseId,
        tid: crate::types::TenantId,
        collection: &str,
    ) -> crate::Result<Vec<PartitionListing>> {
        self.ensure_ts_registry(tid, database_id, collection)?;
        let key = (database_id, tid, collection.to_string());
        Ok(match self.ts_registries.get(&key) {
            Some(registry) => registry
                .iter()
                .filter(|(_, entry)| entry.meta.is_queryable())
                .map(|(_, entry)| PartitionListing::from_entry(entry))
                .collect(),
            None => Vec::new(),
        })
    }
}
