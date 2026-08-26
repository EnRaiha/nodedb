// SPDX-License-Identifier: BUSL-1.1

//! KV engine CoW insert interception.
//!
//! An INSERT into a `Shadowed` clone writes only the target. The source row
//! carrying the same key stays visible, so the merged read returns the key
//! twice. Recording a KV tombstone for each written key suppresses the source
//! copy; the row the statement just wrote is target-resident and unaffected.

use pgwire::error::PgWireResult;

use nodedb_types::{CloneStatus, TenantId};

use crate::control::clone::tombstone::{KvTombstoneParams, perform_kv_clone_tombstone};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::core::NodeDbPgHandler;
use super::entry::CloneWriteOutcome;
use super::util::{strip_db_prefix, write_err};

/// Collect `(collection, keys)` for the KV write shapes that create rows.
fn classify(plan: &PhysicalPlan) -> Option<(&str, Vec<&[u8]>)> {
    match plan {
        PhysicalPlan::Kv(
            KvOp::Put {
                collection, key, ..
            }
            | KvOp::Insert {
                collection, key, ..
            }
            | KvOp::InsertIfAbsent {
                collection, key, ..
            }
            | KvOp::InsertOnConflictUpdate {
                collection, key, ..
            },
        ) => Some((collection.as_str(), vec![key.as_slice()])),
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection,
            entries,
            ..
        }) => Some((
            collection.as_str(),
            entries.iter().map(|(k, _)| k.as_slice()).collect(),
        )),
        _ => None,
    }
}

impl NodeDbPgHandler {
    /// Hide the source rows an insert into a clone would otherwise duplicate.
    pub(super) async fn intercept_kv_clone_insert(
        &self,
        task: &PhysicalTask,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let Some((collection_qualified, keys)) = classify(&task.plan) else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, collection_qualified);

        let catalog = self.state.credentials.catalog();
        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone kv insert: get_collection: {e}")))?;
        let Some(desc) = desc else {
            return Ok(CloneWriteOutcome::Passthrough);
        };
        if desc.cloned_from.is_none() {
            return Ok(CloneWriteOutcome::Passthrough);
        }
        match desc.clone_status {
            CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        for key in keys {
            let kv_key = String::from_utf8_lossy(key).into_owned();
            perform_kv_clone_tombstone(KvTombstoneParams {
                state: &self.state,
                target_db_id: db_id,
                target_collection: coll_name,
                kv_key,
            })
            .map_err(|e| write_err(&format!("clone kv insert tombstone: {e}")))?;
        }

        Ok(CloneWriteOutcome::Passthrough)
    }
}
