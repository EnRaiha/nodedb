// SPDX-License-Identifier: BUSL-1.1

//! Shard-side `ARRAY_FLUSH` / `ARRAY_COMPACT`: run the maintenance on the core
//! that owns the shard's vShard.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::wire::{ArrayShardMaintOp, ArrayShardMaintReq};
use nodedb_cluster::error::{ClusterError, Result};

use crate::types::VShardId;
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

use super::executor::DataPlaneArrayExecutor;
use super::refusal::refusal_error;

impl DataPlaneArrayExecutor {
    pub(super) async fn maint(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardMaintReq,
    ) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_maint: {e}"),
            })?;
        let (plan, key, context) = match req.op {
            // A frontier read of this node's WAL: `Flush` appends no record,
            // and every cell the segment holds was written below the frontier.
            ArrayShardMaintOp::Flush => (
                ArrayOp::Flush {
                    array_id: array_id.clone(),
                    wal_lsn: self.state.wal.next_lsn().as_u64(),
                },
                "flushed",
                "array flush",
            ),
            ArrayShardMaintOp::Compact { audit_retain_ms } => (
                ArrayOp::Compact {
                    array_id: array_id.clone(),
                    audit_retain_ms,
                },
                "compacted",
                "array compact",
            ),
        };
        let resp = self
            .dispatch_and_await(
                &array_id,
                VShardId::new(local_vshard_id),
                PhysicalPlan::Array(plan),
                None,
            )
            .await?;
        if resp.status == crate::bridge::envelope::Status::Error {
            return Err(refusal_error(context, &resp));
        }
        read_count(&resp.payload, key).ok_or_else(|| ClusterError::Storage {
            detail: format!("{context}: the data plane answer carried no '{key}' count"),
        })
    }
}

/// The `key` count of a `{key: n}` Data Plane answer.
fn read_count(payload: &[u8], key: &str) -> Option<u64> {
    let value = nodedb_types::json_from_msgpack(payload).ok()?;
    value.get(key).and_then(serde_json::Value::as_u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_count_is_read_from_its_key() {
        let payload =
            crate::data::executor::response_codec::encode_count("compacted", 1).expect("encode");
        assert_eq!(read_count(&payload, "compacted"), Some(1));
        assert_eq!(read_count(&payload, "flushed"), None);
    }
}
