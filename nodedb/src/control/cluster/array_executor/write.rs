// SPDX-License-Identifier: BUSL-1.1

//! Write handlers for [`DataPlaneArrayExecutor`] — put and delete.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::wire::ArrayShardPutReq;
use nodedb_cluster::error::{ClusterError, Result};

use super::executor::DataPlaneArrayExecutor;
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

impl DataPlaneArrayExecutor {
    pub(super) async fn put(&self, req: &ArrayShardPutReq) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_put: {e}"),
            })?;

        // The coordinator encodes cells as `Vec<Vec<u8>>` (a blob-vec where
        // each inner bytes is a separately-encoded `ArrayPutCell`). The Data
        // Plane handler expects `Vec<ArrayPutCell>` encoded as a flat msgpack
        // array. Decode the outer blob-vec, parse each blob, and re-encode.
        let cell_blobs: Vec<Vec<u8>> =
            zerompk::from_msgpack(&req.cells_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("cell blob-vec decode in exec_put: {e}"),
            })?;

        let cells: Vec<crate::engine::array::wal::ArrayPutCell> = cell_blobs
            .iter()
            .map(|blob| {
                zerompk::from_msgpack(blob).map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayPutCell decode in exec_put: {e}"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let cells_msgpack = zerompk::to_msgpack_vec(&cells).map_err(|e| ClusterError::Codec {
            detail: format!("cells re-encode in exec_put: {e}"),
        })?;

        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id,
            cells_msgpack,
            wal_lsn: req.wal_lsn,
            provenance: None,
        });

        let resp = self.dispatch_and_await(plan).await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array put Data Plane error: {detail}"),
            });
        }

        Ok(req.wal_lsn)
    }

    pub(super) async fn delete(
        &self,
        array_id_msgpack: &[u8],
        coords_msgpack: &[u8],
        wal_lsn: u64,
    ) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_delete: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id,
            coords_msgpack: coords_msgpack.to_vec(),
            wal_lsn,
            provenance: None,
        });

        let resp = self.dispatch_and_await(plan).await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array delete Data Plane error: {detail}"),
            });
        }

        Ok(wal_lsn)
    }
}
