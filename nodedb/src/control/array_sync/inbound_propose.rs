// SPDX-License-Identifier: BUSL-1.1

//! Raft propose helpers for [`OriginArrayInbound`].
//!
//! These methods handle the Raft path (`propose_and_await`), plus the small
//! helpers for binding a put's cell surrogate and computing the destination
//! vShard.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_cluster::array_routing::{array_vshard_for_name, vshard_for_array_coord};
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};
use tracing::{error, warn};

use crate::control::wal_replication::ReplicatedEntry;
use crate::types::{TraceId, VShardId};

use super::inbound::OriginArrayInbound;
use super::reject::build_reject;

impl OriginArrayInbound {
    fn consume_array_write_authorization(
        &self,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
        array: &str,
        hlc: Hlc,
    ) -> Result<(), Option<ArrayRejectMsg>> {
        let (tenant_id, database_id, collection, permission) = authorization.into_scope();
        if tenant_id != self.tenant_id()
            || database_id != self.database_id()
            || collection != array
            || permission != crate::control::security::identity::Permission::Write
        {
            return Err(Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                "array authorization scope mismatch".to_string(),
            )));
        }
        Ok(())
    }

    /// Propose a `ReplicatedEntry` to Raft and await its commit + apply.
    ///
    /// Returns `Ok(())` when the entry has been committed and executed by the
    /// distributed applier. Returns a reject on proposal or timeout failure.
    pub(super) async fn propose_and_await(
        &self,
        mut entry: ReplicatedEntry,
        array: &str,
        hlc: Hlc,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
    ) -> Result<(), Option<ArrayRejectMsg>> {
        self.consume_array_write_authorization(authorization, array, hlc)?;
        if entry.tenant_id != self.tenant_id().as_u64()
            || entry.database_id != self.database_id().as_u64()
        {
            return Err(Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                "array replicated entry scope mismatch".to_string(),
            )));
        }
        // Both proposers below take prebuilt bytes, so the floor is stamped
        // here: replicas hold the write until they applied the array's DDL.
        // The commit instant is stamped once, so every replica dates the
        // write alike.
        entry.write_hlc = self.shared().hlc_clock.now().wall_ns;
        crate::control::wal_replication::stamp_metadata_floor(self.shared(), &mut entry);
        crate::control::array_catalog::cell_route::stamp_incarnation(self.shared(), &mut entry);
        let vshard_id = entry.vshard_id;
        let idempotency_key = entry.idempotency_key;
        let data = entry.encode().map_err(|e| {
            error!(array = %array, error = %e, "array_inbound: replicated entry encode failed");
            Some(build_reject(
                array,
                hlc,
                ArrayRejectReason::EngineRejected,
                format!("replicated entry encode failed: {e}"),
            ))
        })?;

        // The async proposer forwards to the group leader and waits for the
        // apply. It returns the apply payload directly.
        let async_proposer = self
            .shared()
            .async_raft_proposer()
            .map_err(|e| {
                Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("raft proposer not available: {e}"),
                ))
            })?
            .as_ref();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(self.shared().tuning.network.default_deadline_secs);
        match async_proposer(vshard_id, idempotency_key, data, deadline).await {
            Ok((_payload, _committed_version)) => Ok(()),
            Err(e) => {
                warn!(array = %array, error = %e, "array_inbound: raft propose+apply failed");
                Err(Some(build_reject(
                    array,
                    hlc,
                    ArrayRejectReason::EngineRejected,
                    format!("raft propose failed: {e}"),
                )))
            }
        }
    }

    /// The bound surrogate of a put's cell, assigned at the array's home under
    /// the `(array, zerompk(coord))` key the SQL insert path binds. A delete
    /// or erasure names a coordinate, not a row, and carries none.
    pub(super) async fn cell_surrogate(
        &self,
        op: &ArrayOp,
    ) -> Result<Option<nodedb_types::Surrogate>, Option<ArrayRejectMsg>> {
        use nodedb_array::sync::op::ArrayOpKind;
        if !matches!(op.kind, ArrayOpKind::Put) {
            return Ok(None);
        }
        let reject = |detail: String| {
            Some(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::EngineRejected,
                detail,
            ))
        };
        let pk = zerompk::to_msgpack_vec(&op.coord)
            .map_err(|e| reject(format!("array cell coord encode: {e}")))?;
        let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
            self.shared(),
            nodedb_types::CollectionKey::from_bare(self.database_id(), &op.header.array),
            self.tenant_id(),
            &pk,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| reject(format!("array cell surrogate assign: {e}")))?;
        Ok(Some(surrogate))
    }

    /// Compute the vShard that owns this op's tile.
    ///
    /// Extracts tile extents from the schema registry and casts the op's coord
    /// to `u64` for tile routing. Falls back to collection-level routing with
    /// a warning when the schema is unavailable or coord cannot be cast.
    pub(super) fn vshard_for_op(&self, op: &ArrayOp) -> VShardId {
        use nodedb_array::types::coord::value::CoordValue;

        let tile_extents = self.schemas().tile_extents_in_database(
            self.database_id(),
            self.tenant_id().as_u64(),
            &op.header.array,
        );

        let Some(tile_extents) = tile_extents else {
            warn!(
                array = %op.header.array,
                "array_inbound: schema unavailable; routing by name only"
            );
            return VShardId::new(array_vshard_for_name(&op.header.array));
        };

        let coord_u64: Vec<u64> = op
            .coord
            .iter()
            .map(|c| match c {
                CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v as u64,
                CoordValue::Float64(v) => v.to_bits(),
                CoordValue::String(_) => 0,
            })
            .collect();

        VShardId::new(vshard_for_array_coord(
            &op.header.array,
            &coord_u64,
            &tile_extents,
        ))
    }
}
