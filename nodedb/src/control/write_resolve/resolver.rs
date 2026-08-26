// SPDX-License-Identifier: BUSL-1.1

//! The engine-agnostic resolve-before-propose protocol.
//!
//! A collection carrying an RLS write policy cannot replicate a bare predicate
//! `UPDATE`/`DELETE`: the leader would have to re-decide the policy after
//! commit (rejecting what followers already applied — divergence), and a
//! follower has no writing identity to evaluate `$auth.*` against (silent
//! bypass either way). `wal_replication::encode` refuses such a write rather
//! than replicate it unsafely; this protocol is what makes it succeed again,
//! correctly:
//!
//! 1. **Resolve** on the Data Plane, against the native rows it holds, with
//!    the same write-policy evaluator a direct predicate DML uses.
//! 2. **Apply** the decided row set into a resolved write stamped
//!    `RlsWriteCheck::decided_earlier_in_request()` — this same request
//!    already admitted these exact row images.
//! 3. **Propose** it through the same Raft proposer an ordinary replicated
//!    write uses, retrying on drift.

use crate::types::VShardId;
use async_trait::async_trait;
use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;

use super::resolved_rows::ResolvedRows;

/// Request-scoped identity the resolve dispatch and the propose both need.
#[derive(Clone, Copy)]
pub struct WriteResolveContext {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
}

/// One engine's resolve-before-propose implementation.
///
/// [`super::resolver_for_plan`] builds an implementor from the intercepted
/// plan, so an implementor owns that write's already-extracted fields and
/// every method below is total without re-matching the plan.
#[async_trait]
pub trait EngineWriteResolver: Send + Sync {
    /// Control Plane. Collection the write targets.
    fn collection(&self) -> &str;

    /// Control Plane. Home vShard the resolved write is proposed to.
    ///
    /// Collection-homed for every engine that keys its rows by collection. A
    /// graph edge is key-homed on its source endpoint instead, so the graph
    /// resolver overrides this — routing that write by collection would
    /// propose it to a shard that never held the edge.
    fn vshard(&self, database_id: DatabaseId) -> VShardId {
        VShardId::from_collection_in_database(database_id, self.collection())
    }

    /// Control Plane. Pure, no I/O: the read-only op that resolves this write.
    fn build_resolve_op(&self) -> PhysicalPlan;

    /// Data Plane. Dispatches `op` over the SPSC path; the handler scans the
    /// collection, applies the statement's filters and assignments, and
    /// decides the write policy against every match's exact image.
    ///
    /// Decodes the response with `zerompk` into `nodedb_types::Value`
    /// directly. Never through a JSON path — see [`ResolvedRows`].
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows>;

    /// Control Plane. Pure, no I/O: the write carrying `resolved` and a
    /// decided `RlsWriteCheck`.
    ///
    /// Fallible only because [`ResolvedRows`] spans every engine's decided
    /// shape while an implementor handles one: a shape from another engine is
    /// an internal dispatch break, reported rather than silently rewritten
    /// into a write it does not describe.
    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan>;
}
