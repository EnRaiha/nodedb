// SPDX-License-Identifier: BUSL-1.1

//! The engine-agnostic resolve-before-propose protocol.
//!
//! A collection with an RLS write policy can't replicate a bare predicate
//! `UPDATE`/`DELETE` — a follower has no writing identity to evaluate
//! `$auth.*`. Instead: resolve on the Data Plane, apply into a write stamped
//! `decided_earlier_in_request()`, propose via ordinary Raft, retry on drift.

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
/// [`super::resolver_for_plan`] builds it from the intercepted plan, so it
/// owns that write's already-extracted fields — no re-matching below.
#[async_trait]
pub trait EngineWriteResolver: Send + Sync {
    /// Control Plane. Collection the write targets.
    fn collection(&self) -> &str;

    /// Control Plane. Home vShard the resolved write is proposed to.
    /// Collection-homed by default; the graph resolver overrides this since
    /// an edge is key-homed on its source endpoint instead.
    fn vshard(&self, database_id: DatabaseId) -> VShardId {
        VShardId::from_collection_in_database(database_id, self.collection())
    }

    /// Control Plane. Pure, no I/O: the read-only op that resolves this write.
    fn build_resolve_op(&self) -> PhysicalPlan;

    /// Data Plane. Dispatches `op` over the SPSC path; the handler scans,
    /// applies filters/assignments, and decides the policy per match. Decodes
    /// the response with `zerompk` into `Value` directly, never via JSON.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows>;

    /// Control Plane. Pure, no I/O: the write carrying `resolved` and a
    /// decided `RlsWriteCheck`. Fallible because [`ResolvedRows`] spans every
    /// engine's shape while an implementor handles one — a mismatched shape
    /// is an internal dispatch break, reported rather than rewritten.
    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan>;
}
