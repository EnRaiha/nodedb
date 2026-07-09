// SPDX-License-Identifier: BUSL-1.1

//! Shared decode context + surrogate-binding helpers used across every
//! per-engine decode submodule.

use crate::control::surrogate::SurrogateAssigner;
use crate::types::{DatabaseId, TenantId};

/// Bundles the ambient decode parameters (surrogate assigner + tenancy
/// scope) threaded through every per-engine decode helper.
pub(super) struct DecodeCtx<'a> {
    pub(super) assigner: Option<&'a SurrogateAssigner>,
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
}

pub(super) fn assign_or_zero(
    ctx: &DecodeCtx,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    match ctx.assigner {
        Some(a) => a.assign(ctx.database_id, ctx.tenant_id, collection, pk_bytes),
        None => Ok(nodedb_types::Surrogate::ZERO),
    }
}

/// Resolve `carried` for a mutating op that does NOT create rows (UPDATE /
/// DELETE). When `carried` is authoritative (non-ZERO, from a member
/// coordinator) the binding is installed first-wins via `bind`. When `carried`
/// is ZERO (non-member coordinator that missed resolution) the catalog is
/// queried READ-ONLY; ZERO is never bound, so a later INSERT of the same pk
/// gets a freshly allocated surrogate instead of the corrupt ZERO entry.
pub(super) fn bind_or_lookup(
    ctx: &DecodeCtx,
    collection: &str,
    pk_bytes: &[u8],
    carried: nodedb_types::Surrogate,
) -> crate::Result<nodedb_types::Surrogate> {
    match ctx.assigner {
        Some(a) if carried != nodedb_types::Surrogate::ZERO => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            pk_bytes,
            carried,
        ),
        Some(a) => Ok(a
            .lookup(ctx.database_id, ctx.tenant_id, collection, pk_bytes)?
            .unwrap_or(nodedb_types::Surrogate::ZERO)),
        None => Ok(carried),
    }
}
