// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Crdt`.

use super::super::decode_sync_engines;
use super::super::types::ConstraintChangeOp;
use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{CrdtOp, ReturningSpec};

pub(super) struct ApplyArgs<'a> {
    pub(super) collection: &'a str,
    pub(super) document_id: &'a str,
    pub(super) delta: &'a [u8],
    pub(super) peer_id: u64,
    pub(super) provenance_bytes: &'a Option<Vec<u8>>,
    pub(super) constraint_version_required: u64,
    pub(super) expected_frontier_digest: Option<[u8; 32]>,
    pub(super) auth_user_id: u64,
    pub(super) auth_device_id: u64,
    pub(super) auth_seq_no: u64,
    pub(super) delta_signature: [u8; 32],
    pub(super) signing_required: bool,
    pub(super) authenticated: bool,
    /// Leader-assigned surrogate carried on the wire. `Surrogate::ZERO`
    /// means a record written before the surrogate field existed — see
    /// `resolve_apply_surrogate`.
    pub(super) carried_surrogate: u32,
}

/// Resolve the surrogate `CrdtOp::Apply` / `ApplyAuthenticated` materializes
/// under. The leader assigned this surrogate at plan time
/// (`plan_builder/crdt.rs::build_apply`) and it is now carried on the wire —
/// bind it first-wins so every replica installs the SAME identity for
/// `document_id`, exactly like `decode/kv.rs::put`.
///
/// A carried `ZERO` means this entry was written before the surrogate field
/// existed (pre-migration WAL / Raft log). There is no leader value to bind
/// in that case, so this falls back to the pre-fix behavior — allocate via
/// this node's own assigner — but loudly: this is the exact per-node
/// allocation divergence the surrogate-carrying fix closes, tolerated only
/// as a one-time compatibility path for entries already committed before
/// upgrade, never for live post-upgrade writes (which always carry a
/// non-zero surrogate).
fn resolve_apply_surrogate(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    carried_surrogate: u32,
) -> crate::Result<nodedb_types::Surrogate> {
    let carried = nodedb_types::Surrogate::new(carried_surrogate);
    match ctx.assigner {
        Some(a) if carried != nodedb_types::Surrogate::ZERO => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            document_id.as_bytes(),
            carried,
        ),
        Some(a) => {
            tracing::warn!(
                database_id = ctx.database_id.as_u64(),
                tenant_id = ctx.tenant_id.as_u64(),
                collection,
                document_id,
                "CRDT apply entry carries no surrogate (pre-migration wire format); \
                 falling back to per-node allocation, which can diverge from other replicas"
            );
            a.assign(
                ctx.database_id,
                ctx.tenant_id,
                collection,
                document_id.as_bytes(),
            )
        }
        None => Ok(carried),
    }
}

pub(super) fn apply(ctx: &DecodeCtx, args: ApplyArgs<'_>) -> crate::Result<PhysicalPlan> {
    let ApplyArgs {
        collection,
        document_id,
        delta,
        peer_id,
        provenance_bytes,
        constraint_version_required,
        expected_frontier_digest,
        auth_user_id,
        auth_device_id,
        auth_seq_no,
        delta_signature,
        signing_required,
        authenticated,
        carried_surrogate,
    } = args;
    let surrogate = resolve_apply_surrogate(ctx, collection, document_id, carried_surrogate)?;
    let provenance = decode_sync_engines::decode_provenance(provenance_bytes)?;
    if authenticated {
        let provenance = provenance.ok_or_else(|| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: "authenticated CRDT replay is missing sync provenance".into(),
        })?;
        Ok(PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        }))
    } else {
        Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        }))
    }
}

/// Per-collection Loro doc import — no surrogate, no provenance. Every
/// replica applies the same snapshot via the same idempotent Loro merge,
/// converging deterministically.
pub(super) fn import_collection(tenant_id: u64, collection: &str, bytes: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id,
        collection: collection.to_owned(),
        bytes: bytes.to_vec(),
    })
}

/// Narrow a wire `u64` list position/index to the `usize` the live
/// `execute_crdt_list_*` handlers take. `usize::try_from` (never `as`): a
/// value that doesn't fit `usize` on this platform is a corrupt/incompatible
/// wire payload, not a value to silently truncate and replay at the wrong
/// position.
fn list_index(field: &str, value: u64) -> crate::Result<usize> {
    usize::try_from(value).map_err(|_| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("CrdtList {field}={value} does not fit usize on this platform"),
    })
}

/// Reconstruct `CrdtOp::ListInsert` from its wire intent. The current
/// dispatch handler (`data/executor/dispatch/crdt.rs::CrdtOp::ListInsert`)
/// still ignores `surrogate`, but the field is documented as the parent
/// document's identity — bind it the same way `CrdtDocUpsert` does (via
/// `bind_or_lookup`, never allocating) so it is correct the moment a
/// consumer starts reading it, rather than a second latent bug. A list op
/// mutates an existing document, so it never creates identity: `ZERO`
/// (legacy wire entry, or a non-member coordinator that missed resolution)
/// resolves via read-only catalog lookup, never binds.
pub(super) fn list_insert(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
    fields_json: &str,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = bind_or_lookup(
        ctx,
        collection,
        document_id.as_bytes(),
        nodedb_types::Surrogate::new(surrogate),
    )?;
    Ok(PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        fields_json: fields_json.to_owned(),
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::ListDelete` from its wire intent. See
/// [`list_insert`] for the surrogate note.
pub(super) fn list_delete(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = bind_or_lookup(
        ctx,
        collection,
        document_id.as_bytes(),
        nodedb_types::Surrogate::new(surrogate),
    )?;
    Ok(PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: list_index("index", index)?,
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::ListMove` from its wire intent. `from_index` and
/// `to_index` are narrowed independently so a value that fits one but not
/// the other still surfaces as a typed decode error rather than silently
/// substituting. See [`list_insert`] for the surrogate note.
pub(super) fn list_move(
    ctx: &DecodeCtx,
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: u64,
    to_index: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let surrogate = bind_or_lookup(
        ctx,
        collection,
        document_id.as_bytes(),
        nodedb_types::Surrogate::new(surrogate),
    )?;
    Ok(PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        from_index: list_index("from_index", from_index)?,
        to_index: list_index("to_index", to_index)?,
        surrogate,
    }))
}

/// Reconstruct `CrdtOp::DocUpsert` from its wire intent. Unlike the block-list
/// ops, the row's own top-level `surrogate` is carried across the wire and
/// rebuilt via `Surrogate::new` — the live dispatch handler uses it to gate +
/// key the sparse-store materialization.
pub(super) fn doc_upsert(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    fields_json: &str,
    partial: bool,
    returning: Option<ReturningSpec>,
    rls_filters: &[u8],
) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        fields_json: fields_json.to_owned(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        partial,
        // Carried on the record — a replay re-executes this write for the
        // originating request, not just for the follower's own state.
        returning,
        rls_filters: rls_filters.to_vec(),
    })
}

/// Reconstruct `CrdtOp::DocDelete` from its wire intent. See [`doc_upsert`]
/// for the surrogate note.
pub(super) fn doc_delete(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    returning: Option<ReturningSpec>,
    rls_filters: &[u8],
) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        // Carried on the record — see `doc_upsert`.
        returning,
        rls_filters: rls_filters.to_vec(),
    })
}

pub(super) fn constraint_change(
    collection: &str,
    op: &ConstraintChangeOp,
    constraint_version: u64,
    constraints: &[Vec<u8>],
) -> PhysicalPlan {
    match op {
        ConstraintChangeOp::Set => PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: collection.to_owned(),
            constraint_version,
            constraints: constraints.to_vec(),
        }),
        ConstraintChangeOp::Drop => PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: collection.to_owned(),
            constraint_version,
        }),
    }
}
