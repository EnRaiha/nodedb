// SPDX-License-Identifier: BUSL-1.1

//! Vector ops: a row with a primary key binds by it; a headless row self-keys
//! by its surrogate's own bytes, the key `assign_anonymous` binds under.

use nodedb_physical::physical_plan::{VectorOp, VectorResolvedMutation};

use super::binder::{BindSlot, IdentityBinder};

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut VectorOp) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            surrogate,
            pk_bytes,
            ..
        } => match pk_bytes {
            Some(pk) => {
                binder.resolve_in_place(binder.plan_key(collection.as_str())?, pk, surrogate)
            }
            None => {
                binder.resolve_self_keyed_in_place(binder.plan_key(collection.as_str())?, surrogate)
            }
        },
        VectorOp::BatchInsert {
            collection,
            surrogates,
            ..
        } => {
            let key = binder.plan_key(collection.as_str())?;
            let slots = surrogates
                .iter_mut()
                .map(|surrogate| BindSlot::self_keyed(key, surrogate))
                .collect();
            binder.resolve_slots_in_place(slots)
        }
        VectorOp::MultiVectorInsert {
            collection,
            document_surrogate,
            pk_bytes,
            ..
        } => match pk_bytes {
            Some(pk) => binder.resolve_in_place(
                binder.plan_key(collection.as_str())?,
                pk,
                document_surrogate,
            ),
            None => binder.resolve_self_keyed_in_place(
                binder.plan_key(collection.as_str())?,
                document_surrogate,
            ),
        },
        VectorOp::DirectUpsert {
            collection,
            surrogate,
            pk_bytes,
            ..
        }
        | VectorOp::DirectInsert {
            collection,
            surrogate,
            pk_bytes,
            ..
        }
        | VectorOp::DirectInsertIfAbsent {
            collection,
            surrogate,
            pk_bytes,
            ..
        } => bind_keyed_or_self(
            binder,
            binder.plan_key(collection.as_str())?,
            pk_bytes,
            surrogate,
        ),
        VectorOp::DirectBatchWrite {
            collection, rows, ..
        } => {
            let key = binder.plan_key(collection.as_str())?;
            let slots = rows
                .iter_mut()
                .map(|row| keyed_or_self_slot(key, &row.pk_bytes, &mut row.surrogate))
                .collect();
            binder.resolve_slots_in_place(slots)
        }
        VectorOp::ResolveDirectWrite(inner) => bind(binder, inner),
        VectorOp::ResolvedDirectWrite {
            collection,
            mutations,
            ..
        } => {
            let key = binder.plan_key(collection.as_str())?;
            let mut slots = Vec::with_capacity(mutations.len());
            for mutation in mutations.iter_mut() {
                match mutation {
                    VectorResolvedMutation::Upsert {
                        surrogate,
                        pk_bytes,
                        ..
                    } => slots.push(keyed_or_self_slot(key, pk_bytes, surrogate)),
                    // Named by a surrogate bound when the row was inserted.
                    VectorResolvedMutation::Delete { .. }
                    | VectorResolvedMutation::Update { .. } => {}
                }
            }
            binder.resolve_slots_in_place(slots)
        }
        // Deletes and updates name an existing surrogate; searches, sparse
        // rows (keyed by doc id in their own store) and index maintenance
        // create no identity.
        VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => Ok(()),
    }
}

/// An empty `pk_bytes` is a headless row: self-key it.
fn bind_keyed_or_self(
    binder: &IdentityBinder<'_>,
    key: nodedb_types::CollectionKey<'_>,
    pk_bytes: &[u8],
    surrogate: &mut nodedb_types::Surrogate,
) -> crate::Result<()> {
    if pk_bytes.is_empty() {
        return binder.resolve_self_keyed_in_place(key, surrogate);
    }
    binder.resolve_in_place(key, pk_bytes, surrogate)
}

/// The bind slot of one row: keyed by `pk_bytes`, or self-keyed when it is
/// empty (a headless row).
fn keyed_or_self_slot<'k, 's>(
    key: nodedb_types::CollectionKey<'k>,
    pk_bytes: &'s [u8],
    surrogate: &'s mut nodedb_types::Surrogate,
) -> BindSlot<'k, 's> {
    if pk_bytes.is_empty() {
        return BindSlot::self_keyed(key, surrogate);
    }
    BindSlot::keyed(key, pk_bytes, surrogate)
}
