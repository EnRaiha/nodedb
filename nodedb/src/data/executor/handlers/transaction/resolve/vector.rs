// SPDX-License-Identifier: BUSL-1.1

//! Vector serializer for transaction resolve.
//!
//! Unlike the KV / document / graph serializers, the vector serializer is
//! **plan-driven**, not overlay-driven: vector writes are never staged into a
//! transaction overlay (there is no `stage_vector`). A vector post-image is
//! also inexpressible — the HNSW graph mutation has no compact absolute form —
//! so the redo record logs the INSERT itself and replay rebuilds the index
//! (`replay_vector_wal`, dispatched from the redo reconstitute path). This
//! module therefore reads the [`VectorOp`] plan node directly and emits the
//! SAME engine-native WAL sub-record shape the autocommit vector path produces,
//! reusing its encoders (`control::server::wal_dispatch::vector`) so producer
//! and replay never drift:
//!
//! * `Insert` → `RecordType::VectorPut`, the 7-element
//!   `(collection, vector, dim, field_name, doc_id_compat, surrogate, provenance)`
//!   shape carrying the row's cross-engine surrogate identity.
//! * `BatchInsert` → `RecordType::VectorPut`, the 3-element
//!   `(collection, vectors, dim)` headless-batch shape.
//! * `Delete` → `RecordType::VectorDelete`, `(collection, vector_id, None)`.
//! * `DeleteBySurrogate` → `RecordType::VectorDelete`,
//!   `(collection, surrogate, field_name, provenance)`.
//!
//! ## Ops that raise a typed error
//!
//! `DirectUpsert`, `MultiVectorInsert`, `MultiVectorDelete`, `SparseInsert`,
//! and `SparseDelete` are writes with NO autocommit WAL shape (they fall to the
//! `_ => None` arm of `wal_append_if_write_with_creds`), so no redo sub-record
//! decoder exists for them. Silently omitting a write from the redo record
//! would lose it on install, so each raises a typed error rather than being
//! dropped. `SetParams` (vector-index DDL) likewise raises a typed error,
//! matching how the KV / document serializers reject index / DDL ops.
//!
//! ## Ops that emit nothing
//!
//! Read and index-maintenance ops carry no persisted logical post-image: the
//! logical vectors survive via their `VectorPut` records and the index is
//! rebuilt from them on replay, so `Seal` / `CompactIndex` / `Rebuild` are
//! naturally reconstructed and need no redo sub-record.
//!
//! ## Determinism
//!
//! Emission is in plan order, which is already deterministic (the plan set is a
//! fixed `&[PhysicalPlan]`). A `VectorParams` record would have to precede its
//! puts on replay, but `SetParams` is rejected here, so ordering reduces to the
//! given plan order.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_put_payload,
};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record(s) for a single vector plan op to `ops`.
///
/// Writes serialize to their engine-native `VectorPut` / `VectorDelete` shape;
/// read and index-maintenance ops emit nothing; writes without a redo shape
/// raise a typed error (see module docs).
pub(super) fn serialize_vector_op(
    op: &VectorOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        } => {
            let payload = encode_vector_put_payload(
                collection,
                vector,
                *dim,
                field_name,
                *surrogate,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        } => {
            let payload = encode_vector_batch_put_payload(collection, vectors, *dim)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::Delete {
            collection,
            vector_id,
        } => {
            let payload = encode_vector_delete_payload(collection, *vector_id)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => {
            let payload = encode_vector_delete_by_surrogate_payload(
                collection,
                *surrogate,
                field_name,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }

        // Read families: no persisted post-image.
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::QueryStats { .. } => Ok(()),

        // Index maintenance: the logical vectors survive via their `VectorPut`
        // records and the index is rebuilt from them on replay, so seal /
        // compact / rebuild are reconstructed without a redo sub-record.
        VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. } => Ok(()),

        // Vector-index configuration DDL: rejected like the KV / document
        // index-DDL ops. No row-level post-image; a CREATE VECTOR INDEX rides
        // its own autocommit `VectorParams` record, not a transaction redo.
        VectorOp::SetParams { .. } => Err(crate::Error::PlanError {
            detail: "vector SetParams (index DDL) is not supported in transaction resolve"
                .to_string(),
        }),

        // Writes with no autocommit WAL shape and therefore no redo sub-record
        // decoder. Rejecting keeps their rows out of a silently lossy redo
        // record rather than inventing an unreplayable shape.
        VectorOp::DirectUpsert { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. } => Err(crate::Error::PlanError {
            detail: "vector direct-upsert / multi-vector / sparse writes have no redo sub-record \
                     shape and are not supported in transaction resolve"
                .to_string(),
        }),
    }
}
