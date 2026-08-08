// SPDX-License-Identifier: BUSL-1.1

//! Columnar + timeseries serializer for transaction resolve.
//!
//! Like the vector serializer, these are **plan-driven**: columnar and
//! timeseries batch inserts are not staged as per-surrogate overlay
//! post-images — they ride the buffered-plan path — and their redo replay
//! re-runs the engine's native batch payload rather than a per-row shape
//! (`replay_timeseries_wal` → `replay_columnar_payload` / `replay_timeseries_payload`).
//! So this module reads the plan node directly and emits the SAME
//! `RecordType::TimeseriesBatch` sub-record the autocommit path produces,
//! reusing its encoders (`control::server::wal_dispatch`).
//!
//! ## One record type, two `kind` tags
//!
//! Both columnar and timeseries writes share `RecordType::TimeseriesBatch`.
//! They are disambiguated on replay by the encoded payload:
//!
//! * Columnar `Insert` → a map-shaped [`nodedb_types::columnar::ColumnarWalRecord`]
//!   with `kind = "columnar"`, carrying the per-row cross-engine surrogates.
//!   `decode_batch_record` matches the msgpack map first and routes it to
//!   `replay_columnar_payload`.
//! * Timeseries `Ingest` → the format-preserving 5-element tuple
//!   `("timeseries", collection, payload, provenance, format)`. The msgpack
//!   array never matches the map form, so it falls through to the timeseries
//!   replay path without relying on payload-byte heuristics.
//!
//! ## Predicate DML
//!
//! `ColumnarOp::Update` / `ColumnarOp::Delete` are predicate DML with no
//! per-row post-image (the matching set is only known once the Data Plane
//! re-scans current state). They serialize to the SAME predicate-carrying
//! [`nodedb_types::columnar::ColumnarDmlWalRecord`] (`kind = "columnar_dml"`,
//! under `RecordType::TimeseriesBatch`) the autocommit path appends via
//! `encode_columnar_dml_payload`; replay re-executes the predicate through the
//! live handler (`try_replay_columnar_predicate_dml`), so an in-tx columnar
//! UPDATE/DELETE is restart-durable exactly like its autocommit twin.
//!
//! ## Determinism
//!
//! Emission is in plan order (a fixed `&[PhysicalPlan]`), which is already
//! deterministic; each `Insert` / `Ingest` maps to exactly one sub-record.

use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    encode_columnar_batch_payload, encode_columnar_dml_payload,
    encode_timeseries_batch_payload_with_format,
};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single columnar plan op to `ops`.
///
/// `Insert` serializes to a `TimeseriesBatch` sub-record tagged `"columnar"`;
/// read ops emit nothing; predicate DML (`Update` / `Delete`) serializes to a
/// `TimeseriesBatch` sub-record tagged `"columnar_dml"` (see module docs).
pub(super) fn serialize_columnar_op(
    op: &ColumnarOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        ColumnarOp::Insert {
            collection,
            payload,
            format: _,
            intent: _,
            on_conflict_updates: _,
            surrogates,
            schema_bytes: _,
            provenance,
            wal_lsn: _,
            rls_write_check: _,
            // The redo record carries the row image, not the response shape a
            // projection and its read gate would have produced for one caller.
            returning: _,
            rls_filters: _,
        } => {
            let sub_payload = encode_columnar_batch_payload(
                collection,
                payload,
                provenance.as_ref(),
                surrogates,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }

        // Read families: no persisted post-image.
        ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => Ok(()),

        // Predicate DML: emit the SAME `ColumnarDmlWalRecord` (kind
        // `"columnar_dml"`, carried under `RecordType::TimeseriesBatch`) the
        // autocommit path appends via `encode_columnar_dml_payload`. Replay
        // routes it back through `try_replay_columnar_predicate_dml`, which
        // re-executes the predicate through the live handler — so an in-tx
        // columnar UPDATE/DELETE is restart-durable exactly like its autocommit
        // twin, rather than the redo record dropping it (and the commit
        // failing) for lack of a per-row post-image.
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check: _,
        } => {
            let sub_payload = encode_columnar_dml_payload(collection, true, filters, updates)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check: _,
        } => {
            let sub_payload = encode_columnar_dml_payload(collection, false, filters, &[])?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }
    }
}

/// Append the redo sub-record for a single timeseries plan op to `ops`.
///
/// `Ingest` serializes to a `TimeseriesBatch` sub-record tagged `"timeseries"`;
/// the scan op emits nothing.
pub(super) fn serialize_timeseries_op(
    op: &TimeseriesOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            wal_lsn: _,
            surrogates: _,
            provenance,
            rls_write_check: _,
            // The redo record carries the ingested payload, not the response
            // shape a projection and its read gate would have produced for one
            // caller. Replay reconstructs stored state; nobody is waiting on it.
            returning: _,
            rls_filters: _,
        } => {
            let sub_payload = encode_timeseries_batch_payload_with_format(
                collection,
                payload,
                provenance.as_ref(),
                format,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }

        // Read family: no persisted post-image.
        TimeseriesOp::Scan { .. } => Ok(()),
    }
}
