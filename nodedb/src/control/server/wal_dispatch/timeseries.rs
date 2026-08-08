// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch + payload encoders for the columnar-family engines
//! (`PhysicalPlan::Timeseries` and the columnar batch/DML records).

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::control::security::credential::CredentialStore;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append the WAL record for a single `TimeseriesOp`, returning the allocated
/// LSN for the ingest write (`Some`), `None` for `Scan`, and `None` when the
/// collection is configured `wal=false` (WAL bypass).
///
/// The match over [`TimeseriesOp`] is **exhaustive** (`wildcard_enum_match_arm`
/// is denied), so a future write variant cannot silently become non-durable.
///
/// `credentials` is threaded through solely for the per-collection WAL-bypass
/// check on `Ingest`; it is `None` on paths that never bypass.
pub(super) fn wal_append_timeseries_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &TimeseriesOp,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format: _,
            provenance,
            ..
        } => {
            // WAL bypass: skip WAL if collection has wal=false in timeseries_config.
            if let Some(creds) = credentials
                && let Ok(Some(coll)) =
                    creds
                        .catalog()
                        .get_collection(database_id, tenant_id.as_u64(), collection)
                && let Some(config) = coll.get_timeseries_config()
                && config.get("wal").and_then(|v| v.as_str()) == Some("false")
            {
                // WAL bypassed — acceptable data loss of last flush interval on crash.
                None
            } else {
                // Provenance is appended last; older 3-element decoders ignore
                // the trailing field via their arity-fallback paths.
                let wal_payload =
                    encode_timeseries_batch_payload(collection, payload, provenance.as_ref())?;
                Some(wal.append_timeseries_batch(
                    tenant_id,
                    vshard_id,
                    database_id,
                    &wal_payload,
                )?)
            }
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
        TimeseriesOp::Scan { .. } => None,
    };
    Ok(appended)
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a timeseries
/// ingest.
///
/// Produces the legacy 4-element tuple `("timeseries", collection, payload,
/// provenance)`. New transaction redo must instead use
/// [`encode_timeseries_batch_payload_with_format`] so replay retains the
/// format discriminator; this encoder remains for backward-compatible callers.
pub(crate) fn encode_timeseries_batch_payload(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&("timeseries", collection, payload, provenance)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch: {e}"),
        }
    })
}

/// Encode the format-preserving 5-element timeseries WAL/redo tuple.
///
/// The final format field is required for transaction redo because payload
/// bytes alone cannot distinguish canonical ILP MessagePack from ordinary row
/// MessagePack. Replay decodes this shape before all legacy tuple forms.
pub(crate) fn encode_timeseries_batch_payload_with_format(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    format: &str,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&("timeseries", collection, payload, provenance, format)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch with format: {e}"),
        }
    })
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar batch.
///
/// Produces the map-shaped [`nodedb_types::columnar::ColumnarWalRecord`] with
/// `kind = "columnar"`, carrying the per-row cross-engine surrogates. The map
/// encoding is distinct from the timeseries tuple, so `decode_batch_record`
/// routes it to `replay_columnar_payload`. The ONE encoder for this shape:
/// the autocommit `ColumnarOp::Insert` arm, `wal_append_columnar`, and the
/// transaction-resolve serializer all call it.
pub(crate) fn encode_columnar_batch_payload(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    surrogates: &[nodedb_types::Surrogate],
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: collection.to_string(),
        payload: payload.to_vec(),
        provenance: provenance.cloned(),
        surrogates: surrogates.to_vec(),
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar batch: {e}"),
    })
}

/// Stable routing and collection scope for a timeseries WAL append.
///
/// Keeping these fields together prevents callers from accidentally mixing the
/// authenticated database or tenant with a payload from another scope.
pub(crate) struct TimeseriesWalAppendContext<'a> {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
}

/// Append a timeseries batch to WAL and return the assigned LSN.
///
/// Used by the ILP listener and the sync timeseries handler to propagate the
/// WAL LSN to the Data Plane for proper dedup tracking and `flush_wal_lsn` in
/// partition metadata. Returns `None` if WAL is bypassed for this collection.
///
/// `provenance` is `None` for the ILP direct-ingest path; the sync path passes
/// the frame's `SyncProvenance` so the WAL record carries full idempotency context.
pub(crate) fn wal_append_timeseries(
    wal: &WalManager,
    context: TimeseriesWalAppendContext<'_>,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let TimeseriesWalAppendContext {
        tenant_id,
        vshard_id,
        database_id,
        collection,
    } = context;
    // WAL bypass check.
    if let Some(creds) = credentials
        && let Ok(Some(coll)) =
            creds
                .catalog()
                .get_collection(database_id, tenant_id.as_u64(), collection)
        && let Some(config) = coll.get_timeseries_config()
        && config.get("wal").and_then(|v| v.as_str()) == Some("false")
    {
        return Ok(None);
    }

    let wal_payload = encode_timeseries_batch_payload(collection, payload, provenance)?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar
/// predicate DML (`ColumnarOp::Update` / `ColumnarOp::Delete`).
///
/// Produces the map-shaped [`nodedb_types::columnar::ColumnarDmlWalRecord`]
/// with `kind = "columnar_dml"`, carrying the predicate (filters) and, for an
/// update, the field assignments — NOT row post-images (the matching set is
/// only known once the Data Plane re-scans current state at apply time). The
/// `columnar_dml` kind is disjoint from both the row-payload `"columnar"` map
/// shape and the legacy tuple shapes (see the type's doc comment), so
/// `decode_batch_record` cannot mis-route between them. The ONE encoder for
/// this shape: the autocommit `ColumnarOp::Update` / `ColumnarOp::Delete` arms
/// call it directly.
pub(crate) fn encode_columnar_dml_payload(
    collection: &str,
    is_update: bool,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarDmlWalRecord {
        kind: "columnar_dml".to_string(),
        collection: collection.to_string(),
        is_update,
        filters: filters.to_vec(),
        updates: updates.to_vec(),
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar dml: {e}"),
    })
}

/// Record-level fields for a columnar WAL append.
///
/// Groups the collection identity, row payload, sync provenance, and
/// cross-engine surrogates that together describe a single columnar batch
/// write, reducing the call-site argument count on [`wal_append_columnar`].
pub struct ColumnarWalAppendArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. Pass an empty
    /// slice when the caller does not carry surrogate identity (e.g. the
    /// sync/CRDT path).
    pub surrogates: &'a [nodedb_types::Surrogate],
}

/// Append a columnar batch to WAL and return the assigned LSN.
///
/// Mirrors `wal_append_timeseries` but encodes a map-shaped
/// [`nodedb_types::columnar::ColumnarWalRecord`] (kind `"columnar"`) so the
/// WAL replay decoder routes to `replay_columnar_payload` and can restore the
/// per-row cross-engine surrogates. The map encoding is distinct from the
/// legacy 4-tuple array, so old records still decode via the replay fallback
/// path.
/// Returns `None` if WAL is bypassed (columnar collections do not currently
/// support `wal=false`, so this always returns `Some`).
pub fn wal_append_columnar(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: ColumnarWalAppendArgs<'_>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let ColumnarWalAppendArgs {
        collection,
        payload,
        provenance,
        surrogates,
    } = args;
    let wal_payload = encode_columnar_batch_payload(collection, payload, provenance, surrogates)?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::PhysicalPlan;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn ingest_appends_timeseries_batch_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".to_string(),
            payload: vec![1, 2, 3],
            format: "samples".to_string(),
            wal_lsn: None,
            surrogates: vec![],
            provenance: None,
            rls_write_check: vec![],
            returning: None,
            rls_filters: vec![],
        });

        // No credentials => no WAL bypass; the ingest must produce a record.
        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Ingest must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::TimeseriesBatch
        ));
    }

    #[test]
    fn scan_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "metrics".to_string(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 10,
            filters: vec![],
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            computed_columns: vec![],
            rls_filters: vec![],
            system_time: Default::default(),
            valid_at_ms: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "Scan must produce no durable LSN");
    }
}
