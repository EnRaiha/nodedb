// SPDX-License-Identifier: BUSL-1.1

use crate::control::security::credential::CredentialStore;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append a timeseries batch to WAL and return the assigned LSN.
///
/// Used by the ILP listener and the sync timeseries handler to propagate the
/// WAL LSN to the Data Plane for proper dedup tracking and `flush_wal_lsn` in
/// partition metadata. Returns `None` if WAL is bypassed for this collection.
///
/// `provenance` is `None` for the ILP direct-ingest path; the sync path passes
/// the frame's `SyncProvenance` so the WAL record carries full idempotency context.
pub fn wal_append_timeseries(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let database_id = DatabaseId::DEFAULT;
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

    let payload_vec = payload.to_vec();
    let wal_payload = zerompk::to_msgpack_vec(&("timeseries", collection, payload_vec, provenance))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch: {e}"),
        })?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
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
    let record = nodedb_types::columnar::ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: collection.to_string(),
        payload: payload.to_vec(),
        provenance: provenance.cloned(),
        surrogates: surrogates.to_vec(),
    };
    let wal_payload =
        zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal columnar batch: {e}"),
        })?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}
