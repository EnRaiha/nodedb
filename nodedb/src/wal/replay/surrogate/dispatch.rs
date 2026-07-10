// SPDX-License-Identifier: BUSL-1.1

//! Dispatch loop for replaying `SurrogateAlloc` / `SurrogateBind` WAL records.

use nodedb_types::{DatabaseId, TenantId};
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::control::security::catalog::SystemCatalog;
use crate::control::surrogate::SurrogateRegistryHandle;

use super::{apply_surrogate_alloc, apply_surrogate_bind};

/// Replay every `SurrogateAlloc` and `SurrogateBind` record in `records`
/// into the live `SurrogateRegistry` + `SystemCatalog`. Run once at
/// startup, after the registry has been seeded from the catalog hwm row
/// and after the catalog has been opened — replay then advances both
/// past the WAL's tail so any binding emitted before the crash is
/// durable on the next allocation.
pub fn replay_surrogate_records(
    records: &[WalRecord],
    catalog: &SystemCatalog,
    registry: &SurrogateRegistryHandle,
) -> crate::Result<ReplayStats> {
    let mut stats = ReplayStats::default();
    for record in records {
        let raw = record.logical_record_type();
        let Some(rt) = RecordType::from_raw(raw) else {
            continue;
        };
        match rt {
            RecordType::SurrogateAlloc => {
                apply_surrogate_alloc(&record.payload, registry)?;
                stats.allocs += 1;
            }
            RecordType::SurrogateBind => {
                let db = DatabaseId::new(record.header.database_id);
                let tenant = TenantId::new(record.header.tenant_id);
                apply_surrogate_bind(&record.payload, db, tenant, catalog, registry)?;
                stats.binds += 1;
            }
            RecordType::Noop
            | RecordType::Put
            | RecordType::Delete
            | RecordType::VectorPut
            | RecordType::VectorDelete
            | RecordType::VectorParams
            | RecordType::VectorDirectUpsert
            | RecordType::SparseVectorPut
            | RecordType::SparseVectorDelete
            | RecordType::MultiVectorPut
            | RecordType::MultiVectorDelete
            | RecordType::CrdtDelta
            // CrdtListOp carries no surrogate; the list ops it replays never
            // read `surrogate` (see `CrdtOp::ListInsert`/`ListDelete`/
            // `ListMove`'s dispatch arms, all `surrogate: _`).
            | RecordType::CrdtListOp
            | RecordType::TimeseriesBatch
            | RecordType::LogBatch
            | RecordType::ArrayPut
            | RecordType::ArrayDelete
            | RecordType::ArrayFlush
            | RecordType::Transaction
            | RecordType::TransactionRedo
            | RecordType::Checkpoint
            | RecordType::CollectionTombstoned
            | RecordType::LsnMsAnchor
            | RecordType::TemporalPurge
            | RecordType::CalvinApplied
            // SyncSeqAdvance: not relevant to surrogate replay; the sync
            // idempotency replay pass handles HWM reconstruction.
            | RecordType::SyncSeqAdvance
            | RecordType::FtsIndex
            | RecordType::FtsDelete
            | RecordType::SpatialPut
            | RecordType::SpatialDelete
            | RecordType::GraphNodeLabelSet
            | RecordType::GraphNodeLabelRemove => {}
        }
    }
    Ok(stats)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub allocs: usize,
    pub binds: usize,
}
