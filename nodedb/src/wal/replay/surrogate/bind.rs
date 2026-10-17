// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for `RecordType::SurrogateBindBatch`: idempotently re-apply
//! every `(surrogate, pk_bytes)` binding of the batch to the catalog and
//! advance the in-memory registry past the highest bound surrogate.

use nodedb_types::DatabaseId;
use nodedb_types::Surrogate;
use nodedb_types::TenantId;
use nodedb_wal::record::SurrogateBindBatchPayload;

use crate::control::security::catalog::SystemCatalog;
use crate::control::surrogate::SurrogateRegistryHandle;

/// Write every binding of `batch` in one catalog transaction, insert or
/// overwrite, in batch order. Returns the number of bindings applied.
pub fn apply_surrogate_bind_batch(
    batch: &SurrogateBindBatchPayload,
    database_id: DatabaseId,
    tenant_id: TenantId,
    catalog: &SystemCatalog,
    registry: &SurrogateRegistryHandle,
) -> crate::Result<usize> {
    let rows: Vec<(&[u8], Surrogate)> = batch
        .entries
        .iter()
        .map(|(surrogate, pk)| (pk.as_slice(), Surrogate::new(*surrogate)))
        .collect();
    catalog.overwrite_surrogates(
        nodedb_types::CollectionKey::from_bare(database_id, &batch.collection),
        tenant_id,
        &rows,
    )?;
    let Some(highest) = batch.entries.iter().map(|(surrogate, _)| *surrogate).max() else {
        return Ok(0);
    };
    let guard = registry.write().map_err(|_| crate::Error::Internal {
        detail: "surrogate registry lock poisoned during WAL replay".into(),
    })?;
    // Bind records can legally be below the current hwm — `SurrogateAlloc`
    // records flush in batches at the 1024-ops boundary, so during replay
    // the alloc record may advance the registry past binds replayed later
    // in the stream. Only raise; never lower; never fail.
    if highest > guard.current_hwm() {
        guard.restore_hwm(highest)?;
    }
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::control::surrogate::SurrogateRegistry;

    fn open_test() -> (tempfile::TempDir, SystemCatalog, SurrogateRegistryHandle) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        (dir, cat, reg)
    }

    fn users() -> nodedb_types::CollectionKey<'static> {
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
    }

    #[test]
    fn batch_writes_catalog_and_advances_registry() {
        let (_dir, cat, reg) = open_test();
        let batch = SurrogateBindBatchPayload::new(
            "users",
            vec![(7, b"alice".to_vec()), (5, b"bob".to_vec())],
        );
        let applied =
            apply_surrogate_bind_batch(&batch, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg)
                .unwrap();
        assert_eq!(applied, 2);
        assert_eq!(
            cat.get_surrogate_for_pk(users(), TenantId::new(0), b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate(users(), TenantId::new(0), Surrogate::new(5))
                .unwrap(),
            Some(b"bob".to_vec())
        );
        assert_eq!(reg.read().unwrap().current_hwm(), 7);
    }

    #[test]
    fn batch_is_idempotent() {
        let (_dir, cat, reg) = open_test();
        let batch = SurrogateBindBatchPayload::new("users", vec![(3, b"bob".to_vec())]);
        apply_surrogate_bind_batch(&batch, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg)
            .unwrap();
        apply_surrogate_bind_batch(&batch, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg)
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(users(), TenantId::new(0), b"bob")
                .unwrap(),
            Some(Surrogate::new(3))
        );
        assert_eq!(reg.read().unwrap().current_hwm(), 3);
    }

    /// Two first-wins chunks, the second rewriting half the first's keys,
    /// log only the rows they inserted. Replaying those records into an
    /// empty catalog rebuilds the same map in both directions.
    #[test]
    fn replaying_logged_binds_rebuilds_the_first_wins_map() {
        let (_live_dir, live, _) = open_test();
        let (_replay_dir, replayed, reg) = open_test();
        let t0 = TenantId::new(0);
        let pks: Vec<Vec<u8>> = (0..150u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let chunk = |range: std::ops::Range<usize>, base: u32| {
            range
                .map(|i| (pks[i].as_slice(), Surrogate::new(base + i as u32)))
                .collect::<Vec<_>>()
        };
        let mut records = Vec::new();
        for rows in [chunk(0..100, 1), chunk(50..150, 1_000)] {
            let bound = live.put_surrogates(users(), t0, &rows).unwrap();
            // The assigner logs exactly the rows its commit inserted.
            let inserted: Vec<(u32, Vec<u8>)> = rows
                .iter()
                .zip(&bound)
                .filter(|(_, binding)| binding.inserted)
                .map(|(&(pk, _), binding)| (binding.surrogate.as_u32(), pk.to_vec()))
                .collect();
            records.push(SurrogateBindBatchPayload::new("users", inserted));
        }
        for batch in &records {
            apply_surrogate_bind_batch(batch, DatabaseId::DEFAULT, t0, &replayed, &reg).unwrap();
        }
        let live_map = live.scan_surrogates_for_collection(users(), t0).unwrap();
        assert_eq!(live_map.len(), 150);
        assert_eq!(
            replayed
                .scan_surrogates_for_collection(users(), t0)
                .unwrap(),
            live_map
        );
        for (pk, surrogate) in &live_map {
            assert_eq!(
                replayed
                    .get_pk_for_surrogate(users(), t0, *surrogate)
                    .unwrap(),
                Some(pk.clone())
            );
        }
        assert_eq!(reg.read().unwrap().current_hwm(), 1_149);
    }

    #[test]
    fn empty_batch_changes_nothing() {
        let (_dir, cat, reg) = open_test();
        let batch = SurrogateBindBatchPayload::new("users", Vec::new());
        let applied =
            apply_surrogate_bind_batch(&batch, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg)
                .unwrap();
        assert_eq!(applied, 0);
        assert_eq!(reg.read().unwrap().current_hwm(), 0);
    }
}
