// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for `RecordType::SurrogateBind`: idempotently re-apply
//! the `(surrogate, collection, pk_bytes)` triple to the catalog and
//! advance the in-memory registry past the bound surrogate.

use nodedb_types::DatabaseId;
use nodedb_types::Surrogate;
use nodedb_types::TenantId;
use nodedb_wal::record::SurrogateBindPayload;

use crate::control::security::catalog::SystemCatalog;
use crate::control::surrogate::SurrogateRegistryHandle;

pub fn apply_surrogate_bind(
    payload: &[u8],
    database_id: DatabaseId,
    tenant_id: TenantId,
    catalog: &SystemCatalog,
    registry: &SurrogateRegistryHandle,
) -> crate::Result<()> {
    let parsed = SurrogateBindPayload::from_bytes(payload).map_err(crate::Error::Wal)?;
    let surrogate = Surrogate::new(parsed.surrogate);
    catalog.put_surrogate(
        database_id,
        tenant_id,
        &parsed.collection,
        &parsed.pk_bytes,
        surrogate,
    )?;
    let guard = registry.write().map_err(|_| crate::Error::Internal {
        detail: "surrogate registry lock poisoned during WAL replay".into(),
    })?;
    // Bind records can legally be below the current hwm — allocations
    // happen one at a time, but `SurrogateAlloc` records flush in
    // batches at the 1024-ops boundary, so during replay the alloc
    // record may advance the registry past binds replayed later in
    // the stream. Only raise; never lower; never fail.
    if parsed.surrogate > guard.current_hwm() {
        guard.restore_hwm(parsed.surrogate)?;
    }
    Ok(())
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

    #[test]
    fn bind_writes_catalog_and_advances_registry() {
        let (_dir, cat, reg) = open_test();
        let payload = SurrogateBindPayload::new(7u32, "users", b"alice".to_vec())
            .to_bytes()
            .unwrap();
        apply_surrogate_bind(&payload, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg).unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"alice")
                .unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate(
                DatabaseId::DEFAULT,
                TenantId::new(0),
                "users",
                Surrogate::new(7)
            )
            .unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(reg.read().unwrap().current_hwm(), 7);
    }

    #[test]
    fn bind_is_idempotent() {
        let (_dir, cat, reg) = open_test();
        let payload = SurrogateBindPayload::new(3u32, "users", b"bob".to_vec())
            .to_bytes()
            .unwrap();
        apply_surrogate_bind(&payload, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg).unwrap();
        apply_surrogate_bind(&payload, DatabaseId::DEFAULT, TenantId::new(0), &cat, &reg).unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk(DatabaseId::DEFAULT, TenantId::new(0), "users", b"bob")
                .unwrap(),
            Some(Surrogate::new(3))
        );
        assert_eq!(reg.read().unwrap().current_hwm(), 3);
    }
}
