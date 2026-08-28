// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for `RecordType::SurrogateAlloc`: monotonically advance
//! the in-memory `SurrogateRegistry` hwm.

use nodedb_wal::record::SurrogateAllocPayload;

use crate::control::surrogate::SurrogateRegistryHandle;

pub fn apply_surrogate_alloc(
    payload: &[u8],
    registry: &SurrogateRegistryHandle,
) -> crate::Result<()> {
    let parsed = SurrogateAllocPayload::from_bytes(payload).map_err(crate::Error::Wal)?;
    let guard = registry.write().map_err(|_| crate::Error::Internal {
        detail: "surrogate registry lock poisoned during WAL replay".into(),
    })?;
    guard.restore_hwm(parsed.hi)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::Surrogate;

    use super::*;
    use crate::control::surrogate::{SurrogateRegistry, SurrogateRegistryMode};

    #[test]
    fn alloc_advances_registry() {
        let reg: SurrogateRegistryHandle = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let payload = SurrogateAllocPayload::new(42).to_bytes();
        apply_surrogate_alloc(&payload, &reg).unwrap();
        let next = reg.read().unwrap().current_hwm();
        assert_eq!(next, 42);
    }

    #[test]
    fn alloc_below_current_hwm_is_noop() {
        let reg: SurrogateRegistryHandle =
            Arc::new(RwLock::new(SurrogateRegistry::from_persisted_hwm(100)));
        let payload = SurrogateAllocPayload::new(50).to_bytes();
        apply_surrogate_alloc(&payload, &reg).unwrap();
        assert_eq!(reg.read().unwrap().current_hwm(), 100);
    }

    #[test]
    fn alloc_idempotent_at_same_hwm() {
        let reg: SurrogateRegistryHandle =
            Arc::new(RwLock::new(SurrogateRegistry::from_persisted_hwm(7)));
        let payload = SurrogateAllocPayload::new(7).to_bytes();
        apply_surrogate_alloc(&payload, &reg).unwrap();
        let guard = reg.read().unwrap();
        let SurrogateRegistryMode::Local(local) = guard.mode() else {
            panic!("expected Local mode");
        };
        let s = local.alloc_one().unwrap();
        assert_eq!(s, Surrogate::new(8));
    }
}
