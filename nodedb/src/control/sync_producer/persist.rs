// SPDX-License-Identifier: BUSL-1.1

//! `SystemCatalog`-backed persistence for the producer-id hwm.
//!
//! Delegates `checkpoint` / `load` to `put_producer_hwm` /
//! `get_producer_hwm` on the system catalog, mirroring the pattern of
//! `SurrogateHwmPersist` + `SystemCatalogHwm` in
//! `crate::control::surrogate::persist`.
//!
//! ## Stage-5 follow-up
//!
//! S5 will additionally append a WAL record (`RecordType::ProducerHwm`) at
//! every `checkpoint` so post-crash replay can rebuild the hwm even if the
//! redb table has fallen behind.  The record kind is reserved but not emitted
//! here, exactly as `SURROGATE_HWM` (WAL record `51 | 0x8000`) is reserved
//! in the surrogate design.

use std::sync::Arc;

use crate::control::security::catalog::SystemCatalog;
use crate::control::sync_producer::allocator::ProducerHwmPersist;

/// `SystemCatalog`-backed producer-hwm persistence — delegates to
/// `put_producer_hwm` / `get_producer_hwm`.
pub struct SystemCatalogProducerHwm {
    catalog: Arc<SystemCatalog>,
}

impl SystemCatalogProducerHwm {
    pub fn new(catalog: Arc<SystemCatalog>) -> Self {
        Self { catalog }
    }
}

impl ProducerHwmPersist for SystemCatalogProducerHwm {
    fn checkpoint(&self, hwm: u64) -> crate::Result<()> {
        self.catalog.put_producer_hwm(hwm)
    }

    fn load(&self) -> crate::Result<u64> {
        self.catalog.get_producer_hwm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_roundtrip_via_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let p = SystemCatalogProducerHwm::new(catalog);
        assert_eq!(p.load().unwrap(), 0);
        p.checkpoint(999).unwrap();
        assert_eq!(p.load().unwrap(), 999);
    }
}
