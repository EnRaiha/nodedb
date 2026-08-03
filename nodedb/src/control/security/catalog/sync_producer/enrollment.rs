// SPDX-License-Identifier: BUSL-1.1

//! Raft-replicated enrollment exceptions, keyed by certificate SPKI.

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};
use redb::ReadableDatabase;

/// Active Raft-replicated enrollment exceptions, keyed by certificate SPKI.
pub const ENROLLMENT_PREAUTHORIZATIONS: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("_system.enrollment_preauthorizations");

impl SystemCatalog {
    /// Persist an active enrollment exception before exposing it in memory.
    pub fn put_enrollment_preauthorization(
        &self,
        spki: &[u8; 32],
        expires_at_ms: u64,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enrollment preauthorization write txn", e))?;
        {
            let mut table = txn
                .open_table(ENROLLMENT_PREAUTHORIZATIONS)
                .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
            table
                .insert(spki.as_slice(), expires_at_ms)
                .map_err(|e| catalog_err("insert enrollment preauthorization", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("enrollment preauthorization commit", e))
    }

    /// Remove a revoked enrollment exception durably.
    pub fn remove_enrollment_preauthorization(&self, spki: &[u8; 32]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enrollment preauthorization delete txn", e))?;
        {
            let mut table = txn
                .open_table(ENROLLMENT_PREAUTHORIZATIONS)
                .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
            table
                .remove(spki.as_slice())
                .map_err(|e| catalog_err("remove enrollment preauthorization", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("enrollment preauthorization delete commit", e))
    }

    /// Load nonexpired enrollment exceptions for transport rehydration.
    pub fn list_enrollment_preauthorizations(
        &self,
        now_ms: u64,
    ) -> crate::Result<Vec<([u8; 32], u64)>> {
        use redb::ReadableTable as _;

        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("enrollment preauthorization read txn", e))?;
        let table = txn
            .open_table(ENROLLMENT_PREAUTHORIZATIONS)
            .map_err(|e| catalog_err("open enrollment preauthorizations", e))?;
        let mut entries = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate enrollment preauthorizations", e))?
        {
            let (spki, expiry) =
                row.map_err(|e| catalog_err("read enrollment preauthorization", e))?;
            let expires_at_ms = expiry.value();
            if expires_at_ms > now_ms {
                let spki: [u8; 32] =
                    spki.value().try_into().map_err(|_| crate::Error::Storage {
                        engine: "catalog".into(),
                        detail: "invalid enrollment SPKI length".into(),
                    })?;
                entries.push((spki, expires_at_ms));
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrollment_preauthorization_survives_reopen_and_revoke() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let spki = [0x71; 32];
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_enrollment_preauthorization(&spki, 50_000).unwrap();
        }
        {
            let cat = SystemCatalog::open(&path).unwrap();
            assert_eq!(
                cat.list_enrollment_preauthorizations(10_000).unwrap(),
                vec![(spki, 50_000)]
            );
            cat.remove_enrollment_preauthorization(&spki).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        assert!(
            cat.list_enrollment_preauthorizations(10_000)
                .unwrap()
                .is_empty()
        );
    }
}
