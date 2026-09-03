// SPDX-License-Identifier: BUSL-1.1
//! Stored procedure metadata operations for the system catalog.

use super::procedure_types::StoredProcedure;
use super::types::{PROCEDURES, SystemCatalog, catalog_err};
use nodedb_types::id::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;

impl SystemCatalog {
    pub fn put_procedure(&self, procedure: &StoredProcedure) -> crate::Result<()> {
        let key = procedure_key(procedure.tenant_id, procedure.database_id, &procedure.name);
        let bytes = zerompk::to_msgpack_vec(procedure)
            .map_err(|e| catalog_err("serialize procedure", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("open procedures", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert procedure", e))?;
        }
        txn.commit().map_err(|e| catalog_err("commit", e))
    }
    pub fn get_procedure(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredProcedure>> {
        self.get_procedure_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }
    /// Get a procedure in an exact database scope, with the calling
    /// connection's buffered transactional DDL merged in — a `CREATE
    /// PROCEDURE` this same transaction has buffered but not yet committed
    /// resolves here too.
    pub fn get_procedure_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredProcedure>> {
        let committed = self.get_committed_procedure_in_database(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_procedure(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// Committed-only read, bypassing the transaction DDL overlay. The
    /// descriptor stamper reads through this — see
    /// `get_committed_collection` for why.
    pub fn get_committed_procedure_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredProcedure>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(PROCEDURES)
            .map_err(|e| catalog_err("open procedures", e))?;
        let key = procedure_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get procedure", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser procedure", e))
    }
    pub fn delete_procedure(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        self.delete_procedure_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }
    pub fn delete_procedure_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("open procedures", e))?;
            existed = table
                .remove(procedure_key(tenant_id, database_id, name).as_str())
                .map_err(|e| catalog_err("remove procedure", e))?
                .is_some();
        }
        txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }
    pub fn load_all_procedures(&self) -> crate::Result<Vec<StoredProcedure>> {
        self.load_procedures_matching(|_| true)
    }
    pub fn load_procedures_for_tenant(
        &self,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredProcedure>> {
        self.load_procedures_matching(|p| p.tenant_id == tenant_id)
    }
    pub fn load_procedures_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredProcedure>> {
        self.load_procedures_matching(|p| p.database_id == database_id && p.tenant_id == tenant_id)
    }
    fn load_procedures_matching(
        &self,
        include: impl Fn(&StoredProcedure) -> bool,
    ) -> crate::Result<Vec<StoredProcedure>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(PROCEDURES)
            .map_err(|e| catalog_err("open procedures", e))?;
        let mut rows = HashMap::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range procedures", e))?
        {
            let (_key, value) = entry.map_err(|e| catalog_err("read procedure", e))?;
            let procedure: StoredProcedure = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser procedure", e))?;
            if include(&procedure) {
                rows.insert(
                    (
                        procedure.tenant_id,
                        procedure.database_id,
                        procedure.name.clone(),
                    ),
                    procedure,
                );
            }
        }
        Ok(rows.into_values().collect())
    }
}
fn procedure_key(tenant_id: u64, database_id: DatabaseId, name: &str) -> String {
    format!("v2:{tenant_id}:{}:{name}", database_id.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::{
        ParamDirection, ProcedureParam, ProcedureRoutability,
    };

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn procedure(database_id: DatabaseId, body_sql: &str) -> StoredProcedure {
        StoredProcedure {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![ProcedureParam {
                name: "x".into(),
                data_type: "INT".into(),
                direction: ParamDirection::In,
            }],
            body_sql: body_sql.into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            routability: ProcedureRoutability::MultiCollection,
            owner: "admin".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    #[test]
    fn procedures_are_isolated_by_database() {
        let catalog = catalog();
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        catalog
            .put_procedure(&procedure(db1, "BEGIN SELECT 1; END"))
            .unwrap();
        catalog
            .put_procedure(&procedure(db2, "BEGIN SELECT 2; END"))
            .unwrap();

        assert_eq!(
            catalog
                .get_procedure_in_database(db1, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "BEGIN SELECT 1; END"
        );
        assert_eq!(
            catalog
                .get_procedure_in_database(db2, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "BEGIN SELECT 2; END"
        );
        assert_eq!(
            catalog.load_procedures_in_database(db1, 1).unwrap().len(),
            1
        );
        assert_eq!(
            catalog.load_procedures_in_database(db2, 1).unwrap().len(),
            1
        );
        assert!(
            catalog
                .delete_procedure_in_database(db1, 1, "same_name")
                .unwrap()
        );
        assert!(
            catalog
                .get_procedure_in_database(db1, 1, "same_name")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_procedure_in_database(db2, 1, "same_name")
                .unwrap()
                .is_some()
        );
    }
}
