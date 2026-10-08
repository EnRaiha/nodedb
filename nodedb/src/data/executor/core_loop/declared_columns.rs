// SPDX-License-Identifier: BUSL-1.1

//! The declared numeric columns a schemaless document or KV write re-types,
//! read from the collection's registered config.

use nodedb_physical::physical_plan::DeclaredColumn;

use crate::types::{DatabaseId, TenantId};

use super::CoreLoop;

impl CoreLoop {
    /// The declared numeric columns of the collection `config_key` names.
    ///
    /// Empty when the collection declares none, is strict or columnar, or is
    /// not registered on this core.
    pub(in crate::data::executor) fn declared_columns(
        &self,
        config_key: &(DatabaseId, TenantId, String),
    ) -> &[DeclaredColumn] {
        self.doc_configs
            .get(config_key)
            .map(|config| config.declared_columns.as_slice())
            .unwrap_or(&[])
    }

    /// [`Self::declared_columns`] for a collection named by its parts.
    pub(in crate::data::executor) fn declared_columns_of(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> &[DeclaredColumn] {
        self.declared_columns(&(
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        ))
    }
}
