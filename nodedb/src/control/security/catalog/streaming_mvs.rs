// SPDX-License-Identifier: BUSL-1.1

//! Streaming MV metadata operations for the system catalog.

use redb::ReadableDatabase;
use std::collections::HashMap;

use crate::event::streaming_mv::StreamingMvDef;
use crate::event::streaming_mv::types::AggDef;
use crate::types::DatabaseId;

use super::types::{STREAMING_MVS, SystemCatalog, catalog_err};

/// Positional format written before streaming MVs became database-scoped.
#[derive(zerompk::FromMessagePack)]
struct LegacyStreamingMvDef {
    tenant_id: u64,
    name: String,
    source_stream: String,
    group_by_columns: Vec<String>,
    aggregates: Vec<AggDef>,
    filter_expr: Option<String>,
    owner: String,
    created_at: u64,
}

impl From<LegacyStreamingMvDef> for StreamingMvDef {
    fn from(legacy: LegacyStreamingMvDef) -> Self {
        Self {
            // Legacy definitions predate database selection and belong to default.
            database_id: DatabaseId::DEFAULT,
            tenant_id: legacy.tenant_id,
            name: legacy.name,
            source_stream: legacy.source_stream,
            group_by_columns: legacy.group_by_columns,
            aggregates: legacy.aggregates,
            filter_expr: legacy.filter_expr,
            owner: legacy.owner,
            created_at: legacy.created_at,
        }
    }
}

impl SystemCatalog {
    pub fn put_streaming_mv(&self, def: &StreamingMvDef) -> crate::Result<()> {
        let key = mv_key(def.database_id, def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize streaming_mv", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert streaming_mv", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_streaming_mv(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = mv_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let mut existed;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete streaming_mv", e))?
                .is_some();
            if database_id == DatabaseId::DEFAULT {
                let legacy_key = legacy_mv_key(tenant_id, name);
                let legacy_existed = table
                    .remove(legacy_key.as_str())
                    .map_err(|e| catalog_err("delete legacy streaming_mv", e))?
                    .is_some();
                existed |= legacy_existed;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    pub fn load_all_streaming_mvs(&self) -> crate::Result<Vec<StreamingMvDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(STREAMING_MVS)
            .map_err(|e| catalog_err("open streaming_mvs", e))?;
        let mut mvs = HashMap::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range streaming_mvs", e))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| catalog_err("read streaming_mv", e))?;
            let is_v2 = key.value().starts_with("v2:");
            let def = zerompk::from_msgpack::<StreamingMvDef>(value.value()).or_else(|_| {
                zerompk::from_msgpack::<LegacyStreamingMvDef>(value.value()).map(Into::into)
            });
            if let Ok(def) = def {
                let identity = (def.database_id, def.tenant_id, def.name.clone());
                // A v2 record is authoritative when both a migrated and legacy row exist.
                if is_v2 || !mvs.contains_key(&identity) {
                    mvs.insert(identity, def);
                }
            }
        }
        Ok(mvs.into_values().collect())
    }
}

fn mv_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> String {
    format!("v2:{}:{tenant_id}:{name}", database_id.as_u64())
}

fn legacy_mv_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}
