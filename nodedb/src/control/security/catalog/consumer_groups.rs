// SPDX-License-Identifier: BUSL-1.1

//! Consumer group metadata operations for the system catalog.

use std::collections::HashMap;

use redb::{ReadableDatabase, ReadableTable};

use super::types::{CONSUMER_GROUPS, SystemCatalog, catalog_err};
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Store a consumer group definition under an unambiguous v2 key.
    pub fn put_consumer_group(&self, def: &ConsumerGroupDef) -> crate::Result<()> {
        let key = group_key(def.database_id, def.tenant_id, &def.stream_name, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert consumer_group", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Insert a consumer group only when its durable identity is absent.
    ///
    /// The existence check and insertion share one redb write transaction, so
    /// concurrent CREATE requests cannot overwrite each other's definition.
    /// Returns `true` when the definition was inserted.
    pub fn put_consumer_group_if_absent(&self, def: &ConsumerGroupDef) -> crate::Result<bool> {
        let key = group_key(def.database_id, def.tenant_id, &def.stream_name, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let inserted = {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            if table
                .get(key.as_str())
                .map_err(|e| catalog_err("get consumer_group", e))?
                .is_some()
            {
                false
            } else {
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert consumer_group", e))?;
                true
            }
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(inserted)
    }

    /// Delete a consumer group. Legacy unscoped records belong to DEFAULT.
    pub fn delete_consumer_group(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> crate::Result<bool> {
        let key = group_key(database_id, tenant_id, stream, group);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let mut existed;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete consumer_group", e))?
                .is_some();
            if database_id == DatabaseId::DEFAULT {
                let legacy = legacy_group_key(tenant_id, stream, group);
                existed |= table
                    .remove(legacy.as_str())
                    .map_err(|e| catalog_err("delete legacy consumer_group", e))?
                    .is_some();
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Move a legacy bare-topic group definition to its canonical stream key.
    /// The caller must establish that `legacy_stream` names a topic before
    /// invoking this; ordinary change-stream names are never rewritten.
    pub fn migrate_consumer_group_stream(
        &self,
        def: &ConsumerGroupDef,
        legacy_stream: &str,
    ) -> crate::Result<()> {
        let mut canonical = def.clone();
        canonical.stream_name = format!("topic:{legacy_stream}");
        let canonical_key = group_key(
            canonical.database_id,
            canonical.tenant_id,
            &canonical.stream_name,
            &canonical.name,
        );
        let legacy_key = group_key(def.database_id, def.tenant_id, legacy_stream, &def.name);
        let bytes = zerompk::to_msgpack_vec(&canonical)
            .map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            table
                .insert(canonical_key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert consumer_group", e))?;
            let _ = table
                .remove(legacy_key.as_str())
                .map_err(|e| catalog_err("delete consumer_group", e))?;
            if def.database_id == DatabaseId::DEFAULT {
                let legacy_key = legacy_group_key(def.tenant_id, legacy_stream, &def.name);
                let _ = table
                    .remove(legacy_key.as_str())
                    .map_err(|e| catalog_err("delete legacy consumer_group", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load all groups while preferring v2 rows over a legacy DEFAULT row with
    /// the same logical identity.
    pub fn load_all_consumer_groups(&self) -> crate::Result<Vec<ConsumerGroupDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;

        let mut groups = HashMap::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range consumer_groups", e))?;
        while let Some(Ok((key, value))) = range.next() {
            if let Some(mut def) = decode_consumer_group(value.value()) {
                let is_v2 = key.value().starts_with("v2:");
                if !is_v2 {
                    def.database_id = DatabaseId::DEFAULT;
                }
                let identity = (
                    def.database_id,
                    def.tenant_id,
                    def.stream_name.clone(),
                    def.name.clone(),
                );
                if is_v2 || !groups.contains_key(&identity) {
                    groups.insert(identity, def);
                }
            }
        }
        Ok(groups.into_values().collect())
    }
}

fn group_key(database_id: DatabaseId, tenant_id: u64, stream: &str, group: &str) -> String {
    format!(
        "v2:{}:{tenant_id}:{}:{stream}:{}:{group}",
        database_id.as_u64(),
        stream.len(),
        group.len()
    )
}

fn legacy_group_key(tenant_id: u64, stream: &str, group: &str) -> String {
    format!("{tenant_id}:{stream}:{group}")
}

/// Positional wire shape written before consumer groups adopted map encoding.
#[derive(zerompk::FromMessagePack, zerompk::ToMessagePack)]
#[msgpack(array)]
struct LegacyConsumerGroupDef {
    tenant_id: u64,
    name: String,
    stream_name: String,
    owner: String,
    created_at: u64,
}

impl From<LegacyConsumerGroupDef> for ConsumerGroupDef {
    fn from(legacy: LegacyConsumerGroupDef) -> Self {
        Self {
            tenant_id: legacy.tenant_id,
            name: legacy.name,
            stream_name: legacy.stream_name,
            owner: legacy.owner,
            created_at: legacy.created_at,
            database_id: DatabaseId::DEFAULT,
        }
    }
}

pub(super) fn decode_consumer_group(bytes: &[u8]) -> Option<ConsumerGroupDef> {
    zerompk::from_msgpack(bytes).ok().or_else(|| {
        zerompk::from_msgpack::<LegacyConsumerGroupDef>(bytes)
            .ok()
            .map(ConsumerGroupDef::from)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn group(database_id: DatabaseId, stream: &str, name: &str) -> ConsumerGroupDef {
        ConsumerGroupDef {
            tenant_id: 1,
            name: name.into(),
            stream_name: stream.into(),
            owner: "admin".into(),
            created_at: 0,
            database_id,
        }
    }

    #[test]
    fn groups_are_isolated_by_database() {
        let catalog = make_catalog();
        catalog
            .put_consumer_group(&group(DatabaseId::new(7), "orders", "analytics"))
            .unwrap();
        catalog
            .put_consumer_group(&group(DatabaseId::new(8), "orders", "analytics"))
            .unwrap();
        assert_eq!(catalog.load_all_consumer_groups().unwrap().len(), 2);
    }

    #[test]
    fn concurrent_put_if_absent_has_one_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let first_catalog = Arc::clone(&catalog);
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_catalog.put_consumer_group_if_absent(&group(
                DatabaseId::new(7),
                "orders",
                "analytics",
            ))
        });
        let second_catalog = Arc::clone(&catalog);
        let second_barrier = Arc::clone(&barrier);
        let second = std::thread::spawn(move || {
            second_barrier.wait();
            second_catalog.put_consumer_group_if_absent(&group(
                DatabaseId::new(7),
                "orders",
                "analytics",
            ))
        });
        barrier.wait();
        assert_ne!(
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap()
        );
        assert_eq!(catalog.load_all_consumer_groups().unwrap().len(), 1);
    }

    #[test]
    fn put_if_absent_preserves_first_definition() {
        let catalog = make_catalog();
        let def = group(DatabaseId::new(7), "orders", "analytics");
        assert!(catalog.put_consumer_group_if_absent(&def).unwrap());
        assert!(!catalog.put_consumer_group_if_absent(&def).unwrap());
        let groups = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, def.name);
    }

    #[test]
    fn legacy_default_group_is_loaded_and_deleted() {
        let catalog = make_catalog();
        let legacy = LegacyConsumerGroupDef {
            tenant_id: 1,
            name: "analytics".into(),
            stream_name: "orders".into(),
            owner: "admin".into(),
            created_at: 0,
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).unwrap();
        let txn = catalog.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(CONSUMER_GROUPS).unwrap();
            table
                .insert(
                    legacy_group_key(1, "orders", "analytics").as_str(),
                    bytes.as_slice(),
                )
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            catalog.load_all_consumer_groups().unwrap()[0].database_id,
            DatabaseId::DEFAULT
        );
        assert!(
            catalog
                .delete_consumer_group(DatabaseId::DEFAULT, 1, "orders", "analytics")
                .unwrap()
        );
    }
}
