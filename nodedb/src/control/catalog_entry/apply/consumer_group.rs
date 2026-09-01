// SPDX-License-Identifier: BUSL-1.1

//! Apply consumer-group catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader resolves the canonical stream name and reports a
//! duplicate before proposing, so apply carries no policy of its own.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

/// Apply a `PutConsumerGroupIfAbsent` entry. An existing row is kept, so a
/// re-delivered entry cannot overwrite a group's owner or creation time.
pub fn put_if_absent(def: &ConsumerGroupDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_consumer_group_if_absent(def)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "put_consumer_group '{}' on stream '{}'",
                    def.name, def.stream_name
                ),
                e,
            )
        })
        .map(|_| ())
}

/// Apply a `DeleteConsumerGroup` entry. A missing row is not an error.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    stream_name: &str,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_consumer_group(DatabaseId::new(database_id), tenant_id, stream_name, name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_consumer_group '{name}' on stream '{stream_name}'"),
                e,
            )
        })
        .map(|_| ())
}

/// Apply a `MigrateConsumerGroupStream` entry. Writing the canonical key and
/// removing the legacy keys share one transaction, so a replay is a no-op.
pub fn migrate_stream(
    def: &ConsumerGroupDef,
    legacy_stream: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .migrate_consumer_group_stream(def, legacy_stream)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "migrate_consumer_group '{}' from '{legacy_stream}'",
                    def.name
                ),
                e,
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const GROUP: &str = "readers";
    const STREAM: &str = "topic:orders";
    const LEGACY: &str = "orders";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> ConsumerGroupDef {
        ConsumerGroupDef {
            database_id: DatabaseId::new(DB),
            tenant_id: TENANT,
            name: GROUP.to_string(),
            stream_name: STREAM.to_string(),
            owner: "admin".to_string(),
            created_at: 1_000,
        }
    }

    fn delete_entry() -> CatalogEntry {
        CatalogEntry::DeleteConsumerGroup {
            database_id: DB,
            tenant_id: TENANT,
            stream_name: STREAM.to_string(),
            name: GROUP.to_string(),
        }
    }

    #[test]
    fn put_consumer_group_if_absent_roundtrips_through_codec() {
        let entry = CatalogEntry::PutConsumerGroupIfAbsent(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutConsumerGroupIfAbsent(def) => {
                assert_eq!(def.database_id, DatabaseId::new(DB));
                assert_eq!(def.tenant_id, TENANT);
                assert_eq!(def.name, GROUP);
                assert_eq!(def.stream_name, STREAM);
                assert_eq!(def.owner, "admin");
                assert_eq!(def.created_at, 1_000);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_consumer_group_roundtrips_through_codec() {
        let decoded = decode(&encode(&delete_entry()).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteConsumerGroup {
                database_id,
                tenant_id,
                stream_name,
                name,
            } => {
                assert_eq!(database_id, DB);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(stream_name, STREAM);
                assert_eq!(name, GROUP);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn migrate_consumer_group_stream_roundtrips_through_codec() {
        let legacy = ConsumerGroupDef {
            stream_name: LEGACY.to_string(),
            ..sample()
        };
        let entry = CatalogEntry::MigrateConsumerGroupStream {
            def: Box::new(legacy),
            legacy_stream: LEGACY.to_string(),
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::MigrateConsumerGroupStream { def, legacy_stream } => {
                assert_eq!(def.name, GROUP);
                assert_eq!(def.stream_name, LEGACY);
                assert_eq!(legacy_stream, LEGACY);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_consumer_group_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(sample())),
            &catalog,
        )
        .unwrap();
        let stored = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, GROUP);

        apply::apply_to(&delete_entry(), &catalog).unwrap();
        assert!(catalog.load_all_consumer_groups().unwrap().is_empty());
    }

    #[test]
    fn apply_put_keeps_the_existing_consumer_group_definition() {
        let (_dir, catalog) = open_catalog();
        put_if_absent(&sample(), &catalog).expect("apply initial put");

        let reowned = ConsumerGroupDef {
            owner: "someone-else".to_string(),
            created_at: 2_000,
            ..sample()
        };
        put_if_absent(&reowned, &catalog).expect("apply re-put");

        let stored = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(stored.len(), 1, "create-only writes one row: {stored:?}");
        assert_eq!(
            stored[0].owner, "admin",
            "a re-delivered create must not overwrite the original definition"
        );
        assert_eq!(stored[0].created_at, 1_000);
    }

    #[test]
    fn apply_migrate_rekeys_the_group_onto_its_canonical_stream() {
        let (_dir, catalog) = open_catalog();
        let legacy = ConsumerGroupDef {
            stream_name: LEGACY.to_string(),
            ..sample()
        };
        put_if_absent(&legacy, &catalog).expect("seed legacy row");

        migrate_stream(&legacy, LEGACY, &catalog).expect("apply migrate");

        let stored = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(stored.len(), 1, "the legacy row is removed: {stored:?}");
        assert_eq!(stored[0].stream_name, STREAM);
        assert_eq!(stored[0].name, GROUP);
    }

    #[test]
    fn deleting_an_absent_consumer_group_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete(DB, TENANT, STREAM, "never-defined", &catalog).expect("delete absent group");
    }
}
