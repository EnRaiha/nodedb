// SPDX-License-Identifier: BUSL-1.1

//! Apply durable-topic catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader checks the topic name and the duplicate before
//! proposing, so apply runs the unvalidated catalog path: a rejection here
//! would leave followers without a topic the leader already accepted.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;

/// Apply a `CreateTopicIfAbsent` entry.
///
/// An existing definition is kept, so a re-delivered entry cannot rewind the
/// topic's durable sequence or LSN high-water marks.
pub fn create_if_absent(def: &TopicDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .create_ep_topic_unchecked(def)
        .map_err(|e| catalog_err(&format!("create_ep_topic '{}'", def.name), e))
        .map(|_| ())
}

/// Apply a `DeleteTopicWithConsumerGroups` entry.
///
/// A missing topic is not an error: the entry is idempotent under replay.
pub fn delete_with_consumer_groups(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_ep_topic_with_consumer_groups_unchecked(
            DatabaseId::new(database_id),
            tenant_id,
            name,
        )
        .map_err(|e| catalog_err(&format!("delete_ep_topic '{name}'"), e))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};
    use crate::event::cdc::stream_def::RetentionConfig;

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const NAME: &str = "orders";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> TopicDef {
        TopicDef {
            database_id: DatabaseId::new(DB),
            tenant_id: TENANT,
            name: NAME.to_string(),
            retention: RetentionConfig {
                max_events: 10_000,
                max_age_secs: 3_600,
            },
            owner: "admin".to_string(),
            created_at: 1_000,
            last_sequence: 0,
            last_lsn: 0,
        }
    }

    fn delete_entry() -> CatalogEntry {
        CatalogEntry::DeleteTopicWithConsumerGroups {
            database_id: DB,
            tenant_id: TENANT,
            name: NAME.to_string(),
        }
    }

    #[test]
    fn create_topic_if_absent_roundtrips_through_codec() {
        let entry = CatalogEntry::CreateTopicIfAbsent(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::CreateTopicIfAbsent(def) => {
                assert_eq!(def.database_id, DatabaseId::new(DB));
                assert_eq!(def.tenant_id, TENANT);
                assert_eq!(def.name, NAME);
                assert_eq!(def.owner, "admin");
                assert_eq!(def.retention.max_age_secs, 3_600);
                assert_eq!(def.created_at, 1_000);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_topic_with_consumer_groups_roundtrips_through_codec() {
        let decoded = decode(&encode(&delete_entry()).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteTopicWithConsumerGroups {
                database_id,
                tenant_id,
                name,
            } => {
                assert_eq!(database_id, DB);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(name, NAME);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_topic_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::CreateTopicIfAbsent(Box::new(sample())),
            &catalog,
        )
        .unwrap();
        let stored = catalog.load_all_ep_topics().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, NAME);

        apply::apply_to(&delete_entry(), &catalog).unwrap();
        assert!(catalog.load_all_ep_topics().unwrap().is_empty());
    }

    #[test]
    fn apply_create_keeps_the_existing_topic_definition() {
        let (_dir, catalog) = open_catalog();
        let mut advanced = sample();
        advanced.last_sequence = 42;
        advanced.last_lsn = 99;
        create_if_absent(&advanced, &catalog).expect("apply initial create");

        create_if_absent(&sample(), &catalog).expect("apply re-create");

        let stored = catalog.load_all_ep_topics().unwrap();
        assert_eq!(stored.len(), 1, "create-only writes one row: {stored:?}");
        assert_eq!(
            stored[0].last_sequence, 42,
            "a re-delivered create must not rewind the sequence high-water mark"
        );
        assert_eq!(stored[0].last_lsn, 99);
    }

    #[test]
    fn apply_delete_removes_the_attached_consumer_groups() {
        use crate::event::cdc::consumer_group::ConsumerGroupDef;

        let (_dir, catalog) = open_catalog();
        create_if_absent(&sample(), &catalog).expect("apply create");
        catalog
            .put_consumer_group(&ConsumerGroupDef {
                database_id: DatabaseId::new(DB),
                tenant_id: TENANT,
                name: "readers".to_string(),
                stream_name: format!("topic:{NAME}"),
                owner: "admin".to_string(),
                created_at: 1_000,
            })
            .expect("seed group");

        delete_with_consumer_groups(DB, TENANT, NAME, &catalog).expect("apply delete");

        assert!(catalog.load_all_consumer_groups().unwrap().is_empty());
    }

    #[test]
    fn deleting_an_absent_topic_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete_with_consumer_groups(DB, TENANT, "never-defined", &catalog)
            .expect("delete absent topic");
    }
}
