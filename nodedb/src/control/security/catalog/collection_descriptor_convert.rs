// SPDX-License-Identifier: BUSL-1.1

//! Bidirectional conversions between [`StoredCollection`] (catalog record)
//! and [`CollectionDescriptor`] (engine-agnostic sync wire type), so
//! `CREATE COLLECTION` and CRDT sync produce the same descriptor shape.
//!
//! `From<&StoredCollection> for CollectionDescriptor` is the emit side: it
//! reads only the 12 fields the descriptor carries, dropping ownership,
//! timestamps, and enforcement/index/constraint state that never travels
//! over sync. [`stored_from_descriptor`] is the receive side: it starts
//! from [`StoredCollection::new`] (which sets sane enforcement defaults
//! and a fresh `created_at`) and overlays only the descriptor-carried
//! fields.

use nodedb_types::sync::wire::CollectionDescriptor;

use super::collection::StoredCollection;

impl From<&StoredCollection> for CollectionDescriptor {
    fn from(stored: &StoredCollection) -> Self {
        Self {
            tenant_id: stored.tenant_id,
            database_id: stored.database_id,
            name: stored.name.clone(),
            collection_type: stored.collection_type.clone(),
            bitemporal: stored.bitemporal,
            crdt: stored.crdt,
            fields: stored.fields.clone(),
            primary: stored.primary,
            vector_primary: stored.vector_primary.clone(),
            partition_strategy: stored.partition_strategy.clone(),
            declared_primary_key: stored.declared_primary_key.clone(),
            descriptor_version: stored.descriptor_version,
        }
    }
}

/// Materialize a [`StoredCollection`] from a synced [`CollectionDescriptor`].
///
/// `owner` is assigned to the receiving peer's identity — the descriptor
/// carries no owner, since ownership is a local catalog concept, not a
/// sync-wire one. All fields not carried by the descriptor (field_defs,
/// event_defs, indexes, constraints, `is_active`, etc.) are left at the
/// [`StoredCollection::new`] defaults.
pub(crate) fn stored_from_descriptor(
    descriptor: &CollectionDescriptor,
    owner: &str,
) -> StoredCollection {
    let mut stored = StoredCollection::new(descriptor.tenant_id, &descriptor.name, owner);
    stored.database_id = descriptor.database_id;
    stored.collection_type = descriptor.collection_type.clone();
    stored.bitemporal = descriptor.bitemporal;
    stored.crdt = descriptor.crdt;
    stored.fields = descriptor.fields.clone();
    stored.primary = descriptor.primary;
    stored.vector_primary = descriptor.vector_primary.clone();
    stored.partition_strategy = descriptor.partition_strategy.clone();
    stored.declared_primary_key = descriptor.declared_primary_key.clone();
    stored.descriptor_version = descriptor.descriptor_version;
    stored
}

#[cfg(test)]
mod tests {
    use nodedb_types::CollectionType;
    use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine, VectorPrimaryConfig};
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::kv::KvConfig;

    use super::*;

    fn assert_mapped_fields_match(stored: &StoredCollection, back: &StoredCollection) {
        assert_eq!(back.name, stored.name);
        assert_eq!(back.tenant_id, stored.tenant_id);
        assert_eq!(back.database_id, stored.database_id);
        assert_eq!(back.collection_type, stored.collection_type);
        assert_eq!(back.bitemporal, stored.bitemporal);
        assert_eq!(back.crdt, stored.crdt);
        assert_eq!(back.fields, stored.fields);
        assert_eq!(back.primary, stored.primary);
        assert_eq!(back.vector_primary, stored.vector_primary);
        assert_eq!(back.partition_strategy, stored.partition_strategy);
        assert_eq!(back.declared_primary_key, stored.declared_primary_key);
        assert_eq!(back.descriptor_version, stored.descriptor_version);
    }

    fn base_stored(name: &str, collection_type: CollectionType) -> StoredCollection {
        let mut stored = StoredCollection::new(7, name, "alice");
        stored.partition_strategy =
            PartitionStrategy::default_for_collection_type(&collection_type);
        stored.collection_type = collection_type;
        stored.bitemporal = true;
        stored.crdt = true;
        stored.declared_primary_key = Some("id".to_string());
        stored.descriptor_version = 5;
        stored
    }

    #[test]
    fn document_schemaless_round_trips() {
        let stored = base_stored("users", CollectionType::document());
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn document_strict_round_trips() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("bio", ColumnType::String),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let stored = base_stored("people", CollectionType::strict(schema));
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn key_value_round_trips() {
        let schema = StrictSchema {
            columns: vec![ColumnDef::required("id", ColumnType::Int64).with_primary_key()],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let stored = base_stored("sessions", CollectionType::kv(schema));
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
        // Sanity: the KvConfig payload itself survived intact.
        match (&stored.collection_type, &back.collection_type) {
            (CollectionType::KeyValue(a), CollectionType::KeyValue(b)) => {
                let a: &KvConfig = a;
                let b: &KvConfig = b;
                assert_eq!(a.schema, b.schema);
            }
            _ => panic!("expected KeyValue collection type"),
        }
    }

    #[test]
    fn columnar_plain_round_trips() {
        let stored = base_stored("events", CollectionType::columnar());
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn timeseries_round_trips() {
        let stored = base_stored("metrics", CollectionType::timeseries("ts", "1m"));
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn spatial_round_trips() {
        let stored = base_stored("places", CollectionType::spatial("geom"));
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn vector_primary_round_trips() {
        let mut stored = base_stored("embeddings", CollectionType::document());
        stored.primary = PrimaryEngine::Vector;
        stored.vector_primary = Some(VectorPrimaryConfig {
            vector_field: "emb".to_string(),
            dim: 768,
            ..VectorPrimaryConfig::default()
        });
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_mapped_fields_match(&stored, &back);
    }

    #[test]
    fn bitemporal_flag_round_trips() {
        let mut stored = base_stored("audit_log", CollectionType::document());
        stored.bitemporal = true;
        let descriptor = CollectionDescriptor::from(&stored);
        assert!(descriptor.bitemporal);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert!(back.bitemporal);
    }

    #[test]
    fn crdt_flag_round_trips() {
        let mut stored = base_stored("synced_docs", CollectionType::document());
        stored.crdt = true;
        let descriptor = CollectionDescriptor::from(&stored);
        assert!(descriptor.crdt);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert!(back.crdt);
    }

    #[test]
    fn owner_assigned_on_receive() {
        let stored = base_stored("owned", CollectionType::document());
        assert_eq!(stored.owner, "alice");
        let descriptor = CollectionDescriptor::from(&stored);
        let back = stored_from_descriptor(&descriptor, "sync");
        assert_eq!(back.owner, "sync");
    }
}
