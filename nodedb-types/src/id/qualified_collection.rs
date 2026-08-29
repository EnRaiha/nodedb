// SPDX-License-Identifier: Apache-2.0

//! Database-qualified collection name.
//!
//! Storage engines key data on `(tenant_id, collection, document_id)`.
//! Embedding the database ID into the collection token makes isolation
//! between databases automatic, but only if every reader and writer
//! qualifies the same way. This type is the single place that rule lives.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::DatabaseId;

/// A collection name qualified by its database.
///
/// The default database yields the bare collection name; any other
/// database prefixes it with `{database_id}/`. Security state (RLS
/// policies, redaction policies, CRDT write gates) must be stored and
/// looked up under the same `QualifiedCollection`, never a bare name,
/// or the policy silently misses outside the default database.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct QualifiedCollection(String);

impl QualifiedCollection {
    /// Qualify `collection` for `database_id`.
    ///
    /// `DatabaseId::DEFAULT` yields the bare name; any other database
    /// yields `{database_id}/{collection}`.
    pub fn new(database_id: DatabaseId, collection: &str) -> Self {
        if database_id == DatabaseId::DEFAULT {
            Self(collection.to_string())
        } else {
            Self(format!("{}/{}", database_id.as_u64(), collection))
        }
    }

    /// Rebuild a `QualifiedCollection` from a string already qualified on
    /// disk or on the wire. The only way to construct one without going
    /// through `new`; never call this with an unqualified name.
    pub fn from_stored(qualified: String) -> Self {
        Self(qualified)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QualifiedCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_database_yields_bare_name() {
        let q = QualifiedCollection::new(DatabaseId::DEFAULT, "users");
        assert_eq!(q.as_str(), "users");
        assert_eq!(q.to_string(), "users");
    }

    #[test]
    fn non_default_database_yields_prefixed_name() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        assert_eq!(q.as_str(), "7/users");
    }

    #[test]
    fn serde_roundtrips_as_plain_string() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        let json = sonic_rs::to_string(&q).expect("serialize");
        assert_eq!(json, "\"7/users\"");
        let decoded: QualifiedCollection = sonic_rs::from_str(&json).expect("deserialize");
        assert_eq!(q, decoded);
    }

    #[test]
    fn zerompk_roundtrips() {
        let q = QualifiedCollection::new(DatabaseId::new(7), "users");
        let bytes = zerompk::to_msgpack_vec(&q).expect("msgpack serialization must succeed");
        let decoded: QualifiedCollection =
            zerompk::from_msgpack(&bytes).expect("msgpack deserialization must succeed");
        assert_eq!(q, decoded);
    }

    #[test]
    fn equal_inputs_produce_equal_and_same_hash_values() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = QualifiedCollection::new(DatabaseId::new(7), "users");
        let b = QualifiedCollection::new(DatabaseId::new(7), "users");
        assert_eq!(a, b);

        let mut ha = DefaultHasher::new();
        a.hash(&mut ha);
        let mut hb = DefaultHasher::new();
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn from_stored_does_not_qualify() {
        let q = QualifiedCollection::from_stored("7/users".to_string());
        assert_eq!(q.as_str(), "7/users");
    }
}
