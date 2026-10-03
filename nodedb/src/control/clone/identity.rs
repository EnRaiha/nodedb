// SPDX-License-Identifier: BUSL-1.1

//! A clone copy keeps its row's identity.
//!
//! A schemaless row with no `id` in its body takes its identity from its
//! surrogate. A copy into the clone target lands under a new surrogate, so
//! without `id` in the body the copy shows a new identity, and a clone
//! read cannot match it to its source row by primary key. The copy writes
//! the source identity into `id` instead. A collection that declares its
//! primary key already carries the key in the body. A strict row carries its
//! identity in `_rowid`, and a hash-chained row carries `id` from its first
//! write, so the copy leaves both unchanged.

use nodedb_query::msgpack_scan;
use nodedb_types::DEFAULT_IDENTITY_COLUMN;

use crate::control::security::catalog::StoredCollection;

/// `body` as the clone target of `coll` stores it, for the row whose identity
/// is `identity`.
pub(crate) fn carry_identity(coll: &StoredCollection, body: Vec<u8>, identity: &str) -> Vec<u8> {
    if coll.declared_primary_key.is_some()
        || !coll.collection_type.is_schemaless()
        || msgpack_scan::map_header(&body, 0).is_none()
    {
        return body;
    }
    // Leaves a body that already carries `id` unchanged.
    msgpack_scan::inject_str_field(&body, DEFAULT_IDENTITY_COLUMN, identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{RowIdentity, StorageKey, Surrogate};

    /// The copy of a minted row under a new surrogate keeps the source
    /// identity.
    #[test]
    fn minted_row_copy_keeps_its_identity() {
        let coll = StoredCollection::new(1, "docs", "admin");
        assert!(coll.collection_type.is_schemaless());
        let body = msgpack_scan::build_str_map(&[("content", "a")]);
        let source_key = StorageKey::for_surrogate(Surrogate::new(123));
        let identity = RowIdentity::of_stored_row(&body, None, source_key).into_string();
        assert_eq!(identity, "123");

        let copied = carry_identity(&coll, body, &identity);
        let target_key = StorageKey::for_surrogate(Surrogate::new(456));
        assert_eq!(
            RowIdentity::of_stored_row(&copied, None, target_key).as_str(),
            "123"
        );
    }

    #[test]
    fn body_with_id_is_unchanged() {
        let coll = StoredCollection::new(1, "docs", "admin");
        let body = msgpack_scan::build_str_map(&[("id", "d1"), ("content", "a")]);
        assert_eq!(carry_identity(&coll, body.clone(), "d1"), body);
    }
}
