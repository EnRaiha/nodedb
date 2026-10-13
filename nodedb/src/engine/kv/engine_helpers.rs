// SPDX-License-Identifier: BUSL-1.1

//! Key encoding and field extraction helpers for the KV engine.

use super::index::kv_index_key;

/// Compute a numeric key for per-collection HashMap lookups.
///
/// Uses FxHash on `(database_id, tenant_id, collection)` to produce a `u64` —
/// zero allocation, O(1). The `database_id` scopes the key so collections of
/// the same name in different databases never collide.
pub(super) fn table_key(database_id: u64, tenant_id: u64, collection: &str) -> u64 {
    super::hash_helpers::fxhash_multi(&[
        &database_id.to_le_bytes(),
        b":",
        &tenant_id.to_le_bytes(),
        b":",
        collection.as_bytes(),
    ])
}

/// The `{database_id}:{tenant_id}:{collection}\0` prefix shared by the expiry
/// wheel's composite keys and by collection-purge prefix matching. Centralised
/// so the encode shape stays in one place and the two callers can't drift.
/// The null byte separator is safe because collection names can't contain null.
pub(super) fn expiry_prefix(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}\0")
}

/// Construct a composite key for the expiry wheel:
/// "{database_id}:{tenant_id}:{collection}\0{key_bytes}".
pub(super) fn expiry_key(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    key: &[u8],
) -> Vec<u8> {
    let mut composite = expiry_prefix(database_id, tenant_id, collection).into_bytes();
    composite.extend_from_slice(key);
    composite
}

/// Parse a composite expiry key back into (database_id, tenant_id, collection, key_bytes).
pub(super) fn parse_expiry_key(composite: &[u8]) -> Option<(u64, u64, String, Vec<u8>)> {
    let null_pos = composite.iter().position(|&b| b == 0)?;
    let prefix = std::str::from_utf8(&composite[..null_pos]).ok()?;
    let db_colon = prefix.find(':')?;
    let database_id: u64 = prefix[..db_colon].parse().ok()?;
    let rest = &prefix[db_colon + 1..];
    let tenant_colon = rest.find(':')?;
    let tenant_id: u64 = rest[..tenant_colon].parse().ok()?;
    let collection = rest[tenant_colon + 1..].to_string();
    let key = composite[null_pos + 1..].to_vec();
    Some((database_id, tenant_id, collection, key))
}

// ---------------------------------------------------------------------------
// Field extraction from MessagePack values (for secondary index maintenance)
// ---------------------------------------------------------------------------

/// Decode a MessagePack-encoded KV value. `None` when it does not decode.
pub(super) fn decode_document(data: &[u8]) -> Option<serde_json::Value> {
    nodedb_types::json_from_msgpack(data).ok()
}

/// The top-level fields of a MessagePack-encoded KV value, each with the
/// key a secondary index files it under ([`kv_index_key`]). Empty when the
/// value is not a map.
///
/// The fields are read as `nodedb_types::Value`, the form a WHERE predicate
/// compares, so a datetime keeps its instant.
pub(super) fn extract_all_field_values_from_msgpack(data: &[u8]) -> Vec<(String, Vec<u8>)> {
    match nodedb_types::value_from_msgpack(data) {
        Ok(nodedb_types::Value::Object(map)) => map
            .iter()
            .map(|(field, value)| (field.clone(), kv_index_key(value)))
            .collect(),
        _ => Vec::new(),
    }
}

/// One field's index key in a MessagePack-encoded KV value. Empty when the
/// value is not a map or lacks the field.
pub(super) fn extract_field_values_from_msgpack(data: &[u8], field: &str) -> Vec<Vec<u8>> {
    match nodedb_types::value_from_msgpack(data) {
        Ok(nodedb_types::Value::Object(map)) => {
            map.get(field).map(kv_index_key).into_iter().collect()
        }
        _ => Vec::new(),
    }
}
