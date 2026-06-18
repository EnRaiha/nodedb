// SPDX-License-Identifier: BUSL-1.1

//! Key encoding and field extraction helpers for the KV engine.

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

/// Extract all field name → value bytes pairs from a MessagePack-encoded document.
///
/// The value is assumed to be a MessagePack map (as produced by SQL INSERT).
/// Returns `(field_name, value_bytes)` where `value_bytes` is the JSON-serialized
/// scalar value suitable for B-Tree comparison.
pub(super) fn extract_all_field_values_from_msgpack(data: &[u8]) -> Vec<(String, Vec<u8>)> {
    let Ok(value) = nodedb_types::json_from_msgpack(data) else {
        return Vec::new();
    };

    let Some(obj) = value.as_object() else {
        return Vec::new();
    };

    obj.iter()
        .map(|(k, v)| {
            let bytes = json_value_to_index_bytes(v);
            (k.clone(), bytes)
        })
        .collect()
}

/// Extract a single field's value from a MessagePack-encoded document.
pub(super) fn extract_field_values_from_msgpack(data: &[u8], field: &str) -> Vec<Vec<u8>> {
    let Ok(value) = nodedb_types::json_from_msgpack(data) else {
        return Vec::new();
    };

    let Some(obj) = value.as_object() else {
        return Vec::new();
    };

    match obj.get(field) {
        Some(v) => vec![json_value_to_index_bytes(v)],
        None => Vec::new(),
    }
}

/// Convert a JSON value to bytes suitable for B-Tree ordering.
///
/// Strings are stored as UTF-8 bytes. Numbers are stored as big-endian bytes
/// for correct lexicographic ordering. Booleans as 0/1. Null as empty.
fn json_value_to_index_bytes(v: &serde_json::Value) -> Vec<u8> {
    match v {
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Shift to unsigned for correct B-Tree ordering (negatives sort before positives).
                let sortable = (i as u64) ^ (1u64 << 63);
                sortable.to_be_bytes().to_vec()
            } else if let Some(f) = n.as_f64() {
                let bits = f.to_bits();
                let sortable = if bits >> 63 == 1 {
                    !bits // Negative: flip all bits for correct ordering.
                } else {
                    bits | (1u64 << 63) // Positive: set sign bit.
                };
                sortable.to_be_bytes().to_vec()
            } else {
                Vec::new()
            }
        }
        serde_json::Value::Bool(b) => vec![*b as u8],
        serde_json::Value::Null => Vec::new(),
        // Arrays and objects: serialize as JSON string for comparison.
        other => other.to_string().into_bytes(),
    }
}
