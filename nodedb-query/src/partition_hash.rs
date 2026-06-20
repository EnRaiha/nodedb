// SPDX-License-Identifier: Apache-2.0

//! Stable, fixed-seed partition hash over a MessagePack row's join-key bytes.
//!
//! This is the ONE routing hash shared by the node-local grace-hash join (Data
//! Plane) and the cross-node shuffle producer (Control Plane). Both planes MUST
//! hash identically: a build-side row and a probe-side row with equal key bytes
//! have to land in the same partition (and therefore on the same consumer node)
//! or the distributed join silently loses matches. Keeping the hash in one
//! shared, pure function — rather than duplicating it per plane — makes that
//! invariant impossible to drift.
//!
//! Hashing rules (must match the join's `hash_join_key` extraction exactly):
//! - present field → hash the raw extracted value bytes;
//! - missing field → hash the `0xc0` (msgpack NIL) sentinel.
//!
//! Only the VALUE bytes are hashed (never the field name), in `keys` order.
//! Uses [`std::hash::DefaultHasher`] (deterministic, fixed keys) rather than
//! `RandomState`, so the same `(keys, bytes)` always produces the same hash
//! across processes and nodes.

use std::hash::Hasher;

use crate::msgpack_scan::extract_field;

/// Locate the value byte range of `key` in a MessagePack map `doc`.
///
/// Matches the join's `extract_join_key_range`: a direct field lookup, falling
/// back to the trailing segment of a dotted/qualified name (`alias.field` →
/// `field`) so qualified probe keys resolve against unqualified row columns.
fn extract_key_range(doc: &[u8], key: &str) -> Option<(usize, usize)> {
    extract_field(doc, 0, key).or_else(|| {
        key.rsplit_once('.')
            .and_then(|(_, field)| extract_field(doc, 0, field))
    })
}

/// Fixed-seed partition hash over `doc`'s `keys` value bytes (seed `0`).
///
/// `keys` is generic over `AsRef<str>` so callers pass `&[&str]` or `&[String]`
/// without an intermediate allocation. This is the TOP-LEVEL partition routing
/// hash; the seed-`0` behaviour is fixed and must not change.
pub fn partition_hash<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> u64 {
    partition_hash_seeded(doc, keys, 0)
}

/// Seeded variant of [`partition_hash`] for RECURSIVE re-partitioning of a
/// skewed partition. The seed is mixed in BEFORE the key bytes so the same
/// `(seed, keys)` pair on build and probe sides produces matching routing while
/// a fresh seed redistributes distinct keys across sub-buckets.
pub fn partition_hash_seeded<S: AsRef<str>>(doc: &[u8], keys: &[S], seed: u64) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    hasher.write_u64(seed);
    for key in keys {
        if let Some((start, end)) = extract_key_range(doc, key.as_ref()) {
            hasher.write(&doc[start..end]);
        } else {
            // Missing field — hash the same NIL sentinel `hash_join_key` uses.
            hasher.write_u8(0xc0);
        }
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
    }

    #[test]
    fn equal_key_bytes_hash_identically() {
        let a = row(&[("k", serde_json::json!(7)), ("v", serde_json::json!("a"))]);
        let b = row(&[("k", serde_json::json!(7)), ("v", serde_json::json!("b"))]);
        assert_eq!(
            partition_hash(&a, &["k"]),
            partition_hash(&b, &["k"]),
            "equal key bytes must co-locate regardless of other columns"
        );
    }

    #[test]
    fn distinct_keys_usually_differ() {
        let a = row(&[("k", serde_json::json!(1))]);
        let b = row(&[("k", serde_json::json!(2))]);
        assert_ne!(partition_hash(&a, &["k"]), partition_hash(&b, &["k"]));
    }

    #[test]
    fn missing_field_uses_nil_sentinel() {
        let present = row(&[("other", serde_json::json!(1))]);
        // Hashing a missing "k" must be deterministic (NIL sentinel), so two rows
        // both missing "k" co-locate.
        let other = row(&[("zzz", serde_json::json!(9))]);
        assert_eq!(
            partition_hash(&present, &["k"]),
            partition_hash(&other, &["k"])
        );
    }

    #[test]
    fn dotted_key_falls_back_to_trailing_segment() {
        let r = row(&[("id", serde_json::json!(5))]);
        assert_eq!(
            partition_hash(&r, &["t.id"]),
            partition_hash(&r, &["id"]),
            "qualified key resolves to the unqualified column"
        );
    }

    #[test]
    fn seed_changes_routing() {
        let r = row(&[("k", serde_json::json!(3))]);
        assert_ne!(
            partition_hash_seeded(&r, &["k"], 0),
            partition_hash_seeded(&r, &["k"], 1),
            "a different seed must redistribute the same key"
        );
    }
}
