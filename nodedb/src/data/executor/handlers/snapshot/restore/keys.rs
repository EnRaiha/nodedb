// SPDX-License-Identifier: BUSL-1.1

//! Snapshot-key parsing helpers shared across the per-engine restore paths.

/// Parse a timeseries snapshot key into `(database_id, tenant_id, collection)`.
///
/// Backward-compatible with pre-scoping snapshots:
///   * 3+ parts → `database:tenant:collection` (collection may itself contain
///     ':' — only the first two ':' are structural).
///   * 2 parts → legacy `tenant:collection`; database defaults to 0
///     (`DatabaseId::DEFAULT`).
///   * 1 part → bare collection; database and tenant default to 0.
pub(in crate::data::executor::handlers::snapshot) fn parse_timeseries_snapshot_key(
    key: &str,
) -> (u64, u64, String) {
    let mut it = key.splitn(3, ':');
    let first = it.next().unwrap_or("");
    let second = it.next();
    let third = it.next();
    match (second, third) {
        (Some(tenant), Some(collection)) => {
            let db = first.parse::<u64>().unwrap_or(0);
            let tid = tenant.parse::<u64>().unwrap_or(0);
            (db, tid, collection.to_string())
        }
        (Some(collection), None) => {
            // Legacy 2-part key: "{tenant}:{collection}".
            let tid = first.parse::<u64>().unwrap_or(0);
            (0, tid, collection.to_string())
        }
        _ => (0, 0, first.to_string()),
    }
}

/// Parse a vector snapshot key into `(database_id, collection_key)`.
///
/// Backward-compatible with pre-scoping snapshots:
///   * 3 parts where the first two are BOTH numeric → `db:tenant:coll_key`
///     (new format; `coll_key` may itself contain ':').
///   * otherwise → legacy `tenant:coll_key`; strip the leading numeric tenant
///     component and default the database to 0 (`DatabaseId::DEFAULT`).
///
/// `coll_key` is returned as a borrowed slice of `key` so the caller can pass
/// it straight into `restore_vector_collection` without an allocation.
pub(super) fn parse_vector_snapshot_key(key: &str, tenant_id: u64) -> (u64, &str) {
    let mut it = key.splitn(3, ':');
    let first = it.next().unwrap_or("");
    let second = it.next();
    let third = it.next();
    if let (Some(second), Some(_)) = (second, third)
        && let (Ok(db), Ok(_tid)) = (first.parse::<u64>(), second.parse::<u64>())
    {
        // New format: "{db}:{tid}:{coll_key}".
        let prefix_len = first.len() + 1 + second.len() + 1;
        return (db, &key[prefix_len..]);
    }
    // Legacy 2-part key "{tid}:{coll_key}" (or a bare key); strip the tenant
    // prefix if present and default the database to 0.
    let tid_prefix = format!("{tenant_id}:");
    let coll_key = key.strip_prefix(&tid_prefix).unwrap_or(key);
    (0, coll_key)
}

/// Recover the database id encoded in a db-qualified collection name.
///
/// Non-default databases qualify their collections as `"{database_id}/{name}"`;
/// the default database uses the bare name. A bare name (no leading numeric
/// segment before a `/`) maps to `DatabaseId::DEFAULT` (0).
///
/// Shared with the KV checkpoint path (`data::executor::kv_checkpoint`), which
/// faces the same problem: `KvEngine` stores the db-qualified name in
/// `hash_to_collection`, and the database id must be recovered from it to
/// rebuild the table key a live read computes.
pub(in crate::data::executor) fn database_id_from_qualified(collection: &str) -> u64 {
    match collection.split_once('/') {
        Some((prefix, _)) => prefix.parse::<u64>().unwrap_or(0),
        None => 0,
    }
}
