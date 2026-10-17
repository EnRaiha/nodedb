// SPDX-License-Identifier: BUSL-1.1

//! Surrogate scope ids and the key layout of the surrogate PK map tables.
//!
//! A scope id names one `(database, tenant, collection)` PK map. The forward
//! table keys by `scope_id` (big-endian) followed by the pk bytes. The reverse
//! table keys by `(scope_id << 32) | surrogate`. One scope's rows are one
//! contiguous key range in each table.

use std::ops::Range;

use nodedb_types::{CollectionKey, TenantId};
use redb::{ReadTransaction, ReadableTable, WriteTransaction};

use super::types::{
    SURROGATE_PK, SURROGATE_PK_REV, SURROGATE_SCOPE_SEQ, SURROGATE_SCOPES, catalog_err,
};

/// Bytes of the scope id prefix of a forward-table key.
pub(super) const SCOPE_PREFIX_LEN: usize = 8;

/// The first scope id the allocator hands out.
const FIRST_SCOPE: u64 = 1;

/// The forward-table key of `pk` in `scope`.
pub(super) fn fwd_key(scope: u64, pk: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(SCOPE_PREFIX_LEN + pk.len());
    key.extend_from_slice(&scope.to_be_bytes());
    key.extend_from_slice(pk);
    key
}

/// The pk bytes of a forward-table key. A key shorter than its scope prefix
/// is corrupt.
pub(super) fn pk_of(key: &[u8]) -> crate::Result<&[u8]> {
    key.get(SCOPE_PREFIX_LEN..).ok_or_else(|| {
        catalog_err(
            "surrogate_pk row",
            format!("a {}-byte key is shorter than its scope prefix", key.len()),
        )
    })
}

/// The forward-table key range that holds every row of `scope`.
pub(super) fn fwd_range(scope: u64) -> ([u8; SCOPE_PREFIX_LEN], [u8; SCOPE_PREFIX_LEN]) {
    (scope.to_be_bytes(), scope.saturating_add(1).to_be_bytes())
}

/// The reverse-table key of `surrogate` in `scope`.
pub(super) fn rev_key(scope: u64, surrogate: u32) -> u128 {
    (u128::from(scope) << 32) | u128::from(surrogate)
}

/// The reverse-table key range that holds every row of `scope`.
pub(super) fn rev_range(scope: u64) -> Range<u128> {
    rev_key(scope, 0)..(u128::from(scope) + 1) << 32
}

/// The scope id of `key` under `tenant_id` in a read transaction. `None`
/// when the collection has never bound a key.
pub(super) fn read_scope(
    txn: &ReadTransaction,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
) -> crate::Result<Option<u64>> {
    let scopes = txn
        .open_table(SURROGATE_SCOPES)
        .map_err(|e| catalog_err("open surrogate_scopes", e))?;
    lookup_scope(&scopes, key, tenant_id)
}

/// The scope id of `key` under `tenant_id` in a write transaction, without
/// creating one.
pub(super) fn write_scope(
    txn: &WriteTransaction,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
) -> crate::Result<Option<u64>> {
    let scopes = txn
        .open_table(SURROGATE_SCOPES)
        .map_err(|e| catalog_err("open surrogate_scopes", e))?;
    lookup_scope(&scopes, key, tenant_id)
}

/// The scope id of `key` under `tenant_id`, allocated in `txn` when the
/// collection has none.
pub(super) fn scope_or_create(
    txn: &WriteTransaction,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
) -> crate::Result<u64> {
    let mut scopes = txn
        .open_table(SURROGATE_SCOPES)
        .map_err(|e| catalog_err("open surrogate_scopes", e))?;
    if let Some(scope) = lookup_scope(&scopes, key, tenant_id)? {
        return Ok(scope);
    }
    let mut seq = txn
        .open_table(SURROGATE_SCOPE_SEQ)
        .map_err(|e| catalog_err("open surrogate_scope_seq", e))?;
    let next = seq
        .get(())
        .map_err(|e| catalog_err("get surrogate_scope_seq", e))?
        .map_or(FIRST_SCOPE, |v| v.value());
    // `fwd_range` and `rev_range` name the scope after this one, so the last
    // id is never handed out.
    let after = next
        .checked_add(1)
        .filter(|after| *after < u64::MAX)
        .ok_or_else(|| catalog_err("surrogate scope allocate", "scope ids exhausted"))?;
    seq.insert((), after)
        .map_err(|e| catalog_err("insert surrogate_scope_seq", e))?;
    scopes
        .insert(
            (key.database_id().as_u64(), tenant_id.as_u64(), key.name()),
            next,
        )
        .map_err(|e| catalog_err("insert surrogate_scopes", e))?;
    Ok(next)
}

/// Remove the scope row of `key` under `tenant_id` and return its id. The
/// map rows of the scope stay: the caller drains or re-points them.
pub(super) fn take_scope(
    txn: &WriteTransaction,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
) -> crate::Result<Option<u64>> {
    let mut scopes = txn
        .open_table(SURROGATE_SCOPES)
        .map_err(|e| catalog_err("open surrogate_scopes", e))?;
    let removed = scopes
        .remove((key.database_id().as_u64(), tenant_id.as_u64(), key.name()))
        .map_err(|e| catalog_err("remove surrogate_scopes", e))?
        .map(|v| v.value());
    Ok(removed)
}

/// Name `scope` as the scope of `key` under `tenant_id`. The caller has
/// checked that `key` has no scope.
pub(super) fn assign_scope(
    txn: &WriteTransaction,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
    scope: u64,
) -> crate::Result<()> {
    let mut scopes = txn
        .open_table(SURROGATE_SCOPES)
        .map_err(|e| catalog_err("open surrogate_scopes", e))?;
    scopes
        .insert(
            (key.database_id().as_u64(), tenant_id.as_u64(), key.name()),
            scope,
        )
        .map_err(|e| catalog_err("insert surrogate_scopes", e))?;
    Ok(())
}

/// Remove every map row of `scope` from both tables and return the removed
/// `(pk_bytes, surrogate)` pairs in pk order.
pub(super) fn drain_scope(
    txn: &WriteTransaction,
    scope: u64,
) -> crate::Result<Vec<(Vec<u8>, u32)>> {
    let mut fwd = txn
        .open_table(SURROGATE_PK)
        .map_err(|e| catalog_err("open surrogate_pk", e))?;
    let (lo, hi) = fwd_range(scope);
    let mut rows = Vec::new();
    for row in fwd
        .range(lo.as_slice()..hi.as_slice())
        .map_err(|e| catalog_err("range surrogate_pk", e))?
    {
        let (k, v) = row.map_err(|e| catalog_err("iter surrogate_pk row", e))?;
        rows.push((pk_of(k.value())?.to_vec(), v.value()));
    }
    fwd.retain_in(lo.as_slice()..hi.as_slice(), |_, _| false)
        .map_err(|e| catalog_err("drain surrogate_pk", e))?;
    let mut rev = txn
        .open_table(SURROGATE_PK_REV)
        .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
    rev.retain_in(rev_range(scope), |_, _| false)
        .map_err(|e| catalog_err("drain surrogate_pk_rev", e))?;
    Ok(rows)
}

fn lookup_scope(
    scopes: &impl ReadableTable<(u64, u64, &'static str), u64>,
    key: CollectionKey<'_>,
    tenant_id: TenantId,
) -> crate::Result<Option<u64>> {
    Ok(scopes
        .get((key.database_id().as_u64(), tenant_id.as_u64(), key.name()))
        .map_err(|e| catalog_err("get surrogate_scopes", e))?
        .map(|v| v.value()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scope_range_holds_its_keys_and_no_neighbour() {
        let (lo, hi) = fwd_range(7);
        let inside = fwd_key(7, b"\xff\xff");
        assert!(lo.as_slice() <= inside.as_slice() && inside.as_slice() < hi.as_slice());
        assert!(fwd_key(8, b"").as_slice() >= hi.as_slice());
        let rev = rev_range(7);
        assert!(rev.contains(&rev_key(7, u32::MAX)));
        assert!(!rev.contains(&rev_key(8, 0)));
        assert!(!rev.contains(&rev_key(6, u32::MAX)));
    }
}
