// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Kv` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;

/// Resolve the wall-clock instant for a TTL-bearing replicated KV write,
/// exactly once, at Raft-proposal time -- the `Put` family's "no TTL"
/// sentinel: `None` when `ttl_ms == 0`, so the write carries no instant to
/// disagree about. Mirrors `wal_dispatch_kv::append::resolve_expiry`'s
/// `now_ms` half; the corresponding `expire_at_ms` is computed by the Data
/// Plane from `ttl_ms` at apply time via `CoreLoop::kv_ttl_now_ms`, using
/// this same instant on every replica instead of one independent wall-clock
/// read per replica per apply.
fn resolve_now_ms(ttl_ms: u64) -> Option<u64> {
    if ttl_ms == 0 {
        None
    } else {
        Some(crate::engine::kv::current_ms())
    }
}

pub(super) fn put(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvPut {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn delete(collection: &str, keys: &[Vec<u8>]) -> ReplicatedWrite {
    ReplicatedWrite::KvDelete {
        collection: collection.to_owned(),
        keys: keys.to_vec(),
    }
}

pub(super) fn insert(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsert {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn insert_if_absent(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsertIfAbsent {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn insert_on_conflict_update(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsertOnConflictUpdate {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        updates: updates.to_vec(),
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn batch_put(
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    surrogates: &[Surrogate],
) -> ReplicatedWrite {
    ReplicatedWrite::KvBatchPut {
        collection: collection.to_owned(),
        entries: entries.to_vec(),
        ttl_ms,
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn expire(collection: &str, key: &[u8], ttl_ms: u64) -> ReplicatedWrite {
    // `EXPIRE` has no "no TTL" sentinel (`ttl_ms == 0` is a legitimate
    // "expire now" request), so the instant is always resolved -- mirrors
    // `wal_dispatch_kv::append::wal_append_kv_op`'s `KvOp::Expire` arm.
    ReplicatedWrite::KvExpire {
        collection: collection.to_owned(),
        key: key.to_vec(),
        ttl_ms,
        resolved_now_ms: Some(crate::engine::kv::current_ms()),
    }
}

pub(super) fn persist(collection: &str, key: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::KvPersist {
        collection: collection.to_owned(),
        key: key.to_vec(),
    }
}

pub(super) fn incr(
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvIncr {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta,
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
    }
}

pub(super) fn incr_float(
    collection: &str,
    key: &[u8],
    delta: f64,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvIncrFloat {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta,
        surrogate,
    }
}

pub(super) fn cas(
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvCas {
        collection: collection.to_owned(),
        key: key.to_vec(),
        expected: expected.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
    }
}

pub(super) fn get_set(
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvGetSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
    }
}

/// Fields of `KvOp::RegisterSortedIndex`, bundled so
/// [`register_sorted_index`] stays under the `too_many_arguments` clippy
/// threshold.
pub(super) struct RegisterSortedIndexFields<'a> {
    pub(super) collection: &'a str,
    pub(super) index_name: &'a str,
    pub(super) sort_columns: &'a [(String, String)],
    pub(super) key_column: &'a str,
    pub(super) window_type: &'a str,
    pub(super) window_timestamp_column: &'a str,
    pub(super) window_start_ms: u64,
    pub(super) window_end_ms: u64,
}

pub(super) fn register_sorted_index(f: RegisterSortedIndexFields) -> ReplicatedWrite {
    ReplicatedWrite::KvRegisterSortedIndex {
        collection: f.collection.to_owned(),
        index_name: f.index_name.to_owned(),
        sort_columns: f.sort_columns.to_vec(),
        key_column: f.key_column.to_owned(),
        window_type: f.window_type.to_owned(),
        window_timestamp_column: f.window_timestamp_column.to_owned(),
        window_start_ms: f.window_start_ms,
        window_end_ms: f.window_end_ms,
    }
}

pub(super) fn drop_sorted_index(index_name: &str) -> ReplicatedWrite {
    ReplicatedWrite::KvDropSortedIndex {
        index_name: index_name.to_owned(),
    }
}

pub(super) fn register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> ReplicatedWrite {
    ReplicatedWrite::KvRegisterIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
        field_position,
        backfill,
    }
}

pub(super) fn drop_index(collection: &str, field: &str) -> ReplicatedWrite {
    ReplicatedWrite::KvDropIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
    }
}

pub(super) fn field_set(
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvFieldSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        updates: updates.to_vec(),
        surrogate,
    }
}

pub(super) fn transfer(
    collection: &str,
    source_key: &[u8],
    dest_key: &[u8],
    field: &str,
    amount: f64,
    debit_surrogate: u32,
    credit_surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvTransfer {
        collection: collection.to_owned(),
        source_key: source_key.to_vec(),
        dest_key: dest_key.to_vec(),
        field: field.to_owned(),
        amount,
        debit_surrogate,
        credit_surrogate,
    }
}

/// `KvOp::Truncate` replicates as a plain `KvTruncate` entry: same
/// autocommit-only, idempotent-replay contract as `document::truncate`.
pub(super) fn truncate(collection: &str) -> ReplicatedWrite {
    ReplicatedWrite::KvTruncate {
        collection: collection.to_owned(),
    }
}

pub(super) fn transfer_item(
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvTransferItem {
        source_collection: source_collection.to_owned(),
        dest_collection: dest_collection.to_owned(),
        item_key: item_key.to_vec(),
        dest_key: dest_key.to_vec(),
        surrogate,
    }
}
