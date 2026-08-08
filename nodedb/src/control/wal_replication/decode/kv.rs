// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Kv`.

use super::ctx::{DecodeCtx, bind_or_lookup};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::KvOp;

pub(super) fn put(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::Put {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Every plan reconstructed in this module carries an empty RLS write check.
/// The predicate is a property of the session that issued the write, not of the
/// row, and is deliberately absent from the durable record: a replay re-applies
/// a write that was already admitted, so re-deciding it against the policies of
/// whoever is connected at recovery time would make recovery non-deterministic.
pub(super) fn delete(collection: &str, keys: &[Vec<u8>]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Delete {
        collection: collection.to_owned(),
        keys: keys.to_vec(),
        rls_write_check: Vec::new(),
    })
}

pub(super) fn insert(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::Insert {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn insert_if_absent(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::InsertIfAbsent {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn insert_on_conflict_update(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried)?,
        None => carried,
    };
    Ok(PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        updates: updates.to_vec(),
        surrogate,
        rls_write_check: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn batch_put(
    ctx: &DecodeCtx,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    surrogates: &[u32],
) -> crate::Result<PhysicalPlan> {
    let resolved = entries
        .iter()
        .zip(surrogates.iter())
        .map(|((key, _value), carried)| {
            let carried = nodedb_types::Surrogate::new(*carried);
            match ctx.assigner {
                Some(a) => a.bind(ctx.database_id, ctx.tenant_id, collection, key, carried),
                None => Ok(carried),
            }
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: collection.to_owned(),
        entries: entries.to_vec(),
        ttl_ms,
        surrogates: resolved,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(super) fn expire(collection: &str, key: &[u8], ttl_ms: u64) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Expire {
        collection: collection.to_owned(),
        key: key.to_vec(),
        ttl_ms,
        rls_write_check: Vec::new(),
    })
}

pub(super) fn persist(collection: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Persist {
        collection: collection.to_owned(),
        key: key.to_vec(),
        rls_write_check: Vec::new(),
    })
}

pub(super) fn incr(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::Incr {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta,
        ttl_ms,
        surrogate,
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn incr_float(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    delta: f64,
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta,
        surrogate,
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn cas(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::Cas {
        collection: collection.to_owned(),
        key: key.to_vec(),
        expected: expected.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        rls_write_check: Vec::new(),
    }))
}

pub(super) fn get_set(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::GetSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
    }))
}

/// Fields of the `KvRegisterSortedIndex` wire variant, bundled so
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

pub(super) fn register_sorted_index(f: RegisterSortedIndexFields) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: f.collection.to_owned(),
        index_name: f.index_name.to_owned(),
        sort_columns: f.sort_columns.to_vec(),
        key_column: f.key_column.to_owned(),
        window_type: f.window_type.to_owned(),
        window_timestamp_column: f.window_timestamp_column.to_owned(),
        window_start_ms: f.window_start_ms,
        window_end_ms: f.window_end_ms,
    })
}

pub(super) fn drop_sorted_index(index_name: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_owned(),
    })
}

/// Reconstruct a `RegisterIndex` plan. No surrogate binding — a secondary
/// index carries no per-row identity; apply re-runs registration (and, if
/// `backfill`, the scan of pre-existing rows) live on the follower, and the
/// local WAL append makes it durable there.
pub(super) fn register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
        field_position,
        backfill,
    })
}

/// Reconstruct a `DropIndex` plan. Same surrogate-free contract as
/// [`register_index`].
pub(super) fn drop_index(collection: &str, field: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::DropIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
    })
}

pub(super) fn field_set(
    ctx: &DecodeCtx,
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, collection, key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        updates: updates.to_vec(),
        surrogate,
        rls_write_check: Vec::new(),
    }))
}

/// Fields of the `KvTransfer` wire variant, bundled so [`transfer`] stays
/// under the `too_many_arguments` clippy threshold.
pub(super) struct TransferFields<'a> {
    pub(super) collection: &'a str,
    pub(super) source_key: &'a [u8],
    pub(super) dest_key: &'a [u8],
    pub(super) field: &'a str,
    pub(super) amount: f64,
    pub(super) debit_surrogate: u32,
    pub(super) credit_surrogate: u32,
}

pub(super) fn transfer(ctx: &DecodeCtx, f: TransferFields) -> crate::Result<PhysicalPlan> {
    let carried_debit = nodedb_types::Surrogate::new(f.debit_surrogate);
    let debit_surrogate = bind_or_lookup(ctx, f.collection, f.source_key, carried_debit)?;
    let carried_credit = nodedb_types::Surrogate::new(f.credit_surrogate);
    let credit_surrogate = bind_or_lookup(ctx, f.collection, f.dest_key, carried_credit)?;
    Ok(PhysicalPlan::Kv(KvOp::Transfer {
        collection: f.collection.to_owned(),
        source_key: f.source_key.to_vec(),
        dest_key: f.dest_key.to_vec(),
        field: f.field.to_owned(),
        amount: f.amount,
        debit_surrogate,
        credit_surrogate,
        rls_write_check: Vec::new(),
    }))
}

/// Reconstruct a `Truncate` plan. Same idempotent-replay contract as
/// `document::truncate` — no surrogate binding, whole-collection clear.
pub(super) fn truncate(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Truncate {
        collection: collection.to_owned(),
    })
}

pub(super) fn transfer_item(
    ctx: &DecodeCtx,
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> crate::Result<PhysicalPlan> {
    let carried = nodedb_types::Surrogate::new(surrogate);
    let surrogate = bind_or_lookup(ctx, dest_collection, dest_key, carried)?;
    Ok(PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection: source_collection.to_owned(),
        dest_collection: dest_collection.to_owned(),
        item_key: item_key.to_vec(),
        dest_key: dest_key.to_vec(),
        surrogate,
        source_rls_write_check: Vec::new(),
        dest_rls_write_check: Vec::new(),
    }))
}
