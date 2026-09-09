// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{KvInsertIntent, SqlExpr, SqlValue, VectorPrimaryRow};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    assignments_to_update_values, sql_value_to_bytes, sql_value_to_nodedb_value,
    write_msgpack_map_header, write_msgpack_str, write_msgpack_value,
};
use super::insert::assign_for_pk;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_kv_insert(
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    ttl_secs: u64,
    intent: KvInsertIntent,
    on_conflict_updates: &[(String, SqlExpr)],
    key_column: &str,
    sequence_defaults: &[(String, String)],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let update_values = if on_conflict_updates.is_empty() {
        Vec::new()
    } else {
        assignments_to_update_values(on_conflict_updates)?
    };
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let ttl_ms = ttl_secs * 1000;
    let mut tasks = Vec::with_capacity(entries.len());
    for (key_val, value_cols) in entries {
        // Sequence-backed defaults (key or value column) advance the CP-side
        // registry here — the planner cannot run them. The planner
        // substitutes `SqlValue::Null` for a column the statement omitted,
        // so a NULL key either names a declared sequence default (filled
        // below) or violates the PRIMARY KEY's NOT NULL (#310) and is
        // rejected.
        let mut key_val = key_val.clone();
        let mut value_cols = value_cols.clone();
        if matches!(key_val, SqlValue::Null)
            && let Some((_, expr)) = sequence_defaults
                .iter()
                .find(|(name, _)| name == key_column)
        {
            key_val = sequence_default_value(ctx, expr)?;
            // Named primary-key columns are mirrored into the value map
            // so scans can project/filter them (see the builder's
            // exclusion rule); a defaulted key must mirror too, or the
            // row reads back with a blank key column.
            if key_column != "key" && !value_cols.iter().any(|(c, _)| c == key_column) {
                value_cols.push((key_column.to_string(), key_val.clone()));
            }
        }
        for (name, expr) in sequence_defaults {
            if name != key_column && !value_cols.iter().any(|(c, _)| c == name) {
                let val = sequence_default_value(ctx, expr)?;
                value_cols.push((name.clone(), val));
            }
        }
        if matches!(key_val, SqlValue::Null) {
            return Err(crate::Error::RejectedConstraint {
                collection: collection.to_string(),
                constraint: "not_null".to_string(),
                detail: "primary key cannot be NULL or omitted".to_string(),
            });
        }
        let key = sql_value_to_bytes(&key_val);
        let value = if value_cols.len() == 1 && value_cols[0].0 == "value" {
            sql_value_to_bytes(&value_cols[0].1)
        } else {
            let mut buf = Vec::with_capacity(value_cols.len() * 32);
            write_msgpack_map_header(&mut buf, value_cols.len());
            for (col, val) in &value_cols {
                write_msgpack_str(&mut buf, col);
                write_msgpack_value(&mut buf, val);
            }
            buf
        };
        let surrogate = assign_for_pk(ctx, collection, &key)?;
        let op = match intent {
            KvInsertIntent::Insert => KvOp::Insert {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::InsertIfAbsent => KvOp::InsertIfAbsent {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put if !update_values.is_empty() => KvOp::InsertOnConflictUpdate {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                updates: update_values.clone(),
                surrogate,
                // Filled by the RLS injection pass, which runs after plan
                // conversion.
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put => KvOp::Put {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
        };
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Kv(op),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }
    Ok(tasks)
}

pub(in super::super) struct VectorPrimaryInsertCfg<'a> {
    pub field: &'a str,
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

pub(in super::super) fn convert_vector_primary_insert(
    collection: &str,
    cfg: &VectorPrimaryInsertCfg<'_>,
    rows: &[VectorPrimaryRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        // Enforce per-tenant vector dimension quota before building any task.
        // 0 means unlimited.
        if ctx.max_vector_dim > 0 {
            let dim = row.vector.len() as u32;
            if dim > ctx.max_vector_dim {
                return Err(crate::Error::TenantVectorDimExceeded {
                    dim,
                    limit: ctx.max_vector_dim,
                });
            }
        }
        let pk_bytes: Vec<u8> = row
            .vector
            .iter()
            .take(4)
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let surrogate = assign_for_pk(ctx, collection, &pk_bytes)?;

        let payload = if row.payload_fields.is_empty() {
            Vec::new()
        } else {
            let value_map: std::collections::HashMap<String, nodedb_types::Value> = row
                .payload_fields
                .iter()
                .map(|(k, v)| (k.clone(), sql_value_to_nodedb_value(v)))
                .collect();
            zerompk::to_msgpack_vec(&value_map).unwrap_or_default()
        };

        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: qualified_collection.clone(),
                field: cfg.field.to_string(),
                surrogate,
                vector: row.vector.clone(),
                payload,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes.to_vec(),
                // Both filled in after conversion: the RETURNING spec by the
                // protocol layer's injection pass, the read filter by the RLS
                // injection pass.
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }
    Ok(tasks)
}

fn sequence_default_value(ctx: &ConvertContext, expr: &str) -> crate::Result<SqlValue> {
    let Some(registry) = &ctx.sequence_registry else {
        return Err(crate::Error::PlanError {
            detail: format!("sequence default '{expr}' requires sequence registry access"),
        });
    };
    let name = super::super::value::sequence_name(expr).ok_or_else(|| crate::Error::PlanError {
        detail: format!("unrecognized sequence default expression: '{expr}'"),
    })?;
    let value = match registry.nextval(ctx.database_id.as_u64(), ctx.tenant_id.as_u64(), &name) {
        Ok(v) => v,
        Err(crate::control::sequence::SequenceError::NotFound { .. }) => {
            return Err(crate::Error::UndefinedSequence { name });
        }
        Err(e) => {
            return Err(crate::Error::PlanError {
                detail: format!("nextval('{name}'): {e}"),
            });
        }
    };
    Ok(SqlValue::Int(value))
}

#[cfg(test)]
mod tests {
    use super::super::super::convert::ConvertContext;
    use nodedb_sql::types::VectorPrimaryRow;
    use nodedb_types::VectorQuantization;

    fn make_ctx(max_vector_dim: u32) -> ConvertContext {
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            sequence_registry: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn row(dim: usize) -> VectorPrimaryRow {
        VectorPrimaryRow {
            surrogate: nodedb_types::Surrogate::ZERO,
            vector: vec![0.0f32; dim],
            payload_fields: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn tenant_vector_dim_under_bound_succeeds() {
        let ctx = make_ctx(128);
        let rows = vec![row(64), row(128)];
        let result = super::convert_vector_primary_insert(
            "vecs",
            &super::VectorPrimaryInsertCfg {
                field: "emb",
                quantization: VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
            },
            &rows,
            crate::types::TenantId::new(1),
            &ctx,
        );
        assert!(result.is_ok(), "dimensions under/at cap must succeed");
    }

    #[test]
    fn tenant_vector_dim_exceeded_rejected() {
        let ctx = make_ctx(64);
        let rows = vec![row(65)];
        let result = super::convert_vector_primary_insert(
            "vecs",
            &super::VectorPrimaryInsertCfg {
                field: "emb",
                quantization: VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
            },
            &rows,
            crate::types::TenantId::new(1),
            &ctx,
        );
        match result {
            Err(crate::Error::TenantVectorDimExceeded { dim, limit }) => {
                assert_eq!(dim, 65);
                assert_eq!(limit, 64);
            }
            other => panic!("expected TenantVectorDimExceeded, got {other:?}"),
        }
    }

    #[test]
    fn tenant_vector_dim_zero_means_unlimited() {
        let ctx = make_ctx(0); // 0 = unlimited
        let rows = vec![row(99999)];
        let result = super::convert_vector_primary_insert(
            "vecs",
            &super::VectorPrimaryInsertCfg {
                field: "emb",
                quantization: VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
            },
            &rows,
            crate::types::TenantId::new(1),
            &ctx,
        );
        assert!(result.is_ok(), "limit=0 means unlimited, must succeed");
    }
}
