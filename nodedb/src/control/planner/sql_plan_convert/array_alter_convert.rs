// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::AlterArray` → `PhysicalTask` lowering.
//!
//! All state mutations happen on the Control Plane:
//! 1. Load current catalog entry (error if not found).
//! 2. Compute updated fields (apply `minimum_audit_retain_ms` first so the
//!    floor is current before the re-register step).
//! 3. Floor validation rejects new `audit_retain_ms` values below the floor.
//! 4. Update in-memory `ArrayCatalog` (unregister + register).
//! 5. Persist to `_system.arrays` via redb.
//! 6. Update `BitemporalRetentionRegistry`.
//! 7. Emit `MetaOp::AlterArray` — a lightweight ack op so the task travels
//!    through standard dispatch with `Permission::Admin` classification.

use nodedb_types::config::retention::BitemporalRetention;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::engine::bitemporal::registry::BitemporalEngineKind;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

use super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Convert `SqlPlan::AlterArray` to a `PhysicalTask`.
///
/// Double-`Option` semantics for diff fields:
/// - `None`          = key absent from SET clause → field unchanged.
/// - `Some(None)`    = key present with value `NULL` → unregister.
/// - `Some(Some(v))` = key present with value `v` → update.
pub(super) fn convert_alter_array(
    name: &str,
    audit_retain_ms: Option<Option<i64>>,
    minimum_audit_retain_ms: Option<u64>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "ALTER ARRAY: no array catalog wired into convert context".into(),
        })?;
    let credentials = ctx
        .credentials
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "ALTER ARRAY: no credential store wired into convert context".into(),
        })?;

    // 1. Load current entry.
    let current: ArrayCatalogEntry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name(name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("ALTER ARRAY {name}: not found"),
            })?
    };

    // 2. Compute updated fields.
    let new_min =
        minimum_audit_retain_ms.unwrap_or_else(|| current.minimum_audit_retain_ms.unwrap_or(0));
    let new_retain = match audit_retain_ms {
        None => current.audit_retain_ms,
        Some(inner) => inner,
    };

    // 3. Floor validation before any state mutation.
    if let Some(retain_ms) = new_retain {
        let retention = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: retain_ms as u64,
            minimum_audit_retain_ms: new_min,
        };
        retention.validate().map_err(|e| crate::Error::PlanError {
            detail: format!("ALTER ARRAY {name}: {e}"),
        })?;
    }

    let updated = ArrayCatalogEntry {
        audit_retain_ms: new_retain,
        minimum_audit_retain_ms: if minimum_audit_retain_ms.is_some() {
            Some(new_min)
        } else {
            current.minimum_audit_retain_ms
        },
        ..current.clone()
    };

    // 4. Update in-memory catalog.
    {
        let mut cat = array_catalog.write().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.unregister(name);
        cat.register(updated.clone())
            .map_err(|e| crate::Error::PlanError {
                detail: format!("ALTER ARRAY {name}: catalog re-register: {e}"),
            })?;
    }

    // 5. Persist to system catalog.
    {
        let catalog = credentials.catalog();
        crate::control::array_catalog::persist::persist(catalog, &updated).map_err(|e| {
            crate::Error::PlanError {
                detail: format!("ALTER ARRAY {name}: catalog persist: {e}"),
            }
        })?;
    }

    // 6. Update bitemporal retention registry.
    if let Some(registry) = &ctx.bitemporal_retention_registry {
        let array_tid = TenantId::new(0);
        match audit_retain_ms {
            Some(None) => {
                registry.unregister(ctx.database_id, array_tid, name);
            }
            Some(Some(retain_ms)) => {
                let retention = BitemporalRetention {
                    data_retain_ms: 0,
                    audit_retain_ms: retain_ms as u64,
                    minimum_audit_retain_ms: new_min,
                };
                registry
                    .register(
                        ctx.database_id,
                        array_tid,
                        name,
                        BitemporalEngineKind::Array,
                        retention,
                    )
                    .map_err(|e| crate::Error::PlanError {
                        detail: format!("ALTER ARRAY {name}: registry register: {e}"),
                    })?;
            }
            None => {
                // audit_retain_ms unchanged; re-register with updated floor if set.
                if let Some(retain_ms) = updated.audit_retain_ms {
                    let retention = BitemporalRetention {
                        data_retain_ms: 0,
                        audit_retain_ms: retain_ms as u64,
                        minimum_audit_retain_ms: new_min,
                    };
                    registry
                        .register(
                            ctx.database_id,
                            array_tid,
                            name,
                            BitemporalEngineKind::Array,
                            retention,
                        )
                        .map_err(|e| crate::Error::PlanError {
                            detail: format!("ALTER ARRAY {name}: registry re-register: {e}"),
                        })?;
                }
            }
        }
    }

    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id: name.to_string(),
            audit_retain_ms,
            minimum_audit_retain_ms: minimum_audit_retain_ms.map(Some),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
