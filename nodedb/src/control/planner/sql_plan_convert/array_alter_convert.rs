// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::AlterArray` → `PhysicalTask` lowering.
//!
//! Conversion only reads the current catalog entry and validates the requested
//! change. The front door proposes the updated entry as a replicated
//! `PutArray` (`array_catalog::ddl`).

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::types::TenantId;
use nodedb_physical::physical_plan::MetaOp;
use nodedb_types::config::retention::BitemporalRetention;

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
    ctx.require_execute("ALTER ARRAY")?;
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "ALTER ARRAY: no array catalog wired into convert context".into(),
        })?;
    // 1. Load current entry.
    let current: ArrayCatalogEntry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
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

    let vshard = ctx.collection_key(name).vshard();
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
