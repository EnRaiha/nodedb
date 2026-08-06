// SPDX-License-Identifier: BUSL-1.1

//! The permission-cache and identity inputs every permission-tree arm keys on.
//!
//! Each engine module receives a [`PermCtx`] and resolves one of exactly three
//! outcomes per plan variant: filter the op down to the caller's permitted
//! subtree, refuse the plan because its result cannot carry that filter, or
//! no-op because the op is not a collection-scoped data operation.
//!
//! Two things separate this pass from the row-level-security pass that shares
//! this directory. It is a subtree filter rather than a row policy, so the
//! predicate it injects is always `resource_column IN (<accessible ids>)`
//! computed from the resource hierarchy. And it carries three levels rather
//! than one: a read is filtered down to the readable subtree, while a write or
//! a delete is checked against `write_level` / `delete_level` — which is why
//! write operations are decided here instead of being left to a separate
//! write-path check the way the RLS pass leaves them.

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::permission_tree::resolver::accessible_resources;
use crate::control::security::permission_tree::{PermissionCache, PermissionTreeDef};
use crate::types::TenantId;

use super::super::filters::merge_filters;

/// Which threshold of the tree definition an operation must clear.
#[derive(Clone, Copy)]
pub(super) enum PermTreeLevel {
    Read,
    Write,
    Delete,
}

impl PermTreeLevel {
    /// The level name this operation is measured against.
    fn required_in(self, def: &PermissionTreeDef) -> &str {
        match self {
            Self::Read => &def.read_level,
            Self::Write => &def.write_level,
            Self::Delete => &def.delete_level,
        }
    }
}

/// Permission cache plus the requester's tenant and authenticated identity.
///
/// A superuser bypasses permission-tree filtering entirely, so [`PermCtx::new`]
/// returns `None` for one and no walk runs at all — no arm has to restate the
/// bypass. The "this collection has no permission tree" early-out lives in the
/// methods below, so no arm restates that either.
pub(super) struct PermCtx<'a> {
    cache: &'a PermissionCache,
    tenant_id: u64,
    auth: &'a AuthContext,
}

impl<'a> PermCtx<'a> {
    /// Build the context for one task, or `None` when the caller is a
    /// superuser and the whole pass is bypassed.
    pub(super) fn new(
        cache: &'a PermissionCache,
        tenant_id: u64,
        auth: &'a AuthContext,
    ) -> Option<Self> {
        if auth.is_superuser() {
            return None;
        }
        Some(Self {
            cache,
            tenant_id,
            auth,
        })
    }

    /// AND the caller's permitted subtree into a filter slot of the plan.
    ///
    /// `slot` is whichever predicate the handler evaluates against the rows
    /// this operation produces or acts on: a storage-pushdown `filters` field
    /// or a dedicated post-fetch `rls_filters` field. The bytes are merged
    /// rather than replaced because this pass runs after RLS injection, which
    /// may already own the slot.
    ///
    /// An identity with no accessible resource yields `IN ()`, which matches
    /// nothing — the caller sees an empty result rather than an error, exactly
    /// as a row outside their subtree is invisible rather than reported.
    pub(super) fn filter_into(
        &self,
        collection: &str,
        level: PermTreeLevel,
        slot: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let Some(def) = self.cache.get_tree_def(self.tenant_id, collection) else {
            return Ok(());
        };
        let accessible = self.accessible(def, level);
        let in_filter = ScanFilter {
            field: def.resource_column.clone(),
            op: FilterOp::In,
            value: nodedb_types::Value::Array(
                accessible
                    .into_iter()
                    .map(nodedb_types::Value::String)
                    .collect(),
            ),
            clauses: Vec::new(),
            expr: None,
        };
        let filter_bytes =
            zerompk::to_msgpack_vec(&vec![in_filter]).map_err(|e| crate::Error::PlanError {
                detail: format!("permission tree filter serialization: {e}"),
            })?;
        merge_filters(slot, &filter_bytes)
    }

    /// Require the identity to hold `level` on at least one resource of the
    /// collection's tree.
    ///
    /// Used where the operation acts on rows it does not select through a
    /// filter slot — a keyed write, a truncate, an index-maintenance write.
    /// Per-row restriction is impossible there, so the check is the blanket
    /// one, and a caller with no grant at all is rejected outright rather than
    /// silently writing.
    pub(super) fn authorize(&self, collection: &str, level: PermTreeLevel) -> crate::Result<()> {
        let Some(def) = self.cache.get_tree_def(self.tenant_id, collection) else {
            return Ok(());
        };
        if self.accessible(def, level).is_empty() {
            let required = level.required_in(def);
            return Err(crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(self.tenant_id),
                resource: format!(
                    "permission tree on '{collection}': user has no '{required}' access"
                ),
            });
        }
        Ok(())
    }

    /// Refuse the plan while a permission tree governs `collection`.
    ///
    /// `why` completes the sentence "…is not supported with this operation:
    /// {why}", so it must state what the result carries instead of filterable
    /// rows and why the subtree filter cannot be evaluated against it.
    pub(super) fn refuse_if_tree(&self, collection: &str, why: &str) -> crate::Result<()> {
        if collection.is_empty()
            || self
                .cache
                .get_tree_def(self.tenant_id, collection)
                .is_none()
        {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "permission tree on '{collection}' is not supported with this operation: {why}"
            ),
        })
    }

    /// Refuse when any collection in the tenant carries a permission tree.
    ///
    /// Used only where the plan does not name the collection it reads, so the
    /// narrow per-collection question cannot be asked and the plan cannot be
    /// shown to avoid a governed collection. Mirrors the RLS pass's
    /// tenant-wide fallback for the same shapes.
    pub(super) fn refuse_if_any_tree(&self, why: &str) -> crate::Result<()> {
        if !self.cache.has_tree_defs_for_tenant(self.tenant_id) {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "a permission tree applies to this tenant and the plan names no collection, so \
                 this operation is not supported: {why}"
            ),
        })
    }

    /// Resource ids this identity holds at least `level` on.
    fn accessible(&self, def: &PermissionTreeDef, level: PermTreeLevel) -> Vec<String> {
        accessible_resources(
            self.cache,
            def,
            self.tenant_id,
            &self.auth.id,
            &self.auth.roles,
            level.required_in(def),
        )
    }
}
