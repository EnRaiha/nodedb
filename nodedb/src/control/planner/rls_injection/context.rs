// SPDX-License-Identifier: BUSL-1.1

//! The policy-store and identity inputs every injection arm keys on.
//!
//! Each engine module receives an [`RlsCtx`] and resolves one of exactly three
//! outcomes per plan variant: inject the policy into a filter slot, refuse the
//! plan because its result cannot carry a row filter, or no-op because the op
//! reads no user rows.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::{PolicyType, RlsPolicyStore};

use super::filters::{get_rls, merge_filters};

/// Policy registry plus the requester's tenant and authenticated identity.
///
/// Superuser bypass and fail-closed handling of unresolved `$auth.*`
/// references both live inside `combined_read_predicate_with_auth`, which
/// every method here reaches through [`get_rls`] — so no arm has to restate
/// either rule.
pub(super) struct RlsCtx<'a> {
    pub(super) store: &'a RlsPolicyStore,
    pub(super) tenant_id: u64,
    pub(super) auth: &'a AuthContext,
}

impl RlsCtx<'_> {
    /// The concrete read filters for `collection`; empty when no policy
    /// restricts this identity.
    pub(super) fn read_filters(&self, collection: &str) -> crate::Result<Vec<u8>> {
        get_rls(self.store, self.tenant_id, collection, self.auth)
    }

    /// AND the collection's read policy into a scan-style pushdown slot.
    pub(super) fn merge_into(&self, collection: &str, filters: &mut Vec<u8>) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            merge_filters(filters, &rls)?;
        }
        Ok(())
    }

    /// Store the collection's read policy in a dedicated post-fetch slot.
    pub(super) fn set_post_filters(
        &self,
        collection: &str,
        rls_filters: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            *rls_filters = rls;
        }
        Ok(())
    }

    /// Refuse the plan while a read policy restricts this identity on
    /// `collection`.
    ///
    /// `why` completes the sentence "…is not supported with this operation:
    /// {why}", so it must state what the result carries instead of rows and
    /// why the filter cannot be evaluated against it.
    pub(super) fn refuse_if_policy(&self, collection: &str, why: &str) -> crate::Result<()> {
        if collection.is_empty() || self.read_filters(collection)?.is_empty() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS policy on '{collection}' is not supported with this operation: {why}"
            ),
        })
    }

    /// Refuse when this identity holds any read policy anywhere in the tenant.
    ///
    /// Used only where the plan does not name the collection it reads, so the
    /// narrow per-collection question cannot be asked and the plan cannot be
    /// shown to avoid a protected collection. Mirrors the redaction pass's
    /// tenant-wide fallback for an unscoped MATCH.
    pub(super) fn refuse_if_any_policy(&self, why: &str) -> crate::Result<()> {
        if self.auth.is_superuser() || !self.identity_has_any_read_policy() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS is not supported with this operation while a read policy applies to this \
                 identity and the plan names no collection: {why}"
            ),
        })
    }

    /// Whether any enabled, non-vacuous read policy exists in this tenant.
    ///
    /// A policy with no compiled predicate filters nothing, so it is ignored
    /// here exactly as `combined_read_predicate_with_auth` ignores it.
    fn identity_has_any_read_policy(&self) -> bool {
        self.store
            .all_policies_for_tenant(self.tenant_id)
            .iter()
            .any(|policy| {
                policy.enabled
                    && policy.compiled_predicate.is_some()
                    && matches!(policy.policy_type, PolicyType::Read | PolicyType::All)
            })
    }
}
