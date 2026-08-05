// SPDX-License-Identifier: BUSL-1.1

//! Identity-scoped rule lookups shared by the aggregate and graph refusals.

use crate::control::security::redaction::RedactionStore;

/// The inputs every refusal check keys on: the policy registry plus the
/// requester's tenant and roles.
///
/// Roles are the only identity input a redaction policy is keyed on, exactly
/// as on the masking path (`RedactionStore::apply_flat_row`) — so a plan is
/// refused for precisely the identities whose result rows would have been
/// rewritten, and no others.
pub(super) struct RefusalCtx<'a> {
    pub(super) store: &'a RedactionStore,
    pub(super) tenant_id: u64,
    pub(super) roles: &'a [String],
}

impl RefusalCtx<'_> {
    /// True when one of the requester's roles has a redaction rule covering
    /// `collection`.`field`.
    ///
    /// `field` is matched as written and, when it is qualified (`alias.col`),
    /// also by its column part: an aggregate argument over a join carries the
    /// side's qualifier, while a rule always names the stored field.
    pub(super) fn field_is_redacted(&self, collection: &str, field: &str) -> bool {
        if self
            .store
            .has_rule_for_field(self.tenant_id, collection, self.roles, field)
        {
            return true;
        }
        field.rsplit_once('.').is_some_and(|(_, column)| {
            self.store
                .has_rule_for_field(self.tenant_id, collection, self.roles, column)
        })
    }

    /// True when one of the requester's roles has any redaction rule on
    /// `collection`.
    pub(super) fn collection_is_redacted(&self, collection: &str) -> bool {
        self.store
            .has_any_rule_for_collection(self.tenant_id, collection, self.roles)
    }

    /// True when one of the requester's roles has any redaction rule at all in
    /// this tenant.
    ///
    /// Used only where the plan does not name the collection it reads, so the
    /// narrow per-collection question cannot be asked — see
    /// `graph::refuse_unscoped_match`.
    pub(super) fn identity_has_any_rule(&self) -> bool {
        self.store
            .has_any_rule_for_roles(self.tenant_id, self.roles)
    }
}
