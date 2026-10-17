// SPDX-License-Identifier: BUSL-1.1

//! Identity-scoped rule lookups shared by the aggregate and graph refusals.

use crate::control::security::redaction::RedactionStore;
use crate::types::TenantId;

/// The error every redaction refusal raises: `42501`, with `detail` naming
/// the redacted column or collection and the clause that reads it.
pub(super) fn redaction_refusal(tenant_id: u64, detail: String) -> crate::Error {
    crate::Error::RejectedAuthz {
        tenant_id: TenantId::new(tenant_id),
        resource: detail,
    }
}

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
    /// The database bare op-carried collection names (a parsed
    /// `MatchQuery.collection`) are qualified against before a rule lookup —
    /// unqualified, the lookup silently misses outside the default database.
    pub(super) database_id: nodedb_types::DatabaseId,
}

impl RefusalCtx<'_> {
    /// [`redaction_refusal`] for this requester's tenant.
    pub(super) fn refusal(&self, detail: String) -> crate::Error {
        redaction_refusal(self.tenant_id, detail)
    }

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

    /// The collection of `sources` whose redacted column `field` names, if
    /// any.
    ///
    /// A field qualified with a source's qualifier (`o.ssn`) is checked
    /// against the sources that carry that qualifier. Any other field,
    /// unqualified or with a qualifier no source carries (a derived table's
    /// alias, a nested field path), is checked against every source, so an
    /// ambiguous name cannot slip past. [`Self::field_is_redacted`] matches
    /// the field as written and by its column part.
    pub(super) fn redacted_source<'s>(
        &self,
        sources: &'s [(String, String)],
        field: &str,
    ) -> Option<&'s str> {
        let qualifier = field.split_once('.').map(|(qualifier, _)| qualifier);
        let names_a_source = qualifier.is_some_and(|qualifier| {
            sources
                .iter()
                .any(|(source_qualifier, _)| source_qualifier == qualifier)
        });
        sources
            .iter()
            .filter(|(source_qualifier, _)| {
                !names_a_source || qualifier == Some(source_qualifier.as_str())
            })
            .find(|(_, collection)| {
                !collection.is_empty() && self.field_is_redacted(collection, field)
            })
            .map(|(_, collection)| collection.as_str())
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
