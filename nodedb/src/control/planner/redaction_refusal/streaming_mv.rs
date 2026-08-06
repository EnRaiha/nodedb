// SPDX-License-Identifier: BUSL-1.1

//! Refusal of streaming materialized views that would persist a redacted value.
//!
//! A streaming MV is maintained in the Event Plane from the CDC events of its
//! source stream: the group-by key is built from the stored columns of
//! `new_value`, and each aggregate reads one of them. Both outlive the event —
//! the key IS the MV's row identity and the aggregate its accumulated state — so
//! grouping by, or aggregating, a redacted column writes the protected value
//! into the view's own storage. Reading the view back through the redacting
//! SELECT path does not undo that: the cleartext is already at rest, keyed by or
//! summarised from exactly what the policy exists to withhold.
//!
//! This is the definition-time twin of the aggregate refusal beside it, which
//! refuses `MIN(<redacted>)` for the same reason one level earlier — a value
//! that has already been read cannot be masked afterwards.
//!
//! Unlike the query-path refusals, the question here is role-agnostic. A view is
//! defined once and read by every identity thereafter, so a rule held by ANY
//! role is enough: there is no requester to key the decision on at the moment
//! the storage decision is made. `RefusalCtx` answers the role-scoped question
//! and cannot express this one; both reach the same matching primitive on
//! [`RedactionStore`], so the mask/no-mask decision still has a single
//! definition.
//!
//! ORDERING: this closes the case where the policy exists first. A policy
//! created AFTER a view has already been maintained cannot un-persist what the
//! view stored — the refusal is a gate on new definitions, not a retroactive
//! erase — so an existing view keeps aggregating the now-protected column until
//! it is dropped and recreated.

use crate::control::security::redaction::RedactionStore;
use crate::event::streaming_mv::types::AggDef;

/// Refuse a streaming MV definition whose state would hold redacted values.
///
/// `source_collection` is the collection the source change stream watches, or
/// `None` when the stream is a wildcard: rows then arrive from every collection
/// in the tenant, so the column is matched tenant-wide rather than passing
/// unchecked.
///
/// Returns `Err(crate::Error::PlanError)` — the same typed refusal the planner
/// raises for an aggregate over a redacted column.
pub fn refuse_redacted_streaming_mv(
    store: &RedactionStore,
    tenant_id: u64,
    source_collection: Option<&str>,
    group_by_columns: &[String],
    aggregates: &[AggDef],
) -> crate::Result<()> {
    // A group-by column is matched as written. The processor resolves a handful
    // of names (`event_type`, `collection`, `row_id`, …) from the event envelope
    // instead of from `new_value`, but a collection that also has a stored column
    // of that name and a rule on it is refused rather than reasoned about: the
    // safe direction for a name that is ambiguous between an envelope field and
    // a protected column is not to persist it.
    for column in group_by_columns {
        if store.has_rule_for_field_any_role(tenant_id, source_collection, column) {
            return Err(refusal(source_collection, column, "grouping by"));
        }
    }

    // `AggDef::source_field` is the same resolution the processor extracts the
    // aggregate input with, so the column refused here is the column that would
    // have been read. `COUNT` reads none and is never refused.
    for aggregate in aggregates {
        let Some(column) = aggregate.source_field() else {
            continue;
        };
        if store.has_rule_for_field_any_role(tenant_id, source_collection, column) {
            return Err(refusal(source_collection, column, "aggregating"));
        }
    }

    Ok(())
}

fn refusal(collection: Option<&str>, column: &str, action: &str) -> crate::Error {
    let scope = match collection {
        Some(name) => format!("on '{name}'"),
        None => "on a collection of this tenant (the source stream is a wildcard)".to_string(),
    };
    crate::Error::PlanError {
        detail: format!(
            "column '{column}' {scope} is redacted: {action} it in a streaming materialized view \
             is not permitted — the view persists what it derives from each event, so the \
             protected value would be stored in the clear in the view's own state"
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
    use crate::event::streaming_mv::types::AggFunction;

    use super::*;

    const TENANT: u64 = 1;

    fn store_with_rule(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: TENANT,
            collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn agg(function: AggFunction, input_expr: &str) -> AggDef {
        AggDef {
            output_name: "out".into(),
            function,
            input_expr: input_expr.into(),
        }
    }

    fn check(
        store: &RedactionStore,
        source: Option<&str>,
        group_by: &[&str],
        aggregates: &[AggDef],
    ) -> crate::Result<()> {
        let group_by: Vec<String> = group_by.iter().map(|c| (*c).to_string()).collect();
        refuse_redacted_streaming_mv(store, TENANT, source, &group_by, aggregates)
    }

    fn assert_refused(result: crate::Result<()>) {
        match result {
            Err(crate::Error::PlanError { .. }) => {}
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// The leak this closes: the group key IS the MV row's identity, so
    /// grouping by a redacted column stores that column's cleartext values as
    /// the keys of the view.
    #[test]
    fn group_by_a_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "email");
        assert_refused(check(
            &store,
            Some("users"),
            &["email"],
            &[agg(AggFunction::Count, "")],
        ));
    }

    /// An aggregate accumulates the stored value into the view's state, which
    /// the SELECT-path mask never sees.
    #[test]
    fn aggregate_over_a_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "salary");
        assert_refused(check(
            &store,
            Some("users"),
            &["status"],
            &[agg(AggFunction::Sum, "salary")],
        ));
    }

    /// The same column reached through the `doc_get` form is the same column.
    #[test]
    fn aggregate_over_a_doc_get_wrapped_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "salary");
        assert_refused(check(
            &store,
            Some("users"),
            &[],
            &[agg(AggFunction::Sum, "doc_get(new_value, '$.salary')")],
        ));
    }

    /// The refusal is per column: a rule on another column of the same
    /// collection must not block a view that never reads it.
    #[test]
    fn an_unredacted_column_on_a_redacted_collection_is_allowed() {
        let store = store_with_rule("users", "support", "email");
        assert!(
            check(
                &store,
                Some("users"),
                &["status"],
                &[agg(AggFunction::Sum, "total")],
            )
            .is_ok()
        );
    }

    /// …and per collection: a rule on another collection's column of the same
    /// name does not reach this source.
    #[test]
    fn a_rule_on_another_collection_does_not_refuse() {
        let store = store_with_rule("users", "support", "email");
        assert!(
            check(
                &store,
                Some("orders"),
                &["email"],
                &[agg(AggFunction::Count, "")],
            )
            .is_ok()
        );
    }

    /// `COUNT` reads no column, so it is never refused — a per-group event
    /// count discloses nothing about a protected value.
    #[test]
    fn count_is_never_refused() {
        let store = store_with_rule("users", "support", "email");
        assert!(
            check(
                &store,
                Some("users"),
                &["status"],
                &[agg(AggFunction::Count, "email")],
            )
            .is_ok()
        );
    }

    /// A wildcard stream carries rows from every collection, so the column
    /// cannot be cleared against one — it is matched tenant-wide instead of
    /// passing unchecked.
    #[test]
    fn a_wildcard_source_matches_the_column_across_the_tenant() {
        let store = store_with_rule("users", "support", "email");
        assert_refused(check(
            &store,
            None,
            &["email"],
            &[agg(AggFunction::Count, "")],
        ));
        assert!(
            check(&store, None, &["status"], &[agg(AggFunction::Count, "")]).is_ok(),
            "a column no policy in the tenant names is still allowed"
        );
    }

    /// A policy belonging to another tenant must not refuse this one's view.
    #[test]
    fn another_tenants_policy_does_not_refuse() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "other".into(),
            tenant_id: TENANT + 1,
            collection: "users".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        assert!(
            check(
                &store,
                Some("users"),
                &["email"],
                &[agg(AggFunction::Count, "")],
            )
            .is_ok()
        );
    }

    /// The refusal does not depend on which role holds the rule: the view is
    /// read by every identity once it exists, so any policy at all is enough.
    #[test]
    fn any_roles_rule_refuses_regardless_of_who_creates_the_view() {
        let store = store_with_rule("users", "an_unrelated_role", "email");
        assert_refused(check(
            &store,
            Some("users"),
            &["email"],
            &[agg(AggFunction::Count, "")],
        ));
    }
}
