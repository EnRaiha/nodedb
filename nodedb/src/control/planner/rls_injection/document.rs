// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for document-engine operations.

use nodedb_physical::physical_plan::DocumentOp;

use super::context::RlsCtx;

/// Exhaustive over [`DocumentOp`] so a new document operation forces a
/// decision between injecting, refusing, and no-op.
pub(super) fn inject_document(ctx: &RlsCtx<'_>, op: &mut DocumentOp) -> crate::Result<()> {
    match op {
        // Inject: the scan pushes its predicate into storage, so the policy
        // ANDs into the same slot the user's WHERE clause occupies.
        DocumentOp::Scan {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: the indexed equality resolves doc ids, then every fetched
        // body is tested against `filters` — the residual post-filter slot the
        // policy ANDs into.
        DocumentOp::IndexedFetch {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: no storage pushdown slot, so the handler evaluates the
        // policy on the rows it fetched. An excluded row reads back as absent
        // — indistinguishable from a missing key, so a caller cannot probe for
        // rows it may not read.
        DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        }
        | DocumentOp::RangeScan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Inject both policies: the read filter for what may be shown back, and
        // the compiled write predicate for what may be persisted.
        //
        // These writes surface rows through a `RETURNING` clause, and that
        // output is a read — a row the policy hides must not become visible
        // just because the statement wrote it. The handler evaluates the filter
        // against each full pre-projection document, so a predicate on a column
        // the `RETURNING` list omits still decides the row.
        //
        // The write is gated in the Data Plane rather than here, because the
        // image a write policy decides is not in the plan: for an update it
        // exists only after the handler has read the stored row and applied the
        // assignments, for a delete only after it has read the row being
        // removed. Shipping the compiled predicate lets the handler test the
        // actual row bytes just before it persists them, and reject the whole
        // statement when one fails — the two slots stay separate because one
        // decides visibility and the other decides the write.
        DocumentOp::PointDelete {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::BulkUpdate {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::BulkDelete {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        // The joined source is read, but every row these two return — and every
        // row they write — belongs to the TARGET, so the target's policy is the
        // one that gates both halves.
        | DocumentOp::UpdateFromJoin {
            target_collection: collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::Merge {
            target_collection: collection,
            rls_filters,
            rls_write_check,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // Refuse: returns index entries rather than rows, so there is no row
        // body to evaluate a policy against.
        DocumentOp::IndexLookup { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the lookup returns index entries, not row bodies, so the row filter has nothing to \
             evaluate against",
        ),

        // Refuse: an HLL cardinality estimate counts rows the policy hides,
        // and a scalar count carries no row for the filter to test. Redaction
        // ignores this shape (a count exposes no column value); RLS cannot,
        // because the row set itself is what the policy restricts.
        DocumentOp::EstimateCount { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the estimate is a row count, which the row filter cannot be evaluated against",
        ),

        // Refuse: the clone materializer streams raw `(id, surrogate, value)`
        // triples with no filter slot, so every stored body would be copied
        // regardless of the policy.
        DocumentOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored bodies through a cursor payload that \
             carries no row filter",
        ),

        // Admit the write image, then inject the read filter: the plan carries
        // the whole post-image, so the write policy is evaluated against the
        // exact row that will exist afterwards. The planner emits these bodies
        // as MessagePack for every storage mode — the Data Plane re-encodes a
        // strict collection's tuple on the way to disk — so the predicate reads
        // the same field names a `SELECT` would.
        //
        // The read filter is not redundant with that admission. It gates a
        // different thing: a `RETURNING` clause on these writes ships rows back,
        // and that output is a read, so a row a read-only policy hides must not
        // become visible just because the statement wrote it. The two policies
        // are independent — a collection can carry a `FOR SELECT` policy and no
        // write policy at all, in which case the write is unrestricted and the
        // returned row set still shrinks.
        DocumentOp::PointPut {
            collection,
            value,
            rls_filters,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            value,
            rls_filters,
            ..
        } => {
            ctx.admit_write_image(collection, value)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        DocumentOp::BatchInsert {
            collection,
            documents,
            rls_filters,
            ..
        } => {
            for (_, value) in documents.iter() {
                ctx.admit_write_image(collection, value)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the write predicate: the insert body is in the plan, but the
        // conflict branch persists a merge of that body with the stored row (or
        // the assignments in `on_conflict_updates` applied to it), and neither
        // of those images exists until the handler has read the stored row.
        // Admitting on the insert body alone would clear a write whose actual
        // post-image the policy never saw, so the handler tests whichever body
        // it is about to store. The read filter rides along for the same reason
        // it does on the plain inserts above: `RETURNING` output is a read.
        DocumentOp::Upsert {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Refuse: the rows come from a scan resolved after this pass, so the
        // plan carries no image to evaluate.
        DocumentOp::InsertSelect {
            target_collection, ..
        } => ctx.refuse_if_write_policy(
            target_collection,
            "the inserted rows are produced by a scan resolved after planning, so the plan carries \
             no row image",
        ),

        // Refuse: a truncate removes every row without reading one, so there is
        // no image the policy could be evaluated against — and a policy that
        // restricts which rows this identity may write is precisely a statement
        // that it may not remove all of them.
        DocumentOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),

        // No-op: index and collection DDL. These write no user row, so neither
        // the read nor the write policy has anything to restrict in them; the
        // DDL itself is authorized by the permission check that precedes this
        // pass.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => Ok(()),

        // No-op: a derived balance write carries no user intent and no user
        // identity. Its admission was decided when the SOURCE row it was
        // derived from was admitted; deciding it again against the TARGET's own
        // policy would refuse every governed write on the strength of a row
        // whose only changed column is one the engine maintains — the same
        // reason the co-resident derived write runs with `enforce: false`.
        DocumentOp::ApplyBalanceDelta { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy, store_with_predicate,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::PolicyType;

    /// `alice` in the shared fixture has user id 42, so this row satisfies
    /// `owner_id = $auth.id` and that one does not.
    fn body(owner_id: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack_or_empty(&serde_json::json!({
            "owner_id": owner_id,
            "amount": 100,
        }))
    }

    fn point_insert(collection: &str, owner_id: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: collection.into(),
            document_id: "d1".into(),
            value: body(owner_id),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn point_update(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: collection.into(),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &[u8] {
        match plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Merge {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The insert body is the row that will exist afterwards, so a conforming
    /// row is admitted and a violating one fails the statement.
    #[test]
    fn insert_is_admitted_or_rejected_on_its_own_post_image() {
        let store = store_with_write_policy("orders");

        let mut conforming = point_insert("orders", "42");
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = point_insert("orders", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A batch fails whole when any one of its rows violates the policy: a
    /// silently dropped row would report a write that never happened.
    #[test]
    fn batch_insert_is_rejected_when_any_row_violates_the_policy() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: "orders".into(),
            documents: vec![("d1".into(), body("42")), ("d2".into(), body("99"))],
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// An unresolvable `$auth.*` reference in a write predicate denies the
    /// write rather than admitting it.
    #[test]
    fn insert_fails_closed_on_an_unresolvable_auth_reference() {
        let store = store_with_predicate(
            "orders",
            PolicyType::Write,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("nonexistent_field".into()),
            },
        );
        let mut plan = point_insert("orders", "42");
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The post-image of an update exists only after the stored row is read, so
    /// the compiled predicate rides along for the handler to test it against.
    #[test]
    fn point_update_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = point_update("orders");
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !write_check(&plan).is_empty(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// A delete is gated the same way: the row it removes is only known after
    /// the handler reads it, so the predicate travels with the plan.
    #[test]
    fn bulk_delete_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: "orders".into(),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// Every MERGE arm writes a row resolved against the target, so the
    /// target's write policy is the one that reaches the gate.
    #[test]
    fn merge_carries_the_target_write_predicate() {
        let store = store_with_write_policy("target");
        let mut plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: "target".into(),
            source_collection: "source".into(),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            clauses: Vec::new(),
            returning: None,
            resolve_only: false,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// An upsert's conflict branch persists a merge with the stored row, so its
    /// gate is the same shipped predicate rather than a plan-time admission.
    #[test]
    fn upsert_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::Upsert {
            collection: "orders".into(),
            document_id: "d1".into(),
            value: body("42"),
            on_conflict_updates: Vec::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// A `RETURNING` on an insert ships rows back, so a read-only policy must
    /// land in the insert's post-filter slot. Leaving it empty would return
    /// rows the same principal's `SELECT` hides.
    #[test]
    fn insert_receives_the_read_policy_filter() {
        let store = store_with_read_policy("orders");
        let mut plan = point_insert("orders", "42");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::PointInsert { rls_filters, .. }) => assert!(
                !rls_filters.is_empty(),
                "the read policy must gate RETURNING output"
            ),
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// An unresolvable `$auth.*` in a write predicate denies the statement
    /// instead of compiling to an empty (allow-everything) gate.
    #[test]
    fn a_gated_write_fails_closed_on_an_unresolvable_auth_reference() {
        let store = store_with_predicate(
            "orders",
            PolicyType::Write,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("nonexistent_field".into()),
            },
        );
        let mut plan = point_update("orders");
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A truncate removes every row without reading one, so there is no image
    /// the policy could decide.
    #[test]
    fn truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: "orders".into(),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
        });
        assert_write_refused(inject(&mut plan, &store), "orders");
    }

    /// A `FOR ALL` policy decides both halves: the read filter lands in the
    /// post-fetch slot and the write predicate lands in the gate slot, as two
    /// separate fields.
    #[test]
    fn a_for_all_policy_gates_the_read_slot_and_the_write() {
        let store = store_with_predicate(
            "orders",
            PolicyType::All,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
        );
        let mut plan = point_update("orders");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_filters,
                rls_write_check,
                ..
            }) => {
                assert!(!rls_filters.is_empty(), "read half must gate RETURNING");
                assert!(
                    !rls_write_check.is_empty(),
                    "write half must gate the write"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }

        let mut conforming = point_insert("orders", "42");
        assert!(inject(&mut conforming, &store).is_ok());
        let mut violating = point_insert("orders", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A read policy alone leaves the write gate empty — the write half is
    /// keyed on write policies only, so a `FOR SELECT` policy must not silently
    /// start rejecting writes.
    #[test]
    fn a_read_policy_alone_does_not_gate_writes() {
        let store = store_with_read_policy("orders");
        let mut insert = point_insert("orders", "99");
        assert!(inject(&mut insert, &store).is_ok());

        let mut update = point_update("orders");
        assert!(inject(&mut update, &store).is_ok());
        assert!(write_check(&update).is_empty());
    }

    /// With no policy at all every write shape runs untouched.
    #[test]
    fn writes_without_a_policy_are_untouched() {
        for mut plan in [point_insert("orders", "99"), point_update("orders")] {
            let before = plan.clone();
            assert!(inject_without_policy(&mut plan).is_ok());
            assert_eq!(plan, before);
        }
    }

    fn indexed_fetch(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: collection.into(),
            path: "$.email".into(),
            value: "a@b.c".into(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
        })
    }

    /// The indexed fetch applies `filters` to every fetched body, so the
    /// policy lands there rather than refusing the plan.
    #[test]
    fn indexed_fetch_receives_the_policy_filter() {
        let store = store_with_read_policy("users");
        let mut plan = indexed_fetch("users");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::IndexedFetch { filters, .. }) => {
                assert!(!filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// With no policy the same fetch is untouched.
    #[test]
    fn indexed_fetch_without_a_policy_is_untouched() {
        let mut plan = indexed_fetch("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A `RETURNING` write ships rows back, so the policy lands in its
    /// post-filter slot — leaving the statement's own write predicate alone.
    #[test]
    fn bulk_update_receives_the_policy_filter() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "users".into(),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                filters,
                rls_filters,
                ..
            }) => {
                assert!(
                    !rls_filters.is_empty(),
                    "policy must land in the post-filter slot"
                );
                assert!(
                    filters.is_empty(),
                    "the statement's own write predicate must stay untouched"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// Every row a MERGE returns belongs to the target, so the target's policy
    /// is the one injected — a policy on the source gates nothing here.
    #[test]
    fn merge_receives_the_target_collection_policy() {
        let store = store_with_read_policy("target");
        let mut plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: "target".into(),
            source_collection: "source".into(),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            clauses: Vec::new(),
            returning: None,
            resolve_only: false,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::Merge { rls_filters, .. }) => {
                assert!(!rls_filters.is_empty(), "target policy must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A cardinality estimate counts rows the policy hides.
    #[test]
    fn estimate_count_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: "users".into(),
            field: "id".into(),
        });
        assert_refused(inject(&mut plan, &store), "users");
    }
}
