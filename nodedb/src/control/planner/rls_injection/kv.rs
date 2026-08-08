// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for key-value engine operations.

use nodedb_physical::physical_plan::KvOp;

use super::context::RlsCtx;

/// Exhaustive over [`KvOp`] so a new key-value operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_kv(ctx: &RlsCtx<'_>, op: &mut KvOp) -> crate::Result<()> {
    match op {
        // Inject: the predicate scan pushes filters down, so the policy ANDs
        // into the same slot as the user's predicate.
        KvOp::Scan {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: no pushdown slot, so the handler evaluates the policy on the
        // fetched value. An excluded row reads back as absent, which a caller
        // cannot distinguish from a missing key.
        KvOp::Get {
            collection,
            rls_filters,
            ..
        }
        | KvOp::BatchGet {
            collection,
            rls_filters,
            ..
        }
        | KvOp::FieldGet {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: returns only the key's remaining lifetime. There is no row
        // body to filter, and answering at all confirms that a row the policy
        // hides exists.
        KvOp::GetTtl { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the reply is a TTL rather than a row body, so the row filter cannot be evaluated \
             and the answer alone discloses that the key exists",
        ),

        // Refuse: the clone materializer streams raw `(key, value)` pairs
        // through a cursor payload with no filter slot.
        KvOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored values through a cursor payload that \
             carries no row filter",
        ),

        // Refuse: a sorted-index read returns ranks, counts, and ranked keys
        // drawn from the collection the index was built over, through a
        // payload with no filter slot. The plan names only the index, so the
        // narrow per-collection question cannot be asked here — this pass
        // holds the policy store and the identity, not the catalog that binds
        // an index name to its collection. The handler resolves that binding
        // from the index registry and refuses on the owning collection; this
        // pass asks the tenant-wide question instead, the same fallback every
        // collection-less shape uses, so a plan reaching the Data Plane
        // through any other route still fails closed.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => ctx.refuse_if_any_policy(
            "a sorted-index read returns ranked keys, a rank, or a count taken from stored rows, \
             and the plan names only the index",
        ),

        // Admit the write image, then inject the read filter. The plan carries
        // the whole post-image, so the write policy is evaluated against the
        // exact row that will exist afterwards. A multi-column SQL write
        // encodes its columns as a MessagePack map, so the predicate reads the
        // same field names a `SELECT` would; a single-column `value` write
        // stores one opaque scalar, which carries no field for the predicate to
        // name and is therefore rejected by the same evaluation rather than by
        // a carve-out.
        //
        // The read filter is not redundant with that admission. It gates a
        // different thing: a `RETURNING` clause on these writes ships rows
        // back, and that output is a read, so a row a read-only policy hides
        // must not become visible just because the statement wrote it. The two
        // policies are independent — a collection can carry a `FOR SELECT`
        // policy and no write policy at all, in which case the write is
        // unrestricted and the returned row set still shrinks.
        KvOp::Put {
            collection,
            value,
            rls_filters,
            ..
        }
        | KvOp::Insert {
            collection,
            value,
            rls_filters,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            value,
            rls_filters,
            ..
        } => {
            ctx.admit_write_image(collection, value)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Admit every entry: one violating row fails the whole statement, since
        // a silently dropped row would report a write that never happened. The
        // read filter rides along for the same reason it does on the point
        // writes above — `RETURNING` output is a read.
        KvOp::BatchPut {
            collection,
            entries,
            rls_filters,
            ..
        } => {
            for (_, value) in entries.iter() {
                ctx.admit_write_image(collection, value)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the write predicate: the image these persist is produced where
        // it is persisted, not here. The conflict branch stores a merge of the
        // incoming row with the stored one; a delete's image is the row it
        // removes; a TTL mutation leaves the body untouched, so the stored row
        // is the image; a field merge exists only after the stored row is read.
        // The Data Plane evaluates the predicate against those exact bytes just
        // before persisting, and rejects the whole statement when one fails.
        KvOp::InsertOnConflictUpdate {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            // `RETURNING` output is a read — see the point-write arms above.
            ctx.set_post_filters(collection, rls_filters)
        }

        KvOp::Delete {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Expire {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Persist {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::FieldSet {
            collection,
            rls_write_check,
            ..
        } => ctx.set_write_check(collection, rls_write_check),

        // Ship the write predicate, and refuse under a read policy: each of
        // these replies with a value computed from the stored row — the new
        // counter, the pre-swap value, the two balances — through a payload
        // with no row-filter slot, so a policy that hides the row cannot be
        // honored on the way out and the answer alone discloses it.
        KvOp::Incr {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Cas {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Transfer {
            collection,
            rls_write_check,
            ..
        } => {
            ctx.refuse_if_policy(collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // Inject both policies: the old value this returns is a row body, so
        // the read filter decides whether it may be shown — an excluded one
        // comes back absent, indistinguishable from a key that did not exist —
        // while the compiled write predicate decides `new_value`. The two slots
        // stay separate because one bounds visibility and the other the write.
        KvOp::GetSet {
            collection,
            rls_filters,
            rls_write_check,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // A move spans two collections with independent policies, so each side
        // ships its own predicate: the source's decides the row being removed,
        // the destination's the row being inserted. The read half refuses for
        // the same reason as the atomics — a `NotFound` reply reports whether
        // a row the policy hides exists.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            source_rls_write_check,
            dest_rls_write_check,
            ..
        } => {
            ctx.refuse_if_policy(source_collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.refuse_if_policy(dest_collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.set_write_check(source_collection, source_rls_write_check)?;
            ctx.set_write_check(dest_collection, dest_rls_write_check)
        }

        // Refuse: a truncate removes every row without reading one, so there is
        // no image the policy could be evaluated against — and a policy that
        // restricts which rows this identity may write is precisely a statement
        // that it may not remove all of them. The document engine's truncate
        // refuses for the same reason.
        KvOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),

        // No-op: index DDL writes no user row, so no row policy restricts it.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

/// Why a read policy cannot be honored by a write that replies with a value
/// derived from the stored row.
const KV_COMPUTED_REPLY_REASON: &str = "the reply is a value computed from the stored row rather than a row body, so the row filter \
     cannot be evaluated against it and the answer alone discloses that the key exists";

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy, store_with_predicate,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::PolicyType;

    /// `alice` in the shared fixture has user id 42, so a row carrying
    /// `owner_id = "42"` satisfies `owner_id = $auth.id` and any other does not.
    fn body(owner_id: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack_or_empty(&serde_json::json!({
            "owner_id": owner_id,
            "amount": 100,
        }))
    }

    fn kv_put_row(collection: &str, owner_id: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.into(),
            key: b"k1".to_vec(),
            value: body(owner_id),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn kv_delete(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.into(),
            keys: vec![b"k1".to_vec()],
            rls_write_check: Vec::new(),
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &[u8] {
        match plan {
            PhysicalPlan::Kv(KvOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Expire {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::FieldSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Incr {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::GetSet {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A multi-column KV row is a MessagePack map, exactly like a document
    /// body, so the write policy decides it at plan time: a conforming row is
    /// admitted and a violating one fails the statement.
    #[test]
    fn kv_put_is_admitted_or_rejected_on_its_own_post_image() {
        let store = store_with_write_policy("sessions");

        let mut conforming = kv_put_row("sessions", "42");
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = kv_put_row("sessions", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A single-column `value` write stores one opaque scalar: it carries no
    /// field the predicate could name, so it fails closed rather than being
    /// waved through as "not a document".
    #[test]
    fn an_opaque_scalar_value_is_rejected_under_a_write_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A `RETURNING` on a KV write ships rows back, so a read-only policy must
    /// land in the write's post-filter slot. Leaving it empty would return rows
    /// the same principal's `SELECT` hides.
    #[test]
    fn a_kv_write_receives_the_read_policy_filter() {
        let store = store_with_read_policy("sessions");
        let mut plan = kv_put_row("sessions", "42");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::Put { rls_filters, .. }) => assert!(
                !rls_filters.is_empty(),
                "the read policy must gate RETURNING output"
            ),
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A batch fails whole when any one of its rows violates the policy.
    #[test]
    fn batch_put_is_rejected_when_any_row_violates_the_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::BatchPut {
            collection: "sessions".into(),
            entries: vec![(b"k1".to_vec(), body("42")), (b"k2".to_vec(), body("99"))],
            ttl_ms: 0,
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The row a delete removes is only known once the handler reads it, so
    /// the compiled predicate travels with the plan instead of refusing it.
    #[test]
    fn kv_delete_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = kv_delete("sessions");
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !write_check(&plan).is_empty(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// A TTL mutation leaves the body untouched, so the stored row is the image
    /// the policy decides — shipped, not refused.
    #[test]
    fn expire_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// A field merge exists only after the stored row is read.
    #[test]
    fn field_set_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            updates: Vec::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// The incremented value is computed inside the engine, so the predicate
    /// rides along for the engine to decide the computed image against.
    #[test]
    fn incr_carries_the_write_predicate() {
        let store = store_with_write_policy("counters");
        let mut plan = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"k1".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(!write_check(&plan).is_empty());
    }

    /// `GETSET` needs both halves: the read filter bounds the old value it
    /// hands back, the write predicate bounds the value it stores. They are two
    /// distinct slots, never the same bytes reused.
    #[test]
    fn getset_carries_both_halves_in_separate_slots() {
        let store = store_with_predicate(
            "sessions",
            PolicyType::All,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
        );
        let mut plan = PhysicalPlan::Kv(KvOp::GetSet {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            new_value: body("42"),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::GetSet {
                rls_filters,
                rls_write_check,
                ..
            }) => {
                assert!(
                    !rls_filters.is_empty(),
                    "read half must gate the returned old value"
                );
                assert!(
                    !rls_write_check.is_empty(),
                    "write half must gate the write"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A cross-collection move carries one predicate per side, so a policy on
    /// either end reaches the gate that decides that end's row.
    #[test]
    fn transfer_item_carries_a_predicate_for_each_side() {
        let store = store_with_write_policy("vault");
        let mut plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "vault".into(),
            dest_collection: "inbox".into(),
            item_key: b"i1".to_vec(),
            dest_key: b"d1".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            source_rls_write_check: Vec::new(),
            dest_rls_write_check: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_rls_write_check,
                dest_rls_write_check,
                ..
            }) => {
                assert!(
                    !source_rls_write_check.is_empty(),
                    "the policed source must reach its own gate"
                );
                assert!(
                    dest_rls_write_check.is_empty(),
                    "an unpoliced destination must not inherit the source's predicate"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A truncate removes every row without reading one, so there is no image
    /// the policy could decide.
    #[test]
    fn truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Truncate {
            collection: "sessions".into(),
        });
        assert_write_refused(inject(&mut plan, &store), "sessions");
    }

    /// A read policy alone must not start rejecting writes: the write half is
    /// keyed on write policies only.
    #[test]
    fn a_read_policy_alone_leaves_the_write_gate_empty() {
        let store = store_with_read_policy("sessions");
        let mut put = kv_put_row("sessions", "99");
        assert!(inject(&mut put, &store).is_ok());

        let mut delete = kv_delete("sessions");
        assert!(inject(&mut delete, &store).is_ok());
        assert!(write_check(&delete).is_empty());
    }

    /// A policy on a different collection must not restrict this one.
    #[test]
    fn kv_put_on_an_unpoliced_collection_runs() {
        let store = store_with_write_policy("other");
        let mut plan = kv_put_row("sessions", "99");
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// With no policy the write is untouched.
    #[test]
    fn kv_put_without_a_policy_is_untouched() {
        let mut plan = kv_put_row("sessions", "99");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A TTL probe on a policed collection discloses that a hidden key exists.
    #[test]
    fn get_ttl_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
        });
        assert_refused(inject(&mut plan, &store), "sessions");
    }

    /// A sorted-index read names no collection, so a read policy anywhere in
    /// the tenant refuses it: its ranked keys come from stored rows and carry
    /// no filter slot the policy could be applied through.
    #[test]
    fn sorted_index_read_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        match inject(&mut plan, &store) {
            Err(crate::Error::PlanError { detail }) => {
                assert!(detail.contains("sorted-index"), "got {detail}")
            }
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// …and every other sorted-index shape is refused for the same reason.
    #[test]
    fn every_sorted_index_shape_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("scores");
        for op in [
            KvOp::SortedIndexRank {
                index_name: "leaderboard".into(),
                primary_key: b"p1".to_vec(),
            },
            KvOp::SortedIndexRange {
                index_name: "leaderboard".into(),
                score_min: None,
                score_max: None,
            },
            KvOp::SortedIndexCount {
                index_name: "leaderboard".into(),
            },
        ] {
            let mut plan = PhysicalPlan::Kv(op);
            assert!(
                inject(&mut plan, &store).is_err(),
                "expected refusal for {plan:?}"
            );
        }
    }

    /// With no policy in the tenant the read is untouched, so an authorized
    /// caller sees exactly what it saw before.
    #[test]
    fn sorted_index_read_without_a_policy_is_untouched() {
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
