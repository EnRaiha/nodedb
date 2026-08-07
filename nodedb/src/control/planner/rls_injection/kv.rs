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

        // Refuse: the key-value engine stores opaque values rather than the
        // field-addressed rows a policy predicate names, and the atomics,
        // TTL mutations, and transfers derive their post-image from the stored
        // value inside the handler. There is no point in this plan where the
        // image a write policy decides can be evaluated, so a policy on the
        // collection refuses the write instead of letting it through unchecked.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Truncate { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::Transfer { collection, .. } => {
            ctx.refuse_if_write_policy(collection, KV_WRITE_REASON)
        }

        // Refuse: a transfer moves an item between two collections, so a policy
        // on either end restricts it.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            ctx.refuse_if_write_policy(source_collection, KV_WRITE_REASON)?;
            ctx.refuse_if_write_policy(dest_collection, KV_WRITE_REASON)
        }

        // No-op: index DDL writes no user row, so no row policy restricts it.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

/// Why a key-value write cannot be gated by a row policy.
const KV_WRITE_REASON: &str = "the key-value engine persists opaque values and derives every mutated image inside the \
     handler, so no row image is available for the policy to be evaluated against";

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn kv_put(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.into(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        })
    }

    /// The key-value engine cannot evaluate a row predicate against the opaque
    /// value it stores, so a write policy refuses the write rather than letting
    /// it land unchecked.
    #[test]
    fn kv_put_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = kv_put("sessions");
        assert_write_refused(inject(&mut plan, &store), "sessions");
    }

    /// A policy on a different collection must not refuse this one.
    #[test]
    fn kv_put_on_an_unpoliced_collection_runs() {
        let store = store_with_write_policy("other");
        let mut plan = kv_put("sessions");
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// With no policy the write is untouched.
    #[test]
    fn kv_put_without_a_policy_is_untouched() {
        let mut plan = kv_put("sessions");
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
