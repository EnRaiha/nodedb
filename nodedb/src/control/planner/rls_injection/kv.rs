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

        // No-op: a sorted-index read is keyed by index name alone and carries
        // no collection, so this pass has no `(tenant, collection)` pair to
        // resolve a policy against — the same position a collection-less graph
        // traversal is in. Enforcement for these belongs at the handler, which
        // can resolve the index's owning collection.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => Ok(()),

        // No-op: writes, atomics, TTL mutations, transfers, and index DDL. The
        // read policy does not apply; write policies are enforced separately by
        // `RlsPolicyStore::check_write_with_auth`.
        KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::BatchPut { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{assert_refused, inject, store_with_read_policy};
    use crate::bridge::envelope::PhysicalPlan;

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

    /// A sorted-index read names no collection, so this pass leaves it alone.
    #[test]
    fn sorted_index_read_is_untouched() {
        let store = store_with_read_policy("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(inject(&mut plan, &store).is_ok());
        assert_eq!(plan, before);
    }
}
