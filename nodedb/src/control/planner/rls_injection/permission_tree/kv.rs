// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for key-value engine operations.

use nodedb_physical::physical_plan::KvOp;

use super::context::{PermCtx, PermTreeLevel};

/// Exhaustive over [`KvOp`] so a new key-value operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_kv(ctx: &PermCtx<'_>, op: &mut KvOp) -> crate::Result<()> {
    match op {
        // Filter: the predicate scan pushes filters down, so the subtree ANDs
        // into the same slot as the user's predicate.
        KvOp::Scan {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: no pushdown slot, so the handler evaluates the subtree on
        // the fetched value. A row outside the subtree reads back as absent,
        // which a caller cannot distinguish from a missing key.
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
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: returns only the key's remaining lifetime. There is no row
        // body carrying the resource column, and answering at all confirms
        // that a key outside the caller's subtree exists.
        KvOp::GetTtl { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the reply is a TTL rather than a row body, so the subtree filter cannot be evaluated \
             and the answer alone discloses that the key exists",
        ),

        // Refuse: the clone materializer streams raw `(key, value)` pairs
        // through a cursor payload with no filter slot.
        KvOp::MaterializeScan { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the materializing scan streams raw stored values through a cursor payload that \
             carries no subtree filter",
        ),

        // Filter (write level, blanket): every one of these writes a value at
        // a key it names directly — including the read-modify-write atomics,
        // whose reply is derived from the value they just wrote. There is no
        // predicate to narrow, so the identity must hold write access
        // somewhere in the tree.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::Transfer { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Filter (delete level, blanket): both remove rows they name directly.
        KvOp::Delete { collection, .. } | KvOp::Truncate { collection } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }

        // Filter (both levels, blanket): the item leaves the source collection
        // and lands in the destination, so it is a delete on one and a write
        // on the other.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            ctx.authorize(source_collection, PermTreeLevel::Delete)?;
            ctx.authorize(dest_collection, PermTreeLevel::Write)
        }

        // No-op: a sorted-index read is keyed by index name alone and carries
        // no collection, so this pass has no `(tenant, collection)` pair to
        // resolve a tree definition against — the same call the RLS pass made
        // for these shapes. Enforcement for them belongs at the handler, which
        // can resolve the index's owning collection.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => Ok(()),

        // No-op: index DDL. It describes the collection rather than acting on
        // its rows, and is authorized as DDL rather than against a level.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// A key-value get is narrowed to the readable subtree.
    #[test]
    fn get_receives_the_subtree_filter() {
        let cache = cache_with_tree("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::Get { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A TTL probe on a governed collection discloses that a hidden key
    /// exists.
    #[test]
    fn get_ttl_is_refused_under_a_tree() {
        let cache = cache_with_tree("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
        });
        assert_refused(apply(&mut plan, &cache), "sessions");
    }

    /// A sorted-index read names no collection, so this pass leaves it alone.
    #[test]
    fn sorted_index_read_is_untouched() {
        let cache = cache_with_tree("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(apply(&mut plan, &cache).is_ok());
        assert_eq!(plan, before);
    }
}
