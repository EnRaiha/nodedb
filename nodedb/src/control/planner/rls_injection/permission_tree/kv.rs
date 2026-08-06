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

        // Refuse: a sorted-index read returns ranked keys, a rank, or a count
        // taken from the rows of the collection the index was built over, and
        // the reply carries no slot the subtree filter could go in. The plan
        // names only the index, and this pass holds the permission cache
        // rather than the catalog that binds an index name to its collection,
        // so it asks the tenant-wide question — the same call the RLS pass
        // makes for these shapes. The handler resolves the binding from the
        // index registry and refuses on the owning collection.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => ctx.refuse_if_any_tree(
            "a sorted-index read returns ranked keys, a rank, or a count taken from stored rows, \
             and the plan names only the index",
        ),

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

    /// A sorted-index read names no collection, so a permission tree anywhere
    /// in the tenant refuses it: its ranked keys come from stored rows and
    /// carry no slot the subtree filter could go in.
    #[test]
    fn sorted_index_read_is_refused_under_a_tree() {
        let cache = cache_with_tree("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        match apply(&mut plan, &cache) {
            Err(crate::Error::PlanError { detail }) => {
                assert!(detail.contains("sorted-index"), "got {detail}")
            }
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// With no tree in the tenant the read is untouched, so an authorized
    /// caller sees exactly what it saw before.
    #[test]
    fn sorted_index_read_without_a_tree_is_untouched() {
        use super::super::plan::test_support::apply_without_tree;

        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
