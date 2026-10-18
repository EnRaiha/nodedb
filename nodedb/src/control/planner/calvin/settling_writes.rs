// SPDX-License-Identifier: BUSL-1.1

//! The request guard for writes that move one materialized-sum balance.

use std::collections::BTreeSet;

use nodedb_physical::physical_task::PhysicalTask;

use super::is_dependent_predicate;
use crate::Error;

/// Refuse a request in which a predicate write and another write move the
/// same materialized-sum balance row. Each write settles its delta from the
/// committed rows, so a later write cannot see the rows an earlier one
/// changes and the balance would drift from the sum of the source.
pub fn refuse_several_settling_writes(tasks: &[PhysicalTask]) -> crate::Result<()> {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    if !tasks.iter().any(|task| is_dependent_predicate(&task.plan)) {
        return Ok(());
    }
    let mut targets = BTreeSet::new();
    for task in tasks {
        if let PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection,
            document_id,
            ..
        }) = &task.plan
            && !targets.insert((collection.to_string(), document_id.clone()))
        {
            return Err(Error::BadRequest {
                detail: "several writes of one request move the same materialized-sum balance \
                         and one is a predicate write; run them as separate statements or in a \
                         transaction block"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

    use crate::types::{DatabaseId, TenantId, VShardId};

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn bulk_delete(collection: &str) -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }))
    }

    #[test]
    fn plain_predicate_writes_are_not_refused() {
        let tasks = vec![bulk_delete("orders"), bulk_delete("users")];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }
}
