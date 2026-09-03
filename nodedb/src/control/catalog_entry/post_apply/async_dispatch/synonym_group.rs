// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for synonym group catalog entries.
//!
//! `PutSynonymGroup` installs the group in every core's FTS backend on this
//! node, and `DeleteSynonymGroup` removes it. Both run on every node, so a
//! follower expands a query's terms exactly as the node that ran the statement
//! does.
//!
//! ## The backend holds its own copy
//!
//! The FTS backend persists each group itself, keyed by database. Nothing
//! reseeds it from the catalog at boot, so a lost dispatch leaves this node
//! expanding differently for good. That is why every failure files a report:
//! the catalog row is already committed and nothing here can propagate.
//!
//! The single-node DDL handlers call these directly, where no applier runs and
//! the post-apply lane never fires.

use nodedb_fts::SynonymGroupRecord;
use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredSynonymGroup;
use crate::control::state::SharedState;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Name the fan-out reports a synonym group dispatch under.
///
/// A group belongs to a database and a tenant, not to a collection. The
/// fan-out addresses each core directly, so this name only labels the ack
/// line and the unreached-core error.
const SYNONYM_SENTINEL_COLLECTION: &str = "_synonym_groups";

/// Install one synonym group in every core's FTS backend on this node.
pub async fn put_async(stored: StoredSynonymGroup, shared: &SharedState) {
    let record = SynonymGroupRecord {
        name: stored.name.clone(),
        terms: stored.terms.clone(),
        created_at: stored.created_at,
    };
    let record_json = match sonic_rs::to_string(&record) {
        Ok(json) => json,
        Err(error) => {
            let error = crate::Error::Internal {
                detail: format!("serialize synonym group: {error}"),
            };
            report(&error, "put_serialize", &stored.name, &target(&stored));
            return;
        }
    };

    let plan = PhysicalPlan::Meta(MetaOp::PutSynonymGroup {
        tenant_id: stored.tenant_id,
        record_json,
    });
    let fanout = target(&stored);
    if let Err(error) = dispatch_to_every_core(shared, &fanout, &plan).await {
        report(&error, "put_dispatch", &stored.name, &fanout);
    }
}

/// Remove one synonym group from every core's FTS backend on this node.
pub async fn delete_async(database_id: u64, tenant_id: u64, name: String, shared: &SharedState) {
    let plan = PhysicalPlan::Meta(MetaOp::DeleteSynonymGroup {
        tenant_id,
        name: name.clone(),
    });
    let fanout = fanout_for(database_id, tenant_id, &name);
    if let Err(error) = dispatch_to_every_core(shared, &fanout, &plan).await {
        report(&error, "delete_dispatch", &name, &fanout);
    }
}

/// Name one group for the core fan-out's ack line and error detail.
fn target(stored: &StoredSynonymGroup) -> CoreFanout<'_> {
    fanout_for(stored.database_id, stored.tenant_id, &stored.name)
}

fn fanout_for(database_id: u64, tenant_id: u64, name: &str) -> CoreFanout<'_> {
    CoreFanout {
        database_id,
        tenant_id,
        collection: SYNONYM_SENTINEL_COLLECTION,
        what: "synonym group change",
        detail: name,
    }
}

/// File the one report for a stage this node lost, naming the group.
fn report(error: &crate::Error, stage: &'static str, name: &str, fanout: &CoreFanout<'_>) {
    crate::diag::synonym_group_not_applied(
        error,
        stage,
        fanout.database_id,
        fanout.tenant_id,
        name,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> StoredSynonymGroup {
        StoredSynonymGroup {
            database_id: 7,
            tenant_id: 3,
            name: "db_terms".to_string(),
            terms: vec!["database".to_string(), "db".to_string()],
            created_at: 42,
        }
    }

    /// The fan-out carries the group's own database, so a group created in one
    /// database never installs itself in another's cores.
    #[test]
    fn the_fanout_carries_the_groups_own_database() {
        let entry = stored();
        let fanout = target(&entry);
        assert_eq!(fanout.database_id, 7);
        assert_eq!(fanout.tenant_id, 3);
        assert_eq!(fanout.detail, "db_terms");
        assert_eq!(fanout.collection, SYNONYM_SENTINEL_COLLECTION);
    }

    /// The delete fan-out addresses the same database the put fan-out
    /// installed under, so a DROP reaches the group the CREATE made.
    #[test]
    fn delete_targets_the_database_that_holds_the_group() {
        let entry = stored();
        let put = target(&entry);
        let delete = fanout_for(entry.database_id, entry.tenant_id, &entry.name);
        assert_eq!(put.database_id, delete.database_id);
        assert_eq!(put.tenant_id, delete.tenant_id);
        assert_eq!(put.detail, delete.detail);
    }
}
