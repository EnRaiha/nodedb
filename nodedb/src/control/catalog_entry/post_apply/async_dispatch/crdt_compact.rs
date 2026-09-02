// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for the COMPACT HISTORY catalog entry.
//!
//! `CompactHistory` deletes the checkpoint rows on every node and
//! dispatches `CrdtOp::CompactAtVersion` to every core on that node. The
//! compaction discards durable oplog entries, so a node that skips it keeps a
//! history its peers reclaimed and answers an old-version read differently.
//!
//! The entry carries `target_version_json` because apply removes the
//! checkpoint row that holds the version vector. This module reads the target
//! from the entry, never from the catalog.
//!
//! Nothing here can propagate a failure: the catalog rows are already
//! committed. Every failed stage files a `Capture` instead, because a node
//! silently keeping the compacted history is the defect this module exists
//! to stop.
//!
//! The COMPACT HISTORY handler calls this directly on a single node, where no
//! applier runs and the post-apply lane never fires.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Build the compaction plan every node runs against its own oplog.
fn compact_plan(database_id: u64, collection: &str, target_version_json: String) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(database_id),
            collection,
        ),
        target_version_json,
    })
}

/// Discard this node's oplog entries below the committed compaction target.
pub async fn compact_async(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    target_version_json: &str,
    shared: &SharedState,
) {
    let plan = compact_plan(database_id, collection, target_version_json.to_string());
    let target = CoreFanout {
        database_id,
        tenant_id,
        collection,
        what: "history compaction",
        detail: "",
    };

    if let Err(error) = dispatch_to_every_core(shared, &target, &plan).await {
        crate::diag::history_compaction_not_applied(
            &error,
            "compact_dispatch",
            database_id,
            tenant_id,
            collection,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plan names the qualified collection and carries the committed
    /// target verbatim. A rewritten target compacts a node to a version its
    /// peers never agreed on.
    #[test]
    fn compact_plan_carries_the_committed_target() {
        let PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
            collection,
            target_version_json,
        }) = compact_plan(7, "documents", "{\"n1\":4}".to_string())
        else {
            panic!("compact_plan must build a CrdtOp::CompactAtVersion");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(target_version_json, "{\"n1\":4}");
    }
}
