// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for CRDT history post-apply capture sites.
//!
//! COMPACT HISTORY discards oplog entries on every node. A node that misses
//! the compaction keeps a history its peers reclaimed, so a read at an old
//! version answers differently depending on which node serves it.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed `CompactHistory` whose per-node oplog compaction
/// failed, so this node's history diverges from the replicated catalog.
pub(in crate::diag) struct HistoryCompactionNotApplied<'a> {
    /// Stage that failed (`compact_dispatch`).
    pub stage: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection holding the document whose history the statement compacts.
    pub collection: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for HistoryCompactionNotApplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.history_compaction_not_applied"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the bug; the collection is the occurrence,
        // so one broken node files one report.
        format!("stage={};cause={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "error_class": self.error_class,
            "why_fatal": "the checkpoint range delete is already committed by consensus, so \
                          the boundary this node needs to retry the compaction is gone from \
                          the catalog. This node keeps oplog entries its peers discarded, \
                          so a read at an old version or a restore answers differently here \
                          than on a peer, and the storage the operator asked to reclaim \
                          stays held",
            "operator_action": "re-run COMPACT HISTORY against a surviving checkpoint on the \
                                 same document, which proposes a fresh entry every node \
                                 compacts from. Compare SHOW VERSIONS output against a \
                                 healthy replica before serving history reads from this one",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> HistoryCompactionNotApplied<'static> {
        HistoryCompactionNotApplied {
            stage: "compact_dispatch",
            database_id: 1,
            tenant_id: 2,
            collection: "documents",
            error_class: "dispatch",
        }
    }

    #[test]
    fn grouping_ignores_the_collection_identity() {
        let first = sample();
        let second = HistoryCompactionNotApplied {
            database_id: 90,
            tenant_id: 91,
            collection: "other",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(
            first.grouping_key(),
            "stage=compact_dispatch;cause=dispatch"
        );
    }
}
