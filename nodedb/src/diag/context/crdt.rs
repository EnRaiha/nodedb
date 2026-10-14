// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for CRDT capture sites: history post-apply, rejected
//! deltas the dead-letter queue refused, and constraint installs a committed
//! DDL left to the reconcile loop.
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

/// A constraint-rejected CRDT delta whose dead-letter entry the queue
/// refused, so no node-local record of the rejection exists.
pub(in crate::diag) struct CrdtDeadLetterNotEnqueued<'a> {
    pub tenant_id: u64,
    /// Collection the delta wrote.
    pub collection: &'a str,
    /// Constraint the delta violated.
    pub constraint: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for CrdtDeadLetterNotEnqueued<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.crdt_dead_letter_not_enqueued"
    }

    fn grouping_key(&self) -> String {
        // The error class names the cause. Tenant, collection and constraint
        // are the occurrence, so a full queue files one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "constraint": self.constraint,
            "error_class": self.error_class,
            "why_reported": "the delta violated a constraint and was refused, but the \
                             dead-letter queue did not take its entry. The apply is \
                             refused as an error so the sender keeps the delta, and every \
                             further rejected delta is refused the same way until the \
                             queue has room",
            "operator_action": "inspect and drain the tenant's dead-letter queue, then let \
                                 the sender re-push. A full queue means rejected deltas \
                                 arrive faster than they are resolved",
        })
    }
}

/// A committed DDL whose CRDT constraint install on the collection's data
/// Raft log failed, so the DDL returned before its constraint set was live.
pub(in crate::diag) struct ConstraintInstallDeferred<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection whose constraint version the DDL changed.
    pub collection: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for ConstraintInstallDeferred<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.constraint_install_deferred"
    }

    fn grouping_key(&self) -> String {
        // The error class names the cause. The collection is the occurrence,
        // so one unreachable group leader files one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "error_class": self.error_class,
            "why_reported": "the DDL committed a new constraint version, but its install on \
                             the collection's data Raft log failed. A CRDT delta admitted \
                             against the new version is refused as a retryable gap on every \
                             replica that has not installed it",
            "operator_action": "check that the collection's data group has a reachable \
                                 leader. The constraint-reconcile loop on the metadata leader \
                                 re-delivers the set. A report that keeps counting means that \
                                 delivery fails too",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_install_grouping_ignores_the_collection() {
        let first = ConstraintInstallDeferred {
            database_id: 1,
            tenant_id: 2,
            collection: "users",
            error_class: "deadline exceeded",
        };
        let second = ConstraintInstallDeferred {
            database_id: 9,
            tenant_id: 8,
            collection: "orders",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=deadline exceeded");
    }

    #[test]
    fn dead_letter_grouping_ignores_the_occurrence() {
        let first = CrdtDeadLetterNotEnqueued {
            tenant_id: 1,
            collection: "users",
            constraint: "users_email_unique",
            error_class: "dead-letter queue full",
        };
        let second = CrdtDeadLetterNotEnqueued {
            tenant_id: 7,
            collection: "orders",
            constraint: "orders_fk",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=dead-letter queue full");
    }

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
