// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for continuous-aggregate post-apply capture sites.
//!
//! The continuous-aggregate manager is a per-core in-memory registry. A core
//! that misses a committed register or unregister computes a different set
//! of aggregates than the catalog every node agreed on.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed continuous-aggregate change that did not reach every core on
/// this node.
pub(in crate::diag) struct ContinuousAggregateNotApplied<'a> {
    /// Stage that failed (`put_decode`, `put_dispatch`, `delete_dispatch`).
    pub stage: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Aggregate the entry names.
    pub aggregate: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for ContinuousAggregateNotApplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.continuous_aggregate_not_applied"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the bug; the aggregate is the occurrence,
        // so one broken node files one report.
        format!("stage={};cause={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "aggregate": self.aggregate,
            "error_class": self.error_class,
            "why_fatal": "the catalog change is already committed by consensus, and nothing \
                          re-dispatches it to this node's cores. A core without the \
                          register computes no buckets for the aggregate. A core without \
                          the unregister keeps computing buckets for a dropped aggregate",
            "operator_action": "for a lost register, drop and re-create the aggregate so \
                                 every node registers it again. For a lost unregister, \
                                 restart this node: the per-core registry is in memory, and \
                                 the catalog no longer holds the aggregate",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ContinuousAggregateNotApplied<'static> {
        ContinuousAggregateNotApplied {
            stage: "put_dispatch",
            database_id: 1,
            tenant_id: 2,
            aggregate: "hourly",
            error_class: "internal",
        }
    }

    #[test]
    fn grouping_ignores_the_aggregate_identity() {
        let first = sample();
        let second = ContinuousAggregateNotApplied {
            database_id: 90,
            tenant_id: 91,
            aggregate: "other",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "stage=put_dispatch;cause=internal");
    }
}
