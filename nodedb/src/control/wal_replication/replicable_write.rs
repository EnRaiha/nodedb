// SPDX-License-Identifier: BUSL-1.1

//! A write plan proven safe to put on the Raft wire.
//!
//! A compiled RLS write predicate is decided against the writing identity. A
//! follower has no such identity, so the predicate cannot cross the wire — and
//! the leader applies through the same decode as every follower, so dropping it
//! leaves the write ungoverned on every replica.
//!
//! [`ReplicableWrite`] makes that state unrepresentable instead of refusing it
//! again at each encode site.

use crate::bridge::envelope::PhysicalPlan;

/// A write plan whose RLS write-check slots are all decided.
///
/// Guarantee: every slot is decided — one of `NoPolicyApplies`,
/// `AlreadyDecidedElsewhere`, `DecidedEarlierInRequest`, or
/// `SystemInternalCollection`. Never `Predicate`, never `PendingInjection`.
///
/// The field is private and only the two constructors below build the type, so
/// the guarantee holds for every value that exists.
pub struct ReplicableWrite<'a>(&'a PhysicalPlan);

impl<'a> ReplicableWrite<'a> {
    /// Decide `plan` for replication, or refuse it. Control Plane only.
    ///
    /// An op whose policy decision is deferred to the handler must be resolved
    /// to a concrete row set before it is proposed. Columnar `UPDATE`/`DELETE`
    /// already are, by `control::columnar_predicate_dml_orchestrator`. Every
    /// other engine reaches here still carrying the predicate, and this refuses
    /// it rather than replicating a write no replica will govern.
    ///
    /// Plain inserts are unaffected: the policy decides their rows at plan
    /// time, so they arrive stamped `DecidedEarlierInRequest` (or
    /// `NoPolicyApplies` where no policy restricts the identity).
    ///
    /// A slot still at `PendingInjection` is refused too, and separately: that
    /// write was never policy-checked at all, so replicating it would run it
    /// ungoverned on every replica.
    pub fn decide_for_replication(plan: &'a PhysicalPlan) -> crate::Result<Self> {
        let checks = plan.rls_write_checks();
        if checks.iter().any(|c| c.is_pending_injection()) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "internal invariant break: this write on '{}' reached the Raft propose path \
                     before RLS injection decided its write-policy check \
                     (RlsWriteCheck::PendingInjection); this is a skipped injection step, not a \
                     policy rejection.",
                    plan.collection().unwrap_or("<unknown>")
                ),
            });
        }
        if checks.iter().any(|c| c.has_predicate()) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "this write on '{}' carries an RLS write policy whose decision is deferred \
                     to the handler, and a follower has no writing identity to evaluate it \
                     against. It must be resolved to a concrete row set before it is proposed.",
                    plan.collection().unwrap_or("<unknown>")
                ),
            });
        }
        Ok(Self(plan))
    }

    /// Wrap a plan rebuilt from an entry that is already committed, without
    /// deciding anything.
    ///
    /// Sound only on the replay/catchup side: the entry's policy decision was
    /// made when it was first proposed, and a policy edited since then would
    /// change replay if it were re-decided here. The caller owes the guarantee
    /// that the plan's checks came from the committed entry, not from a live
    /// identity.
    ///
    /// No caller re-encodes a committed entry today: follower apply decodes,
    /// and WAL catchup dispatches straight to the Data Plane. This exists for
    /// the moment `decide_for_replication` starts resolving predicates rather
    /// than only refusing them — replay must keep taking this path instead.
    pub fn for_already_committed_entry(plan: &'a PhysicalPlan) -> Self {
        Self(plan)
    }

    /// The wrapped plan.
    pub fn plan(&self) -> &'a PhysicalPlan {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::ColumnarOp;
    use nodedb_types::RlsWriteCheck;

    fn delete_with(check: RlsWriteCheck) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: "orders".to_string(),
            filters: Vec::new(),
            rls_write_check: check,
        })
    }

    #[test]
    fn a_live_predicate_cannot_be_decided_for_replication() {
        let plan = delete_with(RlsWriteCheck::from_injected(vec![1, 2, 3]));
        assert!(ReplicableWrite::decide_for_replication(&plan).is_err());
    }

    /// An un-injected slot is refused separately: that write was never
    /// policy-checked, so it is a skipped step, not a deferred decision.
    #[test]
    fn an_uninjected_check_cannot_be_decided_for_replication() {
        let plan = delete_with(RlsWriteCheck::pending_injection());
        let Err(crate::Error::PlanError { detail }) =
            ReplicableWrite::decide_for_replication(&plan)
        else {
            panic!("an un-injected write must be refused with a PlanError");
        };
        assert!(
            detail.contains("PendingInjection"),
            "the refusal must name the invariant that broke; got {detail}"
        );
    }

    #[test]
    fn a_decided_check_is_accepted() {
        let plan = delete_with(RlsWriteCheck::decided_earlier_in_request());
        let write =
            ReplicableWrite::decide_for_replication(&plan).expect("decided check must be accepted");
        assert!(matches!(write.plan(), PhysicalPlan::Columnar(_)));
    }

    /// Replay never re-decides, so it wraps whatever the committed entry held.
    #[test]
    fn replay_wraps_without_deciding() {
        let plan = delete_with(RlsWriteCheck::already_decided_elsewhere());
        let write = ReplicableWrite::for_already_committed_entry(&plan);
        assert!(matches!(write.plan(), PhysicalPlan::Columnar(_)));
    }
}
