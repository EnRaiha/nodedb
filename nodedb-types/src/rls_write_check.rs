// SPDX-License-Identifier: BUSL-1.1

//! The compiled row-level-security WRITE predicate carried on a write plan.
//!
//! This type exists because a bare `Vec<u8>` could not tell two very
//! different things apart:
//!
//! - no write policy restricts this identity here, so admit every row;
//! - a policy does restrict it, but the predicate was dropped somewhere
//!   between the planner and the handler, so admit every row.
//!
//! Both read as "empty", and the second one silently disables the policy.
//! Every value of this type names which case it is, so a lost predicate
//! cannot be mistaken for an unrestricted write.
//!
//! There is deliberately no `Default`. A write plan cannot be built with an
//! unexplained empty check.

use serde::{Deserialize, Serialize};

/// What the Data Plane gate must do with one write's policy check.
///
/// Returned by [`RlsWriteCheck::decision`]. Matching on it is exhaustive, so
/// a caller cannot handle the admit cases and forget the deny case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteGateDecision<'a> {
    /// Admit every row without evaluating anything.
    AdmitAll,
    /// Evaluate these compiled predicate bytes against each row image.
    Evaluate(&'a [u8]),
    /// Deny. The plan reached the gate before RLS injection ran over it.
    /// This is a bug in the write path, not a policy outcome.
    DenyNotInjected,
}

/// The write-policy check attached to one write operation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RlsWriteCheck {
    /// A compiled predicate. The Data Plane decodes these bytes as
    /// `Vec<ScanFilter>` and evaluates them against each row image.
    Predicate(Vec<u8>),

    /// RLS injection ran and found no write policy restricting this identity
    /// on this collection. Admits every row.
    NoPolicyApplies,

    /// A follower applying a replicated write, or a WAL replay after a crash.
    ///
    /// The identity that authored the write is not present here. Re-deciding
    /// the predicate against a missing identity could deny a write the leader
    /// already committed, which would diverge the replicas. So this admits
    /// every row on purpose.
    ///
    /// Only follower-apply and WAL-replay code may build this. Never build it
    /// on a path that still has a live writing identity.
    AlreadyDecidedElsewhere,

    /// The same request already ran the write policy over this exact row
    /// image, in the Control Plane, moments before this op was built.
    ///
    /// A merge's delete arm and a mirrored implicit-edge delete both work this
    /// way: the policy admitted the row, and this op removes that same row.
    /// The writing identity is live and known here — that is what separates
    /// this from [`RlsWriteCheck::AlreadyDecidedElsewhere`].
    ///
    /// Only build this where the earlier decision provably covered the same
    /// row image. If the op could touch a row the earlier check did not see,
    /// it is not this variant.
    DecidedEarlierInRequest,

    /// The write targets a collection NodeDB maintains for itself, which no
    /// user can create and no policy can be attached to.
    ///
    /// Rate-limit bookkeeping is the example. These writes never go through
    /// RLS injection, so they cannot use [`RlsWriteCheck::NoPolicyApplies`],
    /// which means injection ran and found nothing.
    SystemInternalCollection,

    /// The plan has been built but RLS injection has not run over it yet.
    ///
    /// This is a transient state inside the Control Plane. It must never reach
    /// the Data Plane. If it does, the gate denies the write rather than
    /// admitting it, so a missed injection fails loudly instead of quietly
    /// disabling the policy.
    PendingInjection,
}

impl RlsWriteCheck {
    /// Build the check from the policy compiler's output.
    ///
    /// Empty bytes mean no policy restricts this identity, and become
    /// [`RlsWriteCheck::NoPolicyApplies`] rather than an empty predicate, so a
    /// later reader cannot confuse "no policy" with "predicate lost".
    ///
    /// Call this only from the RLS injection pass.
    pub fn from_injected(predicate_bytes: Vec<u8>) -> Self {
        if predicate_bytes.is_empty() {
            RlsWriteCheck::NoPolicyApplies
        } else {
            RlsWriteCheck::Predicate(predicate_bytes)
        }
    }

    /// The explicit bypass for a follower's replicated apply or a WAL replay.
    ///
    /// See [`RlsWriteCheck::AlreadyDecidedElsewhere`] for why these paths carry
    /// no predicate. Do not use it to silence a compile error on a path that
    /// has a live writing identity — use [`RlsWriteCheck::pending_injection`]
    /// there instead, which fails closed.
    pub fn already_decided_elsewhere() -> Self {
        RlsWriteCheck::AlreadyDecidedElsewhere
    }

    /// The bypass for an op whose rows this same request already admitted.
    ///
    /// See [`RlsWriteCheck::DecidedEarlierInRequest`]. Use it only when the
    /// earlier check covered the same row image.
    pub fn decided_earlier_in_request() -> Self {
        RlsWriteCheck::DecidedEarlierInRequest
    }

    /// The bypass for a write to one of NodeDB's own internal collections.
    ///
    /// See [`RlsWriteCheck::SystemInternalCollection`].
    pub fn system_internal_collection() -> Self {
        RlsWriteCheck::SystemInternalCollection
    }

    /// The placeholder a plan carries between construction and injection.
    ///
    /// Safe to use wherever the correct value is not yet known: it fails
    /// closed, and the dispatch boundary rejects any write plan still holding
    /// it.
    pub fn pending_injection() -> Self {
        RlsWriteCheck::PendingInjection
    }

    /// What the Data Plane gate must do with this check.
    pub fn decision(&self) -> WriteGateDecision<'_> {
        match self {
            RlsWriteCheck::Predicate(bytes) => WriteGateDecision::Evaluate(bytes),
            RlsWriteCheck::NoPolicyApplies
            | RlsWriteCheck::AlreadyDecidedElsewhere
            | RlsWriteCheck::DecidedEarlierInRequest
            | RlsWriteCheck::SystemInternalCollection => WriteGateDecision::AdmitAll,
            RlsWriteCheck::PendingInjection => WriteGateDecision::DenyNotInjected,
        }
    }

    /// True only when a compiled predicate is attached.
    ///
    /// Control Plane callers use this to ask "does a write policy restrict
    /// this collection for this identity". It is not a gate decision — the
    /// Data Plane uses [`RlsWriteCheck::decision`] for that.
    pub fn has_predicate(&self) -> bool {
        matches!(self, RlsWriteCheck::Predicate(_))
    }

    /// True while this plan has not been through RLS injection.
    ///
    /// The dispatch boundary uses this to refuse a write plan that would
    /// otherwise reach the Data Plane un-injected.
    pub fn is_pending_injection(&self) -> bool {
        matches!(self, RlsWriteCheck::PendingInjection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_compiler_output_is_no_policy_not_an_empty_predicate() {
        assert_eq!(
            RlsWriteCheck::from_injected(Vec::new()),
            RlsWriteCheck::NoPolicyApplies
        );
        assert!(!RlsWriteCheck::from_injected(Vec::new()).has_predicate());
    }

    #[test]
    fn compiled_bytes_become_a_predicate_the_gate_evaluates() {
        let check = RlsWriteCheck::from_injected(vec![1, 2, 3]);
        assert!(check.has_predicate());
        assert_eq!(check.decision(), WriteGateDecision::Evaluate(&[1, 2, 3]));
    }

    #[test]
    fn every_bypass_admits_every_row() {
        for check in [
            RlsWriteCheck::NoPolicyApplies,
            RlsWriteCheck::already_decided_elsewhere(),
            RlsWriteCheck::decided_earlier_in_request(),
            RlsWriteCheck::system_internal_collection(),
        ] {
            assert_eq!(
                check.decision(),
                WriteGateDecision::AdmitAll,
                "{check:?} must admit"
            );
            assert!(!check.has_predicate(), "{check:?} carries no predicate");
        }
    }

    /// The whole point of the type: a plan that never went through injection
    /// is denied, where a bare empty `Vec<u8>` used to be admitted.
    #[test]
    fn an_uninjected_plan_is_denied_rather_than_admitted() {
        let check = RlsWriteCheck::pending_injection();
        assert_eq!(check.decision(), WriteGateDecision::DenyNotInjected);
        assert!(check.is_pending_injection());
        assert!(!check.has_predicate());
    }
}
