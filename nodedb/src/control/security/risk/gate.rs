// SPDX-License-Identifier: BUSL-1.1

//! Turning a stamped `$auth.risk_score` into a refusal.
//!
//! Scoring happens once, where the client address is genuinely known, in
//! `RequestAuthScopeBuilder::build`; enforcement happens at the composed
//! request-admission gate. [`RiskScorer::refusal_for`] is the pure step
//! between the two, so the decision can be tested without a running server.

use crate::control::security::auth_context::AuthContext;

use super::config::RiskDecision;
use super::scorer::RiskScorer;

/// The one reason string for "this principal must authenticate more
/// strongly before proceeding".
///
/// Shared with conditional scope grants: `GrantCondition::RequireMfa` and
/// `GrantCondition::StepUpAuth` reach the same outcome as
/// [`RiskDecision::StepUpMfa`], so they report it with this exact string
/// rather than a second one clients would have to learn separately.
pub const STEP_UP_REQUIRED: &str = "step-up authentication required";

/// A refusal produced by the risk gate: the client-facing resource string
/// and the detail recorded in the audit log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskRefusal {
    /// Client-facing reason, carried as `RejectedAuthz { resource }`.
    pub resource: String,
    /// Longer detail for the audit trail.
    pub audit_detail: String,
}

impl RiskScorer {
    /// Decide whether this context must be refused.
    ///
    /// Returns `None` when the request may proceed: either scoring is
    /// disabled, or the stamped score falls in the allow band.
    ///
    /// Fails closed in two cases:
    ///
    /// * **Score absent while scoring is enabled.** The scope was built
    ///   without a usable client address, so no honest assessment exists.
    ///   Scoring a placeholder address instead would mis-score every request
    ///   behind that transport, so the request is refused rather than
    ///   silently admitted on an unassessed path.
    /// * **[`RiskDecision::StepUpMfa`].** There is no step-up authentication
    ///   protocol in the server yet, so the middle band is refused with its
    ///   own distinct reason telling the client that step-up is required —
    ///   rather than admitted, which would collapse the middle band into
    ///   `Allow` and leave `deny_threshold` as the only live knob. When a
    ///   real step-up flow exists it replaces this refusal with a challenge.
    ///   `GrantCondition::RequireMfa` / `GrantCondition::StepUpAuth` report
    ///   the same outcome through the same [`STEP_UP_REQUIRED`] string.
    pub fn refusal_for(&self, auth: &AuthContext) -> Option<RiskRefusal> {
        if !self.is_enabled() {
            return None;
        }

        let Some(score) = auth.risk_score else {
            return Some(RiskRefusal {
                resource: "risk assessment unavailable for this request".into(),
                audit_detail: format!(
                    "risk scoring is enabled but no client address was available to score \
                     session '{}' — refusing rather than admitting an unassessed request",
                    auth.session_id
                ),
            });
        };

        match self.decide(score) {
            RiskDecision::Allow => None,
            RiskDecision::StepUpMfa => Some(RiskRefusal {
                resource: STEP_UP_REQUIRED.into(),
                audit_detail: format!(
                    "risk score {score:.3} is in the step-up band \
                     ({} < score < {})",
                    self.config().allow_threshold,
                    self.config().deny_threshold
                ),
            }),
            RiskDecision::Deny => Some(RiskRefusal {
                resource: "denied by risk policy".into(),
                audit_detail: format!(
                    "risk score {score:.3} is at or above the deny threshold {}",
                    self.config().deny_threshold
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::security::risk::config::RiskConfig;
    use crate::types::TenantId;

    fn context(score: Option<f64>) -> AuthContext {
        let mut ctx = AuthContext::from_identity(
            &AuthenticatedIdentity::new_regular(
                1,
                "alice",
                TenantId::new(1),
                AuthMethod::ApiKey,
                vec![Role::ReadWrite],
                None,
                DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
            ),
            "s_gate".into(),
        );
        ctx.risk_score = score;
        ctx
    }

    fn enabled() -> RiskScorer {
        RiskScorer::new(RiskConfig {
            enabled: true,
            ..Default::default()
        })
    }

    #[test]
    fn disabled_scorer_never_refuses() {
        let scorer = RiskScorer::default();
        assert!(scorer.refusal_for(&context(None)).is_none());
        assert!(scorer.refusal_for(&context(Some(0.99))).is_none());
    }

    #[test]
    fn allow_band_proceeds() {
        assert!(enabled().refusal_for(&context(Some(0.1))).is_none());
    }

    #[test]
    fn step_up_band_is_refused_distinctly() {
        let refusal = enabled()
            .refusal_for(&context(Some(0.5)))
            .expect("step-up band must refuse");
        assert_eq!(refusal.resource, "step-up authentication required");
    }

    #[test]
    fn deny_band_is_refused() {
        let refusal = enabled()
            .refusal_for(&context(Some(0.9)))
            .expect("deny band must refuse");
        assert_eq!(refusal.resource, "denied by risk policy");
    }

    #[test]
    fn missing_score_fails_closed_when_enabled() {
        let refusal = enabled()
            .refusal_for(&context(None))
            .expect("an unassessed request must not be admitted");
        assert_eq!(
            refusal.resource,
            "risk assessment unavailable for this request"
        );
    }
}
