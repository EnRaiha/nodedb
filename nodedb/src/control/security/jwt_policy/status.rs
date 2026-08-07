// SPDX-License-Identifier: BUSL-1.1

//! Account-status blocking from a JWT claim.
//!
//! `[auth.jwt] status_claim` names the claim carrying the provider's account
//! state; `blocked_statuses` lists the values that deny access. The check runs
//! in the JWKS registry right after signature, issuer, audience, and time
//! validation, before any identity is issued, so it covers both bearer routes
//! (HTTP static providers and native/OIDC catalog providers).
//!
//! Comparison is case-insensitive: a provider that spells the value
//! `"Suspended"` must not slip past a configured `"suspended"`.
//!
//! A token that does not carry the claim at all — or carries it as JSON `null`
//! — is **not** blocked: the operator asked for specific values to be denied,
//! not for the claim to be mandatory. A claim that is present but structurally
//! uncomparable (an object) *is* rejected, because status blocking is on and
//! the value cannot be evaluated.

use crate::control::security::jwt::{JwtClaims, JwtError};
use crate::control::security::jwt_policy::resolve_claim;

/// Reject a verified token whose status claim carries a blocked value.
///
/// Inert when no `status_claim` is configured or `blocked_statuses` is empty.
pub fn check_blocked_status(
    status_claim: Option<&str>,
    blocked_statuses: &[String],
    claims: &JwtClaims,
) -> Result<(), JwtError> {
    let Some(claim_name) = status_claim else {
        return Ok(());
    };
    if blocked_statuses.is_empty() {
        return Ok(());
    }
    // `status_claim` is an operator-supplied claim name, so it is resolved the
    // same way every other configured claim name is: exact key first, dotted
    // path second. A nested provider status must not be silently unreachable —
    // that would turn status blocking off without any signal.
    let Some(value) = resolve_claim(&claims.extra, claim_name) else {
        return Ok(());
    };

    match value {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::String(status) => reject_if_blocked(status, blocked_statuses),
        serde_json::Value::Bool(flag) => reject_if_blocked(&flag.to_string(), blocked_statuses),
        serde_json::Value::Number(number) => {
            reject_if_blocked(&number.to_string(), blocked_statuses)
        }
        serde_json::Value::Array(items) => {
            for item in items {
                match item {
                    serde_json::Value::String(status) => {
                        reject_if_blocked(status, blocked_statuses)?
                    }
                    // A non-string element cannot be compared against the
                    // configured list; treat it the same as an uncomparable
                    // scalar rather than skipping it.
                    _ => return Err(JwtError::BlockedStatus),
                }
            }
            Ok(())
        }
        // Present but uncomparable while blocking is enabled — fail closed.
        serde_json::Value::Object(_) => Err(JwtError::BlockedStatus),
    }
}

fn reject_if_blocked(status: &str, blocked_statuses: &[String]) -> Result<(), JwtError> {
    if blocked_statuses
        .iter()
        .any(|blocked| blocked.eq_ignore_ascii_case(status))
    {
        return Err(JwtError::BlockedStatus);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with_status(value: Option<serde_json::Value>) -> JwtClaims {
        let mut extra = std::collections::HashMap::new();
        if let Some(value) = value {
            extra.insert("account_status".to_owned(), value);
        }
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: Vec::new(),
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1,
            iss: "https://idp.example.com".into(),
            aud: vec!["nodedb".into()],
            user_id: 7,
            is_superuser: false,
            extra,
        }
    }

    fn blocked() -> Vec<String> {
        vec!["suspended".to_owned(), "banned".to_owned()]
    }

    #[test]
    fn blocked_status_value_is_rejected() {
        let claims = claims_with_status(Some(serde_json::json!("suspended")));
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Err(JwtError::BlockedStatus)
        );
    }

    #[test]
    fn allowed_status_value_is_accepted() {
        let claims = claims_with_status(Some(serde_json::json!("active")));
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Ok(())
        );
    }

    /// A token that never carries the claim is not blocked — the knob denies
    /// listed values, it does not make the claim mandatory.
    #[test]
    fn missing_status_claim_is_accepted() {
        let claims = claims_with_status(None);
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Ok(())
        );
    }

    #[test]
    fn casing_does_not_defeat_the_block() {
        let claims = claims_with_status(Some(serde_json::json!("SUSPENDED")));
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Err(JwtError::BlockedStatus)
        );
    }

    #[test]
    fn any_blocked_element_of_an_array_claim_rejects() {
        let claims = claims_with_status(Some(serde_json::json!(["active", "banned"])));
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Err(JwtError::BlockedStatus)
        );
    }

    #[test]
    fn uncomparable_object_claim_fails_closed() {
        let claims = claims_with_status(Some(serde_json::json!({ "state": "suspended" })));
        assert_eq!(
            check_blocked_status(Some("account_status"), &blocked(), &claims),
            Err(JwtError::BlockedStatus)
        );
    }

    #[test]
    fn unconfigured_knob_is_inert() {
        let claims = claims_with_status(Some(serde_json::json!("suspended")));
        assert_eq!(check_blocked_status(None, &blocked(), &claims), Ok(()));
        assert_eq!(
            check_blocked_status(Some("account_status"), &[], &claims),
            Ok(())
        );
    }
}
