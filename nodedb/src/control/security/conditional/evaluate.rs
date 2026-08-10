// SPDX-License-Identifier: BUSL-1.1

//! Evaluating a scope grant's conditions against the request being served.
//!
//! Every condition fails **closed**: when the input a condition needs is
//! missing, the condition fails and its grant contributes nothing. A grant
//! that cannot be honestly evaluated is not a grant that may be applied.
//!
//! * `Temporal` — reads the clock the caller passes, which is always
//!   available; a request outside the window fails.
//! * `RequireMfa` — `$auth.metadata.mfa_verified` absent or not `"true"`
//!   fails. An unauthenticated-for-MFA request is not an MFA-verified one.
//! * `RequireIp` — no client address available (a transport that never
//!   resolved without a client address, or one whose peer address did not parse)
//!   fails, exactly like risk scoring refuses an unassessed request rather
//!   than admitting it.
//! * `StepUpAuth` — `$auth.auth_time` absent or zero fails: with no known
//!   authentication time, no step-up window can be shown to hold.
//! * `RequireDeviceTrust` — `$auth.metadata.device_trusted` absent or not
//!   `"true"` fails.
//!
//! A condition payload that does not decode into a known [`GrantCondition`]
//! never reaches here: `ScopeGrantStore::open` drops the whole grant instead
//! of loading it condition-free.
//!
//! `RequireMfa` and `StepUpAuth` are the same outcome adaptive-auth risk
//! scoring already produces for `RiskDecision::StepUpMfa` — the client must
//! re-authenticate more strongly — so they carry the very same
//! [`RiskRefusal`] shape and reason string rather than a second one that
//! clients would have to learn separately.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::blacklist::ip::check_ip_against_cidrs;
use crate::control::security::risk::{RiskRefusal, STEP_UP_REQUIRED};

use super::condition::GrantCondition;

/// Seconds in a day.
const SECS_PER_DAY: u64 = 86_400;

/// Seconds in an hour.
const SECS_PER_HOUR: u64 = 3_600;

/// Weekday of the Unix epoch (Thursday), as `0=Sunday`.
const EPOCH_WEEKDAY: u64 = 4;

/// Evaluate every condition on a grant.
///
/// Returns `Ok(())` when the grant is effective for this request, or the
/// refusal describing the first condition that failed.
///
/// `now_secs` is supplied by the caller rather than read here so a request's
/// scope enrichment, quota reads, and condition evaluation all agree on one
/// clock — the same reason `enrich_auth_context_with_scopes` takes it.
///
/// `client_ip` is `None` when the transport had no usable peer address; see
/// the module docs for why that fails closed rather than skipping the check.
pub fn evaluate_conditions(
    conditions: &[GrantCondition],
    auth: &AuthContext,
    client_ip: Option<&str>,
    now_secs: u64,
) -> Result<(), RiskRefusal> {
    for condition in conditions {
        match condition {
            GrantCondition::Temporal {
                start_hour,
                end_hour,
                days,
            } => {
                let (hour, weekday) = time_components(now_secs);
                if !hour_in_window(hour, *start_hour, *end_hour) {
                    return Err(RiskRefusal {
                        resource: "outside the grant's permitted hours".into(),
                        audit_detail: format!(
                            "temporal condition: hour {hour} is outside {start_hour}..{end_hour}"
                        ),
                    });
                }
                if !days.is_empty() && !days.contains(&weekday) {
                    return Err(RiskRefusal {
                        resource: "outside the grant's permitted days".into(),
                        audit_detail: format!(
                            "temporal condition: weekday {weekday} is not among {days:?}"
                        ),
                    });
                }
            }

            GrantCondition::RequireMfa => {
                if !flag_is_set(auth, "mfa_verified") {
                    return Err(RiskRefusal {
                        resource: STEP_UP_REQUIRED.into(),
                        audit_detail: format!(
                            "scope grant requires MFA but session '{}' carries no \
                             mfa_verified marker",
                            auth.session_id
                        ),
                    });
                }
            }

            GrantCondition::RequireIp { allowed_cidrs } => {
                let Some(ip) = client_ip else {
                    return Err(RiskRefusal {
                        resource: "client address unavailable for an IP-restricted grant".into(),
                        audit_detail: format!(
                            "scope grant is restricted to {allowed_cidrs:?} but no client \
                             address was available for session '{}' — dropping the grant \
                             rather than applying it unchecked",
                            auth.session_id
                        ),
                    });
                };
                if check_ip_against_cidrs(ip, allowed_cidrs).is_none() {
                    return Err(RiskRefusal {
                        resource: "request address is outside the grant's permitted networks"
                            .into(),
                        audit_detail: format!(
                            "client address {ip} matches none of {allowed_cidrs:?}"
                        ),
                    });
                }
            }

            GrantCondition::StepUpAuth { max_age_secs } => {
                let age = match auth.auth_time {
                    Some(auth_time) if auth_time > 0 => now_secs.saturating_sub(auth_time),
                    _ => {
                        return Err(RiskRefusal {
                            resource: STEP_UP_REQUIRED.into(),
                            audit_detail: format!(
                                "scope grant requires authentication within {max_age_secs}s but \
                                 session '{}' carries no authentication time",
                                auth.session_id
                            ),
                        });
                    }
                };
                if age > *max_age_secs {
                    return Err(RiskRefusal {
                        resource: STEP_UP_REQUIRED.into(),
                        audit_detail: format!(
                            "last authentication was {age}s ago, beyond the grant's \
                             {max_age_secs}s step-up window"
                        ),
                    });
                }
            }

            GrantCondition::RequireDeviceTrust => {
                if !flag_is_set(auth, "device_trusted") {
                    return Err(RiskRefusal {
                        resource: "trusted device required".into(),
                        audit_detail: format!(
                            "scope grant requires device trust but session '{}' carries no \
                             device_trusted marker",
                            auth.session_id
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

/// True when `auth.metadata[key]` is an affirmative boolean flag. An absent
/// key is false — the marker has to be present and affirmative. Delegates to
/// `AuthContext::metadata_flag` so this idiom has exactly one implementation
/// shared with the risk scorer's `device_trusted` check, rather than two
/// copies that could silently drift.
fn flag_is_set(auth: &AuthContext, key: &str) -> bool {
    auth.metadata_flag(key)
}

/// Whether `hour` falls in `[start, end)`, wrapping past midnight when the
/// window's end is at or before its start (`22..6` = 22:00 through 05:59).
fn hour_in_window(hour: u8, start_hour: u8, end_hour: u8) -> bool {
    if start_hour < end_hour {
        hour >= start_hour && hour < end_hour
    } else {
        hour >= start_hour || hour < end_hour
    }
}

/// UTC hour (0-23) and weekday (0=Sunday) for a Unix timestamp.
fn time_components(now_secs: u64) -> (u8, u8) {
    let hour = ((now_secs % SECS_PER_DAY) / SECS_PER_HOUR) as u8;
    let weekday = ((now_secs / SECS_PER_DAY + EPOCH_WEEKDAY) % 7) as u8;
    (hour, weekday)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;

    /// A Thursday (the Unix epoch's weekday) at 12:00 UTC.
    const THURSDAY_NOON: u64 = 12 * SECS_PER_HOUR;

    fn auth() -> AuthContext {
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
        );
        AuthContext::from_identity(&identity, "s_cond".into())
    }

    #[test]
    fn temporal_window_admits_inside_and_refuses_outside() {
        let cond = GrantCondition::Temporal {
            start_hour: 9,
            end_hour: 17,
            days: Vec::new(),
        };
        let ctx = auth();
        assert!(
            evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, THURSDAY_NOON).is_ok()
        );
        let after_hours = 20 * SECS_PER_HOUR;
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, after_hours).is_err());
    }

    #[test]
    fn temporal_window_wraps_past_midnight() {
        let cond = GrantCondition::Temporal {
            start_hour: 22,
            end_hour: 6,
            days: Vec::new(),
        };
        let ctx = auth();
        let at_23 = 23 * SECS_PER_HOUR;
        let at_03 = 3 * SECS_PER_HOUR;
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, at_23).is_ok());
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, at_03).is_ok());
        assert!(
            evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, THURSDAY_NOON).is_err()
        );
    }

    #[test]
    fn temporal_day_selector_refuses_other_days() {
        // The epoch day is a Thursday (weekday 4).
        let cond = GrantCondition::Temporal {
            start_hour: 0,
            end_hour: 24,
            days: vec![1, 2, 3],
        };
        assert!(evaluate_conditions(&[cond], &auth(), None, THURSDAY_NOON).is_err());
    }

    #[test]
    fn require_ip_without_a_client_address_fails_closed() {
        let cond = GrantCondition::RequireIp {
            allowed_cidrs: vec!["10.0.0.0/8".into()],
        };
        let refusal = evaluate_conditions(&[cond], &auth(), None, THURSDAY_NOON)
            .expect_err("a grant that cannot be checked must not apply");
        assert_eq!(
            refusal.resource,
            "client address unavailable for an IP-restricted grant"
        );
    }

    #[test]
    fn require_ip_matches_only_the_configured_networks() {
        let cond = GrantCondition::RequireIp {
            allowed_cidrs: vec!["10.0.0.0/8".into()],
        };
        let ctx = auth();
        assert!(
            evaluate_conditions(
                std::slice::from_ref(&cond),
                &ctx,
                Some("10.4.5.6"),
                THURSDAY_NOON
            )
            .is_ok()
        );
        assert!(
            evaluate_conditions(
                std::slice::from_ref(&cond),
                &ctx,
                Some("192.168.1.1"),
                THURSDAY_NOON
            )
            .is_err()
        );
    }

    #[test]
    fn require_mfa_uses_the_risk_step_up_refusal_shape() {
        let refusal = evaluate_conditions(&[GrantCondition::RequireMfa], &auth(), None, 0)
            .expect_err("no MFA marker must refuse");
        assert_eq!(refusal.resource, STEP_UP_REQUIRED);

        let mut verified = auth();
        verified
            .metadata
            .insert("mfa_verified".into(), "true".into());
        assert!(evaluate_conditions(&[GrantCondition::RequireMfa], &verified, None, 0).is_ok());
    }

    #[test]
    fn step_up_without_an_auth_time_uses_the_same_refusal_shape() {
        let cond = GrantCondition::StepUpAuth { max_age_secs: 900 };
        let mut ctx = auth();
        ctx.auth_time = None;
        let refusal = evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, 10_000)
            .expect_err("an unknown authentication time must refuse");
        assert_eq!(refusal.resource, STEP_UP_REQUIRED);

        ctx.auth_time = Some(9_500);
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, 10_000).is_ok());
        ctx.auth_time = Some(1_000);
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &ctx, None, 10_000).is_err());
    }

    /// `flag_is_set` accepts both a real `Value::Bool(true)` (what a provider
    /// issuing a proper JSON boolean claim now produces) and the legacy
    /// `Value::String("true")` every existing deployment still sends, and
    /// rejects `Value::Bool(false)`, `Value::String("false")`, and a missing
    /// key — accepting only one typed form would break the other.
    #[test]
    fn flag_is_set_accepts_both_bool_and_string_true_and_rejects_everything_else() {
        let mut ctx = auth();
        assert!(!flag_is_set(&ctx, "verified"), "missing key must be false");

        ctx.metadata
            .insert("verified".into(), nodedb_types::Value::Bool(true));
        assert!(flag_is_set(&ctx, "verified"), "Value::Bool(true) must pass");

        ctx.metadata
            .insert("verified".into(), nodedb_types::Value::Bool(false));
        assert!(
            !flag_is_set(&ctx, "verified"),
            "Value::Bool(false) must fail"
        );

        ctx.metadata.insert("verified".into(), "true".into());
        assert!(
            flag_is_set(&ctx, "verified"),
            "legacy Value::String(\"true\") must pass"
        );

        ctx.metadata.insert("verified".into(), "false".into());
        assert!(
            !flag_is_set(&ctx, "verified"),
            "Value::String(\"false\") must fail"
        );
    }

    #[test]
    fn device_trust_requires_an_affirmative_marker() {
        let mut ctx = auth();
        assert!(evaluate_conditions(&[GrantCondition::RequireDeviceTrust], &ctx, None, 0).is_err());
        ctx.metadata.insert("device_trusted".into(), "false".into());
        assert!(evaluate_conditions(&[GrantCondition::RequireDeviceTrust], &ctx, None, 0).is_err());
        ctx.metadata.insert("device_trusted".into(), "true".into());
        assert!(evaluate_conditions(&[GrantCondition::RequireDeviceTrust], &ctx, None, 0).is_ok());
    }
}
