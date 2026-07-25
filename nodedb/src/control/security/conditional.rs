// SPDX-License-Identifier: BUSL-1.1

//! Conditional grant evaluation: temporal windows, MFA, IP, device trust.
//!
//! Conditions are attached to scope grants via `GRANT SCOPE ... WHEN/REQUIRE`.
//! Evaluated at query time against the `AuthContext`.

use serde::{Deserialize, Serialize};

use super::auth_context::AuthContext;

/// A condition that must be satisfied for a scope grant to be effective.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GrantCondition {
    /// Temporal window: grant is only active during specified hours/days.
    /// `WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS`
    Temporal {
        /// Start hour (0-23).
        start_hour: u8,
        /// End hour (0-23, exclusive).
        end_hour: u8,
        /// Days of week (0=Sunday, 1=Monday, ..., 6=Saturday). Empty = all days.
        days: Vec<u8>,
    },

    /// MFA requirement: grant requires recent MFA verification.
    /// `REQUIRE MFA`
    RequireMfa,

    /// IP requirement: grant only effective from specified IP ranges.
    /// `REQUIRE IP IN ('10.0.0.0/8', '192.168.0.0/16')`
    RequireIp {
        /// Allowed CIDR ranges.
        allowed_cidrs: Vec<String>,
    },

    /// Step-up auth: grant requires recent authentication.
    /// `$auth.auth_time > (now() - INTERVAL '15 minutes')`
    StepUpAuth {
        /// Maximum seconds since last authentication.
        max_age_secs: u64,
    },

    /// Device trust requirement.
    /// `REQUIRE $auth.metadata.device_trusted = 'true'`
    RequireDeviceTrust,
}

/// Evaluate whether all conditions on a grant are satisfied.
///
/// Returns `Ok(())` if all conditions pass, `Err(reason)` if any fails.
pub fn evaluate_conditions(
    conditions: &[GrantCondition],
    auth: &AuthContext,
    client_ip: &str,
) -> crate::Result<()> {
    for cond in conditions {
        match cond {
            GrantCondition::Temporal {
                start_hour,
                end_hour,
                days,
            } => {
                let now = current_time_components();
                let hour = now.0;
                let weekday = now.1;

                if hour < *start_hour || hour >= *end_hour {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: format!(
                            "temporal condition: current hour {hour} not in {start_hour}..{end_hour}"
                        ),
                    });
                }
                if !days.is_empty() && !days.contains(&weekday) {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: format!("temporal condition: day {weekday} not in allowed days"),
                    });
                }
            }

            GrantCondition::RequireMfa => {
                // MFA is indicated by $auth.metadata.mfa_verified = "true".
                let mfa_ok = auth
                    .metadata
                    .get("mfa_verified")
                    .is_some_and(|v| v == "true");
                if !mfa_ok {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: "MFA verification required".to_string(),
                    });
                }
            }

            GrantCondition::RequireIp { allowed_cidrs } => {
                let ip_ok = super::blacklist::ip::check_ip_against_cidrs(client_ip, allowed_cidrs)
                    .is_some();
                if !ip_ok {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: format!("IP {client_ip} not in allowed ranges"),
                    });
                }
            }

            GrantCondition::StepUpAuth { max_age_secs } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let auth_time = auth.auth_time.unwrap_or(0);
                if auth_time == 0 || now.saturating_sub(auth_time) > *max_age_secs {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: format!(
                            "step-up auth required: last auth {}s ago, max {max_age_secs}s",
                            now.saturating_sub(auth_time)
                        ),
                    });
                }
            }

            GrantCondition::RequireDeviceTrust => {
                let trusted = auth
                    .metadata
                    .get("device_trusted")
                    .is_some_and(|v| v == "true");
                if !trusted {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: auth.tenant_id,
                        resource: "device trust required".to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Parse condition clauses from DDL parts.
///
/// Recognizes:
/// - `WHEN BETWEEN '<start>' AND '<end>' ON WEEKDAYS`
/// - `REQUIRE MFA`
/// - `REQUIRE IP IN ('<cidr>', ...)`
/// - `REQUIRE STEP_UP <seconds>`
/// - `REQUIRE DEVICE_TRUST`
pub fn parse_conditions(parts: &[&str]) -> Vec<GrantCondition> {
    let mut conditions = Vec::new();
    let mut i = 0;

    while i < parts.len() {
        let upper = parts[i].to_uppercase();

        if upper == "WHEN" && i + 4 < parts.len() && parts[i + 1].to_uppercase() == "BETWEEN" {
            let start = parts[i + 2].trim_matches('\'');
            let end = parts[i + 4].trim_matches('\'');
            let start_hour = parse_hour(start);
            let end_hour = parse_hour(end);

            // Check for ON WEEKDAYS.
            let days = if i + 6 < parts.len() && parts[i + 5].to_uppercase() == "ON" {
                let day_str = parts[i + 6].to_uppercase();
                match day_str.as_str() {
                    "WEEKDAYS" => vec![1, 2, 3, 4, 5],
                    "WEEKENDS" => vec![0, 6],
                    "ALL" => vec![],
                    _ => vec![],
                }
            } else {
                vec![]
            };

            conditions.push(GrantCondition::Temporal {
                start_hour,
                end_hour,
                days,
            });
            i += 7;
            continue;
        }

        if upper == "REQUIRE" && i + 1 < parts.len() {
            let req = parts[i + 1].to_uppercase();
            match req.as_str() {
                "MFA" => {
                    conditions.push(GrantCondition::RequireMfa);
                    i += 2;
                }
                "IP" => {
                    // REQUIRE IP IN ('cidr1', 'cidr2')
                    let cidrs: Vec<String> = parts[i + 3..]
                        .iter()
                        .take_while(|p| !p.starts_with(')'))
                        .map(|s| {
                            s.trim_matches('\'')
                                .trim_matches('(')
                                .trim_matches(')')
                                .trim_end_matches(',')
                                .to_string()
                        })
                        .filter(|s| !s.is_empty() && s.to_uppercase() != "IN")
                        .collect();
                    conditions.push(GrantCondition::RequireIp {
                        allowed_cidrs: cidrs,
                    });
                    i += 4;
                }
                "STEP_UP" => {
                    let secs = parts
                        .get(i + 2)
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(900); // Default 15 min.
                    conditions.push(GrantCondition::StepUpAuth { max_age_secs: secs });
                    i += 3;
                }
                "DEVICE_TRUST" => {
                    conditions.push(GrantCondition::RequireDeviceTrust);
                    i += 2;
                }
                _ => {
                    i += 1;
                }
            }
            continue;
        }

        i += 1;
    }

    conditions
}

/// Get current hour (0-23) and weekday (0=Sunday).
fn current_time_components() -> (u8, u8) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Approximate: UTC hour and day-of-week from epoch.
    let hour = ((secs % 86_400) / 3600) as u8;
    // Epoch was Thursday (4). Days since epoch mod 7.
    let day = ((secs / 86_400 + 4) % 7) as u8;
    (hour, day)
}

/// Parse "HH:MM" to hour.
fn parse_hour(s: &str) -> u8 {
    s.split(':')
        .next()
        .and_then(|h| h.parse::<u8>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_auth() -> AuthContext {
        let identity = super::super::identity::AuthenticatedIdentity::new_regular(
            42,
            "alice",
            crate::types::TenantId::new(1),
            super::super::identity::AuthMethod::ApiKey,
            vec![super::super::identity::Role::ReadWrite],
            None,
            super::super::identity::AuthenticatedIdentity::default_database_set(false),
        );
        let mut auth = AuthContext::from_identity(&identity, "test".into());
        auth.auth_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        auth
    }

    #[test]
    fn mfa_required_passes() {
        let mut auth = test_auth();
        auth.metadata.insert("mfa_verified".into(), "true".into());
        assert!(evaluate_conditions(&[GrantCondition::RequireMfa], &auth, "10.0.0.1").is_ok());
    }

    #[test]
    fn mfa_required_fails() {
        let auth = test_auth();
        assert!(evaluate_conditions(&[GrantCondition::RequireMfa], &auth, "10.0.0.1").is_err());
    }

    #[test]
    fn ip_requirement() {
        let auth = test_auth();
        let cond = GrantCondition::RequireIp {
            allowed_cidrs: vec!["10.0.0.0/8".into()],
        };
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &auth, "10.0.0.5").is_ok());
        assert!(evaluate_conditions(std::slice::from_ref(&cond), &auth, "192.168.1.1").is_err());
    }

    #[test]
    fn step_up_auth() {
        let auth = test_auth();
        // auth_time is now → should pass with 900s window.
        let cond = GrantCondition::StepUpAuth { max_age_secs: 900 };
        assert!(evaluate_conditions(&[cond], &auth, "10.0.0.1").is_ok());
    }

    #[test]
    fn device_trust() {
        let mut auth = test_auth();
        auth.metadata.insert("device_trusted".into(), "true".into());
        assert!(
            evaluate_conditions(&[GrantCondition::RequireDeviceTrust], &auth, "10.0.0.1").is_ok()
        );
    }

    #[test]
    fn parse_mfa_condition() {
        let parts = vec!["REQUIRE", "MFA"];
        let conditions = parse_conditions(&parts);
        assert_eq!(conditions.len(), 1);
        assert!(matches!(conditions[0], GrantCondition::RequireMfa));
    }
}
