// SPDX-License-Identifier: BUSL-1.1

//! [`GrantCondition`] — the conditions a scope grant may carry.
//!
//! A condition narrows *when* and *from where* a grant is effective. The
//! grant itself still exists (and still shows up in `SHOW SCOPE GRANTS`);
//! the condition decides whether it contributes its scope to the request
//! being served. Evaluation lives in [`super::evaluate`], the SQL grammar in
//! [`super::parse`].
//!
//! The variants are serde-only shapes (a struct variant with a `Vec<String>`
//! payload does not fit zerompk's derive), so persistence encodes the whole
//! list to a JSON string — see `StoredScopeGrant::conditions_json`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A condition that must be satisfied for a scope grant to be effective.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantCondition {
    /// Temporal window: the grant is only effective during these hours, and
    /// optionally only on these days.
    ///
    /// `WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS`
    Temporal {
        /// Start hour (0-23), inclusive.
        start_hour: u8,
        /// End hour (1-24), exclusive. When `end_hour <= start_hour` the
        /// window wraps past midnight (`22` → `6` means 22:00-06:00).
        end_hour: u8,
        /// Days of week (0=Sunday .. 6=Saturday). Empty = every day.
        days: Vec<u8>,
    },

    /// The request must carry a recent MFA verification.
    ///
    /// `REQUIRE MFA`
    RequireMfa,

    /// The request must originate from one of these CIDR ranges.
    ///
    /// `REQUIRE IP IN ('10.0.0.0/8', '192.168.0.0/16')`
    RequireIp {
        /// Allowed CIDR ranges, validated at parse time.
        allowed_cidrs: Vec<String>,
    },

    /// The principal must have authenticated within `max_age_secs`.
    ///
    /// `REQUIRE STEP_UP 900`
    StepUpAuth {
        /// Maximum seconds since the last authentication.
        max_age_secs: u64,
    },

    /// The request must come from a device marked trusted.
    ///
    /// `REQUIRE DEVICE_TRUST`
    RequireDeviceTrust,
}

/// Default step-up window when `REQUIRE STEP_UP` omits its interval.
pub const DEFAULT_STEP_UP_SECS: u64 = 900;

/// Weekday selector for `ON WEEKDAYS` (Monday..Friday).
pub const WEEKDAYS: [u8; 5] = [1, 2, 3, 4, 5];

/// Weekend selector for `ON WEEKENDS` (Sunday, Saturday).
pub const WEEKENDS: [u8; 2] = [0, 6];

impl fmt::Display for GrantCondition {
    /// Render back into the SQL clause that produced it, so `SHOW SCOPE
    /// GRANTS` shows an operator the exact text to re-issue.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Temporal {
                start_hour,
                end_hour,
                days,
            } => {
                write!(
                    f,
                    "WHEN BETWEEN '{start_hour:02}:00' AND '{end_hour:02}:00'"
                )?;
                if days.is_empty() {
                    return Ok(());
                }
                if days.as_slice() == WEEKDAYS.as_slice() {
                    write!(f, " ON WEEKDAYS")
                } else if days.as_slice() == WEEKENDS.as_slice() {
                    write!(f, " ON WEEKENDS")
                } else {
                    let rendered: Vec<String> = days.iter().map(u8::to_string).collect();
                    write!(f, " ON DAYS ({})", rendered.join(","))
                }
            }
            Self::RequireMfa => write!(f, "REQUIRE MFA"),
            Self::RequireIp { allowed_cidrs } => {
                let quoted: Vec<String> = allowed_cidrs.iter().map(|c| format!("'{c}'")).collect();
                write!(f, "REQUIRE IP IN ({})", quoted.join(", "))
            }
            Self::StepUpAuth { max_age_secs } => write!(f, "REQUIRE STEP_UP {max_age_secs}"),
            Self::RequireDeviceTrust => write!(f, "REQUIRE DEVICE_TRUST"),
        }
    }
}

/// Render a condition list for display. Empty lists render as `-` so a grant
/// with no conditions is visibly unconditional rather than blank.
pub fn render_conditions(conditions: &[GrantCondition]) -> String {
    if conditions.is_empty() {
        return "-".to_string();
    }
    conditions
        .iter()
        .map(GrantCondition::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_renders_its_day_selector() {
        let cond = GrantCondition::Temporal {
            start_hour: 9,
            end_hour: 17,
            days: WEEKDAYS.to_vec(),
        };
        assert_eq!(
            cond.to_string(),
            "WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS"
        );
    }

    #[test]
    fn unconditional_grant_renders_as_dash() {
        assert_eq!(render_conditions(&[]), "-");
    }

    #[test]
    fn multiple_conditions_are_joined() {
        let rendered = render_conditions(&[
            GrantCondition::RequireMfa,
            GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            },
        ]);
        assert_eq!(rendered, "REQUIRE MFA; REQUIRE IP IN ('10.0.0.0/8')");
    }
}
