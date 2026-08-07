// SPDX-License-Identifier: BUSL-1.1

//! In-memory scope grant record, its time-bound status, and the parameter
//! bundle a `GRANT SCOPE` statement carries.

use crate::control::security::catalog::StoredScopeGrant;
use crate::control::security::conditional::GrantCondition;
use crate::control::security::time::now_secs;

/// In-memory scope grant record with time-bound support.
#[derive(Debug, Clone)]
pub struct ScopeGrant {
    pub scope_name: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub granted_by: String,
    pub granted_at: u64,
    /// Unix timestamp when this grant expires. 0 = no expiry (permanent).
    pub expires_at: u64,
    /// Grace period in seconds after expiry before hard cutoff.
    pub grace_period_secs: u64,
    /// Action on expiry: "revoke_all", "grant:<scope_name>", or "" (just expire).
    pub on_expire_action: String,
    /// Conditions that must hold for this grant to contribute its scope to a
    /// request. Empty = unconditional.
    ///
    /// Distinct from `expires_at` / `grace_period_secs`, which retire the
    /// whole grant on a wall clock: a conditional grant stays granted and
    /// simply does not apply to requests that fail its conditions.
    pub conditions: Vec<GrantCondition>,
}

/// Status of a time-bound scope grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeStatus {
    /// Grant is active (not expired, or no expiry set).
    Active,
    /// Grant is in grace period (expired but within grace window).
    Grace,
    /// Grant is fully expired (past grace period).
    Expired,
    /// Grant does not exist for this grantee.
    None,
}

impl std::fmt::Display for ScopeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Grace => write!(f, "grace"),
            Self::Expired => write!(f, "expired"),
            Self::None => write!(f, "none"),
        }
    }
}

impl ScopeGrant {
    /// Check the time-bound status of this grant.
    pub fn status(&self) -> ScopeStatus {
        if self.expires_at == 0 {
            return ScopeStatus::Active; // No expiry = permanent.
        }
        let now = now_secs();
        if now < self.expires_at {
            ScopeStatus::Active
        } else if now < self.expires_at + self.grace_period_secs {
            ScopeStatus::Grace
        } else {
            ScopeStatus::Expired
        }
    }

    /// Check if this grant is still effective (active or in grace period).
    pub fn is_effective(&self) -> bool {
        matches!(self.status(), ScopeStatus::Active | ScopeStatus::Grace)
    }

    /// Rebuild a grant from its catalog record.
    ///
    /// Fails when the stored condition payload does not decode into known
    /// [`GrantCondition`]s — a grant whose conditions cannot be read must
    /// not be loaded as if it had none, so the caller drops it entirely.
    pub(super) fn from_stored(s: &StoredScopeGrant) -> crate::Result<Self> {
        let conditions = if s.conditions_json.is_empty() {
            Vec::new()
        } else {
            sonic_rs::from_str(&s.conditions_json).map_err(|e| crate::Error::BadRequest {
                detail: format!(
                    "scope grant '{}' for {} '{}' has undecodable conditions: {e}",
                    s.scope_name, s.grantee_type, s.grantee_id
                ),
            })?
        };
        Ok(Self {
            scope_name: s.scope_name.clone(),
            grantee_type: s.grantee_type.clone(),
            grantee_id: s.grantee_id.clone(),
            granted_by: s.granted_by.clone(),
            granted_at: s.granted_at,
            expires_at: s.expires_at,
            grace_period_secs: s.grace_period_secs,
            on_expire_action: s.on_expire_action.clone(),
            conditions,
        })
    }

    pub(super) fn to_stored(&self) -> crate::Result<StoredScopeGrant> {
        let conditions_json = if self.conditions.is_empty() {
            String::new()
        } else {
            sonic_rs::to_string(&self.conditions).map_err(|e| crate::Error::BadRequest {
                detail: format!("cannot serialize scope grant conditions: {e}"),
            })?
        };
        Ok(StoredScopeGrant {
            scope_name: self.scope_name.clone(),
            grantee_type: self.grantee_type.clone(),
            grantee_id: self.grantee_id.clone(),
            granted_by: self.granted_by.clone(),
            granted_at: self.granted_at,
            expires_at: self.expires_at,
            grace_period_secs: self.grace_period_secs,
            on_expire_action: self.on_expire_action.clone(),
            conditions_json,
        })
    }
}

/// Parameters for a `GRANT SCOPE` statement, consumed by
/// [`super::ScopeGrantStore::prepare_grant`].
pub struct ScopeGrantParams<'a> {
    pub scope_name: &'a str,
    pub grantee_type: &'a str,
    pub grantee_id: &'a str,
    pub granted_by: &'a str,
    /// 0 means permanent (no expiry).
    pub expires_at: u64,
    /// Seconds after expiry before hard cutoff.
    pub grace_period_secs: u64,
    /// "revoke_all", "grant:<scope>", or "" (just expire).
    pub on_expire_action: &'a str,
    /// Conditions parsed from the statement's `WHEN` / `REQUIRE` clauses.
    /// Empty = unconditional.
    pub conditions: Vec<GrantCondition>,
}

/// Key under which a grant is filed, in both the in-memory map and the
/// `SCOPE_GRANTS` redb table — the two must agree so a replicated delete
/// removes the same row the replicated put wrote.
pub(super) fn grant_key(scope: &str, grantee_type: &str, grantee_id: &str) -> String {
    format!("{scope}:{grantee_type}:{grantee_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(conditions_json: &str) -> StoredScopeGrant {
        StoredScopeGrant {
            scope_name: "pro:all".into(),
            grantee_type: "user".into(),
            grantee_id: "u1".into(),
            granted_by: "admin".into(),
            granted_at: 1_000,
            expires_at: 0,
            grace_period_secs: 0,
            on_expire_action: String::new(),
            conditions_json: conditions_json.into(),
        }
    }

    /// The catalog record is what survives a restart and what travels
    /// through raft, so conditions have to make the round trip unchanged.
    #[test]
    fn conditions_round_trip_through_the_catalog_record() {
        let conditions = vec![
            GrantCondition::Temporal {
                start_hour: 9,
                end_hour: 17,
                days: vec![1, 2, 3, 4, 5],
            },
            GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            },
        ];
        let grant = ScopeGrant {
            scope_name: "pro:all".into(),
            grantee_type: "user".into(),
            grantee_id: "u1".into(),
            granted_by: "admin".into(),
            granted_at: 1_000,
            expires_at: 0,
            grace_period_secs: 0,
            on_expire_action: String::new(),
            conditions: conditions.clone(),
        };

        let restored =
            ScopeGrant::from_stored(&grant.to_stored().expect("serialize")).expect("decode");

        assert_eq!(restored.conditions, conditions);
    }

    #[test]
    fn an_unconditional_grant_stores_no_condition_payload() {
        let grant = ScopeGrant::from_stored(&stored("")).expect("decode");
        assert!(grant.conditions.is_empty());
        assert!(
            grant
                .to_stored()
                .expect("serialize")
                .conditions_json
                .is_empty()
        );
    }

    /// Fail closed on an unreadable restriction: a condition payload naming
    /// something this build does not understand must not decode into an
    /// unconditional grant. `ScopeGrantStore::open` drops such a grant.
    #[test]
    fn undecodable_conditions_are_rejected_rather_than_ignored() {
        assert!(ScopeGrant::from_stored(&stored("[{\"RequireTelepathy\":{}}]")).is_err());
        assert!(ScopeGrant::from_stored(&stored("not json at all")).is_err());
    }
}
