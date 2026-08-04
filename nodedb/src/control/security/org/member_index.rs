// SPDX-License-Identifier: BUSL-1.1

//! In-memory org membership index: forward map (`org:user` → record) plus a
//! reverse map (`user` → set of org ids) maintained together so
//! `orgs_for_user` never has to scan every membership server-wide.
//!
//! Both maps live behind a single `RwLock` (see `OrgStore::members`) so a
//! concurrent reader can never observe one map updated and the other stale.
//! `MemberIndex` itself has no locking — it is the guarded value, not the
//! guard.

use std::collections::{HashMap, HashSet};

use crate::control::security::catalog::StoredOrgMember;

/// In-memory org membership record.
#[derive(Debug, Clone)]
pub struct OrgMemberRecord {
    pub auth_user_id: String,
    pub org_id: String,
    pub role: String,
    pub joined_at: u64,
}

impl OrgMemberRecord {
    pub(super) fn from_stored(s: &StoredOrgMember) -> Self {
        Self {
            auth_user_id: s.auth_user_id.clone(),
            org_id: s.org_id.clone(),
            role: s.role.clone(),
            joined_at: s.joined_at,
        }
    }

    pub(super) fn to_stored(&self) -> StoredOrgMember {
        StoredOrgMember {
            auth_user_id: self.auth_user_id.clone(),
            org_id: self.org_id.clone(),
            role: self.role.clone(),
            joined_at: self.joined_at,
        }
    }
}

/// Forward + reverse membership index, kept consistent as one unit.
#[derive(Debug, Default)]
pub struct MemberIndex {
    /// Key: `"{org_id}:{user_id}"` → membership.
    members: HashMap<String, OrgMemberRecord>,
    /// user_id → set of org_ids the user belongs to. Derived state, rebuilt
    /// from `members` on every load; never persisted on its own.
    by_user: HashMap<String, HashSet<String>>,
}

impl MemberIndex {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            by_user: HashMap::new(),
        }
    }

    pub fn member_key(org_id: &str, user_id: &str) -> String {
        format!("{org_id}:{user_id}")
    }

    /// Insert (or overwrite) a membership, maintaining both maps.
    pub fn insert(&mut self, record: OrgMemberRecord) {
        let key = Self::member_key(&record.org_id, &record.auth_user_id);
        self.by_user
            .entry(record.auth_user_id.clone())
            .or_default()
            .insert(record.org_id.clone());
        self.members.insert(key, record);
    }

    /// Remove a single membership, maintaining both maps. Returns `true` if
    /// a membership existed.
    pub fn remove(&mut self, org_id: &str, user_id: &str) -> bool {
        let key = Self::member_key(org_id, user_id);
        let removed = self.members.remove(&key).is_some();
        if removed && let Some(orgs) = self.by_user.get_mut(user_id) {
            orgs.remove(org_id);
            if orgs.is_empty() {
                self.by_user.remove(user_id);
            }
        }
        removed
    }

    /// Remove every membership belonging to `org_id` (e.g. when the org
    /// itself is dropped), maintaining both maps.
    pub fn remove_org(&mut self, org_id: &str) {
        let prefix = format!("{org_id}:");
        let doomed_users: Vec<String> = self
            .members
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.auth_user_id.clone())
            .collect();
        self.members.retain(|k, _| !k.starts_with(&prefix));
        for user_id in doomed_users {
            if let Some(orgs) = self.by_user.get_mut(&user_id) {
                orgs.remove(org_id);
                if orgs.is_empty() {
                    self.by_user.remove(&user_id);
                }
            }
        }
    }

    /// List members of an organization.
    pub fn members_of(&self, org_id: &str) -> Vec<OrgMemberRecord> {
        let prefix = format!("{org_id}:");
        self.members
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// List all orgs a user belongs to — O(orgs the user is in), via the
    /// reverse index, never a scan of all memberships.
    pub fn orgs_for_user(&self, user_id: &str) -> Vec<String> {
        self.by_user
            .get(user_id)
            .map(|orgs| orgs.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }
}
