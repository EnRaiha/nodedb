// SPDX-License-Identifier: BUSL-1.1

//! Organization store: in-memory cache + redb persistence.
//!
//! Manages `_system.orgs` and `_system.org_members` records.
//! Supports JIT org creation from JWT `$auth.org_id` claim.
//! Org status overrides member status (suspended org = all members blocked).

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::info;

use super::member_index::MemberIndex;
use crate::control::security::catalog::{StoredOrg, SystemCatalog};
use crate::control::security::time::now_secs;

// `OrgMemberRecord` is defined in the `pub(crate)` `member_index` module but
// appears in `OrgStore`'s public API (`members_of`) — re-export it here so
// it is reachable via `org::store::OrgMemberRecord`.
pub use super::member_index::OrgMemberRecord;

/// In-memory organization record.
#[derive(Debug, Clone)]
pub struct OrgRecord {
    pub org_id: String,
    pub name: String,
    pub tenant_id: u64,
    pub status: String,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
    /// Org-level rate limit QPS (0 = use default tier).
    pub rate_limit_qps: u64,
    /// Org-level max storage bytes (0 = unlimited).
    pub quota_max_storage: u64,
    /// Org-level max members (0 = unlimited).
    pub quota_max_members: u32,
}

impl OrgRecord {
    fn from_stored(s: &StoredOrg) -> Self {
        Self {
            org_id: s.org_id.clone(),
            name: s.name.clone(),
            tenant_id: s.tenant_id,
            status: s.status.clone(),
            created_at: s.created_at,
            metadata: s.metadata.clone(),
            rate_limit_qps: 0,
            quota_max_storage: 0,
            quota_max_members: 0,
        }
    }

    fn to_stored(&self) -> StoredOrg {
        StoredOrg {
            org_id: self.org_id.clone(),
            name: self.name.clone(),
            tenant_id: self.tenant_id,
            status: self.status.clone(),
            created_at: self.created_at,
            metadata: self.metadata.clone(),
        }
    }
}

/// Thread-safe organization store.
pub struct OrgStore {
    orgs: RwLock<HashMap<String, OrgRecord>>,
    /// Forward (`org:user` → membership) and reverse (`user` → org ids)
    /// membership index, behind one lock so the two views can never be
    /// observed out of sync by a concurrent reader.
    members: RwLock<MemberIndex>,
    catalog: Option<SystemCatalog>,
}

impl OrgStore {
    pub fn new() -> Self {
        Self {
            orgs: RwLock::new(HashMap::new()),
            members: RwLock::new(MemberIndex::new()),
            catalog: None,
        }
    }

    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored_orgs = catalog.load_all_orgs()?;
        let mut orgs = HashMap::with_capacity(stored_orgs.len());
        for s in &stored_orgs {
            orgs.insert(s.org_id.clone(), OrgRecord::from_stored(s));
        }
        // Load members for all orgs on startup. This is the ONLY place the
        // in-memory member index is populated from durable state, so it is
        // also the only place the reverse (user -> orgs) index gets built —
        // every insert below goes through `MemberIndex::insert`, which keeps
        // both maps in lockstep. If this rebuild is ever skipped, the
        // reverse index is silently empty after a restart even though
        // `members` looks fully populated.
        let mut members = MemberIndex::new();
        for org_id in orgs.keys() {
            if let Ok(stored_members) = catalog.load_members_for_org(org_id) {
                for sm in &stored_members {
                    members.insert(OrgMemberRecord::from_stored(sm));
                }
            }
        }
        if !orgs.is_empty() {
            info!(
                orgs = orgs.len(),
                members = members.len(),
                "orgs loaded from catalog"
            );
        }
        Ok(Self {
            orgs: RwLock::new(orgs),
            members: RwLock::new(members),
            catalog: Some(catalog),
        })
    }

    /// Create a new organization.
    pub fn create_org(&self, org_id: &str, name: &str, tenant_id: u64) -> crate::Result<()> {
        let now = now_secs();
        let record = OrgRecord {
            org_id: org_id.into(),
            name: name.into(),
            tenant_id,
            status: "active".into(),
            created_at: now,
            metadata: HashMap::new(),
            rate_limit_qps: 0,
            quota_max_storage: 0,
            quota_max_members: 0,
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_org(&record.to_stored())?;
        }

        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        orgs.insert(org_id.into(), record);
        info!(org_id = %org_id, name = %name, "org created");
        Ok(())
    }

    /// Get an organization by ID.
    pub fn get(&self, org_id: &str) -> Option<OrgRecord> {
        let orgs = self.orgs.read().unwrap_or_else(|p| p.into_inner());
        orgs.get(org_id).cloned()
    }

    /// Check if an org is active. Returns false if org doesn't exist.
    pub fn is_active(&self, org_id: &str) -> bool {
        self.get(org_id).is_some_and(|o| o.status == "active")
    }

    /// Set org status. Suspended org blocks all members.
    pub fn set_status(&self, org_id: &str, status: &str) -> crate::Result<bool> {
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        if let Some(org) = orgs.get_mut(org_id) {
            org.status = status.into();
            if let Some(ref catalog) = self.catalog {
                catalog.put_org(&org.to_stored())?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set org-level rate limit.
    pub fn set_rate_limit(&self, org_id: &str, qps: u64) -> crate::Result<bool> {
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        if let Some(org) = orgs.get_mut(org_id) {
            org.rate_limit_qps = qps;
            if let Some(ref catalog) = self.catalog {
                catalog.put_org(&org.to_stored())?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set org-level quotas.
    pub fn set_quota(
        &self,
        org_id: &str,
        max_storage: Option<u64>,
        max_members: Option<u32>,
    ) -> crate::Result<bool> {
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        if let Some(org) = orgs.get_mut(org_id) {
            if let Some(s) = max_storage {
                org.quota_max_storage = s;
            }
            if let Some(m) = max_members {
                org.quota_max_members = m;
            }
            if let Some(ref catalog) = self.catalog {
                catalog.put_org(&org.to_stored())?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Drop an organization and every membership it owns, in both the
    /// catalog and the in-memory forward/reverse index. A dropped org that
    /// left its memberships behind would leak stale org ids out of
    /// `orgs_for_user` for every former member.
    pub fn drop_org(&self, org_id: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_members_for_org(org_id)?;
            catalog.delete_org(org_id)?;
        }
        {
            let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
            members.remove_org(org_id);
        }
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        Ok(orgs.remove(org_id).is_some())
    }

    /// List all organizations, optionally filtered by tenant.
    pub fn list(&self, tenant_filter: Option<u64>) -> Vec<OrgRecord> {
        let orgs = self.orgs.read().unwrap_or_else(|p| p.into_inner());
        orgs.values()
            .filter(|o| tenant_filter.is_none_or(|t| o.tenant_id == t))
            .cloned()
            .collect()
    }

    // ── Membership ──────────────────────────────────────────────────

    /// Add a member to an organization.
    pub fn add_member(&self, org_id: &str, user_id: &str, role: &str) -> crate::Result<()> {
        let record = OrgMemberRecord {
            auth_user_id: user_id.into(),
            org_id: org_id.into(),
            role: role.into(),
            joined_at: now_secs(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_org_member(&record.to_stored())?;
        }

        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        members.insert(record);
        Ok(())
    }

    /// Remove a member from an organization.
    pub fn remove_member(&self, org_id: &str, user_id: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_org_member(org_id, user_id)?;
        }
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        Ok(members.remove(org_id, user_id))
    }

    /// List members of an organization.
    pub fn members_of(&self, org_id: &str) -> Vec<OrgMemberRecord> {
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members.members_of(org_id)
    }

    /// List all orgs a user belongs to — O(orgs the user is in) via the
    /// reverse index, read-lock only.
    pub fn orgs_for_user(&self, user_id: &str) -> Vec<String> {
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members.orgs_for_user(user_id)
    }

    /// JIT org creation: create org if it doesn't exist.
    pub fn ensure_org(&self, org_id: &str, tenant_id: u64) -> crate::Result<()> {
        if self.get(org_id).is_some() {
            return Ok(());
        }
        self.create_org(org_id, org_id, tenant_id)?;
        info!(org_id = %org_id, "JIT org created");
        Ok(())
    }

    pub fn catalog(&self) -> Option<&SystemCatalog> {
        self.catalog.as_ref()
    }

    pub fn count(&self) -> usize {
        self.orgs.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

impl Default for OrgStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get_org() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme Corp", 1).unwrap();
        let org = store.get("acme").unwrap();
        assert_eq!(org.name, "Acme Corp");
        assert_eq!(org.status, "active");
    }

    #[test]
    fn set_org_status() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        assert!(store.is_active("acme"));

        store.set_status("acme", "suspended").unwrap();
        assert!(!store.is_active("acme"));
    }

    #[test]
    fn membership() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.add_member("acme", "user_42", "member").unwrap();
        store.add_member("acme", "user_99", "admin").unwrap();

        assert_eq!(store.members_of("acme").len(), 2);
        assert_eq!(store.orgs_for_user("user_42"), vec!["acme"]);

        store.remove_member("acme", "user_42").unwrap();
        assert_eq!(store.members_of("acme").len(), 1);
    }

    #[test]
    fn ensure_org_idempotent() {
        let store = OrgStore::new();
        store.ensure_org("acme", 1).unwrap();
        store.ensure_org("acme", 1).unwrap(); // No error.
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn orgs_for_user_returns_exactly_users_orgs() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.create_org("globex", "Globex", 1).unwrap();
        store.add_member("acme", "user_1", "member").unwrap();

        assert_eq!(store.orgs_for_user("user_1"), vec!["acme"]);
        assert!(store.orgs_for_user("nobody").is_empty());
    }

    #[test]
    fn orgs_for_user_multiple_orgs() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.create_org("globex", "Globex", 1).unwrap();
        store.create_org("initech", "Initech", 1).unwrap();
        store.add_member("acme", "user_1", "member").unwrap();
        store.add_member("globex", "user_1", "admin").unwrap();

        let mut orgs = store.orgs_for_user("user_1");
        orgs.sort();
        assert_eq!(orgs, vec!["acme", "globex"]);
        // Not a member of initech.
        assert!(
            !store
                .orgs_for_user("user_1")
                .contains(&"initech".to_string())
        );
    }

    #[test]
    fn removing_membership_drops_it_from_reverse_index() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.add_member("acme", "user_1", "member").unwrap();
        assert_eq!(store.orgs_for_user("user_1"), vec!["acme"]);

        store.remove_member("acme", "user_1").unwrap();
        assert!(store.orgs_for_user("user_1").is_empty());
    }

    #[test]
    fn dropping_org_drops_its_memberships_from_reverse_index() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.create_org("globex", "Globex", 1).unwrap();
        store.add_member("acme", "user_1", "member").unwrap();
        store.add_member("globex", "user_1", "admin").unwrap();

        store.drop_org("acme").unwrap();

        assert_eq!(store.orgs_for_user("user_1"), vec!["globex"]);
        assert!(store.members_of("acme").is_empty());
    }

    #[test]
    fn reload_from_catalog_rebuilds_reverse_index() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let db_path = dir.path().join("orgs.redb");

        {
            let catalog = SystemCatalog::open(&db_path).expect("open catalog");
            let store = OrgStore::open(catalog).expect("open store");
            store.create_org("acme", "Acme", 1).unwrap();
            store.add_member("acme", "user_1", "member").unwrap();
            store.add_member("acme", "user_2", "admin").unwrap();
        }

        // Fresh OrgStore over the same catalog simulates a process restart:
        // `members` is repopulated from disk, and the reverse index MUST be
        // rebuilt alongside it, not left empty.
        let catalog = SystemCatalog::open(&db_path).expect("reopen catalog");
        let reloaded = OrgStore::open(catalog).expect("reopen store");

        assert_eq!(reloaded.orgs_for_user("user_1"), vec!["acme"]);
        assert_eq!(reloaded.orgs_for_user("user_2"), vec!["acme"]);
        assert_eq!(reloaded.members_of("acme").len(), 2);
    }

    #[test]
    fn two_users_in_same_org_do_not_leak_into_each_other() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.add_member("acme", "user_1", "member").unwrap();
        store.add_member("acme", "user_2", "admin").unwrap();

        assert_eq!(store.orgs_for_user("user_1"), vec!["acme"]);
        assert_eq!(store.orgs_for_user("user_2"), vec!["acme"]);

        store.remove_member("acme", "user_1").unwrap();
        assert!(store.orgs_for_user("user_1").is_empty());
        // user_2's membership is untouched.
        assert_eq!(store.orgs_for_user("user_2"), vec!["acme"]);
    }
}
