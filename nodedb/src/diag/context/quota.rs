// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for resource-quota capture sites.
//!
//! A quota that fails to install or persist leaves its scope uncapped, and
//! the only visible symptom is load that no cap refuses.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// Scope label a database-quota report carries.
pub const DATABASE_SCOPE: &str = "database";

/// Scope label a tenant-quota report carries.
pub const TENANT_SCOPE: &str = "tenant";

/// A persisted quota row that boot replay could not push into live
/// enforcement, so its scope runs uncapped until an operator re-runs the DDL.
pub(in crate::diag) struct QuotaRowNotInstalled<'a> {
    /// Why the row was skipped (`undecodable`, `invalid_record`).
    pub cause: &'static str,
    /// Scope the row caps (`database`, `tenant`).
    pub scope: &'static str,
    pub database_id: u64,
    /// Tenant the row caps; absent on a database-scope row.
    pub tenant_id: Option<u64>,
    /// What the detecting site saw.
    pub detail: &'a str,
}

impl DomainContext for QuotaRowNotInstalled<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.quota_row_not_installed"
    }

    fn grouping_key(&self) -> String {
        // Cause + scope name the bug; the ids are the occurrence, so a boot
        // that skips a thousand rows files one report.
        format!("cause={};scope={}", self.cause, self.scope)
    }

    fn to_json(&self) -> Value {
        json!({
            "cause": self.cause,
            "scope": self.scope,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "detail": self.detail,
            "why_fatal": "the admission registry, memory governor, and maintenance CPU \
                          budget are rebuilt empty on every start, so a row that never \
                          replays leaves its scope with no connection cap, no memory \
                          ceiling, and no CPU share. SHOW QUOTA keeps reporting the \
                          persisted numbers, so the cap looks armed while nothing \
                          enforces it, and one scope can starve every other",
            "operator_action": "re-run the DDL for the named scope to rewrite and reinstall \
                                 the row — ALTER DATABASE <name> SET QUOTA (...) for a \
                                 database, ALTER TENANT <name> IN DATABASE <db> SET QUOTA \
                                 (...) for a tenant. An undecodable row was written by a \
                                 build with a different record shape; an invalid one holds \
                                 values this build rejects",
        })
    }
}

/// A boot quota replay that could not read the catalog, so no row of that
/// scope reached live enforcement.
pub(in crate::diag) struct QuotaScopeReplayAborted<'a> {
    /// Scope whose listing failed (`database`, `tenant`).
    pub scope: &'static str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for QuotaScopeReplayAborted<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.quota_scope_replay_aborted"
    }

    fn grouping_key(&self) -> String {
        // Scope + error class name the bug; nothing else varies per boot.
        format!("scope={};cause={}", self.scope, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "scope": self.scope,
            "error_class": self.error_class,
            "why_fatal": "the listing failed before any row was read, so every scope of \
                          this kind starts with no connection cap, no memory ceiling, and \
                          no CPU share on this node. Boot continues and SHOW QUOTA still \
                          reports the persisted numbers, so the node accepts unbounded \
                          load while appearing fully configured",
            "operator_action": "this node's system catalog is unreadable for the named \
                                 quota table — check the data directory for a truncated or \
                                 permission-denied system.redb before serving traffic, then \
                                 restart. Every quota of this scope must be re-run as DDL \
                                 if the table cannot be recovered",
        })
    }
}

/// A quota row write or delete that redb refused while applying a committed
/// catalog entry, so this node's durable rows diverge from consensus.
pub(in crate::diag) struct QuotaRowWriteFailed<'a> {
    /// Catalog operation that failed (`write_database_quota`, ...).
    pub operation: &'static str,
    pub database_id: u64,
    /// Tenant the row caps; absent on a database-scope row.
    pub tenant_id: Option<u64>,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for QuotaRowWriteFailed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.quota_row_write_failed"
    }

    fn grouping_key(&self) -> String {
        // Operation + error class name the bug; the ids are the occurrence.
        format!("op={};cause={}", self.operation, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "operation": self.operation,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "error_class": self.error_class,
            "why_fatal": "the entry was already committed by consensus, so every other \
                          replica holds the row this node dropped. The DDL returns success \
                          to the client, and the divergence stays invisible until the next \
                          restart replays the catalog and this node comes up enforcing a \
                          stale cap, or none at all",
            "operator_action": "re-run the DDL for the named scope once the underlying redb \
                                 error is cleared — ALTER DATABASE <name> SET QUOTA (...) or \
                                 ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...). Compare \
                                 SHOW QUOTA on this node against a healthy replica first, \
                                 since the two now disagree",
        })
    }
}

/// A dropped scope whose tenant quota rows could not be scanned, so some of
/// them survive the drop.
pub(in crate::diag) struct QuotaScopePurgeIncomplete<'a> {
    /// Scope being dropped (`database`, `tenant`).
    pub scope: &'static str,
    /// Database being dropped; absent on a tenant drop.
    pub database_id: Option<u64>,
    /// Tenant being dropped; absent on a database drop.
    pub tenant_id: Option<u64>,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for QuotaScopePurgeIncomplete<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.quota_scope_purge_incomplete"
    }

    fn grouping_key(&self) -> String {
        // Scope + error class name the bug; the ids are the occurrence.
        format!("scope={};cause={}", self.scope, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "scope": self.scope,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "error_class": self.error_class,
            "why_fatal": "the scan names the tenant quota rows the drop must remove, so a \
                          failed scan leaves rows keyed to an id that no longer exists. A \
                          later id reuse inherits caps nobody set, and boot replay installs \
                          them, so the new scope is throttled with no DDL explaining it",
            "operator_action": "clear the underlying redb error, then re-issue the DROP for \
                                 the named scope to rerun the purge. Until it succeeds, \
                                 treat the dropped id as reserved and do not recreate it",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skipped_row_grouping_ignores_the_scope_ids() {
        let first = QuotaRowNotInstalled {
            cause: "undecodable",
            scope: "tenant",
            database_id: 4,
            tenant_id: Some(9),
            detail: "value did not decode",
        };
        let second = QuotaRowNotInstalled {
            database_id: 77,
            tenant_id: Some(1),
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "cause=undecodable;scope=tenant");
    }

    #[test]
    fn skipped_row_grouping_separates_the_two_causes() {
        let undecodable = QuotaRowNotInstalled {
            cause: "undecodable",
            scope: "database",
            database_id: 4,
            tenant_id: None,
            detail: "value did not decode",
        };
        let invalid = QuotaRowNotInstalled {
            cause: "invalid_record",
            ..undecodable
        };
        assert_ne!(undecodable.grouping_key(), invalid.grouping_key());
    }

    #[test]
    fn write_failure_grouping_drops_the_errno_text() {
        let full = QuotaRowWriteFailed {
            operation: "write_database_quota",
            database_id: 1,
            tenant_id: None,
            error_class: "catalog",
        };
        let denied = QuotaRowWriteFailed {
            database_id: 900,
            ..full
        };
        assert_eq!(full.grouping_key(), denied.grouping_key());
        assert_eq!(full.grouping_key(), "op=write_database_quota;cause=catalog");
    }

    #[test]
    fn replay_and_purge_group_by_scope() {
        let aborted = QuotaScopeReplayAborted {
            scope: "tenant",
            error_class: "catalog",
        };
        assert_eq!(aborted.grouping_key(), "scope=tenant;cause=catalog");
        let purge = QuotaScopePurgeIncomplete {
            scope: "database",
            database_id: Some(3),
            tenant_id: None,
            error_class: "catalog",
        };
        let other = QuotaScopePurgeIncomplete {
            database_id: Some(88),
            ..purge
        };
        assert_eq!(purge.grouping_key(), other.grouping_key());
    }
}
