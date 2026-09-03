// SPDX-License-Identifier: Apache-2.0

//! Persistent quota record for a database or tenant.
//!
//! Stored in `_system.database_quotas` (keyed by `DatabaseId`) and
//! `_system.tenant_quotas` (keyed by `(DatabaseId, TenantId)`).
//! Serialized with zerompk for catalog persistence; also implements serde
//! for pgwire result rows and configuration files.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::priority_class::PriorityClass;

/// Reasons a [`QuotaRecord`] can fail invariant validation.
///
/// Returned by [`QuotaRecord::validate`]. Each variant carries the offending
/// value so callers can render an actionable diagnostic without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaValidationError {
    /// `maintenance_cpu_pct` exceeded 100%.
    MaintenanceCpuPctTooHigh { got: u8 },
    /// `cache_weight` was zero (must be ≥ 1 — zero would mean "no doc-cache
    /// capacity at all", which is never the intended configuration).
    CacheWeightZero,
}

impl fmt::Display for QuotaValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaintenanceCpuPctTooHigh { got } => {
                write!(f, "maintenance_cpu_pct must be ≤ 100, got {got}")
            }
            Self::CacheWeightZero => f.write_str("cache_weight must be ≥ 1, got 0"),
        }
    }
}

impl std::error::Error for QuotaValidationError {}

/// Resource budget for a single database or tenant.
///
/// All fields with `_bytes` or count suffixes represent hard ceilings.
/// Zero means "no limit configured" — enforcement code skips the check
/// when the field is zero.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct QuotaRecord {
    /// Maximum resident memory this database/tenant may consume (bytes).
    /// 0 = no per-database limit (global governor still applies).
    pub max_memory_bytes: u64,
    /// Maximum persisted storage (bytes).
    /// 0 = no limit.
    pub max_storage_bytes: u64,
    /// Maximum queries per second.
    /// 0 = no limit.
    pub max_qps: u32,
    /// Maximum concurrent connections.
    /// 0 = no limit.
    pub max_connections: u32,
    /// Relative weight for doc-cache allocation. Higher values receive a
    /// proportionally larger share of the global LRU capacity.
    /// Must be ≥ 1; default 1.
    pub cache_weight: u32,
    /// WAL group-commit priority class and weighted-fair dispatch weight.
    pub priority_class: PriorityClass,
    /// Maximum fraction of per-core time that background maintenance tasks
    /// (compaction, HNSW link repair, etc.) may consume for this database.
    /// Valid range: 0–100. 0 = no budget cap.
    pub maintenance_cpu_pct: u8,
    /// Maximum in-flight requests.
    /// 0 = no limit.
    pub max_concurrent_requests: u32,
    /// Maximum vector dimension accepted on write and on query.
    /// 0 = no limit.
    pub max_vector_dim: u32,
    /// Maximum hop count accepted on a graph traversal or MATCH pattern.
    /// 0 = no limit.
    pub max_graph_depth: u32,
    /// Override for the deactivated-collection retention window, in days.
    /// `None` inherits the system-wide default; `Some(0)` purges on the
    /// next sweep.
    pub deactivated_collection_retention_days: Option<u32>,
}

impl QuotaRecord {
    /// Default quota record applied when no explicit quota has been set.
    ///
    /// All resource ceilings are zero (no limit); `cache_weight` is 1 so every
    /// database gets an equal share of the doc cache; `maintenance_cpu_pct` is
    /// 25 to prevent background tasks from saturating cores.
    pub const DEFAULT: QuotaRecord = QuotaRecord {
        max_memory_bytes: 0,
        max_storage_bytes: 0,
        max_qps: 0,
        max_connections: 0,
        cache_weight: 1,
        priority_class: PriorityClass::Standard,
        maintenance_cpu_pct: 25,
        max_concurrent_requests: 0,
        max_vector_dim: 0,
        max_graph_depth: 0,
        deactivated_collection_retention_days: None,
    };

    /// Validate invariants. Returns the first violation found, or `Ok(())` if
    /// the record is well-formed.
    pub fn validate(&self) -> Result<(), QuotaValidationError> {
        if self.maintenance_cpu_pct > 100 {
            return Err(QuotaValidationError::MaintenanceCpuPctTooHigh {
                got: self.maintenance_cpu_pct,
            });
        }
        if self.cache_weight == 0 {
            return Err(QuotaValidationError::CacheWeightZero);
        }
        Ok(())
    }

    /// Compact one-line summary used in audit log entries.
    ///
    /// Format: `mem=<n>,storage=<n>,qps=<n>,conns=<n>,cache_w=<n>,prio=<class>,
    /// maint=<pct>,concurrent=<n>,vector_dim=<n>,graph_depth=<n>,retention_days=<n>`.
    /// Each numeric dimension renders zero as `unlimited` to match the SHOW
    /// QUOTA presentation. The retention override renders `None` as `inherit`.
    pub fn audit_summary(&self) -> String {
        fn lim(v: u64) -> String {
            if v == 0 {
                "unlimited".into()
            } else {
                v.to_string()
            }
        }
        let retention = match self.deactivated_collection_retention_days {
            Some(days) => days.to_string(),
            None => "inherit".into(),
        };
        format!(
            "mem={},storage={},qps={},conns={},cache_w={},prio={},maint={}%,\
             concurrent={},vector_dim={},graph_depth={},retention_days={}",
            lim(self.max_memory_bytes),
            lim(self.max_storage_bytes),
            lim(self.max_qps as u64),
            lim(self.max_connections as u64),
            self.cache_weight,
            self.priority_class,
            self.maintenance_cpu_pct,
            lim(self.max_concurrent_requests as u64),
            lim(self.max_vector_dim as u64),
            lim(self.max_graph_depth as u64),
            retention
        )
    }

    /// Merge a [`QuotaSpec`] (partial update) into this record in place.
    /// Fields present in the spec overwrite the corresponding field;
    /// absent fields are left unchanged.
    pub fn merge(&mut self, spec: &QuotaSpec) {
        if let Some(v) = spec.max_memory_bytes {
            self.max_memory_bytes = v;
        }
        if let Some(v) = spec.max_storage_bytes {
            self.max_storage_bytes = v;
        }
        if let Some(v) = spec.max_qps {
            self.max_qps = v;
        }
        if let Some(v) = spec.max_connections {
            self.max_connections = v;
        }
        if let Some(v) = spec.cache_weight {
            self.cache_weight = v;
        }
        if let Some(ref v) = spec.priority_class {
            self.priority_class = *v;
        }
        if let Some(v) = spec.maintenance_cpu_pct {
            self.maintenance_cpu_pct = v;
        }
    }
}

impl Default for QuotaRecord {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Partial quota update from `SET QUOTA (...)` DDL.
///
/// All fields are optional; `None` means "leave the existing value unchanged".
/// Applied via [`QuotaRecord::merge`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaSpec {
    pub max_memory_bytes: Option<u64>,
    pub max_storage_bytes: Option<u64>,
    pub max_qps: Option<u32>,
    pub max_connections: Option<u32>,
    pub cache_weight: Option<u32>,
    pub priority_class: Option<PriorityClass>,
    pub maintenance_cpu_pct: Option<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        assert!(QuotaRecord::DEFAULT.validate().is_ok());
    }

    #[test]
    fn maintenance_cpu_pct_over_100_invalid() {
        let mut r = QuotaRecord::DEFAULT;
        r.maintenance_cpu_pct = 101;
        assert!(r.validate().is_err());
    }

    #[test]
    fn cache_weight_zero_invalid() {
        let mut r = QuotaRecord::DEFAULT;
        r.cache_weight = 0;
        assert!(r.validate().is_err());
    }

    #[test]
    fn merge_partial_spec() {
        let mut r = QuotaRecord::DEFAULT;
        let spec = QuotaSpec {
            max_memory_bytes: Some(1073741824),
            priority_class: Some(PriorityClass::Critical),
            ..Default::default()
        };
        r.merge(&spec);
        assert_eq!(r.max_memory_bytes, 1073741824);
        assert_eq!(r.priority_class, PriorityClass::Critical);
        // Unset fields stay at default.
        assert_eq!(r.max_qps, 0);
        assert_eq!(r.cache_weight, 1);
    }

    #[test]
    fn msgpack_roundtrip() {
        let record = QuotaRecord {
            max_memory_bytes: 1073741824,
            max_storage_bytes: 10737418240,
            max_qps: 1000,
            max_connections: 100,
            cache_weight: 2,
            priority_class: PriorityClass::Critical,
            maintenance_cpu_pct: 25,
            max_concurrent_requests: 64,
            max_vector_dim: 1536,
            max_graph_depth: 8,
            deactivated_collection_retention_days: Some(14),
        };
        let bytes = zerompk::to_msgpack_vec(&record).unwrap();
        let decoded: QuotaRecord = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn audit_summary_renders_every_dimension() {
        let mut r = QuotaRecord::DEFAULT;
        r.max_concurrent_requests = 64;
        r.max_vector_dim = 1536;
        r.deactivated_collection_retention_days = Some(14);
        let s = r.audit_summary();
        assert!(s.contains("concurrent=64"), "{s}");
        assert!(s.contains("vector_dim=1536"), "{s}");
        assert!(s.contains("graph_depth=unlimited"), "{s}");
        assert!(s.contains("retention_days=14"), "{s}");
        assert!(
            QuotaRecord::DEFAULT
                .audit_summary()
                .contains("retention_days=inherit")
        );
    }
}
