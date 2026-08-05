// SPDX-License-Identifier: BUSL-1.1

//! Prometheus rendering for metering-store capacity and dropped-entry counters.
//!
//! `UsageStore` and `QuotaManager` are process-lifetime, bounded maps: once a
//! totals map hits its configured cap, new distinct users/orgs/tenants/quota
//! grantees are refused (existing ones keep accumulating) and the refusal is
//! counted via `dropped_*_entries()`. Refusing to record usage is a billing
//! correctness event — a `tracing::warn!` alone is gone at the next restart,
//! so this renders the live counters into the `/metrics` scrape output
//! (the same surface `AuthMetrics::to_prometheus()` and `SystemMetrics`
//! already use) instead of leaving them reachable only via source code.

use std::fmt::Write as _;

use super::quota::QuotaManager;
use super::store::UsageStore;

/// Append metering capacity/drop metrics to a Prometheus text-exposition buffer.
///
/// Called from the `/metrics` HTTP route alongside `SystemMetrics` and
/// `AuthMetrics`. Read live from the stores rather than mirrored into a
/// separate counter struct, so there is exactly one source of truth for
/// these numbers.
pub fn render_prometheus(usage_store: &UsageStore, quota_manager: &QuotaManager, out: &mut String) {
    out.push_str(
        "# HELP nodedb_metering_dropped_entries_total Distinct entities refused after a metering totals map hit its configured cap; existing entities keep accumulating.\n",
    );
    out.push_str("# TYPE nodedb_metering_dropped_entries_total counter\n");
    let _ = writeln!(
        out,
        "nodedb_metering_dropped_entries_total{{map=\"usage_store_users\"}} {}",
        usage_store.dropped_user_entries()
    );
    let _ = writeln!(
        out,
        "nodedb_metering_dropped_entries_total{{map=\"usage_store_orgs\"}} {}",
        usage_store.dropped_org_entries()
    );
    let _ = writeln!(
        out,
        "nodedb_metering_dropped_entries_total{{map=\"usage_store_tenants\"}} {}",
        usage_store.dropped_tenant_entries()
    );
    let _ = writeln!(
        out,
        "nodedb_metering_dropped_entries_total{{map=\"quota_manager_grantees\"}} {}",
        quota_manager.dropped_usage_entries()
    );

    out.push_str(
        "# HELP nodedb_metering_tracked_scopes_max Configured cap on distinct keys for the totals map the paired nodedb_metering_dropped_entries_total series reports against.\n",
    );
    out.push_str("# TYPE nodedb_metering_tracked_scopes_max gauge\n");
    let _ = writeln!(
        out,
        "nodedb_metering_tracked_scopes_max{{map=\"usage_store\"}} {}",
        usage_store.max_tracked_scopes()
    );
    let _ = writeln!(
        out,
        "nodedb_metering_tracked_scopes_max{{map=\"quota_manager\"}} {}",
        quota_manager.max_tracked_grantees()
    );
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_zero_drops_when_under_capacity() {
        let usage_store = UsageStore::with_bounds(1000, 100);
        let quota_manager = QuotaManager::with_bounds(100);

        let mut out = String::new();
        render_prometheus(&usage_store, &quota_manager, &mut out);

        assert!(out.contains("map=\"usage_store_users\"} 0"));
        assert!(out.contains("map=\"usage_store_orgs\"} 0"));
        assert!(out.contains("map=\"usage_store_tenants\"} 0"));
        assert!(out.contains("map=\"quota_manager_grantees\"} 0"));
        assert!(out.contains("nodedb_metering_tracked_scopes_max{map=\"usage_store\"} 100"));
        assert!(out.contains("nodedb_metering_tracked_scopes_max{map=\"quota_manager\"} 100"));
    }

    #[test]
    fn surfaces_dropped_entries_reachable_through_the_prometheus_surface() {
        let usage_store = UsageStore::with_bounds(1000, 1);
        usage_store.ingest(vec![super::super::counter::UsageEvent {
            auth_user_id: "u1".into(),
            org_id: "org1".into(),
            tenant_id: 1,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: "point_get".into(),
            tokens: 1,
            timestamp_secs: 0,
        }]);
        // A second distinct user exceeds the cap of 1.
        usage_store.ingest(vec![super::super::counter::UsageEvent {
            auth_user_id: "u2".into(),
            org_id: "org2".into(),
            tenant_id: 2,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: "point_get".into(),
            tokens: 1,
            timestamp_secs: 0,
        }]);

        let quota_manager = QuotaManager::with_bounds(1);
        quota_manager.record_usage("free", "g1", 1, 0);
        quota_manager.record_usage("free", "g2", 1, 0); // Refused, cap of 1.

        let mut out = String::new();
        render_prometheus(&usage_store, &quota_manager, &mut out);

        assert!(out.contains("map=\"usage_store_users\"} 1"));
        assert!(out.contains("map=\"usage_store_orgs\"} 1"));
        assert!(out.contains("map=\"usage_store_tenants\"} 1"));
        assert!(out.contains("map=\"quota_manager_grantees\"} 1"));
    }
}
