// SPDX-License-Identifier: BUSL-1.1

//! Lease / drain inspector methods on [`TestClusterNode`].

use crate::cluster_harness::node::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Whether this node's `lease_drain` tracker currently holds
    /// an ACTIVE drain entry (not expired) for the given
    /// `(descriptor_id, min_version)`. Used by the drain tests
    /// to assert replicated drain state.
    pub fn has_drain_for(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
        min_version: u64,
    ) -> bool {
        let now_wall_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        self.shared
            .lease_drain
            .is_draining(descriptor_id, min_version, now_wall_ns)
    }

    /// Total number of leases (across all descriptors and node_ids)
    /// in this node's `MetadataCache.leases` map. Includes expired
    /// records — for filtered counts use [`active_lease_count`].
    pub fn lease_count(&self) -> usize {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.len()
    }

    /// Number of leases whose `expires_at` is strictly greater
    /// than this node's current HLC peek.
    pub fn active_lease_count(&self) -> usize {
        let now = self.shared.hlc_clock.peek();
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.values().filter(|l| l.expires_at > now).count()
    }

    /// Whether this node's `MetadataCache` holds a non-expired
    /// lease at the given version (or higher) on
    /// `(kind, tenant_id, name)` granted to `holder_node_id`.
    pub fn has_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u64,
        name: &str,
        holder_node_id: u64,
        min_version: u64,
    ) -> bool {
        let now = self.shared.hlc_clock.peek();
        let id = nodedb_cluster::DescriptorId::new(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            tenant_id,
            kind,
            name,
        );
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(id, holder_node_id))
            .map(|l| l.expires_at > now && l.version >= min_version)
            .unwrap_or(false)
    }

    /// Snapshot of every lease on this node for the given descriptor.
    pub fn leases_for_descriptor(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u64,
        name: &str,
    ) -> Vec<nodedb_cluster::DescriptorLease> {
        let id = nodedb_cluster::DescriptorId::new(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            tenant_id,
            kind,
            name,
        );
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .iter()
            .filter(|((did, _), _)| did == &id)
            .map(|(_, l)| l.clone())
            .collect()
    }

    /// Acquire a lease on this node via the SharedState facade.
    /// Called directly from the test's tokio runtime worker so the
    /// `block_in_place` inside `acquire_descriptor_lease` lands on
    /// a real runtime thread (which is what `block_in_place`
    /// requires — it cannot be called from a `spawn_blocking`
    /// worker).
    pub async fn acquire_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u64,
        name: &str,
        version: u64,
        duration: std::time::Duration,
    ) -> Result<nodedb_cluster::DescriptorLease, String> {
        let id = nodedb_cluster::DescriptorId::new(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            tenant_id,
            kind,
            name.to_string(),
        );
        self.shared
            .acquire_descriptor_lease(id, version, duration)
            .map_err(|e| format!("acquire failed: {e}"))
    }

    /// Release a batch of leases on this node via the SharedState facade.
    pub async fn release_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> Result<(), String> {
        self.shared
            .release_descriptor_leases(descriptor_ids)
            .map_err(|e| format!("release failed: {e}"))
    }
}
