// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-drain, lease-release, and CA-trust-change host-side effects.

use tracing::debug;

use nodedb_cluster::{DescriptorId, DrainOwner};
use nodedb_types::Hlc;

use super::audit::apply_ca_trust_change;
use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    pub(super) fn apply_drain_start(
        &self,
        descriptor_id: &DescriptorId,
        owner: &DrainOwner,
        up_to_version: u64,
        expires_at: Hlc,
        proposer_node_id: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        crate::control::lease::apply_drain_start(
            &shared,
            descriptor_id,
            owner,
            up_to_version,
            expires_at,
            proposer_node_id,
        )?;
        debug!(
            descriptor = ?descriptor_id,
            up_to_version,
            "drain_start applied to host tracker"
        );
        Ok(())
    }

    /// A release of this node's lease ends the statements still running
    /// under it: other nodes now treat the lease as gone.
    pub(super) fn apply_lease_release(
        &self,
        node_id: u64,
        descriptor_ids: &[DescriptorId],
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        self.credentials
            .catalog()
            .remove_descriptor_leases(node_id, descriptor_ids)?;
        crate::control::lease::revoke_on_release(&shared, node_id, descriptor_ids);
        // A drain waiting on one of these leases counts again now.
        shared.lease_drain.wake_drain_waiters();
        Ok(())
    }

    pub(super) fn apply_drain_end(
        &self,
        descriptor_id: &DescriptorId,
        owner: &DrainOwner,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        crate::control::lease::apply_drain_ends(
            &shared,
            &[(descriptor_id.clone(), owner.clone())],
        )?;
        debug!(
            descriptor = ?descriptor_id,
            ?owner,
            "drain_end applied to host tracker"
        );
        Ok(())
    }

    pub(super) fn apply_ca_trust(
        &self,
        add_ca_cert: Option<&[u8]>,
        remove_ca_fingerprint: Option<&[u8; 32]>,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        apply_ca_trust_change(&shared, add_ca_cert, remove_ca_fingerprint, raft_index)
    }
}
