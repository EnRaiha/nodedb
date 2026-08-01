// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-drain and CA-trust-change host-side effects.

use tracing::debug;

use nodedb_cluster::DescriptorId;
use nodedb_types::Hlc;

use super::audit::apply_ca_trust_change;
use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    pub(super) fn apply_drain_start(
        &self,
        descriptor_id: &DescriptorId,
        up_to_version: u64,
        expires_at: Hlc,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            // This exact gate also covers plan admission's drain check,
            // refcount increment, and first-holder acquire.
            let _admission_gate = shared
                .lease_admission_gate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            shared
                .lease_drain
                .install_start(descriptor_id.clone(), up_to_version, expires_at);
            debug!(
                descriptor = ?descriptor_id,
                up_to_version,
                "drain_start applied to host tracker"
            );
        }
        Ok(())
    }

    pub(super) fn apply_drain_end(&self, descriptor_id: &DescriptorId) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            shared.lease_drain.install_end(descriptor_id);
            debug!(
                descriptor = ?descriptor_id,
                "drain_end applied to host tracker"
            );
        }
        Ok(())
    }

    pub(super) fn apply_ca_trust(
        &self,
        add_ca_cert: Option<&[u8]>,
        remove_ca_fingerprint: Option<&[u8; 32]>,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            apply_ca_trust_change(&shared, add_ca_cert, remove_ca_fingerprint, raft_index);
        }
        Ok(())
    }
}
