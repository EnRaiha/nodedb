// SPDX-License-Identifier: BUSL-1.1

//! Host-side effects of committed membership entries: join tokens,
//! enrollment preauthorizations, and a node's leave.

use nodedb_cluster::JoinTokenTransitionKind;

use super::types::MetadataCommitApplier;

/// Unix-epoch milliseconds. A clock before the epoch reads as the far
/// future, so no preauthorization is applied against it.
fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(u64::MAX)
}

impl MetadataCommitApplier {
    /// Mirror a join-token transition and persist the token's new state.
    pub(super) fn apply_join_token_transition(
        &self,
        token_hash: &[u8; 32],
        transition: &JoinTokenTransitionKind,
        ts_ms: u64,
    ) -> Result<(), crate::Error> {
        nodedb_cluster::apply_token_transition_to_mirror(
            &self.token_state,
            *token_hash,
            transition,
            ts_ms,
        );
        let state = self
            .token_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(token_hash)
            .cloned();
        if let Some(state) = state {
            self.credentials.catalog().put_join_token_state(&state)?;
        }
        Ok(())
    }

    /// Persist an enrollment preauthorization and admit its identity until it
    /// expires. An expired entry applies nothing.
    pub(super) fn apply_enrollment_preauthorization(
        &self,
        spki: &[u8; 32],
        expires_at_ms: u64,
    ) -> Result<(), crate::Error> {
        let now_ms = unix_now_ms();
        if expires_at_ms <= now_ms {
            return Ok(());
        }
        self.credentials
            .catalog()
            .put_enrollment_preauthorization(spki, expires_at_ms)?;
        let ttl = std::time::Duration::from_millis(expires_at_ms - now_ms);
        let transport = self.transport.get().ok_or_else(|| crate::Error::Internal {
            detail: "metadata enrollment apply has no cluster transport".into(),
        })?;
        if !transport.preauthorize_peer_identity(*spki, ttl) {
            // Admission remains fail-closed, but replicated metadata
            // application must never wedge on a bounded runtime cache.
            // The issuer reserves capacity before proposing, so this is
            // only a defensive path for stale/corrupt excess entries.
            tracing::error!(
                ?spki,
                "metadata enrollment preauthorization capacity exhausted; entry persisted but not admitted"
            );
        }
        Ok(())
    }

    /// Remove an enrollment preauthorization and revoke its admission. An
    /// expired entry applies nothing.
    pub(super) fn apply_enrollment_revoke(
        &self,
        spki: &[u8; 32],
        expires_at_ms: u64,
    ) -> Result<(), crate::Error> {
        let now_ms = unix_now_ms();
        if expires_at_ms <= now_ms {
            return Ok(());
        }
        self.credentials
            .catalog()
            .remove_enrollment_preauthorization(spki)?;
        let transport = self.transport.get().ok_or_else(|| crate::Error::Internal {
            detail: "metadata enrollment revoke has no cluster transport".into(),
        })?;
        transport.revoke_peer_preauthorization(
            spki,
            std::time::Duration::from_millis(expires_at_ms - now_ms),
        );
        Ok(())
    }

    /// Queue the cleanup a node that left owes.
    ///
    /// A node that left can never release its own leases or end its own
    /// drains. The owed cleanup is durable before the entry counts as
    /// applied. The spawned drive, the boot drain, and the retry worker
    /// carry it out. The proposals run off the raft loop task: waiting on
    /// them here will deadlock the applied-index watcher.
    pub(super) fn apply_node_leave(
        &self,
        node_id: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        self.credentials
            .catalog()
            .enqueue_pending_leave_cleanup(node_id, raft_index)?;
        tokio::spawn(async move {
            if let Err(error) =
                crate::control::lease::leave_cleanup::drive_leave_cleanup(&shared, node_id).await
            {
                tracing::warn!(
                    node_id,
                    %error,
                    "leave cleanup did not finish; the retry worker re-drives it"
                );
            }
        });
        Ok(())
    }
}
