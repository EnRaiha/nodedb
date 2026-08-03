// SPDX-License-Identifier: BUSL-1.1

//! Durable apply-side mirror of Raft-replicated join-token lifecycle state.
//!
//! The mirror is loaded before metadata-Raft replay begins, so a node that
//! restarts mid-enrollment resumes with the same view of every token.

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};
use redb::ReadableDatabase;

/// Durable apply-side image of Raft-replicated join-token lifecycle state.
pub const JOIN_TOKEN_STATES: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("_system.join_token_states");

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct StoredJoinTokenState {
    lifecycle: u8,
    node_addr: Option<String>,
    expires_at_ms: u64,
    attempt: u32,
    consumed_at_ms: u64,
    lease_id: Option<[u8; 16]>,
    lease_expires_at_ms: u64,
    recovery_bundle: Vec<u8>,
}

impl SystemCatalog {
    /// Persist the apply-side image of one committed join-token transition.
    pub fn put_join_token_state(
        &self,
        state: &nodedb_cluster::JoinTokenState,
    ) -> crate::Result<()> {
        let (lifecycle, node_addr, consumed_at_ms, lease_id, lease_expires_at_ms, recovery_bundle) =
            match &state.lifecycle {
                nodedb_cluster::JoinTokenLifecycle::Issued => (0, None, 0, None, 0, Vec::new()),
                nodedb_cluster::JoinTokenLifecycle::InFlight {
                    node_addr,
                    lease_id,
                    lease_expires_at_ms,
                } => (
                    1,
                    Some(node_addr.to_string()),
                    0,
                    Some(*lease_id),
                    *lease_expires_at_ms,
                    Vec::new(),
                ),
                nodedb_cluster::JoinTokenLifecycle::Consumed {
                    node_addr,
                    lease_id,
                    ts_ms,
                    recovery_bundle,
                } => (
                    2,
                    Some(node_addr.to_string()),
                    *ts_ms,
                    Some(*lease_id),
                    0,
                    recovery_bundle.clone(),
                ),
                nodedb_cluster::JoinTokenLifecycle::Expired => (3, None, 0, None, 0, Vec::new()),
                nodedb_cluster::JoinTokenLifecycle::Aborted => (4, None, 0, None, 0, Vec::new()),
            };
        let bytes = zerompk::to_msgpack_vec(&StoredJoinTokenState {
            lifecycle,
            node_addr,
            expires_at_ms: state.expires_at_ms,
            attempt: state.attempt,
            consumed_at_ms,
            lease_id,
            lease_expires_at_ms,
            recovery_bundle,
        })
        .map_err(|e| catalog_err("serialize join token state", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("join_token_states write txn", e))?;
        {
            let mut table = txn
                .open_table(JOIN_TOKEN_STATES)
                .map_err(|e| catalog_err("open join_token_states", e))?;
            table
                .insert(state.token_hash.as_slice(), bytes.as_slice())
                .map_err(|e| catalog_err("insert join_token_states", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("join_token_states commit", e))
    }

    /// Load the durable token mirror before metadata-Raft replay begins.
    pub fn list_join_token_states(&self) -> crate::Result<Vec<nodedb_cluster::JoinTokenState>> {
        use redb::ReadableTable as _;

        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("join_token_states read txn", e))?;
        let table = txn
            .open_table(JOIN_TOKEN_STATES)
            .map_err(|e| catalog_err("open join_token_states", e))?;
        let mut states = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate join_token_states", e))?
        {
            let (hash, value) = row.map_err(|e| catalog_err("read join_token_states", e))?;
            let token_hash: [u8; 32] =
                hash.value().try_into().map_err(|_| crate::Error::Storage {
                    engine: "catalog".into(),
                    detail: "invalid join-token hash length".into(),
                })?;
            let stored: StoredJoinTokenState = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize join token state", e))?;
            let node_addr = stored
                .node_addr
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|e| crate::Error::Storage {
                    engine: "catalog".into(),
                    detail: format!("invalid join-token address: {e}"),
                })?;
            let lifecycle = match (stored.lifecycle, node_addr, stored.lease_id) {
                (0, _, _) => nodedb_cluster::JoinTokenLifecycle::Issued,
                (1, Some(node_addr), Some(lease_id)) => {
                    nodedb_cluster::JoinTokenLifecycle::InFlight {
                        node_addr,
                        lease_id,
                        lease_expires_at_ms: stored.lease_expires_at_ms,
                    }
                }
                (2, Some(node_addr), Some(lease_id)) => {
                    nodedb_cluster::JoinTokenLifecycle::Consumed {
                        node_addr,
                        lease_id,
                        ts_ms: stored.consumed_at_ms,
                        recovery_bundle: stored.recovery_bundle,
                    }
                }
                (3, _, _) => nodedb_cluster::JoinTokenLifecycle::Expired,
                (4, _, _) => nodedb_cluster::JoinTokenLifecycle::Aborted,
                _ => {
                    return Err(crate::Error::Storage {
                        engine: "catalog".into(),
                        detail: "invalid join-token lifecycle record".into(),
                    });
                }
            };
            states.push(nodedb_cluster::JoinTokenState {
                token_hash,
                lifecycle,
                expires_at_ms: stored.expires_at_ms,
                attempt: stored.attempt,
            });
        }
        Ok(states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumed_join_token_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let state = nodedb_cluster::JoinTokenState {
            token_hash: [7; 32],
            lifecycle: nodedb_cluster::JoinTokenLifecycle::Consumed {
                node_addr: "127.0.0.1:9000".parse().unwrap(),
                lease_id: [8; 16],
                ts_ms: 55,
                recovery_bundle: vec![9, 10],
            },
            expires_at_ms: 99,
            attempt: 1,
        };
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.put_join_token_state(&state).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        let loaded = cat.list_join_token_states().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(matches!(
            loaded[0].lifecycle,
            nodedb_cluster::JoinTokenLifecycle::Consumed { ts_ms: 55, .. }
        ));
    }
}
