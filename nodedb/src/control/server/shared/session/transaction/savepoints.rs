// SPDX-License-Identifier: BUSL-1.1

//! SAVEPOINT / RELEASE / ROLLBACK TO on an open transaction.

use std::collections::BTreeMap;

use crate::types::VShardId;

use super::super::connection::SessionId;
use super::super::state::SavepointEntry;
use super::super::store::SessionStore;

impl SessionStore {
    /// Create a savepoint at the current tx_buffer position.
    ///
    /// `markers` maps each vShard that had staged writes at savepoint time to its
    /// Data-Plane value/TTL and GRAPH overlay undo-journal lengths (captured via
    /// `MetaOp::MarkSavepoint`), so a later ROLLBACK TO can rewind every staging
    /// overlay to exactly this point.
    pub fn create_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: String,
        markers: BTreeMap<VShardId, (usize, usize)>,
    ) {
        self.write_session(addr, |session| {
            let buffer_len = session.tx_buffer.len();
            let pending_offset_len = session.pending_offset_commits.len();
            let pending_inference_len = session.pending_field_inference.len();
            session.savepoints.push(SavepointEntry {
                name,
                buffer_len,
                pending_offset_len,
                pending_inference_len,
                markers,
            });
        });
    }

    /// Release a savepoint: destroy the named savepoint and every savepoint
    /// established after it, keeping their buffered/staged effects (PostgreSQL
    /// semantics). Returns `Err` (SQLSTATE 3B001) if the name does not exist.
    pub fn release_savepoint(&self, addr: impl Into<SessionId>, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            session.savepoints.truncate(pos);
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position and
    /// return the per-vShard `(value_marker, graph_marker)` overlay journal
    /// markers the caller must rewind each staged vShard's Data-Plane staging
    /// overlays to. A vShard first staged AFTER the savepoint is absent from the
    /// returned map; the caller rewinds it to `(0, 0)`.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: &str,
    ) -> crate::Result<BTreeMap<VShardId, (usize, usize)>> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_len = session.savepoints[pos].buffer_len;
            let pending_offset_len = session.savepoints[pos].pending_offset_len;
            let pending_inference_len = session.savepoints[pos].pending_inference_len;
            let markers = session.savepoints[pos].markers.clone();
            if session.tx_buffer.len() != session.tx_lease_scopes.len() {
                return Err(crate::Error::Internal {
                    detail: "transaction lease scope holders are misaligned".into(),
                });
            }
            session.tx_buffer.truncate(buffer_len);
            session.tx_lease_scopes.truncate(buffer_len);
            session.pending_offset_commits.truncate(pending_offset_len);
            session
                .pending_field_inference
                .truncate(pending_inference_len);
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.savepoints.truncate(pos + 1);
            Ok(markers)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}
