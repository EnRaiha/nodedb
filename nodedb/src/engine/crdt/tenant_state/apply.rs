// SPDX-License-Identifier: BUSL-1.1

//! Delta apply paths: Raft-committed import and peer-validated upsert.

use loro::LoroValue;
use nodedb_types::columnar::schema::TS_SYSTEM;

use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::validator::ProposedChange;

use super::core::{TenantCrdtEngine, TenantRowLookup};

impl TenantCrdtEngine {
    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(&self, change: &ProposedChange) -> PreValidationResult {
        let view = TenantRowLookup {
            collections: &self.collections,
            array_surrogate_ids: &self.array_surrogate_ids,
        };
        pre_validate::pre_validate(&self.validator, &view, change)
    }

    /// Test-only raw import used to seed transaction rollback fixtures.
    ///
    /// Production applies always go through the validated / authenticated paths
    /// ([`Self::apply_committed_delta_validated`] /
    /// [`Self::apply_committed_delta_authenticated`]), which apply into a
    /// detached candidate and enforce constraints and signing before any
    /// authoritative state moves — so no unvalidated peer delta can mutate it.
    #[cfg(test)]
    pub(crate) fn apply_committed_delta(
        &mut self,
        collection: &str,
        delta: &[u8],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .import(delta)
            .map(|_admission| ())
            .map_err(crate::Error::Crdt)
    }

    /// Validate and attempt to apply a delta from a peer.
    ///
    /// If constraints are violated, the delta is routed to the DLQ.
    /// Returns `Ok(())` on success, or the constraint violation error.
    ///
    /// For bitemporal collections, `_ts_system` is always stamped with the
    /// receiving node's clock, overwriting any value the sender supplied.
    /// This keeps system-time receiver-authoritative so convergence does
    /// not depend on clock agreement between peers.
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        auth: nodedb_crdt::CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> crate::Result<()> {
        // Tenant-wide view over all collections + array surrogates. The view
        // borrows `self.collections` / `self.array_surrogate_ids` immutably
        // while `self.validator` is borrowed mutably — disjoint fields, so both
        // borrows coexist. The view borrow ends before the upsert below.
        {
            let view = TenantRowLookup {
                collections: &self.collections,
                array_surrogate_ids: &self.array_surrogate_ids,
            };
            self.validator
                .validate_or_reject(&view, peer_id, auth, change, delta_bytes)
                .map_err(crate::Error::Crdt)?;
        }

        let is_bitemporal = self.validator.is_bitemporal(&change.collection);
        // no-determinism: peer delta validation path, not Calvin apply_committed_delta path
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .filter(|(k, _)| !(is_bitemporal && k == TS_SYSTEM))
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        let state = self.state_mut(&change.collection)?;
        if is_bitemporal {
            fields.push((TS_SYSTEM, LoroValue::I64(now_ms)));
            state
                .upsert_versioned(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        } else {
            state
                .upsert(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        }
    }
}
