// SPDX-License-Identifier: BUSL-1.1

//! WAL append logic: delegates to shared dispatch utilities.

use crate::control::server::wal_dispatch::WalAppendOutcome;
use crate::types::{TenantId, VShardId};

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Append a write operation to the WAL for single-node durability.
    ///
    /// Delegates to the shared `dispatch_utils::wal_append_if_write` to
    /// avoid duplication between pgwire and HTTP endpoints. Returns the
    /// allocated WAL LSN (`Some`) for writes, `None` for reads, alongside the
    /// resolved TTL instant (if any) for a KV write — see [`WalAppendOutcome`].
    pub(super) fn wal_append_if_write(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: crate::types::DatabaseId,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<WalAppendOutcome> {
        crate::control::server::wal_dispatch::wal_append_if_write(
            &self.state.wal,
            tenant_id,
            vshard_id,
            database_id,
            plan,
        )
    }
}
