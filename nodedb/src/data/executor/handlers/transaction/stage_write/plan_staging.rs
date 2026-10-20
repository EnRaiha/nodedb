// SPDX-License-Identifier: BUSL-1.1

//! Opening one write plan's staging in a transaction overlay.
//!
//! Every core that stages a plan for a transaction holds its overlay, even
//! when the plan stages no row: resolve refuses a transaction with staged
//! writes whose overlay is missing. The overlay tags what the plan stages
//! with the source it stages under, so a trigger body's rows commit as the
//! body's.

use nodedb_physical::physical_plan::{PhysicalPlan, written_collections};

use crate::data::executor::core_loop::CoreLoop;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId, TxnId};

impl CoreLoop {
    /// Open `plan`'s staging in transaction `txn_id`'s overlay under
    /// `source`, for every collection the plan writes.
    pub(in crate::data::executor) fn begin_plan_staging(
        &mut self,
        txn_id: TxnId,
        source: EventSource,
        database_id: DatabaseId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
    ) {
        let collections = written_collections(std::slice::from_ref(plan));
        self.txn_overlay_mut(txn_id).begin_staging(
            source,
            collections
                .into_iter()
                .map(|collection| (database_id, tenant_id, collection)),
        );
    }
}
