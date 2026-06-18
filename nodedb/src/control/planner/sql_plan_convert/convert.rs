// SPDX-License-Identifier: BUSL-1.1

//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::SqlPlan;

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, QueryOp};

use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::security::credential::CredentialStore;
use crate::control::surrogate::SurrogateAssigner;
use crate::engine::bitemporal::BitemporalRetentionRegistry;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::TenantId;
use crate::wal::WalManager;

use nodedb_physical::physical_task::PhysicalTask;

/// Qualify a raw collection name with its database ID so that storage keys
/// for collections in different databases never collide.
///
/// The resulting string is used as the `collection` field in every physical
/// plan variant that reaches the Data Plane. Storage engines key data on
/// `(tenant_id, collection, document_id)` — by embedding the database ID
/// into the collection token, isolation between databases is automatic.
pub fn db_qualified(database_id: crate::types::DatabaseId, collection: &str) -> String {
    if database_id == crate::types::DatabaseId::DEFAULT {
        collection.to_string()
    } else {
        format!("{}/{}", database_id.as_u64(), collection)
    }
}

/// Conversion context holding optional references needed during plan conversion.
pub struct ConvertContext {
    pub retention_registry: Option<Arc<RetentionPolicyRegistry>>,
    /// Array DDL/DML targets — when `None`, array statements fail with a
    /// deterministic error so converters used by sub-planners (which do
    /// not own array state) cannot accidentally mutate the catalog.
    pub array_catalog: Option<ArrayCatalogHandle>,
    /// Used by `SqlPlan::CreateArray` / `DropArray` to persist or
    /// remove `_system.arrays` rows.
    pub credentials: Option<Arc<CredentialStore>>,
    /// LSN allocator for array Put/Delete dispatches.
    pub wal: Option<Arc<WalManager>>,
    /// CP-side surrogate assigner — bound to the same `Arc` held on
    /// `SharedState`. Threaded into INSERT/UPSERT/KV-INSERT converters
    /// to bind `(collection, pk_bytes)` → `Surrogate` before the op
    /// crosses the SPSC bridge. `None` only for converters used by
    /// sub-planners that never lower to the surrogate-bearing variants
    /// (e.g. CREATE/DROP/ARRAY paths).
    pub surrogate_assigner: Option<Arc<SurrogateAssigner>>,
    /// `true` when the node is running in cluster mode with a live
    /// topology. Array DML/query converters emit `ClusterArray` variants
    /// when this flag is set; single-node mode emits local `Array` variants.
    pub cluster_enabled: bool,
    /// Bitemporal retention registry — required by `ALTER ARRAY` to
    /// update the purge-scheduler's view of the array's retention policy.
    /// `None` for sub-planners that don't own array DDL.
    pub bitemporal_retention_registry: Option<Arc<BitemporalRetentionRegistry>>,
    /// Per-tenant maximum vector dimension (0 = unlimited). Checked in
    /// `VectorPrimaryInsert` conversion before the task is built.
    pub max_vector_dim: u32,
    /// Database scope for vShard computation. All `VShardId::from_collection_in_database`
    /// calls must use this value so that collections in different databases are
    /// routed to distinct shards and data-plane isolates them correctly.
    pub database_id: crate::types::DatabaseId,
    /// Tenant scope for surrogate identity. Threaded into every surrogate
    /// `assign`/`lookup` so two tenants with the same primary key in a
    /// same-named collection resolve to distinct surrogates.
    pub tenant_id: crate::types::TenantId,
}

impl ConvertContext {
    /// Build the deployment-neutral subset shared with `nodedb-physical`'s
    /// converter helpers. Cheap: 3 `Copy` fields + an `Arc` clone.
    pub fn shared(&self) -> nodedb_physical::SharedConvertContext {
        nodedb_physical::SharedConvertContext {
            database_id: self.database_id,
            max_vector_dim: self.max_vector_dim,
            cluster_enabled: self.cluster_enabled,
            surrogate_assigner: self
                .surrogate_assigner
                .as_ref()
                .map(|a| a.clone() as std::sync::Arc<dyn nodedb_physical::SurrogateAssigner>),
        }
    }
}

/// Convert a list of SqlPlans to PhysicalTasks.
///
/// After each task is produced, any top-level read plan that is a sharded
/// source is wrapped in `Exchange{Gather}` so the coordinator knows to fan
/// it to all Data Plane cores and merge the results. Non-sharded plans
/// (point gets, writes, constant `ProviderScan`s, coordinator-local joins)
/// are left unwrapped.
pub fn convert(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        let mut one = convert_one(plan, tenant_id, ctx)?;
        for task in &mut one {
            if task.plan.is_sharded_source() {
                let as_aggregate = matches!(
                    &task.plan,
                    PhysicalPlan::Query(QueryOp::Aggregate { .. })
                        | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
                );
                // Move the plan out, wrap it in Exchange{Gather}, put it back.
                let sentinel = PhysicalPlan::Query(QueryOp::ProviderScan {
                    provider: None,
                    rows: Vec::new(),
                    filters: Vec::new(),
                    projection: Vec::new(),
                    sort_keys: Vec::new(),
                    limit: None,
                    offset: 0,
                    distinct: false,
                });
                let inner = std::mem::replace(&mut task.plan, sentinel);
                task.plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                    child: Box::new(inner),
                    mode: ExchangeMode::Gather { as_aggregate },
                }));
            }
        }
        tasks.extend(one);
    }
    Ok(tasks)
}

pub(super) fn convert_one(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut visitor = super::visitor::ConvertVisitor { tenant_id, ctx };
    nodedb_sql::dispatch(&mut visitor, plan)
}
