// SPDX-License-Identifier: BUSL-1.1

//! SQL-text gateway execution with descriptor-version-aware plan caching.

use std::sync::Arc;

use tracing::debug;

use crate::Error;
use crate::control::server::shared::authorization::AuthorizedTask;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::core::{Gateway, QueryContext, authorized_plan_for_context};
use super::plan_cache::{PlanCacheKey, SqlKey, hash_placeholder_types, hash_sql};

impl Gateway {
    /// Execute SQL through the two-phase, descriptor-version-aware plan cache.
    ///
    /// `plan_fn` runs at most once on a cache miss. `authorize_fn` mints the
    /// exact capability for either the cached plan or the newly planned value.
    pub async fn execute_sql(
        &self,
        ctx: &QueryContext,
        sql: &str,
        placeholder_types: &[&str],
        plan_fn: impl FnOnce() -> Result<PhysicalPlan, Error>,
        authorize_fn: impl Fn(&PhysicalPlan) -> Result<AuthorizedTask, Error>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let sql_hash = hash_sql(sql);
        let ph_hash = hash_placeholder_types(placeholder_types);
        let sql_key = SqlKey {
            sql_text_hash: sql_hash,
            placeholder_types_hash: ph_hash,
        };

        // Recover the prior version set, verify it against current catalog
        // descriptors, then use the complete cache key only while current.
        if let Some(stored_vs) = self.plan_cache.lookup_version_set(&sql_key) {
            let shared = self.shared()?;
            let catalog = shared.credentials.catalog();
            let current_vs = stored_vs.reverify(|name| {
                catalog
                    .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), name)
                    .ok()
                    .flatten()
                    .map(|collection| collection.descriptor_version.max(1))
                    .unwrap_or(0)
            });
            if current_vs == stored_vs {
                let full_key = PlanCacheKey {
                    sql_text_hash: sql_hash,
                    placeholder_types_hash: ph_hash,
                    version_set: stored_vs.clone(),
                };
                if let Some(cached_plan) = self.plan_cache.get(&full_key) {
                    debug!(sql = %sql, "gateway: plan cache hit (two-phase)");
                    let authorized = authorize_fn(cached_plan.as_ref())?;
                    let plan = authorized_plan_for_context(ctx, authorized)?;
                    return self
                        .execute_with_version_set(ctx, plan, stored_vs)
                        .await
                        .map(|(payloads, _watermarks, _read_version)| payloads);
                }
            }
        }

        let plan = plan_fn()?;
        let actual_vs = self.collect_version_set(&plan, ctx.tenant_id.as_u64(), ctx.database_id)?;
        let actual_key = PlanCacheKey {
            sql_text_hash: sql_hash,
            placeholder_types_hash: ph_hash,
            version_set: actual_vs.clone(),
        };

        self.plan_cache
            .insert_version_set(sql_key, actual_vs.clone());
        self.plan_cache.insert(actual_key, Arc::new(plan.clone()));

        let authorized = authorize_fn(&plan)?;
        let plan = authorized_plan_for_context(ctx, authorized)?;
        self.execute_with_version_set(ctx, plan, actual_vs)
            .await
            .map(|(payloads, _watermarks, _read_version)| payloads)
    }
}
