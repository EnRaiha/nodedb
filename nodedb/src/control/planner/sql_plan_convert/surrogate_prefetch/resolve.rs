// SPDX-License-Identifier: BUSL-1.1

//! The async resolution of a plan batch's surrogate keys: the keys read off
//! the plans before conversion, and the misses a conversion pass records.

use std::sync::Arc;

use nodedb_sql::types::SqlPlan;
use nodedb_types::CollectionKey;

use super::super::convert::ConvertContext;
use super::super::dml::plan_key_batches;
use super::cache::{PrefetchedSurrogates, SurrogateMisses};
use crate::control::surrogate::SurrogateAssigner;
use crate::types::TenantId;

/// Resolve every surrogate key `plans` convert with before conversion runs,
/// one batch per plan.
///
/// A write that creates its rows binds each absent key: at the key's
/// collection home in a cluster, in this node's catalog on a single node. A
/// metadata plan, and a write that only reads its rows, never binds. Each
/// answer is kept in the returned set, and conversion answers these keys
/// without a request.
///
/// A row that names no key takes a fresh identity, drawn here too. An empty
/// cluster reservation is awaited from the refill loop, never blocked on. A
/// statement that then fails leaves its drawn identities unused.
pub async fn prefetch_plan_surrogates(
    plans: &[SqlPlan],
    ctx: &ConvertContext,
) -> crate::Result<PrefetchedSurrogates> {
    let resolver = Resolver::new(ctx)?;
    let mut prefetched = PrefetchedSurrogates::default();
    for batch in plan_key_batches(plans, ctx) {
        let pks: Vec<&[u8]> = batch.pks.iter().map(Vec::as_slice).collect();
        let (binds, lookups) = if batch.binds {
            (pks, Vec::new())
        } else {
            (Vec::new(), pks)
        };
        let request = KeyRequest {
            key: ctx.collection_key(batch.collection),
            binds: &binds,
            lookups: &lookups,
            fresh: batch.fresh,
        };
        resolver.resolve(request, &mut prefetched).await?;
    }
    Ok(prefetched)
}

/// Resolve the misses one conversion pass recorded into `ctx`'s answers.
pub(super) async fn resolve_misses(
    ctx: &mut ConvertContext,
    misses: &SurrogateMisses,
) -> crate::Result<()> {
    let resolver = Resolver::new(ctx)?;
    for (key, recorded) in misses.iter() {
        let binds: Vec<&[u8]> = recorded.binds.iter().map(Vec::as_slice).collect();
        let lookups: Vec<&[u8]> = recorded.lookups.iter().map(Vec::as_slice).collect();
        let request = KeyRequest {
            key,
            binds: &binds,
            lookups: &lookups,
            fresh: recorded.fresh,
        };
        resolver.resolve(request, &mut ctx.prefetched).await?;
    }
    Ok(())
}

/// The keys of one collection to resolve.
struct KeyRequest<'r> {
    key: CollectionKey<'r>,
    /// Keys bound when absent.
    binds: &'r [&'r [u8]],
    /// Keys only looked up.
    lookups: &'r [&'r [u8]],
    /// Fresh identities to draw.
    fresh: usize,
}

/// Awaits each draw and home request a batch's keys need.
struct Resolver {
    assigner: Arc<SurrogateAssigner>,
    tenant_id: TenantId,
    /// A metadata plan never binds and draws nothing.
    metadata: bool,
    /// Whether keys resolve at a collection home: a cluster node. A single
    /// node's catalog answers every lookup, so conversion reads it directly.
    at_home: bool,
}

impl Resolver {
    fn new(ctx: &ConvertContext) -> crate::Result<Self> {
        let assigner = Arc::clone(&ctx.surrogate_assigner);
        let at_home = assigner.resolves_at_home()?;
        Ok(Self {
            assigner,
            tenant_id: ctx.tenant_id,
            metadata: ctx.is_metadata(),
            at_home,
        })
    }

    async fn resolve(
        &self,
        request: KeyRequest<'_>,
        into: &mut PrefetchedSurrogates,
    ) -> crate::Result<()> {
        let KeyRequest {
            key,
            binds,
            lookups,
            fresh,
        } = request;
        if self.metadata {
            // A metadata plan binds nothing: its binding keys are lookups.
            self.look_up(key, binds, into).await?;
            return self.look_up(key, lookups, into).await;
        }
        for _ in 0..fresh {
            let drawn = self.assigner.assign_fresh(key, self.tenant_id).await?;
            into.push_fresh(key, drawn);
        }
        if !binds.is_empty() {
            let bound = self
                .assigner
                .assign_many(key, self.tenant_id, binds)
                .await?;
            for (pk, surrogate) in binds.iter().zip(bound) {
                into.insert(key, pk, Some(surrogate));
            }
        }
        self.look_up(key, lookups, into).await
    }

    async fn look_up(
        &self,
        key: CollectionKey<'_>,
        pks: &[&[u8]],
        into: &mut PrefetchedSurrogates,
    ) -> crate::Result<()> {
        if pks.is_empty() || !self.at_home {
            return Ok(());
        }
        let found = self.assigner.lookup_many(key, self.tenant_id, pks).await?;
        for (pk, answer) in pks.iter().zip(found) {
            into.insert(key, pk, answer);
        }
        Ok(())
    }
}
