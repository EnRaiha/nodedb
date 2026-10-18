// SPDX-License-Identifier: BUSL-1.1

//! The async resolution of a plan batch's surrogate keys: the keys read off
//! the plans before conversion, and the misses a conversion pass records.

use std::sync::Arc;

use nodedb_sql::types::SqlPlan;
use nodedb_types::{CollectionKey, Surrogate};

use super::super::array_convert::ArrayCellKeys;
use super::super::convert::ConvertContext;
use super::super::dml::{ArrayBatchRows, KeyBatch, plan_key_batches};
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
        let KeyBatch {
            collection,
            pks,
            binds: binding,
            fresh,
            array_rows,
        } = batch;
        let key = ctx.collection_key(collection);
        // An array batch keeps its answers by row, so conversion reads each
        // cell's surrogate by index and no answer enters the key map.
        let keep = if array_rows.is_some() {
            Keep::ByPosition
        } else {
            Keep::ByKey
        };
        let surrogates = {
            let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();
            let (binds, lookups) = if binding {
                (refs, Vec::new())
            } else {
                (Vec::new(), refs)
            };
            let request = KeyRequest {
                key,
                binds: &binds,
                lookups: &lookups,
                fresh,
            };
            resolver.resolve(request, keep, &mut prefetched).await?
        };
        // An array batch's coordinates and keys stay for conversion, which
        // then neither coerces nor encodes a coordinate again.
        if let Some(ArrayBatchRows { rows, coords }) = array_rows {
            prefetched.keep_array_keys(
                key,
                rows,
                ArrayCellKeys {
                    coords,
                    pks,
                    surrogates,
                },
            );
        }
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
        resolver
            .resolve(request, Keep::ByKey, &mut ctx.prefetched)
            .await?;
    }
    Ok(())
}

/// Where a resolve keeps the surrogates its bound keys answer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Keep {
    /// In the answer map, for conversion to look up by key.
    ByKey,
    /// Returned in key order, for the caller to keep by row.
    ByPosition,
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

    /// Resolve `request` into `into`. With [`Keep::ByPosition`] the bound
    /// keys' surrogates return in `binds` order and enter no map; otherwise
    /// the return is empty.
    async fn resolve(
        &self,
        request: KeyRequest<'_>,
        keep: Keep,
        into: &mut PrefetchedSurrogates,
    ) -> crate::Result<Vec<Surrogate>> {
        let KeyRequest {
            key,
            binds,
            lookups,
            fresh,
        } = request;
        if self.metadata {
            // A metadata plan binds nothing: its binding keys are lookups.
            self.look_up(key, binds, into).await?;
            self.look_up(key, lookups, into).await?;
            return Ok(Vec::new());
        }
        if fresh > 0 {
            let drawn = self
                .assigner
                .assign_fresh_many(key, self.tenant_id, fresh)
                .await?;
            for identity in drawn {
                into.push_fresh(key, identity);
            }
        }
        let mut kept = Vec::new();
        if !binds.is_empty() {
            let bound = self
                .assigner
                .assign_many(key, self.tenant_id, binds)
                .await?;
            match keep {
                Keep::ByPosition => kept = bound,
                Keep::ByKey => {
                    for (pk, surrogate) in binds.iter().zip(bound) {
                        into.insert(key, pk, Some(surrogate));
                    }
                }
            }
        }
        self.look_up(key, lookups, into).await?;
        Ok(kept)
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
