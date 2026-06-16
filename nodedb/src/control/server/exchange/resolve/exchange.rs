// SPDX-License-Identifier: BUSL-1.1

//! Pass 2 of plan resolution: Exchange node resolution.
//!
//! - `Gather{as_aggregate}` at the plan root → fan child to all vShards,
//!   merge, and return `Resolved::Gathered`.
//! - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!   gather child to coordinator, encode as a merged msgpack array, and
//!   embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!   self-contained and returned as `Resolved::Plan`.
//! - `Shuffle{..}` → typed error (reserved seam, not implemented).
//! - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, ExchangeMode, ExchangeOp, KvOp, PhysicalPlan, QueryOp, TimeseriesOp,
};
use nodedb_types::{CollectionType, ColumnarProfile, DocumentMode, SystemTimeScope};

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::flatten_to_relational_rows;
use crate::types::{DatabaseId, TenantId, TraceId};

use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_cores, gather_all_cores_stream,
    gather_all_vshards, outcome_to_response,
};
use crate::control::server::result_stream::ResultStream;

use super::materialize::materialize_providers;

/// Result of `resolve_and_materialize`.
pub enum Resolved {
    /// The plan was a root-level `Gather` — the coordinator has already
    /// executed it and the response is ready to return to the client.
    Gathered(Response),
    /// The plan (possibly mutated by catalog materialization or Broadcast
    /// embedding) is self-contained and should be dispatched normally.
    Plan(PhysicalPlan),
    /// The plan was a single-node, unordered, non-aggregate scan eligible for
    /// streaming. The coordinator has eagerly dispatched it to all cores; the
    /// carried [`ResultStream`] yields row batches as they arrive. The pgwire
    /// path surfaces this lazily to the client; all other consumers
    /// `materialize` it back into a `Response`/bytes (behaviour-preserving).
    Stream(ResultStream),
}

/// Materialize catalog providers and resolve Exchange nodes in `plan`.
///
/// See module-level documentation for the two-pass behaviour.
pub async fn resolve_and_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    // Pass 1: fill empty ProviderScan rows (identity-scoped, per-request).
    let plan = materialize_providers(state, identity, plan).await?;

    // Pass 2: resolve Exchange nodes.
    resolve_exchange(state, database_id, tenant_id, plan, trace_id).await
}

/// Resolve only `Exchange` nodes (pass 2), without catalog provider
/// materialization. Used by the shared `dispatch_to_data_plane` funnel so that
/// internal query consumers (COPY, cursors, materialized-view refresh,
/// constraint subqueries) — which build `Exchange{Gather}`-wrapped read plans
/// over user tables but never carry catalog providers — still fan out and merge
/// correctly. Identity-free: catalog materialization happens earlier on the
/// pgwire/native paths that own the request identity. A no-op for plans with no
/// `Exchange` node.
pub async fn resolve_exchange_in_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    resolve_exchange(state, database_id, tenant_id, plan, trace_id).await
}

// ── pass 2 ───────────────────────────────────────────────────────────────────

/// Resolve any `Exchange` nodes in `plan`.
///
/// - Root-level `Gather` → gather all vShards, return `Resolved::Gathered`.
/// - `Broadcast` nested inside a `HashJoin` input → gather the child, embed
///   the `merged_array` as `ProviderScan{None, rows}`, return `Resolved::Plan`.
/// - `Shuffle` → typed error.
/// - Anything else → `Resolved::Plan` unchanged.
async fn resolve_exchange(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    match plan {
        // Root-level Gather: fan child to all vShards and merge. First resolve any
        // Exchange{Broadcast} nodes nested inside the child (e.g. a HashJoin's
        // build side) so the plan fanned to cores is self-contained — no
        // Exchange node may reach a Data-Plane core.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let child = match Box::pin(resolve_exchange(
                state,
                database_id,
                tenant_id,
                *child,
                trace_id,
            ))
            .await?
            {
                Resolved::Plan(p) => p,
                Resolved::Gathered(resp) => return Ok(Resolved::Gathered(resp)),
                // A nested Exchange that itself resolved to a stream cannot be
                // re-wrapped by an outer Gather without materializing first;
                // surface it as the stream (the outer Gather is redundant —
                // nested root-level Gathers do not occur in practice, but if one
                // did, the inner stream is already the correct result).
                Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
            };

            // Streaming fast path: a non-aggregate, unordered scan can stream
            // straight to the client without coordinator-side materialization.
            //
            // - Single-node (`gateway.is_none()`): fan to all local cores via
            //   `gather_all_cores_stream`.
            // - Cluster (`gateway.is_some()`): `gateway.execute_stream` routes
            //   the scan to its owning vShard — local cores when this node owns
            //   it, or the remote owner over QUIC (L4 streaming transport) —
            //   and merges the per-route streams with the same `select_all`.
            //
            // Aggregate gathers keep the materialize-then-merge behaviour.
            if !as_aggregate && child.is_streamable_unordered_scan() {
                let stream = if let Some(gw) = state.gateway.as_ref() {
                    let ctx = crate::control::gateway::core::QueryContext {
                        tenant_id,
                        trace_id,
                        database_id,
                    };
                    gw.execute_stream(&ctx, child).await?
                } else {
                    gather_all_cores_stream(state, tenant_id, database_id, child, trace_id)?
                };
                return Ok(Resolved::Stream(stream));
            }

            let outcome: GatherOutcome =
                gather_all_vshards(state, tenant_id, database_id, child, trace_id).await?;
            let payload = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            Ok(Resolved::Gathered(outcome_to_response(
                payload,
                outcome.watermark_lsn,
            )))
        }

        // Root-level Broadcast: unusual but treat as Gather without merge.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            let outcome =
                gather_all_vshards(state, tenant_id, database_id, *child, trace_id).await?;
            Ok(Resolved::Gathered(outcome_to_response(
                outcome.merged_array,
                outcome.watermark_lsn,
            )))
        }

        // Shuffle: reserved seam, not implemented.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "distributed shuffle is not yet implemented".into(),
        }),

        // HashJoin: resolve Broadcast children embedded in left_input / right_input.
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }) => {
            let left_input =
                resolve_join_input(state, database_id, tenant_id, left_input, trace_id).await?;
            let mut right_input =
                resolve_join_input(state, database_id, tenant_id, right_input, trace_id).await?;

            // Cross-node build-side gather (cluster only).
            //
            // The HashJoin task routes to the LEFT (probe) collection's owning
            // vShard, where the LEFT side is scanned locally. The RIGHT (build)
            // collection is otherwise scanned BY NAME from that same node — but
            // a single-vShard-homed build collection may live on a DIFFERENT
            // node, so the by-name scan returns nothing and the join drops rows.
            //
            // When running in cluster mode (`gateway.is_some()`), and the build
            // side has not already been materialized by `resolve_join_input`
            // (i.e. `right_input` is still `None`), and `right_collection` names
            // a real user collection (catalog sides carry an empty name and are
            // already embedded as a ProviderScan), gather the build collection
            // across all vShards on the coordinator and inline it as a
            // `ProviderScan`. The HashJoin shipped to the probe node is then
            // self-contained. Only the RIGHT/build side is gathered; the
            // LEFT/probe side stays local to the routed vShard.
            if state.gateway.is_some() && right_input.is_none() && !right_collection.is_empty() {
                right_input = gather_join_build_side(
                    state,
                    database_id,
                    tenant_id,
                    &right_collection,
                    trace_id,
                )
                .await?;
            }

            Ok(Resolved::Plan(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                post_group_by,
                post_aggregates,
                projection,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
            })))
        }

        // All other plan variants: pass through unchanged.
        other => Ok(Resolved::Plan(other)),
    }
}

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, merged_array}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
async fn resolve_join_input(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    input: Option<Box<PhysicalPlan>>,
    trace_id: TraceId,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    let Some(boxed) = input else {
        return Ok(None);
    };

    match *boxed {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            // Gather the broadcast child to the coordinator, then embed the
            // merged msgpack array as an inline ProviderScan.
            //
            // We use `merged_array` (not `raw`) because `merged_array` is a
            // single well-formed msgpack array.  The Data-Plane executor
            // materialises the ProviderScan via `response_with_payload(rows)`,
            // producing a Response whose payload is exactly `merged_array`.
            // `decode_response_to_docs` in `hash_handlers.rs` then reads that
            // Response as a msgpack array — so the two shapes match.
            let outcome = gather_all_cores(state, tenant_id, database_id, *child, trace_id).await?;
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&outcome.merged_array),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Exchange{Shuffle} inside a join input: error.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "distributed shuffle in join input is not yet implemented".into(),
        }),

        // Exchange{Gather} inside a join input: unusual but execute and embed.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let outcome = gather_all_cores(state, tenant_id, database_id, *child, trace_id).await?;
            let merged = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&merged),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Already resolved (ProviderScan{None, ..} or any other plan):
        // pass through.
        other => Ok(Some(Box::new(other))),
    }
}

/// Gather a HashJoin build collection across all vShards and inline it as a
/// `ProviderScan` (cluster mode only).
///
/// Looks up `collection`'s engine in the catalog, builds a minimal unfiltered
/// full-collection scan for that engine, gathers it across all vShards via the
/// gateway, and embeds the merged rows as a `ProviderScan{provider: None, rows}`
/// — mirroring the embedding shape used by `resolve_join_input`.
///
/// Returns `Ok(None)` (the name-scan fallback) when the catalog has no record
/// for `collection`. This is a graceful degradation, never an error: a missing
/// catalog entry on the coordinator falls back to the existing by-name scan on
/// the executing node.
async fn gather_join_build_side(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    trace_id: TraceId,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Catalog lookup: identity-free, scoped by database_id + tenant_id. The
    // collection name carried in the HashJoin is the same (possibly
    // db-qualified) name used as the scan-plan collection and the catalog key,
    // matching the gateway's `collect_version_set` lookup convention.
    let catalog_ref = state.credentials.catalog();
    let Some(catalog) = catalog_ref.as_ref() else {
        // No catalog on this node: fall back to name-scan.
        return Ok(None);
    };
    let stored = match catalog.get_collection(database_id, tenant_id.as_u64(), collection)? {
        Some(s) => s,
        // Unknown collection: fall back to name-scan (do not error).
        None => return Ok(None),
    };

    // The build side of a hash join must be COMPLETE — every build row is needed
    // for correct match output, so the gather scan is unbounded (no row cap). A
    // fixed cap would silently drop join matches for larger collections. This is
    // allocation-safe: the scan path sizes its buffer as `with_capacity(limit
    // .min(256))` and bounds output with `take(limit)` (see `btree_scan.rs`), and
    // `fetch_limit` uses `saturating_mul`, so `usize::MAX` returns all rows
    // without pre-allocating or overflowing.
    //
    // (Memory for a very large build relation is the inherent cost of a hash
    // join; spill-to-disk is a future optimization, not a reason to truncate. The
    // probe side's local name-scan and the converter's 10k default for unbounded
    // SELECTs remain separately capped — that engine-wide unbounded-scan limit is
    // its own effort and is the remaining truncation source, TRACKED.)
    const COMPLETE_BUILD_SIDE: usize = usize::MAX;

    // Build a minimal, unfiltered, unprojected full-collection scan for the
    // engine. This matches the executor's by-name build-side scan semantics
    // (`scan_collection`), which applies no build-side filter today. Match the
    // engine EXHAUSTIVELY — `CollectionType` (and its nested profiles/modes) is
    // the closed set of catalog-creatable engines, so every variant is handled
    // and there is no name-scan fallback for "unsupported engine". The Array
    // engine is intentionally absent: it is not a `CollectionType` variant
    // (Array uses its own `CREATE ARRAY` DDL and never appears as a
    // `StoredCollection` here), so it cannot reach this path.
    let scan_plan = match &stored.collection_type {
        CollectionType::Document(DocumentMode::Schemaless)
        | CollectionType::Document(DocumentMode::Strict(_)) => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: collection.into(),
                limit: COMPLETE_BUILD_SIDE,
                offset: 0,
                sort_keys: Vec::new(),
                filters: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            })
        }
        CollectionType::KeyValue(_) => PhysicalPlan::Kv(KvOp::Scan {
            collection: collection.into(),
            cursor: Vec::new(),
            count: COMPLETE_BUILD_SIDE,
            filters: Vec::new(),
            match_pattern: None,
            sort_keys: Vec::new(),
            surrogate_ceiling: None,
        }),
        CollectionType::Columnar(ColumnarProfile::Plain)
        | CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => {
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: collection.into(),
                projection: Vec::new(),
                limit: COMPLETE_BUILD_SIDE,
                filters: Vec::new(),
                rls_filters: Vec::new(),
                sort_keys: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            })
        }
        CollectionType::Columnar(ColumnarProfile::Timeseries { .. }) => {
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: collection.into(),
                // (0, i64::MAX) = no time filter — scan all rows.
                time_range: (0, i64::MAX),
                projection: Vec::new(),
                limit: COMPLETE_BUILD_SIDE,
                filters: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            })
        }
    };

    // `Box::pin` breaks the async-fn recursion cycle: `gather_all_vshards`
    // dispatches through the gateway, which re-enters `resolve_exchange_in_plan`
    // → `resolve_exchange` → here. The cycle terminates at runtime (the scan
    // plan is Exchange-free), but the future must be heap-indirected so its size
    // is finite.
    let outcome = Box::pin(gather_all_vshards(
        state,
        tenant_id,
        database_id,
        scan_plan,
        trace_id,
    ))
    .await?;

    Ok(Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: flatten_to_relational_rows(&outcome.merged_array),
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    }))))
}
