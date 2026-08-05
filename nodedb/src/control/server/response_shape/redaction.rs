// SPDX-License-Identifier: BUSL-1.1

//! Column-level redaction inputs for SELECT response shaping.
//!
//! Redaction is Control-Plane-only work: it needs the requester's roles, which
//! never cross the SPSC bridge, so it is applied to the decoded result rows
//! rather than inside an engine.
//!
//! [`QueryRedaction`] resolves the two per-query inputs — the requester's
//! roles and the plan's source collections — exactly ONCE. [`RedactionCtx`] is
//! the borrowed view handed to the shaper, and a streaming statement builds one
//! from the same `QueryRedaction` for every batch it shapes, so an early batch
//! can never ship rows a later batch would have redacted.

use nodedb_physical::physical_plan::QueryOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::project::is_scan_wrapper;
use crate::control::server::shared::plan_util::extract_collection;
use nodedb_types::TenantId;

/// Everything the redaction hook needs to rewrite one query's result rows.
pub struct RedactionCtx<'a> {
    pub store: &'a RedactionStore,
    pub tenant_id: u64,
    pub roles: &'a [String],
    /// One entry per source collection the plan reads, as
    /// `(qualifier, collection)`. `qualifier` is the prefix that appears on
    /// this collection's keys in a row map — empty for a single-collection
    /// plan, and the join alias (or the collection name when there is no
    /// alias) for each side of a join.
    pub collections: &'a [(String, String)],
}

/// The per-query redaction inputs, resolved once and owned.
///
/// Owned rather than borrowed because a lazy streaming response outlives the
/// handler frame that resolved it: the row generator moves this in and hands
/// out a [`RedactionCtx`] per batch.
#[derive(Clone, Debug)]
pub struct QueryRedaction {
    tenant_id: u64,
    roles: Vec<String>,
    collections: Vec<(String, String)>,
}

impl QueryRedaction {
    /// Resolve the redaction inputs for a statement reading `plan`.
    pub fn for_plan(tenant_id: TenantId, auth: &AuthContext, plan: &PhysicalPlan) -> Self {
        Self::for_collections(tenant_id, auth, plan_source_collections(plan))
    }

    /// Resolve the redaction inputs for a statement whose rows come from
    /// several plans (set-op branches, clone/gateway merges, Calvin batches).
    ///
    /// The union of every branch's sources is used, so a column is redacted
    /// whichever branch produced the row it sits in.
    pub fn for_plans<'p, I>(tenant_id: TenantId, auth: &AuthContext, plans: I) -> Self
    where
        I: IntoIterator<Item = &'p PhysicalPlan>,
    {
        let mut collections: Vec<(String, String)> = Vec::new();
        for plan in plans {
            for source in plan_source_collections(plan) {
                if !collections.contains(&source) {
                    collections.push(source);
                }
            }
        }
        Self::for_collections(tenant_id, auth, collections)
    }

    /// Resolve the redaction inputs from an already-known source list.
    ///
    /// Used by producers with no `PhysicalPlan` in scope (the ClusterArray
    /// coordinator path), which know their collection directly.
    pub fn for_collections(
        tenant_id: TenantId,
        auth: &AuthContext,
        collections: Vec<(String, String)>,
    ) -> Self {
        Self::new(tenant_id, auth.roles.clone(), collections)
    }

    /// Assemble from already-extracted roles and sources.
    pub fn new(
        tenant_id: TenantId,
        roles: Vec<String>,
        collections: Vec<(String, String)>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.as_u64(),
            roles,
            collections,
        }
    }

    /// True when this statement can never redact anything — no roles to match
    /// a policy against, or no source collection to key one on.
    pub fn is_inert(&self) -> bool {
        self.roles.is_empty() || self.collections.is_empty()
    }

    /// Borrow these inputs together with `store` as the shaper's hook input.
    pub fn ctx<'a>(&'a self, store: &'a RedactionStore) -> RedactionCtx<'a> {
        RedactionCtx {
            store,
            tenant_id: self.tenant_id,
            roles: &self.roles,
            collections: &self.collections,
        }
    }
}

/// Redact one raw scan-envelope row in place, leaving its wire shape intact.
///
/// The document scan's `{id, data}` wrapper is unwrapped first so the rules,
/// which name stored fields, match the fields the row actually carries. This
/// is the shared hook for every client-facing path whose rows never reach the
/// named-projection shaping core — the pgwire single-column streamed-text
/// shape and the WS-RPC orchestrated `InsertSelect`/`Merge`/`UpdateFromJoin`
/// RETURNING results both ship whatever the payload decodes to, envelope
/// wrapper included, so redaction has to be applied at this level instead of
/// inside `shape_decoded_rows`.
pub fn redact_envelope_row(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    item: &mut serde_json::Value,
) {
    let Some(resolved) = redaction else {
        return;
    };
    let ctx = resolved.ctx(store);
    let Some(map) = item.as_object_mut() else {
        return;
    };
    let target = if is_scan_wrapper(map) {
        map.get_mut("data")
            .and_then(serde_json::Value::as_object_mut)
    } else {
        Some(map)
    };
    if let Some(fields) = target {
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, fields);
    }
}

/// Redact a decoded WS-RPC result value in place, whichever of the shapes it
/// decoded into.
///
/// WS-RPC's orchestrated `InsertSelect`/`Merge`/`UpdateFromJoin` statements
/// and its generic dispatch path all turn `decode_payload_to_json` output
/// straight into a `serde_json::Value` and hand it to the client, never
/// routing through the named-projection shaping core — see
/// [`redact_envelope_row`]'s doc comment for why that hook exists at this
/// level instead. Three shapes reach here:
///
/// - A JSON array of scan-envelope rows (a plain multi-row SELECT/scan
///   result) — each element is redacted via [`redact_envelope_row`].
/// - A `RowsPayload` DML-`RETURNING` object (`{"columns": [...], "rows":
///   [[cell, ...], ...]}`) — cells are positional, keyed by the sibling
///   `columns` list rather than carried inline per row, so
///   `redact_envelope_row`'s field-keyed matching cannot reach them; these go
///   through [`redact_rows_payload`] instead.
/// - Anything else (a scalar count object like `{"inserted": N}`, or a single
///   scan-envelope row) — redacted via [`redact_envelope_row`], a no-op when
///   there is nothing shaped like a stored field to match.
pub fn redact_decoded_value(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    value: &mut serde_json::Value,
) {
    if redaction.is_none() {
        return;
    }
    if let serde_json::Value::Array(items) = value {
        for item in items {
            redact_envelope_row(redaction, store, item);
        }
        return;
    }
    if is_rows_payload_shape(value) {
        redact_rows_payload(redaction, store, value);
        return;
    }
    redact_envelope_row(redaction, store, value);
}

/// True when `value` has the `RowsPayload` DML-`RETURNING` shape: a
/// `"columns"` array of strings alongside a `"rows"` array of arrays.
///
/// This is a structural check, not a plan-driven one — the 4 WS-RPC sites
/// that call [`redact_decoded_value`] cover every `DocumentOp` variant that
/// can carry a `ReturningSpec` (point/bulk update, point/bulk delete, the
/// join orchestrators), so matching on the decoded shape instead of
/// enumerating those variants keeps this one check correct as new
/// `RETURNING`-capable ops are added.
fn is_rows_payload_shape(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    let Some(columns) = obj.get("columns").and_then(serde_json::Value::as_array) else {
        return false;
    };
    if columns.is_empty() || !columns.iter().all(serde_json::Value::is_string) {
        return false;
    }
    let Some(rows) = obj.get("rows").and_then(serde_json::Value::as_array) else {
        return false;
    };
    rows.iter().all(serde_json::Value::is_array)
}

/// Redact one decoded `RowsPayload` RETURNING response in place.
///
/// Each row is positional (`rows[i][j]` is the value of `columns[j]`), so it
/// is round-tripped through [`RedactionStore::apply_flat_row`]'s name-keyed
/// matching by zipping it into a scratch map keyed by `columns`, then the
/// (possibly rewritten) cells are written back at their original positions —
/// the `{"columns": ..., "rows": ...}` wire shape itself never changes.
pub fn redact_rows_payload(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    item: &mut serde_json::Value,
) {
    let Some(resolved) = redaction else {
        return;
    };
    let ctx = resolved.ctx(store);
    let Some(obj) = item.as_object_mut() else {
        return;
    };
    let columns: Vec<String> = match obj.get("columns").and_then(serde_json::Value::as_array) {
        Some(cols) => cols
            .iter()
            .filter_map(|c| c.as_str().map(str::to_string))
            .collect(),
        None => return,
    };
    if columns.is_empty() {
        return;
    }
    let Some(rows) = obj
        .get_mut("rows")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    // One scratch map for the whole payload, cleared per row: `apply_flat_row`
    // is name-keyed while the wire rows are positional, so each row has to be
    // zipped into a map and written back. Allocating that map per row would put
    // an allocation on every RETURNING row of every request.
    let mut scratch: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for row in rows {
        let Some(cells) = row.as_array_mut() else {
            continue;
        };
        if cells.len() != columns.len() {
            continue;
        }
        scratch.clear();
        for (col, cell) in columns.iter().zip(cells.iter()) {
            scratch.insert(col.clone(), cell.clone());
        }
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut scratch);
        for (cell, col) in cells.iter_mut().zip(columns.iter()) {
            if let Some(v) = scratch.get(col) {
                *cell = v.clone();
            }
        }
    }
}

/// The source collections a plan reads, as `(qualifier, collection)`.
///
/// `qualifier` is the prefix the executor puts on that source's columns in a
/// result row: empty for a single-collection plan, the alias (or collection
/// name) per side for a join or LATERAL.
///
/// This deliberately does NOT use `extract_collection` alone: that helper
/// reports only the LEFT side of a join, which would leave every right-side
/// column unredacted.
pub fn plan_source_collections(plan: &PhysicalPlan) -> Vec<(String, String)> {
    let mut out = Vec::new();
    collect_sources(plan, "", &mut out);
    out
}

/// Walk `plan`, attributing each source it reads to `qualifier`.
///
/// Only the relational [`QueryOp`]s can introduce a second source or rename a
/// qualifier; every other plan reads at most one collection, and resolving
/// that is delegated to `extract_collection`, whose match over `PhysicalPlan`
/// is exhaustive — so a new plan variant still forces a decision there.
fn collect_sources(plan: &PhysicalPlan, qualifier: &str, out: &mut Vec<(String, String)>) {
    if let PhysicalPlan::Query(op) = plan {
        match op {
            // A join side's rows may come from a resolved child plan
            // (`*_input`) or from a local scan of `*_collection`. The side's
            // qualifier prefixes its columns either way, so the collection is
            // recorded unconditionally and the child, when present, is walked
            // under that same qualifier.
            QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                left_input,
                right_input,
                ..
            } => {
                let left = left_alias.as_deref().unwrap_or(left_collection.as_str());
                push_source(out, left, left_collection);
                if let Some(child) = left_input {
                    collect_sources(child, left, out);
                }
                let right = right_alias.as_deref().unwrap_or(right_collection.as_str());
                push_source(out, right, right_collection);
                if let Some(child) = right_input {
                    collect_sources(child, right, out);
                }
                return;
            }
            // Neither variant takes a resolved child input: both sides are
            // always scanned locally, and neither carries an alias, so the
            // collection name is the qualifier.
            QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                ..
            }
            | QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                ..
            } => {
                push_source(out, left_collection, left_collection);
                push_source(out, right_collection, right_collection);
                return;
            }
            QueryOp::LateralTopK {
                outer_plan,
                outer_alias,
                inner_collection,
                lateral_alias,
                ..
            }
            | QueryOp::LateralLoop {
                outer_plan,
                outer_alias,
                inner_collection,
                lateral_alias,
                ..
            } => {
                collect_sources(outer_plan, outer_alias, out);
                push_source(out, lateral_alias, inner_collection);
                return;
            }
            QueryOp::Exchange(exchange) => {
                collect_sources(&exchange.child, qualifier, out);
                return;
            }
            QueryOp::PostProcess { input, .. } => {
                collect_sources(input, qualifier, out);
                return;
            }
            QueryOp::Aggregate {
                collection, input, ..
            }
            | QueryOp::PartialAggregateState {
                collection, input, ..
            } => {
                push_source(out, qualifier, collection);
                if let Some(child) = input {
                    collect_sources(child, qualifier, out);
                }
                return;
            }
            // Every remaining relational op reads at most the single
            // collection `extract_collection` reports, resolved below.
            _ => {}
        }
    }

    if let Some(collection) = extract_collection(plan) {
        push_source(out, qualifier, collection);
    }
}

fn push_source(out: &mut Vec<(String, String)>, qualifier: &str, collection: &str) {
    if out
        .iter()
        .any(|(q, c)| q.as_str() == qualifier && c.as_str() == collection)
    {
        return;
    }
    out.push((qualifier.to_string(), collection.to_string()));
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, ExchangeMode, ExchangeOp};

    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    use super::*;

    fn store_with_mask(collection: &str, role: &str, field: &str, mask: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask(mask.into()),
            }],
        });
        store
    }

    fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
        QueryRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            vec![(String::new(), collection.to_string())],
        )
    }

    /// `UPDATE ... FROM <source> RETURNING <col>` (autocommit, orchestrated
    /// via `update_from_join_orchestrator`) encodes its response as exactly
    /// this `{"columns": [...], "rows": [[...]]}` shape — see
    /// `data::executor::handlers::returning_rows::build_rows_payload`. Before
    /// wiring `redact_decoded_value` into the WS-RPC dispatch loop, this shape
    /// shipped over WS-RPC untouched: `redact_envelope_row` alone cannot reach
    /// it, since its cells are positional rather than name-keyed. This is the
    /// regression guard for that leak.
    #[test]
    fn redact_rows_payload_masks_the_ruled_column_by_position() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"], ["2", "d@e.f"]],
        });

        redact_rows_payload(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][0], "1");
        assert_eq!(value["rows"][0][1], "***");
        assert_eq!(value["rows"][1][0], "2");
        assert_eq!(value["rows"][1][1], "***");
        // The wire shape itself — column list, row count, cell positions —
        // must be untouched, only the ruled cell's value.
        assert_eq!(value["columns"], serde_json::json!(["id", "email"]));
    }

    /// A role with no matching policy must see the RETURNING cells in the
    /// clear — the fix must not over-redact.
    #[test]
    fn redact_rows_payload_leaves_unruled_role_untouched() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "analyst");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });

        redact_rows_payload(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][1], "a@b.c");
    }

    /// The dispatcher `redact_decoded_value` — the entry point wired into
    /// every WS-RPC result-decode site — must route the `RowsPayload` shape
    /// to `redact_rows_payload` rather than treating it as a plain object
    /// (which would be a silent no-op, since it has no `email` key to match).
    #[test]
    fn redact_decoded_value_routes_rows_payload_shape_correctly() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][1], "***");
    }

    /// A plain multi-row scan array — the shape a generic (non-RETURNING)
    /// WS-RPC dispatch decodes into — must still be redacted per-element via
    /// `redact_envelope_row`, unwrapping each element's `{id, data}` wrapper.
    #[test]
    fn redact_decoded_value_routes_array_of_envelope_rows_correctly() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!([
            {"id": "1", "data": {"email": "a@b.c"}},
            {"id": "2", "data": {"email": "d@e.f"}},
        ]);

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value[0]["data"]["email"], "***");
        assert_eq!(value[1]["data"]["email"], "***");
    }

    /// A scalar command-tag object (`{"affected": N}` / `{"inserted": N}`,
    /// the shape non-RETURNING orchestrated statements return) must survive
    /// `redact_decoded_value` unchanged — it has no ruled field to match.
    #[test]
    fn redact_decoded_value_leaves_scalar_count_object_untouched() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({ "affected": 3 });

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value, serde_json::json!({ "affected": 3 }));
    }

    /// `None` redaction (no policy could possibly apply) must be a hard
    /// no-op, never a panic, across every shape.
    #[test]
    fn redact_decoded_value_is_a_no_op_without_a_resolved_redaction() {
        let store = RedactionStore::new();
        let mut rows_payload = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });
        let mut array = serde_json::json!([{"id": "1", "data": {"email": "a@b.c"}}]);

        redact_decoded_value(None, &store, &mut rows_payload);
        redact_decoded_value(None, &store, &mut array);

        assert_eq!(rows_payload["rows"][0][1], "a@b.c");
        assert_eq!(array[0]["data"]["email"], "a@b.c");
    }

    /// A minimal single-collection leaf plan.
    fn scan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: collection.to_string(),
            field: "id".to_string(),
        })
    }

    fn hash_join(
        left_alias: Option<&str>,
        right_alias: Option<&str>,
        left_input: Option<PhysicalPlan>,
    ) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "workspaces".into(),
            right_collection: "boards".into(),
            left_alias: left_alias.map(str::to_string),
            right_alias: right_alias.map(str::to_string),
            on: Vec::new(),
            join_type: "inner".into(),
            limit: 0,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: left_input.map(Box::new),
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
        })
    }

    #[test]
    fn single_collection_plan_uses_an_empty_qualifier() {
        assert_eq!(
            plan_source_collections(&scan("users")),
            vec![(String::new(), "users".to_string())]
        );
    }

    /// Both join sides must appear — the whole reason this does not reuse
    /// `extract_collection`, which reports only the left one.
    #[test]
    fn join_reports_both_sides_under_their_aliases() {
        assert_eq!(
            plan_source_collections(&hash_join(Some("w"), Some("b"), None)),
            vec![
                ("w".to_string(), "workspaces".to_string()),
                ("b".to_string(), "boards".to_string()),
            ]
        );
    }

    /// An unaliased side qualifies its columns with the collection name.
    #[test]
    fn join_without_aliases_qualifies_by_collection_name() {
        assert_eq!(
            plan_source_collections(&hash_join(None, None, None)),
            vec![
                ("workspaces".to_string(), "workspaces".to_string()),
                ("boards".to_string(), "boards".to_string()),
            ]
        );
    }

    /// A resolved child plan is walked under its side's qualifier, so a
    /// coordinator-resolved join side is still attributed.
    #[test]
    fn join_child_input_inherits_its_sides_qualifier() {
        let sources =
            plan_source_collections(&hash_join(Some("w"), Some("b"), Some(scan("audit"))));
        assert!(sources.contains(&("w".to_string(), "audit".to_string())));
        assert!(sources.contains(&("b".to_string(), "boards".to_string())));
    }

    /// Exchange is transparent: a gathered scan is still a single-collection
    /// plan with an empty qualifier.
    #[test]
    fn exchange_is_transparent() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(scan("users")),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        assert_eq!(
            plan_source_collections(&plan),
            vec![(String::new(), "users".to_string())]
        );
    }
}
