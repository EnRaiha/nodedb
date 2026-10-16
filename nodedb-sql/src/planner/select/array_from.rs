// SPDX-License-Identifier: Apache-2.0

//! `FROM ARRAY_*(...)`: an array table-function call as a SELECT's source.
//!
//! `ARRAY_SLICE` and `ARRAY_PROJECT` read cells. Their rows form a relation
//! whose columns are the array's dims and the requested attrs, so the SELECT
//! plans over it like over a derived table: the call becomes the one
//! definition of a CTE, and the SELECT reads that definition by name through
//! the generic relational tail (WHERE, projection, GROUP BY, HAVING, ORDER
//! BY, DISTINCT, LIMIT / OFFSET, windows).
//!
//! `ARRAY_AGG` and `ARRAY_ELEMENTWISE` emit rows of their own shape. A
//! SELECT over them takes `*` or plain output columns and a row bound;
//! every other clause is refused by name.

use sqlparser::ast::{self, Select};

use super::cte_catalog::CteCatalog;
use super::helpers::{convert_projection, plain_table_factor};
use super::query_tail::QueryTail;
use super::select_stmt::{PlannedSelect, has_aggregation, plan_select};
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::array_fn::{ArrayTableFn, push_down_into_slice, try_plan_array_table_fn};
use crate::planner::ast_helpers::{single_table_qualifiers, strip_single_table_qualifiers};
use crate::resolver::array_tvf::{
    ARRAY_SYSTEM_TIME_COLUMN, array_relation_columns, array_slice_columns,
};
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan a SELECT whose FROM is one array table-function call. Returns
/// `Ok(None)` when the FROM is anything else.
pub(in crate::planner::select) fn try_plan_array_from(
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
    tail: &QueryTail<'_>,
    statement_output: bool,
) -> Result<Option<PlannedSelect>> {
    let Some(call) = try_plan_array_table_fn(&select.from, catalog, temporal)? else {
        return Ok(None);
    };
    let ctx = OuterContext {
        select,
        catalog,
        functions,
        tail,
        statement_output,
    };
    let planned = match call {
        ArrayTableFn::Slice(mut slice) => {
            let view = array_view(catalog, &slice.name)?;
            let read = RelationRead::new(&ctx, "ARRAY_SLICE", &view.name)?;
            let columns = array_slice_columns(&view, &slice);
            let outer = read.plan_outer(&ctx, columns)?;
            push_down_into_slice(&mut slice, &view, &outer.plan, &read.name);
            read.into_cte(SqlPlan::ArraySlice(slice), outer)
        }
        ArrayTableFn::Project(project) => {
            let view = array_view(catalog, &project.name)?;
            let read = RelationRead::new(&ctx, "ARRAY_PROJECT", &view.name)?;
            let columns = array_relation_columns(&view, &project.attr_projection);
            let outer = read.plan_outer(&ctx, columns)?;
            read.into_cte(SqlPlan::ArrayProject(project), outer)
        }
        call @ (ArrayTableFn::Agg(_) | ArrayTableFn::Elementwise(_)) => {
            plan_non_relational(&ctx, temporal, call)?
        }
    };
    Ok(Some(planned))
}

/// The parts of the enclosing SELECT every arm reads.
struct OuterContext<'a> {
    select: &'a Select,
    catalog: &'a dyn SqlCatalog,
    functions: &'a FunctionRegistry,
    tail: &'a QueryTail<'a>,
    statement_output: bool,
}

/// The relation one cell-reading call exposes to the SELECT over it.
struct RelationRead {
    /// The name the outer query reads the rows by. No stored relation or
    /// enclosing CTE holds it, so a subquery that names a real collection
    /// never resolves to the array.
    name: String,
    /// The qualifier column references use: the FROM alias, or the array
    /// name when the call has none.
    qualifier: ast::Ident,
}

impl RelationRead {
    fn new(ctx: &OuterContext<'_>, fn_name: &str, array: &str) -> Result<Self> {
        let alias = match &ctx.select.from[0].relation {
            ast::TableFactor::Table { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if let Some(alias) = alias
            && !alias.columns.is_empty()
        {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "{fn_name}: a column alias list on '{}' is not supported; \
                     alias each column in the SELECT list instead",
                    alias.name
                ),
            });
        }
        let qualifier = match alias {
            Some(alias) => alias.name.clone(),
            None => ast::Ident::with_quote('"', array),
        };
        Ok(Self {
            name: unused_relation_name(ctx.catalog, &format!("{fn_name}({array})"))?,
            qualifier,
        })
    }

    /// Plan the SELECT as a read of this relation, whose rows carry `columns`.
    fn plan_outer(
        &self,
        ctx: &OuterContext<'_>,
        columns: Vec<ColumnInfo>,
    ) -> Result<PlannedSelect> {
        let star_columns: Vec<String> = columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|name| name != ARRAY_SYSTEM_TIME_COLUMN)
            .collect();
        let info = CollectionInfo {
            name: self.name.clone(),
            engine: EngineType::DocumentSchemaless,
            columns,
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: false,
        };
        let relation_catalog = CteCatalog {
            inner: ctx.catalog,
            relations: vec![(self.name.clone(), info)],
        };
        let qualifier = crate::reserved::check_ast_identifier(&self.qualifier)?;
        let mut outer_select = ctx.select.clone();
        outer_select.from[0].relation = plain_table_factor(
            ast::Ident::with_quote('"', self.name.clone()),
            Some(ast::TableAlias {
                explicit: true,
                name: self.qualifier.clone(),
                columns: Vec::new(),
                at: None,
            }),
        );
        outer_select.projection =
            expand_wildcards(&ctx.select.projection, &qualifier, &star_columns)?;
        // The temporal qualifier reads the array's versions and rides on the
        // slice plan. The relation the SELECT reads is those versions'
        // rows, with no history of its own.
        plan_select(
            &outer_select,
            &relation_catalog,
            ctx.functions,
            TemporalScope::default(),
            ctx.tail,
            ctx.statement_output,
        )
    }

    fn into_cte(self, source: SqlPlan, outer: PlannedSelect) -> PlannedSelect {
        PlannedSelect {
            plan: SqlPlan::Cte(CtePlan {
                definitions: vec![(self.name, source)],
                outer: Box::new(outer.plan),
            }),
            scope: outer.scope,
        }
    }
}

fn array_view(catalog: &dyn SqlCatalog, name: &str) -> Result<ArrayCatalogView> {
    catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })
}

/// `base`, or `base#N` for the first `N` from 2 that no relation the
/// catalog resolves holds.
fn unused_relation_name(catalog: &dyn SqlCatalog, base: &str) -> Result<String> {
    let mut candidate = base.to_string();
    let mut n: u32 = 1;
    while catalog
        .resolve_relation(nodedb_types::DatabaseId::DEFAULT, &candidate)?
        .is_some()
    {
        n = n.checked_add(1).ok_or_else(|| SqlError::Unsupported {
            detail: format!("no free relation name for {base}"),
        })?;
        candidate = format!("{base}#{n}");
    }
    Ok(candidate)
}

/// The SELECT list with each plain `*`, and each `<qualifier>.*`, replaced by
/// one column per name in `columns`, in that order.
///
/// The relation's columns are known, so the result announces them in
/// schema order with their types. A wildcard with options (`EXCLUDE`,
/// `REPLACE`, ...) or another qualifier stays as written.
fn expand_wildcards(
    projection: &[ast::SelectItem],
    qualifier: &str,
    columns: &[String],
) -> Result<Vec<ast::SelectItem>> {
    let mut out = Vec::with_capacity(projection.len() + columns.len());
    for item in projection {
        let expands = match item {
            ast::SelectItem::Wildcard(options) => is_plain_wildcard(options),
            ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) => {
                is_plain_wildcard(options)
                    && crate::parser::normalize::normalize_object_name_checked(name)? == qualifier
            }
            ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::Expr(_),
                _,
            )
            | ast::SelectItem::UnnamedExpr(_)
            | ast::SelectItem::ExprWithAlias { .. }
            | ast::SelectItem::ExprWithAliases { .. } => false,
        };
        if expands {
            out.extend(columns.iter().map(|name| {
                ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident::with_quote(
                    '"',
                    name.clone(),
                )))
            }));
        } else {
            out.push(item.clone());
        }
    }
    Ok(out)
}

fn is_plain_wildcard(options: &ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

/// Plan a SELECT over `ARRAY_AGG` / `ARRAY_ELEMENTWISE`.
///
/// The call's rows are its result. The SELECT may list `*` or the call's
/// own output columns, and bound the rows with LIMIT / FETCH. A filter,
/// grouping, ordering, dedup, offset, window or computed column has no
/// dims-and-attrs relation to read, so each is refused by name.
fn plan_non_relational(
    ctx: &OuterContext<'_>,
    temporal: TemporalScope,
    call: ArrayTableFn,
) -> Result<PlannedSelect> {
    let fn_name = call.sql_name();
    let select = ctx.select;
    let refuse = |clause: &str| -> Result<PlannedSelect> {
        Err(SqlError::Unsupported {
            detail: format!(
                "{clause} over {fn_name} is not supported: {fn_name} emits its own result \
                 rows, not the array's dims and attrs; read the cells with ARRAY_SLICE"
            ),
        })
    };
    if select.selection.is_some() {
        return refuse("WHERE");
    }
    if has_aggregation(select, ctx.functions) {
        return refuse("GROUP BY or an aggregate");
    }
    if select.having.is_some() {
        return refuse("HAVING");
    }
    if select.qualify.is_some() {
        return refuse("QUALIFY");
    }
    if !select.named_window.is_empty() {
        return refuse("WINDOW");
    }
    if select.distinct.is_some() {
        return refuse("DISTINCT");
    }
    if ctx.tail.order_by.is_some() {
        return refuse("ORDER BY");
    }
    if ctx.tail.limit_offset()?.1 != 0 {
        return refuse("OFFSET");
    }

    let scope = TableScope::resolve_from(ctx.catalog, ctx.functions, temporal, &select.from)?;
    let table = scope
        .single_table()
        .cloned()
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("{fn_name}: FROM must name the call alone"),
        })?;
    let normalized = strip_single_table_qualifiers(select, &single_table_qualifiers(&table))?;
    let projection = convert_projection(&normalized.projection, &scope)?;
    let plain = projection.iter().all(|p| match p {
        Projection::Star | Projection::QualifiedStar(_) | Projection::Column(_) => true,
        Projection::Computed { expr, .. } => matches!(expr, SqlExpr::Column { .. }),
        Projection::CpComputed { .. } => false,
    });
    if !plain {
        return refuse("a computed SELECT-list expression");
    }

    let source = call.into_plan();
    let star_only = projection
        .iter()
        .all(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)));
    let plan = if star_only {
        source
    } else {
        SqlPlan::Subquery {
            input: Box::new(source),
            filters: Vec::new(),
            projection,
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        }
    };
    Ok(PlannedSelect { plan, scope })
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::error::{Result, SqlError};
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::*;
    use crate::types_array::{
        ArrayAttrAst, ArrayAttrType, ArrayCoordLiteral, ArrayDimAst, ArrayDimType, ArrayDomainBound,
    };

    /// A catalog holding the 4x4 `img` array. `taken` names a stored
    /// collection, to collide with the relation name the planner picks.
    struct ImgCatalog {
        taken: Option<&'static str>,
    }

    impl SqlCatalog for ImgCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok((self.taken == Some(name)).then(|| CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: true,
            }))
        }

        fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
            let dim = |name: &str| ArrayDimAst {
                name: name.into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(3),
            };
            let attr = |name: &str| ArrayAttrAst {
                name: name.into(),
                dtype: ArrayAttrType::Float64,
                nullable: true,
            };
            (name == "img").then(|| ArrayCatalogView {
                name: "img".into(),
                dims: vec![dim("x"), dim("y")],
                attrs: vec![attr("nir"), attr("red")],
                tile_extents: vec![2, 2],
            })
        }
    }

    fn plan_with(catalog: &ImgCatalog, sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql)?;
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query: {sql}");
        };
        crate::planner::select::plan_query(
            q,
            catalog,
            &FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )
    }

    fn plan(sql: &str) -> Result<SqlPlan> {
        plan_with(&ImgCatalog { taken: None }, sql)
    }

    /// The relation name, the slice body and the outer read of a planned
    /// cell read.
    fn parts(plan: SqlPlan) -> (String, ArraySlicePlan, SqlPlan) {
        let SqlPlan::Cte(CtePlan {
            mut definitions,
            outer,
        }) = plan
        else {
            panic!("expected Cte, got {plan:?}");
        };
        let (name, body) = definitions.remove(0);
        let SqlPlan::ArraySlice(slice) = body else {
            panic!("expected ArraySlice body, got {body:?}");
        };
        (name, slice, *outer)
    }

    fn projected_names(plan: &SqlPlan) -> Vec<String> {
        let SqlPlan::Scan { projection, .. } = plan else {
            panic!("expected outer Scan, got {plan:?}");
        };
        projection
            .iter()
            .map(|p| match p {
                Projection::Column(name) => name.clone(),
                Projection::Computed { alias, .. } => alias.clone(),
                other => panic!("unexpected projection {other:?}"),
            })
            .collect()
    }

    #[test]
    fn star_expands_to_dims_then_attrs() {
        let (name, _, outer) =
            parts(plan("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}', ['red'])").expect("plans"));
        assert_eq!(name, "ARRAY_SLICE(img)");
        assert_eq!(projected_names(&outer), ["x", "y", "red"]);
    }

    #[test]
    fn where_and_computed_projection_ride_the_outer_read() {
        let (name, slice, outer) = parts(
            plan(
                "SELECT x, y, (nir - red) / (nir + red) AS ndvi \
                 FROM ARRAY_SLICE('img', '{x: [0, 3]}') AS t WHERE t.nir > 0.5",
            )
            .expect("plans"),
        );
        let SqlPlan::Scan {
            collection,
            filters,
            ..
        } = &outer
        else {
            panic!("expected outer Scan, got {outer:?}");
        };
        assert_eq!(collection, &name);
        assert_eq!(filters.len(), 1, "the WHERE stays on the outer read");
        assert_eq!(projected_names(&outer), ["x", "y", "ndvi"]);
        assert_eq!(slice.limit, 0);
    }

    #[test]
    fn an_unknown_column_is_refused() {
        let err = plan("SELECT blue FROM ARRAY_SLICE('img', '{x: [0, 3]}')").expect_err("refused");
        assert!(
            matches!(err, SqlError::UnknownColumn { .. }),
            "unexpected error {err:?}"
        );
    }

    #[test]
    fn a_dim_bound_narrows_an_uncapped_slice() {
        let (_, slice, _) = parts(
            plan("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}') WHERE y >= 2 AND x < 3")
                .expect("plans"),
        );
        let range = |dim: &str| {
            slice
                .slice
                .dim_ranges
                .iter()
                .find(|r| r.dim == dim)
                .map(|r| (r.lo.clone(), r.hi.clone()))
        };
        assert_eq!(
            range("x"),
            Some((ArrayCoordLiteral::Int64(0), ArrayCoordLiteral::Int64(2)))
        );
        assert_eq!(
            range("y"),
            Some((ArrayCoordLiteral::Int64(2), ArrayCoordLiteral::Int64(3)))
        );
    }

    #[test]
    fn an_outer_limit_folds_to_the_smaller_cap() {
        let (_, slice, _) = parts(
            plan("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}', [], 2) LIMIT 100")
                .expect("plans"),
        );
        assert_eq!(slice.limit, 2);
        let (_, slice, outer) =
            parts(plan("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}') LIMIT 3").expect("plans"));
        assert_eq!(slice.limit, 3);
        assert!(
            matches!(outer, SqlPlan::Scan { limit: Some(3), .. }),
            "the LIMIT stays on the outer read: {outer:?}"
        );
    }

    #[test]
    fn an_ordered_limit_does_not_fold() {
        let (_, slice, _) = parts(
            plan("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}') ORDER BY nir DESC LIMIT 3")
                .expect("plans"),
        );
        assert_eq!(slice.limit, 0);
    }

    #[test]
    fn group_by_plans_an_aggregate_over_the_relation() {
        let (name, _, outer) = parts(
            plan("SELECT x, AVG(nir) AS a FROM ARRAY_SLICE('img', '{x: [0, 3]}') GROUP BY x")
                .expect("plans"),
        );
        let SqlPlan::Aggregate { input, .. } = outer else {
            panic!("expected Aggregate, got {outer:?}");
        };
        assert!(matches!(*input, SqlPlan::Scan { ref collection, .. } if *collection == name));
    }

    #[test]
    fn the_relation_name_avoids_a_stored_collection() {
        let (name, _, _) = parts(
            plan_with(
                &ImgCatalog {
                    taken: Some("ARRAY_SLICE(img)"),
                },
                "SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3]}')",
            )
            .expect("plans"),
        );
        assert_eq!(name, "ARRAY_SLICE(img)#2");
    }

    #[test]
    fn the_inlined_read_filters_the_slice_rows() {
        let SqlPlan::Cte(CtePlan { definitions, outer }) =
            plan("SELECT x FROM ARRAY_SLICE('img', '{x: [0, 3]}') WHERE nir > 0.5").expect("plans")
        else {
            panic!("expected Cte");
        };
        let (name, body) = &definitions[0];
        match crate::planner::cte::inline_cte(&outer, name, body) {
            SqlPlan::Subquery { input, filters, .. } => {
                assert!(matches!(*input, SqlPlan::ArraySlice(_)));
                assert_eq!(filters.len(), 1);
            }
            other => panic!("expected Subquery over the slice, got {other:?}"),
        }
    }

    #[test]
    fn where_over_array_agg_is_refused_by_name() {
        let err = plan("SELECT * FROM ARRAY_AGG('img', 'nir', 'sum') WHERE result > 1")
            .expect_err("refused");
        let SqlError::Unsupported { detail } = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(detail.starts_with("WHERE over ARRAY_AGG"), "{detail}");
    }

    #[test]
    fn order_by_over_array_elementwise_is_refused_by_name() {
        let err =
            plan("SELECT * FROM ARRAY_ELEMENTWISE('img', 'img', 'add', 'nir') ORDER BY coords")
                .expect_err("refused");
        let SqlError::Unsupported { detail } = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            detail.starts_with("ORDER BY over ARRAY_ELEMENTWISE"),
            "{detail}"
        );
    }

    #[test]
    fn plain_agg_columns_wrap_in_a_projecting_tail() {
        match plan("SELECT result FROM ARRAY_AGG('img', 'nir', 'sum') LIMIT 1").expect("plans") {
            SqlPlan::Subquery {
                input,
                projection,
                limit,
                ..
            } => {
                assert!(matches!(*input, SqlPlan::ArrayAgg(_)));
                assert_eq!(projection.len(), 1);
                assert_eq!(limit, Some(1));
            }
            other => panic!("expected Subquery over ArrayAgg, got {other:?}"),
        }
    }
}
