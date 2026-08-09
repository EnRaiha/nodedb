// SPDX-License-Identifier: BUSL-1.1

//! RETURNING clause handling for DML statements: strip it from the text,
//! decide whether the resulting plan can carry it, and attach it.
//!
//! The planner does not parse RETURNING on DML, so the clause is removed from
//! the raw SQL before planning and its projected column list is parsed here.
//! The spec is then injected into the plan variant that will produce the rows.
//!
//! Protocol-neutral: both the pgwire planner and the neutral DDL router's
//! `UPSERT` path go through this module, so a statement's clause is stripped,
//! judged, and attached identically on either transport.

// Re-export bridge types so callers only import from this module.
pub use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, KvOp, QueryOp, TimeseriesOp, VectorOp,
};
use nodedb_sql::parser::preprocess::lex::{find_ascii_keyword, keyword_position_outside_literals};
use nodedb_types::starts_with_ascii_case_insensitive;

const RETURNING_KEYWORD: &str = "RETURNING";

/// Check if a DML statement contains a RETURNING clause and strip it.
///
/// Returns `(cleaned_sql, returning_spec)`. The cleaned SQL has the
/// `RETURNING ...` suffix removed so DataFusion can parse it.
///
/// RETURNING is honored on INSERT, UPSERT, UPDATE, DELETE and MERGE. Whether
/// the resulting plan has a slot to carry the clause is not decidable from the
/// statement text — it depends on the shape the planner produces — so that
/// judgement is made once the plan exists, by
/// [`refuse_unprojectable_insert_returning`].
///
/// Arithmetic expressions (e.g. `RETURNING stock * 2`) are rejected with
/// a typed error — only bare column names and `*` are supported.
pub fn strip_returning(sql: &str) -> Result<(String, Option<ReturningSpec>), Error> {
    let trimmed = sql.trim_start();

    // Gated on the DML verbs rather than on "everything that is not a SELECT",
    // so an unrelated statement whose text merely contains the word is never
    // truncated at it.
    if !starts_with_ascii_case_insensitive(trimmed, "INSERT")
        && !starts_with_ascii_case_insensitive(trimmed, "UPSERT")
        && !starts_with_ascii_case_insensitive(trimmed, "UPDATE")
        && !starts_with_ascii_case_insensitive(trimmed, "DELETE")
        && !starts_with_ascii_case_insensitive(trimmed, "MERGE")
    {
        return Ok((sql.to_string(), None));
    }

    if let Some(pos) = keyword_position_outside_literals(sql, RETURNING_KEYWORD) {
        let cleaned = sql[..pos].trim_end().to_string();
        let columns_str = sql[pos + RETURNING_KEYWORD.len()..].trim();
        let spec = parse_returning_columns(columns_str)?;
        Ok((cleaned, Some(spec)))
    } else {
        Ok((sql.to_string(), None))
    }
}

/// Refuse an `INSERT ... RETURNING` whose plan SHAPE has nowhere to carry the
/// clause.
///
/// Every engine now carries it on its insert op — document (schemaless and
/// strict), key-value, columnar, spatial, timeseries, and vector-primary each
/// own a `returning` slot paired with an `rls_filters` read gate, so the
/// statement returns the STORED post-image bounded by the read policy. What
/// remains here is not an engine gap but a plan-shape one: `INSERT ... SELECT`
/// never reaches the Data Plane as a single insert op, so there is no slot on
/// it for the clause to ride in, whatever engine it targets.
///
/// Refusing is the honest answer. Silently dropping the clause answered a
/// statement that asked for rows with a bare command tag, and nothing anywhere
/// said the request had been discarded.
///
/// Still runs against the plan rather than the statement text: the expansion
/// that removes the slot is a planning decision, not a syntactic one.
pub fn refuse_unprojectable_insert_returning(plan: &PhysicalPlan) -> Result<(), Error> {
    let unsupported = match plan {
        // `INSERT ... SELECT` never reaches the Data Plane as this op: it is
        // expanded on the Control Plane into fresh-surrogate insert tasks whose
        // rows the expander, not the plan, decides — so there is no slot on
        // this plan for the clause to ride in.
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => "INSERT ... SELECT",
        // Exchange wraps an unresolved child; judge the child.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => {
            return refuse_unprojectable_insert_returning(&op.child);
        }
        // Everything else either carries the clause already or is not an
        // insert. Enumerated per engine rather than via a catch-all so a new
        // `PhysicalPlan` variant forces a decision instead of silently
        // inheriting "supported" and dropping the clause.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => return Ok(()),
    };
    Err(Error::BadRequest {
        detail: format!(
            "RETURNING is not supported on {unsupported}; it is supported on every engine's \
             direct INSERT — document collections (schemaless and strict), key-value, \
             columnar, spatial, timeseries, and vector-primary collections — and on UPDATE, \
             DELETE, and MERGE. Follow the insert with a SELECT on the inserted key to read \
             the stored rows."
        ),
    })
}

/// The error a row-returning write must fail with when an open transaction
/// buffers or stages it instead of executing it.
///
/// Both in-transaction routes are structurally unable to answer the clause, and
/// for different reasons — which is why this refuses rather than returning an
/// empty row set:
///
/// - A **buffered** write performs no engine work at all until COMMIT, so at
///   statement time there is no stored row to project. Nothing could be
///   returned however the response were shaped.
/// - A **staged** write does touch the transaction overlay, but every staging
///   handler answers with an affected-count payload; the one payload-bearing
///   staged outcome is reserved for the atomic key-value ops that compute a
///   value. No staged write carries a row image back.
///
/// COMMIT then answers with a single tag for the whole transaction, so the rows
/// cannot be surfaced later either. Reporting success with no rows is the exact
/// silence this clause exists to remove, so the statement is refused and says
/// which limitation it hit. Verb-agnostic on purpose: it fires for any plan the
/// shaper classifies as row-returning, so INSERT, UPSERT, UPDATE, DELETE and
/// MERGE all behave identically inside a transaction.
pub fn in_transaction_returning_unsupported() -> Error {
    Error::BadRequest {
        detail: "RETURNING is not supported inside an explicit transaction: the write is staged \
                 or buffered until COMMIT, so it has no stored row to project at this point. Run \
                 the statement in autocommit, or follow the write with a SELECT after COMMIT."
            .to_string(),
    }
}

/// Inject a RETURNING spec into a DML physical plan variant.
///
/// Only `PointInsert`, `PointPut`, `BatchInsert`, `Upsert`, `PointUpdate`,
/// `BulkUpdate`, `PointDelete`, `BulkDelete`, `UpdateFromJoin`, `Merge`, the KV
/// `Insert` / `InsertIfAbsent` / `InsertOnConflictUpdate` / `Put` / `BatchPut`
/// ops, the columnar `Insert`, the timeseries `Ingest`, the vector
/// `DirectUpsert`, and the CRDT `DocUpsert` / `DocDelete` ops are affected.
/// Every other variant is left unchanged — an insert shape among them has
/// already been refused by [`refuse_unprojectable_insert_returning`], which
/// runs first.
pub fn inject_returning_spec(plan: &mut PhysicalPlan, spec: ReturningSpec) {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointInsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::Insert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::InsertIfAbsent { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::Put { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Kv(KvOp::BatchPut { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Columnar(ColumnarOp::Insert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Vector(VectorOp::DirectUpsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointPut { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BatchInsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::Upsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::Merge { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocUpsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        _ => {}
    }
}

/// Parse the column list that appears after the RETURNING keyword.
///
/// Supports:
/// - `*`
/// - `col1, col2`
/// - `col1 AS alias1, col2`
///
/// Rejects arithmetic expressions (e.g. `stock * 2`) with a typed error.
fn parse_returning_columns(columns_str: &str) -> Result<ReturningSpec, Error> {
    let columns_str = columns_str.trim();
    if columns_str == "*" {
        return Ok(ReturningSpec {
            columns: ReturningColumns::Star,
        });
    }

    let mut items = Vec::new();
    for raw_item in columns_str.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            continue;
        }

        // Reject arithmetic: contains operators that are not part of a name.
        if contains_arithmetic(item) {
            return Err(Error::BadRequest {
                detail: format!(
                    "RETURNING expression '{item}' is not supported; \
                     only bare column names and RETURNING * are allowed"
                ),
            });
        }

        // Parse `name [AS alias]` — case-insensitive AS.
        if let Some(as_pos) = find_ascii_keyword(item, "AS") {
            let name = item[..as_pos].trim().to_string();
            let alias = item[as_pos + 2..].trim().to_string();
            if name.is_empty() || alias.is_empty() {
                return Err(Error::BadRequest {
                    detail: format!("invalid RETURNING column expression: '{item}'"),
                });
            }
            items.push(ReturningItem {
                name,
                alias: Some(alias),
            });
        } else {
            let name = item.to_string();
            if !is_valid_column_name(&name) {
                return Err(Error::BadRequest {
                    detail: format!(
                        "RETURNING expression '{name}' is not supported; \
                         only bare column names and RETURNING * are allowed"
                    ),
                });
            }
            items.push(ReturningItem { name, alias: None });
        }
    }

    if items.is_empty() {
        return Err(Error::BadRequest {
            detail: "empty RETURNING column list".into(),
        });
    }

    Ok(ReturningSpec {
        columns: ReturningColumns::Named(items),
    })
}

/// Return true if the expression token contains arithmetic operators
/// (*, /, +, -) outside of quoted identifiers.
fn contains_arithmetic(expr: &str) -> bool {
    let mut in_quote = false;
    let mut prev = '\0';
    for ch in expr.chars() {
        if ch == '"' {
            in_quote = !in_quote;
            prev = ch;
            continue;
        }
        if in_quote {
            prev = ch;
            continue;
        }
        if matches!(ch, '+' | '/' | '%') {
            return true;
        }
        // `-` is arithmetic only when not a leading sign or part of an identifier.
        if ch == '-' && (prev.is_ascii_alphanumeric() || prev == '_') {
            return true;
        }
        // `*` is arithmetic when preceded by an identifier character.
        if ch == '*' && (prev.is_ascii_alphanumeric() || prev == '_') {
            return true;
        }
        prev = ch;
    }
    false
}

/// Return true if the given name is a valid bare identifier (letters, digits, underscores).
fn is_valid_column_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The document engines carry the clause, so it is stripped and parsed like
    /// any other verb's — the statement text alone cannot decide the engine, so
    /// nothing is refused here.
    #[test]
    fn insert_returning_is_stripped_and_parsed() {
        let (sql, spec) =
            strip_returning("INSERT INTO items (id, name) VALUES ('a', 'alpha') RETURNING *")
                .expect("INSERT RETURNING must plan");
        assert_eq!(sql, "INSERT INTO items (id, name) VALUES ('a', 'alpha')");
        assert_eq!(spec.expect("spec").columns, ReturningColumns::Star);

        let (sql, spec) = strip_returning("insert into items (id) values ('a') returning id AS k")
            .expect("INSERT RETURNING must plan");
        assert_eq!(sql, "insert into items (id) values ('a')");
        assert_eq!(
            spec.expect("spec").columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: Some("k".into()),
            }])
        );
    }

    /// An insert shape with no `returning` slot is refused at the plan, naming
    /// the shape and where the clause IS honored. Silently dropping it left
    /// the caller with a command tag for a statement that asked for rows.
    #[test]
    fn an_insert_plan_with_no_returning_slot_is_refused() {
        let plan = PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: "dst".into(),
            source_collection: "src".into(),
            source_filters: Vec::new(),
            source_limit: 0,
        });
        let detail = refuse_unprojectable_insert_returning(&plan)
            .expect_err("an INSERT ... SELECT cannot carry the clause")
            .to_string();
        assert!(
            detail.contains("INSERT ... SELECT") && detail.contains("document"),
            "the refusal must name the plan shape and where it IS supported; got {detail}"
        );
    }

    /// A vector-primary upsert now carries the clause, so the same gate admits
    /// it. Pinned beside the refusal above for the same reason the columnar and
    /// timeseries cases are: an engine dropped from the refusal without gaining
    /// the slot silently drops the clause, and only asserting both halves
    /// catches that.
    #[test]
    fn a_vector_primary_upsert_plan_is_admitted() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: "vectors".into(),
            field: "emb".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            vector: Vec::new(),
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A timeseries ingest now carries the clause, so the same gate admits it.
    /// Pinned beside the refusal above for the same reason the columnar case is:
    /// an engine dropped from the refusal without gaining the slot silently
    /// drops the clause, and only asserting both halves catches that.
    #[test]
    fn a_timeseries_ingest_plan_is_admitted() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".into(),
            payload: Vec::new(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A columnar insert now carries the clause, so the same gate admits it.
    /// This is the assertion that would fail if the columnar arm were ever
    /// restored to the refusal while the op kept its `returning` slot — the
    /// combination that silently drops the clause.
    #[test]
    fn a_columnar_insert_plan_is_admitted() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "metrics".into(),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// A document insert carries the clause, so the same gate admits it.
    #[test]
    fn a_document_insert_plan_is_admitted() {
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: "items".into(),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(refuse_unprojectable_insert_returning(&plan).is_ok());
    }

    /// An INSERT with no such clause is untouched — planning must not turn
    /// ordinary inserts into errors.
    #[test]
    fn a_plain_insert_is_untouched() {
        let sql = "INSERT INTO items (id, name) VALUES ('a', 'alpha')";
        let (out, spec) = strip_returning(sql).expect("a plain insert must plan");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    /// The word inside a string literal is data, not a clause.
    #[test]
    fn returning_inside_a_string_literal_is_not_a_clause() {
        let sql = "INSERT INTO items (id, note) VALUES ('a', 'RETURNING soon')";
        let (out, spec) = strip_returning(sql).expect("a quoted keyword is not a clause");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    /// Only DML verbs are scanned for the clause: a SELECT whose column name
    /// merely embeds the word is left alone.
    #[test]
    fn a_non_dml_statement_is_not_scanned_for_the_clause() {
        let sql = "SELECT returning_count FROM items";
        let (out, spec) = strip_returning(sql).expect("a select must pass through");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    #[test]
    fn strips_star_returning_from_update() {
        let (sql, spec) =
            strip_returning("UPDATE products SET stock = 1 WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "UPDATE products SET stock = 1 WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_columns_returning_from_update() {
        let (sql, spec) = strip_returning(
            "UPDATE products SET stock = stock - 1 WHERE id = 'p1' RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(sql, "UPDATE products SET stock = stock - 1 WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None
                },
                ReturningItem {
                    name: "stock".into(),
                    alias: None
                },
            ])
        );
    }

    #[test]
    fn strips_star_returning_from_delete() {
        let (sql, spec) =
            strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_returning_from_delete() {
        let (sql, spec) =
            strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING id").unwrap();
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: None
            }])
        );
    }

    #[test]
    fn strips_star_returning_from_merge() {
        let (sql, spec) = strip_returning(
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET stock = s.stock RETURNING *",
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET stock = s.stock"
        );
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_returning_from_merge() {
        let (sql, spec) = strip_returning(
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
        );
        assert_eq!(
            spec.unwrap().columns,
            ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None
                },
                ReturningItem {
                    name: "stock".into(),
                    alias: None
                },
            ])
        );
    }

    #[test]
    fn merge_without_returning_is_unchanged() {
        let original = "MERGE INTO products t USING staging s ON t.id = s.id \
                        WHEN MATCHED THEN DELETE";
        let (sql, spec) = strip_returning(original).unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, original);
    }

    #[test]
    fn no_returning() {
        let (sql, spec) = strip_returning("UPDATE products SET stock = 0 WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "UPDATE products SET stock = 0 WHERE id = 'p1'");
    }

    #[test]
    fn returning_inside_identifier_not_treated_as_keyword() {
        // A collection/table whose name embeds "returning" (with `_` as an
        // identifier boundary) must NOT match the RETURNING keyword inside the
        // name — the real keyword is the trailing one after WHERE.
        let (sql, spec) =
            strip_returning("DELETE FROM orders_returning WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);

        // Same identifier with no trailing RETURNING clause → no spec, unchanged.
        let (sql, spec) = strip_returning("DELETE FROM orders_returning WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
    }

    #[test]
    fn returning_in_string_literal_ignored() {
        let (sql, spec) =
            strip_returning("UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(
            sql,
            "UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'"
        );
    }

    #[test]
    fn select_not_affected() {
        let (sql, spec) = strip_returning("SELECT * FROM products").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "SELECT * FROM products");
    }

    #[test]
    fn case_insensitive() {
        let (sql, spec) =
            strip_returning("update products set stock = 0 where id = 'p1' returning id").unwrap();
        let spec = spec.unwrap();
        assert_eq!(sql, "update products set stock = 0 where id = 'p1'");
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: None
            }])
        );
    }

    #[test]
    fn unicode_identifier_before_returning_preserves_original_offsets() {
        let (sql, spec) = strip_returning("DELETE FROM tﬀﬀ RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM tﬀﬀ");
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);
    }

    #[test]
    fn unicode_returning_column_before_alias_preserves_original_offsets() {
        let (_, spec) = strip_returning("UPDATE t SET x = 1 RETURNING ﬀﬀ AS alias").unwrap();
        assert_eq!(
            spec.unwrap().columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "ﬀﬀ".into(),
                alias: Some("alias".into()),
            }])
        );
    }

    #[test]
    fn arithmetic_in_returning_is_error() {
        let result = strip_returning("UPDATE t SET x=1 RETURNING x*2");
        assert!(result.is_err());
        let e = result.unwrap_err().to_string();
        assert!(
            e.contains("not supported") || e.contains("expression"),
            "unexpected error: {e}"
        );
    }

    #[test]
    fn returning_with_alias() {
        let (sql, spec) =
            strip_returning("UPDATE t SET x=2 WHERE id='a' RETURNING x AS new_x").unwrap();
        assert_eq!(sql, "UPDATE t SET x=2 WHERE id='a'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "x".into(),
                alias: Some("new_x".into()),
            }])
        );
    }

    #[test]
    fn output_names_star_returns_none() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        assert!(spec.output_names().is_none());
    }

    #[test]
    fn output_names_named_uses_aliases() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None,
                },
                ReturningItem {
                    name: "x".into(),
                    alias: Some("val".into()),
                },
            ]),
        };
        assert_eq!(
            spec.output_names(),
            Some(vec!["id".to_string(), "val".to_string()])
        );
    }
}
