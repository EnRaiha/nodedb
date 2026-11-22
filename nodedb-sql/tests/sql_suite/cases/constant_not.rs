// SPDX-License-Identifier: Apache-2.0

//! `NOT` over a constant in a `SELECT` with no `FROM` folds at plan time. The
//! statement has no run time to ask, so a `NOT` the folder left alone came out
//! as NULL.

use nodedb_sql::types::{CollectionInfo, SqlValue};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, SqlPlan, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        _: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(None)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn constant_row(sql: &str) -> Vec<SqlValue> {
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("plan {sql}: {e}"));
    assert_eq!(plans.len(), 1, "expected one plan for: {sql}");
    match plans.pop().expect("one plan") {
        SqlPlan::ConstantResult { values, .. } => values,
        other => panic!("{sql} must plan as a constant row, got {other:?}"),
    }
}

#[test]
fn not_over_a_boolean_constant_answers_its_negation() {
    assert_eq!(constant_row("SELECT NOT true"), [SqlValue::Bool(false)]);
    assert_eq!(constant_row("SELECT NOT false"), [SqlValue::Bool(true)]);
    assert_eq!(
        constant_row("SELECT NOT CAST('t' AS BOOLEAN)"),
        [SqlValue::Bool(false)]
    );
}

#[test]
fn not_null_stays_null() {
    assert_eq!(constant_row("SELECT NOT NULL"), [SqlValue::Null]);
}

#[test]
fn not_over_text_that_is_not_a_boolean_is_refused() {
    let error = plan_sql("SELECT NOT 'maybe'", &Catalog).expect_err("must refuse");
    assert!(
        matches!(error, SqlError::InvalidTextRepresentation { .. }),
        "got {error:?}"
    );
}
