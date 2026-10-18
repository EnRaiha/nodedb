// SPDX-License-Identifier: BUSL-1.1

//! Which planning errors a Parse message reports.
//!
//! PostgreSQL runs parse analysis at Parse. A statement that names an
//! unknown relation, column or function fails there with its SQLSTATE, so a
//! driver's prepare fails. Planning and execution errors, such as a division
//! by zero folded from constants, surface at Bind or Execute.
//!
//! Parse plans the statement with every `$N` replaced by the text `NULL`.
//! Execute binds the client's values into the parsed statement instead. So
//! with placeholders, Parse plans a different statement than Execute does.
//! It reports only the errors no value can change: an unknown table, column
//! or function, an ambiguous column, and a column outside GROUP BY. Every
//! other error can come from the substituted `NULL`. Examples are a `LIMIT`
//! or a function argument that must be a literal value, or a type check
//! against the placeholder. Those errors defer to Execute, which plans with
//! the bound values.

use nodedb_types::error::sqlstate;

/// SQLSTATE class `42`: syntax error or access rule violation.
const SYNTAX_OR_ACCESS_CLASS: &str = "42";

/// Whether a Parse reports the planning error with SQLSTATE `state`.
///
/// `has_placeholders` says whether the statement text holds a `$N`.
///
/// A statement without placeholders plans at Parse exactly as Execute plans
/// it, so a class-`42` error and `0A000` (feature not supported) report at
/// Parse. Three kinds defer to Execute, as PostgreSQL does:
/// - `42501` (insufficient privilege): Execute checks privileges under the
///   session's adopted authorization, which Parse does not hold.
/// - A data error of class `22`: it comes from evaluating a value. PostgreSQL
///   folds `1 / 0` while planning, after Parse, and raises `22012` there.
/// - A retryable error of class `40`: Execute retries the statement.
pub(super) fn reported_at_parse(state: &str, has_placeholders: bool) -> bool {
    if has_placeholders {
        return matches!(
            state,
            sqlstate::UNDEFINED_TABLE
                | sqlstate::UNDEFINED_COLUMN
                | sqlstate::AMBIGUOUS_COLUMN
                | sqlstate::UNDEFINED_FUNCTION
                | sqlstate::GROUPING_ERROR
        );
    }
    if state == sqlstate::INSUFFICIENT_PRIVILEGE {
        return false;
    }
    state.starts_with(SYNTAX_OR_ACCESS_CLASS) || state == sqlstate::FEATURE_NOT_SUPPORTED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_resolution_reports_with_or_without_placeholders() {
        for state in [
            sqlstate::UNDEFINED_TABLE,
            sqlstate::UNDEFINED_COLUMN,
            sqlstate::AMBIGUOUS_COLUMN,
            sqlstate::UNDEFINED_FUNCTION,
            sqlstate::GROUPING_ERROR,
        ] {
            assert!(reported_at_parse(state, false), "{state}");
            assert!(reported_at_parse(state, true), "{state}");
        }
    }

    #[test]
    fn a_value_dependent_error_defers_with_placeholders() {
        for state in [
            sqlstate::SYNTAX_ERROR,
            sqlstate::DATATYPE_MISMATCH,
            sqlstate::FEATURE_NOT_SUPPORTED,
            sqlstate::INVALID_LIMIT_VALUE,
        ] {
            assert!(!reported_at_parse(state, true), "{state}");
        }
        assert!(reported_at_parse(sqlstate::SYNTAX_ERROR, false));
        assert!(reported_at_parse(sqlstate::FEATURE_NOT_SUPPORTED, false));
    }

    #[test]
    fn privilege_data_and_retryable_errors_defer() {
        for state in [
            sqlstate::INSUFFICIENT_PRIVILEGE,
            sqlstate::DIVISION_BY_ZERO,
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            sqlstate::SERIALIZATION_FAILURE,
        ] {
            assert!(!reported_at_parse(state, false), "{state}");
        }
    }
}
