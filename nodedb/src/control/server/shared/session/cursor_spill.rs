// SPDX-License-Identifier: BUSL-1.1

//! The row cap a declared cursor's result set must fit inside.
//!
//! `DECLARE ... CURSOR` materializes its whole result set before the cursor
//! exists, so a cap here bounds what one session's cursor retains. A result
//! set past the cap is refused. Dropping the tail instead would hand the
//! client a short cursor it cannot tell from a complete one.

use nodedb_types::error::sqlstate;

/// Default maximum rows one cursor's result set can hold.
pub const DEFAULT_CURSOR_MAX_ROWS: usize = 100_000;

/// A cursor result set larger than the cap allows.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "cursor result set has {row_count} rows, over the {max_rows}-row limit. \
     Narrow the query with WHERE or LIMIT"
)]
pub struct CursorLimitExceeded {
    /// Rows the query produced.
    pub row_count: usize,
    /// Rows a cursor can hold.
    pub max_rows: usize,
}

impl CursorLimitExceeded {
    /// The SQLSTATE pgwire reports for this refusal.
    pub fn sqlstate(&self) -> &'static str {
        sqlstate::PROGRAM_LIMIT_EXCEEDED
    }
}

/// Row cap applied to a declared cursor.
#[derive(Debug, Clone)]
pub struct CursorSpillConfig {
    /// Maximum rows one cursor's result set can hold.
    pub max_in_memory_rows: usize,
}

impl Default for CursorSpillConfig {
    fn default() -> Self {
        Self {
            max_in_memory_rows: DEFAULT_CURSOR_MAX_ROWS,
        }
    }
}

/// Accept a cursor result set only when it fits inside the cap.
///
/// The error names both counts, so a client learns the query was too large
/// rather than reading a truncated set as the whole answer.
pub fn enforce_cursor_limit(
    rows: Vec<String>,
    config: &CursorSpillConfig,
) -> Result<Vec<String>, CursorLimitExceeded> {
    if rows.len() > config.max_in_memory_rows {
        return Err(CursorLimitExceeded {
            row_count: rows.len(),
            max_rows: config.max_in_memory_rows,
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("row{i}")).collect()
    }

    #[test]
    fn a_set_inside_the_cap_passes_through_whole() {
        let config = CursorSpillConfig {
            max_in_memory_rows: 100,
        };
        let kept = enforce_cursor_limit(rows(50), &config).expect("50 rows fit under 100");
        assert_eq!(kept.len(), 50);
    }

    #[test]
    fn a_set_exactly_at_the_cap_passes_through_whole() {
        let config = CursorSpillConfig {
            max_in_memory_rows: 100,
        };
        let kept = enforce_cursor_limit(rows(100), &config).expect("the cap is inclusive");
        assert_eq!(kept.len(), 100);
    }

    #[test]
    fn a_set_over_the_cap_is_refused_rather_than_shortened() {
        let config = CursorSpillConfig {
            max_in_memory_rows: 100,
        };
        let error = enforce_cursor_limit(rows(200), &config)
            .expect_err("an over-cap set must not resolve to a short cursor");
        assert_eq!(error.row_count, 200);
        assert_eq!(error.max_rows, 100);
        assert_eq!(error.sqlstate(), "54000");
    }

    #[test]
    fn the_message_names_both_counts() {
        let error = CursorLimitExceeded {
            row_count: 200,
            max_rows: 100,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("200"), "{rendered}");
        assert!(rendered.contains("100"), "{rendered}");
    }
}
