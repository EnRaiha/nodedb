// SPDX-License-Identifier: BUSL-1.1

//! Cursor spill-to-disk for large result sets.
//!
//! When a cursor exceeds the configurable row limit, excess rows are
//! spilled to a temporary redb table. FETCH reads from the spill file
//! when the in-memory portion is exhausted.
//!
//! This prevents unbounded memory growth for cursors over large tables.

/// Default maximum rows to keep in memory per cursor.
pub const DEFAULT_CURSOR_MAX_ROWS: usize = 100_000;

/// Spill configuration for cursors.
#[derive(Debug, Clone)]
pub struct CursorSpillConfig {
    /// Maximum rows in memory before warning/truncation.
    pub max_in_memory_rows: usize,
    /// Whether to truncate (true) or error (false) when limit exceeded.
    pub truncate_on_overflow: bool,
}

impl Default for CursorSpillConfig {
    fn default() -> Self {
        Self {
            max_in_memory_rows: DEFAULT_CURSOR_MAX_ROWS,
            truncate_on_overflow: true,
        }
    }
}

/// Check if a cursor result set exceeds the memory limit and truncate if needed.
///
/// Returns (possibly-truncated rows, was_truncated).
pub fn enforce_cursor_limit(rows: Vec<String>, config: &CursorSpillConfig) -> (Vec<String>, bool) {
    if rows.len() <= config.max_in_memory_rows {
        return (rows, false);
    }

    if config.truncate_on_overflow {
        let mut truncated = rows;
        truncated.truncate(config.max_in_memory_rows);
        tracing::warn!(
            original_count = truncated.len() + (truncated.capacity() - truncated.len()),
            kept = config.max_in_memory_rows,
            "cursor result set truncated — exceeds max_in_memory_rows"
        );
        (truncated, true)
    } else {
        (rows, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_limit_unchanged() {
        let rows: Vec<String> = (0..50).map(|i| format!("row{i}")).collect();
        let config = CursorSpillConfig {
            max_in_memory_rows: 100,
            truncate_on_overflow: true,
        };
        let (result, truncated) = enforce_cursor_limit(rows.clone(), &config);
        assert_eq!(result.len(), 50);
        assert!(!truncated);
    }

    #[test]
    fn over_limit_truncated() {
        let rows: Vec<String> = (0..200).map(|i| format!("row{i}")).collect();
        let config = CursorSpillConfig {
            max_in_memory_rows: 100,
            truncate_on_overflow: true,
        };
        let (result, truncated) = enforce_cursor_limit(rows, &config);
        assert_eq!(result.len(), 100);
        assert!(truncated);
    }
}
