// SPDX-License-Identifier: BUSL-1.1

//! Typed virtual-table representation: schema + rows, with alias-aware column
//! resolution so cross-table joins resolve qualified names unambiguously.

use super::value::{VColumn, VValue};

#[derive(Debug, Clone)]
pub struct VTable {
    pub columns: Vec<VColumn>,
    pub rows: Vec<Vec<VValue>>,
}

/// Failure resolving a column reference against a (possibly joined) schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    Unknown(String),
    Ambiguous(String),
}

impl VTable {
    pub fn new(columns: Vec<VColumn>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// A single-row, zero-column table — the relation for a no-`FROM` scalar
    /// `SELECT`, which yields exactly one output row.
    pub fn single_empty_row() -> Self {
        Self {
            columns: Vec::new(),
            rows: vec![Vec::new()],
        }
    }

    /// Return every column tagged with `qualifier` (the relation alias). Used
    /// when a base relation is folded into a combined join schema.
    pub fn with_qualifier(&self, qualifier: &str) -> Vec<VColumn> {
        self.columns
            .iter()
            .map(|c| c.qualified(qualifier))
            .collect()
    }

    /// Resolve a column reference to its index. `qualifier` is the optional
    /// table/alias qualifier (`c` in `c.relname`). A bare name must be unique
    /// across all relations or resolution is ambiguous (SQL semantics).
    pub fn resolve_column(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<usize, ResolveError> {
        let mut found: Option<usize> = None;
        for (i, col) in self.columns.iter().enumerate() {
            if !col.name.eq_ignore_ascii_case(name) {
                continue;
            }
            if let Some(q) = qualifier {
                let matches_q = col
                    .qualifier
                    .as_deref()
                    .map(|cq| cq.eq_ignore_ascii_case(q))
                    .unwrap_or(false);
                if !matches_q {
                    continue;
                }
                return Ok(i);
            }
            // Bare name: require uniqueness across relations.
            if found.is_some() {
                let label = match qualifier {
                    Some(q) => format!("{q}.{name}"),
                    None => name.to_string(),
                };
                return Err(ResolveError::Ambiguous(label));
            }
            found = Some(i);
        }
        found.ok_or_else(|| {
            let label = match qualifier {
                Some(q) => format!("{q}.{name}"),
                None => name.to_string(),
            };
            ResolveError::Unknown(label)
        })
    }

    pub fn push(&mut self, row: Vec<VValue>) {
        debug_assert_eq!(row.len(), self.columns.len());
        self.rows.push(row);
    }
}
