// SPDX-License-Identifier: BUSL-1.1

//! The stored columns one plan node reads outside its plain projection.
//!
//! A predicate, a sort key, a group key, a join key, an index probe, an
//! assignment, and a computed expression all consume the stored value on the
//! Data Plane. The result-path masking hook never sees that value, so each
//! such read is collected here and checked against the redaction policy.

use nodedb_physical::physical_plan::{GroupKeySpec, SortKeySpec, UpdateValue};
use nodedb_query::expr::{ComputedColumn, SqlExpr};
use nodedb_query::scan_filter::ScanFilter;
use nodedb_query::window::WindowFuncSpec;

use super::super::aggregate::collect_columns;

/// The clause a column read comes from, named in the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Clause {
    Filter,
    Computed,
    Window,
    Sort,
    Group,
    JoinKey,
    IndexProbe,
    Assignment,
    Projection,
    Distinct,
    KeyLookup,
}

impl Clause {
    /// The phrase the refusal uses for this clause.
    pub(super) fn describe(self) -> &'static str {
        match self {
            Self::Filter => "a predicate over a redacted column",
            Self::Computed => "an expression over a redacted column",
            Self::Window => "a window function over a redacted column",
            Self::Sort => "ordering by a redacted column",
            Self::Group => "grouping by a redacted column",
            Self::JoinKey => "joining on a redacted column",
            Self::IndexProbe => "an index probe on a redacted column",
            Self::Assignment => "an assignment that reads a redacted column",
            Self::Projection => "projecting a renamed redacted column through a subquery",
            Self::Distinct => "DISTINCT over a redacted column",
            Self::KeyLookup => "a lookup by a redacted key column, whose hit or miss shows the key",
        }
    }
}

/// Every column one node reads, and every alias it gives a stored column.
#[derive(Debug, Default)]
pub(super) struct NodeReads {
    /// `(field, clause)` for each read outside the plain projection.
    pub(super) reads: Vec<(String, Clause)>,
    /// `(alias, field)` for each computed column that renames a stored
    /// column. The output schema masks the stored column, so the rename is
    /// not refused here. A parent node that reads the alias reads the
    /// stored value.
    pub(super) renames: Vec<(String, String)>,
    /// Row keys a node passes through to its output. Only a renamed
    /// redacted column from a child is refused among them.
    pub(super) passthrough: Vec<String>,
    /// `Some` when the node reads every stored column in this clause, as a
    /// `DISTINCT` over a whole row does.
    pub(super) every_column: Option<Clause>,
    /// `true` when the node finds its row by primary-key value. A hit or a
    /// miss tells the caller whether a row with that key exists.
    pub(super) key_lookup: bool,
}

impl NodeReads {
    pub(super) fn is_empty(&self) -> bool {
        self.reads.is_empty()
            && self.renames.is_empty()
            && self.passthrough.is_empty()
            && self.every_column.is_none()
            && !self.key_lookup
    }

    /// A `DISTINCT` compares the projected stored values. An empty
    /// projection compares every column.
    pub(super) fn distinct(&mut self, distinct: bool, projection: &[String]) {
        if !distinct {
            return;
        }
        if projection.is_empty() {
            self.every_column = Some(Clause::Distinct);
        }
        for name in projection {
            self.field(name, Clause::Distinct);
        }
    }

    /// One field read in `clause`. An index path (`$.email`) names the
    /// stored column `email`.
    pub(super) fn field(&mut self, field: &str, clause: Clause) {
        let field = field.strip_prefix("$.").unwrap_or(field);
        if !field.is_empty() && field != "*" {
            self.reads.push((field.to_string(), clause));
        }
    }

    /// Every column `expr` reads, in `clause`.
    pub(super) fn expr(&mut self, expr: &SqlExpr, clause: Clause) {
        let mut columns = Vec::new();
        collect_columns(expr, &mut columns);
        for column in columns {
            self.field(column, clause);
        }
    }

    /// Every column a serialized `Vec<ScanFilter>` reads.
    pub(super) fn filter_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let filters: Vec<ScanFilter> = decode(bytes, "scan filters")?;
        self.filters(&filters);
        Ok(())
    }

    fn filters(&mut self, filters: &[ScanFilter]) {
        for filter in filters {
            self.field(&filter.field, Clause::Filter);
            for group in &filter.clauses {
                self.filters(group);
            }
            if let Some(expr) = &filter.expr {
                self.expr(expr, Clause::Filter);
            }
        }
    }

    /// Every column a serialized `Vec<ComputedColumn>` reads.
    ///
    /// A column that only names a stored column passes it through under its
    /// alias. It is a rename when the alias differs, and no read otherwise.
    pub(super) fn computed_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let columns: Vec<ComputedColumn> = decode(bytes, "computed columns")?;
        for column in &columns {
            match &column.expr {
                SqlExpr::Column(name) if *name == column.alias => {}
                SqlExpr::Column(name) => self.renames.push((column.alias.clone(), name.clone())),
                expr => self.expr(expr, Clause::Computed),
            }
        }
        Ok(())
    }

    /// Every column a serialized `Vec<WindowFuncSpec>` reads.
    pub(super) fn window_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let specs: Vec<WindowFuncSpec> = decode(bytes, "window functions")?;
        for spec in &specs {
            let order = spec.order_by.iter().map(|(expr, _)| expr);
            for expr in spec.args.iter().chain(&spec.partition_by).chain(order) {
                self.expr(expr, Clause::Window);
            }
        }
        Ok(())
    }

    pub(super) fn sort_keys(&mut self, keys: &[SortKeySpec]) {
        for key in keys {
            self.expr(&key.expr, Clause::Sort);
        }
    }

    pub(super) fn group_keys(&mut self, keys: &[GroupKeySpec]) {
        for key in keys {
            if let Some(field) = &key.field {
                self.field(field, Clause::Group);
            }
            if let Some(expr) = &key.expr {
                self.expr(expr, Clause::Group);
            }
        }
    }

    pub(super) fn group_names(&mut self, names: &[String]) {
        for name in names {
            self.field(name, Clause::Group);
        }
    }

    /// Every column an assignment's right-hand side reads.
    pub(super) fn updates(&mut self, updates: &[(String, UpdateValue)]) {
        self.updates_of_target(updates, None);
    }

    /// Every target column an assignment's right-hand side reads. A column
    /// qualified with `source_alias` is the source row's, and the source
    /// relation is checked on its own.
    pub(super) fn updates_of_target(
        &mut self,
        updates: &[(String, UpdateValue)],
        source_alias: Option<&str>,
    ) {
        for (_, value) in updates {
            let UpdateValue::Expr(expr) = value else {
                continue;
            };
            let mut columns = Vec::new();
            collect_columns(expr, &mut columns);
            for column in columns {
                let from_source = source_alias.is_some_and(|alias| {
                    column
                        .split_once('.')
                        .is_some_and(|(qualifier, _)| qualifier == alias)
                });
                if !from_source {
                    self.field(column, Clause::Assignment);
                }
            }
        }
    }

    /// Both columns of each `(left, right)` join key pair.
    pub(super) fn join_keys(&mut self, keys: &[(String, String)]) {
        for (left, right) in keys {
            self.field(left, Clause::JoinKey);
            self.field(right, Clause::JoinKey);
        }
    }

    pub(super) fn passthrough(&mut self, names: &[String]) {
        self.passthrough.extend(names.iter().cloned());
    }
}

/// Decode a serialized plan payload. A payload that does not decode fails
/// the statement: the refusal cannot vouch for reads it cannot see.
fn decode<'a, T: zerompk::FromMessagePack<'a>>(bytes: &'a [u8], what: &str) -> crate::Result<T> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Internal {
        detail: format!("redaction check: {what} do not decode: {e}"),
    })
}
