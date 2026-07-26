// SPDX-License-Identifier: Apache-2.0

//! Which relations a column reference can name, and whether that naming is
//! unambiguous.
//!
//! This is the safety contract of catalog-backed inference. Resolving a
//! column to the wrong relation reports a wrong OID, and a wrong OID makes
//! the client commit to a binary encoding the server cannot decode — a hard
//! client-side failure where saying nothing would have degraded to text
//! format. Every method here therefore returns `None` on the first hint of
//! ambiguity rather than picking a candidate.

use nodedb_types::DatabaseId;
use sqlparser::ast::{ObjectName, TableFactor, TableWithJoins};

use crate::catalog::SqlCatalog;
use crate::types::{CollectionInfo, ColumnInfo};

/// One relation visible to a column reference.
struct Relation {
    /// The name a qualified reference must use: the alias when one is
    /// declared, otherwise the relation's own name.
    qualifier: String,
    info: CollectionInfo,
}

/// The relations a single statement body can address.
pub(super) struct Scope {
    relations: Vec<Relation>,
    /// `true` when at least one item in scope could not be resolved to
    /// catalog metadata — an unknown table, a derived table, a table-valued
    /// function, a CTE reference.
    ///
    /// Such a relation may declare any column name at all, so a *bare* column
    /// reference can no longer be proven unique and must not resolve.
    /// Qualified references are unaffected: they name one relation outright,
    /// and an unresolved relation contributes no qualifier to match against.
    opaque: bool,
}

impl Scope {
    /// The relations named by a `FROM` clause (or an `UPDATE` target).
    pub(super) fn from_tables(catalog: &dyn SqlCatalog, tables: &[TableWithJoins]) -> Self {
        let mut scope = Self {
            relations: Vec::new(),
            opaque: false,
        };
        scope.add_tables(catalog, tables);
        scope
    }

    /// Widen the scope with further relations (`UPDATE ... FROM`,
    /// `DELETE ... USING`).
    pub(super) fn add_tables(&mut self, catalog: &dyn SqlCatalog, tables: &[TableWithJoins]) {
        for table in tables {
            self.add_factor(catalog, &table.relation);
            for join in &table.joins {
                self.add_factor(catalog, &join.relation);
            }
        }
    }

    fn add_factor(&mut self, catalog: &dyn SqlCatalog, factor: &TableFactor) {
        // A plain named table is the only factor whose columns this pass can
        // enumerate. Derived tables, table functions (`TableFactor::Table`
        // with `args`), `UNNEST`, pivots and the rest are intentionally not a
        // form this pass resolves — they poison bare-name resolution instead
        // of being guessed at.
        let TableFactor::Table {
            name, alias, args, ..
        } = factor
        else {
            self.opaque = true;
            return;
        };
        if args.is_some() {
            self.opaque = true;
            return;
        }
        let Some(relation_name) = last_ident(name) else {
            self.opaque = true;
            return;
        };
        let info = match catalog.resolve_relation(DatabaseId::DEFAULT, &relation_name) {
            Ok(Some(info)) => info,
            // Absent, soft-deleted, or mid-drain: the columns are unknown to
            // this pass either way. Catalog failures are never surfaced as
            // errors here — inference is best-effort by construction.
            Ok(None) | Err(_) => {
                self.opaque = true;
                return;
            }
        };
        let qualifier = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or(relation_name);
        self.relations.push(Relation { qualifier, info });
    }

    /// Resolve a dotted column reference to its catalog column.
    ///
    /// `None` whenever the reference could name more than one relation, names
    /// a relation not in scope, or is deeper than `qualifier.column`.
    pub(super) fn resolve_column(&self, parts: &[&str]) -> Option<&ColumnInfo> {
        match parts {
            [column] => self.resolve_bare(column),
            [qualifier, column] => self.resolve_qualified(qualifier, column),
            // A deeper path is a schema qualification or a JSON traversal,
            // neither of which is a form this pass resolves. Intentional.
            _ => None,
        }
    }

    fn resolve_bare(&self, column: &str) -> Option<&ColumnInfo> {
        if self.opaque {
            return None;
        }
        let mut found: Option<&ColumnInfo> = None;
        for relation in &self.relations {
            let Some(candidate) = column_of(&relation.info, column) else {
                continue;
            };
            if found.is_some() {
                // Present in more than one relation in scope. The SQL itself
                // does not say which one is meant, so neither may this pass.
                return None;
            }
            found = Some(candidate);
        }
        found
    }

    fn resolve_qualified(&self, qualifier: &str, column: &str) -> Option<&ColumnInfo> {
        let mut matching = self
            .relations
            .iter()
            .filter(|relation| relation.qualifier.eq_ignore_ascii_case(qualifier));
        let relation = matching.next()?;
        if matching.next().is_some() {
            // The same qualifier bound twice: not unambiguously in scope.
            return None;
        }
        column_of(&relation.info, column)
    }
}

/// Look up the relation an `INSERT` / statement target names.
pub(super) fn lookup_relation(
    catalog: &dyn SqlCatalog,
    name: &ObjectName,
) -> Option<CollectionInfo> {
    let relation_name = last_ident(name)?;
    match catalog.resolve_relation(DatabaseId::DEFAULT, &relation_name) {
        Ok(Some(info)) => Some(info),
        // Unknown or unavailable: leave every position of this statement
        // unresolved rather than typing them against a guess.
        Ok(None) | Err(_) => None,
    }
}

/// A relation's column by name, matched case-insensitively the way the
/// planner's own identifier resolution does.
pub(super) fn column_of<'a>(info: &'a CollectionInfo, column: &str) -> Option<&'a ColumnInfo> {
    info.columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
}

/// The final identifier of an object name — `t` in both `t` and `public.t`.
///
/// `None` when the last part is not a plain identifier (a dialect-specific
/// name-producing function), which is not a form this pass resolves.
fn last_ident(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
}
