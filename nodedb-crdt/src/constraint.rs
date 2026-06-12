// SPDX-License-Identifier: Apache-2.0

//! SQL constraint definitions for CRDT collections.
//!
//! Constraints are checked at commit time against the leader's state.
//! They define invariants that must hold globally, even though individual
//! agents operate optimistically without them.

use serde::{Deserialize, Serialize};

/// The kind of SQL constraint to enforce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    /// No two rows may have the same value for this key.
    /// Analogous to SQL `UNIQUE(column)`.
    Unique,

    /// The value must reference an existing key in another collection.
    /// Analogous to SQL `FOREIGN KEY(column) REFERENCES other(key)`.
    ForeignKey {
        /// The referenced collection name.
        ref_collection: String,
        /// The referenced key field.
        ref_key: String,
    },

    /// Bitemporal foreign key. Write-side semantics match `ForeignKey`
    /// (referent must exist live), but on referent delete the referrer
    /// row/edge is *closed* by appending a new version with
    /// `valid_until_ms = now` rather than being cascade-deleted. This
    /// preserves the historical truth that the relationship existed.
    BiTemporalFK {
        ref_collection: String,
        ref_key: String,
    },

    /// The value must not be null/empty.
    /// Analogous to SQL `NOT NULL`.
    NotNull,

    /// Custom predicate — evaluated as a boolean expression on the row.
    /// Analogous to SQL `CHECK(expression)`.
    Check {
        /// Human-readable description of the check.
        description: String,
    },
}

/// A constraint bound to a specific collection and field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    /// Unique name for this constraint (e.g., "users_email_unique").
    pub name: String,
    /// The collection (table) this constraint applies to.
    pub collection: String,
    /// The field (column) this constraint applies to.
    pub field: String,
    /// The kind of constraint.
    pub kind: ConstraintKind,
}

/// A set of constraints for a schema.
#[derive(Debug, Clone, Default)]
pub struct ConstraintSet {
    constraints: Vec<Constraint>,
}

impl ConstraintSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a constraint.
    pub fn add(&mut self, constraint: Constraint) {
        self.constraints.push(constraint);
    }

    /// Add a UNIQUE constraint.
    pub fn add_unique(&mut self, name: &str, collection: &str, field: &str) {
        self.add(Constraint {
            name: name.to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            kind: ConstraintKind::Unique,
        });
    }

    /// Add a FOREIGN KEY constraint.
    pub fn add_foreign_key(
        &mut self,
        name: &str,
        collection: &str,
        field: &str,
        ref_collection: &str,
        ref_key: &str,
    ) {
        self.add(Constraint {
            name: name.to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            kind: ConstraintKind::ForeignKey {
                ref_collection: ref_collection.to_string(),
                ref_key: ref_key.to_string(),
            },
        });
    }

    /// Add a BITEMPORAL FOREIGN KEY constraint.
    pub fn add_bitemporal_fk(
        &mut self,
        name: &str,
        collection: &str,
        field: &str,
        ref_collection: &str,
        ref_key: &str,
    ) {
        self.add(Constraint {
            name: name.to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            kind: ConstraintKind::BiTemporalFK {
                ref_collection: ref_collection.to_string(),
                ref_key: ref_key.to_string(),
            },
        });
    }

    /// Add a NOT NULL constraint.
    pub fn add_not_null(&mut self, name: &str, collection: &str, field: &str) {
        self.add(Constraint {
            name: name.to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            kind: ConstraintKind::NotNull,
        });
    }

    /// Get all constraints for a given collection.
    pub fn for_collection(&self, collection: &str) -> Vec<&Constraint> {
        self.constraints
            .iter()
            .filter(|c| c.collection == collection)
            .collect()
    }

    /// Get all constraints.
    pub fn all(&self) -> &[Constraint] {
        &self.constraints
    }

    /// Number of constraints.
    pub fn len(&self) -> usize {
        self.constraints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.constraints.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_set_operations() {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
        cs.add_bitemporal_fk("orders_user_btfk", "orders", "user_id", "users", "id");

        assert_eq!(cs.len(), 4);
        assert_eq!(cs.for_collection("users").len(), 2);
        assert_eq!(cs.for_collection("posts").len(), 1);
        assert_eq!(cs.for_collection("orders").len(), 1);
        assert_eq!(cs.for_collection("missing").len(), 0);

        let btfk = cs.for_collection("orders")[0];
        assert!(matches!(btfk.kind, ConstraintKind::BiTemporalFK { .. }));
    }
}
