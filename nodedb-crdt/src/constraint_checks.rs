// SPDX-License-Identifier: Apache-2.0

//! Constraint-checking methods for the [`Validator`].
//!
//! All methods are `impl Validator` blocks and belong logically to the
//! validator, but live here to respect file-size guidelines.

use crate::constraint::{Constraint, ConstraintKind};
use crate::dead_letter::CompensationHint;
use crate::row_lookup::RowLookup;
use crate::validator::{ProposedChange, Validator, Violation};
use loro::LoroValue;

impl Validator {
    pub(crate) fn check_constraint(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        match &constraint.kind {
            ConstraintKind::Unique => self.check_unique(state, change, constraint),
            ConstraintKind::ForeignKey {
                ref_collection,
                ref_key,
            }
            | ConstraintKind::BiTemporalFK {
                ref_collection,
                ref_key,
            } => self.check_foreign_key(state, change, constraint, ref_collection, ref_key),
            ConstraintKind::NotNull => self.check_not_null(change, constraint),
            ConstraintKind::Check { .. } => {
                // Custom checks are application-defined; we can't evaluate them
                // generically. They'd be registered as closures in a real impl.
                None
            }
        }
    }

    pub(crate) fn check_unique(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        let value = &field_value.1;

        // Bitemporal collections: only consider live (non-superseded) rows,
        // so a new version of the same logical row with the same value does
        // not spuriously collide with its prior version.
        // Exclude the row's own already-committed version so re-validating a
        // committed row does not falsely collide with itself; a second, distinct
        // row carrying the same value still collides.
        let exclude = Some(change.row_id.as_str());
        let exists = if self.is_bitemporal(&change.collection) {
            state.field_value_exists_live(&change.collection, &constraint.field, value, exclude)
        } else {
            state.field_value_exists(&change.collection, &constraint.field, value, exclude)
        };

        if exists {
            let value_str = format!("{:?}", value);
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "value {} for field `{}` already exists in `{}`",
                    value_str, constraint.field, constraint.collection
                ),
                hint: CompensationHint::RetryWithDifferentValue {
                    field: constraint.field.clone(),
                    conflicting_value: value_str.clone(),
                    suggestion: format!("{value_str}-dedup"),
                },
            })
        } else {
            None
        }
    }

    pub(crate) fn check_foreign_key(
        &self,
        state: &impl RowLookup,
        change: &ProposedChange,
        constraint: &Constraint,
        ref_collection: &str,
        ref_key: &str,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        // The FK value should reference an existing row_id in the ref collection.
        let ref_id = match &field_value.1 {
            LoroValue::String(s) => s.to_string(),
            LoroValue::I64(n) => n.to_string(),
            other => format!("{:?}", other),
        };

        if !state.row_exists(ref_collection, &ref_id) {
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "foreign key `{}` references `{}.{}` = `{}` which does not exist",
                    constraint.field, ref_collection, ref_key, ref_id
                ),
                hint: CompensationHint::CreateReferencedRow {
                    ref_collection: ref_collection.to_string(),
                    ref_key: ref_key.to_string(),
                    missing_value: ref_id,
                },
            })
        } else {
            None
        }
    }

    pub(crate) fn check_not_null(
        &self,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field);

        match field_value {
            None => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` is required but not provided", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            Some((_, LoroValue::Null)) => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` must not be null", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod bitemporal_fk_tests {
    use super::*;
    use crate::constraint::ConstraintKind;
    use crate::state::CrdtState;
    use crate::validator::Validator;
    use loro::LoroValue;

    fn make_btfk_constraint(ref_collection: &str, ref_key: &str) -> Constraint {
        Constraint {
            name: "test_btfk".to_string(),
            collection: "referrer".to_string(),
            field: "ref_id".to_string(),
            kind: ConstraintKind::BiTemporalFK {
                ref_collection: ref_collection.to_string(),
                ref_key: ref_key.to_string(),
            },
        }
    }

    fn make_change(ref_value: &str) -> ProposedChange {
        ProposedChange {
            collection: "referrer".to_string(),
            row_id: "row1".to_string(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("ref_id".to_string(), LoroValue::String(ref_value.into()))],
        }
    }

    /// Test-only `RowLookup` that treats a fixed set of ids as live array
    /// surrogates, mirroring the tenant-level cross-engine FK registry that now
    /// lives outside `CrdtState`.
    struct ArraySurrogateLookup<'a> {
        state: &'a CrdtState,
        surrogates: std::collections::HashSet<String>,
    }

    impl crate::row_lookup::RowLookup for ArraySurrogateLookup<'_> {
        fn row_exists(&self, collection: &str, row_id: &str) -> bool {
            self.state.row_exists(collection, row_id) || self.surrogates.contains(row_id)
        }
        fn field_value_exists(
            &self,
            collection: &str,
            field: &str,
            value: &LoroValue,
            exclude_row_id: Option<&str>,
        ) -> bool {
            self.state
                .field_value_exists(collection, field, value, exclude_row_id)
        }
        fn field_value_exists_live(
            &self,
            collection: &str,
            field: &str,
            value: &LoroValue,
            exclude_row_id: Option<&str>,
        ) -> bool {
            self.state
                .field_value_exists_live(collection, field, value, exclude_row_id)
        }
    }

    #[test]
    fn bitemporal_fk_passes_when_array_surrogate_exists() {
        let state = CrdtState::new(1).unwrap();
        let lookup = ArraySurrogateLookup {
            state: &state,
            surrogates: std::iter::once("surr-42".to_string()).collect(),
        };

        let validator = Validator::new(Default::default(), 16);
        let constraint = make_btfk_constraint("variants", "id");
        let change = make_change("surr-42");

        let violation =
            validator.check_foreign_key(&lookup, &change, &constraint, "variants", "id");
        assert!(violation.is_none());
    }

    #[test]
    fn bitemporal_fk_fails_when_array_surrogate_missing() {
        let state = CrdtState::new(1).unwrap();
        let validator = Validator::new(Default::default(), 16);
        let constraint = make_btfk_constraint("variants", "id");
        let change = make_change("surr-99");

        let violation = validator.check_foreign_key(&state, &change, &constraint, "variants", "id");
        assert!(violation.is_some());
        let v = violation.unwrap();
        assert_eq!(v.constraint_name, "test_btfk");
        assert!(v.reason.contains("surr-99"));
    }
}
