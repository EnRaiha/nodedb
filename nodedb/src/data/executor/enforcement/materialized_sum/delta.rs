// SPDX-License-Identifier: BUSL-1.1

//! The pure fold from a write's row images to the signed balance deltas that
//! write owes each materialized-sum target.
//!
//! Deliberately free functions with no `CoreLoop`, no storage handle and no
//! transaction: everything here is arithmetic over two documents and a binding,
//! so the interesting cases — a DELETE subtracting, an UPDATE contributing only
//! its difference, an UPDATE that moves a row from one target to another — are
//! all testable without opening a database.
//!
//! The defect this replaces derived ONE positive delta from ONE document. A
//! total maintained that way only ever grows: a DELETE never subtracted, an
//! UPDATE re-added the row's whole new value on top of its old contribution, and
//! a row whose join key changed kept crediting the target it had left.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::MaterializedSumBinding;

use crate::data::executor::enforcement::images::RowImages;

/// One signed contribution a write makes to one target row's running total.
///
/// `join_value` is the JOIN-KEY VALUE the source row carries, not a storage
/// key. It identifies which of `EnforcementCtx::resolved_targets`' entries
/// addresses the target row; the surrogate in that entry is the only thing that
/// may be used to read or write it.
#[derive(Debug, PartialEq)]
pub(in crate::data::executor) struct SumDelta {
    /// Value of the source row's join column.
    pub join_value: String,
    /// Signed amount to add to the target's balance column. Negative on the
    /// losing side of a DELETE or a join-key move.
    pub delta: Decimal,
}

/// Fold one binding over a write's row images into the signed deltas it owes.
///
/// The match is exhaustive with no `_` arm, so a new mutation shape cannot fall
/// silently into the INSERT case — which is precisely how a delete came to be
/// accounted as a positive contribution.
///
/// # The join-key move
///
/// An `Update` whose join key CHANGED yields TWO deltas against TWO targets:
/// the old target loses the row's old value, the new target gains its new one.
/// That is why [`RowImages`] needs no `Move` variant — the split is derived
/// here, where the binding names the join column and the comparison means
/// something, rather than being pushed onto every other enforcement.
///
/// A row that carries no join value contributes nothing: it does not
/// participate in the binding at all, so there is no target for it to move
/// value onto. That mirrors the Control-Plane resolution, which skips the same
/// rows rather than failing them.
pub(in crate::data::executor) fn fold_sum_deltas(
    binding: &MaterializedSumBinding,
    images: &RowImages<'_>,
) -> crate::Result<Vec<SumDelta>> {
    match images {
        RowImages::Insert { new_doc } => Ok(contribution(binding, new_doc, Sign::Plus)?
            .into_iter()
            .collect()),
        RowImages::Delete { old_doc } => Ok(contribution(binding, old_doc, Sign::Minus)?
            .into_iter()
            .collect()),
        RowImages::Update { old_doc, new_doc } => {
            let old_target = join_value_of(binding, old_doc);
            let new_target = join_value_of(binding, new_doc);
            match (old_target, new_target) {
                // Same target: only the DIFFERENCE moves. Re-adding the new
                // value in full is the double-count the old code shipped.
                (Some(old_target), Some(new_target)) if old_target == new_target => {
                    let delta = amount_of(binding, new_doc)? - amount_of(binding, old_doc)?;
                    Ok(vec![SumDelta {
                        join_value: new_target,
                        delta,
                    }])
                }
                // The join-key move: two targets, two writes, opposite signs.
                (Some(old_target), Some(new_target)) => Ok(vec![
                    SumDelta {
                        join_value: old_target,
                        delta: -amount_of(binding, old_doc)?,
                    },
                    SumDelta {
                        join_value: new_target,
                        delta: amount_of(binding, new_doc)?,
                    },
                ]),
                // The row left the binding (its join column was cleared) or
                // joined it (the column was set). One side only.
                (Some(old_target), None) => Ok(vec![SumDelta {
                    join_value: old_target,
                    delta: -amount_of(binding, old_doc)?,
                }]),
                (None, Some(new_target)) => Ok(vec![SumDelta {
                    join_value: new_target,
                    delta: amount_of(binding, new_doc)?,
                }]),
                (None, None) => Ok(Vec::new()),
            }
        }
    }
}

/// Which way a single-image contribution points.
enum Sign {
    Plus,
    Minus,
}

/// The one delta a single row image contributes, or `None` when the row does
/// not participate in the binding.
fn contribution(
    binding: &MaterializedSumBinding,
    doc: &serde_json::Value,
    sign: Sign,
) -> crate::Result<Option<SumDelta>> {
    let Some(join_value) = join_value_of(binding, doc) else {
        return Ok(None);
    };
    let amount = amount_of(binding, doc)?;
    Ok(Some(SumDelta {
        join_value,
        delta: match sign {
            Sign::Plus => amount,
            Sign::Minus => -amount,
        },
    }))
}

/// The source row's join-key value, or `None` when the row does not carry one.
fn join_value_of(binding: &MaterializedSumBinding, doc: &serde_json::Value) -> Option<String> {
    doc.get(&binding.join_column)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Evaluate the binding's value expression against one row image.
///
/// A materialized-sum binding fires on the write path, so a division or modulus
/// by zero fails the write rather than silently skipping the balance update. An
/// expression that evaluates to NULL or to something non-numeric contributes
/// zero — the row is in the binding but has nothing to add.
fn amount_of(binding: &MaterializedSumBinding, doc: &serde_json::Value) -> crate::Result<Decimal> {
    let row = nodedb_types::Value::from(doc.clone());
    let evaluated = binding
        .value_expr
        .eval(&row)
        .map_err(|_e| crate::Error::DivisionByZero)?;
    Ok(json_to_decimal(&serde_json::Value::from(evaluated)).unwrap_or(Decimal::ZERO))
}

/// Convert a JSON value to `rust_decimal::Decimal`.
///
/// Strings parse exactly, which is how a balance survives more than 15
/// significant digits: the write-back stores the total as a string for the same
/// reason.
pub(in crate::data::executor) fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Decimal::from(i))
            } else {
                n.as_f64().and_then(|f| Decimal::try_from(f).ok())
            }
        }
        serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("decimal literal")
    }

    fn binding() -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: "ms_accounts".to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    fn row(account: &str, amount: i64) -> serde_json::Value {
        serde_json::json!({"account_id": account, "amount": amount})
    }

    /// An INSERT credits its target with the row's whole value.
    #[test]
    fn insert_adds_the_new_rows_value() {
        let new_doc = row("a1", 25);
        let deltas =
            fold_sum_deltas(&binding(), &RowImages::Insert { new_doc: &new_doc }).expect("fold");
        assert_eq!(
            deltas,
            vec![SumDelta {
                join_value: "a1".to_string(),
                delta: d("25"),
            }]
        );
    }

    /// A DELETE SUBTRACTS. The old API could not express this at all — it took
    /// one document and derived one positive delta — so a deleted row's
    /// contribution stayed in the stored total forever.
    #[test]
    fn delete_subtracts_the_old_rows_value() {
        let old_doc = row("a1", 25);
        let deltas =
            fold_sum_deltas(&binding(), &RowImages::Delete { old_doc: &old_doc }).expect("fold");
        assert_eq!(
            deltas,
            vec![SumDelta {
                join_value: "a1".to_string(),
                delta: d("-25"),
            }]
        );
    }

    /// An UPDATE that keeps its join key contributes only the DIFFERENCE.
    #[test]
    fn update_in_place_contributes_only_the_difference() {
        let old_doc = row("a1", 25);
        let new_doc = row("a1", 40);
        let deltas = fold_sum_deltas(
            &binding(),
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        )
        .expect("fold");
        assert_eq!(
            deltas,
            vec![SumDelta {
                join_value: "a1".to_string(),
                delta: d("15"),
            }]
        );
    }

    /// The join-key MOVE: two targets, two deltas, opposite signs. Accounting
    /// it as a single positive contribution — the old behaviour — leaves the
    /// abandoned target permanently overstated and the new one short.
    #[test]
    fn update_that_moves_the_join_key_yields_two_opposite_deltas() {
        let old_doc = row("a1", 25);
        let new_doc = row("a2", 40);
        let deltas = fold_sum_deltas(
            &binding(),
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        )
        .expect("fold");

        assert_eq!(deltas.len(), 2, "a move touches exactly two targets");
        assert_ne!(
            deltas[0].join_value, deltas[1].join_value,
            "the two deltas must address DIFFERENT targets"
        );
        assert_eq!(deltas[0].join_value, "a1");
        assert_eq!(
            deltas[0].delta,
            d("-25"),
            "the old target loses the old value"
        );
        assert_eq!(deltas[1].join_value, "a2");
        assert_eq!(
            deltas[1].delta,
            d("40"),
            "the new target gains the new value"
        );
        assert!(
            deltas[0].delta.is_sign_negative() && deltas[1].delta.is_sign_positive(),
            "the signs must be opposite: {deltas:?}"
        );
    }

    /// A row with no join value does not participate in the binding.
    #[test]
    fn a_row_without_the_join_column_contributes_nothing() {
        let new_doc = serde_json::json!({"amount": 25});
        let deltas =
            fold_sum_deltas(&binding(), &RowImages::Insert { new_doc: &new_doc }).expect("fold");
        assert!(deltas.is_empty());
    }

    /// Clearing the join column on an UPDATE takes the row's old contribution
    /// back off the target it used to belong to.
    #[test]
    fn clearing_the_join_column_reverses_the_old_contribution() {
        let old_doc = row("a1", 25);
        let new_doc = serde_json::json!({"amount": 25});
        let deltas = fold_sum_deltas(
            &binding(),
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        )
        .expect("fold");
        assert_eq!(
            deltas,
            vec![SumDelta {
                join_value: "a1".to_string(),
                delta: d("-25"),
            }]
        );
    }

    #[test]
    fn json_to_decimal_integer() {
        assert_eq!(json_to_decimal(&serde_json::json!(100)), Some(d("100")));
    }

    #[test]
    fn json_to_decimal_float() {
        assert!(json_to_decimal(&serde_json::json!(99.5)).is_some());
    }

    #[test]
    fn json_to_decimal_string() {
        assert_eq!(
            json_to_decimal(&serde_json::json!("1500.75")),
            Some(d("1500.75"))
        );
    }

    #[test]
    fn json_to_decimal_null() {
        assert_eq!(json_to_decimal(&serde_json::Value::Null), None);
    }

    #[test]
    fn json_to_decimal_negative() {
        assert_eq!(json_to_decimal(&serde_json::json!(-250)), Some(d("-250")));
    }
}
