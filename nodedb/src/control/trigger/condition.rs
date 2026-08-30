// SPDX-License-Identifier: BUSL-1.1

//! Constant-condition evaluation shortcut, bypassing DataFusion.

/// Evaluate a simple boolean condition without DataFusion.
///
/// Returns `Some(true/false)` for constant conditions (TRUE, FALSE, 1, 0, NULL).
/// Returns `None` for complex conditions that need DataFusion evaluation.
pub fn try_eval_simple_condition(condition: &str) -> Option<bool> {
    let trimmed = condition.trim().to_uppercase();
    match trimmed.as_str() {
        "TRUE" | "1" => Some(true),
        "FALSE" | "0" | "NULL" => Some(false),
        _ => None,
    }
}
