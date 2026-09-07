// SPDX-License-Identifier: BUSL-1.1

//! Sequence-aware DEFAULT expression evaluation.
//!
//! The pure `nodedb_sql::planner::defaults::evaluate_default_expr` handles
//! stateless defaults (UUID, now(), literals). Sequence-backed defaults —
//! `DEFAULT nextval('seq')` — are stateful and evaluate through the CP-side
//! `SequenceRegistry` threaded on `ConvertContext`. Without this route the
//! expression fell through to "unknown function -> None", which silently
//! omitted the column (NULL key, per #294).

use nodedb_sql::types::SqlValue;
use nodedb_types::Value;

use super::super::convert::ConvertContext;

/// Classify a DEFAULT expression against the canonical sequence-accessor
/// parser, routing `nextval` to the CP-side registry and rejecting the
/// other accessors loudly — `currval`/`setval` have no per-row meaning in a
/// DEFAULT, and a DDL-accepted DEFAULT must never silently vanish into NULL.
fn evaluate_sequence_default(ctx: &ConvertContext, expr: &str) -> crate::Result<Option<Value>> {
    let Some((accessor, name)) = nodedb_sql::planner::defaults::sequence_accessor(expr) else {
        // Accessor-shaped but malformed (e.g. nextval('')): raise loudly, the
        // pure evaluator would silently vanish it (#294 class).
        if nodedb_sql::planner::defaults::looks_like_sequence_accessor(expr) {
            return Err(crate::Error::PlanError {
                detail: format!("malformed sequence default: '{expr}'"),
            });
        }
        return Ok(None);
    };
    use nodedb_sql::planner::defaults::SequenceAccessor;
    let value = match accessor {
        SequenceAccessor::Nextval => {
            let Some(registry) = &ctx.sequence_registry else {
                return Err(crate::Error::PlanError {
                    detail: format!("sequence default '{expr}' requires sequence registry access"),
                });
            };
            match registry.nextval(ctx.database_id.as_u64(), ctx.tenant_id.as_u64(), &name) {
                Ok(value) => Value::Integer(value),
                Err(e) => {
                    return Err(crate::Error::PlanError {
                        detail: format!("nextval('{name}'): {e}"),
                    });
                }
            }
        }
        SequenceAccessor::Currval | SequenceAccessor::Setval => {
            let accessor_name = match accessor {
                SequenceAccessor::Currval => "currval",
                SequenceAccessor::Setval => "setval",
                SequenceAccessor::Nextval => unreachable!("handled above"),
            };
            return Err(crate::Error::PlanError {
                detail: format!(
                    "DEFAULT {accessor_name}('{name}') is not supported — sequence defaults must use nextval('{name}')"
                ),
            });
        }
    };
    Ok(Some(value))
}

/// Extract the sequence name from a `nextval('name')` DEFAULT. Shared with
/// the kv converter, which advances the registry on a different code path.
pub(crate) fn sequence_name(expr: &str) -> Option<String> {
    let (accessor, name) = nodedb_sql::planner::defaults::sequence_accessor(expr)?;
    if accessor == nodedb_sql::planner::defaults::SequenceAccessor::Nextval {
        Some(name)
    } else {
        None
    }
}

/// Materialize every missing column DEFAULT for one row, in order:
/// sequence accessors first (CP registry), then the pure evaluator.
/// Shared by the INSERT and UPSERT document-family paths so no statement
/// shape can silently drop a declared default (#294).
pub(crate) fn expand_row_defaults(
    ctx: &ConvertContext,
    row: &[(String, SqlValue)],
    column_defaults: &[(String, String)],
) -> crate::Result<Vec<(String, SqlValue)>> {
    let mut expanded: Vec<(String, SqlValue)> = row.to_vec();
    for (col_name, default_expr) in column_defaults {
        if expanded.iter().any(|(k, _)| k == col_name) {
            continue;
        }
        let maybe_val = match evaluate_sequence_default(ctx, default_expr) {
            Ok(Some(value)) => Some(value),
            Ok(None) => {
                super::evaluate_default_expr(default_expr).map_err(|e| crate::Error::PlanError {
                    detail: format!("default for column '{col_name}': {e}"),
                })?
            }
            Err(e) => return Err(e),
        };
        if let Some(val) = maybe_val {
            expanded.push((col_name.clone(), super::convert::nodedb_value_to_sql(val)));
        }
    }
    Ok(expanded)
}
