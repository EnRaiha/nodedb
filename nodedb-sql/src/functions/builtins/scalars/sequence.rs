// SPDX-License-Identifier: Apache-2.0

//! Sequence accessor registrations (`nextval`/`currval`/`setval`).
//!
//! These names are stateful and CP-side only: they evaluate as column
//! DEFAULTs via the plan converter, never as SQL-expression scalars. The
//! plan-time existence gate still needs them registered so that a bare
//! expression use is planned (typed, arity-checked) and then fails LOUDLY
//! at fold/row-eval time with 0A000 — instead of being rejected as
//! "function does not exist" (42883) or silently NULLing at runtime.
//!
//! This list must stay in sync with the sequence guard arm in
//! `nodedb_query::functions::eval_function`.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{ArgTypeSpec, FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

static SEQ_1_ARGS: &[ArgTypeSpec] = &[arg_types::any("seq")];
static SEQ_2_ARGS: &[ArgTypeSpec] = &[arg_types::any("seq"), arg_types::any("value")];

pub(super) fn sequence_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "nextval",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            SEQ_1_ARGS,
        ),
        m(
            "currval",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            SEQ_1_ARGS,
        ),
        m(
            "setval",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Int64),
            SEQ_2_ARGS,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use crate::functions::registry::FunctionRegistry;

    #[test]
    fn accessors_registered_for_plan_gate() {
        let reg = FunctionRegistry::new();
        for name in ["nextval", "currval", "setval"] {
            assert!(reg.lookup(name).is_some(), "{name} must be registered");
        }
    }

    #[test]
    fn accessors_error_loudly_in_expression_eval() {
        // A1+A3 contract: registered, arity-checked, then loud 0A000 at
        // fold/row-eval — never a silent Null (parity invariant seed).
        for name in ["nextval", "currval"] {
            let err = nodedb_query::functions::eval_function(
                name,
                &[nodedb_types::Value::String("s".into())],
            )
            .unwrap_err();
            assert!(
                matches!(err, nodedb_query::EvalError::FeatureNotSupported { .. }),
                "{name} must raise FeatureNotSupported, got {err:?}"
            );
        }
    }
}
