// SPDX-License-Identifier: Apache-2.0

//! Math scalar function registrations.
//!
//! The plan-time existence gate
//! (`resolver::expr::functions::convert_function_depth`) rejects any name
//! missing here, so this list is audited to match every match arm in
//! `nodedb_query::functions::math::try_eval` exactly: `abs`, `ceil` /
//! `ceiling`, `floor`, `round`, `power` / `pow`, `sqrt`, `mod`, `sign`,
//! `log` / `ln`, `log10`, `log2`, `exp`. Before this audit only `abs`,
//! `ceil`, `floor`, and `round` were registered — the rest resolved at
//! runtime but were unreachable from SQL because the gate rejected them
//! first. Keep both lists in sync.

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn math_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "abs",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "ceil",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "floor",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "round",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::ROUND_ARGS,
        ),
        // `ceiling` is an alias for `ceil` — same
        // `nodedb_query::functions::math::try_eval` match arm
        // (`"ceil" | "ceiling"`).
        m(
            "ceiling",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "sqrt",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        // `mod(a, b)`: the zero-modulus arm raises `EvalError::DivisionByZero`
        // at the runtime evaluator rather than folding to
        // NULL — this registration only gates existence/arity, not that
        // runtime behavior.
        m(
            "mod",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::MATH_2_ARGS,
        ),
        m(
            "power",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::MATH_2_ARGS,
        ),
        // `pow` is an alias for `power` — same
        // `nodedb_query::functions::math::try_eval` match arm (`"power" | "pow"`).
        m(
            "pow",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::MATH_2_ARGS,
        ),
        m(
            "sign",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "log",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        // `ln` is an alias for `log` (natural log) — same
        // `nodedb_query::functions::math::try_eval` match arm (`"log" | "ln"`).
        m(
            "ln",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "log10",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "log2",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "exp",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
    ]
}
