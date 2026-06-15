// SPDX-License-Identifier: BUSL-1.1

//! Parse / lowering error for virtual-table SELECTs.

use super::super::expr::EvalError;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unsupported on virtual catalog tables: {0}")]
    Unsupported(String),
    #[error("eval error: {0}")]
    Eval(#[from] EvalError),
}
