// SPDX-License-Identifier: BUSL-1.1

//! Expression AST and value types for virtual-table queries.

use super::super::value::VValue;

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(VValue),
    /// A column reference, optionally qualified by a relation alias.
    Column {
        qualifier: Option<String>,
        name: String,
    },
    /// Sentinel for `COUNT(*)`.
    Star,
    BinaryOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryNot(Box<Expr>),
    UnaryNeg(Box<Expr>),
    IsNull(Box<Expr>, bool /*negated*/),
    InList(Box<Expr>, Vec<Expr>, bool /*negated*/),
    Between(Box<Expr>, Box<Expr>, Box<Expr>, bool /*negated*/),
    Like(Box<Expr>, String, bool /*negated*/),
    /// A `CAST` / `::` expression to a supported target type.
    Cast(Box<Expr>, CastType),
    /// `left <op> ANY(array)` (`any = true`) or `left <op> ALL(array)`.
    AnyAll {
        left: Box<Expr>,
        op: BinOp,
        array: Box<Expr>,
        any: bool,
    },
    /// An `ARRAY[...]` literal.
    Array(Vec<Expr>),
    /// A catalog scalar function call.
    ScalarFn(ScalarFn, Vec<Expr>),
    /// An aggregate function (only valid in projection position).
    Aggregate(AggFn, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// Cast targets the catalog evaluator resolves. `Regclass` / `Regtype` perform
/// catalog name → OID lookups; the rest are value coercions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastType {
    Regclass,
    Regtype,
    Oid,
    Int8,
    Int4,
    Text,
    Bool,
}

/// Catalog scalar functions emitted by PostgreSQL clients during connection
/// setup and schema reflection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFn {
    CurrentSchemas,
    CurrentSchema,
    CurrentDatabase,
    CurrentUser,
    CurrentRole,
    Version,
}

#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("unknown column: {0}")]
    UnknownColumn(String),
    #[error("column reference {0} is ambiguous")]
    AmbiguousColumn(String),
    #[error("type mismatch in expression: {0}")]
    TypeMismatch(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("type \"{0}\" does not exist")]
    UndefinedType(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("aggregate functions only allowed in projection")]
    AggregateInPredicate,
}
