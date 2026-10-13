// SPDX-License-Identifier: BUSL-1.1

//! PromQL abstract syntax tree.

use super::label::LabelMatcher;

/// Top-level PromQL expression.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A number literal: `42`, `3.14`.
    Scalar(f64),

    /// A string literal: `"hello"`.
    StringLiteral(String),

    /// Instant vector selector: `metric_name{label="value"}`.
    VectorSelector {
        name: Option<String>,
        matchers: Vec<LabelMatcher>,
        offset: Option<Duration>,
    },

    /// Range vector selector: `metric_name{...}[5m]`.
    MatrixSelector {
        /// Inner instant-vector selector.
        selector: Box<Expr>,
        /// Range duration.
        range: Duration,
    },

    /// Binary operation: `a + b`, `a > bool b`.
    BinaryOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        return_bool: bool,
        matching: Option<VectorMatching>,
    },

    /// Unary negation: `-expr`.
    Negate(Box<Expr>),

    /// Aggregation: `sum by (label) (expr)`.
    Aggregate {
        op: AggOp,
        expr: Box<Expr>,
        param: Option<Box<Expr>>,
        grouping: Grouping,
    },

    /// Function call: `rate(expr[5m])`.
    Call { func: String, args: Vec<Expr> },

    /// Parenthesized expression.
    Paren(Box<Expr>),

    /// Subquery: `expr[range:step]` or `expr[range:]`, optionally followed
    /// by `offset`.
    Subquery {
        expr: Box<Expr>,
        range: Duration,
        /// `None` for `[range:]`: the default evaluation step.
        step: Option<Duration>,
        offset: Option<Duration>,
    },
}

/// Duration in milliseconds (PromQL durations: 5s, 1m, 1h, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Duration(pub i64);

impl Duration {
    pub fn ms(&self) -> i64 {
        self.0
    }

    /// Parse a PromQL duration string (e.g., "5m", "1h30m", "300s").
    pub fn parse(s: &str) -> Option<Self> {
        let mut total_ms: i64 = 0;
        let mut num_buf = String::new();

        for ch in s.chars() {
            if ch.is_ascii_digit() || ch == '.' {
                num_buf.push(ch);
            } else {
                let n: f64 = num_buf.parse().ok()?;
                num_buf.clear();
                let multiplier: i64 = match ch {
                    'y' => 365 * 24 * 3600 * 1000,
                    'w' => 7 * 24 * 3600 * 1000,
                    'd' => 24 * 3600 * 1000,
                    'h' => 3600 * 1000,
                    'm' => 60 * 1000,
                    's' => 1000,
                    _ => return None,
                };
                total_ms += (n * multiplier as f64) as i64;
            }
        }
        // Bare number without suffix = seconds.
        if !num_buf.is_empty() {
            let n: f64 = num_buf.parse().ok()?;
            total_ms += (n * 1000.0) as i64;
        }

        if total_ms > 0 {
            Some(Self(total_ms))
        } else {
            None
        }
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Unless,
}

impl BinOp {
    pub fn precedence(&self) -> u8 {
        match self {
            Self::Pow => 6,
            Self::Mul | Self::Div | Self::Mod => 5,
            Self::Add | Self::Sub => 4,
            Self::Eq | Self::Neq | Self::Lt | Self::Gt | Self::Lte | Self::Gte => 3,
            Self::And | Self::Unless => 2,
            Self::Or => 1,
        }
    }

    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Lt | Self::Gt | Self::Lte | Self::Gte
        )
    }

    pub fn is_set_op(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }
}

/// Aggregation operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Topk,
    Bottomk,
    Quantile,
    CountValues,
    Group,
}

/// Aggregation grouping modifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// No grouping (aggregate over all series).
    None,
    /// `by (label1, label2)`.
    By(Vec<String>),
    /// `without (label1, label2)`.
    Without(Vec<String>),
}

/// Vector matching modifier for binary operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMatching {
    pub card: MatchCard,
    pub on: Vec<String>,
    pub ignoring: Vec<String>,
    pub group_left: Vec<String>,
    pub group_right: Vec<String>,
}

/// Cardinality of a binary vector match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchCard {
    OneToOne,
    ManyToOne,
    OneToMany,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parse() {
        assert_eq!(Duration::parse("5m"), Some(Duration(300_000)));
        assert_eq!(Duration::parse("1h"), Some(Duration(3_600_000)));
        assert_eq!(Duration::parse("1h30m"), Some(Duration(5_400_000)));
        assert_eq!(Duration::parse("300s"), Some(Duration(300_000)));
        assert_eq!(Duration::parse("1d"), Some(Duration(86_400_000)));
        assert_eq!(Duration::parse("1w"), Some(Duration(604_800_000)));
    }

    #[test]
    fn binop_precedence() {
        assert!(BinOp::Mul.precedence() > BinOp::Add.precedence());
        assert!(BinOp::Pow.precedence() > BinOp::Mul.precedence());
        assert!(BinOp::Add.precedence() > BinOp::Eq.precedence());
        assert!(BinOp::Eq.precedence() > BinOp::And.precedence());
        assert!(BinOp::And.precedence() > BinOp::Or.precedence());
    }
}
