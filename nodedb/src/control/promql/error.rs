// SPDX-License-Identifier: BUSL-1.1

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PromqlError {
    #[error("unexpected token: expected {expected}, found {found}")]
    UnexpectedToken { expected: String, found: String },

    #[error("unexpected end of input")]
    UnexpectedEof,

    #[error("unknown PromQL function '{name}'")]
    UnknownFunction { name: String },

    #[error("wrong argument count for '{func}': expected {expected}, got {got}")]
    WrongArgCount {
        func: String,
        expected: usize,
        got: usize,
    },

    #[error("type error in '{context}': {detail}")]
    TypeError { context: String, detail: String },

    #[error("invalid duration literal '{literal}'")]
    InvalidDuration { literal: String },

    #[error("invalid string literal: {detail}")]
    InvalidString { detail: String },

    #[error("label matcher error: {detail}")]
    LabelMatcher { detail: String },

    #[error("selector error: {detail}")]
    Selector { detail: String },

    #[error("invalid regex in label matcher on '{name}': {detail}")]
    InvalidRegex { name: String, detail: String },

    #[error("unknown remote-read label match type {match_type}")]
    UnknownMatchType { match_type: i32 },

    #[error(
        "vector selector must contain at least one matcher that does not match the empty string"
    )]
    UnselectiveSelector,

    #[error("query processing would load too many samples into memory (limit {limit})")]
    TooManySamples { limit: usize },

    #[error(
        "exceeded maximum resolution of {limit} points per timeseries; increase the query step"
    )]
    TooManyPoints { limit: usize },

    #[error("collection '{collection}' is not a metric collection: {detail}")]
    NotAMetricCollection { collection: String, detail: String },

    #[error("bad request parameter '{param}': {detail}")]
    BadParameter { param: String, detail: String },

    #[error("series of metric '{metric}' cannot be stored: {detail}")]
    UnwritableSeries { metric: String, detail: String },
}

impl PromqlError {
    /// Whether the error is the client's request, answered as `bad_data`.
    /// Every other variant is an evaluation error, answered as `execution`.
    pub fn is_bad_data(&self) -> bool {
        match self {
            Self::UnexpectedToken { .. }
            | Self::UnexpectedEof
            | Self::UnknownFunction { .. }
            | Self::WrongArgCount { .. }
            | Self::InvalidDuration { .. }
            | Self::InvalidString { .. }
            | Self::LabelMatcher { .. }
            | Self::Selector { .. }
            | Self::InvalidRegex { .. }
            | Self::UnknownMatchType { .. }
            | Self::UnselectiveSelector
            | Self::TooManyPoints { .. }
            | Self::BadParameter { .. }
            | Self::UnwritableSeries { .. } => true,
            Self::TypeError { .. }
            | Self::TooManySamples { .. }
            | Self::NotAMetricCollection { .. } => false,
        }
    }
}
