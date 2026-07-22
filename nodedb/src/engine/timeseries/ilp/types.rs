// SPDX-License-Identifier: BUSL-1.1

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;

/// A parsed ILP line with canonical, decoded names and values.
#[derive(Debug, PartialEq)]
pub struct IlpLine<'a> {
    /// Physical source-line number within the submitted batch (one-based).
    pub line_number: usize,
    /// The original physical line, excluding its newline terminator.
    pub raw: &'a str,
    /// Measurement name (the collection routing key).
    pub measurement: Cow<'a, str>,
    /// Canonical tag key/value pairs, sorted by decoded key.
    pub tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
    /// Canonical field key/value pairs in wire order.
    pub fields: Vec<(Cow<'a, str>, FieldValue<'a>)>,
    /// Timestamp in nanoseconds; absent means server-assigned.
    pub timestamp_ns: Option<i64>,
}

/// Field value types in ILP.
#[derive(Debug, PartialEq)]
pub enum FieldValue<'a> {
    Float(f64),
    Int(i64),
    UInt(u64),
    Str(Cow<'a, str>),
    Bool(bool),
}

/// A strictly parsed batch. No line is exposed if any non-comment line fails.
#[derive(Debug, PartialEq)]
pub struct ParsedIlpBatch<'a> {
    lines: Vec<IlpLine<'a>>,
}

impl<'a> ParsedIlpBatch<'a> {
    pub(crate) fn new(lines: Vec<IlpLine<'a>>) -> Self {
        Self { lines }
    }

    pub fn lines(&self) -> &[IlpLine<'a>] {
        &self.lines
    }

    pub fn into_lines(self) -> Vec<IlpLine<'a>> {
        self.lines
    }

    /// Groups lines by their decoded measurement without changing first-seen
    /// measurement order or the order of lines in each group.
    pub fn groups(&self) -> Vec<(&str, Vec<&IlpLine<'a>>)> {
        let mut groups: Vec<(&str, Vec<&IlpLine<'a>>)> = Vec::new();
        let mut group_indexes: HashMap<&str, usize> = HashMap::new();
        for line in &self.lines {
            let measurement = line.measurement.as_ref();
            if let Some(&index) = group_indexes.get(measurement) {
                groups[index].1.push(line);
            } else {
                group_indexes.insert(measurement, groups.len());
                groups.push((measurement, vec![line]));
            }
        }
        groups
    }
}

/// The precise reason a source line was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IlpErrorKind {
    EmptyLine,
    MissingMeasurement,
    MissingFields,
    InvalidEscape,
    InvalidQuote,
    InvalidTag,
    InvalidField,
    InvalidFieldValue,
    InvalidTimestamp,
    DuplicateTagKey,
    DuplicateFieldKey,
    TagFieldCollision,
    TrailingJunk,
}

/// Source-aware ILP parse error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IlpError {
    pub line_number: usize,
    pub raw: String,
    pub span: Range<usize>,
    pub kind: IlpErrorKind,
}

impl IlpError {
    pub(crate) fn new(
        line_number: usize,
        raw: &str,
        span: Range<usize>,
        kind: IlpErrorKind,
    ) -> Self {
        Self {
            line_number,
            raw: raw.to_owned(),
            span,
            kind,
        }
    }
}

impl std::fmt::Display for IlpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ILP line {} at bytes {}..{}: {:?}",
            self.line_number, self.span.start, self.span.end, self.kind
        )
    }
}

impl std::error::Error for IlpError {}
