// SPDX-License-Identifier: BUSL-1.1

//! Strict ILP grammar validation and source-aware line parsing.

use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Range;

use super::tokenizer::{self, ScanError};
use super::types::{FieldValue, IlpError, IlpErrorKind, IlpLine, ParsedIlpBatch};

struct ParsedMeasurementTags<'a> {
    measurement: Cow<'a, str>,
    tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
}

/// Parse one physical ILP line.
pub fn parse_line(line: &str) -> Result<IlpLine<'_>, IlpError> {
    parse_line_at(line, 1)
}

/// Parse a complete batch atomically. Blank lines and comment lines are
/// deliberately skipped; every other physical line either parses or rejects
/// the entire batch with its original line number and source text.
pub fn parse_batch(input: &str) -> Result<ParsedIlpBatch<'_>, IlpError> {
    let mut lines = Vec::new();
    for (index, raw) in input.lines().enumerate() {
        // `str::lines` strips `\n` but retains the preceding `\r`; both are
        // physical line terminators and neither belongs in source identity.
        let raw = raw.strip_suffix('\r').unwrap_or(raw);
        if raw.trim().is_empty() || raw.trim_start().starts_with('#') {
            continue;
        }
        lines.push(parse_line_at(raw, index + 1)?);
    }
    Ok(ParsedIlpBatch::new(lines))
}

fn parse_line_at(raw: &str, line_number: usize) -> Result<IlpLine<'_>, IlpError> {
    let leading = raw.len() - raw.trim_start().len();
    let line = raw.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(error(
            raw,
            line_number,
            0..raw.len(),
            IlpErrorKind::EmptyLine,
        ));
    }

    let tokens = split_spaces(line).map_err(|scan| scan_error(raw, line_number, leading, scan))?;
    if tokens.len() < 2 {
        return Err(error(
            raw,
            line_number,
            leading..raw.len(),
            IlpErrorKind::MissingFields,
        ));
    }
    if let Some((_, span)) = tokens.get(3) {
        return Err(error(
            raw,
            line_number,
            leading + span.start..leading + line.len(),
            IlpErrorKind::TrailingJunk,
        ));
    }

    let (measurement_token, measurement_span) = &tokens[0];
    let ParsedMeasurementTags { measurement, tags } = parse_measurement_tags(
        raw,
        line_number,
        leading,
        (measurement_token, measurement_span.clone()),
    )?;
    let (field_token, field_span) = &tokens[1];
    let fields = parse_fields(
        raw,
        line_number,
        leading + field_span.start,
        field_token,
        &tags,
    )?;
    let timestamp_ns = if let Some((timestamp, span)) = tokens.get(2) {
        timestamp.parse::<i64>().map(Some).map_err(|_| {
            error(
                raw,
                line_number,
                leading + span.start..leading + span.end,
                IlpErrorKind::InvalidTimestamp,
            )
        })?
    } else {
        None
    };

    Ok(IlpLine {
        line_number,
        raw,
        measurement,
        tags,
        fields,
        timestamp_ns,
    })
}

fn split_spaces(source: &str) -> Result<Vec<(&str, Range<usize>)>, ScanError> {
    Ok(tokenizer::split_line_tokens(source)?
        .into_iter()
        .filter(|(token, _)| !token.is_empty())
        .collect())
}

fn parse_measurement_tags<'a>(
    raw: &str,
    line_number: usize,
    base: usize,
    token: (&'a str, Range<usize>),
) -> Result<ParsedMeasurementTags<'a>, IlpError> {
    let parts = tokenizer::split_escaped(token.0, ',')
        .map_err(|scan| scan_error(raw, line_number, base + token.1.start, scan))?;
    let Some((measurement_raw, measurement_span)) = parts.first() else {
        return Err(error(
            raw,
            line_number,
            base..base,
            IlpErrorKind::MissingMeasurement,
        ));
    };
    let measurement = decode_name(
        raw,
        line_number,
        base + token.1.start,
        measurement_span.clone(),
        measurement_raw,
        false,
    )?;
    if measurement.is_empty() {
        return Err(error(
            raw,
            line_number,
            base + token.1.start..base + token.1.start + measurement_raw.len(),
            IlpErrorKind::MissingMeasurement,
        ));
    }

    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for (tag_raw, tag_span) in parts.into_iter().skip(1) {
        if tag_raw.is_empty() {
            return Err(error(
                raw,
                line_number,
                base + token.1.start + tag_span.start..base + token.1.start + tag_span.end,
                IlpErrorKind::InvalidTag,
            ));
        }
        let Some(eq) = tokenizer::find_unescaped_delimiter(tag_raw, '=').map_err(|scan| {
            scan_error(
                raw,
                line_number,
                base + token.1.start + tag_span.start,
                scan,
            )
        })?
        else {
            return Err(error(
                raw,
                line_number,
                base + token.1.start + tag_span.start..base + token.1.start + tag_span.end,
                IlpErrorKind::InvalidTag,
            ));
        };
        if tokenizer::find_unescaped_delimiter(&tag_raw[eq + 1..], '=')
            .map_err(|scan| {
                scan_error(
                    raw,
                    line_number,
                    base + token.1.start + tag_span.start + eq + 1,
                    scan,
                )
            })?
            .is_some()
        {
            return Err(error(
                raw,
                line_number,
                base + token.1.start + tag_span.start..base + token.1.start + tag_span.end,
                IlpErrorKind::InvalidTag,
            ));
        }
        let key = decode_name(
            raw,
            line_number,
            base + token.1.start + tag_span.start,
            0..eq,
            &tag_raw[..eq],
            true,
        )?;
        let value = decode_name(
            raw,
            line_number,
            base + token.1.start + tag_span.start,
            eq + 1..tag_raw.len(),
            &tag_raw[eq + 1..],
            true,
        )?;
        if key.is_empty() || value.is_empty() {
            return Err(error(
                raw,
                line_number,
                base + token.1.start + tag_span.start..base + token.1.start + tag_span.end,
                IlpErrorKind::InvalidTag,
            ));
        }
        if !seen.insert(key.to_string()) {
            return Err(error(
                raw,
                line_number,
                base + token.1.start + tag_span.start..base + token.1.start + tag_span.end,
                IlpErrorKind::DuplicateTagKey,
            ));
        }
        tags.push((key, value));
    }
    tags.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(ParsedMeasurementTags { measurement, tags })
}

fn parse_fields<'a>(
    raw: &str,
    line_number: usize,
    base: usize,
    token: &'a str,
    tags: &[(Cow<'a, str>, Cow<'a, str>)],
) -> Result<Vec<(Cow<'a, str>, FieldValue<'a>)>, IlpError> {
    let parts = tokenizer::split_field_delimited(token, ',')
        .map_err(|scan| scan_error(raw, line_number, base, scan))?;
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    for (field_raw, span) in parts {
        let Some(eq) = tokenizer::find_unescaped_delimiter(field_raw, '=')
            .map_err(|scan| scan_error(raw, line_number, base + span.start, scan))?
        else {
            return Err(error(
                raw,
                line_number,
                base + span.start..base + span.end,
                IlpErrorKind::InvalidField,
            ));
        };
        let key = decode_name(
            raw,
            line_number,
            base + span.start,
            0..eq,
            &field_raw[..eq],
            true,
        )?;
        if key.is_empty() {
            return Err(error(
                raw,
                line_number,
                base + span.start..base + span.end,
                IlpErrorKind::InvalidField,
            ));
        }
        if !seen.insert(key.to_string()) {
            return Err(error(
                raw,
                line_number,
                base + span.start..base + span.end,
                IlpErrorKind::DuplicateFieldKey,
            ));
        }
        if tags.iter().any(|(tag, _)| tag == &key) {
            return Err(error(
                raw,
                line_number,
                base + span.start..base + span.end,
                IlpErrorKind::TagFieldCollision,
            ));
        }
        let value = parse_field_value(
            raw,
            line_number,
            base + span.start + eq + 1,
            &field_raw[eq + 1..],
        )?;
        fields.push((key, value));
    }
    if fields.is_empty() {
        return Err(error(
            raw,
            line_number,
            base..base + token.len(),
            IlpErrorKind::MissingFields,
        ));
    }
    Ok(fields)
}

fn parse_field_value<'a>(
    raw: &str,
    line_number: usize,
    base: usize,
    value: &'a str,
) -> Result<FieldValue<'a>, IlpError> {
    if value.is_empty() {
        return Err(error(
            raw,
            line_number,
            base..base,
            IlpErrorKind::InvalidFieldValue,
        ));
    }
    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') {
            return Err(error(
                raw,
                line_number,
                base..base + value.len(),
                IlpErrorKind::InvalidFieldValue,
            ));
        }
        let inner = &value[1..value.len() - 1];
        let decoded = tokenizer::decode_string(inner)
            .map_err(|scan| scan_error(raw, line_number, base + 1, scan))?;
        return Ok(FieldValue::Str(decoded));
    }
    if value.contains('"') || value.contains('\\') {
        return Err(error(
            raw,
            line_number,
            base..base + value.len(),
            IlpErrorKind::InvalidFieldValue,
        ));
    }
    match value {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }
    if let Some(number) = value.strip_suffix('i') {
        return number.parse::<i64>().map(FieldValue::Int).map_err(|_| {
            error(
                raw,
                line_number,
                base..base + value.len(),
                IlpErrorKind::InvalidFieldValue,
            )
        });
    }
    if let Some(number) = value.strip_suffix('u') {
        return number.parse::<u64>().map(FieldValue::UInt).map_err(|_| {
            error(
                raw,
                line_number,
                base..base + value.len(),
                IlpErrorKind::InvalidFieldValue,
            )
        });
    }
    let number = value.parse::<f64>().map_err(|_| {
        error(
            raw,
            line_number,
            base..base + value.len(),
            IlpErrorKind::InvalidFieldValue,
        )
    })?;
    if !number.is_finite() {
        return Err(error(
            raw,
            line_number,
            base..base + value.len(),
            IlpErrorKind::InvalidFieldValue,
        ));
    }
    Ok(FieldValue::Float(number))
}

fn decode_name<'a>(
    raw: &str,
    line_number: usize,
    base: usize,
    span: Range<usize>,
    token: &'a str,
    permit_equals: bool,
) -> Result<Cow<'a, str>, IlpError> {
    tokenizer::decode_name(token, permit_equals)
        .map_err(|scan| scan_error(raw, line_number, base + span.start, scan))
}

fn scan_error(raw: &str, line_number: usize, base: usize, scan: ScanError) -> IlpError {
    let (offset, kind) = match scan {
        ScanError::DanglingEscape(offset) => (offset, IlpErrorKind::InvalidEscape),
        ScanError::InvalidQuote(offset) => (offset, IlpErrorKind::InvalidQuote),
    };
    let start = base.saturating_add(offset).min(raw.len());
    let end = start.saturating_add(1).min(raw.len());
    error(raw, line_number, start..end, kind)
}

fn error(raw: &str, line_number: usize, span: Range<usize>, kind: IlpErrorKind) -> IlpError {
    IlpError::new(line_number, raw, span, kind)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn decodes_protocol_escapes_and_sorts_decoded_tags() {
        let line =
            parse_line(r"cpu\ load,host\ name=west\,1,zone=a\=b value=1i").expect("valid ILP");
        assert_eq!(line.measurement, "cpu load");
        assert_eq!(line.tags[0].0, "host name");
        assert_eq!(line.tags[0].1, "west,1");
        assert_eq!(line.tags[1].1, "a=b");
    }

    #[test]
    fn quoted_strings_keep_commas_and_spaces() {
        let line = parse_line(r#"log message="hello, spaced \"world\" \\ ok",n=1i 9"#)
            .expect("valid string");
        assert_eq!(
            line.fields[0].1,
            FieldValue::Str(Cow::Owned("hello, spaced \"world\" \\ ok".into()))
        );
    }

    #[test]
    fn quotes_are_literal_in_identifiers_but_strict_in_string_values() {
        let line = parse_line(r#"cpu",host"=west field"=1i"#)
            .expect("quotes are valid identifier characters");
        assert_eq!(line.measurement, "cpu\"");
        assert_eq!(
            line.tags[0],
            (Cow::Borrowed("host\""), Cow::Borrowed("west"))
        );
        assert_eq!(line.fields[0].0, "field\"");

        assert_eq!(
            parse_line("m f=\"unterminated")
                .expect_err("unterminated string")
                .kind,
            IlpErrorKind::InvalidQuote
        );
        assert_eq!(
            parse_line("m f=\"a\"\"b\"")
                .expect_err("unescaped quote")
                .kind,
            IlpErrorKind::InvalidQuote
        );
    }

    #[test]
    fn numeric_forms_are_strict_and_finite() {
        assert!(parse_line("m f=-9223372036854775808i").is_ok());
        assert!(parse_line("m f=18446744073709551615u").is_ok());
        for value in ["NaN", "inf", "1e999", "1x", "-1u"] {
            assert_eq!(
                parse_line(&format!("m f={value}"))
                    .expect_err("invalid")
                    .kind,
                IlpErrorKind::InvalidFieldValue
            );
        }
        assert_eq!(
            parse_line("m f=1 1x").expect_err("invalid timestamp").kind,
            IlpErrorKind::InvalidTimestamp
        );
    }

    #[test]
    fn rejects_duplicates_and_tag_field_collisions() {
        assert_eq!(
            parse_line("m,a=1,a=2 f=1i")
                .expect_err("duplicate tag")
                .kind,
            IlpErrorKind::DuplicateTagKey
        );
        assert_eq!(
            parse_line("m a=1i,a=2i").expect_err("duplicate field").kind,
            IlpErrorKind::DuplicateFieldKey
        );
        assert_eq!(
            parse_line("m,a=1 a=2i").expect_err("collision").kind,
            IlpErrorKind::TagFieldCollision
        );
    }

    #[test]
    fn batch_is_atomic_and_preserves_physical_source_identity() {
        let error = parse_batch("# ignored\nm f=1i\n\nm f=bad\nm f=3i").expect_err("invalid batch");
        assert_eq!(error.line_number, 4);
        assert_eq!(error.raw, "m f=bad");
        assert!(error.span.start < error.span.end);

        let batch = parse_batch("# ignored\nm f=1i\n\nm f=3i").expect("valid batch");
        assert_eq!(batch.lines()[0].line_number, 2);
        assert_eq!(batch.lines()[1].line_number, 4);
        assert_eq!(batch.lines()[1].raw, "m f=3i");

        let crlf_batch = parse_batch("m f=1i\r\n").expect("valid CRLF batch");
        assert_eq!(crlf_batch.lines()[0].raw, "m f=1i");
    }

    #[test]
    fn borrowed_and_owned_cow_values_are_preserved() {
        let borrowed = parse_line("m,host=a f=\"value\"").expect("valid ILP");
        assert!(matches!(borrowed.measurement, Cow::Borrowed(_)));
        assert!(matches!(borrowed.tags[0].0, Cow::Borrowed(_)));
        assert!(matches!(
            borrowed.fields[0].1,
            FieldValue::Str(Cow::Borrowed(_))
        ));
        let owned = parse_line(r#"m\,x f="v\\""#).expect("valid ILP");
        assert!(matches!(owned.measurement, Cow::Owned(_)));
        assert!(matches!(owned.fields[0].1, FieldValue::Str(Cow::Owned(_))));
    }

    #[test]
    fn grouping_uses_canonical_measurements_stably() {
        let batch = parse_batch("cpu\\ load f=1i\ncpu\\ load f=2i\nmem f=3i").expect("valid batch");
        let groups = batch.groups();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "cpu load");
        assert_eq!(groups[0].1.len(), 2);
        assert_eq!(groups[1].0, "mem");
    }

    #[test]
    fn rejects_dangling_escapes_and_trailing_tokens() {
        assert_eq!(
            parse_line("m f=1i\\").expect_err("dangling escape").kind,
            IlpErrorKind::InvalidEscape
        );
        assert_eq!(
            parse_line("m f=1i 1 more")
                .expect_err("trailing token")
                .kind,
            IlpErrorKind::TrailingJunk
        );
    }
}
