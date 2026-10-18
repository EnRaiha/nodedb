// SPDX-License-Identifier: BUSL-1.1

//! Record splitting for `COPY ... FROM STDIN` input, in CSV or PostgreSQL
//! text format.
//!
//! The reader borrows the input and yields one record at a time, so a load
//! holds the input and one record, never a parsed copy of every row. A field
//! is `None` for SQL NULL. A field borrows the input unless it has quotes or
//! escapes to decode.

use std::borrow::Cow;

use nodedb_sql::ddl_ast::statement::CopyStdinFormat;

use super::decode::RowError;

/// One record's fields. `None` is SQL NULL.
pub(super) type Fields<'a> = Vec<Option<Cow<'a, str>>>;

/// Splits COPY input into records.
pub(super) struct RecordReader<'a> {
    src: &'a str,
    pos: usize,
    format: CopyStdinFormat,
    delimiter: u8,
    /// The 1-based number of the last record read, header included.
    record_no: usize,
    /// Whether the reader met the `\.` end-of-data line.
    ended: bool,
}

/// Refuse a delimiter that is not one ASCII character other than a quote, a
/// backslash or a line break.
pub(super) fn check_delimiter(delimiter: char) -> Result<(), RowError> {
    if delimiter.is_ascii() && !matches!(delimiter, '"' | '\\' | '\n' | '\r') {
        return Ok(());
    }
    Err(RowError::new(
        0,
        "22023",
        format!(
            "COPY delimiter must be one ASCII character other than a quote, \
             a backslash or a line break — found {delimiter:?}"
        ),
    ))
}

/// Where the scan of a CSV stream sits within a record.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CsvState {
    /// At the start of a field, where a quote opens a quoted field.
    FieldStart,
    /// Inside an unquoted field, where a quote is data.
    Unquoted,
    /// Inside a quoted field, where a line break is data.
    Quoted,
    /// Just past a quote inside a quoted field: it closed the field, or a
    /// second quote follows and the pair is one quote of data.
    AfterQuote,
}

/// Where whole records end in a stream of COPY input. A text-format record
/// ends at every line break. A CSV record ends at a line break outside a
/// quoted field, and a quote opens a quoted field only at the start of a
/// field, as [`RecordReader`] reads it. The scan carries its state from one
/// slice of the stream to the next.
pub(super) struct RecordBoundary {
    csv: bool,
    delimiter: u8,
    state: CsvState,
}

impl RecordBoundary {
    /// A boundary scan for `format`. The delimiter passed [`check_delimiter`].
    pub(super) fn new(format: CopyStdinFormat, delimiter: char) -> Self {
        Self {
            csv: matches!(format, CopyStdinFormat::Csv),
            // An ASCII char is one byte.
            delimiter: delimiter as u8,
            state: CsvState::FieldStart,
        }
    }

    /// Scan `buf` from the current state. Returns the offset just past the
    /// first record end when `first` is set, else past the last one. `None`
    /// when `buf` ends no record. The state moves to the end of the bytes
    /// scanned: the returned offset when `first` finds an end, else the end
    /// of `buf`.
    pub(super) fn scan(&mut self, buf: &[u8], first: bool) -> Option<usize> {
        let mut found = None;
        for (i, &b) in buf.iter().enumerate() {
            if !self.csv {
                if b == b'\n' {
                    found = Some(i + 1);
                    if first {
                        return found;
                    }
                }
                continue;
            }
            let ends = self.step(b);
            if ends {
                found = Some(i + 1);
                if first {
                    return found;
                }
            }
        }
        found
    }

    /// Move the CSV state past `b`. True when `b` ends a record.
    fn step(&mut self, b: u8) -> bool {
        let (next, ends) = match self.state {
            CsvState::FieldStart | CsvState::Unquoted => match b {
                b'\n' => (CsvState::FieldStart, true),
                b'"' if self.state == CsvState::FieldStart => (CsvState::Quoted, false),
                b if b == self.delimiter => (CsvState::FieldStart, false),
                _ => (CsvState::Unquoted, false),
            },
            CsvState::Quoted => match b {
                b'"' => (CsvState::AfterQuote, false),
                _ => (CsvState::Quoted, false),
            },
            CsvState::AfterQuote => match b {
                b'"' => (CsvState::Quoted, false),
                b'\n' => (CsvState::FieldStart, true),
                b if b == self.delimiter => (CsvState::FieldStart, false),
                // A `\r` before the line break, or a stray byte the reader
                // refuses.
                _ => (CsvState::AfterQuote, false),
            },
        };
        self.state = next;
        ends
    }
}

impl<'a> RecordReader<'a> {
    /// A reader over `src`, a run of whole records whose first record is
    /// number `first_record_no`. The delimiter passed [`check_delimiter`].
    pub(super) fn new(
        src: &'a str,
        format: CopyStdinFormat,
        delimiter: char,
        first_record_no: usize,
    ) -> Result<Self, RowError> {
        check_delimiter(delimiter)?;
        Ok(Self {
            src,
            pos: 0,
            format,
            // An ASCII char is one byte.
            delimiter: delimiter as u8,
            record_no: first_record_no.saturating_sub(1),
            ended: false,
        })
    }

    /// Whether the reader stopped at the `\.` end-of-data line.
    pub(super) fn at_end_marker(&self) -> bool {
        self.ended
    }

    /// The 1-based number of the last record read, header included.
    pub(super) fn record_no(&self) -> usize {
        self.record_no
    }

    /// Read the next record into `out`. `Ok(false)` at the end of the input
    /// or at the `\.` end-of-data line. An empty line is skipped.
    pub(super) fn next_into(&mut self, out: &mut Fields<'a>) -> Result<bool, RowError> {
        loop {
            if self.pos >= self.src.len() {
                return Ok(false);
            }
            let line_end = self.src[self.pos..]
                .find('\n')
                .map_or(self.src.len(), |i| self.pos + i);
            let line = self.src[self.pos..line_end].trim_end_matches('\r');
            if line == "\\." {
                self.pos = self.src.len();
                self.ended = true;
                return Ok(false);
            }
            self.record_no += 1;
            if line.is_empty() {
                self.pos = (line_end + 1).min(self.src.len());
                continue;
            }
            out.clear();
            match self.format {
                CopyStdinFormat::Csv => self.read_csv(out)?,
                CopyStdinFormat::Text => self.read_text(out, line_end)?,
            }
            return Ok(true);
        }
    }

    /// Read one CSV record from `pos`. A quoted field can span lines.
    fn read_csv(&mut self, out: &mut Fields<'a>) -> Result<(), RowError> {
        let bytes = self.src.as_bytes();
        loop {
            if bytes.get(self.pos) == Some(&b'"') {
                out.push(Some(self.read_quoted()?));
            } else {
                let start = self.pos;
                while let Some(&b) = bytes.get(self.pos) {
                    if b == self.delimiter || b == b'\n' {
                        break;
                    }
                    self.pos += 1;
                }
                let raw = self.src[start..self.pos].trim_end_matches('\r');
                // An unquoted empty field is NULL. A quoted one is "".
                out.push((!raw.is_empty()).then_some(Cow::Borrowed(raw)));
            }
            match bytes.get(self.pos) {
                Some(&b) if b == self.delimiter => self.pos += 1,
                Some(b'\n') => {
                    self.pos += 1;
                    return Ok(());
                }
                Some(b'\r') if bytes.get(self.pos + 1) == Some(&b'\n') => {
                    self.pos += 2;
                    return Ok(());
                }
                None => return Ok(()),
                Some(_) => {
                    return Err(self.error("22P02", "unexpected character after a closing quote"));
                }
            }
        }
    }

    /// Read one `"..."` field from `pos`, which holds the opening quote.
    fn read_quoted(&mut self) -> Result<Cow<'a, str>, RowError> {
        let bytes = self.src.as_bytes();
        let start = self.pos + 1;
        let mut i = start;
        let mut doubled = false;
        loop {
            match bytes.get(i) {
                Some(b'"') if bytes.get(i + 1) == Some(&b'"') => {
                    doubled = true;
                    i += 2;
                }
                Some(b'"') => break,
                Some(_) => i += 1,
                None => return Err(self.error("22P02", "unterminated CSV quoted field")),
            }
        }
        let inner = &self.src[start..i];
        self.pos = i + 1;
        Ok(if doubled {
            Cow::Owned(inner.replace("\"\"", "\""))
        } else {
            Cow::Borrowed(inner)
        })
    }

    /// Read one text-format record: the line from `pos` to `line_end`.
    fn read_text(&mut self, out: &mut Fields<'a>, line_end: usize) -> Result<(), RowError> {
        let bytes = self.src.as_bytes();
        let line_stop = if line_end > self.pos && bytes[line_end - 1] == b'\r' {
            line_end - 1
        } else {
            line_end
        };
        let mut start = self.pos;
        let mut i = self.pos;
        let mut escaped = false;
        loop {
            let at_end = i >= line_stop;
            if at_end || bytes[i] == self.delimiter {
                // A trailing backslash steps past the line. The field ends at it.
                let raw = &self.src[start..i.min(line_stop)];
                out.push(self.text_field(raw, escaped)?);
                if at_end {
                    break;
                }
                i += 1;
                start = i;
                escaped = false;
                continue;
            }
            if bytes[i] == b'\\' {
                escaped = true;
                // The escaped character is data, even a delimiter.
                i += 2;
                continue;
            }
            i += 1;
        }
        self.pos = (line_end + 1).min(self.src.len());
        Ok(())
    }

    /// Decode one text-format field. `\N` is NULL.
    fn text_field(&self, raw: &'a str, escaped: bool) -> Result<Option<Cow<'a, str>>, RowError> {
        if raw == "\\N" {
            return Ok(None);
        }
        if !escaped {
            return Ok(Some(Cow::Borrowed(raw)));
        }
        let mut out: Vec<u8> = Vec::with_capacity(raw.len());
        let b = raw.as_bytes();
        let mut i = 0;
        while i < b.len() {
            if b[i] != b'\\' || i + 1 >= b.len() {
                out.push(b[i]);
                i += 1;
                continue;
            }
            let c = b[i + 1];
            i += 2;
            match c {
                b'b' => out.push(0x08),
                b'f' => out.push(0x0C),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'v' => out.push(0x0B),
                b'0'..=b'7' => {
                    let mut value = u32::from(c - b'0');
                    let mut digits = 1;
                    while digits < 3 && i < b.len() && (b'0'..=b'7').contains(&b[i]) {
                        value = value * 8 + u32::from(b[i] - b'0');
                        i += 1;
                        digits += 1;
                    }
                    // Three octal digits reach 511. PostgreSQL keeps the low byte.
                    out.push((value & 0xFF) as u8);
                }
                b'x' if i < b.len() && b[i].is_ascii_hexdigit() => {
                    let mut value = hex_value(b[i]);
                    i += 1;
                    if i < b.len() && b[i].is_ascii_hexdigit() {
                        value = value * 16 + hex_value(b[i]);
                        i += 1;
                    }
                    out.push(value);
                }
                other => out.push(other),
            }
        }
        String::from_utf8(out)
            .map(|s| Some(Cow::Owned(s)))
            .map_err(|_| self.error("22021", "an escape decodes to invalid UTF-8"))
    }

    fn error(&self, sqlstate: &'static str, message: &str) -> RowError {
        RowError::new(self.record_no, sqlstate, message.to_string())
    }
}

fn hex_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(src: &str, format: CopyStdinFormat, delimiter: char) -> Vec<Vec<Option<String>>> {
        let mut reader = RecordReader::new(src, format, delimiter, 1).expect("reader");
        let mut out = Vec::new();
        let mut fields = Fields::new();
        while reader.next_into(&mut fields).expect("record") {
            out.push(
                fields
                    .iter()
                    .map(|f| f.as_ref().map(|s| s.to_string()))
                    .collect(),
            );
        }
        out
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    #[test]
    fn csv_splits_quotes_and_nulls() {
        let got = records(
            "1,\"a,b\",\n2,\"say \"\"hi\"\"\",\"\"\r\n",
            CopyStdinFormat::Csv,
            ',',
        );
        assert_eq!(
            got,
            vec![
                vec![s("1"), s("a,b"), None],
                vec![s("2"), s("say \"hi\""), s("")],
            ]
        );
    }

    #[test]
    fn csv_quoted_field_spans_lines() {
        let got = records("1,\"x\ny\"\n2,z", CopyStdinFormat::Csv, ',');
        assert_eq!(got, vec![vec![s("1"), s("x\ny")], vec![s("2"), s("z")]]);
    }

    #[test]
    fn csv_unterminated_quote_is_refused() {
        let mut reader = RecordReader::new("1,\"x", CopyStdinFormat::Csv, ',', 1).expect("reader");
        let mut fields = Fields::new();
        let err = reader.next_into(&mut fields).expect_err("unterminated");
        assert_eq!(err.sqlstate, "22P02");
    }

    #[test]
    fn text_decodes_escapes_and_null_marker() {
        let got = records(
            "1\t\\N\ta\\tb\n2\t\\\\\t\\x41\\101\n\\.\n9\t9\t9",
            CopyStdinFormat::Text,
            '\t',
        );
        assert_eq!(
            got,
            vec![
                vec![s("1"), None, s("a\tb")],
                vec![s("2"), s("\\"), s("AA")],
            ]
        );
    }

    #[test]
    fn empty_lines_are_skipped_and_counted() {
        let mut reader =
            RecordReader::new("a\n\nb\n", CopyStdinFormat::Text, '\t', 1).expect("reader");
        let mut fields = Fields::new();
        assert!(reader.next_into(&mut fields).expect("first"));
        assert!(reader.next_into(&mut fields).expect("second"));
        assert_eq!(reader.record_no(), 3);
        assert_eq!(fields, vec![Some(Cow::Borrowed("b"))]);
        assert!(!reader.next_into(&mut fields).expect("end"));
    }

    #[test]
    fn a_quote_delimiter_is_refused() {
        assert!(RecordReader::new("", CopyStdinFormat::Csv, '"', 1).is_err());
    }

    #[test]
    fn record_numbers_continue_from_the_first_record_number() {
        let mut reader =
            RecordReader::new("a\nb\n", CopyStdinFormat::Text, '\t', 41).expect("reader");
        let mut fields = Fields::new();
        assert!(reader.next_into(&mut fields).expect("first"));
        assert_eq!(reader.record_no(), 41);
        assert!(reader.next_into(&mut fields).expect("second"));
        assert_eq!(reader.record_no(), 42);
    }

    #[test]
    fn the_end_marker_is_reported() {
        let mut reader =
            RecordReader::new("a\n\\.\nb\n", CopyStdinFormat::Text, '\t', 1).expect("reader");
        let mut fields = Fields::new();
        assert!(reader.next_into(&mut fields).expect("first"));
        assert!(!reader.next_into(&mut fields).expect("end"));
        assert!(reader.at_end_marker());
    }

    /// Every record end of `src`, scanned in slices of `step` bytes.
    fn ends(src: &str, format: CopyStdinFormat, step: usize) -> Vec<usize> {
        let mut boundary = RecordBoundary::new(format, ',');
        let bytes = src.as_bytes();
        let mut out = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let stop = (at + step).min(bytes.len());
            let mut from = at;
            while let Some(end) = boundary.scan(&bytes[from..stop], true) {
                out.push(from + end);
                from += end;
            }
            at = stop;
        }
        out
    }

    #[test]
    fn csv_boundaries_skip_line_breaks_in_quoted_fields() {
        let src = "1,\"a\nb\"\n2,\"say \"\"hi\"\"\"\n3,x\"y\n";
        let whole = ends(src, CopyStdinFormat::Csv, src.len());
        assert_eq!(whole, vec![8, 23, 29]);
        for step in 1..src.len() {
            assert_eq!(ends(src, CopyStdinFormat::Csv, step), whole, "step {step}");
        }
    }

    #[test]
    fn text_boundaries_are_line_breaks() {
        assert_eq!(ends("a\"\nb\n", CopyStdinFormat::Text, 2), vec![3, 5]);
    }

    #[test]
    fn a_last_scan_reports_the_last_record_end() {
        let mut boundary = RecordBoundary::new(CopyStdinFormat::Csv, ',');
        assert_eq!(boundary.scan(b"1,a\n2,b\n3,\"c", false), Some(8));
        assert_eq!(boundary.scan(b"\nd\"\n", false), Some(4));
    }
}
