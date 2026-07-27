// SPDX-License-Identifier: BUSL-1.1

//! Format template engine for formatted sequences.
//!
//! Parses templates like `'INV-{YY}-{MM}-{SEQ:05}'` into a token list and
//! resolves tokens at runtime to produce formatted sequence values.

use std::collections::HashMap;

/// A single token in a format template.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum FormatToken {
    /// Literal text (e.g. `"INV-"`).
    Literal(String),
    /// Sequence counter, optionally zero-padded to `padding` digits.
    Seq { padding: u8 },
    /// 4-digit year (e.g. `2026`).
    Year4,
    /// 2-digit year (e.g. `26`).
    Year2,
    /// 2-digit month (e.g. `04`).
    Month,
    /// 2-digit day (e.g. `02`).
    Day,
    /// Quarter digit (1–4).
    Quarter,
    /// ISO week number (01–53).
    IsoWeek,
    /// Tenant short code from session context.
    Tenant,
    /// Custom session variable.
    Custom(String),
}

/// Parse a format template string into tokens.
///
/// Template syntax: literal text with `{TOKEN}` placeholders.
/// - `{SEQ}` or `{SEQ:N}` — counter with optional zero-padding
/// - `{YYYY}` — 4-digit year
/// - `{YY}` — 2-digit year
/// - `{MM}` — 2-digit month
/// - `{DD}` — 2-digit day
/// - `{Q}` — quarter (1–4)
/// - `{WW}` — ISO week (01–53)
/// - `{TENANT}` — tenant short code
/// - `{CUSTOM:key}` — session variable
///
/// Validation: exactly one `{SEQ}` token required.
pub fn parse_format_template(
    template: &str,
) -> Result<Vec<FormatToken>, super::types::SequenceError> {
    use super::types::SequenceError;

    let fmt_err = |detail: String| SequenceError::FormatParse { detail };

    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut chars = template.chars().peekable();
    let mut seq_count = 0;

    while let Some(ch) = chars.next() {
        if ch == '{' {
            // Flush pending literal.
            if !literal.is_empty() {
                tokens.push(FormatToken::Literal(std::mem::take(&mut literal)));
            }

            // Collect token name until '}'.
            let mut token_name = String::new();
            let mut found_close = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    found_close = true;
                    break;
                }
                token_name.push(inner);
            }
            if !found_close {
                return Err(fmt_err(format!(
                    "unclosed '{{' in format template: missing '}}' after '{token_name}'"
                )));
            }

            let token = if token_name.eq_ignore_ascii_case("SEQ") {
                seq_count += 1;
                FormatToken::Seq { padding: 0 }
            } else if let Some(rest) =
                nodedb_types::strip_prefix_ascii_case_insensitive(&token_name, "SEQ:")
            {
                seq_count += 1;
                let padding: u8 = rest
                    .parse()
                    .map_err(|_| fmt_err(format!("invalid SEQ padding width: '{rest}'")))?;
                FormatToken::Seq { padding }
            } else if token_name.eq_ignore_ascii_case("YYYY") {
                FormatToken::Year4
            } else if token_name.eq_ignore_ascii_case("YY") {
                FormatToken::Year2
            } else if token_name.eq_ignore_ascii_case("MM") {
                FormatToken::Month
            } else if token_name.eq_ignore_ascii_case("DD") {
                FormatToken::Day
            } else if token_name.eq_ignore_ascii_case("Q") {
                FormatToken::Quarter
            } else if token_name.eq_ignore_ascii_case("WW") {
                FormatToken::IsoWeek
            } else if token_name.eq_ignore_ascii_case("TENANT") {
                FormatToken::Tenant
            } else if let Some(key) =
                nodedb_types::strip_prefix_ascii_case_insensitive(&token_name, "CUSTOM:")
            {
                if key.is_empty() {
                    return Err(fmt_err("CUSTOM token requires a key: {CUSTOM:key}".into()));
                }
                FormatToken::Custom(key.to_string())
            } else {
                return Err(fmt_err(format!("unknown format token: '{{{token_name}}}'")));
            };
            tokens.push(token);
        } else {
            literal.push(ch);
        }
    }

    // Flush trailing literal.
    if !literal.is_empty() {
        tokens.push(FormatToken::Literal(literal));
    }

    if seq_count == 0 {
        return Err(fmt_err(
            "format template must contain exactly one {SEQ} token".into(),
        ));
    }
    if seq_count > 1 {
        return Err(fmt_err(format!(
            "format template must contain exactly one {{SEQ}} token, found {seq_count}"
        )));
    }

    Ok(tokens)
}

/// Context for resolving format tokens at runtime.
pub struct FormatContext<'a> {
    /// Current counter value (from nextval).
    pub counter: i64,
    /// Current date/time components.
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub quarter: u8,
    pub iso_week: u8,
    /// Tenant short code (from session).
    pub tenant: &'a str,
    /// Custom session variables.
    pub session_vars: &'a HashMap<String, String>,
}

impl<'a> FormatContext<'a> {
    /// Create a context from the current UTC time.
    pub fn now(counter: i64, tenant: &'a str, session_vars: &'a HashMap<String, String>) -> Self {
        let dt = nodedb_types::NdbDateTime::now();
        let c = dt.components();
        Self {
            counter,
            year: c.year as u16,
            month: c.month,
            day: c.day,
            quarter: ((c.month - 1) / 3) + 1,
            iso_week: iso_week_number(c.year, c.month, c.day),
            tenant,
            session_vars,
        }
    }
}

/// Compute ISO 8601 week number (1–53).
///
/// Week 01 is the week containing the year's first Thursday.
/// Days before Week 01 belong to the last week of the previous year;
/// days after the last week belong to Week 01 of the next year.
fn iso_week_number(year: i32, month: u8, day: u8) -> u8 {
    let doy = day_of_year(year, month, day) as i32;
    let dow = day_of_week_iso(year, month, day) as i32; // 1=Mon..7=Sun

    // ISO week: (ordinal - weekday + 10) / 7
    let w = (doy - dow + 10) / 7;

    if w < 1 {
        // Belongs to the last week of the previous year.
        iso_weeks_in_year(year - 1)
    } else if w > iso_weeks_in_year(year) as i32 {
        // Belongs to week 1 of the next year.
        1
    } else {
        w as u8
    }
}

/// Day of year (1-based ordinal).
fn day_of_year(year: i32, month: u8, day: u8) -> u16 {
    let days_in_months: [u16; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let mut doy: u16 = day as u16;
    for (m, &dim) in days_in_months.iter().enumerate().take(month as usize - 1) {
        doy += dim;
        if m == 1 && is_leap {
            doy += 1;
        }
    }
    doy
}

/// ISO day of week: 1=Monday .. 7=Sunday (Tomohiko Sakamoto's algorithm).
fn day_of_week_iso(year: i32, month: u8, day: u8) -> u8 {
    let t = [0i32, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let mut y = year;
    if month < 3 {
        y -= 1;
    }
    let raw = ((y + y / 4 - y / 100 + y / 400 + t[(month - 1) as usize] + day as i32) % 7 + 7) % 7;
    // Sakamoto: 0=Sun, 1=Mon..6=Sat → ISO: Sun=7, Mon=1..Sat=6
    if raw == 0 { 7 } else { raw as u8 }
}

/// Number of ISO weeks in a year (52 or 53).
///
/// A year has 53 weeks iff Jan 1 is Thursday, or the year is a leap year
/// and Jan 1 is Wednesday.
fn iso_weeks_in_year(year: i32) -> u8 {
    let jan1_dow = day_of_week_iso(year, 1, 1);
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if jan1_dow == 4 || (is_leap && jan1_dow == 3) {
        53
    } else {
        52
    }
}

/// Resolve a format template to a string using the given context.
pub fn format_sequence_value(tokens: &[FormatToken], ctx: &FormatContext<'_>) -> String {
    let mut result = String::with_capacity(32);

    for token in tokens {
        match token {
            FormatToken::Literal(s) => result.push_str(s),
            FormatToken::Seq { padding } => {
                let pad = *padding as usize;
                if pad > 0 {
                    result.push_str(&format!("{:0>width$}", ctx.counter, width = pad));
                } else {
                    result.push_str(&ctx.counter.to_string());
                }
            }
            FormatToken::Year4 => result.push_str(&format!("{:04}", ctx.year)),
            FormatToken::Year2 => result.push_str(&format!("{:02}", ctx.year % 100)),
            FormatToken::Month => result.push_str(&format!("{:02}", ctx.month)),
            FormatToken::Day => result.push_str(&format!("{:02}", ctx.day)),
            FormatToken::Quarter => result.push_str(&ctx.quarter.to_string()),
            FormatToken::IsoWeek => result.push_str(&format!("{:02}", ctx.iso_week)),
            FormatToken::Tenant => result.push_str(ctx.tenant),
            FormatToken::Custom(key) => {
                let val = ctx.session_vars.get(key).map(|s| s.as_str()).unwrap_or("");
                result.push_str(val);
            }
        }
    }

    result
}

/// Compute the period key for the given reset scope and date.
///
/// Returns the period key string that identifies the current period.
/// When the period key changes, the sequence counter resets.
pub fn compute_period_key(scope: &ResetScope, year: u16, month: u8, day: u8) -> String {
    match scope {
        ResetScope::Never => String::new(),
        ResetScope::Yearly => format!("{year:04}"),
        ResetScope::Monthly => format!("{year:04}-{month:02}"),
        ResetScope::Quarterly => {
            let q = ((month - 1) / 3) + 1;
            format!("{year:04}-Q{q}")
        }
        ResetScope::Daily => format!("{year:04}-{month:02}-{day:02}"),
    }
}

/// Reset scope — when the counter should auto-reset to START.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ResetScope {
    /// Never reset (default).
    #[default]
    Never = 0,
    /// Reset at the start of each calendar year.
    Yearly = 1,
    /// Reset at the start of each calendar month.
    Monthly = 2,
    /// Reset at the start of each calendar quarter.
    Quarterly = 3,
    /// Reset at the start of each day.
    Daily = 4,
}

impl ResetScope {
    /// Parse from a SQL keyword.
    pub fn parse(s: &str) -> Result<Self, super::types::SequenceError> {
        match s.to_uppercase().as_str() {
            "NEVER" => Ok(Self::Never),
            "YEARLY" | "ANNUAL" => Ok(Self::Yearly),
            "MONTHLY" => Ok(Self::Monthly),
            "QUARTERLY" => Ok(Self::Quarterly),
            "DAILY" => Ok(Self::Daily),
            other => Err(super::types::SequenceError::InvalidResetScope {
                detail: format!(
                    "unknown reset scope '{other}'. Valid: NEVER, YEARLY, MONTHLY, QUARTERLY, DAILY"
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_template() {
        let tokens = parse_format_template("INV-{YY}-{MM}-{SEQ:05}").unwrap();
        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0], FormatToken::Literal("INV-".into()));
        assert_eq!(tokens[1], FormatToken::Year2);
        assert_eq!(tokens[2], FormatToken::Literal("-".into()));
        assert_eq!(tokens[3], FormatToken::Month);
        assert_eq!(tokens[4], FormatToken::Literal("-".into()));
        assert_eq!(tokens[5], FormatToken::Seq { padding: 5 });
    }

    #[test]
    fn unicode_custom_key_preserves_original_bytes() {
        let tokens = parse_format_template("{CUSTOM:ß}{sEq}").unwrap();
        assert!(matches!(&tokens[0], FormatToken::Custom(key) if key == "ß"));
    }

    #[test]
    fn parse_all_tokens() {
        let tokens =
            parse_format_template("{YYYY}{YY}{MM}{DD}{Q}{WW}{TENANT}{CUSTOM:dept}{SEQ}").unwrap();
        assert_eq!(tokens.len(), 9);
    }

    #[test]
    fn parse_seq_no_padding() {
        let tokens = parse_format_template("{SEQ}").unwrap();
        assert_eq!(tokens[0], FormatToken::Seq { padding: 0 });
    }

    #[test]
    fn no_seq_token_error() {
        assert!(parse_format_template("INV-{YY}").is_err());
    }

    #[test]
    fn multiple_seq_tokens_error() {
        assert!(parse_format_template("{SEQ}-{SEQ}").is_err());
    }

    #[test]
    fn unknown_token_error() {
        assert!(parse_format_template("{SEQ}-{UNKNOWN}").is_err());
    }

    #[test]
    fn unclosed_brace_error() {
        assert!(parse_format_template("INV-{SEQ").is_err());
    }

    #[test]
    fn format_invoice_number() {
        let tokens = parse_format_template("INV-{YY}-{MM}-{SEQ:05}").unwrap();
        let ctx = FormatContext {
            counter: 23,
            year: 2026,
            month: 4,
            day: 2,
            quarter: 2,
            iso_week: 14,
            tenant: "ACME",
            session_vars: &HashMap::new(),
        };
        assert_eq!(format_sequence_value(&tokens, &ctx), "INV-26-04-00023");
    }

    #[test]
    fn format_with_tenant_and_custom() {
        let tokens = parse_format_template("{TENANT}-{CUSTOM:dept}-{SEQ:03}").unwrap();
        let mut vars = HashMap::new();
        vars.insert("dept".into(), "FIN".into());
        let ctx = FormatContext {
            counter: 7,
            year: 2026,
            month: 1,
            day: 15,
            quarter: 1,
            iso_week: 3,
            tenant: "ACME",
            session_vars: &vars,
        };
        assert_eq!(format_sequence_value(&tokens, &ctx), "ACME-FIN-007");
    }

    #[test]
    fn format_no_padding() {
        let tokens = parse_format_template("N-{SEQ}").unwrap();
        let ctx = FormatContext {
            counter: 42,
            year: 2026,
            month: 1,
            day: 1,
            quarter: 1,
            iso_week: 1,
            tenant: "",
            session_vars: &HashMap::new(),
        };
        assert_eq!(format_sequence_value(&tokens, &ctx), "N-42");
    }

    #[test]
    fn period_key_never() {
        assert_eq!(compute_period_key(&ResetScope::Never, 2026, 4, 2), "");
    }

    #[test]
    fn period_key_yearly() {
        assert_eq!(compute_period_key(&ResetScope::Yearly, 2026, 4, 2), "2026");
    }

    #[test]
    fn period_key_monthly() {
        assert_eq!(
            compute_period_key(&ResetScope::Monthly, 2026, 4, 2),
            "2026-04"
        );
    }

    #[test]
    fn period_key_quarterly() {
        assert_eq!(
            compute_period_key(&ResetScope::Quarterly, 2026, 4, 2),
            "2026-Q2"
        );
        assert_eq!(
            compute_period_key(&ResetScope::Quarterly, 2026, 1, 15),
            "2026-Q1"
        );
    }

    #[test]
    fn period_key_daily() {
        assert_eq!(
            compute_period_key(&ResetScope::Daily, 2026, 4, 2),
            "2026-04-02"
        );
    }

    #[test]
    fn reset_scope_parse() {
        assert_eq!(ResetScope::parse("MONTHLY").unwrap(), ResetScope::Monthly);
        assert_eq!(ResetScope::parse("yearly").unwrap(), ResetScope::Yearly);
        assert_eq!(ResetScope::parse("NEVER").unwrap(), ResetScope::Never);
        assert!(ResetScope::parse("BIWEEKLY").is_err());
    }

    // --- ISO week edge cases ---

    #[test]
    fn iso_week_mid_year() {
        // 2026-04-02 is a Thursday in week 14.
        assert_eq!(iso_week_number(2026, 4, 2), 14);
    }

    #[test]
    fn iso_week_jan1_thursday() {
        // 2026-01-01 is Thursday → week 1.
        assert_eq!(iso_week_number(2026, 1, 1), 1);
    }

    #[test]
    fn iso_week_dec31_belongs_to_week1_next_year() {
        // 2025-12-29 is Monday → ISO week 1 of 2026 (Jan 1, 2026 is Thursday).
        assert_eq!(iso_week_number(2025, 12, 29), 1);
    }

    #[test]
    fn iso_week_jan1_belongs_to_prev_year() {
        // 2016-01-01 is Friday → belongs to week 53 of 2015.
        assert_eq!(iso_week_number(2016, 1, 1), 53);
    }

    #[test]
    fn iso_week_53_long_year() {
        // 2015-12-31 is Thursday → week 53 of 2015 (long year).
        assert_eq!(iso_week_number(2015, 12, 31), 53);
    }

    #[test]
    fn iso_week_leap_year() {
        // 2024-02-29 (leap day) is Thursday → week 9.
        assert_eq!(iso_week_number(2024, 2, 29), 9);
    }

    #[test]
    fn iso_week_dec28_always_same_year() {
        // Dec 28 always belongs to the last week of its own year.
        assert_eq!(iso_week_number(2026, 12, 28), 53);
    }

    #[test]
    fn day_of_week_known_dates() {
        // 2026-01-01 = Thursday (4)
        assert_eq!(day_of_week_iso(2026, 1, 1), 4);
        // 2024-02-29 = Thursday (4)
        assert_eq!(day_of_week_iso(2024, 2, 29), 4);
        // 2025-12-29 = Monday (1)
        assert_eq!(day_of_week_iso(2025, 12, 29), 1);
        // 2016-01-01 = Friday (5)
        assert_eq!(day_of_week_iso(2016, 1, 1), 5);
    }
}
