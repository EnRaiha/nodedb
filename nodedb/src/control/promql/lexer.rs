// SPDX-License-Identifier: BUSL-1.1

//! PromQL tokenizer.

use super::error::PromqlError;

/// Token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    Number(f64),
    String(String),
    Duration(String), // raw string like "5m", "1h30m"
    Ident(String),

    // Operators
    Add,           // +
    Sub,           // -
    Mul,           // *
    Div,           // /
    Mod,           // %
    Pow,           // ^
    Eq,            // ==
    Neq,           // !=
    Lt,            // <
    Gt,            // >
    Lte,           // <=
    Gte,           // >=
    Assign,        // =
    MatchRegex,    // =~
    NotMatchRegex, // !~

    // Punctuation
    LParen,   // (
    RParen,   // )
    LBrace,   // {
    RBrace,   // }
    LBracket, // [
    RBracket, // ]
    Comma,    // ,
    Colon,    // :

    // Keywords (contextual — identified from Ident during parsing)
    // The lexer emits all keywords as Ident; the parser checks names.
    Eof,
}

/// Tokenize a PromQL expression.
pub fn tokenize(input: &str) -> Result<Vec<Token>, PromqlError> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    // Inside `[...]` a `:` separates a subquery's range from its step. A
    // metric name can start with `:`, but no metric name appears there.
    let mut bracket_depth = 0usize;

    while i < bytes.len() {
        // Skip whitespace.
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Skip line comments.
        if bytes[i] == b'#' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // String literals.
        if bytes[i] == b'"' || bytes[i] == b'\'' || bytes[i] == b'`' {
            let (tok, end) = lex_string(bytes, i)?;
            tokens.push(tok);
            i = end;
            continue;
        }

        // Number or duration.
        if bytes[i].is_ascii_digit()
            || (bytes[i] == b'.' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let (tok, end) = lex_number_or_duration(bytes, i)?;
            tokens.push(tok);
            i = end;
            continue;
        }

        if bytes[i] == b':' && bracket_depth > 0 {
            tokens.push(Token::Colon);
            i += 1;
            continue;
        }

        // Identifier or keyword.
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' || bytes[i] == b':' {
            let (tok, end) = lex_ident(bytes, i);
            tokens.push(tok);
            i = end;
            continue;
        }

        // Multi-char operators.
        if i + 1 < bytes.len() {
            match (bytes[i], bytes[i + 1]) {
                (b'=', b'=') => {
                    tokens.push(Token::Eq);
                    i += 2;
                    continue;
                }
                (b'!', b'=') => {
                    tokens.push(Token::Neq);
                    i += 2;
                    continue;
                }
                (b'<', b'=') => {
                    tokens.push(Token::Lte);
                    i += 2;
                    continue;
                }
                (b'>', b'=') => {
                    tokens.push(Token::Gte);
                    i += 2;
                    continue;
                }
                (b'=', b'~') => {
                    tokens.push(Token::MatchRegex);
                    i += 2;
                    continue;
                }
                (b'!', b'~') => {
                    tokens.push(Token::NotMatchRegex);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators/punctuation.
        let tok = match bytes[i] {
            b'+' => Token::Add,
            b'-' => Token::Sub,
            b'*' => Token::Mul,
            b'/' => Token::Div,
            b'%' => Token::Mod,
            b'^' => Token::Pow,
            b'=' => Token::Assign,
            b'<' => Token::Lt,
            b'>' => Token::Gt,
            b'(' => Token::LParen,
            b')' => Token::RParen,
            b'{' => Token::LBrace,
            b'}' => Token::RBrace,
            b'[' => {
                bracket_depth += 1;
                Token::LBracket
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                Token::RBracket
            }
            b',' => Token::Comma,
            b':' => Token::Colon,
            _ => {
                return Err(PromqlError::UnexpectedToken {
                    expected: "valid PromQL token".to_string(),
                    found: format!("'{}' at position {i}", bytes[i] as char),
                });
            }
        };
        tokens.push(tok);
        i += 1;
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn lex_string(bytes: &[u8], start: usize) -> Result<(Token, usize), PromqlError> {
    let quote = bytes[start];
    let mut i = start + 1;
    // Collected as bytes so a multi-byte UTF-8 character stays one character.
    let mut s: Vec<u8> = Vec::new();

    while i < bytes.len() {
        if bytes[i] == quote {
            let text = String::from_utf8(s).map_err(|_| PromqlError::InvalidString {
                detail: format!("string starting at position {start} is not valid UTF-8"),
            })?;
            return Ok((Token::String(text), i + 1));
        }
        if bytes[i] == b'\\' && quote != b'`' && i + 1 < bytes.len() {
            i += 1;
            match bytes[i] {
                b'n' => s.push(b'\n'),
                b't' => s.push(b'\t'),
                b'\\' => s.push(b'\\'),
                b'\'' => s.push(b'\''),
                b'"' => s.push(b'"'),
                c => {
                    s.push(b'\\');
                    s.push(c);
                }
            }
        } else {
            s.push(bytes[i]);
        }
        i += 1;
    }
    Err(PromqlError::InvalidString {
        detail: format!("unterminated string starting at position {start}"),
    })
}

fn lex_number_or_duration(bytes: &[u8], start: usize) -> Result<(Token, usize), PromqlError> {
    let mut i = start;
    let mut has_dot = false;
    let mut has_exp = false;

    // Consume digits and at most one dot.
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            i += 1;
        } else if bytes[i] == b'.' && !has_dot && !has_exp {
            has_dot = true;
            i += 1;
        } else if (bytes[i] == b'e' || bytes[i] == b'E') && !has_exp {
            has_exp = true;
            i += 1;
            if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                i += 1;
            }
        } else {
            break;
        }
    }

    // Check for duration suffix (s, m, h, d, w, y).
    if i < bytes.len() && matches!(bytes[i], b's' | b'm' | b'h' | b'd' | b'w' | b'y') {
        // It's a duration — consume all duration parts.
        while i < bytes.len()
            && (bytes[i].is_ascii_digit()
                || matches!(bytes[i], b's' | b'm' | b'h' | b'd' | b'w' | b'y' | b'.'))
        {
            i += 1;
        }
        let raw = std::str::from_utf8(&bytes[start..i]).unwrap_or("0s");
        return Ok((Token::Duration(raw.to_string()), i));
    }

    let raw = std::str::from_utf8(&bytes[start..i]).unwrap_or("0");
    let num: f64 = raw.parse().map_err(|_| PromqlError::UnexpectedToken {
        expected: "valid number".to_string(),
        found: format!("'{raw}' at position {start}"),
    })?;
    Ok((Token::Number(num), i))
}

fn lex_ident(bytes: &[u8], start: usize) -> (Token, usize) {
    let mut i = start;
    while i < bytes.len()
        && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b':')
    {
        i += 1;
    }
    let word = std::str::from_utf8(&bytes[start..i]).unwrap_or("");

    // Special identifier-like tokens.
    match word {
        "Inf" | "inf" => (Token::Number(f64::INFINITY), i),
        "NaN" | "nan" => (Token::Number(f64::NAN), i),
        _ => (Token::Ident(word.to_string()), i),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_metric() {
        let tokens = tokenize("up").unwrap();
        assert_eq!(tokens, vec![Token::Ident("up".into()), Token::Eof]);
    }

    #[test]
    fn vector_selector() {
        let tokens = tokenize(r#"http_requests_total{method="GET"}"#).unwrap();
        assert_eq!(tokens[0], Token::Ident("http_requests_total".into()));
        assert_eq!(tokens[1], Token::LBrace);
        assert_eq!(tokens[2], Token::Ident("method".into()));
        assert_eq!(tokens[3], Token::Assign);
        assert_eq!(tokens[4], Token::String("GET".into()));
        assert_eq!(tokens[5], Token::RBrace);
    }

    #[test]
    fn range_selector() {
        let tokens = tokenize("rate(requests[5m])").unwrap();
        assert_eq!(tokens[0], Token::Ident("rate".into()));
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Ident("requests".into()));
        assert_eq!(tokens[3], Token::LBracket);
        assert_eq!(tokens[4], Token::Duration("5m".into()));
        assert_eq!(tokens[5], Token::RBracket);
        assert_eq!(tokens[6], Token::RParen);
    }

    #[test]
    fn binary_expr() {
        let tokens = tokenize("a + b * 2").unwrap();
        assert_eq!(tokens[0], Token::Ident("a".into()));
        assert_eq!(tokens[1], Token::Add);
        assert_eq!(tokens[2], Token::Ident("b".into()));
        assert_eq!(tokens[3], Token::Mul);
        assert_eq!(tokens[4], Token::Number(2.0));
    }

    #[test]
    fn comparison_ops() {
        let tokens = tokenize("a == b != c >= d").unwrap();
        assert_eq!(tokens[1], Token::Eq);
        assert_eq!(tokens[3], Token::Neq);
        assert_eq!(tokens[5], Token::Gte);
    }

    #[test]
    fn regex_matchers() {
        let tokens = tokenize(r#"{job=~"api.*", env!~"test"}"#).unwrap();
        assert_eq!(tokens[2], Token::MatchRegex);
        assert_eq!(tokens[6], Token::NotMatchRegex);
    }

    #[test]
    fn aggregation() {
        let tokens = tokenize("sum by (job) (rate(requests[5m]))").unwrap();
        assert_eq!(tokens[0], Token::Ident("sum".into()));
        assert_eq!(tokens[1], Token::Ident("by".into()));
    }

    #[test]
    fn inf_nan() {
        let tokens = tokenize("Inf NaN").unwrap();
        assert!(matches!(tokens[0], Token::Number(v) if v.is_infinite()));
        assert!(matches!(tokens[1], Token::Number(v) if v.is_nan()));
    }

    #[test]
    fn string_escapes() {
        let tokens = tokenize(r#""hello\nworld""#).unwrap();
        assert_eq!(tokens[0], Token::String("hello\nworld".into()));
    }

    #[test]
    fn subquery_colon_inside_brackets() {
        let tokens = tokenize("x[10m:1m]").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("x".into()),
                Token::LBracket,
                Token::Duration("10m".into()),
                Token::Colon,
                Token::Duration("1m".into()),
                Token::RBracket,
                Token::Eof,
            ]
        );
        let tokens = tokenize("x[5m:]").unwrap();
        assert_eq!(tokens[3], Token::Colon);
        assert_eq!(tokens[4], Token::RBracket);
    }

    #[test]
    fn colon_metric_names_outside_brackets() {
        let tokens = tokenize("job:rate5m[5m]").unwrap();
        assert_eq!(tokens[0], Token::Ident("job:rate5m".into()));
        let tokens = tokenize(":recorded").unwrap();
        assert_eq!(tokens[0], Token::Ident(":recorded".into()));
    }

    #[test]
    fn multi_byte_string_survives() {
        let tokens = tokenize(r#""héllo 世界""#).unwrap();
        assert_eq!(tokens[0], Token::String("héllo 世界".into()));
    }

    #[test]
    fn duration_compound() {
        let tokens = tokenize("1h30m").unwrap();
        assert_eq!(tokens[0], Token::Duration("1h30m".into()));
    }
}
