// SPDX-License-Identifier: Apache-2.0

//! The recursive-descent scanner behind the object-literal entry points:
//! whitespace, identifiers, strings, numbers, nested objects, and arrays.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::error::SqlError;

pub(super) fn skip_ws(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn parse_ident(chars: &[char], pos: &mut usize) -> String {
    let mut s = String::new();
    while *pos < chars.len() {
        let c = chars[*pos];
        if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
            s.push(c);
            *pos += 1;
        } else {
            break;
        }
    }
    s
}

fn parse_string(chars: &[char], pos: &mut usize) -> Result<String, SqlError> {
    // Expect opening single-quote
    if *pos >= chars.len() || chars[*pos] != '\'' {
        return Err(SqlError::Parse {
            detail: format!(
                "expected single quote at position {}, found {:?}",
                pos,
                chars.get(*pos)
            ),
        });
    }
    *pos += 1; // consume opening quote
    let mut s = String::new();
    loop {
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: "unterminated string literal".to_string(),
            });
        }
        if chars[*pos] == '\'' {
            *pos += 1; // consume quote
            // SQL escaped quote: '' → '
            if *pos < chars.len() && chars[*pos] == '\'' {
                s.push('\'');
                *pos += 1;
            } else {
                break; // end of string
            }
        } else {
            s.push(chars[*pos]);
            *pos += 1;
        }
    }
    Ok(s)
}

fn parse_double_quoted_string(chars: &[char], pos: &mut usize) -> Result<String, SqlError> {
    if *pos >= chars.len() || chars[*pos] != '"' {
        return Err(SqlError::Parse {
            detail: format!(
                "expected double quote at position {}, found {:?}",
                pos,
                chars.get(*pos)
            ),
        });
    }
    *pos += 1;
    let mut s = String::new();
    loop {
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: "unterminated double-quoted string literal".to_string(),
            });
        }
        match chars[*pos] {
            '"' => {
                *pos += 1;
                if *pos < chars.len() && chars[*pos] == '"' {
                    s.push('"');
                    *pos += 1;
                } else {
                    break;
                }
            }
            c => {
                s.push(c);
                *pos += 1;
            }
        }
    }
    Ok(s)
}

fn parse_number(chars: &[char], pos: &mut usize) -> Result<Value, SqlError> {
    let start = *pos;
    if *pos < chars.len() && chars[*pos] == '-' {
        *pos += 1;
    }
    while *pos < chars.len() && chars[*pos].is_ascii_digit() {
        *pos += 1;
    }
    let is_float = *pos < chars.len() && chars[*pos] == '.';
    if is_float {
        *pos += 1; // consume '.'
        while *pos < chars.len() && chars[*pos].is_ascii_digit() {
            *pos += 1;
        }
    }
    let raw: String = chars[start..*pos].iter().collect();
    if is_float {
        raw.parse::<f64>()
            .map(Value::Float)
            .map_err(|_| SqlError::Parse {
                detail: format!("invalid float: {raw}"),
            })
    } else {
        raw.parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| SqlError::Parse {
                detail: format!("invalid integer: {raw}"),
            })
    }
}

fn parse_array(chars: &[char], pos: &mut usize) -> Result<Vec<Value>, SqlError> {
    // Expect '['
    if *pos >= chars.len() || chars[*pos] != '[' {
        return Err(SqlError::Parse {
            detail: format!(
                "expected '[' at position {pos}, found {:?}",
                chars.get(*pos)
            ),
        });
    }
    *pos += 1; // consume '['
    let mut items = Vec::new();
    loop {
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: "unterminated array literal".to_string(),
            });
        }
        if chars[*pos] == ']' {
            *pos += 1; // consume ']'
            break;
        }
        // trailing comma already consumed; skip it
        if chars[*pos] == ',' {
            *pos += 1;
            continue;
        }
        let val = parse_value(chars, pos)?;
        items.push(val);
        skip_ws(chars, pos);
        if *pos < chars.len() && chars[*pos] == ',' {
            *pos += 1; // consume ','
        }
    }
    Ok(items)
}

pub(super) fn parse_object(
    chars: &[char],
    pos: &mut usize,
) -> Result<HashMap<String, Value>, SqlError> {
    // Expect '{'
    if *pos >= chars.len() || chars[*pos] != '{' {
        return Err(SqlError::Parse {
            detail: format!(
                "expected '{{' at position {pos}, found {:?}",
                chars.get(*pos)
            ),
        });
    }
    *pos += 1; // consume '{'
    let mut map = HashMap::new();
    loop {
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: "unterminated object literal".to_string(),
            });
        }
        if chars[*pos] == '}' {
            *pos += 1; // consume '}'
            break;
        }
        // Trailing comma: skip and re-check for '}'
        if chars[*pos] == ',' {
            *pos += 1;
            continue;
        }

        // Parse key (identifier or JSON-style quoted key).
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: "expected key, reached end of input".to_string(),
            });
        }
        let key = if chars[*pos] == '"' {
            parse_double_quoted_string(chars, pos)?
        } else {
            let first = chars[*pos];
            if !(first.is_ascii_alphabetic() || first == '_') {
                return Err(SqlError::Parse {
                    detail: format!("expected identifier key at position {pos}, found '{first}'"),
                });
            }
            parse_ident(chars, pos)
        };
        if key.is_empty() {
            return Err(SqlError::Parse {
                detail: format!("expected non-empty key at position {pos}"),
            });
        }

        // Expect ':'
        skip_ws(chars, pos);
        if *pos >= chars.len() || chars[*pos] != ':' {
            return Err(SqlError::Parse {
                detail: format!(
                    "expected ':' after key '{key}' at position {pos}, found {:?}",
                    chars.get(*pos)
                ),
            });
        }
        *pos += 1; // consume ':'

        // Parse value
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            return Err(SqlError::Parse {
                detail: format!("expected value for key '{key}', reached end of input"),
            });
        }
        if chars[*pos] == '}' || chars[*pos] == ',' {
            return Err(SqlError::Parse {
                detail: format!("expected value for key '{key}', found '{}'", chars[*pos]),
            });
        }
        let val = parse_value(chars, pos)?;
        map.insert(key, val);

        // Optional comma
        skip_ws(chars, pos);
        if *pos < chars.len() && chars[*pos] == ',' {
            *pos += 1;
        }
    }
    Ok(map)
}

fn parse_value(chars: &[char], pos: &mut usize) -> Result<Value, SqlError> {
    skip_ws(chars, pos);
    if *pos >= chars.len() {
        return Err(SqlError::Parse {
            detail: "unexpected end of input while parsing value".to_string(),
        });
    }
    match chars[*pos] {
        '\'' => parse_string(chars, pos).map(Value::String),
        '{' => parse_object(chars, pos).map(Value::Object),
        '[' => parse_array(chars, pos).map(Value::Array),
        '-' | '0'..='9' => parse_number(chars, pos),
        _ => {
            // bare word: true / false / null / identifier
            let word = parse_ident(chars, pos);
            match word.to_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                "null" => Ok(Value::Null),
                _ if word.is_empty() => Err(SqlError::Parse {
                    detail: format!("unexpected character '{}' at position {pos}", chars[*pos]),
                }),
                _ => Err(SqlError::Parse {
                    detail: format!("unknown bare word: '{word}'"),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::object_literal::{
        parse_object_literal, parse_object_literal_array, parse_object_literal_array_complete,
        parse_object_literal_complete,
    };

    fn parse(s: &str) -> HashMap<String, Value> {
        parse_object_literal(s).unwrap().unwrap()
    }

    /// The prefix parser tolerates trailing input BY DESIGN — the
    /// function-argument rewriter depends on it — so the tolerance is pinned
    /// here rather than left as an accident of the implementation.
    #[test]
    fn the_prefix_parser_stops_at_the_matching_brace() {
        let fields = parse("{ a: 1 } and then some");
        assert_eq!(fields.len(), 1);
    }

    /// …and the strict form reports what the prefix parser would have thrown
    /// away, naming it so the author can see which part was rejected.
    #[test]
    fn the_strict_parser_reports_trailing_input() {
        let error = parse_object_literal_complete("{ a: 1 } RETURNING *")
            .expect("input starts with a brace")
            .expect_err("trailing input must not be accepted");
        let detail = error.to_string();
        assert!(
            detail.contains("RETURNING *"),
            "the error must name the leftover, got: {detail}"
        );
    }

    /// A statement terminator is not content, so it does not trip the check.
    #[test]
    fn the_strict_parser_accepts_a_trailing_semicolon() {
        assert!(
            parse_object_literal_complete("{ a: 1 };")
                .expect("input starts with a brace")
                .is_ok()
        );
    }

    /// A `}` inside a quoted value belongs to the value, so it must not be
    /// mistaken for the end of the literal.
    #[test]
    fn the_strict_parser_ignores_a_brace_inside_a_string() {
        let fields = parse_object_literal_complete("{ note: '} not the end' }")
            .expect("input starts with a brace")
            .expect("a brace inside a string is part of the value");
        assert_eq!(fields.len(), 1);
    }

    /// The array form gets the same contract, measured against the real
    /// closing bracket rather than the last one in the input.
    #[test]
    fn the_strict_array_parser_reports_trailing_input() {
        assert!(
            parse_object_literal_array_complete("[{ a: 1 }] RETURNING *")
                .expect("input starts with a bracket")
                .is_err()
        );
        assert!(
            parse_object_literal_array_complete("[{ note: 'x]y' }]")
                .expect("input starts with a bracket")
                .is_ok(),
            "a bracket inside a string must not be read as the array's end"
        );
    }

    #[test]
    fn simple_string_and_int() {
        let m = parse("{ name: 'Alice', age: 30 }");
        assert_eq!(m["name"], Value::String("Alice".to_string()));
        assert_eq!(m["age"], Value::Integer(30));
    }

    #[test]
    fn nested_object() {
        let m = parse("{ addr: { city: 'NYC' } }");
        let inner = match &m["addr"] {
            Value::Object(o) => o,
            _ => panic!("expected Object"),
        };
        assert_eq!(inner["city"], Value::String("NYC".to_string()));
    }

    #[test]
    fn array_value() {
        let m = parse("{ tags: ['a', 'b'] }");
        assert_eq!(
            m["tags"],
            Value::Array(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ])
        );
    }

    #[test]
    fn mixed_types() {
        let m = parse("{ a: 'str', b: 42, c: 2.78, d: true, e: false, f: null }");
        assert_eq!(m["a"], Value::String("str".to_string()));
        assert_eq!(m["b"], Value::Integer(42));
        assert_eq!(m["c"], Value::Float(2.78));
        assert_eq!(m["d"], Value::Bool(true));
        assert_eq!(m["e"], Value::Bool(false));
        assert_eq!(m["f"], Value::Null);
    }

    #[test]
    fn escaped_quotes() {
        let m = parse("{ name: 'O''Brien' }");
        assert_eq!(m["name"], Value::String("O'Brien".to_string()));
    }

    #[test]
    fn empty_object() {
        let m = parse("{ }");
        assert!(m.is_empty());
    }

    #[test]
    fn trailing_comma() {
        let m = parse("{ name: 'Alice', }");
        assert_eq!(m["name"], Value::String("Alice".to_string()));
    }

    #[test]
    fn not_an_object_returns_none() {
        assert!(parse_object_literal("not an object").is_none());
    }

    #[test]
    fn missing_value_returns_err() {
        let result = parse_object_literal("{ name: }");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn missing_key_returns_err() {
        let result = parse_object_literal("{ : 'val' }");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn negative_numbers() {
        let m = parse("{ x: -42, y: -2.78 }");
        assert_eq!(m["x"], Value::Integer(-42));
        assert_eq!(m["y"], Value::Float(-2.78));
    }

    #[test]
    fn nested_array_in_object() {
        let m = parse("{ data: { items: [1, 2, 3] } }");
        let inner = match &m["data"] {
            Value::Object(o) => o,
            _ => panic!("expected Object"),
        };
        assert_eq!(
            inner["items"],
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ])
        );
    }

    #[test]
    fn dotted_key() {
        let m = parse("{ metadata.source: 'web' }");
        assert_eq!(m["metadata.source"], Value::String("web".to_string()));
    }

    #[test]
    fn parse_array_of_objects() {
        let result = parse_object_literal_array("[{ name: 'Alice' }, { name: 'Bob' }]")
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["name"], Value::String("Alice".to_string()));
        assert_eq!(result[1]["name"], Value::String("Bob".to_string()));
    }

    #[test]
    fn parse_array_empty() {
        let result = parse_object_literal_array("[]").unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_array_not_array_returns_none() {
        assert!(parse_object_literal_array("{ name: 'Alice' }").is_none());
    }

    #[test]
    fn parse_array_non_object_element_returns_err() {
        let result = parse_object_literal_array("[42]");
        assert!(matches!(result, Some(Err(_))));
    }

    #[test]
    fn parse_array_trailing_comma() {
        let result = parse_object_literal_array("[{ a: 1 }, { b: 2 },]")
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 2);
    }
}
