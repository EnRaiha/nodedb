// SPDX-License-Identifier: BUSL-1.1

//! Tokenizer for transaction-control statements.
//!
//! The grammar holds only keywords, identifiers and commas. Any other
//! character means the statement is not transaction control, and the
//! tokenizer returns `None`.

/// One lexical token of a transaction-control statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Token {
    /// An unquoted word. `upper` is its ASCII-upper-cased form for keyword
    /// matching. `folded` is its ASCII-lower-cased form, which is the name
    /// PostgreSQL gives an unquoted identifier.
    Word { upper: String, folded: String },
    /// A double-quoted identifier with `""` escapes resolved. Its case is kept.
    Quoted(String),
    /// A `,` between transaction modes.
    Comma,
}

impl Token {
    /// Return true if this token is the unquoted keyword `keyword`.
    /// `keyword` must be upper case.
    pub(super) fn is_keyword(&self, keyword: &str) -> bool {
        matches!(self, Token::Word { upper, .. } if upper == keyword)
    }

    /// The identifier this token names, or `None` for a comma.
    pub(super) fn identifier(&self) -> Option<&str> {
        match self {
            Token::Word { folded, .. } => Some(folded),
            Token::Quoted(name) => Some(name),
            Token::Comma => None,
        }
    }
}

fn is_word_start(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_'
}

fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

/// Split `sql` into tokens. Whitespace, `--` line comments, `/* */` block
/// comments (nested) and `;` terminators separate tokens and are dropped.
/// A `;` followed by another token returns `None`: that text holds more than
/// one statement. Returns `None` for any character outside the grammar and
/// for an unterminated quote or comment.
pub(super) fn tokenize(sql: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();
    let mut terminated = false;
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '-' && chars.peek() == Some(&'-') {
            for next in chars.by_ref() {
                if next == '\n' {
                    break;
                }
            }
            continue;
        }
        if ch == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut depth = 1usize;
            while depth > 0 {
                match chars.next()? {
                    '*' if chars.peek() == Some(&'/') => {
                        chars.next();
                        depth -= 1;
                    }
                    '/' if chars.peek() == Some(&'*') => {
                        chars.next();
                        depth += 1;
                    }
                    _ => {}
                }
            }
            continue;
        }
        if ch == ';' {
            terminated = true;
            continue;
        }
        if terminated {
            return None;
        }
        if ch == ',' {
            tokens.push(Token::Comma);
        } else if ch == '"' {
            let mut name = String::new();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        name.push('"');
                    }
                    '"' => break,
                    other => name.push(other),
                }
            }
            if name.is_empty() {
                return None;
            }
            tokens.push(Token::Quoted(name));
        } else if is_word_start(ch) {
            let mut word = String::from(ch);
            while let Some(&next) = chars.peek() {
                if !is_word_char(next) {
                    break;
                }
                word.push(next);
                chars.next();
            }
            tokens.push(Token::Word {
                upper: word.to_ascii_uppercase(),
                folded: word.to_ascii_lowercase(),
            });
        } else {
            return None;
        }
    }
    Some(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(sql: &str) -> Vec<String> {
        tokenize(sql)
            .expect("tokenizes")
            .into_iter()
            .map(|t| match t {
                Token::Word { upper, .. } => upper,
                Token::Quoted(name) => format!("\"{name}\""),
                Token::Comma => ",".to_owned(),
            })
            .collect()
    }

    #[test]
    fn drops_whitespace_comments_and_terminators() {
        assert_eq!(
            words("  rollback /* a /* nested */ b */ work -- tail\n ;; "),
            ["ROLLBACK", "WORK"]
        );
    }

    #[test]
    fn keeps_quoted_identifier_case_and_escapes() {
        assert_eq!(
            words(r#"SAVEPOINT "My ""sp""""#),
            ["SAVEPOINT", "\"My \"sp\"\""]
        );
    }

    #[test]
    fn refuses_text_outside_the_grammar() {
        assert!(tokenize("COMMIT PREPARED 'x'").is_none());
        assert!(tokenize("BEGIN; SELECT 1").is_none());
        assert!(tokenize("SAVEPOINT \"open").is_none());
        assert!(tokenize("ROLLBACK /* open").is_none());
        assert!(tokenize("SAVEPOINT \"\"").is_none());
    }
}
