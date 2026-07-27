// SPDX-License-Identifier: Apache-2.0

//! Rewrite `SEARCH <coll> USING VECTOR(<field>, ARRAY[...], <k>)` to the
//! canonical `SELECT * FROM <coll> ORDER BY vector_distance(<field>, ARRAY[...]) LIMIT <k>`.
//!
//! `<field>` may be omitted (and the third arg becomes the limit). When the
//! collection has a single declared vector column the planner resolves the
//! field; otherwise `vector_distance` rejects the call with a typed error.

const SEARCH_KEYWORD: &str = "SEARCH";
const USING_KEYWORD: &str = "USING";
const VECTOR_KEYWORD: &str = "VECTOR";

use nodedb_types::strip_prefix_ascii_case_insensitive;

pub fn try_rewrite_search_using_vector(sql: &str) -> Option<String> {
    let trimmed = sql.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let source = trimmed.trim_start();
    let leading = trimmed.len() - source.len();
    let after_search = strip_prefix_ascii_case_insensitive(source, SEARCH_KEYWORD)?.trim_start();
    if after_search.is_empty() {
        return None;
    }

    let (collection, rest) = take_identifier(after_search)?;
    let rest = rest.trim_start();
    let after_using = strip_prefix_ascii_case_insensitive(rest, USING_KEYWORD)?.trim_start();
    let after_vec = strip_prefix_ascii_case_insensitive(after_using, VECTOR_KEYWORD)?.trim_start();
    let body = strip_parentheses(after_vec)?;
    let (field, vector_expr, limit) = split_vector_args(body)?;

    let trailing = sql[leading + (trimmed.len() - leading)..].to_string();
    let order_by = match field {
        Some(name) => format!("vector_distance({name}, {vector_expr})"),
        None => format!("vector_distance({vector_expr})"),
    };
    Some(format!(
        "SELECT * FROM {collection} ORDER BY {order_by} LIMIT {limit}{trailing}"
    ))
}

fn take_identifier(input: &str) -> Option<(&str, &str)> {
    let end = input
        .char_indices()
        .find(|(_, c)| !is_ident_char(*c))
        .map(|(i, _)| i)
        .unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    Some((&input[..end], &input[end..]))
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn strip_parentheses(input: &str) -> Option<&str> {
    let trimmed = input.trim();
    let bytes = trimmed.as_bytes();
    if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
        return None;
    }
    Some(trimmed[1..trimmed.len() - 1].trim())
}

fn split_vector_args(body: &str) -> Option<(Option<String>, String, String)> {
    let parts = split_top_level_commas(body);
    match parts.as_slice() {
        [field, vec, k] => {
            let field = field.trim();
            let trimmed = if field.is_empty() {
                None
            } else {
                Some(field.to_string())
            };
            Some((trimmed, vec.trim().to_string(), k.trim().to_string()))
        }
        [vec, k] => Some((None, vec.trim().to_string(), k.trim().to_string())),
        _ => None,
    }
}

fn split_top_level_commas(body: &str) -> Vec<String> {
    let mut depth_paren = 0i32;
    let mut depth_bracket = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut current = String::new();
    let mut out = Vec::new();
    for c in body.chars() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth_paren += 1,
            ')' if !in_single && !in_double => depth_paren -= 1,
            '[' if !in_single && !in_double => depth_bracket += 1,
            ']' if !in_single && !in_double => depth_bracket -= 1,
            ',' if !in_single && !in_double && depth_paren == 0 && depth_bracket == 0 => {
                out.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_three_arg_form() {
        let out = try_rewrite_search_using_vector(
            "SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2], 10)",
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM articles ORDER BY vector_distance(embedding, ARRAY[0.1, 0.3, -0.2]) LIMIT 10"
        );
    }

    #[test]
    fn unicode_case_expansions_in_vector_expression_preserve_original_text() {
        let out = try_rewrite_search_using_vector(
            "SEARCH docs USING VECTOR(embedding, ARRAY['ß', 'ﬀ', 'İ'], 5)",
        )
        .expect("search vector form should rewrite");
        assert!(out.contains("ARRAY['ß', 'ﬀ', 'İ']"));
    }

    #[test]
    fn rewrites_two_arg_form_when_field_omitted() {
        let out =
            try_rewrite_search_using_vector("SEARCH articles USING VECTOR(ARRAY[0.1, 0.3], 5)")
                .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM articles ORDER BY vector_distance(ARRAY[0.1, 0.3]) LIMIT 5"
        );
    }

    #[test]
    fn returns_none_when_not_search() {
        assert!(try_rewrite_search_using_vector("SELECT * FROM t").is_none());
    }

    #[test]
    fn returns_none_when_using_fusion() {
        assert!(try_rewrite_search_using_vector("SEARCH c USING FUSION(ARRAY[0.5])").is_none());
    }

    #[test]
    fn handles_trailing_semicolon() {
        let out =
            try_rewrite_search_using_vector("SEARCH t USING VECTOR(emb, ARRAY[1.0], 3);").unwrap();
        assert!(
            out.starts_with("SELECT * FROM t ORDER BY vector_distance(emb, ARRAY[1.0]) LIMIT 3")
        );
    }
}
