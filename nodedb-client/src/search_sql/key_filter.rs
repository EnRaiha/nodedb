// SPDX-License-Identifier: Apache-2.0

//! The `<key> IN (...)` conjunct that restricts a search to allowed ids.

use std::collections::HashSet;

use crate::sql_escape::{quote_identifier, quote_string_literal};

/// `<key> IN ('a', 'b', ...)` over `ids` in sorted order.
///
/// `key` is the collection's identity column: the declared primary key,
/// else `id`. The server lowers a top-level conjunct on that column to the
/// candidate set a search ranks within. A conjunct on any other column
/// filters after the ranking cut instead.
pub(crate) fn key_in_list(key: &str, ids: &HashSet<String>) -> String {
    let mut ids: Vec<&String> = ids.iter().collect();
    ids.sort();
    let list: Vec<String> = ids
        .iter()
        .map(|id| quote_string_literal(id.as_str()))
        .collect();
    format!("{} IN ({})", quote_identifier(key), list.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_render_sorted_and_quoted_under_the_key_column() {
        let ids: HashSet<String> = ["b", "a'"].iter().map(|s| s.to_string()).collect();
        assert_eq!(key_in_list("sku", &ids), "\"sku\" IN ('a''', 'b')");
    }
}
