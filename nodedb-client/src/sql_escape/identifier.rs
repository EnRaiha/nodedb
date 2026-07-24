// SPDX-License-Identifier: Apache-2.0

//! Client-compatible alias for the shared SQL identifier quoting chokepoint.

pub(crate) use nodedb_types::quote_ident as quote_identifier;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_and_escapes_double_quotes() {
        assert_eq!(quote_identifier("foo"), "\"foo\"");
        // Embedded `"` must be doubled per the SQL identifier-escape rule.
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }
}
