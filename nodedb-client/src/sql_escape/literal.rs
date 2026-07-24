// SPDX-License-Identifier: Apache-2.0

//! Client-compatible alias for the shared SQL literal quoting chokepoint.

pub(crate) use nodedb_types::quote_literal as quote_string_literal;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_single_quotes() {
        assert_eq!(quote_string_literal("plain"), "'plain'");
        // The PG standard rule under `standard_conforming_strings=on`:
        // double every embedded `'`. A `O'Reilly` literal that lost its
        // escape would close the SQL string early and inject the rest.
        assert_eq!(quote_string_literal("O'Reilly"), "'O''Reilly'");
        assert_eq!(
            quote_string_literal("'; DROP TABLE x; --"),
            "'''; DROP TABLE x; --'"
        );
    }

    #[test]
    fn passes_through_json() {
        // The metadata path renders sonic_rs JSON and then quotes it as
        // a SQL string. JSON already escapes its own `"` and `\`, so
        // only the outer `'` needs SQL escaping.
        let json = r#"{"name":"O'Reilly","ok":true}"#;
        let quoted = quote_string_literal(json);
        assert_eq!(quoted, "'{\"name\":\"O''Reilly\",\"ok\":true}'");
    }
}
