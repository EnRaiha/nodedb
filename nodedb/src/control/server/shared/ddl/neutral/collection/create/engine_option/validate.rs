// SPDX-License-Identifier: BUSL-1.1

//! Validate a pre-parsed `engine: Option<&str>` from the typed AST.

use super::super::super::super::super::result::DdlError;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The seven canonical engine names. Anything else is `42601`.
pub const CANONICAL_ENGINES: &[&str] = &[
    "document_schemaless",
    "document_strict",
    "kv",
    "columnar",
    "timeseries",
    "spatial",
    "vector",
];

/// Validate a pre-parsed `engine: Option<&str>` from the typed AST.
///
/// Also checks for deprecated axes (profile=, vector_field= without engine=)
/// from the parsed `options` slice.
///
/// Returns the canonical engine static str (`Some`) or `None` (caller applies default).
pub fn validate_engine_name(
    engine: Option<&str>,
    options: &[(String, String)],
) -> Result<Option<&'static str>, DdlError> {
    // ── Axis B: profile= present ──────────────────────────────────
    if let Some((_, profile_val)) = options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("profile"))
    {
        let profile_up = profile_val.to_uppercase();
        let suggestion = match profile_up.as_str() {
            "TIMESERIES" => "engine='timeseries'",
            "SPATIAL" => "engine='spatial'",
            other => {
                return Err(err(
                    "0A000",
                    format!(
                        "NodeDB has canonicalized engine selection; 'WITH (profile='{other}')' \
                         is no longer accepted. Use WITH (engine='timeseries') or \
                         WITH (engine='spatial')"
                    ),
                ));
            }
        };
        return Err(err(
            "0A000",
            format!(
                "NodeDB has canonicalized engine selection; 'WITH (profile=...)' is no longer \
                 accepted. Use: CREATE COLLECTION foo (...) WITH ({suggestion})"
            ),
        ));
    }

    // ── Axis C: vector_field= without engine= ─────────────────────
    if engine.is_none()
        && options
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("vector_field"))
    {
        return Err(err(
            "0A000",
            "NodeDB has canonicalized engine selection; 'WITH (vector_field=...)' without \
             'engine=...' is no longer accepted. Use: CREATE COLLECTION foo (...) WITH \
             (engine='vector', vector_field='embedding', dim=<n>)"
                .to_string(),
        ));
    }

    let engine_lower = match engine {
        None => return Ok(None),
        Some(e) => e.to_lowercase(),
    };

    let canonical = match engine_lower.as_str() {
        "document_schemaless" => "document_schemaless",
        "document_strict" => "document_strict",
        "kv" => "kv",
        "columnar" => "columnar",
        "timeseries" => "timeseries",
        "spatial" => "spatial",
        "vector" => "vector",
        "strict" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'strict'; did you mean 'document_strict'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "document" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'document'; did you mean 'document_schemaless' or \
                     'document_strict'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "key_value" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'key_value'; did you mean 'kv'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "fts" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'fts'; FTS uses CREATE FULLTEXT INDEX (separate DDL); \
                     graph operations work against existing collections via MATCH. \
                     Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "graph" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'graph'; graph operations work against existing collections \
                     via MATCH / GRAPH INSERT/DELETE — there is no engine='graph'. \
                     Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        other => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine '{other}'; supported: {}. \
                     (FTS uses CREATE FULLTEXT INDEX; graph operations work against existing \
                     collections via MATCH.)",
                    canonical_list()
                ),
            ));
        }
    };

    Ok(Some(canonical))
}

/// Build the canonical-list suffix used in unknown-engine errors.
fn canonical_list() -> String {
    CANONICAL_ENGINES.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn refusal(engine: Option<&str>, options: &[(&str, &str)]) -> String {
        match validate_engine_name(engine, &opts(options)) {
            Ok(engine) => panic!("expected a refusal, got {engine:?}"),
            Err(e) => format!("{}: {}", e.sqlstate, e.message),
        }
    }

    #[test]
    fn every_canonical_engine_is_accepted() {
        for name in CANONICAL_ENGINES.iter().copied() {
            assert_eq!(
                validate_engine_name(Some(name), &[]).expect("canonical"),
                Some(name)
            );
            let upper = name.to_uppercase();
            assert_eq!(
                validate_engine_name(Some(&upper), &[]).expect("case-insensitive"),
                Some(name)
            );
        }
    }

    #[test]
    fn no_engine_leaves_the_default_to_the_caller() {
        assert_eq!(validate_engine_name(None, &[]).expect("no engine"), None);
    }

    #[test]
    fn profile_option_is_refused_with_the_engine_to_use() {
        let e = refusal(None, &[("profile", "timeseries")]);
        assert!(
            e.starts_with("0A000") && e.contains("engine='timeseries'"),
            "{e}"
        );
        let e = refusal(Some("columnar"), &[("profile", "spatial")]);
        assert!(e.contains("engine='spatial'"), "{e}");
        let e = refusal(None, &[("profile", "other")]);
        assert!(e.starts_with("0A000"), "{e}");
    }

    #[test]
    fn vector_field_without_an_engine_is_refused() {
        let e = refusal(None, &[("vector_field", "emb")]);
        assert!(
            e.starts_with("0A000") && e.contains("engine='vector'"),
            "{e}"
        );
        assert_eq!(
            validate_engine_name(Some("vector"), &opts(&[("vector_field", "emb")]))
                .expect("vector with its field"),
            Some("vector")
        );
    }

    #[test]
    fn engine_aliases_are_refused_with_the_canonical_name() {
        for (alias, hint) in [
            ("strict", "document_strict"),
            ("document", "document_schemaless"),
            ("key_value", "'kv'"),
            ("fts", "FULLTEXT"),
            ("graph", "MATCH"),
        ] {
            let e = refusal(Some(alias), &[]);
            assert!(e.starts_with("42601") && e.contains(hint), "{alias}: {e}");
        }
    }

    #[test]
    fn unknown_engine_is_refused_with_the_supported_list() {
        let e = refusal(Some("nonsense"), &[]);
        assert!(e.starts_with("42601"), "{e}");
        assert!(
            e.contains("nonsense") && e.contains("document_schemaless"),
            "{e}"
        );
    }
}
