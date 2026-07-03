// SPDX-License-Identifier: BUSL-1.1

//! Validate a pre-parsed `engine: Option<&str>` from the typed AST.

use super::super::super::super::super::result::DdlError;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
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
             (engine='vector', vector_field='embedding')"
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
pub(super) fn canonical_list() -> String {
    CANONICAL_ENGINES.join(", ")
}
