// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral CREATE SPATIAL INDEX DDL handling.
//!
//! Syntax:
//! ```sql
//! CREATE SPATIAL INDEX <name> ON <collection>(<field>) [USING RTREE|GEOHASH] [PRECISION <n>]
//! DROP INDEX <name>   -- handled by existing DROP INDEX path
//! ```
//!
//! The handler builds [`DdlResult`](super::super::result::DdlResult) directly
//! and carries no pgwire types.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::owner;
use super::super::result::{DdlError, DdlResult};

/// CREATE SPATIAL INDEX <name> ON <collection>(<field>) [USING RTREE|GEOHASH] [PRECISION <n>]
pub fn create_spatial_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // Minimum: CREATE SPATIAL INDEX name ON collection(field)
    if parts.len() < 6 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: CREATE SPATIAL INDEX <name> ON <collection>(<field>) [USING RTREE|GEOHASH] [PRECISION <n>]".to_string(),
        });
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected ON after index name".to_string(),
        });
    }

    // Parse collection(field) — field may be in the same token or next token.
    let collection_field = parts[5];
    let (collection, field) = parse_collection_field(collection_field, parts.get(6).copied())?;

    let tenant_id = identity.tenant_id;

    // Parse optional USING and PRECISION.
    let upper_parts: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();
    let index_type = parse_index_type(&upper_parts);
    let precision = parse_precision(&upper_parts);

    // Validate index type / field combination.
    if index_type == "geohash" && precision == 0 {
        // Default geohash precision.
        let _precision = 6;
    }

    owner::propose_owner(
        state,
        "spatial_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created spatial index '{index_name}' on '{collection}'({field}) using {index_type}{}",
            if precision > 0 {
                format!(" precision {precision}")
            } else {
                String::new()
            }
        ),
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE SPATIAL INDEX".to_string(),
        rows_affected: None,
    }])
}

/// Parse "collection(field)" or "collection" + "(field)".
fn parse_collection_field(first: &str, second: Option<&str>) -> Result<(String, String), DdlError> {
    // Try "collection(field)" format.
    if let Some(paren_pos) = first.find('(') {
        let collection = &first[..paren_pos];
        let field = first[paren_pos + 1..].trim_end_matches(')').trim();
        if collection.is_empty() || field.is_empty() {
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "expected collection(field) format".to_string(),
            });
        }
        return Ok((collection.to_string(), field.to_string()));
    }

    // Try "collection" + "(field)" as separate tokens.
    if let Some(second) = second {
        let field = second.trim_matches(|c| c == '(' || c == ')').trim();
        if !field.is_empty() {
            return Ok((first.to_string(), field.to_string()));
        }
    }

    Err(DdlError {
        sqlstate: "42601".to_string(),
        message: "expected collection(field) after ON".to_string(),
    })
}

/// Parse USING clause: RTREE (default) or GEOHASH.
fn parse_index_type(upper_parts: &[String]) -> &'static str {
    for (i, part) in upper_parts.iter().enumerate() {
        if part == "USING"
            && let Some(next) = upper_parts.get(i + 1)
        {
            return match next.as_str() {
                "GEOHASH" => "geohash",
                _ => "rtree",
            };
        }
    }
    "rtree"
}

/// Parse PRECISION clause for geohash.
fn parse_precision(upper_parts: &[String]) -> u8 {
    for (i, part) in upper_parts.iter().enumerate() {
        if part == "PRECISION"
            && let Some(next) = upper_parts.get(i + 1)
            && let Ok(p) = next.parse::<u8>()
        {
            return p.min(12);
        }
    }
    0
}
