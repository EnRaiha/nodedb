// SPDX-License-Identifier: BUSL-1.1

//! DEFINE FIELD handler.
//!
//! Parses: DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>]
//!         [VALUE <expr>] [ASSERT <expr>] [READONLY]
//!
//! Stores the field definition in the catalog. Applied during writes (DEFAULT,
//! ASSERT, TYPE validation) and reads (VALUE computed fields).

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::types::FieldDefinition;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;
use super::sql_parse::extract_clause;

/// Keywords that delimit DEFINE FIELD clauses.
const FIELD_KEYWORDS: &[&str] = &["TYPE", "DEFAULT", "VALUE", "ASSERT", "READONLY"];

/// Parse and store a DEFINE FIELD statement.
pub fn define_field(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse: DEFINE FIELD <name> ON <collection> ...
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]",
        ));
    }

    let field_name = parts[2].to_lowercase();
    let collection = parts[4].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Parse optional clauses from the remaining SQL.
    let remainder = if sql.len() > parts[..5].iter().map(|p| p.len() + 1).sum::<usize>() {
        &sql[parts[..5].iter().map(|p| p.len() + 1).sum::<usize>()..]
    } else {
        ""
    };
    let upper_rem = remainder.to_uppercase();

    let field_type = extract_clause(&upper_rem, remainder, "TYPE", FIELD_KEYWORDS);
    let default_expr = extract_clause(&upper_rem, remainder, "DEFAULT", FIELD_KEYWORDS);
    let value_expr = extract_clause(&upper_rem, remainder, "VALUE", FIELD_KEYWORDS);
    let assert_expr = extract_clause(&upper_rem, remainder, "ASSERT", FIELD_KEYWORDS);
    let readonly = upper_rem.contains("READONLY");

    let def = FieldDefinition {
        name: field_name.clone(),
        field_type: field_type.unwrap_or_default(),
        default_expr: default_expr.unwrap_or_default(),
        value_expr: value_expr.unwrap_or_default(),
        assert_expr: assert_expr.unwrap_or_default(),
        readonly,
        sequence_name: None,
        is_generated: false,
        generated_deps: Vec::new(),
    };

    // Store in catalog.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection) {
            Ok(Some(mut coll)) => {
                // Remove existing definition for this field if any.
                coll.field_defs.retain(|f| f.name != field_name);

                // `fields` is the canonical schema-structure source read by
                // `catalog/schema.rs`, `maintenance/analyze.rs`,
                // `collection/describe.rs`, `collection/insert.rs`,
                // `create/register.rs`, and `ilp_listener.rs`. `field_defs`
                // carries field behavior (defaults, asserts, generated). Both
                // must stay in sync.
                let resolved_type = if def.field_type.is_empty() {
                    "any".to_string()
                } else {
                    def.field_type.clone()
                };
                coll.field_defs.push(def);

                if let Some(entry) = coll.fields.iter_mut().find(|(n, _)| n == &field_name) {
                    entry.1 = resolved_type;
                } else {
                    coll.fields.push((field_name.clone(), resolved_type));
                }

                if let Err(e) = catalog.put_collection(DatabaseId::DEFAULT, &coll) {
                    return Err(sqlstate_error("XX000", &format!("save collection: {e}")));
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("defined field '{field_name}' on '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEFINE FIELD"))])
}

/// Parse and store a DEFINE EVENT statement.
///
/// Syntax: DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>
pub fn define_event(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    use crate::control::security::catalog::types::EventDefinition;

    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>",
        ));
    }

    let event_name = parts[2].to_lowercase();
    let collection = parts[4].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Extract WHEN and THEN clauses using the shared keyword parser.
    let remainder = if sql.len() > parts[..5].iter().map(|p| p.len() + 1).sum::<usize>() {
        &sql[parts[..5].iter().map(|p| p.len() + 1).sum::<usize>()..]
    } else {
        ""
    };
    let upper_rem = remainder.to_uppercase();
    const EVENT_KEYWORDS: &[&str] = &["WHEN", "THEN"];

    let when_condition =
        extract_clause(&upper_rem, remainder, "WHEN", EVENT_KEYWORDS).unwrap_or_default();
    let then_action =
        extract_clause(&upper_rem, remainder, "THEN", EVENT_KEYWORDS).unwrap_or_default();

    if when_condition.is_empty() || then_action.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "DEFINE EVENT requires both WHEN and THEN clauses",
        ));
    }

    let def = EventDefinition {
        name: event_name.clone(),
        collection: collection.clone(),
        when_condition,
        then_action,
    };

    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection) {
            Ok(Some(mut coll)) => {
                coll.event_defs.retain(|e| e.name != event_name);
                coll.event_defs.push(def);
                if let Err(e) = catalog.put_collection(DatabaseId::DEFAULT, &coll) {
                    return Err(sqlstate_error("XX000", &format!("save collection: {e}")));
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("defined event '{event_name}' on '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEFINE EVENT"))])
}
