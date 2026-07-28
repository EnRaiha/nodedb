// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SPARSE INDEX` DSL handler.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::options::{ColumnMode, HeaderSpec, NameMode, parse_index_statement};

const CONTEXT: &str = "CREATE SPARSE INDEX";
const LEADING: &[&str] = &["CREATE", "SPARSE", "INDEX"];

const SYNTAX: &str = "CREATE SPARSE INDEX [IF NOT EXISTS] [<name>] ON <collection> [(<field>)]";

const HEADER: HeaderSpec = HeaderSpec {
    name: NameMode::Optional {
        fallback: "_auto_sparse",
    },
    columns: ColumnMode::AtMostOne,
    syntax: SYNTAX,
};

/// The field a sparse index covers when the statement names none.
const DEFAULT_FIELD: &str = "_sparse";

/// `CREATE SPARSE INDEX [IF NOT EXISTS] [<name>] ON <collection> [(<field>)]`
pub fn create_sparse_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // This surface carries no options, so any trailing token is a statement
    // the handler does not implement rather than one it may ignore.
    let stmt = parse_index_statement(sql, LEADING, &HEADER, &[], CONTEXT)?;

    let index_name = &stmt.header.name;
    let collection = &stmt.header.collection;
    let field = match stmt.header.column() {
        "" => DEFAULT_FIELD,
        named => named,
    };
    let tenant_id = identity.tenant_id;

    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        "sparse_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created sparse index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![DdlResult::Status {
        command: CONTEXT.to_string(),
        rows_affected: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::super::options::IndexStatement;
    use super::*;

    fn parse(sql: &str) -> Result<IndexStatement, DdlError> {
        parse_index_statement(sql, LEADING, &HEADER, &[], CONTEXT)
    }

    #[test]
    fn name_is_optional() {
        assert_eq!(
            parse("CREATE SPARSE INDEX ON docs (terms)")
                .unwrap()
                .header
                .name,
            "_auto_sparse"
        );
        assert_eq!(
            parse("CREATE SPARSE INDEX idx ON docs (terms)")
                .unwrap()
                .header
                .name,
            "idx"
        );
    }

    #[test]
    fn field_is_optional() {
        assert_eq!(
            parse("CREATE SPARSE INDEX ON docs")
                .unwrap()
                .header
                .column(),
            ""
        );
    }

    #[test]
    fn unrecognized_trailing_tokens_are_rejected() {
        assert!(parse("CREATE SPARSE INDEX ON docs (terms) USING SOMETHING").is_err());
    }
}
