// SPDX-License-Identifier: Apache-2.0

//! Parse OIDC provider DDL statements.
//!
//! Syntax:
//!
//! ```sql
//! CREATE OIDC PROVIDER <name>
//!     ISSUER '<iss>'
//!     JWKS_URI '<uri>'
//!     TENANT <u64>
//!     [AUDIENCE '<aud>']
//!     [CLAIM MAPPING WHEN <claim_name> = '<value>'
//!         [SET DEFAULT_DATABASE = <id>]
//!         [ADD DATABASES [<id>, ...]]
//!         [ADD ROLES ['<role>', ...]]
//!     ...]
//!
//! ALTER OIDC PROVIDER <name>
//!     SET CLAIM MAPPING WHEN <claim_name> = '<value>'
//!         [SET DEFAULT_DATABASE = <id>]
//!         [ADD DATABASES [<id>, ...]]
//!         [ADD ROLES ['<role>', ...]]
//!     [...]
//!
//! DROP OIDC PROVIDER [IF EXISTS] <name>
//!
//! SHOW OIDC PROVIDERS
//! ```
//!
//! Provider clauses come in any order, each at most once, before `CLAIM
//! MAPPING`. A token at a clause position that names no clause is a parse
//! error, so a misspelled clause never falls back to its default.

use super::clause::{ClauseCursor, Tok, tokenize};
use crate::ddl_ast::statement::{AuthStmt, NodedbStatement, OidcClaimMappingClause};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    let known = [
        "CREATE OIDC PROVIDER ",
        "ALTER OIDC PROVIDER ",
        "DROP OIDC PROVIDER ",
        "SHOW OIDC PROVIDERS",
    ];
    if !known.iter().any(|p| upper.starts_with(p)) {
        return None;
    }
    Some(parse(trimmed))
}

fn parse(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "OIDC PROVIDER");
    if c.eat_keywords(&["CREATE", "OIDC", "PROVIDER"]) {
        return parse_create(&mut c);
    }
    if c.eat_keywords(&["ALTER", "OIDC", "PROVIDER"]) {
        let name = c.raw("provider name")?.to_string();
        c.expect_keywords(&["SET", "CLAIM", "MAPPING"])?;
        let claim_mappings = parse_claim_mappings(&mut c)?;
        if claim_mappings.is_empty() {
            return Err(c.error("SET CLAIM MAPPING needs at least one WHEN clause"));
        }
        c.finish()?;
        return Ok(NodedbStatement::Auth(
            AuthStmt::AlterOidcProviderClaimMapping {
                name,
                claim_mappings,
            },
        ));
    }
    if c.eat_keywords(&["DROP", "OIDC", "PROVIDER"]) {
        let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
        let name = c.raw("provider name")?.to_string();
        c.finish()?;
        return Ok(NodedbStatement::Auth(AuthStmt::DropOidcProvider {
            name,
            if_exists,
        }));
    }
    c.expect_keywords(&["SHOW", "OIDC", "PROVIDERS"])?;
    c.finish()?;
    Ok(NodedbStatement::Auth(AuthStmt::ShowOidcProviders))
}

// ── CREATE ─────────────────────────────────────────────────────────────────

fn parse_create(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    let name = c.raw("provider name")?.to_string();
    let mut issuer = None;
    let mut jwks_uri = None;
    let mut tenant_id = None;
    let mut audience = None;
    let mut claim_mappings = Vec::new();

    while !c.at_end() {
        if c.eat_keyword("ISSUER") {
            c.clause("ISSUER")?;
            issuer = Some(quoted_value(c, "ISSUER must be a quoted string")?);
        } else if c.eat_keyword("JWKS_URI") {
            c.clause("JWKS_URI")?;
            jwks_uri = Some(quoted_value(c, "JWKS_URI must be a quoted string")?);
        } else if c.eat_keyword("TENANT") {
            c.clause("TENANT")?;
            tenant_id = Some(
                c.u64("TENANT")
                    .map_err(|_| c.error("TENANT must be a u64"))?,
            );
        } else if c.eat_keyword("AUDIENCE") {
            c.clause("AUDIENCE")?;
            audience = Some(quoted_value(c, "AUDIENCE must be a quoted string")?);
        } else if c.eat_keywords(&["CLAIM", "MAPPING"]) {
            claim_mappings = parse_claim_mappings(c)?;
            if claim_mappings.is_empty() {
                return Err(c.error("CLAIM MAPPING needs at least one WHEN clause"));
            }
            break;
        } else {
            return Err(c.unknown_clause());
        }
    }
    c.finish()?;

    let issuer = issuer.ok_or_else(|| c.error("ISSUER '<url>' is required"))?;
    let jwks_uri = jwks_uri.ok_or_else(|| c.error("JWKS_URI '<url>' is required"))?;
    let tenant_id = tenant_id.ok_or_else(|| c.error("TENANT <u64> is required"))?;

    Ok(NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
        name,
        issuer,
        jwks_uri,
        tenant_id,
        audience,
        claim_mappings,
    }))
}

/// A single- or double-quoted string value.
fn quoted_value(c: &mut ClauseCursor<'_, '_>, detail: &str) -> Result<String, SqlError> {
    if let Ok(value) = c.string(detail) {
        return Ok(value);
    }
    let tokens = c.remaining_tokens();
    match tokens.first().map(|t| &t.tok) {
        Some(Tok::Ident(_)) => {
            // `"..."` is accepted as a string value here.
            let raw = c.raw(detail)?;
            Ok(raw[1..raw.len() - 1].replace("\"\"", "\""))
        }
        _ => Err(c.error(detail)),
    }
}

// ── Claim-mapping parser ────────────────────────────────────────────────────

/// Read zero or more `WHEN <claim_name> = '<value>' [actions]` clauses.
fn parse_claim_mappings(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<Vec<OidcClaimMappingClause>, SqlError> {
    let mut clauses = Vec::new();
    while c.eat_keyword("WHEN") {
        clauses.push(parse_when_clause(c)?);
    }
    Ok(clauses)
}

/// Read `<claim_name> = '<value>' [SET DEFAULT_DATABASE = <id>]
/// [ADD DATABASES [...]] [ADD ROLES [...]]` after `WHEN`. Each action at most
/// once; an unknown action is a parse error.
fn parse_when_clause(c: &mut ClauseCursor<'_, '_>) -> Result<OidcClaimMappingClause, SqlError> {
    let syntax = "CLAIM MAPPING: syntax is WHEN <claim> = '<value>'";
    let claim_name = c.name("claim name").map_err(|_| c.error(syntax))?;
    if !c.eat_punct('=') {
        return Err(c.error(syntax));
    }
    let claim_value = quoted_value(c, syntax)?;

    let mut default_database = None;
    let mut add_databases = None;
    let mut add_roles = None;
    while !c.at_end() && !c.peek_keyword("WHEN") {
        if c.eat_keywords(&["SET", "DEFAULT_DATABASE"]) {
            if default_database.is_some() {
                return Err(c.error("SET DEFAULT_DATABASE given twice in one WHEN clause"));
            }
            c.expect_punct('=')?;
            default_database = Some(c.u64("DEFAULT_DATABASE")?);
        } else if c.eat_keywords(&["ADD", "DATABASES"]) {
            if add_databases.is_some() {
                return Err(c.error("ADD DATABASES given twice in one WHEN clause"));
            }
            add_databases = Some(bracket_list(c, "ADD DATABASES", |c| c.u64("database id"))?);
        } else if c.eat_keywords(&["ADD", "ROLES"]) {
            if add_roles.is_some() {
                return Err(c.error("ADD ROLES given twice in one WHEN clause"));
            }
            add_roles = Some(bracket_list(c, "ADD ROLES", |c| {
                quoted_value(c, "ADD ROLES: a role must be a quoted string")
            })?);
        } else {
            return Err(c.error(format!(
                "unknown CLAIM MAPPING action {} (expected SET DEFAULT_DATABASE = <id> | \
                 ADD DATABASES [...] | ADD ROLES [...])",
                c.describe_next()
            )));
        }
    }

    Ok(OidcClaimMappingClause {
        claim_name,
        claim_value,
        default_database,
        add_databases: add_databases.unwrap_or_default(),
        add_roles: add_roles.unwrap_or_default(),
    })
}

/// `[ item , item ... ]`, possibly empty.
fn bracket_list<T>(
    c: &mut ClauseCursor<'_, '_>,
    what: &str,
    mut item: impl FnMut(&mut ClauseCursor<'_, '_>) -> Result<T, SqlError>,
) -> Result<Vec<T>, SqlError> {
    if !c.eat_punct('[') {
        return Err(c.error(format!("{what} needs a [ ... ] list")));
    }
    let mut out = Vec::new();
    if c.eat_punct(']') {
        return Ok(out);
    }
    loop {
        out.push(item(c)?);
        if c.eat_punct(',') {
            continue;
        }
        if c.eat_punct(']') {
            return Ok(out);
        }
        return Err(c.error(format!("{what}: missing closing ']'")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::NodedbStatement;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
    }

    fn ok(sql: &str) -> NodedbStatement {
        try_sql(sql).expect("expected Some").expect("expected Ok")
    }

    fn is_parse_err(sql: &str) -> bool {
        matches!(try_sql(sql), Some(Err(SqlError::Parse { .. })))
    }

    #[test]
    fn claim_mapping_boundaries_after_unicode_values_preserve_original_offsets() {
        let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            claim_mappings, ..
        }) = ok("ALTER OIDC PROVIDER p SET CLAIM MAPPING \
                 WHEN sub = 'ﬀﬀ' ADD ROLES ['readonly'] WHEN aud = 'api' ADD DATABASES [7]")
        else {
            panic!("expected claim mapping");
        };
        assert_eq!(claim_mappings.len(), 2);
        assert_eq!(claim_mappings[0].add_roles, vec!["readonly"]);
        assert_eq!(claim_mappings[1].add_databases, vec![7]);
    }

    #[test]
    fn show_oidc_providers() {
        assert_eq!(
            ok("SHOW OIDC PROVIDERS"),
            NodedbStatement::Auth(AuthStmt::ShowOidcProviders)
        );
    }

    #[test]
    fn oidc_statements_accept_one_trailing_terminator() {
        assert_eq!(
            ok("SHOW OIDC PROVIDERS;"),
            NodedbStatement::Auth(AuthStmt::ShowOidcProviders)
        );
        assert_eq!(
            ok("DROP OIDC PROVIDER IF EXISTS myidp;"),
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: true,
            })
        );
        match ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1;",
        ) {
            NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                claim_mappings,
                ..
            }) => {
                assert_eq!(claim_mappings[0].default_database, Some(1));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn drop_oidc_provider() {
        let stmt = ok("DROP OIDC PROVIDER myidp");
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_oidc_provider_if_exists() {
        let stmt = ok("DROP OIDC PROVIDER IF EXISTS myidp");
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn create_oidc_provider_requires_numeric_tenant() {
        for sql in [
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/jwks'",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/jwks' TENANT acme",
        ] {
            assert!(
                is_parse_err(sql),
                "CREATE OIDC PROVIDER must require a numeric TENANT: {sql}"
            );
        }
    }

    #[test]
    fn create_oidc_provider_with_tenant_terminator() {
        let sql = "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/.well-known/jwks.json' TENANT 42;";
        assert_eq!(
            ok(sql),
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                name: "auth0".to_string(),
                issuer: "https://auth0.example.com".to_string(),
                jwks_uri: "https://auth0.example.com/.well-known/jwks.json".to_string(),
                tenant_id: 42,
                audience: None,
                claim_mappings: vec![],
            })
        );
    }

    #[test]
    fn provider_name_clause_keywords_do_not_supply_clauses() {
        for name in ["ISSUER", "JWKS_URI", "TENANT", "AUDIENCE"] {
            let stmt = ok(&format!(
                "CREATE OIDC PROVIDER {name} ISSUER 'https://idp.example.com' \
                 JWKS_URI 'https://idp.example.com/jwks' AUDIENCE 'nodedb-api' TENANT 42 \
                 CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
            ));
            match stmt {
                NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                    name: parsed_name,
                    issuer,
                    jwks_uri,
                    tenant_id,
                    audience,
                    claim_mappings,
                }) => {
                    assert_eq!(parsed_name, name);
                    assert_eq!(issuer, "https://idp.example.com");
                    assert_eq!(jwks_uri, "https://idp.example.com/jwks");
                    assert_eq!(tenant_id, 42);
                    assert_eq!(audience.as_deref(), Some("nodedb-api"));
                    assert_eq!(claim_mappings.len(), 1);
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn create_oidc_provider_with_audience_terminator() {
        let sql = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' TENANT 42 AUDIENCE 'nodedb-api';";
        match ok(sql) {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                audience,
                tenant_id,
                ..
            }) => {
                assert_eq!(audience, Some("nodedb-api".to_string()));
                assert_eq!(tenant_id, 42);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn claim_mappings_cannot_supply_provider_clauses() {
        let missing_issuer = "CREATE OIDC PROVIDER auth0 JWKS_URI 'https://idp.example.com/jwks' TENANT 42 \
            CLAIM MAPPING WHEN issuer = 'https://idp.example.com'";
        let missing_jwks = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' TENANT 42 \
            CLAIM MAPPING WHEN jwks_uri = 'https://idp.example.com/jwks'";
        let missing_tenant = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' \
            JWKS_URI 'https://idp.example.com/jwks' CLAIM MAPPING WHEN tenant = '42'";
        for sql in [missing_issuer, missing_jwks, missing_tenant] {
            assert!(is_parse_err(sql), "{sql}");
        }
    }

    #[test]
    fn claim_audience_does_not_set_provider_audience() {
        let stmt = ok(
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' \
             JWKS_URI 'https://idp.example.com/jwks' TENANT 42 \
             CLAIM MAPPING WHEN audience = 'nodedb-api'",
        );
        match stmt {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider { audience, .. }) => {
                assert_eq!(audience, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn create_oidc_provider_requires_quoted_provider_strings() {
        for sql in [
            "CREATE OIDC PROVIDER auth0 ISSUER = 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER https://idp.example.com JWKS_URI 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI = 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' AUDIENCE = 'nodedb-api' TENANT 42",
        ] {
            assert!(is_parse_err(sql), "{sql}");
        }
    }

    #[test]
    fn misspelled_audience_is_refused() {
        assert!(is_parse_err(
            "CREATE OIDC PROVIDER p ISSUER 'i' JWKS_URI 'j' TENANT 1 AUDIANCE 'api'"
        ));
    }

    #[test]
    fn create_oidc_provider_with_claim_mapping() {
        let sql = "CREATE OIDC PROVIDER corp ISSUER 'https://sso.corp.com' JWKS_URI 'https://sso.corp.com/jwks' \
             AUDIENCE 'nodedb' TENANT 42 \
             CLAIM MAPPING WHEN org_id = 'acme' SET DEFAULT_DATABASE = 42 ADD DATABASES [43, 44] ADD ROLES ['readwrite'];";
        match ok(sql) {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                name,
                claim_mappings,
                tenant_id,
                ..
            }) => {
                assert_eq!(name, "corp");
                assert_eq!(tenant_id, 42);
                assert_eq!(claim_mappings.len(), 1);
                let cm = &claim_mappings[0];
                assert_eq!(cm.claim_name, "org_id");
                assert_eq!(cm.claim_value, "acme");
                assert_eq!(cm.default_database, Some(42));
                assert_eq!(cm.add_databases, vec![43, 44]);
                assert_eq!(cm.add_roles, vec!["readwrite"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn alter_oidc_provider_claim_mapping() {
        let sql = "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN sub = '*' ADD ROLES ['admin']";
        match ok(sql) {
            NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                name,
                claim_mappings,
            }) => {
                assert_eq!(name, "auth0");
                assert_eq!(claim_mappings.len(), 1);
                assert_eq!(claim_mappings[0].claim_value, "*");
                assert_eq!(claim_mappings[0].add_roles, vec!["admin"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn alter_requires_set_claim_mapping() {
        assert!(is_parse_err(
            "ALTER OIDC PROVIDER p DROP CLAIM MAPPING WHEN sub = '*'"
        ));
    }

    #[test]
    fn unknown_claim_action_is_refused() {
        assert!(is_parse_err(
            "ALTER OIDC PROVIDER p SET CLAIM MAPPING WHEN groups = 'eng' ADD ROLE ['admin']"
        ));
    }

    #[test]
    fn claim_mapping_values_preserve_spaces_and_doubled_quotes() {
        let single_quoted = ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'platform engineering' \
             ADD ROLES ['reader']",
        );
        let double_quoted = ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN title = \"Director of \"\"Data\"\"\" \
             SET DEFAULT_DATABASE = 7",
        );

        let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            claim_mappings, ..
        }) = single_quoted
        else {
            panic!("expected ALTER OIDC PROVIDER claim mapping");
        };
        assert_eq!(claim_mappings[0].claim_value, "platform engineering");
        assert_eq!(claim_mappings[0].add_roles, vec!["reader"]);

        let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            claim_mappings, ..
        }) = double_quoted
        else {
            panic!("expected ALTER OIDC PROVIDER claim mapping");
        };
        assert_eq!(claim_mappings[0].claim_value, "Director of \"Data\"");
        assert_eq!(claim_mappings[0].default_database, Some(7));
    }

    #[test]
    fn claim_mapping_rejects_unquoted_and_malformed_values() {
        for sql in [
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = engineering",
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'platform engineering",
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'engineering'junk",
        ] {
            assert!(is_parse_err(sql), "{sql}");
        }
    }

    #[test]
    fn claim_mapping_actions_ignore_keyword_like_claim_names_and_values() {
        let absent = ok("ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN roles = \
             'SET DEFAULT_DATABASE ADD DATABASES ADD ROLES'");
        let actual = ok("ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN add = \
             'DEFAULT_DATABASE DATABASES ROLES SET ADD it''s ADD ROLES' \
             SET DEFAULT_DATABASE = 7 ADD DATABASES [8, 9] ADD ROLES ['reader']");

        for statement in [absent, actual] {
            let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                claim_mappings,
                ..
            }) = statement
            else {
                panic!("expected ALTER OIDC PROVIDER claim mapping");
            };
            let mapping = &claim_mappings[0];
            if mapping.claim_name == "roles" {
                assert_eq!(mapping.default_database, None);
                assert!(mapping.add_databases.is_empty());
                assert!(mapping.add_roles.is_empty());
            } else {
                assert_eq!(mapping.claim_name, "add");
                assert_eq!(mapping.default_database, Some(7));
                assert_eq!(mapping.add_databases, vec![8, 9]);
                assert_eq!(mapping.add_roles, vec!["reader"]);
            }
        }
    }
}
