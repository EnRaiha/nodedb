// SPDX-License-Identifier: BUSL-1.1

//! Catalog name→OID resolution, the evaluation context, and evaluation of
//! cast / scalar-function / LIKE expressions.

use std::collections::HashMap;

use super::super::value::VValue;
use super::types::{CastType, EvalError, ScalarFn};

/// Resolves relation and type names to their catalog OIDs for `::regclass` and
/// `::regtype` casts. Built once per query from the catalog snapshot.
#[derive(Debug, Default, Clone)]
pub struct CatalogResolver {
    rel_oids: HashMap<String, i64>,
    type_oids: HashMap<String, i64>,
}

impl CatalogResolver {
    pub fn new(rel_oids: HashMap<String, i64>, type_oids: HashMap<String, i64>) -> Self {
        Self {
            rel_oids,
            type_oids,
        }
    }

    /// Resolve a relation name (optionally schema-qualified) to its OID.
    pub fn resolve_regclass(&self, name: &str) -> Result<i64, EvalError> {
        let key = strip_schema(name).to_ascii_lowercase();
        self.rel_oids
            .get(&key)
            .copied()
            .ok_or_else(|| EvalError::UndefinedTable(name.to_string()))
    }

    /// Resolve a type name to its OID.
    pub fn resolve_regtype(&self, name: &str) -> Result<i64, EvalError> {
        let key = strip_schema(name).to_ascii_lowercase();
        self.type_oids
            .get(&key)
            .copied()
            .ok_or_else(|| EvalError::UndefinedType(name.to_string()))
    }
}

fn strip_schema(name: &str) -> &str {
    // `pg_catalog.pg_class` / `public.users` → trailing component.
    name.rsplit('.').next().unwrap_or(name).trim()
}

/// Session/catalog context threaded through evaluation.
pub struct EvalCtx<'a> {
    pub resolver: &'a CatalogResolver,
    pub username: &'a str,
    pub database: &'a str,
    /// Explicit search-path schemas, in order (e.g. `["public"]`).
    pub search_path: &'a [String],
}

/// Evaluate a cast of an already-evaluated operand to `target`.
pub fn eval_cast(value: VValue, target: CastType, ctx: &EvalCtx) -> Result<VValue, EvalError> {
    if value.is_null() {
        return Ok(VValue::Null);
    }
    match target {
        CastType::Regclass => {
            if let Some(oid) = value.as_i64() {
                return Ok(VValue::Int8(oid));
            }
            let name = value
                .as_text()
                .ok_or_else(|| EvalError::TypeMismatch("::regclass requires a name".into()))?;
            Ok(VValue::Int8(ctx.resolver.resolve_regclass(name)?))
        }
        CastType::Regtype => {
            if let Some(oid) = value.as_i64() {
                return Ok(VValue::Int8(oid));
            }
            let name = value
                .as_text()
                .ok_or_else(|| EvalError::TypeMismatch("::regtype requires a name".into()))?;
            Ok(VValue::Int8(ctx.resolver.resolve_regtype(name)?))
        }
        CastType::Oid | CastType::Int8 => Ok(VValue::Int8(coerce_i64(&value)?)),
        CastType::Int4 => Ok(VValue::Int4(coerce_i64(&value)? as i32)),
        CastType::Text => Ok(VValue::Text(value.to_pg_text().unwrap_or_default())),
        CastType::Bool => match &value {
            VValue::Bool(b) => Ok(VValue::Bool(*b)),
            VValue::Text(s) => parse_bool(s),
            VValue::Int4(i) => Ok(VValue::Bool(*i != 0)),
            VValue::Int8(i) => Ok(VValue::Bool(*i != 0)),
            _ => Err(EvalError::TypeMismatch("invalid cast to bool".into())),
        },
    }
}

fn coerce_i64(v: &VValue) -> Result<i64, EvalError> {
    if let Some(i) = v.as_i64() {
        return Ok(i);
    }
    if let Some(s) = v.as_text() {
        return s
            .trim()
            .parse::<i64>()
            .map_err(|_| EvalError::TypeMismatch(format!("invalid integer cast: {s:?}")));
    }
    Err(EvalError::TypeMismatch("invalid integer cast".into()))
}

fn parse_bool(s: &str) -> Result<VValue, EvalError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "yes" | "on" | "1" => Ok(VValue::Bool(true)),
        "f" | "false" | "no" | "off" | "0" => Ok(VValue::Bool(false)),
        _ => Err(EvalError::TypeMismatch(format!(
            "invalid bool literal: {s:?}"
        ))),
    }
}

/// Evaluate a catalog scalar function over its already-evaluated arguments.
pub fn eval_scalar_fn(func: ScalarFn, args: &[VValue], ctx: &EvalCtx) -> Result<VValue, EvalError> {
    match func {
        ScalarFn::CurrentSchemas => {
            // current_schemas(include_implicit boolean) -> name[]
            let include_implicit = match args.first() {
                Some(VValue::Bool(b)) => *b,
                Some(VValue::Null) | None => false,
                Some(other) => {
                    return Err(EvalError::InvalidArgument(format!(
                        "current_schemas expects a boolean, got {other:?}"
                    )));
                }
            };
            let mut schemas: Vec<VValue> = Vec::new();
            if include_implicit {
                schemas.push(VValue::Text("pg_catalog".into()));
            }
            for s in ctx.search_path {
                schemas.push(VValue::Text(s.clone()));
            }
            Ok(VValue::Array(schemas))
        }
        ScalarFn::CurrentSchema => Ok(ctx
            .search_path
            .first()
            .map(|s| VValue::Text(s.clone()))
            .unwrap_or(VValue::Null)),
        ScalarFn::CurrentDatabase => Ok(VValue::Text(ctx.database.to_string())),
        ScalarFn::CurrentUser | ScalarFn::CurrentRole => Ok(VValue::Text(ctx.username.to_string())),
        ScalarFn::Version => Ok(VValue::Text(format!(
            "PostgreSQL 16.0 (NodeDB {}) on {}",
            crate::version::VERSION,
            std::env::consts::ARCH
        ))),
    }
}

/// SQL `LIKE` with `%` (any string) and `_` (any single char).
pub fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_recursive(&s_chars, &p_chars)
}

fn like_match_recursive(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }
    match p[0] {
        '%' => {
            let mut i = 1;
            while i < p.len() && p[i] == '%' {
                i += 1;
            }
            let rest = &p[i..];
            if rest.is_empty() {
                return true;
            }
            (0..=s.len()).any(|k| like_match_recursive(&s[k..], rest))
        }
        '_' => !s.is_empty() && like_match_recursive(&s[1..], &p[1..]),
        c => !s.is_empty() && s[0] == c && like_match_recursive(&s[1..], &p[1..]),
    }
}
