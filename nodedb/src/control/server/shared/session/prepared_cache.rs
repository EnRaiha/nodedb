// SPDX-License-Identifier: BUSL-1.1

//! Per-session SQL-level prepared statement cache.
//!
//! Stores statements created via `PREPARE name(types) AS query`.
//! Separate from wire-level prepared statements managed by pgwire crate's
//! internal PortalStore — those are handled via Parse/Bind/Execute messages.

use std::collections::HashMap;
use std::net::SocketAddr;

use super::store::SessionStore;

/// A SQL-level prepared statement (via PREPARE name AS ...).
#[derive(Debug, Clone)]
pub struct SqlPreparedStatement {
    /// The original SQL with `$1`, `$2`, etc. placeholders.
    pub sql: String,
    /// Declared parameter type names (from `PREPARE name(type1, type2) AS ...`).
    /// Empty if no types declared (types inferred at EXECUTE time).
    pub param_type_names: Vec<String>,
}

/// Per-session cache for SQL-level prepared statements.
pub struct PreparedStatementCache {
    stmts: HashMap<String, SqlPreparedStatement>,
    max_statements: usize,
}

impl PreparedStatementCache {
    /// Create a new cache with a configurable max capacity.
    pub fn new(max_statements: usize) -> Self {
        Self {
            stmts: HashMap::new(),
            max_statements,
        }
    }

    /// Store a prepared statement. Returns error if at capacity.
    pub fn put(
        &mut self,
        name: String,
        stmt: SqlPreparedStatement,
    ) -> Result<(), PreparedCacheError> {
        // Replacing an existing statement is allowed (PG behavior).
        if !self.stmts.contains_key(&name) && self.stmts.len() >= self.max_statements {
            return Err(PreparedCacheError::AtCapacity {
                max: self.max_statements,
            });
        }
        self.stmts.insert(name, stmt);
        Ok(())
    }

    /// Get a prepared statement by name.
    pub fn get(&self, name: &str) -> Option<&SqlPreparedStatement> {
        self.stmts.get(name)
    }

    /// Remove a prepared statement by name. Returns whether it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.stmts.remove(name).is_some()
    }

    /// Remove all prepared statements.
    pub fn clear(&mut self) {
        self.stmts.clear();
    }

    /// Number of cached statements.
    pub fn len(&self) -> usize {
        self.stmts.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.stmts.is_empty()
    }

    /// List all prepared statement names.
    pub fn names(&self) -> Vec<String> {
        self.stmts.keys().cloned().collect()
    }
}

/// Errors from the prepared statement cache.
#[derive(Debug, Clone)]
pub enum PreparedCacheError {
    /// Cache is at maximum capacity.
    AtCapacity { max: usize },
    /// Statement not found.
    NotFound { name: String },
}

impl std::fmt::Display for PreparedCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreparedCacheError::AtCapacity { max } => {
                write!(
                    f,
                    "prepared statement limit reached ({max}); DEALLOCATE unused statements"
                )
            }
            PreparedCacheError::NotFound { name } => {
                write!(f, "prepared statement \"{name}\" does not exist")
            }
        }
    }
}

impl std::error::Error for PreparedCacheError {}

// ── SessionStore methods for SQL-level prepared statements ─────────

impl SessionStore {
    /// Store a SQL-level prepared statement in the session.
    pub fn prepare_sql_statement(
        &self,
        addr: &SocketAddr,
        name: String,
        stmt: SqlPreparedStatement,
    ) -> Result<(), PreparedCacheError> {
        self.write_session(addr, |session| session.prepared_stmts.put(name, stmt))
            .unwrap_or(Ok(()))
    }

    /// Get a SQL-level prepared statement from the session.
    pub fn get_sql_prepared(&self, addr: &SocketAddr, name: &str) -> Option<SqlPreparedStatement> {
        self.read_session(addr, |session| session.prepared_stmts.get(name).cloned())?
    }

    /// Remove a SQL-level prepared statement. Returns true if it existed.
    pub fn deallocate_sql_prepared(&self, addr: &SocketAddr, name: &str) -> bool {
        self.write_session(addr, |session| session.prepared_stmts.remove(name))
            .unwrap_or(false)
    }

    /// Remove all SQL-level prepared statements from the session.
    pub fn deallocate_all_sql_prepared(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| session.prepared_stmts.clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut cache = PreparedStatementCache::new(10);
        cache
            .put(
                "get_user".into(),
                SqlPreparedStatement {
                    sql: "SELECT * FROM users WHERE id = $1".into(),
                    param_type_names: vec!["BIGINT".into()],
                },
            )
            .unwrap();

        let stmt = cache.get("get_user").unwrap();
        assert_eq!(stmt.sql, "SELECT * FROM users WHERE id = $1");
        assert_eq!(stmt.param_type_names, vec!["BIGINT"]);
    }

    #[test]
    fn replace_existing() {
        let mut cache = PreparedStatementCache::new(10);
        cache
            .put(
                "q".into(),
                SqlPreparedStatement {
                    sql: "SELECT 1".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();
        cache
            .put(
                "q".into(),
                SqlPreparedStatement {
                    sql: "SELECT 2".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();
        assert_eq!(cache.get("q").unwrap().sql, "SELECT 2");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn capacity_enforcement() {
        let mut cache = PreparedStatementCache::new(2);
        cache
            .put(
                "a".into(),
                SqlPreparedStatement {
                    sql: "SELECT 1".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();
        cache
            .put(
                "b".into(),
                SqlPreparedStatement {
                    sql: "SELECT 2".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();

        let err = cache
            .put(
                "c".into(),
                SqlPreparedStatement {
                    sql: "SELECT 3".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap_err();
        assert!(matches!(err, PreparedCacheError::AtCapacity { max: 2 }));
    }

    #[test]
    fn deallocate_and_clear() {
        let mut cache = PreparedStatementCache::new(10);
        cache
            .put(
                "a".into(),
                SqlPreparedStatement {
                    sql: "SELECT 1".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();
        cache
            .put(
                "b".into(),
                SqlPreparedStatement {
                    sql: "SELECT 2".into(),
                    param_type_names: vec![],
                },
            )
            .unwrap();

        assert!(cache.remove("a"));
        assert!(!cache.remove("a"));
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
    }
}
