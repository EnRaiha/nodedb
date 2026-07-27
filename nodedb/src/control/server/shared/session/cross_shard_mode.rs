// SPDX-License-Identifier: BUSL-1.1

//! Session-level `CrossShardTxnMode` — wire `SET` / `SHOW` for the
//! `cross_shard_txn` session parameter.
//!
//! Accepted values:
//!
//! - `'strict'`
//! - `'best_effort_non_atomic'`
//!
//! The longer name for the non-atomic mode is intentional — it acts as a
//! foot-gun guard. `'best_effort'` alone is explicitly rejected.

use super::connection::SessionId;

use super::store::SessionStore;
// The mode enum is a cross-shard *transaction* concept owned by the Calvin
// layer; this module only owns the pgwire `SET`/`SHOW` parsing/formatting and
// the session-store accessor for it.
pub use crate::control::planner::calvin::CrossShardTxnMode;

/// Session parameter key.
pub const PARAM_KEY: &str = "cross_shard_txn";

/// Parse a user-supplied string into a `CrossShardTxnMode`.
///
/// Returns `None` on unrecognised input (including `"best_effort"` without the
/// `_non_atomic` suffix) so the caller can return a helpful 22023 error.
pub fn parse_value(value: &str) -> Option<CrossShardTxnMode> {
    match value.trim().to_lowercase().as_str() {
        "strict" => Some(CrossShardTxnMode::Strict),
        "best_effort_non_atomic" => Some(CrossShardTxnMode::BestEffortNonAtomic),
        _ => None,
    }
}

/// Format a `CrossShardTxnMode` back into its canonical string form so that
/// `SHOW cross_shard_txn` returns something parseable.
pub fn format_value(mode: CrossShardTxnMode) -> &'static str {
    match mode {
        CrossShardTxnMode::Strict => "strict",
        CrossShardTxnMode::BestEffortNonAtomic => "best_effort_non_atomic",
    }
}

impl SessionStore {
    /// Resolve the effective `CrossShardTxnMode` for a session. Falls back to
    /// `Strict` if the parameter is unset or unparseable.
    pub fn cross_shard_txn_mode(&self, addr: impl Into<SessionId>) -> CrossShardTxnMode {
        self.get_parameter(addr, PARAM_KEY)
            .and_then(|v| parse_value(&v))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn parse_value_strict() {
        assert_eq!(parse_value("strict"), Some(CrossShardTxnMode::Strict));
        assert_eq!(parse_value("STRICT"), Some(CrossShardTxnMode::Strict));
        assert_eq!(parse_value("  strict  "), Some(CrossShardTxnMode::Strict));
    }

    #[test]
    fn parse_value_best_effort_non_atomic() {
        assert_eq!(
            parse_value("best_effort_non_atomic"),
            Some(CrossShardTxnMode::BestEffortNonAtomic)
        );
        assert_eq!(
            parse_value("BEST_EFFORT_NON_ATOMIC"),
            Some(CrossShardTxnMode::BestEffortNonAtomic)
        );
    }

    #[test]
    fn parse_value_rejects_bare_best_effort() {
        // The foot-gun guard: bare "best_effort" must be rejected.
        assert_eq!(parse_value("best_effort"), None);
    }

    #[test]
    fn parse_value_rejects_invalid() {
        assert_eq!(parse_value(""), None);
        assert_eq!(parse_value("foobar"), None);
        assert_eq!(parse_value("non_atomic"), None);
    }

    #[test]
    fn default_is_strict() {
        assert_eq!(CrossShardTxnMode::default(), CrossShardTxnMode::Strict);
    }

    #[test]
    fn session_store_defaults_to_strict() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
        store.ensure_session(addr);
        assert_eq!(store.cross_shard_txn_mode(addr), CrossShardTxnMode::Strict);
    }

    #[test]
    fn session_store_reads_set_value() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
        store.ensure_session(addr);
        store.set_parameter(
            addr,
            PARAM_KEY.to_string(),
            "best_effort_non_atomic".to_string(),
        );
        assert_eq!(
            store.cross_shard_txn_mode(addr),
            CrossShardTxnMode::BestEffortNonAtomic
        );
    }
}
