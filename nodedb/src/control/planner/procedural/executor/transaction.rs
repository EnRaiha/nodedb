// SPDX-License-Identifier: BUSL-1.1

//! Post-commit effects of one procedural transaction.
//!
//! A body's DML stages into an open system transaction at the statement
//! (see `control::system_txn::OpenSystemTxn`). Two effects cannot stage as
//! writes: trigger writes homed on another node, and `PUBLISH TO`. Both are
//! held here and commit as messages in the transaction's redo record, which
//! the Event Plane delivers once the COMMIT succeeds. ROLLBACK drops them,
//! and a savepoint rewinds them with the staged writes.

use crate::control::sql_dispatch::PreparedPublish;

/// Upper bound on the post-commit effects one transaction holds.
pub const MAX_POST_COMMIT_EFFECTS: usize = 1024;

/// One trigger statement homed on another node, held until the local commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteWrite {
    /// vShard the statement writes. The outbox sends the request to the
    /// vShard's leader at delivery.
    pub target_vshard: u32,
    /// The bound statement text, re-planned on the target node.
    pub sql: String,
}

/// Effects a committed transaction still owes, in statement order.
#[derive(Debug, Default)]
pub struct PostCommitEffects {
    pub remote: Vec<RemoteWrite>,
    pub publishes: Vec<PreparedPublish>,
}

impl PostCommitEffects {
    pub fn is_empty(&self) -> bool {
        self.remote.is_empty() && self.publishes.is_empty()
    }
}

/// Positions a savepoint rewinds the held effects to.
struct Savepoint {
    name: String,
    remote: usize,
    publishes: usize,
}

/// Post-commit effects held for the open transaction.
#[derive(Default)]
pub struct ProcedureTransactionCtx {
    effects: PostCommitEffects,
    /// Savepoint stack, innermost last.
    savepoints: Vec<Savepoint>,
}

fn over_limit() -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "a procedural transaction holds at most {MAX_POST_COMMIT_EFFECTS} cross-node \
             writes and PUBLISH statements; split the body or the source statement"
        ),
    }
}

impl ProcedureTransactionCtx {
    pub fn new() -> Self {
        Self::default()
    }

    fn held(&self) -> usize {
        self.effects.remote.len() + self.effects.publishes.len()
    }

    /// Hold a remote-homed write until the local COMMIT.
    pub fn buffer_remote(&mut self, write: RemoteWrite) -> crate::Result<()> {
        if self.held() >= MAX_POST_COMMIT_EFFECTS {
            return Err(over_limit());
        }
        self.effects.remote.push(write);
        Ok(())
    }

    /// Hold a checked `PUBLISH TO` until the local COMMIT.
    pub fn buffer_publish(&mut self, publish: PreparedPublish) -> crate::Result<()> {
        if self.held() >= MAX_POST_COMMIT_EFFECTS {
            return Err(over_limit());
        }
        self.effects.publishes.push(publish);
        Ok(())
    }

    /// Take the held effects (at COMMIT). Clears the savepoint stack.
    pub fn take_effects(&mut self) -> PostCommitEffects {
        self.savepoints.clear();
        std::mem::take(&mut self.effects)
    }

    /// Drop the held effects (at ROLLBACK). Clears the savepoint stack.
    pub fn rollback(&mut self) {
        self.effects = PostCommitEffects::default();
        self.savepoints.clear();
    }

    /// Record a savepoint at the current positions.
    pub fn savepoint(&mut self, name: &str) {
        // A redefined name moves to the new position.
        self.savepoints.retain(|sp| sp.name != name);
        self.savepoints.push(Savepoint {
            name: name.to_string(),
            remote: self.effects.remote.len(),
            publishes: self.effects.publishes.len(),
        });
    }

    /// Whether a savepoint named `name` exists.
    pub fn has_savepoint(&self, name: &str) -> bool {
        self.savepoints.iter().any(|sp| sp.name == name)
    }

    /// Drop the effects held after a savepoint, and every later savepoint.
    pub fn rollback_to(&mut self, name: &str) -> crate::Result<()> {
        let Some(idx) = self.savepoints.iter().rposition(|sp| sp.name == name) else {
            return Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            });
        };
        let (remote, publishes) = (self.savepoints[idx].remote, self.savepoints[idx].publishes);
        self.effects.remote.truncate(remote);
        self.effects.publishes.truncate(publishes);
        self.savepoints.truncate(idx + 1);
        Ok(())
    }

    /// Release a savepoint without rolling back.
    pub fn release_savepoint(&mut self, name: &str) -> crate::Result<()> {
        if !self.has_savepoint(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            });
        }
        self.savepoints.retain(|sp| sp.name != name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote(sql: &str) -> RemoteWrite {
        RemoteWrite {
            target_vshard: 7,
            sql: sql.into(),
        }
    }

    fn publish(payload: &str) -> PreparedPublish {
        PreparedPublish {
            database_id: 1,
            tenant_id: 1,
            topic: "events".into(),
            payload: payload.into(),
            metadata_floor: 0,
        }
    }

    #[test]
    fn rollback_drops_every_effect() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_remote(remote("a")).unwrap();
        ctx.buffer_publish(publish("p")).unwrap();
        ctx.rollback();
        assert!(ctx.take_effects().is_empty());
    }

    #[test]
    fn rollback_to_savepoint_truncates_effects() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_remote(remote("a")).unwrap();
        ctx.buffer_publish(publish("kept")).unwrap();
        ctx.savepoint("sp1");
        ctx.buffer_remote(remote("b")).unwrap();
        ctx.buffer_publish(publish("dropped")).unwrap();

        ctx.rollback_to("sp1").unwrap();
        let effects = ctx.take_effects();
        assert_eq!(effects.remote, vec![remote("a")]);
        assert_eq!(effects.publishes, vec![publish("kept")]);
    }

    #[test]
    fn nested_savepoints_rewind_in_order() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_remote(remote("a")).unwrap();
        ctx.savepoint("sp1");
        ctx.buffer_remote(remote("b")).unwrap();
        ctx.savepoint("sp2");
        ctx.buffer_remote(remote("c")).unwrap();

        ctx.rollback_to("sp2").unwrap();
        ctx.rollback_to("sp1").unwrap();
        assert_eq!(ctx.take_effects().remote, vec![remote("a")]);
    }

    #[test]
    fn released_savepoint_keeps_effects() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.savepoint("sp1");
        ctx.buffer_remote(remote("a")).unwrap();
        ctx.release_savepoint("sp1").unwrap();
        assert!(ctx.rollback_to("sp1").is_err());
        assert_eq!(ctx.take_effects().remote.len(), 1);
    }

    #[test]
    fn unknown_savepoint_is_refused() {
        let mut ctx = ProcedureTransactionCtx::new();
        assert!(ctx.rollback_to("nope").is_err());
        assert!(ctx.release_savepoint("nope").is_err());
    }

    #[test]
    fn effects_are_bounded() {
        let mut ctx = ProcedureTransactionCtx::new();
        for _ in 0..MAX_POST_COMMIT_EFFECTS {
            ctx.buffer_remote(remote("w")).unwrap();
        }
        assert!(ctx.buffer_remote(remote("over")).is_err());
        assert!(ctx.buffer_publish(publish("over")).is_err());
        assert_eq!(ctx.take_effects().remote.len(), MAX_POST_COMMIT_EFFECTS);
    }
}
