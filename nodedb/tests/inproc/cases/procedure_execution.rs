// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for stored procedure execution: transaction control,
//! SAVEPOINT, OUT parameters, exception handling, routability, parser changes.

use nodedb::control::planner::procedural::ast::*;
use nodedb::control::planner::procedural::executor::exception::exception_matches;
use nodedb::control::planner::procedural::executor::transaction::{
    ProcedureTransactionCtx, RemoteWrite,
};
use nodedb::control::planner::procedural::parse_block;
use nodedb::control::security::catalog::procedure_types::*;

// ---------------------------------------------------------------------------
// Parser: SAVEPOINT / ROLLBACK TO / RELEASE SAVEPOINT
// ---------------------------------------------------------------------------

#[test]
fn parse_savepoint() {
    let block = parse_block("BEGIN SAVEPOINT sp1; RETURN 1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::Savepoint { name } if name == "sp1"));
}

#[test]
fn parse_rollback_to_savepoint() {
    let block = parse_block("BEGIN ROLLBACK TO sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::RollbackTo { name } if name == "sp1"));
}

#[test]
fn parse_rollback_to_savepoint_keyword() {
    let block = parse_block("BEGIN ROLLBACK TO SAVEPOINT sp2; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::RollbackTo { name } if name == "sp2"));
}

#[test]
fn parse_release_savepoint() {
    let block = parse_block("BEGIN RELEASE SAVEPOINT sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::ReleaseSavepoint { name } if name == "sp1"));
}

#[test]
fn parse_release_without_savepoint_keyword() {
    let block = parse_block("BEGIN RELEASE sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::ReleaseSavepoint { name } if name == "sp1"));
}

#[test]
fn parse_commit_rollback_still_work() {
    let block = parse_block("BEGIN COMMIT; ROLLBACK; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::Commit));
    assert!(matches!(&block.statements[1], Statement::Rollback));
}

// ---------------------------------------------------------------------------
// Parser: EXCEPTION handlers
// ---------------------------------------------------------------------------

#[test]
fn parse_exception_others() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN OTHERS THEN \
             RETURN 0; \
         END",
    )
    .unwrap();
    assert_eq!(block.statements.len(), 1); // INSERT only
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Others
    );
    assert!(!block.exception_handlers[0].body.is_empty());
}

#[test]
fn parse_exception_sqlstate() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN SQLSTATE '23505' THEN \
             RETURN -1; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::SqlState("23505".into())
    );
}

#[test]
fn parse_exception_named() {
    let block = parse_block(
        "BEGIN \
           DELETE FROM t WHERE id = 1; \
         EXCEPTION \
           WHEN UNIQUE_VIOLATION THEN \
             RETURN -1; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Named("UNIQUE_VIOLATION".into())
    );
}

#[test]
fn parse_multiple_exception_handlers() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN UNIQUE_VIOLATION THEN \
             RETURN -1; \
           WHEN OTHERS THEN \
             RETURN -2; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 2);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Named("UNIQUE_VIOLATION".into())
    );
    assert_eq!(
        block.exception_handlers[1].condition,
        ExceptionCondition::Others
    );
}

#[test]
fn parse_no_exception_block() {
    let block = parse_block("BEGIN RETURN 42; END").unwrap();
    assert!(block.exception_handlers.is_empty());
}

// ---------------------------------------------------------------------------
// Transaction context: post-commit effects + savepoint + rollback
// ---------------------------------------------------------------------------

fn remote(sql: &str) -> RemoteWrite {
    RemoteWrite {
        target_vshard: 1,
        sql: sql.into(),
    }
}

#[test]
fn tx_ctx_take_empties_the_held_effects() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_remote(remote("a")).unwrap();
    ctx.buffer_remote(remote("b")).unwrap();
    ctx.buffer_remote(remote("c")).unwrap();
    assert_eq!(ctx.take_effects().remote.len(), 3);
    assert!(ctx.take_effects().is_empty());
}

#[test]
fn tx_ctx_rollback_discards_all() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_remote(remote("a")).unwrap();
    ctx.buffer_remote(remote("b")).unwrap();
    ctx.rollback();
    assert!(ctx.take_effects().is_empty());
}

#[test]
fn tx_ctx_savepoint_rollback_to() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_remote(remote("a")).unwrap();
    ctx.savepoint("sp1");
    ctx.buffer_remote(remote("b")).unwrap();
    ctx.buffer_remote(remote("c")).unwrap();
    ctx.rollback_to("sp1").unwrap();
    assert_eq!(ctx.take_effects().remote, vec![remote("a")]);
}

#[test]
fn tx_ctx_nested_savepoints() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_remote(remote("a")).unwrap();
    ctx.savepoint("sp1");
    ctx.buffer_remote(remote("b")).unwrap();
    ctx.savepoint("sp2");
    ctx.buffer_remote(remote("c")).unwrap();
    ctx.buffer_remote(remote("d")).unwrap();

    ctx.rollback_to("sp2").unwrap();
    assert_eq!(ctx.take_effects().remote.len(), 2);
}

#[test]
fn tx_ctx_release_savepoint() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_remote(remote("a")).unwrap();
    ctx.savepoint("sp1");
    ctx.buffer_remote(remote("b")).unwrap();
    ctx.release_savepoint("sp1").unwrap();

    assert!(ctx.rollback_to("sp1").is_err());
    assert_eq!(ctx.take_effects().remote.len(), 2);
}

#[test]
fn tx_ctx_rollback_to_nonexistent() {
    let mut ctx = ProcedureTransactionCtx::new();
    assert!(ctx.rollback_to("nope").is_err());
}

#[test]
fn tx_ctx_release_nonexistent() {
    let mut ctx = ProcedureTransactionCtx::new();
    assert!(ctx.release_savepoint("nope").is_err());
}

// ---------------------------------------------------------------------------
// Exception matching
// ---------------------------------------------------------------------------

#[test]
fn exception_others_matches_all() {
    assert!(exception_matches(
        &ExceptionCondition::Others,
        "any error at all"
    ));
    assert!(exception_matches(&ExceptionCondition::Others, ""));
}

#[test]
fn exception_sqlstate_match() {
    assert!(exception_matches(
        &ExceptionCondition::SqlState("23505".into()),
        "duplicate key violates unique constraint (23505)"
    ));
}

#[test]
fn exception_sqlstate_no_match() {
    assert!(!exception_matches(
        &ExceptionCondition::SqlState("23505".into()),
        "foreign key violation"
    ));
}

#[test]
fn exception_named_unique_violation() {
    let cond = ExceptionCondition::Named("UNIQUE_VIOLATION".into());
    assert!(exception_matches(&cond, "duplicate key error"));
    assert!(exception_matches(&cond, "UNIQUE constraint failed"));
    assert!(!exception_matches(&cond, "timeout exceeded"));
}

#[test]
fn exception_named_no_data_found() {
    let cond = ExceptionCondition::Named("NO_DATA_FOUND".into());
    assert!(exception_matches(&cond, "collection not found"));
    assert!(!exception_matches(&cond, "success"));
}

// ---------------------------------------------------------------------------
// Routability classification
// ---------------------------------------------------------------------------

#[test]
fn routability_default_is_multi_collection() {
    assert_eq!(
        ProcedureRoutability::default(),
        ProcedureRoutability::MultiCollection
    );
}

// ---------------------------------------------------------------------------
// Fuel metering
// ---------------------------------------------------------------------------

#[test]
fn execution_budget_exhaustion() {
    use nodedb::control::planner::procedural::executor::fuel::ExecutionBudget;

    let mut budget = ExecutionBudget::new(3, 60);
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_err()); // Exhausted
}

#[test]
fn execution_budget_trigger_default() {
    use nodedb::control::planner::procedural::executor::fuel::ExecutionBudget;

    let mut budget = ExecutionBudget::trigger_default();
    for _ in 0..1000 {
        assert!(budget.consume_iteration().is_ok());
    }
}
