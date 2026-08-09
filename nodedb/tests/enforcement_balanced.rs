// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the BALANCED constraint.
//!
//! BALANCED is a write-BOUNDARY predicate: for each `group_key`, the debits and
//! credits a boundary leaves behind must sum equal. A transaction is a
//! boundary, and an autocommit statement IS a transaction — so a statement that
//! writes one leg of a journal on its own is unbalanced by the definition and
//! is refused. A balanced ledger cannot be populated one leg at a time.
//!
//! Every mutation contributes with the sign of its effect on the stored set: an
//! insert adds its post-image, a delete SUBTRACTS its pre-image, an update does
//! both. The negative half is what makes "delete one leg of a balanced journal"
//! a violation rather than a no-op.
//!
//! Both halves are pinned here. A guard that refused everything would pass the
//! rejection tests, so every write shape also has a control that MUST commit.

mod common;

use common::pgwire_harness::TestServer;

/// `CREATE COLLECTION` for a ledger carrying the constraint.
///
/// The clause is parsed from the uppercased statement, so the debit/credit
/// markers are compared against the rows as `DEBIT` / `CREDIT`.
fn create_ledger(name: &str) -> String {
    format!(
        "CREATE COLLECTION {name} WITH BALANCED ON \
         (group_key = journal_id, debit = 'DEBIT', credit = 'CREDIT', amount = amount)"
    )
}

/// A single journal line as a VALUES tuple.
fn leg(id: &str, journal: &str, entry_type: &str, amount: i64) -> String {
    format!("('{id}', '{journal}', '{entry_type}', {amount})")
}

const COLUMNS: &str = "(id, journal_id, entry_type, amount)";

async fn ledger_ids(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {name} ORDER BY id"))
        .await
        .unwrap()
}

fn assert_balance_violation(result: &Result<(), String>) {
    let message = match result {
        Ok(()) => panic!("expected the write to be refused as a balance violation"),
        Err(message) => message,
    };
    assert!(
        message.to_lowercase().contains("balance"),
        "expected a balance violation, got: {message}"
    );
}

// ── DDL-time validation of the declaration itself ──

/// A BALANCED declaration naming a column the collection's DECLARED schema does
/// not have — or one whose declared type the commit-time check cannot read — is
/// refused at DDL time.
///
/// This has to stay closed: such a declaration was silently unenforced, because
/// every row failed to parse into an entry and the check then ran over zero
/// entries and passed. The schemaless spelling above (no column list at all)
/// declares no schema to check against, which is why it is accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balanced_naming_an_unusable_declared_column_is_refused() {
    let server = TestServer::start().await;

    let missing = server
        .exec(
            "CREATE COLLECTION bal_ddl_missing \
             (id TEXT, journal_id TEXT, entry_type TEXT, amount NUMERIC) \
             WITH BALANCED ON (group_key = no_such_column, debit = 'DEBIT', \
             credit = 'CREDIT', amount = amount)",
        )
        .await;
    let message = missing.expect_err("a BALANCED column that is not declared must be refused");
    assert!(
        message.contains("42703"),
        "an undeclared BALANCED column must report SQLSTATE 42703: {message}"
    );

    let non_text = server
        .exec(
            "CREATE COLLECTION bal_ddl_nontext \
             (id TEXT, journal_id BIGINT, entry_type TEXT, amount NUMERIC) \
             WITH BALANCED ON (group_key = journal_id, debit = 'DEBIT', \
             credit = 'CREDIT', amount = amount)",
        )
        .await;
    let message = non_text.expect_err("a non-text BALANCED key column must be refused");
    assert!(
        message.contains("42804"),
        "a non-text BALANCED key column must report SQLSTATE 42804: {message}"
    );
}

/// The declared-column spelling is legal too, and the constraint enforces over
/// it exactly as it does over the schemaless one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balanced_over_declared_columns_enforces() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION bal_declared \
             (id TEXT, journal_id TEXT, entry_type TEXT, amount NUMERIC) \
             WITH BALANCED ON (group_key = journal_id, debit = 'DEBIT', \
             credit = 'CREDIT', amount = amount)",
        )
        .await
        .unwrap();

    let result = server
        .exec(&format!(
            "INSERT INTO bal_declared {COLUMNS} VALUES {}",
            leg("l1", "j-1", "DEBIT", 100)
        ))
        .await;
    assert_balance_violation(&result);

    server
        .exec(&format!(
            "INSERT INTO bal_declared {COLUMNS} VALUES {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 100),
        ))
        .await
        .unwrap();
    assert_eq!(ledger_ids(&server, "bal_declared").await.len(), 2);
}

// ── Autocommit single-row INSERT ──

/// One leg on its own is a journal that does not balance. The statement is its
/// own boundary, so it is refused — and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_single_insert_of_one_leg_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_single")).await.unwrap();

    let result = server
        .exec(&format!(
            "INSERT INTO bal_single {COLUMNS} VALUES {}",
            leg("l1", "j-1", "DEBIT", 100)
        ))
        .await;
    assert_balance_violation(&result);

    assert!(
        ledger_ids(&server, "bal_single").await.is_empty(),
        "a refused insert must leave no rows behind"
    );
}

/// A row that carries none of the constraint's columns is not part of any
/// journal, so it is ignored by the constraint rather than refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_outside_the_journal_is_unaffected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_outside")).await.unwrap();

    server
        .exec("INSERT INTO bal_outside (id, note) VALUES ('n1', 'not a journal line')")
        .await
        .unwrap();

    assert_eq!(ledger_ids(&server, "bal_outside").await.len(), 1);
}

// ── Autocommit multi-row INSERT ──

/// A multi-row INSERT is one boundary and one genuine set: a journal written as
/// several rows of a single statement balances and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_of_a_balanced_journal_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_multi_ok")).await.unwrap();

    server
        .exec(&format!(
            "INSERT INTO bal_multi_ok {COLUMNS} VALUES {}, {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 60),
            leg("l3", "j-1", "CREDIT", 40),
        ))
        .await
        .unwrap();

    assert_eq!(
        ledger_ids(&server, "bal_multi_ok").await,
        vec!["l1".to_string(), "l2".to_string(), "l3".to_string()],
        "a balanced multi-row journal must commit whole"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_of_an_unbalanced_journal_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_multi_bad")).await.unwrap();

    let result = server
        .exec(&format!(
            "INSERT INTO bal_multi_bad {COLUMNS} VALUES {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 60),
        ))
        .await;
    assert_balance_violation(&result);

    assert!(
        ledger_ids(&server, "bal_multi_bad").await.is_empty(),
        "a refused multi-row insert must leave no rows behind — not even the \
         rows that were applied before the shortfall was found"
    );
}

/// Groups are independent: one balanced journal does not excuse another that is
/// short.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_unbalanced_group_refuses_the_whole_statement() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_groups")).await.unwrap();

    let result = server
        .exec(&format!(
            "INSERT INTO bal_groups {COLUMNS} VALUES {}, {}, {}",
            leg("l1", "j-1", "DEBIT", 50),
            leg("l2", "j-1", "CREDIT", 50),
            leg("l3", "j-2", "DEBIT", 200),
        ))
        .await;
    assert_balance_violation(&result);

    assert!(ledger_ids(&server, "bal_groups").await.is_empty());
}

// ── Explicit transaction ──

/// Inside `BEGIN … COMMIT` the boundary is the transaction, so one leg per
/// statement is legal as long as the whole transaction balances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_balanced_across_statements_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_txn_ok")).await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_ok {COLUMNS} VALUES {}",
            leg("l1", "j-1", "DEBIT", 100)
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_ok {COLUMNS} VALUES {}",
            leg("l2", "j-1", "CREDIT", 100)
        ))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        ledger_ids(&server, "bal_txn_ok").await,
        vec!["l1".to_string(), "l2".to_string()],
        "a transaction that balances across its statements must commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_that_does_not_balance_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_txn_bad")).await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_bad {COLUMNS} VALUES {}",
            leg("l1", "j-1", "DEBIT", 100)
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_bad {COLUMNS} VALUES {}",
            leg("l2", "j-1", "CREDIT", 75)
        ))
        .await
        .unwrap();
    let commit = server.exec("COMMIT").await;
    assert!(
        commit.is_err(),
        "a transaction whose journal is short must not commit"
    );
    let _ = server.exec("ROLLBACK").await;

    assert!(
        ledger_ids(&server, "bal_txn_bad").await.is_empty(),
        "a refused transaction must leave no rows behind"
    );
}

/// A multi-row INSERT inside a transaction: the statement's rows are expanded
/// back into point writes for staging, so the transaction still sees them as
/// its own — and the balance is judged over the whole transaction, not the
/// statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_multi_row_insert_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_txn_multi")).await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_multi {COLUMNS} VALUES {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 100),
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_txn_multi {COLUMNS} VALUES {}, {}",
            leg("l3", "j-2", "DEBIT", 25),
            leg("l4", "j-2", "CREDIT", 25),
        ))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        ledger_ids(&server, "bal_txn_multi").await.len(),
        4,
        "two balanced multi-row journals in one transaction must commit whole"
    );
}

/// The negative half, inside a transaction: removing one leg leaves the other
/// behind, so the transaction does not balance even though it wrote nothing new.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_deleting_one_leg_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_txn_del")).await.unwrap();
    seed_journal(&server, "bal_txn_del").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM bal_txn_del WHERE id = 'l1'")
        .await
        .unwrap();
    let commit = server.exec("COMMIT").await;
    assert!(
        commit.is_err(),
        "a transaction that removes one leg of a balanced journal must not \
         commit (pre-fix: deletes contributed nothing, so this passed)"
    );
    let _ = server.exec("ROLLBACK").await;

    assert_eq!(
        ledger_ids(&server, "bal_txn_del").await,
        vec!["l1".to_string(), "l2".to_string()],
        "both legs must survive the refused transaction"
    );
}

// ── UPDATE ──

/// Seed one balanced journal: `l1` DEBIT 100, `l2` CREDIT 100.
async fn seed_journal(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "INSERT INTO {name} {COLUMNS} VALUES {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 100),
        ))
        .await
        .unwrap();
}

async fn amount_of(server: &TestServer, name: &str, id: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT amount FROM {name} WHERE id = '{id}'"))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_that_unbalances_a_journal_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_upd_bad")).await.unwrap();
    seed_journal(&server, "bal_upd_bad").await;

    let result = server
        .exec("UPDATE bal_upd_bad SET amount = 140 WHERE id = 'l1'")
        .await;
    assert_balance_violation(&result);

    assert_eq!(
        amount_of(&server, "bal_upd_bad", "l1").await,
        vec!["100".to_string()],
        "a refused update must leave the row exactly as it was"
    );
}

/// Moving both legs by the same amount nets to zero, so the journal still
/// balances and the update commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_that_preserves_the_balance_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_upd_ok")).await.unwrap();
    seed_journal(&server, "bal_upd_ok").await;

    server
        .exec("UPDATE bal_upd_ok SET amount = 140 WHERE journal_id = 'j-1'")
        .await
        .unwrap();

    assert_eq!(
        amount_of(&server, "bal_upd_ok", "l1").await,
        vec!["140".to_string()],
        "an update that moves both legs equally must commit"
    );
    assert_eq!(
        amount_of(&server, "bal_upd_ok", "l2").await,
        vec!["140".to_string()]
    );
}

// ── DELETE ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_one_leg_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_del_bad")).await.unwrap();
    seed_journal(&server, "bal_del_bad").await;

    let result = server.exec("DELETE FROM bal_del_bad WHERE id = 'l1'").await;
    assert_balance_violation(&result);

    assert_eq!(
        ledger_ids(&server, "bal_del_bad").await,
        vec!["l1".to_string(), "l2".to_string()],
        "a refused delete must leave both legs in place"
    );
}

/// Removing a whole journal nets to zero and is allowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_a_whole_journal_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_del_ok")).await.unwrap();
    seed_journal(&server, "bal_del_ok").await;

    server
        .exec("DELETE FROM bal_del_ok WHERE journal_id = 'j-1'")
        .await
        .unwrap();

    assert!(
        ledger_ids(&server, "bal_del_ok").await.is_empty(),
        "removing a whole balanced journal must commit"
    );
}

// ── MERGE ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_that_unbalances_a_journal_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_merge_bad")).await.unwrap();
    seed_journal(&server, "bal_merge_bad").await;

    server
        .exec("CREATE COLLECTION bal_merge_src")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bal_merge_src (id, amount) VALUES ('l1', 140)")
        .await
        .unwrap();

    let result = server
        .exec(
            "MERGE INTO bal_merge_bad t USING bal_merge_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount",
        )
        .await;
    assert_balance_violation(&result);

    assert_eq!(
        amount_of(&server, "bal_merge_bad", "l1").await,
        vec!["100".to_string()],
        "a refused MERGE must leave the matched row exactly as it was"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_that_preserves_the_balance_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_merge_ok")).await.unwrap();
    seed_journal(&server, "bal_merge_ok").await;

    server
        .exec("CREATE COLLECTION bal_merge_ok_src")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bal_merge_ok_src (id, amount) VALUES ('l1', 140)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO bal_merge_ok_src (id, amount) VALUES ('l2', 140)")
        .await
        .unwrap();

    server
        .exec(
            "MERGE INTO bal_merge_ok t USING bal_merge_ok_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount",
        )
        .await
        .unwrap();

    assert_eq!(
        amount_of(&server, "bal_merge_ok", "l1").await,
        vec!["140".to_string()],
        "a MERGE that moves both legs equally must commit"
    );
}

// ── INSERT … SELECT ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_of_one_leg_is_rejected() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_isel_bad")).await.unwrap();

    server.exec("CREATE COLLECTION bal_isel_src").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_isel_src {COLUMNS} VALUES {}",
            leg("l1", "j-1", "DEBIT", 100)
        ))
        .await
        .unwrap();

    let result = server
        .exec("INSERT INTO bal_isel_bad SELECT * FROM bal_isel_src")
        .await;
    assert_balance_violation(&result);

    assert!(
        ledger_ids(&server, "bal_isel_bad").await.is_empty(),
        "a refused INSERT … SELECT must leave no rows behind"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_of_a_balanced_journal_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_isel_ok")).await.unwrap();

    server
        .exec("CREATE COLLECTION bal_isel_ok_src")
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO bal_isel_ok_src {COLUMNS} VALUES {}, {}",
            leg("l1", "j-1", "DEBIT", 100),
            leg("l2", "j-1", "CREDIT", 100),
        ))
        .await
        .unwrap();

    server
        .exec("INSERT INTO bal_isel_ok SELECT * FROM bal_isel_ok_src")
        .await
        .unwrap();

    assert_eq!(
        ledger_ids(&server, "bal_isel_ok").await,
        vec!["l1".to_string(), "l2".to_string()],
        "a balanced journal copied in one statement must commit"
    );
}

// ── TRUNCATE ──

/// Emptying a collection whose journals all balance nets to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_of_a_balanced_ledger_commits() {
    let server = TestServer::start().await;
    server.exec(&create_ledger("bal_trunc")).await.unwrap();
    seed_journal(&server, "bal_trunc").await;

    server.exec("TRUNCATE TABLE bal_trunc").await.unwrap();

    assert!(ledger_ids(&server, "bal_trunc").await.is_empty());
}
