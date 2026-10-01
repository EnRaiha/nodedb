// SPDX-License-Identifier: BUSL-1.1

//! Rows of a `HASH_CHAIN` collection keep their chain across `kill -9` and
//! reopen, and `VERIFY_HASH_CHAIN` reports the chain valid on both sides.
//!
//! A committed transaction's WAL redo record carries each row's SUBMITTED
//! body. The link is derived at install time from the chain head, so no WAL
//! record carries it. Boot replay re-applies every redo record over rows
//! already durable in redb. A replay that writes the redo body verbatim
//! strips the link from every row a transaction wrote.
//!
//! Each case writes rows through autocommit and through multi-statement
//! transactions, on a schemaless and a strict collection.

mod crash_harness;

use crash_harness::CrashHarness;

/// Every row, in `id` order, with its amount.
const ROWS: [(&str, i64); 4] = [("k0", 10), ("k1", 20), ("k2", 30), ("k3", 40)];

/// SHA-256 hex length of one link.
const LINK_LEN: usize = 64;

/// The storage mode a case creates its collection with.
#[derive(Clone, Copy)]
enum Engine {
    Schemaless,
    Strict,
}

impl Engine {
    fn name(self) -> &'static str {
        match self {
            Engine::Schemaless => "document_schemaless",
            Engine::Strict => "document_strict",
        }
    }
}

/// An incidental checkpoint moves the replay floor past the writes, and
/// replay then has nothing to re-apply. The long interval prevents it.
fn harness() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
}

fn insert(collection: &str, id: &str, amount: i64) -> String {
    format!("INSERT INTO {collection} (id, amount) VALUES ('{id}', {amount})")
}

/// Run `statements` in order on one connection. Returns the first error as
/// `"<statement>: <code>: <message>"`.
async fn run_on_one_connection(h: &CrashHarness, statements: &[String]) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    tokio::spawn(connection);
    for statement in statements {
        client.simple_query(statement).await.map_err(|e| {
            let detail = e
                .as_db_error()
                .map(|db| format!("{}: {}", db.code().code(), db.message()))
                .unwrap_or_else(|| e.to_string());
            format!("{statement}: {detail}")
        })?;
    }
    Ok(())
}

/// Commit `rows` as one multi-statement transaction.
async fn commit_transaction(h: &CrashHarness, collection: &str, rows: &[(&str, i64)]) {
    let mut statements = vec!["BEGIN".to_string()];
    statements.extend(
        rows.iter()
            .map(|(id, amount)| insert(collection, id, *amount)),
    );
    statements.push("COMMIT".to_string());
    if let Err(error) = run_on_one_connection(h, &statements).await {
        panic!("transaction on {collection} failed: {error}");
    }
}

/// `UPDATE` of a row in a chained collection is refused, and so is an INSERT
/// that supplies a chain column.
async fn assert_writes_refused(h: &CrashHarness, collection: &str) {
    let update = format!("UPDATE {collection} SET amount = 99 WHERE id = 'k1'");
    assert!(
        run_on_one_connection(h, &[update]).await.is_err(),
        "{collection} declares HASH_CHAIN, so UPDATE of a linked row must be refused"
    );
    let forged =
        format!("INSERT INTO {collection} (id, amount, _chain_seq) VALUES ('forged', 1, 1)");
    assert!(
        run_on_one_connection(h, &[forged]).await.is_err(),
        "{collection}: an INSERT that supplies _chain_seq must be refused"
    );
}

/// `(id, _chain_hash)` of every row, in `id` order.
async fn links(h: &CrashHarness, collection: &str) -> Vec<(String, String)> {
    let order = format!("FROM {collection} ORDER BY id");
    let ids = h.query_col(&format!("SELECT id {order}"), "id").await;
    let hashes = h
        .query_col(&format!("SELECT _chain_hash {order}"), "_chain_hash")
        .await;
    assert_eq!(ids.len(), hashes.len(), "one link per row");
    ids.into_iter().zip(hashes).collect()
}

async fn amounts(h: &CrashHarness, collection: &str) -> Vec<String> {
    h.query_col(
        &format!("SELECT amount FROM {collection} ORDER BY id"),
        "amount",
    )
    .await
}

/// Every row carries a well-formed link, and no two rows share one.
fn assert_all_linked(links: &[(String, String)], when: &str) {
    for (id, link) in links {
        assert!(
            link.len() == LINK_LEN && link.bytes().all(|b| b.is_ascii_hexdigit()),
            "{when}: row {id} carries no {LINK_LEN}-hex _chain_hash link (got {link:?})"
        );
    }
    let mut distinct: Vec<&String> = links.iter().map(|(_, link)| link).collect();
    distinct.sort();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        links.len(),
        "{when}: two rows share one link"
    );
}

/// `VERIFY_HASH_CHAIN` reports an intact chain of `entries` links.
async fn assert_chain_valid(h: &CrashHarness, collection: &str, entries: usize, when: &str) {
    let result = h
        .query_col(
            &format!("SELECT VERIFY_HASH_CHAIN('{collection}')"),
            "result",
        )
        .await;
    let [verdict] = result.as_slice() else {
        panic!("{when}: VERIFY_HASH_CHAIN must return one row, got {result:?}");
    };
    let verdict: serde_json::Value = serde_json::from_str(verdict)
        .unwrap_or_else(|e| panic!("{when}: verdict is not JSON ({e}): {verdict}"));
    assert_eq!(
        verdict["valid"], true,
        "{when}: VERIFY_HASH_CHAIN reported a break on {collection}: {verdict}"
    );
    assert_eq!(verdict["entries"], entries, "{when}: {verdict}");
}

async fn run(engine: Engine, collection: &str) {
    let mut h = harness();
    h.spawn();
    h.wait_ready();

    h.exec(&format!(
        "CREATE COLLECTION {collection} (id STRING PRIMARY KEY, amount INT) \
         WITH (engine='{}') APPEND_ONLY HASH_CHAIN",
        engine.name()
    ))
    .await;
    let (id, amount) = ROWS[0];
    h.exec(&insert(collection, id, amount)).await;
    commit_transaction(&h, collection, &ROWS[1..3]).await;
    commit_transaction(&h, collection, &ROWS[3..]).await;
    assert_writes_refused(&h, collection).await;

    let before = links(&h, collection).await;
    let expected_ids: Vec<&str> = ROWS.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        before.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
        expected_ids,
        "test setup: {collection} must hold every row before the crash"
    );
    assert_all_linked(&before, "before the crash");
    assert_chain_valid(&h, collection, ROWS.len(), "before the crash").await;
    let amounts_before = amounts(&h, collection).await;

    h.kill_9();
    h.reopen();

    assert_eq!(
        links(&h, collection).await,
        before,
        "boot replay rewrote the _chain_hash links of {collection}: a replayed redo \
         body must keep the link its install derived"
    );
    assert_eq!(amounts(&h, collection).await, amounts_before);
    assert_chain_valid(&h, collection, ROWS.len(), "after the restart").await;
    assert_writes_refused(&h, collection).await;

    // A row committed after the restart links from the recovered head.
    commit_transaction(&h, collection, &[("k4", 50)]).await;
    let after = links(&h, collection).await;
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_all_linked(&after, "after a post-restart commit");
    assert_chain_valid(
        &h,
        collection,
        ROWS.len() + 1,
        "after a post-restart commit",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schemaless_chain_survives_kill_9_with_the_metadata_group() {
    run(Engine::Schemaless, "chain_replay_cluster").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_chain_survives_kill_9_with_the_metadata_group() {
    run(Engine::Strict, "chain_strict_cluster").await;
}
