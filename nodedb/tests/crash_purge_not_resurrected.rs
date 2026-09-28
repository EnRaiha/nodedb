// SPDX-License-Identifier: BUSL-1.1

//! A collection removed by `DROP COLLECTION ... PURGE` must stay empty across
//! `kill -9` and reopen.
//!
//! The purge writes a tombstone that names the collection by its bare catalog
//! name. Outside the default database, the collection's WAL records name it by
//! the database-qualified storage name `"{database_id}/{name}"`. Boot replay
//! must match the two, or it re-applies every purged write and the rows come
//! back under a same-name collection created after the restart.
//!
//! Each case also keeps a second collection of the same engine that is never
//! purged. Its rows must come back through the same replay. That proves the
//! purged rows were still in the replayed WAL tail, so an empty purged
//! collection shows the tombstone gate worked and not that nothing replayed.
//!
//! Every engine runs in a named database and in the default database, with
//! the metadata Raft group and without it (`standalone`). The two modes reach
//! the purge through different apply paths.

mod crash_harness;

use crash_harness::CrashHarness;

const ROWS: usize = 3;

/// The engine a case creates its collections with.
#[derive(Clone, Copy)]
enum Engine {
    Document,
    Kv,
    Columnar,
}

impl Engine {
    fn create(self, name: &str) -> String {
        match self {
            Engine::Document => format!(
                "CREATE COLLECTION {name} (id STRING PRIMARY KEY, content STRING) \
                 WITH (engine='document_schemaless')"
            ),
            Engine::Kv => format!(
                "CREATE COLLECTION {name} (key STRING PRIMARY KEY, value STRING) \
                 WITH (engine='kv')"
            ),
            Engine::Columnar => format!(
                "CREATE COLLECTION {name} COLUMNS (id TEXT, region TEXT, ts BIGINT) \
                 WITH (engine='columnar')"
            ),
        }
    }

    fn insert(self, name: &str, i: usize) -> String {
        match self {
            Engine::Document => {
                format!("INSERT INTO {name} (id, content) VALUES ('k{i}', 'row-{i}')")
            }
            Engine::Kv => format!("INSERT INTO {name} (key, value) VALUES ('k{i}', 'row-{i}')"),
            Engine::Columnar => {
                format!("INSERT INTO {name} (id, region, ts) VALUES ('k{i}', 'row-{i}', {i})")
            }
        }
    }
}

/// One scenario: the engine, the database it runs in, and the boot mode.
struct Case {
    engine: Engine,
    /// `None` runs in the default database.
    database: Option<&'static str>,
    standalone: bool,
    prefix: &'static str,
}

/// An incidental checkpoint could move the replay floor past the inserts, and
/// replay would then have nothing to resurrect.
fn harness(standalone: bool) -> CrashHarness {
    let h = CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600");
    if standalone { h.standalone() } else { h }
}

async fn count(h: &CrashHarness, database: &str, collection: &str) -> Vec<String> {
    h.query_col_idx_in(database, &format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await
}

async fn run(case: Case) {
    let purged = format!("{}_purged", case.prefix);
    let kept = format!("{}_kept", case.prefix);
    let database = case.database.unwrap_or("default");

    let mut h = harness(case.standalone);
    h.spawn();
    h.wait_ready();

    if let Some(name) = case.database {
        h.exec(&format!("CREATE DATABASE {name}")).await;
    }
    for collection in [&purged, &kept] {
        h.exec_in(database, &case.engine.create(collection)).await;
        for i in 0..ROWS {
            h.exec_in(database, &case.engine.insert(collection, i))
                .await;
        }
        assert_eq!(
            count(&h, database, collection).await,
            vec![ROWS.to_string()],
            "test setup: {database}.{collection} must hold its {ROWS} rows before the purge"
        );
    }

    h.exec_in(database, &format!("DROP COLLECTION {purged} PURGE"))
        .await;

    h.kill_9();
    h.reopen();

    assert_eq!(
        count(&h, database, &kept).await,
        vec![ROWS.to_string()],
        "{database}.{kept} was never purged, so WAL replay must restore its {ROWS} rows; \
         without them this run cannot tell a working tombstone gate from a replay that \
         applied nothing"
    );

    // Recreate the purged name. A replay that ignored the tombstone installed
    // the purged rows under the same storage name, and they surface here.
    h.exec_in(database, &case.engine.create(&purged)).await;
    assert_eq!(
        count(&h, database, &purged).await,
        vec!["0".to_string()],
        "{database}.{purged} was purged before the crash; its rows came back after \
         kill -9 + reopen, so boot replay did not honour the purge tombstone"
    );
}

/// CREATE, DROP ... PURGE, and CREATE again on the same name, all before the
/// crash. The purge's tombstone must fence only the first incarnation; the
/// second incarnation's rows, inserted after the recreate, must survive boot
/// replay of the same log.
async fn run_recreated(case: Case) {
    let name = format!("{}_recreated", case.prefix);
    let database = case.database.unwrap_or("default");

    let mut h = harness(case.standalone);
    h.spawn();
    h.wait_ready();

    if let Some(db) = case.database {
        h.exec(&format!("CREATE DATABASE {db}")).await;
    }

    h.exec_in(database, &case.engine.create(&name)).await;
    for i in 0..ROWS {
        h.exec_in(database, &case.engine.insert(&name, i)).await;
    }
    assert_eq!(
        count(&h, database, &name).await,
        vec![ROWS.to_string()],
        "test setup: {database}.{name} must hold its {ROWS} rows before the purge"
    );

    h.exec_in(database, &format!("DROP COLLECTION {name} PURGE"))
        .await;

    // Second incarnation: created and populated before the crash, so the
    // WAL tail carries both the tombstone and the new rows in one replay.
    h.exec_in(database, &case.engine.create(&name)).await;
    for i in 0..ROWS {
        h.exec_in(database, &case.engine.insert(&name, i)).await;
    }
    assert_eq!(
        count(&h, database, &name).await,
        vec![ROWS.to_string()],
        "test setup: the recreated {database}.{name} must hold its {ROWS} rows before \
         the crash"
    );

    h.kill_9();
    h.reopen();

    assert_eq!(
        count(&h, database, &name).await,
        vec![ROWS.to_string()],
        "boot replay must fence the purge to the first incarnation of {database}.{name}; \
         a replay that ignores incarnation identity reclaims the second incarnation and \
         its rows vanish"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recreated_document_survives_replayed_purge() {
    run_recreated(Case {
        engine: Engine::Document,
        database: Some("recreate_doc_db"),
        standalone: false,
        prefix: "rdoc",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recreated_kv_survives_replayed_purge() {
    run_recreated(Case {
        engine: Engine::Kv,
        database: Some("recreate_kv_db"),
        standalone: false,
        prefix: "rkv",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recreated_columnar_survives_replayed_purge() {
    run_recreated(Case {
        engine: Engine::Columnar,
        database: Some("recreate_col_db"),
        standalone: false,
        prefix: "rcol",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_document_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Document,
        database: Some("purge_doc_db"),
        standalone: false,
        prefix: "pdoc",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_document_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Document,
        database: None,
        standalone: false,
        prefix: "pdoc",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_kv_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Kv,
        database: Some("purge_kv_db"),
        standalone: false,
        prefix: "pkv",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_kv_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Kv,
        database: None,
        standalone: false,
        prefix: "pkv",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_columnar_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Columnar,
        database: Some("purge_col_db"),
        standalone: false,
        prefix: "pcol",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_columnar_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Columnar,
        database: None,
        standalone: false,
        prefix: "pcol",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_document_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Document,
        database: Some("purge_doc_db"),
        standalone: true,
        prefix: "pdoc",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_document_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Document,
        database: None,
        standalone: true,
        prefix: "pdoc",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_kv_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Kv,
        database: Some("purge_kv_db"),
        standalone: true,
        prefix: "pkv",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_kv_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Kv,
        database: None,
        standalone: true,
        prefix: "pkv",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_columnar_rows_stay_gone_in_a_named_database() {
    run(Case {
        engine: Engine::Columnar,
        database: Some("purge_col_db"),
        standalone: true,
        prefix: "pcol",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_purged_columnar_rows_stay_gone_in_the_default_database() {
    run(Case {
        engine: Engine::Columnar,
        database: None,
        standalone: true,
        prefix: "pcol",
    })
    .await;
}
