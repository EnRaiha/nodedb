// SPDX-License-Identifier: BUSL-1.1

//! Write isolation fuzz: interleaved writes to clone and source must not
//! cross-contaminate.
//!
//! Fires several concurrent write rounds — alternating between the source and
//! the clone — and then verifies that each database's final state reflects only
//! its own writes, not the other's.

use crate::harness::TestServer;

fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// After interleaved updates the source and clone must each reflect only
/// their own last write.
#[tokio::test]
async fn interleaved_writes_do_not_cross_contaminate() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source setup.
    client
        .simple_query("CREATE DATABASE fuzz_src")
        .await
        .expect("CREATE DATABASE fuzz_src");
    client
        .simple_query("USE DATABASE fuzz_src")
        .await
        .expect("USE fuzz_src");
    client
        .simple_query(
            "CREATE COLLECTION counters \
             (id STRING PRIMARY KEY, val STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION counters");
    client
        .simple_query("INSERT INTO counters (id, val) VALUES ('c1', '0')")
        .await
        .expect("INSERT c1 into source");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE fuzz_clone FROM fuzz_src")
        .await
        .expect("CLONE fuzz_src");

    // Interleave writes: source gets 'source-final', clone gets 'clone-final'.
    for i in 1..=5u32 {
        // Write to source.
        client
            .simple_query("USE DATABASE fuzz_src")
            .await
            .expect("USE fuzz_src");
        client
            .simple_query(&format!(
                "UPDATE counters SET val = 'source-{i}' WHERE id = 'c1'"
            ))
            .await
            .expect("UPDATE source");

        // Write to clone.
        client
            .simple_query("USE DATABASE fuzz_clone")
            .await
            .expect("USE fuzz_clone");
        client
            .simple_query(&format!(
                "UPDATE counters SET val = 'clone-{i}' WHERE id = 'c1'"
            ))
            .await
            .expect("UPDATE clone");
    }

    // Final check: source.
    client
        .simple_query("USE DATABASE fuzz_src")
        .await
        .expect("USE fuzz_src final check");
    let src_msgs = client
        .simple_query("SELECT val FROM counters WHERE id = 'c1'")
        .await
        .expect("SELECT from source");
    assert_eq!(
        first_value(&src_msgs).as_deref(),
        Some("source-5"),
        "source must reflect its own last write"
    );

    // Final check: clone.
    client
        .simple_query("USE DATABASE fuzz_clone")
        .await
        .expect("USE fuzz_clone final check");
    let clone_msgs = client
        .simple_query("SELECT val FROM counters WHERE id = 'c1'")
        .await
        .expect("SELECT from clone");
    assert_eq!(
        first_value(&clone_msgs).as_deref(),
        Some("clone-5"),
        "clone must reflect its own last write"
    );
}

/// Inserting different keys in source and clone must not cause the other to
/// see the extra keys.
///
/// Snapshot isolation for the lazy KV read path is enforced via a
/// surrogate ceiling captured from the source's `SurrogateAssigner` at
/// clone-create time: bindings the source allocates AFTER the AS-OF
/// point are dropped from clone-delegated scans/gets, so source-side
/// post-clone INSERTs are invisible from the clone.
#[tokio::test]
async fn distinct_inserts_remain_isolated() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source.
    client
        .simple_query("CREATE DATABASE fuzz2_src")
        .await
        .expect("CREATE DATABASE fuzz2_src");
    client
        .simple_query("USE DATABASE fuzz2_src")
        .await
        .expect("USE fuzz2_src");
    client
        .simple_query("CREATE COLLECTION rows (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION rows");
    client
        .simple_query("INSERT INTO rows (k, v) VALUES ('common', 'base')")
        .await
        .expect("INSERT common");

    // Clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE fuzz2_clone FROM fuzz2_src")
        .await
        .expect("CLONE fuzz2_src");

    // Insert source-only key.
    client
        .simple_query("USE DATABASE fuzz2_src")
        .await
        .expect("USE fuzz2_src");
    client
        .simple_query("INSERT INTO rows (k, v) VALUES ('src-only', 'S')")
        .await
        .expect("INSERT src-only");

    // Insert clone-only key.
    client
        .simple_query("USE DATABASE fuzz2_clone")
        .await
        .expect("USE fuzz2_clone");
    client
        .simple_query("INSERT INTO rows (k, v) VALUES ('clone-only', 'C')")
        .await
        .expect("INSERT clone-only");

    // Clone must not see 'src-only'.
    let clone_src_key = client
        .simple_query("SELECT v FROM rows WHERE k = 'src-only'")
        .await
        .expect("SELECT src-only from clone");
    let clone_found = clone_src_key
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)));
    assert!(
        !clone_found,
        "clone must not see a key inserted only in the source"
    );

    // Source must not see 'clone-only'.
    client
        .simple_query("USE DATABASE fuzz2_src")
        .await
        .expect("USE fuzz2_src check");
    let src_clone_key = client
        .simple_query("SELECT v FROM rows WHERE k = 'clone-only'")
        .await
        .expect("SELECT clone-only from source");
    let src_found = src_clone_key
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)));
    assert!(
        !src_found,
        "source must not see a key inserted only in the clone"
    );
}
