// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta admission judges a state constraint on the change from the
//! stored row to the merged post-image.
//!
//! The test holds its own Loro document. It writes row `a` in `draft`
//! through `crdt_apply`, declares `status TRANSITIONS ('draft' -> 'active')`,
//! then forks two deltas from that base. The `draft -> closed` delta is
//! refused at admission and leaves the row unchanged. The `draft -> active`
//! delta is admitted.

use crate::harness::TestServer;

const COLL: &str = "crdt_delta_flow";
const ROW: &str = "a";

/// Hex of the updates `doc` holds past `base`.
fn updates_hex(doc: &loro::LoroDoc, base: &loro::VersionVector) -> String {
    let bytes = doc
        .export(loro::ExportMode::updates(base))
        .expect("export loro updates");
    hex::encode(bytes)
}

/// A fork of `base_doc` that sets the row's status to `next`, as the
/// updates past the base.
fn status_delta(base_doc: &loro::LoroDoc, next: &str) -> String {
    let base = base_doc.oplog_vv();
    let fork = base_doc.fork();
    // The row map the server's CRDT state writes: the existing container, or
    // a new one at the row key.
    let coll = fork.get_map(COLL);
    let row = match coll.get(ROW) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) => m,
        _ => coll
            .insert_container(ROW, loro::LoroMap::new())
            .expect("row container"),
    };
    row.insert("status", next).expect("status field");
    fork.commit();
    updates_hex(&fork, &base)
}

/// The row's current `status`, read from the document the apply
/// materializes. `crdt_state` returns the Loro snapshot, which keeps every
/// past value in its history, so it cannot tell the current status.
async fn status(server: &TestServer) -> String {
    let rows = server
        .query_text(&format!("SELECT status FROM {COLL} WHERE id = '{ROW}'"))
        .await
        .expect("read status");
    assert_eq!(rows.len(), 1, "row {ROW} must exist: {rows:?}");
    rows[0].clone()
}

async fn apply(server: &TestServer, delta_hex: &str) -> Result<(), String> {
    server
        .exec(&format!(
            "SELECT crdt_apply('{COLL}', '{ROW}', '{delta_hex}')"
        ))
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_delta_admission_obeys_the_state_constraint() {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"
        ))
        .await
        .unwrap();

    // The base: row `a` in `draft`, written by this document's own peer.
    let doc = loro::LoroDoc::new();
    let empty = doc.oplog_vv();
    let row = doc
        .get_map(COLL)
        .insert_container(ROW, loro::LoroMap::new())
        .expect("row container");
    row.insert("id", ROW).expect("id field");
    row.insert("status", "draft").expect("status field");
    doc.commit();
    apply(&server, &updates_hex(&doc, &empty))
        .await
        .unwrap_or_else(|e| panic!("the base row must be admitted: {e}"));
    assert_eq!(status(&server).await, "draft");

    server
        .exec(&format!(
            "ALTER COLLECTION {COLL} ADD CONSTRAINT flow \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')"
        ))
        .await
        .unwrap();

    let refused = apply(&server, &status_delta(&doc, "closed")).await;
    assert!(
        refused.is_err(),
        "a draft -> closed delta must be refused at admission"
    );
    assert_eq!(
        status(&server).await,
        "draft",
        "a refused delta must leave the row in draft"
    );

    apply(&server, &status_delta(&doc, "active"))
        .await
        .unwrap_or_else(|e| panic!("a draft -> active delta must be admitted: {e}"));
    assert_eq!(
        status(&server).await,
        "active",
        "an admitted delta must store active"
    );
}
