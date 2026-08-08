// SPDX-License-Identifier: BUSL-1.1

//! The one enumeration of which engines carry `INSERT ... RETURNING` and which
//! still refuse it.
//!
//! Engines gain the clause one at a time, and each time they do, every test
//! that named that engine as unsupported becomes a lie that still passes
//! locally and only surfaces at full-suite time. That happened twice. So the
//! list lives HERE, once, and the per-engine test files assert against it
//! rather than restating it — adding an engine means moving one row from
//! [`REFUSED`] to [`SUPPORTED`], and every file that asserts on the split
//! updates with it.
//!
//! Both halves are asserted, deliberately. A refusal list alone would still
//! pass if the clause were silently dropped instead of honored, and a support
//! list alone would not notice a refusal message that names an engine which
//! actually works.

use crate::pgwire_harness::TestServer;

/// An engine whose insert has no `returning` slot on its physical op, so the
/// statement must be refused by name rather than reporting a command tag for
/// rows it never produced.
pub struct RefusedEngine {
    /// Engine name, used in assertion messages.
    pub engine: &'static str,
    /// Substring the refusal message must contain. This is what makes the
    /// error actionable — a caller has to learn WHICH engine refused.
    pub named_in_refusal: &'static str,
    /// DDL with a single `{c}` placeholder for the collection name.
    pub ddl: &'static str,
    /// `INSERT ... RETURNING` with the same `{c}` placeholder.
    pub insert_returning: &'static str,
}

/// An engine that carries the clause and hands back its stored row.
pub struct SupportedEngine {
    pub engine: &'static str,
    pub ddl: &'static str,
    pub insert_returning: &'static str,
    /// Column to read out of the returned row.
    pub probe_column: &'static str,
    /// Value `probe_column` must hold, proving a real stored row came back
    /// rather than an empty row set that would satisfy a weaker check.
    pub expected: &'static str,
}

/// Engines that still refuse `INSERT ... RETURNING`.
///
/// Empty: every engine carries the clause. Kept, with its assertion helper,
/// because the refusal surface is not gone — `INSERT ... SELECT` and
/// in-transaction writes still refuse, on plan-shape and staging grounds
/// respectively — and a future engine that lands without a `returning` slot
/// belongs here rather than in a fresh list.
pub const REFUSED: &[RefusedEngine] = &[];

/// Engines that carry `INSERT ... RETURNING` today.
pub const SUPPORTED: &[SupportedEngine] = &[
    SupportedEngine {
        engine: "document_schemaless",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT)",
        insert_returning: "INSERT INTO {c} (id, n) VALUES ('d1', 1) RETURNING id, n",
        probe_column: "id",
        expected: "d1",
    },
    SupportedEngine {
        engine: "document_strict",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT) \
              WITH (engine='document_strict')",
        insert_returning: "INSERT INTO {c} (id, n) VALUES ('s1', 1) RETURNING id, n",
        probe_column: "id",
        expected: "s1",
    },
    SupportedEngine {
        engine: "kv",
        ddl: "CREATE COLLECTION {c} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
        insert_returning: "INSERT INTO {c} (key, n) VALUES ('k1', 1) RETURNING key, n",
        probe_column: "key",
        expected: "k1",
    },
    SupportedEngine {
        engine: "columnar",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, v FLOAT) WITH (engine='columnar')",
        insert_returning: "INSERT INTO {c} (id, v) VALUES ('c1', 1.5) RETURNING id, v",
        probe_column: "id",
        expected: "c1",
    },
    SupportedEngine {
        engine: "spatial",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, location GEOMETRY, name TEXT) \
              WITH (engine='spatial')",
        insert_returning: "INSERT INTO {c} (id, location, name) VALUES \
                           ('p1', '{\"type\":\"Point\",\"coordinates\":[-122.4,37.8]}', 'SF') \
                           RETURNING id, name",
        probe_column: "id",
        expected: "p1",
    },
    SupportedEngine {
        engine: "timeseries",
        ddl: "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, v FLOAT) \
              WITH (engine='timeseries')",
        insert_returning: "INSERT INTO {c} (ts, v) VALUES (1000, 1.5) RETURNING ts, v",
        probe_column: "v",
        expected: "1.5",
    },
    SupportedEngine {
        engine: "vector_primary",
        ddl: "CREATE COLLECTION {c} (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
              WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                    payload_indexes=['owner'])",
        insert_returning: "INSERT INTO {c} (id, vec, owner) VALUES \
                           ('v1', ARRAY[1.0, 0.0, 0.0], 'alice') RETURNING id, owner",
        probe_column: "id",
        expected: "v1",
    },
];

fn bind(sql: &str, collection: &str) -> String {
    sql.replace("{c}", collection)
}

/// Assert every engine in [`REFUSED`] refuses `INSERT ... RETURNING`, naming
/// itself in the error.
///
/// `prefix` scopes the collections this creates so a single server can host
/// more than one call without colliding.
pub async fn assert_refused_engines_still_refuse(server: &TestServer, prefix: &str) {
    for case in REFUSED {
        let collection = format!("{prefix}_{}", case.engine.replace('-', "_"));
        server
            .exec(&bind(case.ddl, &collection))
            .await
            .unwrap_or_else(|e| panic!("create {} collection {collection}: {e}", case.engine));
        server
            .expect_error(
                &bind(case.insert_returning, &collection),
                case.named_in_refusal,
            )
            .await;
    }
}

/// Assert every engine in [`SUPPORTED`] hands back its stored row.
///
/// The counterpart to [`assert_refused_engines_still_refuse`]: without it, an
/// engine could satisfy the refusal list by being absent from it while quietly
/// dropping the clause instead of honoring it.
pub async fn assert_supported_engines_return_their_row(server: &TestServer, prefix: &str) {
    for case in SUPPORTED {
        let collection = format!("{prefix}_{}", case.engine);
        server
            .exec(&bind(case.ddl, &collection))
            .await
            .unwrap_or_else(|e| panic!("create {} collection {collection}: {e}", case.engine));
        let returned = server
            .query_named_rows(&bind(case.insert_returning, &collection))
            .await
            .unwrap_or_else(|e| panic!("{} INSERT RETURNING: {e}", case.engine));
        assert_eq!(
            returned.len(),
            1,
            "{} must return exactly the row it stored: {returned:?}",
            case.engine
        );
        assert_eq!(
            returned[0].get(case.probe_column).map(String::as_str),
            Some(case.expected),
            "{} must return the stored value for '{}': {returned:?}",
            case.engine,
            case.probe_column
        );
    }
}
