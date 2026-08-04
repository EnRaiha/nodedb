<div align="center">
 
<img src="assets/wordmark.svg" alt="NodeDB" width="420">

<h3>The memory and storage engine for AI agents — from the edge to the cloud.</h3>

<p>
  Durable, queryable, access-guarded memory for AI agents that runs where the agent runs —
  embedded on device, offline-capable, and syncing to a distributed server when connected.
  Semantic, relational, episodic, and time-series memory in one engine, one process, with no
  network hops between them.
</p>

<p>
  <a href="https://nodedb.dev/docs/introduction/quickstart/"><strong>Quickstart</strong></a>
  ·
  <a href="https://nodedb.dev/docs"><strong>Docs</strong></a>
  ·
  <a href="#performance"><strong>Benchmarks</strong></a>
  ·
  <a href="https://github.com/NodeDB-Lab/nodedb-lite"><strong>NodeDB-Lite</strong></a>
  ·
  <a href="https://github.com/NodeDB-Lab/nodedb-cli"><strong>CLI</strong></a>
</p>

<p align="center">
  <a href="https://discord.gg/s54gDMVc7B">
    <img src="assets/discord-cta.svg" alt="Join the NodeDB Discord" width="340">
  </a>
</p>

<p>
  <a href="https://github.com/NodeDB-Lab/nodedb/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/NodeDB-Lab/nodedb/ci.yml?branch=main&label=ci" alt="CI status">
  </a>
  <a href="https://github.com/NodeDB-Lab/nodedb/releases">
    <img src="https://img.shields.io/github/v/release/NodeDB-Lab/nodedb?display_name=tag" alt="Latest release">
  </a>
  <a href="https://github.com/NodeDB-Lab/nodedb/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-BUSL--1.1-blue" alt="License">
  </a>
  <a href="https://github.com/NodeDB-Lab/nodedb/stargazers">
    <img src="https://img.shields.io/github/stars/NodeDB-Lab/nodedb?style=social" alt="GitHub stars">
  </a>
</p>

</div>

AI agents need memory: what they were told, how facts relate, what happened when, and what matters right now. The usual answer stitches a cloud vector store to a graph database to a document store — or dumps everything into a markdown file and hopes. NodeDB puts all of it in one engine that runs where the agent runs: the same engine embeds on a laptop, phone, or browser ([NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite)) and scales to a distributed cluster (NodeDB Origin), with CRDT sync between them.

It already powers **ma8e**, an in-development memory-and-knowledge layer for coding agents built entirely on embedded NodeDB — deterministic capture, hybrid retrieval, and access guards in one process with no network hop.

## Why NodeDB

- **Memory that runs where the agent runs.** The same engines embed on device and in the browser (NodeDB-Lite, WASM), work offline, and sync conflict-free to the server (CRDT). A cloud vector store can't embed next to your agent; a markdown file isn't queryable or access-guarded.
- **All of an agent's memory in one query.** Vector search, graph traversal, full-text ranking, document storage, timeseries, and key-value share one storage core, one identity space, and one planner — so a query flows from semantic search into graph traversal filtered by full-text, in one process with no network hops. Multi-model is _how_ NodeDB does this; the point is the capability, not the label.
- **PostgreSQL wire protocol.** Connect with `psql` or any PostgreSQL client library. Standard SQL with engine-specific extensions where SQL can't express the operation.
- **One engine instead of a polyglot stack.** When you outgrow the edge, the same engine folds a server-side sprawl of separate systems into a single deployment — fewer services to run, no cross-system sync pipelines, no schema drift.
- **Serious about performance.** Thread-per-Core data plane with io_uring, SIMD-accelerated distance functions, zero-copy MessagePack transport, per-column compression (ALP, FastLanes, FSST, Gorilla). See benchmarks below.

## Performance

**Timeseries ingest + query benchmark** — 10M rows, high-cardinality DNS telemetry (50K+ unique domain names). Single node, NVMe storage.

### Ingest

| Engine      | Rate         | Time         | Memory     | Disk         |
| ----------- | ------------ | ------------ | ---------- | ------------ |
| **NodeDB**  | **93,450/s** | 107s         | **120 MB** | 2,217 MB     |
| TimescaleDB | 56,615/s     | 177s         | 963 MB     | 2,802 MB     |
| ClickHouse  | 53,905/s     | 186s         | 1,035 MB   | **1,647 MB** |
| InfluxDB    | 22,715/s     | 88s (2M cap) | 1,656 MB   | 982 MB       |

### Queries (ms, best of 3)

| Query                      | NodeDB  | ClickHouse | TimescaleDB | InfluxDB (2M) |
| -------------------------- | ------- | ---------- | ----------- | ------------- |
| `COUNT(*)`                 | **<1**  | 1          | 423         | 13,110        |
| `WHERE qtype=A COUNT`      | 47      | **6**      | 347         | 5,297         |
| `WHERE rcode=SERVFAIL`     | 41      | **6**      | 334         | 1,048         |
| `GROUP BY qtype`           | 56      | **15**     | 597         | 12,426        |
| `GROUP BY rcode`           | 52      | **16**     | 604         | 13,183        |
| `GROUP BY cached+AVG`      | 120     | **33**     | 677         | 13,652        |
| `GROUP BY client_ip (10K)` | **141** | 157        | 660         | 14,301        |
| `GROUP BY qname (50K+)`    | 2,665   | **288**    | 3,644       | 16,720        |
| `time_bucket 1h`           | 101     | **30**     | 603         | --            |
| `time_bucket 5m+qtype`     | 138     | **99**     | 711         | --            |

NodeDB is not a specialized timeseries database, yet it ingests 1.65x faster than TimescaleDB and 1.73x faster than ClickHouse with 8x less memory. Query latency is competitive with ClickHouse on low-cardinality aggregations and within 3-5x on high-cardinality GROUP BY. This is the tradeoff of a general-purpose engine: you get one system instead of five, with performance that stays in the same ballpark as specialized tools.

## Engines

| Engine                                       | What it replaces             | Key capability                                                                                                          |
| -------------------------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [Vector](docs/vectors.md)                    | pgvector, Pinecone, Weaviate | HNSW with SQ8/PQ quantization, adaptive bitmap pre-filtering                                                            |
| [Graph](docs/graph.md)                       | Neo4j, Amazon Neptune        | CSR adjacency, 13 algorithms, Cypher-subset MATCH, GraphRAG                                                             |
| [Document](docs/documents.md)                | MongoDB, CouchDB             | Schemaless (MessagePack + CRDT) or Strict (Binary Tuples, O(1) field access). Typeguards for gradual schema enforcement |
| [Columnar](docs/columnar.md)                 | ClickHouse, DuckDB           | Per-column codecs (ALP, FastLanes, FSST), predicate pushdown, HTAP bridge                                               |
| [Timeseries](docs/timeseries.md)             | TimescaleDB, InfluxDB        | ILP ingest, continuous aggregation, PromQL, approximate aggregation                                                     |
| [Spatial](docs/spatial.md)                   | PostGIS                      | R\*-tree, geohash, H3, OGC predicates, hybrid spatial-vector                                                            |
| [Key-Value](docs/kv.md)                      | Redis, DynamoDB              | O(1) lookups, TTL, sorted indexes, rate limiting, SQL-queryable                                                         |
| [Full-Text Search](docs/full-text-search.md) | Elasticsearch                | BMW BM25, 27-language support, CJK bigrams, fuzzy, hybrid vector fusion                                                 |
| [Array (NDArray)](docs/array.md)             | TileDB, Zarr                 | Multi-dimensional tiles, Z-order indexing, bitemporal support, tile-level retention                                     |

## Install

```bash
# Docker
docker run -d \
  -p 6432:6432 -p 6433:6433 -p 6480:6480 \
  -v nodedb-data:/var/lib/nodedb \
  farhansyah/nodedb:latest

# Cargo
cargo install nodedb
```

Requires Linux kernel >= 5.1 (io_uring). Connect:

```bash
ndb                              # native CLI (connects to localhost:6433)
psql -h localhost -p 6432        # or any PostgreSQL client
```

```sql
CREATE COLLECTION users;

-- Standard SQL
INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30);

-- Object literal syntax (same result)
INSERT INTO users { name: 'Bob', email: 'bob@example.com', age: 25 };

-- Batch insert
INSERT INTO users [
    { name: 'Charlie', email: 'charlie@example.com', age: 35 },
    { name: 'Dana', email: 'dana@example.com', age: 28 }
];

SELECT * FROM users WHERE age > 25;
```

New here? Start with the [Quickstart](https://nodedb.dev/docs/introduction/quickstart/) on the official documentation site: **[nodedb.dev/docs](https://nodedb.dev/docs)**.

## Deployment Modes

| Mode                | Use case                                                                     |
| ------------------- | ---------------------------------------------------------------------------- |
| **Origin (server)** | Full distributed database. Multi-Raft, io_uring, pgwire. Horizontal scaling. |
| **Origin (local)**  | Same binary, single-node. No cluster overhead.                               |
| **NodeDB-Lite**     | Embedded library for phones, browsers, desktops. CRDT sync to Origin.        |

## NodeDB-Lite

All eight engines as an embedded library. Linux, macOS, Windows, Android, iOS, and browser (WASM, experimental).

- **Lite only** -- local-first apps that don't need a server. Vector search, graph, FTS, documents, arrays, all in-process with sub-ms reads.
- **Lite + Origin** -- offline-first with CRDT sync. Writes happen locally, deltas merge to Origin when online. Multiple devices converge regardless of order.
- **Same API** -- the `NodeDb` trait is identical across Lite and Origin. Switch between embedded and server without changing application code.

See [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite) for platform details and sync configuration.

## Key Features

**Write-time validation** -- Typeguards enforce types, required fields, CHECK constraints, and DEFAULT/VALUE expressions on schemaless collections. Graduate to strict schema with `CONVERT COLLECTION x TO document_strict`.

**Bitemporal queries** -- System time (audit trail) and valid time (temporal semantics) across all engines. Query data as it existed in the past, or as it was valid on a past date. GDPR-compliant tile purge on array engine.

**Multi-dimensional arrays** -- Scientific computing with Z-order indexed tiles, tile-level compression, and bitemporal support. Combine with vector/graph/text in fused queries via cross-engine identity.

**Real-time** -- CDC change streams with consumer groups (~1-5ms latency). Streaming materialized views. Durable topics. Cron scheduler. LISTEN/NOTIFY. All powered by the Event Plane.

**Programmability** -- Stored procedures with `IF/FOR/WHILE/LOOP`. User-defined functions. Triggers (async, sync, deferred). `SECURITY DEFINER`.

**Security** -- RBAC with GRANT/REVOKE. Row-level security with `$auth.*` context across all engines. Hash-chained audit log. Multi-tenancy with per-tenant encryption. JWKS, mTLS, API keys.

**Six wire protocols** -- pgwire (PostgreSQL), HTTP/REST, WebSocket, RESP (Redis), ILP (InfluxDB line protocol), native MessagePack.

## Tools

- **[`ndb`](https://github.com/NodeDB-Lab/nodedb-cli)** -- Native CLI with TUI, syntax highlighting, and tab completion. Alternative to `psql`.
- **[NodeDB Studio](https://github.com/NodeDB-Lab/nodedb-studio)** -- GUI client for managing collections, browsing data, and monitoring. _(coming soon)_
- **[nodedb-bench](https://github.com/NodeDB-Lab/nodedb-bench)** -- Performance benchmarks against competing databases.

## Documentation

The official documentation site is **[nodedb.dev/docs](https://nodedb.dev/docs)** — start with the [Quickstart](https://nodedb.dev/docs/introduction/quickstart/).

In-repo references:

- [Getting Started](docs/getting-started.md) -- Build, run, connect
- [Architecture](docs/architecture.md) -- Three-plane execution model
- [Engine Guides](docs/README.md) -- Deep dives into each engine
- [Security](docs/security/README.md) -- Auth, RBAC, RLS, audit, multi-tenancy
- [Real-Time](docs/real-time.md) -- CDC, pub/sub, LIVE SELECT
- [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite) -- Embedded edge database
- [AI Patterns](docs/ai/README.md) -- RAG, GraphRAG, agent memory, feature store

## Contributing

We welcome bug fixes, engine improvements, new codecs and analyzers, test coverage, and documentation. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR — NodeDB's Three-Plane execution model has hard rules that reviewers enforce.

## Building from Source

For development or contributing:

```bash
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb
cargo build --release
cargo install cargo-nextest --locked  # one-time
cargo nextest run --all-features
```

## Release Status

NodeDB Origin is in **public beta**; the current release is **v0.5.0 (2026-08-04)**. All eight engines are feature-complete and covered by tests. The query-facing wire protocols (pgwire, HTTP, native MessagePack, RESP) are stable — clients written against 0.1.0 will keep working through 1.0. The CRDT sync protocol and ILP ingest are still moving: 0.5.0 changes the sync frame and requires authentication on ILP connections, so upgrade NodeDB Lite and ingest clients together with the server.

v0.5.0 ships as server binaries and Docker images; the crates.io publication follows separately, so `cargo add` still resolves 0.4.0 for now. Library users who need the 0.5.0 fixes today can depend on the `v0.5.0` tag by git.

**v0.5.0 — Beta (today).** Build new products on it. The public surface (SQL dialect, query wire protocols, configuration) is stable; expect internal changes (storage layout, on-disk format, replication internals) between minor releases — a 0.4.0 data directory needs a dump/reload to move to 0.5.0. Patch and minor bumps land as needed — 0.2 through 0.5 shipped as the work demanded. The beta period is deliberately for real workloads to surface edge cases we can't manufacture in-house.

**v1.0.0 — Production-ready (target: after the beta hardening period).** What 1.0 guarantees:

- **API & SQL stability** — semver from 1.0 onward. No breaking SQL or client-API changes within a major.
- **Wire protocol stability** — pgwire, HTTP, native MessagePack, RESP, ILP, WebSocket frozen.
- **On-disk format stability** — no breaking migrations within 1.x. Forward-compatible upgrades only.
- **Cluster & Raft stability** — rolling upgrades supported within 1.x; no quorum-breaking changes.
- **Performance SLAs** — published p50/p99 targets per engine, regression-gated in CI.
- **Security audit** — third-party audit completed and findings remediated before 1.0 ships.
- **Storage, backup, and recovery** — fully exercised under fault injection, sustained load, and crash-restart cycles.

Pre-1.0 versions may change internals between releases — those changes are critical-path work (storage, backup, security, recovery) that has to be hardened in real production conditions before we put a stability stamp on it. The wire protocol and SQL surface won't break; everything underneath is fair game until 1.0.

> **Note:** This release track applies to **NodeDB Origin** (the server) only. [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite), [`ndb` CLI](https://github.com/NodeDB-Lab/nodedb-cli), and [NodeDB Studio](https://github.com/NodeDB-Lab/nodedb-studio) are versioned independently on their own tracks.

**Want to test or experiment with NodeDB?** Join our [Discord](https://discord.gg/s54gDMVc7B) — we provide full support for early adopters during the beta.

## Contributors

<a href="https://github.com/NodeDB-Lab/nodedb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=NodeDB-Lab/nodedb" alt="NodeDB Contributors"/>
</a>

## License

NodeDB is **source-available** under a mixed-license model. The short version:
**you can use NodeDB for anything — internal apps, your own SaaS or AI-agent
platform, commercial products, at any scale. The one thing you may not do is
offer NodeDB itself to third parties as a hosted database service (DBaaS).**

- **Everything except the server — [Apache 2.0](LICENSE.APACHE-2.0), no strings.**
  The shared kernel that both Origin and [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite)
  build on: the engines (`nodedb-vector`, `nodedb-vector-gpu`, `nodedb-graph`,
  `nodedb-fts`, `nodedb-spatial`, `nodedb-columnar`, `nodedb-array`), the
  foundations (`nodedb-mem`, `nodedb-wal`, `nodedb-crdt`), and the supporting
  crates (`nodedb-types`, `nodedb-codec`, `nodedb-strict`, `nodedb-sql`,
  `nodedb-query`, `nodedb-physical`, `nodedb-client`). Use them freely in your own
  projects, SDKs, and tools.
- **The server — [Business Source License 1.1](LICENSE).** Only four crates:
  `nodedb` (the server binary), `nodedb-bridge`, `nodedb-raft`, `nodedb-cluster` —
  the three-plane runtime and distribution layer. Free for any use _except_
  operating it as a managed/hosted database service on others' behalf. Converts
  to Apache 2.0 on 2030-05-01.

Cloud providers and marketplaces may offer one-click or self-deploy NodeDB images
freely — that's explicitly permitted; the line is operational, not promotional.
Only running the database _as a managed service on someone's behalf_ needs a
commercial license.

> **NodeDB-Lite** (the embedded build — WASM/OPFS, mobile, desktop) is **entirely
> Apache 2.0** and pulls in only the Apache crates above. There is no BSL anywhere
> in a Lite dependency tree — embed it wherever you like.
