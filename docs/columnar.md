# Columnar Engine

NodeDB's columnar engine stores data in typed columns with per-column compression — the same approach as ClickHouse and DuckDB, but living alongside your OLTP data in the same database.

## When to Use

- Analytical queries (GROUP BY, aggregations, window functions)
- Reporting dashboards
- Data science workloads
- Any scan-heavy workload where you read a few columns from many rows
- HTAP: pair with strict documents for combined OLTP + OLAP

## Key Features

- **Per-column compression** — Each column gets a codec chain tuned for its data type:
  - **ALP** — Lossless floating-point to integer conversion
  - **FastLanes** — SIMD bit-packing for integers
  - **FSST** — Substring dictionary compression for strings
  - **Gorilla** — XOR-based compression for metrics
  - **Pcodec** — Complex numeric compression
  - **rANS** — Entropy coding for cold-tier data
  - **LZ4** — Terminal stage compression
- **Block statistics** — 1024-row blocks with min/max/null-count stats. The query engine skips entire blocks that can't match the predicate — no decompression needed.
- **Predicate pushdown** — Filters push down to the block level, reading only what's needed.
- **Delete bitmaps** — Roaring Bitmaps track deleted rows. Three-phase crash-safe compaction reclaims space.
- **20-40x compression** — Multi-stage pipeline achieves significantly better ratios than single-codec approaches on typical workloads.

## Peer Engines

Columnar shares its storage layer with two specialized peer engines for time-ordered and geospatial workloads. Pick one engine per collection — they are not sub-modes of `columnar`:

| Engine                            | Optimized for        | Extra features                                                      |
| --------------------------------- | -------------------- | ------------------------------------------------------------------- |
| **`columnar`**                    | General analytics    | Full UPDATE/DELETE support                                          |
| **[`timeseries`](timeseries.md)** | Time-ordered metrics | Append-only, retention policies, continuous aggregation, ILP ingest |
| **[`spatial`](spatial.md)**       | Geospatial data      | Auto R\*-tree indexing, geohash proximity queries                   |

## DDL Syntax

Engine selection is via `WITH (engine='<name>')`. Column modifiers designate special columns:

| Modifier        | Applies to                       | Effect                                                                                                                 |
| --------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `TIME_KEY`      | `TIMESTAMP` / `DATETIME` columns | Designates the primary time column. Enables partition-by-time, block-level skip, retention. Required for `timeseries`. |
| `SPATIAL_INDEX` | `GEOMETRY` columns               | Automatically builds and maintains an R\*-tree index on this column. Required for `spatial`.                           |

```sql
-- Plain columnar collection
CREATE COLLECTION logs (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    level VARCHAR,
    message VARCHAR
) WITH (engine='columnar');

-- Timeseries (TIME_KEY required)
CREATE COLLECTION metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH (engine='timeseries', partition_by='1h');

-- Spatial (SPATIAL_INDEX required)
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (engine='spatial');
```

`CREATE TIMESERIES <name>` remains as a convenience alias — equivalent to `WITH (engine='timeseries')`.

## Physical Plan Types

The query planner emits different physical plan nodes for columnar queries:

- **`ColumnarOp`** — Base plan for plain columnar collections. The peer engines extend this.
- **`TimeseriesOp`** — Extends `ColumnarOp` with `time_range` bounds and time bucketing. Used when a `TIME_KEY` column and time predicate are present.
- **`SpatialOp`** — Extends `ColumnarOp` with R\*-tree lookup. Used when a `SPATIAL_INDEX` column is referenced in a spatial predicate.

All three share the same `columnar_memtables` (the in-memory L0 structure). Engine-specific logic is layered on top.

## Examples

```sql
-- Plain columnar collection with a time column
CREATE COLLECTION logs (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    level VARCHAR,
    message VARCHAR
) WITH (engine='columnar');

-- Ingest
INSERT INTO logs (ts, host, level, message)
VALUES (now(), 'web-01', 'error', 'connection refused');
-- INSERT raises unique_violation on primary-key conflict; use UPSERT or
-- INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col for overwrite semantics.

-- Point get by primary key — O(log N) via segment PK index (not a full scan)
SELECT * FROM logs WHERE ts = '2026-04-24T10:00:00Z';

-- Analytical query — only reads the columns it needs
SELECT level, COUNT(*) AS count
FROM logs
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY level
ORDER BY count DESC;

-- ORDER BY is supported for columnar scans
SELECT ts, host, message FROM logs
WHERE level = 'error'
ORDER BY ts DESC
LIMIT 100;

-- Window functions
SELECT host, message,
       ROW_NUMBER() OVER (PARTITION BY host ORDER BY ts DESC) AS recency_rank
FROM logs;
```

## HTAP Bridge

The real power of columnar is combining it with strict documents for hybrid transactional/analytical processing (HTAP).

```sql
-- OLTP: strict collection for real-time writes
CREATE COLLECTION orders (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at TIMESTAMP DEFAULT now()
) WITH (engine='document_strict');

-- OLAP: materialized view auto-replicates to columnar via CDC
CREATE MATERIALIZED VIEW order_analytics AS
SELECT status, DATE_TRUNC('day', created_at) AS day, COUNT(*), SUM(total)
FROM orders
GROUP BY status, day;

-- Point lookups hit the strict engine (fast)
SELECT * FROM orders WHERE id = '...';

-- Analytical scans hit the columnar engine (fast)
SELECT * FROM order_analytics WHERE day > '2024-06-01';
```

The query planner routes automatically — no application logic needed. Changes to the strict collection propagate to the materialized view via CDC with staleness tracking.

## Converting Between Engines

`CONVERT COLLECTION ... TO <type>` accepts `document_schemaless`, `document_strict`, or `kv` as targets — schema-side moves between document modes and into KV. The columnar / timeseries / spatial engines are chosen at collection-creation time and aren't a CONVERT target.

```sql
CONVERT COLLECTION events TO document_schemaless;
CONVERT COLLECTION users  TO document_strict;
CONVERT COLLECTION cache  TO kv;
```

## Bitemporal Support

Columnar collections track both system time and valid time, enabling corrections, audit trails, and temporal analytics.

```sql
-- Query as of a past system time (historical database state)
SELECT level, COUNT(*) FROM logs
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY level
AS OF SYSTEM TIME '2026-06-07T12:00:00Z';

-- Query records valid at a specific date (forecast corrections, backdated data)
SELECT host, cpu_usage FROM metrics
WHERE ts BETWEEN '2026-04-01' AND '2026-04-02'
AS OF VALID TIME 1712000000000;
```

See [Bitemporal](bitemporal.md) for detailed examples and compliance workflows.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine temporal queries and audit trails
- [Timeseries](timeseries.md) — Peer engine for time-ordered data
- [Spatial](spatial.md) — Peer engine for geospatial data
- [Documents (strict)](documents.md) — HTAP bridge source for OLTP + OLAP

[Back to docs](README.md)
