# Array Engine

NodeDB's array engine stores multi-dimensional data with bitemporal support — system time (when data was entered) and valid time (what the data represents). Use it for scientific computing, geospatial grids, medical imaging, and time-evolving spatial data. Cells are compressed, indexed by Z-order curves, and queryable via SQL table-valued functions.

## When to Use

- Scientific simulations and climate models
- Medical imaging and volumetric analysis
- GIS raster data (elevation maps, satellite imagery)
- Time-evolving geospatial grids
- Sparse multi-dimensional datasets
- Hypervolume analysis with bitemporal audit trails

## Key Features

- **Multi-dimensional storage** — Define arrays with arbitrary dimensions (e.g., 3D space + time)
- **Tile-based compression** — Cells grouped into tiles, each tile independently compressed (ALP, FastLanes, Gorilla, LZ4)
- **Z-order indexing** — Hilbert/Z-order curve linearization for spatial locality and fast range queries
- **Bitemporal support** — Both system time (audit trail) and valid time (temporal semantics) tracked per tile
- **Row-major or column-major layout** — Choose `CELL_ORDER` to match your access patterns
- **Cross-engine identity** — Cells linked via surrogate bitmaps to vector, graph, document, and columnar queries
- **Distributed execution** — Sharded by tile, queries scatter-gather across cores/nodes
- **Tile-level retention** — Purge old versions by system time for compliance (GDPR, data minimization)

## DDL Syntax

Array schemas are defined by dimensions (axes), attributes (stored values), and tile extents (chunk size):

```sql
CREATE ARRAY spatial_grid
  DIMS (
    x INT64 [0..1000],
    y INT64 [0..1000],
    z INT64 [0..1000]
  )
  ATTRS (
    temperature FLOAT64,
    pressure FLOAT64,
    humidity FLOAT64
  )
  TILE_EXTENTS (64, 64, 64)
  WITH (
    prefix_bits = 8,
    audit_retain_ms = 86400000
  );
```

### Parameters

| Parameter         | Required | Default     | Description                                                                                                        |
| ----------------- | -------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `DIMS`            | Yes      | —           | List of dimensions. Each has a name, type (`INT64`, `FLOAT64`, `TIMESTAMP_MS`, `STRING`), and optional domain bounds `[lo..hi]`. |
| `ATTRS`           | Yes      | —           | List of attributes (cell values). Each has a name, a type (`INT64`, `FLOAT64`, `STRING`, `BYTES`), and an optional `NOT NULL`. |
| `TILE_EXTENTS`    | Yes      | —           | Tuple of tile extent per dimension. All > 0. Determines cell locality and compression block granularity.           |
| `CELL_ORDER`      | No       | `HILBERT`   | Unquoted: `ROW_MAJOR`, `COL_MAJOR`, `HILBERT`, or `ZORDER` (`Z_ORDER` is accepted). Affects spatial cache locality. |
| `TILE_ORDER`      | No       | `HILBERT`   | Unquoted: `ROW_MAJOR`, `COL_MAJOR`, `HILBERT`, or `ZORDER` (`Z_ORDER` is accepted). Affects tile layout.           |
| `prefix_bits`     | No       | `8`         | Bits for key prefix compression (1–16). Lower = better compression, higher = faster prefix filtering.              |
| `audit_retain_ms` | No       | `NULL`      | Milliseconds. Tiles older than now - `audit_retain_ms` (by system time) are eligible for purge. `NULL` = keep all. |
| `minimum_audit_retain_ms` | No | —        | Minimum retention window enforced for compliance.                                                                  |

## Examples

### Basic Creation and Insertion

```sql
-- Create a 2D elevation map with tiles of 256x256 cells
CREATE ARRAY elevation_map
  DIMS (
    lon FLOAT64 [-180..180],
    lat FLOAT64 [-90..90]
  )
  ATTRS (
    height FLOAT64
  )
  TILE_EXTENTS (256, 256);

-- Insert cells: COORDS lists the dims in schema order, VALUES lists the attrs in schema order.
-- Values are literals: numbers, strings, or NULL for an attribute.
INSERT INTO ARRAY elevation_map
  COORDS (-73.5, 40.7) VALUES (10.5),
  COORDS (-73.6, 40.8) VALUES (12.3),
  COORDS (-73.7, 40.6) VALUES (8.9);

-- Remove cells by coordinate
DELETE FROM ARRAY elevation_map WHERE COORDS IN ((-73.7, 40.6));

-- Writing a coordinate again overwrites its cell.
-- DROP ARRAY [IF EXISTS] <name> removes the whole array.

-- Flush memtable to persistent storage
SELECT ARRAY_FLUSH('elevation_map');
```

### Temporal Array (System Time + Valid Time)

```sql
-- Create a 3D climate model with temporal tracking
CREATE ARRAY climate_forecast
  DIMS (
    lon INT64 [-180..180],
    lat INT64 [-90..90],
    altitude_m INT64 [0..50000]
  )
  ATTRS (
    temp_c FLOAT64,
    humidity FLOAT64
  )
  TILE_EXTENTS (32, 32, 20)
  WITH (audit_retain_ms = 7776000000);  -- 90 days

-- Insert forecast data at a specific valid time
INSERT INTO ARRAY climate_forecast
  COORDS (10, 20, 5000) VALUES (-10.5, 65.0),
  COORDS (10, 20, 10000) VALUES (-20.3, 40.0);

-- Query at a specific moment in time (system time)
SELECT lon, lat, altitude_m, temp_c
FROM ARRAY_SLICE('climate_forecast', '{lon: [10, 14], lat: [20, 24], altitude_m: [5000, 14999]}', ['temp_c'])
AS OF SYSTEM TIME 1700000000000;
```

## Query Functions

All array queries use table-valued functions in the `FROM` clause. System time and valid time can be specified via `AS OF` clauses.

### ARRAY_SLICE — Range Query

Returns cells within a multi-dimensional range:

```sql
SELECT * FROM ARRAY_SLICE(
  'elevation_map',
  '{lon: [-74.0, -73.0], lat: [40.0, 41.0]}',
  ['height'],  -- projecting only 'height' (optional)
  1000         -- limit to 1000 cells (optional)
);
```

| Parameter | Required | Type          | Description                                                     |
| --------- | -------- | ------------- | --------------------------------------------------------------- |
| `array`   | Yes      | STRING        | Array name, as a string literal                                 |
| `bounds`  | Yes      | STRING        | Quoted object literal `'{ dim: [lo, hi] }'`. Each range is closed: both ends are included. Omitted dims = full range |
| `attrs`   | No       | ARRAY[STRING] | Attributes to project, such as `['height']`. `'*'` = all attributes. Omit it to read all attributes |
| `limit`   | No       | INT         | Max cells returned, as a non-negative integer literal. Omit it for no limit |

#### Result Relation

`ARRAY_SLICE` and `ARRAY_PROJECT` return one row per cell. The row has one column per dimension, then one column per requested attribute, or every attribute when the list is empty. Each column carries its declared type:

| Declared type | Column type        |
| ------------- | ------------------ |
| `INT64`       | `bigint`           |
| `FLOAT64`     | `double precision` |
| `TIMESTAMP_MS` (dimension) | `timestamp` |
| `STRING`      | `text`             |
| `BYTES`       | `bytea` (attribute only) |

`SELECT *` lists the dimensions first, then the attributes, in schema order. For a 2-D image with `nir` and `red` bands:

```sql
SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}');
--  x | y | nir  | red
-- ---+---+------+------
--  0 | 0 | 0.80 | 0.20
--  0 | 1 | 0.60 | 0.30
```

Every SELECT clause applies to the rows: `WHERE`, computed columns, `GROUP BY`, aggregates, `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT` / `OFFSET`, and window functions. Qualify a column with the call's alias, or with the array name when the call has no alias.

```sql
-- NDVI per cell, vegetated cells only, greenest first
SELECT x, y, (nir - red) / (nir + red) AS ndvi
FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}') AS t
WHERE t.nir > t.red
ORDER BY ndvi DESC
LIMIT 10;

-- Mean NIR per column of pixels
SELECT x, AVG(nir) AS mean_nir
FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}')
GROUP BY x;
```

The 4th-argument `limit` caps the cells the slice reads before any clause applies. An outer `LIMIT` applies to the query's result, and a smaller one also lowers the read cap when no other clause reads past the cut. A `WHERE` bound on a dimension narrows the read range only when the slice has no cap, so a capped slice keeps the cells its cap selects.

`AS OF SYSTEM TIME NULL` adds a `_ts_system` column: the version's system time in milliseconds. `SELECT *` omits it. Name it to read it.

`ARRAY_AGG` and `ARRAY_ELEMENTWISE` return rows of their own shape, not dimensions and attributes. A query over them takes `*` or their own output columns, and `LIMIT`. Every other clause is refused with an error that names the clause. Use `ARRAY_SLICE` to filter, group, or order cells.

### ARRAY_PROJECT — Select Attributes

Returns all cells with only the listed attributes. The attribute list is required and must not be empty:

```sql
SELECT * FROM ARRAY_PROJECT(
  'spatial_grid',
  ['temperature', 'pressure']  -- only these attributes
);
```

### ARRAY_AGG — Aggregate an Attribute

Reduces one attribute with a reducer. The optional 4th argument names a dimension to group by:

```sql
-- One result for the whole array
SELECT * FROM ARRAY_AGG('spatial_grid', 'temperature', 'SUM');

-- One result per value of x
SELECT * FROM ARRAY_AGG('spatial_grid', 'temperature', 'SUM', 'x');
```

The result has a `result` column. A grouped call also has a `group` column that holds the dimension value. Reducers are case-insensitive: `'SUM'`, `'COUNT'`, `'MIN'`, `'MAX'`, and `'MEAN'` (or `'AVG'`).

### ARRAY_ELEMENTWISE — Element-wise Operations

Applies an operation between two arrays of the same shape:

```sql
-- Subtract a baseline from all cells
SELECT * FROM ARRAY_ELEMENTWISE(
  'current_grid',
  'baseline_grid',
  'sub',          -- 'add', 'sub', 'mul', or 'div' (case-insensitive)
  'temperature'   -- the attribute to operate on
);
```

The result has `coords` and `attrs` columns, both JSON.

## Maintenance Functions

### ARRAY_FLUSH

Forces in-memory cells to durable storage:

```sql
SELECT ARRAY_FLUSH('spatial_grid');
-- Returns: {"flushed": 1}
```

Returns one object `{"flushed": 1}` on success. A failure raises an error.

### ARRAY_COMPACT

Merges tile versions and reclaims space:

```sql
SELECT ARRAY_COMPACT('spatial_grid');
-- Returns: {"compacted": 1}
```

Returns one object `{"compacted": n}`. `n` is `1` when a merge ran and `0` when the compaction picker found nothing to merge. A failure raises an error. Compaction is background-automatic, but can be triggered manually.

## Bitemporal Support

Arrays support dual timestamping:

- **System Time** — When the cell value was written (audit trail, compliance)
- **Valid Time** — When the cell represents (temporal semantics, forecasts, corrections)

Query as of either or both:

```sql
-- Read cells as they existed at a point in the past
SELECT * FROM ARRAY_SLICE(
  'data',
  '{x: [0, 99], y: [0, 99]}',
  ['value']
)
AS OF SYSTEM TIME 1700000000000;

-- Read cells that were valid at a specific time
SELECT * FROM ARRAY_SLICE(
  'forecast',
  '{x: [0, 99], y: [0, 99]}',
  ['temp']
)
AS OF VALID TIME 1700000000000;

-- Read cells that were valid AND existed at a specific time
SELECT * FROM ARRAY_SLICE(
  'forecast',
  '{x: [0, 99], y: [0, 99]}',
  ['temp']
)
AS OF SYSTEM TIME 1700000000000 AS OF VALID TIME 1700000001000;
```

A time is an integer in milliseconds since the Unix epoch, `NOW()`, or an ISO-8601 string such as `'2024-01-15T00:00:00Z'`.

## Cross-Engine Integration

Array cells are addressable via surrogate identity alongside other engines. An `ARRAY_SLICE` or `ARRAY_PROJECT` call can be a `JOIN` source. A slice row holds one field per dimension and attribute, keyed by name, so a join key reads them directly. A slice row has no `id` column.

```sql
-- Join slice cells to a collection on a dimension value
SELECT c.x, c.y, c.attr1, s.name
FROM ARRAY_SLICE('spatial_data', '{x: [0, 999], y: [0, 999]}', ['attr1']) AS c
JOIN sites s ON c.x = s.grid_x;
```

See [Architecture — Cross-engine identity](architecture.md#cross-engine-identity) for details.

## Performance

- **Tile-level parallelism** — Each tile is read/processed in parallel on separate cores
- **Compression** — Typical 5-20x compression depending on data homogeneity
- **Range queries** — Z-order indexing provides cache-friendly access; skip irrelevant tiles via block statistics
- **Sparse data** — Only materialized cells stored; implicit zeros and empty regions not persisted

## Temporal Purge and Compliance

System time–based retention enables GDPR and data minimization compliance:

```sql
ALTER ARRAY spatial_grid SET (audit_retain_ms = 86400000);  -- keep 1 day

-- Tiles older than now - 1 day are candidates for purge
-- Purge is automatic during compaction
```

Purged tiles are irreversibly removed; historical queries beyond the retention window will see gaps.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine bitemporal architecture
- [Architecture — Cross-engine identity](architecture.md#cross-engine-identity) — Surrogate bitmap linking
- [Columnar](columnar.md) — Related structured analytics engine

[Back to docs](README.md)
