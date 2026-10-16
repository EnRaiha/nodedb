# Spatial Engine

NodeDB's spatial engine provides native geospatial indexing and querying — R\*-tree indexes, OGC predicates, geohash and H3 hexagonal indexing, and hybrid spatial-vector search. It runs identically across Origin, Lite, and WASM.

Spatial is a **columnar profile**. Collections with a `SPATIAL_INDEX` column modifier store data in the same `columnar_memtables` as plain columnar collections. The R\*-tree is maintained as a secondary index over the geometry column; full scans (no spatial predicate) read directly from the columnar memtable.

## When to Use

- Fleet tracking and logistics (GPS + timeseries)
- Location-based search and recommendations
- Geofencing and proximity alerts
- Map-based applications
- Any workload combining location with other data types

## Key Features

- **R\*-tree index** — Bulk loading, nearest neighbor, range queries
- **Geohash** — Encode/decode, neighbor cells, area covering
- **H3 hexagonal index** — Uber's H3 via h3o for uniform-area spatial binning
- **OGC predicates** — `ST_Contains`, `ST_Intersects`, `ST_Within`, `ST_Disjoint`, `ST_DWithin`, `ST_IsValid`
- **Geometry constructors** — `ST_Point`, `ST_MakePoint`, `ST_GeomFromText` (WKT), `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`, `ST_MakeLine`, `ST_MakePolygon`, `ST_MakeEnvelope`
- **Accessors and measures** — `ST_AsText`, `ST_AsGeoJSON`, `ST_X`, `ST_Y`, `ST_GeometryType`, `ST_NPoints`, `ST_SRID`, `ST_SetSRID`, `ST_Transform`, `ST_Distance`, `ST_Length`, `ST_Perimeter`, `ST_Area`
- **Geometry operations** — `ST_Buffer`, `ST_Envelope`, `ST_Centroid`, `ST_Union`, `ST_Intersection`
- **Format support** — WKB, WKT, GeoJSON interchange. GeoParquet v1.1.0 + GeoArrow metadata.
- **Hybrid spatial-vector** — Spatial R\*-tree narrows candidates by location, then HNSW ranks by semantic similarity in one query
- **Spatial join** — R\*-tree probe join between two collections
- **Distributed** — Scatter-gather across shards, shard routing by geometry, geofencing

## Examples

```sql
-- Spatial peer engine with automatic R*-tree on the geometry column
CREATE COLLECTION restaurants (
    location GEOMETRY SPATIAL_INDEX,
    name VARCHAR,
    cuisine VARCHAR,
    rating FLOAT
) WITH (engine='spatial');

-- Alternatively, add a spatial index to a document collection
CREATE COLLECTION restaurants;
CREATE SPATIAL INDEX ON restaurants FIELDS location;

-- Insert with GeoJSON geometry
INSERT INTO restaurants {
    name: 'Sushi Place',
    location: { type: 'Point', coordinates: [-73.985, 40.748] },
    cuisine: 'japanese',
    rating: 4.5
};

-- Insert with geometry constructors (WKT, WKB, or coordinates)
INSERT INTO restaurants (name, location) VALUES
    ('Taco Stand', ST_MakePoint(-73.982, 40.742)),
    ('Pho House',  ST_GeomFromText('POINT(-73.978 40.751)')),
    ('Deli',       ST_GeomFromWKB(X'01010000000000000000000040000000000000F03F'));

-- Find restaurants within 1km
SELECT name, rating, ST_Distance(location, ST_Point(-73.990, 40.750)) AS dist_m
FROM restaurants
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 1000)
ORDER BY dist_m;

-- Geofencing: find all points within a polygon
SELECT name FROM restaurants
WHERE ST_Within(location, ST_GeomFromGeoJSON('{
    "type": "Polygon",
    "coordinates": [[[-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74.0, 40.8], [-74.0, 40.7]]]
}'));

-- Hybrid spatial-vector: nearby AND semantically similar
SELECT name, vector_distance(embedding, $query_vec) AS similarity
FROM restaurants
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 2000)
  AND embedding <-> $query_vec
LIMIT 10;

-- H3 hexagonal binning
SELECT h3_latlngtocell(40.748, -73.985, 9) AS hex;

-- Spatial join
SELECT r.name, z.zone_name
FROM restaurants r, delivery_zones z
WHERE ST_Contains(z.boundary, r.location);
```

## Geometry Functions

Every geometry function works in every position a geometry expression may
appear: an `INSERT` value, a `SELECT` projection, and a spatial predicate's
query-geometry argument. Constructors nest, so `ST_Buffer(ST_Point(...), 500)`
is a valid search area.

A string literal in geometry position is read as either WKT or GeoJSON, so
`ST_DWithin(loc, 'POINT(1 2)', 500)` and
`ST_DWithin(loc, '{"type":"Point","coordinates":[1,2]}', 500)` are equivalent.

### Constructors

| Function | Result |
| --- | --- |
| `ST_Point(lng, lat)` | Point |
| `ST_MakePoint(x, y [, z])` | Point |
| `ST_GeomFromText(wkt [, srid])` | Geometry parsed from WKT |
| `ST_GeomFromGeoJSON(json)` | Geometry parsed from GeoJSON |
| `ST_GeomFromWKB(bytes [, srid])` | Geometry parsed from WKB; accepts an `X'...'` literal or its hex string |
| `ST_MakeLine(point, point, ...)` | LineString |
| `ST_MakePolygon(ring, ...)` | Polygon, each ring an array of `[lng, lat]` pairs |
| `ST_MakeEnvelope(min_lng, min_lat, max_lng, max_lat)` | Rectangular Polygon |

### Accessors

| Function | Result |
| --- | --- |
| `ST_AsText(geom)` | WKT rendering |
| `ST_AsGeoJSON(geom)` | GeoJSON rendering |
| `ST_X(geom)` / `ST_Y(geom)` | Ordinate of a Point; NULL for any other geometry |
| `ST_GeometryType(geom)` | Type name, e.g. `Point` |
| `ST_NPoints(geom)` | Total vertex count |
| `ST_SRID(geom)` | SRID of the geometry; `4326` unless the geometry carries another SRID. See [Spatial reference systems](#spatial-reference-systems) |
| `ST_IsValid(geom)` | Whether the geometry is well-formed |

### Spatial reference systems

Every geometry carries an SRID. The default is `4326` (WGS 84, longitude and latitude in degrees). GeoJSON reads and writes a `crs` member for any SRID other than `4326`.

| SRID | System | Unit |
| --- | --- | --- |
| `4326` | WGS 84 geographic | degrees |
| `32647` | UTM zone 47N on WGS 84 | metres |
| `32648` | UTM zone 48N on WGS 84 | metres |
| `3375` | GDM2000 Peninsula RSO | metres |
| `3376` | GDM2000 East Malaysia (Borneo) RSO | metres |

The set is closed. Any other SRID is refused.

| Function | Result |
| --- | --- |
| `ST_SRID(geom)` | SRID of the geometry |
| `ST_SetSRID(geom, srid)` | Geometry relabeled with `srid`. Coordinates stay as they are. |
| `ST_Transform(geom, srid)` | Geometry with every coordinate reprojected into `srid` |

`ST_GeomFromText` and `ST_GeomFromWKB` take an optional `srid` argument. `ST_SetSRID` and `ST_Transform` return NULL when `srid` is NULL.

```sql
SELECT ST_Transform(ST_Point(101.7117, 3.1579), 32647);
```

Geometry functions read the SRID of each operand. They never transform implicitly. These cases raise a data exception with SQLSTATE `22000`:

- Two operands in different SRIDs. The error reads `operands have different SRIDs <left> and <right>; use ST_Transform`.
- An SRID outside the supported set. The error lists the supported codes.
- A coordinate outside the region the target projection is defined on.

Call `ST_Transform` on one operand to bring both into the same SRID.

A spatial index fixes its SRID at its first entry. It refuses geometries in any other SRID.

### Measures

Every measure is in meters, and square meters for area. A WGS 84 (`4326`) geometry is measured geodesically. A geometry in a projected SRID is measured in planar grid meters. `ST_Distance` needs both operands in the same SRID.

| Function | Result |
| --- | --- |
| `ST_Distance(a, b)` | Distance between two geometries |
| `ST_Length(geom)` | Total length of linear components; `0` for points and polygons |
| `ST_Perimeter(geom)` | Boundary length of areal components, holes included |
| `ST_Area(geom)` | Area of areal components, less holes; `0` for points and lines |

### Predicates and operations

| Function | Result |
| --- | --- |
| `ST_Contains(a, b)` | Whether `a` contains `b` |
| `ST_Within(a, b)` | Whether `a` lies within `b` — the converse of `ST_Contains` |
| `ST_Intersects(a, b)` / `ST_Disjoint(a, b)` | Whether the two geometries share any point |
| `ST_DWithin(a, b, meters)` | Whether the two geometries lie within a distance: geodesic for `4326`, planar for a projected SRID |
| `ST_Buffer(geom, meters [, segments])` | Geometry grown by a distance |
| `ST_Envelope(geom)` | Bounding rectangle |
| `ST_Centroid(geom)` | Centroid, weighted by the highest dimension present |
| `ST_Union(a, b)` / `ST_Intersection(a, b)` | Combined / shared geometry |

`ST_Contains`, `ST_Within`, `ST_Intersects` and `ST_DWithin` are answered from
the R\*-tree when the first argument is an indexed geometry column; the rest
evaluate per row.

```sql
-- Read stored geometry back in either interchange format
SELECT name, ST_AsText(location), ST_X(location), ST_Y(location)
FROM restaurants;

-- Measure and describe
SELECT name, ST_GeometryType(boundary), ST_Area(boundary), ST_Perimeter(boundary)
FROM delivery_zones;

-- A buffered point is a valid query geometry
SELECT name FROM restaurants
WHERE ST_Within(location, ST_Buffer(ST_Point(-73.990, 40.750), 1000));
```

## Spatial as a Peer Engine

The `SPATIAL_INDEX` column modifier designates a geometry column for automatic R\*-tree indexing. Spatial is a peer engine alongside columnar, timeseries, and the rest:

```sql
-- Spatial peer engine
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (engine='spatial');

-- Combine with TIME_KEY for fleet tracking or IoT (timeseries engine + spatial column)
CREATE COLLECTION fleet_positions (
    ts TIMESTAMP TIME_KEY,
    vehicle_id VARCHAR,
    position GEOMETRY SPATIAL_INDEX,
    speed FLOAT
) WITH (engine='timeseries', partition_by='1d');
```

**Query execution model:**

- `SELECT * FROM locations` — full scan reads from the columnar memtable directly (no R\*-tree involved)
- `SELECT * FROM locations WHERE ST_DWithin(geom, ...)` — R\*-tree narrows the candidate set, then the columnar sparse index and predicate pushdown do the final refinement

```sql
-- Bare scan: columnar memtable read, all columns
SELECT name FROM locations WHERE name LIKE 'Park%';

-- Spatial predicate: R*-tree lookup -> sparse refinement -> result
SELECT name, ST_Distance(geom, ST_Point(-73.98, 40.75)) AS dist
FROM locations
WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 500)
ORDER BY dist;
```

## Temporal Spatial Queries

The Spatial engine is an index. It points at records; it does not carry temporal columns itself. To query geometry at a point in time, attach a Spatial index to a Document collection that has `bitemporal = true`. The Document collection holds the geometry column and temporal columns; the R\*-tree narrows candidates by location; `AS OF` filtering happens at the Document layer.

See [Bitemporal Queries — Bitemporal Spatial Queries](bitemporal.md#bitemporal-spatial-queries) for a worked example.

## Related

- [Vector Search](vectors.md) — Hybrid spatial-vector queries
- [Timeseries](timeseries.md) — Combine location with time (fleet tracking)
- [Columnar](columnar.md) — Spatial is a columnar profile
- [Bitemporal Queries](bitemporal.md) — Temporal composition pattern

[Back to docs](README.md)
