# NodeDB P6 Conformance — DeepSeek V4 Pro Review

Assessed from the supplied digest and cited line refs at commit `461c3ad`; no local build performed. Unverifiable claims are explicitly marked `UNCONFIRMED`.

---

## A1 — `EdgeId::seq` always 0 causes parallel-edge collision

**Verdict:** VERIFIED  
**Severity:** **Critical** — silent data loss

**Evidence**
- `nodedb-types/src/id/edge.rs:60-95`
  - `try_first` sets `seq: 0`.
  - `try_with_seq` exists but is opt-in.
- `nodedb-lite trait_impl/graph.rs` `graph_insert_edge_impl` uses `EdgeId::try_first(...)`.
- CRDT document key is `format!("{edge_id}")`.
- Therefore two edges with same `src,dst,label` produce identical `EdgeId`, overwrite the same CRDT document, and one edge silently disappears.

This is not just generic-client correctness; it is a core graph-semantics and CRDT-durability bug.

**Fix design**
- Add a per-`(src,dst,label)` sequence allocator in the graph engine and persist it.
- Replace `EdgeId::try_first` in `graph_insert_edge_impl` with:
  - allocate `seq = sequence.next(src, dst, label)`
  - construct `EdgeId::try_with_seq(src, dst, label, seq)`
- The allocator must be crash-safe and partition-safe. The naive local counter is not sufficient if two nodes can insert concurrently after a partition.
  - Prefer an HLC-backed `u64` seq, or a CRDT-stable `(node_id, local_seq)` tuple encoded into the `seq` field.
  - Do **not** use random UUIDs for `seq` unless `EdgeId` is changed to include a 128-bit field; `seq` is not for uniqueness, it is for ordering.
- Migration for existing duplicated seq-0 edges:
  - scan graph, rewrite each duplicate `EdgeId` with allocated non-zero seq, then update references.
- Update any CRDT key/tombstone logic that assumes `format!("{edge_id}")` is unique; ensure deletes are also addressed to the correct seq.

**Test design**
- CI gate: insert three parallel edges with identical `src,dst,label`; assert all three are returned.
- CI gate: delete one parallel edge; assert the other two remain.
- Crash recovery: insert parallel edges, kill node, restart, assert counts.
- Partition test: two writers insert parallel edges concurrently; assert no collision or lost edge after merge.

**v0.9 ship decision:** **Blocker.**

---

## A2 — Binary parameter format corrupts INT4/UUID/DATE and likely panics

**Verdict:** VERIFIED  
**Severity:** **Critical** — data corruption / crash / remote DoS

**Evidence**
- `pgwire/handler/prepared/execute.rs:204-236`
- Only `NUMERIC`, `TIMESTAMP`, `TIMESTAMPTZ` are rejected.
- Binary `INT4`, `UUID`, `DATE`, and possibly `BYTEA`, `INT8`, `BOOL`, `FLOAT*` are passed to `str::from_utf8`.
- PostgreSQL binary format for `INT4` is four big-endian bytes, not UTF-8 text. This can produce `Utf8Error`, incorrect values, or non-deterministic behavior.

This also breaks `tokio-postgres` when the driver negotiates binary parameters.

**Fix design**
- In `pgwire/handler/prepared/execute.rs`, replace the `from_utf8` fallback with a decoder dispatch based on PostgreSQL type OID:
  - `INT4`: `i32::from_be_bytes`
  - `INT8`: `i64::from_be_bytes`
  - `FLOAT4`: `f32::from_be_bytes`
  - `FLOAT8`: `f64::from_be_bytes`
  - `BOOL`: `value[0] != 0`
  - `UUID`: parse 16 bytes as `Uuid`
  - `DATE`: `i32` days since `2000-01-01`
  - `TIMESTAMP`/`TIMESTAMPTZ`: `i64` microseconds since `2000-01-01`
  - `NUMERIC`: either implement PostgreSQL binary numeric or return `SQLSTATE 0A000` with clear message.
- Unknown binary OID must return `FEATURE_NOT_SUPPORTED`, never fall through to UTF-8.
- Keep text-format handling separate and explicit.
- Centralize OID-to-codec mapping; avoid duplicating OID knowledge across prepared statements and catalog code.

**Test design**
- Integration with `tokio-postgres` in binary mode:
  - `INSERT INTO t VALUES ($1::int4, $2::uuid, $3::date, $4::timestamptz)`
  - Read back and assert round-trip.
- Unit tests for each binary decoder.
- Negative test: binary `NUMERIC` returns `0A000`, not a panic.
- CI matrix entry: “tokio-postgres binary parameter mode”.

**v0.9 ship decision:** **Blocker.**

---

## A3 — `information_schema` and `pg_proc`/`pg_constraint` absent

**Verdict:** VERIFIED  
**Severity:** **High** — ORM introspection and tooling incompatibility

**Evidence**
- `pgwire/pg_catalog/dispatch.rs:60-71`
- Digest states `information_schema` plus `pg_proc`/`pg_constraint` are absent.
- ORMs rely on these catalogs for table discovery, column/type mapping, and constraint detection.

This is not merely a conformance gap; it blocks the stated ORM-introspection conformance goal.

**Fix design**
- Implement minimal PostgreSQL-compatible catalog objects:
  - `pg_catalog.pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`, `pg_proc`, `pg_constraint`, `pg_description`.
  - `information_schema.tables`, `information_schema.columns`, `information_schema.table_constraints`, `information_schema.key_column_usage`, `information_schema.referential_constraints`.
- Populate them from the internal catalog metadata on every DDL operation.
- Add built-in functions:
  - `version()`, `current_schema()`, `current_database()`, `obj_description()`, `format_type()`.
- Location: extend the dispatch logic in `pgwire/pg_catalog/dispatch.rs` to route catalog queries to these virtual tables, not to user tables.

**Test design**
- Run ORM introspection suite: Diesel, SQLx, ActiveRecord, SQLAlchemy; assert table/column discovery.
- Snapshot tests for:
  - `SELECT table_name FROM information_schema.tables`
  - `SELECT column_name, data_type FROM information_schema.columns`
  - `SELECT conname, contype FROM pg_catalog.pg_constraint`
- CI matrix entry: “ORM introspection over information_schema/pg_catalog”.

**v0.9 ship decision:** **Blocker** for the ORM certificate.

---

## A4 — Cursor “disk spill” truncates rows and SCROLL cursors pre-materialize

**Verdict:** VERIFIED  
**Severity:** **Critical** — silent data loss and OOM

**Evidence**
- `pgwire/session/state.rs:33-41`
- `nodedb/src/control/server/shared/session/cursor_spill.rs`
- Digest states only a `Vec<String>` is truncated at 100k rows; no real disk spill exists.
- `SCROLL` cursors pre-materialize all rows, causing unbounded memory.

This is a serious correctness issue: client sees less data than the query actually returned.

**Fix design**
- Replace the `Vec<String>` materializer with a `CursorStore` abstraction:
  - Forward-only cursors: incremental fetch from query result; no full materialization.
  - Both FORWARD and SCROLL cursors: spill to `tempfile::SpooledTempFile` or per-cursor temp file with row offsets.
- Enforce `work_mem`/cursor memory limit; if spill is impossible, raise an explicit error instead of truncating.
- Preserve row order and random access for `SCROLL` by writing an offset index:
  - data file: encoded rows
  - index file: `Vec<u64>` offsets or page map
- Add cleanup on cursor close, transaction end, and process exit.
- Update `pgwire/session/state.rs` and `cursor_spill.rs` accordingly.

**Test design**
- Insert 150k rows, `DECLARE c CURSOR FOR SELECT ...`, fetch all rows, assert 150k exactly.
- SCROLL test: declare 100k-row cursor with a small memory cap, fetch `ABSOLUTE 90000` then `BACKWARD 10`, assert rows.
- Assert no silent truncation: row count from cursor equals underlying table count.
- Failure injection: disk full during spill; assert error and no partial logical result.
- CI matrix entry: “Cursor >100k no truncation + SCROLL spill”.

**v0.9 ship decision:** **Blocker.**

---

## A5 — `server_version`/`version()` banner and `server_version_num` inconsistent

**Verdict:** VERIFIED  
**Severity:** **High** — driver version-sniffing and protocol negotiation break

**Evidence**
- `factory.rs:158` and `session/params.rs:115`
- `server_version` is not PostgreSQL-form.
- `server_version_num` is advertised but unset/inconsistent.

Many clients and ORMs read `server_version` and `server_version_num` immediately after connection to decide feature support, SQL quoting, and wire messages.

**Fix design**
- Emit PostgreSQL-compatible values:
  - `server_version` GUC: `"15.2"` or `"16.4"` — a concrete PostgreSQL version whose behavior NodeDB best matches.
  - `server_version_num`: integer, e.g. `150002` for 15.2.
  - `version()`: `PostgreSQL <v> (NodeDB 0.9.0, <arch>)`.
- Update `factory.rs:158` to set both `server_version` and `server_version_num` from the same constants.
- Update `session/params.rs:115` to expose the already-set value; do not leave it unset.
- Choose a compatibility version deliberately and document supported features against that version.

**Test design**
- Connect with `psql`: `SHOW server_version; SHOW server_version_num; SELECT version();`
- Assert `server_version_num` is numeric and matches `server_version`.
- CI matrix entry: “pg driver version-sniffing banner”.

**v0.9 ship decision:** **Blocker** for driver compatibility; low effort, high impact.

---

## A6 — Chaos suite: node kill, disk-full, slow-core checkpoint, mid-election crash

**Verdict:** UNCONFIRMED  
**Severity:** **High if absent** — cannot certify crash/chaos behavior without evidence

**Evidence**
- No concrete file/line refs, no failpoint inventory, no CI job references in the digest.
- The items listed in the Chaos suite are not connected to code paths in the supplied digest.

The claimed suite is credible as a requirement, but it is not visible in the cited code.

**Fix design**
- Provide explicit failpoint locations:
  - scatter-gather mid-query: failpoint before/after local fetch and before/after merge.
  - disk-full: inject write/sync failure in storage engine or temp file.
  - slow-core checkpoint: checkpoint delay injection.
  - mid-election crash: failpoint in Raft/consensus state transition.
- Use something like `fail` crate plus a CI matrix job with exact scenario names.
- Each chaos test must assert cluster heal, no acknowledged-write loss, and query results consistent with documented guarantees.

**v0.9 ship decision:** **Blocker for the chaos/certificate claim** unless the tests exist elsewhere and can be cited.

---

## A7 — “v1 known limitations” document

**Verdict:** UNCONFIRMED  
**Severity:** **Medium/process** — not a code defect but required for v0.9

**Evidence**
- No referenced file or artifact in the digest.

**Fix design**
- Add `docs/v1-known-limitations.md` listing:
  - unsupported binary types, unsupported catalog objects, cursor behavior, known concurrency limits.
- Link from release notes and conformance matrix.

**v0.9 ship decision:** Need doc before final ship; not a code blocker but an artifact requirement.

---

# NEW conformance issues visible in cited code

1. **`str::from_utf8` on binary `BYTEA`/arbitrary bytes is a panic/DoD vector**  
   Even if the driver sends opaque binary data as `BYTEA`, the current path may attempt UTF-8 conversion and fail. This is adjacent to A2 and should be fixed in the same decoder.

2. **Parallel-edge deletion tombstone risk**  
   Because multiple parallel edges can share `EdgeId` after the seq bug, a delete of one edge can tombstone the shared key and remove all parallel edges. Fixing the seq allocation must also verify tombstone semantics.

3. **SCROLL cursor memory-growth is not bounded**  
   Even with no silent truncation, the current pre-materialization path is O(number of rows). This is an OOM availability issue, not just correctness.

4. **`server_version_num` advertised but unset may be read as `NULL` or 0**  
   This can cause drivers to select deprecated protocol behavior or fail entirely; it is not just cosmetic.

5. **Missing `pg_type`/`pg_attribute` aggravates binary-format issue**  
   Without `pg_catalog.pg_type`, clients may not be able to discover parameter OIDs before binary execution; the catalog fix must land alongside the binary decoder.

---

# Cross-cutting findings

1. **The certificate matrix must be an explicit `FAIL`-if-any-previous-audit-finding-recurs` gate**  
   Map each v1.0 audit finding to a CI job and require it to run before any code merge.

2. **Silent data-loss patterns need invariant checks**  
   Edge insertion should assert `created_count == expected`, cursor fetch should assert `fetched == total`, and binary decode should never fall through to UTF-8.

3. **Failpoint architecture should live in storage and RPC, not just pgwire**  
   The current evidence shows pgwire/server code; crash injection in the storage/CRDT layer is needed for legitimate chaos tests.

4. **Type mapping is currently fragmented**  
   A centralized OID/format codec will reduce future regressions around prepared statements and catalog metadata.

5. **The conformance matrix should be run in two layers**  
   - Layer 1: protocol/catalog static gate — deterministic SQL queries with exact expected result sets.
   - Layer 2: chaos/failpoint dynamic gate — fault injection with assertions.

---

# P6 Conformance ship decision

**NO-GO for v0.9.**

Blockers:

| Item | Reason | Release impact |
|------|--------|----------------|
| A1 EdgeId `seq` collision | Silent graph data loss | Critical |
| A2 Binary parameter corruption | Wrong data/crash with common drivers | Critical |
| A4 Cursor truncation + SCROLL OOM | Silent query result loss/OOM | Critical |
| A5 Server version metadata | Driver incompatibility | High |
| A3 Missing catalogs | ORM introspection failure | High |
| A6 Chaos suite evidence | Certificate claim unsupported | High |

Deferrals after v0.9:

- Full binary `NUMERIC` decode can be shipped as explicit `0A000` error if time-constrained; it must not continue to corrupt or panic.
- Additional `pg_catalog` objects that are not required by the target ORM suite may be documented in `v1-known-limitations` and landed later.
- Full `SCROLL` with page-level random access can be staged after forward-only spill fixes, provided it no longer OOMs and no data is truncated.

**Priority ordering for the matrix/certificate phase:**

1. **A1 EdgeId `seq` — core graph correctness.**  
   Highest risk because it corrupts user data at rest.
2. **A4 Cursor truncation — silent query data loss.**  
   Direct client-facing correctness.
3. **A2 Binary parameter handling — driver corruption/crash surface.**  
   Must be fixed before binary-mode clients can be certified.
4. **A5 `server_version`/`server_version_num` — quick win, unblocks all driver configuration.**  
   Low implementation cost, immediately improves compatibility.
5. **A3 Catalog/introspection — ORM certification.**  
   Larger effort, but required for the conformance claim.
6. **A6 Chaos/failpoint suite — stability certificate.**  
   Should land only after items 1–5 are green; otherwise chaos failures will be dominated by known correctness bugs and obscure regression detection.
