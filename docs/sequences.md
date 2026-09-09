# Sequences

NodeDB sequences are CP-side, cross-core counters with PostgreSQL-style accessors: `nextval`, `currval`, and `setval`. Every value is allocated through a replicated registry, so two cores (or two statements racing on the same sequence) never hand out the same number.

## When to Use

- Surrogate keys and auto-increment columns
- Per-tenant order numbers, invoice IDs, and shard-safe counters
- Deterministic test fixtures that need reproducible value streams

## Creating and Dropping Sequences

```sql
CREATE SEQUENCE order_ids;
DROP SEQUENCE order_ids;
SHOW SEQUENCES;
```

Sequences are database-scoped. Names follow the same case rules as collections. A sequence advances per call and is never rewound by a restart: the registry is loaded from the catalog on startup and its state is replicated with it. `DROP SEQUENCE` participates in transactional DDL visibility like other catalog objects — a sequence created and dropped inside one transaction leaves nothing usable behind.

## DEFAULT Accessors — Every Engine

The primary use is filling a column per row at insert time. `DEFAULT nextval('seq')` works on every table engine (kv, columnar, document, strict) and advances once per inserted row:

```sql
CREATE SEQUENCE order_ids;

CREATE COLLECTION orders (
    id BIGINT DEFAULT nextval('order_ids') PRIMARY KEY,
    sku TEXT
) WITH (engine = 'kv');

INSERT INTO orders (sku) VALUES ('A-100'), ('A-101'), ('A-102');
-- id: 1, 2, 3
```

Rules:

- `nextval('seq')` as a DEFAULT is evaluated by the control plane per row, in insertion order.
- `currval('seq')` and `setval('seq', n)` as a DEFAULT are rejected loudly (a stateful DEFAULT must name the value source, not query it). Malformed defaults — `nextval('')`, extra arguments — raise a plan error instead of silently NULLing.
- A DEFAULT that names a missing sequence fails the insert with a plan error naming the sequence.

## Constant Contexts — Real Evaluation

`nextval`/`currval`/`setval` are real expressions wherever a statement has no row scope: FROM-less `SELECT`, and explicit `VALUES` cells.

```sql
SELECT nextval('order_ids');          -- 1, then 2 on the next execution
SELECT currval('order_ids');          -- the value handed out last in this session
SELECT setval('order_ids', 41);       -- sets and returns 41; the next nextval returns 42

INSERT INTO orders (id) VALUES (nextval('order_ids')), (nextval('order_ids'));
-- advances per VALUES cell, in order
```

Semantics:

- Multiple accessors in one statement evaluate in expression order.
- `setval` takes `(name, value)`; the following `nextval` returns `value + 1`.
- `EXPLAIN SELECT nextval('seq')` plans without advancing — planning is side-effect-free, matching PostgreSQL.
- A statement that folds an accessor at plan time is never admitted to the physical-plan cache; every execution re-plans and advances.

## Row-Scope Contexts — Loud, Typed Errors

Evaluating a stateful accessor once per row would require a control-plane round-trip per row, which the executor deliberately does not have. Anywhere a row scope exists, accessors raise SQLSTATE `0A000` (`feature_not_supported`) instead of silently evaluating to NULL:

```sql
SELECT nextval('order_ids') FROM orders;   -- 0A000
SELECT id FROM orders WHERE nextval('order_ids') > 0;  -- 0A000
SELECT id FROM orders ORDER BY nextval('order_ids');   -- 0A000
UPDATE orders SET sku = nextval('order_ids');          -- 0A000
```

This is a deliberate boundary, not an accident: a missing sequence or a malformed accessor in a row-scope context would otherwise surface as a silent NULL per row. The error text names the boundary (`sequence accessors are supported as column DEFAULTs; SELECT-time evaluation is not yet wired`) so the failure mode is self-describing.

## Error Classes

| Situation | SQLSTATE |
|---|---|
| DEFAULT `nextval` per row | — (works) |
| DEFAULT `currval`/`setval`/malformed | plan error (42601) |
| Row-scope accessor (SELECT list, WHERE, ORDER BY, UPDATE SET, JOIN ON, HAVING, GROUP BY) | `0A000` |
| Missing sequence in a constant context | plan error naming the sequence |
| Missing sequence in a DEFAULT | plan error naming the sequence |

## Notes

- The registry is per-database, not per-collection: two collections can share one sequence.
- Sequence values are `BIGINT`. `currval` returns the value this session's registry last produced for that sequence; the registry overlay is connection-scoped, so a session that has produced no value yet behaves per-session rather than reading another session's last value.
