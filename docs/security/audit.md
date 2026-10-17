# Audit Log & Change Tracking

NodeDB maintains a tamper-evident, hash-chained audit log. Every entry includes a SHA-256 hash of the previous entry — if any record is modified or deleted, the chain breaks.

## Viewing the Audit Log

```sql
-- Show recent entries (superuser only)
SHOW AUDIT LOG;
SHOW AUDIT LOG LIMIT 50;
```

**Columns:** `seq`, `timestamp_us`, `event`, `tenant_id`, `database_id`, `source`, `detail`

`SHOW AUDIT LOG`, `SHOW AUDIT WHERE` and `SHOW AUDIT IN DATABASE` return the newest entries first. Without `LIMIT`, each returns 100 rows. Only a superuser can run them.

The `event` column prints the snake_case event name, for example `auth_success`. `WHERE event_type = '...'` takes the same snake_case name.

A read sees every entry recorded before it starts. There is no flush delay.

`_system.audit_log` has the columns `seq`, `timestamp_us`, `event`, `tenant_id`, `source`, `detail` and `prev_hash`. Its `event` column prints the snake_case name too. It has no `database_id` column. A superuser or a role with `monitor` can read it. It returns at most the newest 100,000 entries.

Each stored entry also records `auth_user_id`, `auth_user_name`, and `session_id` for correlation.

## Audit Events

| Event                                  | Level    | Description                                    |
| -------------------------------------- | -------- | ---------------------------------------------- |
| `auth_success`                         | Minimal  | Successful authentication                      |
| `auth_failure`                         | Minimal  | Failed login attempt                           |
| `permission_denied`                    | Minimal  | Permission check failed                        |
| `authz_denied`                         | Minimal  | Authorization denied                           |
| `rls_rejected`                         | Minimal  | RLS write policy rejected a write              |
| `lockout_triggered`                    | Minimal  | Account locked after failed logins             |
| `login_rate_limited`                   | Minimal  | Too many login attempts                        |
| `session_revoked`                      | Minimal  | Open session revoked                           |
| `audit_bus_lagged`                     | Minimal  | Audit bus dropped events, rows may be missing  |
| `audit_checkpoint`                     | Minimal  | Retention prune checkpoint                     |
| `privilege_change`                     | Standard | GRANT, REVOKE, role changes                    |
| `session_connect`                      | Standard | New connection                                 |
| `session_disconnect`                   | Standard | Connection closed                              |
| `session_handle_fingerprint_mismatch`  | Standard | Session handle used from a different origin    |
| `session_handle_resolve_miss_spike`    | Standard | Many failed session handle lookups             |
| `admin_action`                         | Standard | Administrative actions                         |
| `config_change`                        | Standard | Config change                                  |
| `ddl_change`                           | Standard | DDL change committed to the metadata group     |
| `tenant_created`                       | Standard | New tenant                                     |
| `tenant_deleted`                       | Standard | Tenant removed                                 |
| `tenant_moved`                         | Standard | Tenant reassigned to database                  |
| `database_created`                     | Standard | New database                                   |
| `database_dropped`                     | Standard | Database dropped                               |
| `database_renamed`                     | Standard | Database renamed                               |
| `database_quota_changed`               | Standard | Quota limit changed                            |
| `database_cloned`                      | Standard | Database cloned                                |
| `database_mirrored`                    | Standard | Database mirrored                              |
| `database_promoted`                    | Standard | Mirror promoted to writable primary            |
| `database_materialized`                | Standard | Cloned database materialized                   |
| `database_backed_up`                   | Standard | Database backup started                        |
| `database_restored`                    | Standard | Database restored from backup                  |
| `database_audit_dml_changed`           | Standard | DML audit mode changed                         |
| `database_idle_timeout_changed`        | Standard | Idle timeout changed                           |
| `oidc_provider_changed`                | Standard | OIDC provider created, altered, or dropped     |
| `snapshot_begin`, `snapshot_end`       | Standard | Snapshot lifecycle                             |
| `restore_begin`, `restore_end`         | Standard | Restore lifecycle                              |
| `cert_rotation`                        | Standard | TLS cert rotated                               |
| `cert_rotation_failed`                 | Standard | TLS cert rotation failed                       |
| `key_rotation`                         | Standard | Encryption key rotated                         |
| `node_joined`, `node_left`             | Standard | Cluster membership                             |
| `query_exec`                           | Full     | Every query executed                           |
| `rls_denied`                           | Full     | RLS denial                                     |
| `row_change`                           | Forensic | Individual row mutations                       |
| `dml_audit`                            | Forensic | Individual DML operation (opt-in per database) |

Events at the Minimal level are recorded at every audit level.

## Audit Levels

```toml
# nodedb.toml
[audit]
level = "standard"
```

| Level      | What's recorded                                |
| ---------- | ---------------------------------------------- |
| `minimal`  | Auth events only (login, failure, denial)      |
| `standard` | + admin actions, DDL, sessions, config changes |
| `full`     | + every query, RLS denials                     |
| `forensic` | + row-level mutations, CRDT deltas             |

Higher levels include everything from lower levels.

## Per-Database Filtering

Filter audit events by database to reduce noise when operating a single database:

```sql
-- Show audit events for a specific database
SHOW AUDIT IN DATABASE prod LIMIT 50;

-- Combine with an event type filter
SHOW AUDIT IN DATABASE prod WHERE event_type = 'dml_audit' LIMIT 100;

-- Filter by event type across all databases
SHOW AUDIT WHERE event_type = 'dml_audit' LIMIT 100;
```

`SHOW AUDIT LOG`, `SHOW AUDIT WHERE` and `SHOW AUDIT IN DATABASE <db>` all take `[WHERE event_type = '<snake_case_name>'] [LIMIT n]`. `SHOW AUDIT WHERE` requires the `WHERE` clause. Any other `WHERE` form is refused with SQLSTATE `42601`, and the message names the clause. A bad `LIMIT` value or a stray token is also refused with `42601`. An unknown database name returns SQLSTATE `3D000`.

Database-scoped DDL (CLONE, MIRROR, PROMOTE, etc.) include `database_id` in the audit entry, enabling per-database filtering and forensics.

## Hash Chain Integrity

Every `AuditEntry` contains `prev_hash` — the SHA-256 of the previous entry. If an entry is modified or deleted, the chain breaks. The hash covers `database_id` when the entry has one.

Two commands check the chain. Only a superuser can run them.

```sql
VERIFY AUDIT CHAIN;
SELECT VERIFY_AUDIT_CHAIN(1, 5000);
```

- Both read the whole log: catalog rows plus the in-memory buffer.
- `VERIFY AUDIT CHAIN` checks the whole chain. It returns `status = VALID` and the entry count, or the error `XX001` `audit chain broken at sequence N`.
- `VERIFY_AUDIT_CHAIN(from, to)` checks the entries in that sequence range. Entries before `from` still feed the first link. It returns `valid`, `entries_checked`, `from_seq`, `to_seq`, `broken_at_seq`, and `last_hash`.
- `broken_at_seq` is the first sequence whose link fails. An altered entry is at that sequence or the one before it.
- The newest entry is also compared with the live tail hash.

## Retention Pruning

`audit_retention_days` prunes entries older than that age. `audit_max_entries` prunes the oldest entries beyond that count. Age pruning runs first. Both default to 0, which keeps everything.

Each prune that deletes entries first records an `audit_checkpoint` entry. The checkpoint holds `pruned_count`, `oldest_kept_seq`, and `anchor_hash`, the hash of the last deleted entry. The checkpoint is a normal chain link, so the chain still verifies after a prune. A prune that deletes nothing records no checkpoint.

## DML Audit (Opt-In)

Track individual data modifications (INSERT, UPDATE, DELETE) per database. **Disabled by default** — enable only when required for compliance or forensics, as the volume can be substantial.

```sql
-- Enable DML audit for writes only (INSERT, UPDATE, DELETE)
ALTER DATABASE prod SET AUDIT_DML = 'writes';

-- Enable all DML including reads
ALTER DATABASE prod SET AUDIT_DML = 'all';

-- Disable (default)
ALTER DATABASE prod SET AUDIT_DML = 'none';

-- View DML audit entries
SHOW AUDIT WHERE event_type = 'dml_audit' LIMIT 100;
```

**DML audit entry includes:**

| Field              | Type       | Description                        |
| ------------------ | ---------- | ---------------------------------- |
| `database_id`      | DatabaseId | Scoped to database                 |
| `tenant_id`        | TenantId   | Tenant context                     |
| `user_id`          | u32        | Who performed the operation        |
| `statement_digest` | String     | Hash of the SQL statement          |
| `collection`       | String     | Target collection name             |
| `op`               | String     | Operation (insert, update, delete) |
| `row_id`           | u32        | Surrogate key of affected row      |
| `lsn`              | u64        | Log sequence number (WAL position) |
| `timestamp`        | Timestamp  | When the operation occurred        |

**Storage cost:** Estimate ~1 KB per DML operation. For a production database with 1M writes/day, expect ~1 GB of audit storage per month.

**Implementation:** DML audit events are emitted by the Event Plane (not the request path), so audit fanout does not block writes. Events are buffered in the event bus and persisted asynchronously.

## SIEM Export

Export audit events to external security information and event management systems via CDC webhook:

```sql
CREATE CHANGE STREAM audit_export ON _system.audit
    DELIVERY WEBHOOK 'https://siem.example.com/ingest'
    WITH (format = 'json', hmac_secret = 'your-secret');
```

HMAC signatures allow the receiving system to verify event authenticity.

Database-scoped events include `database_id` field for SIEM correlation.

---

# Document Change Tracking

NodeDB does NOT have automatic `updated_at` / `created_at` columns like PostgreSQL. Here's how to track changes:

## Typeguard DEFAULT + VALUE (Schemaless)

The recommended approach for schemaless collections:

```sql
CREATE TYPEGUARD ON users (
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP VALUE now()
);
```

- `DEFAULT now()` — set once on INSERT, never overwritten
- `VALUE now()` — set on every write (INSERT, UPSERT, UPDATE)

```sql
INSERT INTO users { id: 'u1', name: 'Alice' };
-- created_at = 2026-04-10T..., updated_at = 2026-04-10T...

UPSERT INTO users { id: 'u1', name: 'Alice Updated' };
-- created_at unchanged, updated_at refreshed
```

## Strict Schema DEFAULT

For strict collections:

```sql
CREATE COLLECTION orders (
    id TEXT PRIMARY KEY DEFAULT gen_uuid_v7(),
    customer TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
) WITH (engine='document_strict');

-- created_at auto-filled on INSERT
INSERT INTO orders (customer) VALUES ('Alice');
```

## BEFORE Triggers

For custom logic:

```sql
CREATE TRIGGER set_timestamps BEFORE INSERT ON events FOR EACH ROW
BEGIN
    SET NEW.created_at = now();
    SET NEW.updated_at = now();
END;

CREATE TRIGGER update_timestamp BEFORE UPDATE ON events FOR EACH ROW
BEGIN
    SET NEW.updated_at = now();
END;
```

## CDC Change Streams

For full change history without modifying documents:

```sql
CREATE CHANGE STREAM user_changes ON users;

-- Consume changes (returns old + new values for each mutation)
SELECT * FROM CHANGES('user_changes') LIMIT 10;
```

Change streams capture INSERT, UPDATE, and DELETE events with full before/after payloads. Use consumer groups for durable, at-least-once processing.

[Back to security](README.md)
