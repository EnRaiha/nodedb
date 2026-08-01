# Security

NodeDB has a defense-in-depth security model covering authentication, authorization, encryption, and audit.

## Guides

- [Authentication](auth.md) — Users, passwords, API keys, service accounts, OIDC, mTLS
- [OIDC Single Sign-On](oidc.md) — JWT bearer authentication, claim mapping, provider setup
- [Roles & Permissions (RBAC)](rbac.md) — CREATE ROLE, GRANT, REVOKE, permission hierarchy, ClusterAdmin
- [Session Management](sessions.md) — SHOW SESSIONS, KILL SESSION, idle timeout, lockout, rate limiting
- [Row-Level Security (RLS)](rls.md) — Per-row filtering based on auth context
- [Audit Log](audit.md) — Hash-chained audit trail, database-scoped events, DML audit, SIEM export
- [Multi-Tenancy](tenants.md) — Database vs Tenant, tenant isolation, quotas, purge
- [Encryption](encryption.md) — At-rest cipher per storage tier, key management, TLS
- [Threat Model](threat-model.md) — Trust boundaries, entry points, invariants, and operational assumptions

## Database Scoping

Authentication and access control are now database-aware:

- **API keys** can be narrowed to specific databases
- **Service accounts** are created per database
- **RLS policies** can reference `$auth.database_id`
- **Session management** binds connections to one database
- **Audit events** include `database_id` for filtering
- **Admin DDL** is gated by role (ClusterAdmin for cross-database ops)

See [Authentication](auth.md), [RBAC](rbac.md), [Audit Log](audit.md), and [Session Management](sessions.md).

## Encryption (summary)

- **At rest** — AES-256-GCM for WAL and columnar/timeseries segments (per-collection KEK + per-segment SEGP envelope). Filesystem-level encryption (LUKS / dm-crypt / FileVault) covers redb catalogs and HNSW / Vamana mmap segments. Full per-tier breakdown: [`encryption.md`](encryption.md).
- **In transit** — TLS for all protocols (pgwire, HTTP, WebSocket, native)
- **Lite devices** — AES-256-GCM + Argon2id key derivation for on-device encryption

## Quick Reference

```sql
-- Create a user
CREATE USER alice WITH PASSWORD 'secret' ROLE readwrite;

-- Row-level security
CREATE RLS POLICY own_data ON orders FOR ALL
    USING (customer_id = $auth.id);

-- View audit log
SHOW AUDIT LOG LIMIT 50;

-- Typeguard-based change tracking (schemaless)
CREATE TYPEGUARD ON users (
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP VALUE now()
);
```

[Back to docs](../README.md)
