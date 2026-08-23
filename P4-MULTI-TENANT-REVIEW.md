# NodeDB P4 Multi-Tenant — DeepSeek V4 Pro Review

**Review basis:** Digest and cited snippets only; no live tree access. Tree-wide grep claims that require caller tracing are marked `UNCONFIRMED` unless the digest provides call-site evidence. All fix designs assume the finding is real.

---

## 1. Per-database/tenant max_connections + max_storage_bytes + memory budgets are dead code — **UNCONFIRMED** — **HIGH** (if confirmed)

- Evidence shows setters in `registry.rs` and `ceiling.rs`, but does not show any `acquire()/try_acquire()` call in the connection-accept path or storage-write path.
- The audit itself says “need to verify”; this is the correct posture. Without a grep of the accept path and ceiling enforcement callers, we cannot conclude dead code.
- Severity would be **HIGH** because tenant isolation is the core v0.8 goal.

**Fix design (if confirmed)**  
- File: `nodedb/src/control/server/admission/registry.rs` — expose `acquire_database(db_id)` / `acquire_tenant(tenant_id)` returning an RAII `Permit` guard that releases on `Drop`.
- Wire into pgwire accept/reject: before serving a connection, call `acquire_database()`; on failure return `53300 too many connections`.
- For storage/memory ceilings, add checks in the write path (`ceiling.rs`) and allocator; use atomic usage counters, not just limits.
- Test: create tenant with `max_connections=3`; open 4 concurrent connections; assert 4th rejected; close one; assert another succeeds.

---

## 2. Per-tenant in-flight counter leaks on timeout/panic/over-budget — **UNCONFIRMED** — **HIGH** (if confirmed)

- `grep in_flight/fetch_sub/release` in `dispatch.rs:341-453` returned nothing. This is weak evidence: the counter may live in another module or in a RAII guard.
- If decrement is genuinely absent, tenant quota will drift to exhaustion under panics/timeouts, enabling a denial-of-service against that tenant.

**Fix design (if confirmed)**  
- In `nodedb/src/bridge/dispatch.rs`, wrap request handling in a `struct InFlightGuard { tenant: TenantId, counter: Arc<AtomicI64> }`; increment immediately after admission, decrement in `Drop`. Register the guard before any `await` or fallible operation so panic/unwind releases it.
- Test: fire a task that panics or times out after acquiring in-flight; assert counter returns to zero. Use a metric or test-only introspection endpoint.

---

## 3. statement_timeout accepted but never enforced — **UNCONFIRMED** — **HIGH** (if confirmed)

- File moved to `nodedb/src/control/server/session/params.rs`; existence of the param does not prove enforcement.
- Need to inspect executor path for a timer or deadline check. Absence would allow a tenant to escape time-bound budgets.

**Fix design (if confirmed)**  
- In `params.rs`, validate and store `statement_timeout` in session state.
- In `core_loop/tick.rs` / executor: wrap each statement in `tokio::time::timeout` or a deadline-aware poll. For long-running operators, check a cancellation token between batches.
- Test: set `statement_timeout=1ms`; execute `SELECT pg_sleep(10)`; assert immediate timeout error, not a successful completion.

---

## 4. No mid-execution cancellation — **PARTIAL** — **HIGH**

- Single-threaded core loop (`core_loop/tick.rs:70-84`) processing a query to completion without preemption strongly suggests head-of-line blocking.
- The absence of cancellation checks would let one tenant monopolise the core, violating fairness and isolation.

**Fix design**  
- Introduce cooperative cancellation tokens into the execution engine. Check the token inside tuple-batch loops, hash-join/build phases, and blocking I/O transitions.
- Wire pgwire cancel requests to signal the token for the running query.
- Test: issue a long query; send a cancel from a second connection; assert executor returns within a bounded interval.

---

## 5. Graceful shutdown drops in-flight ring requests — **PARTIAL** — **HIGH**

- `DrainingDataPlane` exists but is reported unwired in `data/runtime.rs:308-402`.
- If the shutdown path does not transition to `Draining` and await in-flight ring requests, those requests are dropped during graceful termination.

**Fix design**  
- In `data/runtime.rs`, on SIGTERM/SIGINT: set state to `Draining`, stop accepting new requests, and wait for in-flight count (or barrier) to reach zero. Apply a drain timeout (e.g. 30s) before force-cancelling.
- Test: start a long-running request, send SIGTERM, assert request completes or is cleanly cancelled without lost state.

---

## 6. Hardcoded 1 GiB memory default, no cgroup/RAM detection — **VERIFIED** — **MEDIUM**

- `config/server/section.rs:100-102` contains a hardcoded default. No detection logic is cited.
- Severity downgraded from audit HIGH: this does not directly break isolation, but it risks severe misconfiguration in containers.

**Fix design**  
- In `config/server/section.rs`, read cgroup v2 `memory.max` or `/proc/meminfo MemTotal`; default to `min(host_cgroup, host_mem) * 0.8`. Allow explicit override.
- Test: run under a cgroup with 256 MiB limit; inspect effective config shows ≤256 MiB budget.

---

## 7. docker-compose healthcheck broken — **PARTIAL** — **LOW**

- `docker-compose.yml:19-24` likely uses `curl`; if the image is `scratch`/`distroless`, curl is absent, and `/health` may not be registered.
- Base image is not specified in digest; cannot fully VERIFY.

**Fix design**  
- Replace healthcheck with a NodeDB-native command or add `/health` to HTTP server and use `wget` if available; otherwise, install curl in the image or use Docker healthcheck via the NodeDB binary.
- Test: `docker compose up -d`; `docker inspect --format='{{.State.Health.Status}}'` returns `healthy`.

---

## Medium findings

### Live quota change resets tracked allocation to 0 — **VERIFIED** — **MEDIUM**  
- `governor.rs:182-208` appears to reset allocation on limit update. This creates a window where the governor believes 0 bytes/conn are used and allows up to 2× the new limit.

**Fix**: Preserve current usage across limit updates; adjust limit atomically. Test: saturate tenant, lower limit, assert no new admissions until usage falls below new limit.

### WFQ priorities map never reaped → hair-trigger suspend — **PARTIAL** — **MEDIUM**  
- Path may have moved to `nodedb/src/bridge/wfq.rs`; if the map is append-only, denominator grows and new/old entries skew scheduling.

**Fix**: Reap idle entries with timestamp; rebuild denominator from active entries. Test: create 1000 tenants, issue no traffic, assert denominator does not grow monotonically.

### Write-batch coalescing bypasses 8:4:2 ratio → Critical writes starve reads — **PARTIAL** — **MEDIUM**  
- `tick.rs:130-141` coalescing may combine unbounded writes, allowing a write burst to monopolise I/O.

**Fix**: Weight coalesced batch by total work/class; cap coalesce size per class. Test: Drive read/write mix, assert read latency P99 remains within SLO.

### No detection/restart of dead/DEGRADED core — **PARTIAL** — **MEDIUM**  
- `data/runtime.rs:316-319` lacks a supervisor. No heartbeat watch is cited.

**Fix**: Add supervisor thread/process that monitors core heartbeat; restart on stall. Test: kill core, assert restart within configured interval.

### No on-disk format migration; no read-only degradation on disk-full — **VERIFIED** — **MEDIUM**  
- Absence of a migration framework is a missing feature, not a correctness bug, but it blocks rolling upgrades.

**Fix**: Implement versioned format + migration steps; on disk-full, set read-only mode and reject writes. Test: fill disk, assert writes fail with clear error while reads continue.

### #153 spurious auth failures under rapid connects — **UNCONFIRMED**  
- May share governance surface with connection limits. Not enough evidence.

---

## Cross-cutting findings / NEW issues

1. **RAII missing for all admission/usage counters** — Even where counters exist, they must be RAII-guarded to survive panics/timeouts. This is a root cause spanning findings #1, #2, and #5.
2. **Tenant ID propagation in data plane** — Enforcement at connection layer may not cover storage/memory operations if tenant ID is not threaded through `data/runtime.rs`. Ensure every ring/request carries tenant ID.
3. **Governor reset bug is a standalone over-commit race** — Already listed as Medium, but it directly enables cross-tenant memory/connection over-commit.
4. **No read-only degradation for disk-full** — Missing isolation under resource exhaustion; should be P1 for production hardening.
5. **Graceful shutdown and watchdog are absence-of-supervision issues** — These are typical production-hardening gaps; the code likely has no process lifecycle strategy.

---

## P4 Multi-Tenant ship decision

**Block v0.8** until the following HIGH findings are resolved or conclusively refuted:

- #1 Tenant limits dead code (if confirmed)
- #2 In-flight counter leak (if confirmed)
- #3 statement_timeout unenforced (if confirmed)
- #4 No mid-execution cancellation
- #5 Graceful shutdown drops in-flight requests

**Required for production, but not necessarily release-blocking if worked in parallel:**

- #6 Hardcoded memory default (fix before GA)
- #7 Healthcheck (fix before GA)
- All MEDIUM findings; at minimum the quota-reset over-commit race must be fixed before v0.8 because it directly contradicts isolation guarantees.

**Deferrals:**

- On-disk format migration framework can be deferred to v0.9 if v0.8 is not intended for rolling upgrades.
- WFQ reaping can be deferred if tenant churn is low in initial v0.8 release, but must be documented.

**Release posture:** Do not ship v0.8 as multi-tenant GA with any HIGH isolation finding unresolved. If the HIGH items are refuted by caller-grep evidence, the release may proceed with the MEDIUM fixes scheduled as P1.
