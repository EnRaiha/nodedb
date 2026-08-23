# P2 GAP-4 Double Review — commit_index restore

**Status: RESOLVED (implemented + committed)** — recovered from session history
2026-08-23 (kilo session, Maya flash + DS V4 Pro).

## Claim under review (GLM 5.3, GAP-4)

> Commit Index boleh regress selepas restore — `core.rs restore()` sets
> volatile.commit_index=0 even when durable_applied is high → LogCompacted
> error; latency after restart.

## Evidence from code (working tree @ 461c3ad)

1. `state.rs:95-100` — `VolatileState::restored(applied_index)` =
   `{ commit_index: 0, last_applied: applied_index }`. Doc comment:
   commit_index legitimately starts at 0 and is re-learned from the leader.
2. `core.rs:124-136` — `restore()`: hard_state = load_hard_state;
   durable_applied = load_applied_index; volatile = `VolatileState::restored(durable_applied)`; log.restore().
3. `log.rs:59-70` — `term_at(index)`: index==0 → Some(0); index==snapshot_index
   → Some(snapshot_term); index<snapshot_index → None (Compacted); else entry_at.
4. `internal.rs` `try_advance_commit_index` — loops n in (commit_index+1..=last).rev(),
   `term_at(n)` → **None => continue (SKIPS compacted, NOT an error)**; then
   requires term_at(n) == current_term.
5. `core.rs:237-240` — `save_durable_applied_index`: monotonic no-op at/below
   floor; called from apply loop (`rpc_dispatch.rs:153`) and
   `install_snapshot.rs:61`.
6. `core.rs:209` — `advance_applied` = delivery watermark (may be ahead of durability).

## Verdict

| Claim | Verdict |
|---|---|
| GLM: "LogCompacted error" | **REFUTED** — `term_at()` returns None for compacted, caller `continue`s (internal.rs). No error path exists. |
| commit_index=0 after restart | **Documented design** (state.rs:93-94) — re-learned from leader. |
| Seeding commit_index = durable_applied | **SAFE** — `append_entries.rs:60-61` uses `>` guard (monotonic, no regression possible); `durable_applied <= commit_index` always holds since apply only runs on committed entries. |
| Severity | Improvement (avoids full-log scan after restart), NOT a must-have bug. |

DS V4 Pro attempt: over-reasoned 71,365 chars without producing content
(finish_reason=length, reasoning quota exhausted) — skipped; verdict above is
grounded in direct code evidence (flash analysis).

## Implementation (TDD)

- RED test: `restore_seeds_commit_index_from_durable_floor` —
  `nodedb-raft/src/node/core.rs:812` (restore with durable_applied=2 asserts
  `commit_index() >= 2`, then normal advance above floor still works).
- Fix: `VolatileState::restored` seeds `commit_index: applied_index` —
  `nodedb-raft/src/state.rs:100-104`.
- Invariant preserved: `commit_index = max(durable_applied, learned leader_commit)`.

## References

- state.rs:93-104 — VolatileState::restored + doc
- log.rs:59-70 — term_at compacted semantics
- internal.rs:290-345 — try_advance_commit_index (continue on compacted)
- append_entries.rs:60-61 — monotonic `>` guard on leader_commit
- core.rs:124-136 — restore(); core.rs:209 — advance_applied; core.rs:237-240 — save_durable_applied_index
