# P2 Cluster Consensus Safety — DeepSeek V4 Pro Review

Scope: review performed on the supplied digest at `461c3ad`; I did not have the full tree. Where the digest does not contain decisive evidence, findings are marked **UNCONFIRMED** and the missing evidence is called out explicitly.

---

## #161 Raft HardState persistence — PARTIAL — HIGH

Original audit finding (“staged but never durably consumed”) is **refuted by current code paths cited**, but full durability/mutation coverage is **not proven from this digest**.

- **Evidence**
  - `nodedb-cluster/src/raft_loop/handle_rpc/consensus.rs:17-33` — `handle_append_entries_rpc` and `handle_request_vote_rpc` call `mr.persist_group_hard_state(req.group_id)?` **before** returning the RPC response.
  - `nodedb-cluster/src/multi_raft/rpc_dispatch.rs:76-81` — `persist_group_hard_state` reaches `node.persist_hard_state_if_dirty()`.
  - `nodedb-raft/src/node/core.rs:196-202` — `persist_hard_state_if_dirty` synchronously calls `save_hard_state` and clears the dirty flag.
  - `nodedb-cluster/src/raft_loop/tick/dispatch_outbound.rs:80,121` and `snapshot_dispatch.rs:95` — persistence is also invoked from outbound tick dispatch.

- **Gap analysis**
  - The digest does not prove `Ready.hard_state` is set on **every** replay-hidden HardState mutation: `become_follower`, `become_candidate`, `term_bump`, `vote_grant`, or leader stepping down.
  - No test is cited for “vote A, crash, restart, must not grant B in same term.”
  - No production proof is cited that `save_hard_state` in `nodedb-wal` actually calls `fsync`/`fdatasync` before returning success.

- **Fix design**
  - In `nodedb-raft/src/node/core.rs`, centralize all HardState mutation behind a helper such as `set_hard_state(new_state)` that always sets the dirty flag and schedules `Ready.hard_state`; add `debug_assert!(ready.hard_state.is_some())` on every term/vote transition.
  - In the production WAL backend, make `save_hard_state` guarantee durability: issue `fsync`/`fdatasync` before returning, and document it as blocking.
  - Add an explicit RPC error contract: if `persist_group_hard_state` fails, the RPC reply must carry a retryable/internal error and must not indicate grant/append success.

- **Test design**
  - Unit: start node A, grant vote to A in term 1, crash, restart from WAL, send `RequestVote` from B in term 1, assert rejection.
  - AppendEntries: replicate entry index 5, crash after `save_hard_state` returns, restart, assert entry and term persisted.
  - Durability: write to a temp WAL, call `save_hard_state`, immediately kill the process before any further write, reopen, assert state survives.
  - Fault injection: make `save_hard_state` fail; assert RPC response is an error and no vote grant is recorded.

- **v0.6**
  - This should be completed before v0.6 because it is safety-critical. Current code appears safe, but the regression test and fsync audit should land with the release.

---

## #162 InstallSnapshot advances index without data — PARTIAL — BLOCKER v0.6

The code digest shows the simple “advance index on receipt” path is likely gone, but **atomic index advancement after engine apply is not proven**.

- **Evidence**
  - `nodedb-cluster/src/install_snapshot/receiver.rs:84-137` — chunked receiver with `PartialSnapshotState`: `group_id`, `leader_id`, `term`, `last_included_index/term`, `next_expected_offset`, `running_crc`.
  - `install_snapshot/state.rs` and `finalize.rs` — partial-state machine and commit path with whole-snapshot CRC.
  - `nodedb-raft/src/node/rpc/install_snapshot.rs:26-65` — term checks for snapshot RPCs.
  - `sender.rs` — chunk framing via `encode_snapshot_chunk`.

- **Gap analysis**
  - There is no cited point where `last_included_index` becomes visible to the Raft log. It is still possible that Raft updates log metadata before the data-group engine apply completes.
  - The digest does not show a crash before log update after engine apply, nor idempotent recovery if the engine write succeeds and the Raft metadata write fails.
  - No test asserts: follower receives snapshot, index advances, query returns snapshot data.

- **Fix design**
  - In `nodedb-cluster/src/install_snapshot/finalize.rs`, implement a strict two-phase finalization:
    1. validate whole-snapshot CRC;
    2. apply snapshot data to the data-group engine;
    3. only then update Raft applied/commit and `last_included_index`.
  - Keep `last_included_index` pending in `PartialSnapshotState` and never publish it from `receiver.rs`.
  - If the Raft metadata update fails after the engine apply, recovery must make the operation idempotent: after restart, detect the already-applied snapshot and re-publish `last_included_index`.
  - Add a crash point between engine apply and Raft index update.

- **Test design**
  - Snapshot apply+query test: send snapshot to a stale follower; before finalization, query old state; after finalization, query returns snapshot data.
  - Index visibility test: assert `last_included_index` equals the snapshot index only **after** engine apply returns.
  - Crash-before-index-publish test: kill the node after engine apply but before Raft metadata update; restart and assert either old state + old index, or new state + new index, but never new index + old state.
  - Bad-CRC test: finalization rejects the snapshot and deletes partial chunks.

- **v0.6**
  - Blocking. This is exactly the type of silent divergence the epic exists to prevent.

---

## #163 Deposed/partitioned leader serves stale Strict reads — VERIFIED — BLOCKER v0.6

- **Evidence**
  - `nodedb-cluster/src/follower_read.rs:59-70` — `Strong` read path is `can_serve_locally = is_leader_fn(group_id)`.
  - No ReadIndex heartbeat, quorum check, or leader lease is mentioned.
  - Raft leader status does not automatically become false merely because a leader is partitioned from the majority; there is no cited `CheckQuorum` stepdown.

- **Gap analysis**
  - A deposed leader partitioned from the majority can continue believing it is leader and serve stale Strong reads from local state.
  - It is unclear whether `is_leader_fn`/group status ever flips on partition, and if so, after how long.

- **Fix design**
  - Add **ReadIndex** for all Strong reads:
    - Leader sends a heartbeat/AppendEntries barrier to a majority and receives quorum acks.
    - Leader waits until its applied index is at least the read index obtained from the barrier.
    - Only then serves the read locally.
  - Or implement a leader lease bound by election timeout and HLC/skew; the leader serves Strong reads only while the lease is fresh.
  - Add **CheckQuorum**: if a leader cannot contact a majority within an election timeout, it steps down to follower. This also makes `is_leader_fn` false.
  - If neither ReadIndex nor lease is implemented before v0.6, Strong reads should **fail closed** rather than serve from local state without quorum evidence.

- **Test design**
  - 3-node cluster; isolate leader A from majority.
  - Before isolation, Strong reads succeed.
  - After isolation and CheckQuorum/lease expiry, Strong reads on A must fail or return `NotLeader`; they must not return stale local data.
  - Rejoin A after new leader elected; A must not serve Strong reads until it is re-elected.
  - ReadIndex quorum loss test: with network partition, leader attempts ReadIndex; read fails closed.

- **v0.6**
  - Blocking. “Linearizable reads are actually linearizable” cannot be claimed otherwise.

---

## #164 BoundedStaleness uses time-since-last-apply rather than leader lag — VERIFIED — HIGH

- **Evidence**
  - `nodedb-cluster/src/closed_timestamp.rs:104-110` — `is_fresh_enough` checks `last.elapsed() <= max_staleness`.
  - `fold_remote_hlc` is only merged on the apply path; no periodic heartbeat watermark is cited.

- **Gap analysis**
  - `last.elapsed()` resets when the node applies any local batch, even if that batch corresponds to arbitrarily old log state. A follower catching up from a long backlog may look “fresh” while still far behind the leader.
  - Local wall-clock elapsed time does not measure leader-vs-follower data lag.

- **Fix design**
  - Replace local `last.elapsed()` with HLC lag:
    - Maintain `last_applied_hlc` from log entry/apply timestamps.
    - Maintain `leader_watermark_hlc` from AppendEntries/snapshot heartbeats even when there are no writes.
    - Compute freshness as `leader_watermark_hlc - last_applied_hlc <= max_staleness`, with HLC skew bounds.
  - If the leader watermark is stale or heartbeat is older than an election timeout, reject bounded reads.

- **Test design**
  - Stop AppendEntries delivery from leader to follower. Write on leader. Wait longer than `max_staleness`; bounded read on follower must reject.
  - Replay old backlog to follower without new writes; local apply wall clock is recent but HLC state is old. Bounded read must reject until fully caught up.
  - Clock-skew test: with bounded skew, leader watermark cannot appear fresher than actual.

- **v0.6**
  - Fix before exposing bounded-staleness reads as a supported isolation level; otherwise document as experimental and gate.

---

## #165 Rolling upgrade blocked: MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION — REFUTED / WONTFIX — WONTFIX

- **Evidence**
  - `nodedb-types/src/wire_version.rs:13-30` explicitly documents pre-1.0 policy: no deployed clusters, bump cannot buy a rolling upgrade, floor==ceiling is deliberate.

- **Gap analysis**
  - This is a design decision, not a safety defect.
  - The only open question is whether v0.6→v0.7 is expected to be online-upgradable. The cited documentation says no pre-1.0 upgrade compatibility.

- **Fix design**
  - No change before 1.0.
  - Before first stable release, separate floor and ceiling so a real rolling upgrade path exists.

- **v0.6**
  - Not blocking.

---

## #166 No fencing tokens anywhere in raft/cluster — VERIFIED as an absence — HIGH

- **Evidence**
  - No fencing/epoch token generation or validation is cited in Raft or cluster paths.
  - Descriptor-lease renewal is described as raw wall clock without fencing.

- **Gap analysis**
  - Raft terms already act as an entry-log fencing mechanism for normal log writes.
  - The risk is external or non-Raft side effects (descriptor leases, node lifecycle operations) that may observe a stale leader as authoritative.

- **Fix design**
  - Define a fencing token as `(term, leader_id, incarnation/boot_id)`.
  - Descriptor leases should include the fencing token and be rejected if the current leader term/epoch is newer.
  - Before granting or renewing descriptor lease, require recent quorum contact and current leadership.

- **Test design**
  - Partition lease-holding leader; wait until deposed; from isolated old leader, attempt lease renewal; assert rejection.
  - Restart a node with a different boot id while old lease record still exists; assert old fencing token cannot be reused.

- **v0.6**
  - Defer unless descriptor leases are externally visible or safety-critical. For protocol safety, log-path Raft term checks are the more important subset.

---

## #167 Scatter-gather no shard-failure/timeout guard — UNCONFIRMED — MEDIUM

- **Evidence**
  - `nodedb-cluster/src/distributed_timeseries/coordinator.rs` tracks `all_responded()`, `merge_results()`, `response_count()`.
  - The digest itself states the timeout/partial-result path needs confirmation.

- **Gap analysis**
  - If no deadline exists, a dead shard can block a distributed query indefinitely.

- **Fix design**
  - Add per-query deadline in `coordinator.rs`: `all_responded_or_deadline`.
  - Add slow-shard circuit breaking and explicit “partial result”/“degraded” error semantics.

- **Test design**
  - Kill one shard, issue scatter-gather query, assert timeout/degraded result instead of hang.

- **v0.6**
  - Defer unless distributed timeseries queries are part of the v0.6 stable API.

---

## #168 Descriptor-lease expiry uses raw cross-node wall clock — UNCONFIRMED — MEDIUM

- **Evidence**
  - Audit file path `nodedb-cluster/src/control/lease/renewal.rs:269-293` was **not found** in the current tree digest.
  - No current lease implementation file is cited.

- **Gap analysis**
  - Raw cross-node wall-clock expiry is unsafe without a bound on clock skew.
  - Actual location and implementation need verification.

- **Fix design**
  - Locate actual lease code, e.g. `rg "lease" nodedb-cluster/src`.
  - Replace raw wall-clock expiry with leader HLC or term+fencing-token checks, with a documented max-skew bound.

- **Test design**
  - Two-node clock-skew test: assert no overlapping descriptor leases are granted.

- **v0.6**
  - Defer unless descriptor leases gate data-plane reads/writes.

---

## #169 SWIM fast-restart rejoin can stick — PARTIAL — MEDIUM

- **Evidence**
  - `nodedb-cluster/src/swim/bootstrap.rs:115-140` inserts seeds with `Incarnation::ZERO`.
  - Audit says restarting at incarnation 0 can be refuted against lingering `Dead` state from the previous process.

- **Gap analysis**
  - Plausible, but the digest does not show the exact SWIM refutation rules for equal incarnation.

- **Fix design**
  - Persist SWIM incarnation high-water mark to WAL and restart at `max_persisted+1`.
  - Or use boot id in seed incarnation.

- **Test design**
  - Mark A dead at incarnation 0, kill A, restart A, assert rejoin completes despite old Dead state.

- **v0.6**
  - Should defer only if fast restart is not part of v0.6 cluster restart validation.

---

## #170 No pre-vote; no leadership transfer — PARTIAL — MEDIUM

- **Evidence**
  - `consensus.rs:4` mentions `TimeoutNow`, so `TimeoutNow` exists in the RPC set.
  - No PreVote mechanism is cited.

- **Gap analysis**
  - Partition-heal term inflation can force a healthy leader to step down.
  - Leadership transfer may exist but not be wired.

- **Fix design**
  - Add PreVote if absent to avoid term inflation on partition heal.
  - Implement leader transfer using `TimeoutNow` after target log catch-up.

- **Test design**
  - Healthy leader, isolate from quorum, heal, assert leader does not step down due to isolated candidate term inflation.
  - Leadership transfer test: request transfer to follower, assert clean stepdown and target election.

- **v0.6**
  - Defer; not a v0.6 release blocker unless term-inflation disruption is observed in normal cluster operation.

---

## #171 Snapshot GC sweep_orphans runs at startup only — REFUTED / PARTIAL — MEDIUM

- **Evidence**
  - `nodedb-cluster/src/install_snapshot/gc.rs:11,32` comment says `sweep_orphans` is called at two points in the node lifecycle.
  - The digest does not show the second call, but the comment contradicts “startup only.”

- **Gap analysis**
  - Need to confirm the second lifecycle point is actually reachable after decommission/shutdown.

- **Fix design**
  - If the second point is missing, add periodic snapshot GC and propagate `ShutdownWatch` to decommission paths.

- **Test design**
  - Create an orphan snapshot, trigger decommission, assert GC removes it without restart.

- **v0.6**
  - Defer unless decommission is a v0.6 production feature.

---

## Cross-cutting findings

1. **Missing leader quorum stepdown is a systemic issue**
   - The Strong-read bug is only one symptom. Without `CheckQuorum`, a partitioned leader may continue to act as leader indefinitely from its local perspective. This should be treated as a core Raft-node fix, not only a read-path fix.

2. **Snapshot atomicity must be specified as a state machine transition, not a chain of side effects**
   - `last_included_index`, engine state, and WAL metadata must transition as one recoverable unit.

3. **HardState persistence is on the right path, but error semantics need a contract**
   - Persist failure during vote grant/AppendEntries must never produce a success reply.

4. **Bounded-staleness freshness and leader lease freshness are conflated with local wall-clock time**
   - Node-local wall clock “last apply” is not a safe freshness signal in a distributed system.

---

## v0.6 ship decision

**Must land before v0.6:**

- #163 Strong local reads: implement ReadIndex or a leader lease, and add CheckQuorum/stepdown. Otherwise fail strong reads closed when quorum cannot be contacted.
- #162 Snapshot install: prove and test that `last_included_index` becomes visible only after data-group apply.
- #161 HardState: add crash/restart vote and append durability tests, and confirm production WAL fsync.
- #164 Bounded staleness: redesign freshness around leader HLC lag, or mark bounded staleness unsupported/experimental.

**Can defer to v0.7+:**

- #166 fencing tokens, unless descriptor leases are externally exposed.
- #167 scatter-gather timeout, unless distributed queries are stable in v0.6.
- #168 descriptor-lease raw clock, if descriptor leases are internal.
- #169 SWIM fast-restart rejoin.
- #170 PreVote/leadership transfer.
- #171 snapshot GC startup/decommission lifecycle.

**WONTFIX pre-1.0:**

- #165 mixed-version rolling upgrade support.
