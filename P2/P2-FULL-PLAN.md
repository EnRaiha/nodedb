# P2 Full Review + Plan

Branch: `p2-rebased-v2` · Base: `main` @ `0f625a6` · 9 commits · 18 files (+5056/−8)
Date: 2026-08-24 · Reviewer: Maya (DeepSeek V4 Pro) · Status: REVIEW DONE, PLAN READY

## 1. Review Verdict

Every code change in `main..HEAD` read and call-traced. Heavy test: 7 full runs,
raft 105+9+7+8, cluster all-features 1015, clippy `-D warnings` 0.

### 1.1 Sound (verified, no action)

| File                                              | Change                                                                                                      | Verdict                                                                                                                                |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `nodedb-raft/src/state.rs`                        | `restored()` seeds `commit_index = applied_index` (was 0)                                                   | SAFE — apply only runs on committed entries; monotonic guard in append_entries; GAP-4 double review refuted "LogCompacted error" claim |
| `nodedb-raft/src/node/internal.rs`                | check-quorum refresh in `try_advance_commit_index`; `last_quorum_contact=None` on step-down / become_leader | CORRECT — voters-only count, `match_index >= last-1` floor, reset on role change                                                       |
| `nodedb-raft/src/node/membership.rs`              | `promote_learner` documented + pinned as unconditional apply-path; 2 tests                                  | CORRECT — catch-up gate must live on propose side (apply-time commit_index already advanced)                                           |
| `nodedb-cluster/src/multi_raft/conf_change.rs`    | propose-side `LearnerNotCaughtUp` guard (Leader role, match_index < commit_index)                           | CORRECT placement + error variant carries match/commit for diagnosis                                                                   |
| `nodedb-cluster/src/error.rs`                     | `LearnerNotCaughtUp` variant                                                                                | OK                                                                                                                                     |
| `nodedb-raft/tests/election.rs`                   | `restart_does_not_double_vote_in_same_term` — replicate #161                                                | STRONG — uses real persist path then `restore()`                                                                                       |
| `nodedb-cluster/src/install_snapshot/finalize.rs` | #162 proof: RecordingApplier with fail injection; data-before-advance + failure-does-not-advance            | STRONG — proves cluster-side commit() ordering                                                                                         |
| `nodedb-cluster/src/multi_raft/membership.rs`     | `group_quorum_lease_valid` wrapper                                                                          | OK as API (see F2)                                                                                                                     |

### 1.2 Findings

**F1 — lease init timing (minor, logic).**
`become_leader()` sets `last_quorum_contact = None`; the leader tick then bootstraps
`Some(now)` on first tick. Comment says "first heartbeat round-trip establishes it".
Reality: an election WIN is itself quorum contact (just won quorum votes), so the
lease is valid from win time. Fix: initialize `Some(now)` directly in `become_leader()`,
update both comments, and flip the fresh-leader lease test from "no lease" to
"lease valid right after win". 1 commit.

**F2 — leader lease not wired to the read path (integration gap). RESOLVED — claim verified, reviewer rebuttal refuted.**
`quorum_lease_valid` / `group_quorum_lease_valid` have zero production callers.
Main already serves Strong reads via ReadIndex quorum confirmation —
verified with file+line+commit evidence (2026-08-24):

| Evidence   | Location                                                                                                                                                                                                               |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Caller     | `nodedb/src/control/cluster/read_index.rs:66` → `nodedb_cluster::confirm_read_index(&self.multi_raft, group_id, timeout)`                                                                                              |
| Waiter     | `nodedb-cluster/src/read_index_wait.rs` — poll quorum, refuse on `ReadIndexNotLeader` / `LeadershipLost`                                                                                                               |
| Raft probe | `nodedb-raft/src/node/read_index.rs` — `start_read_index` (leader-only), `read_index_confirmed`                                                                                                                        |
| Origin     | ALL from main: `f9910444b` "confirm quorum leadership before serving strong reads" (23-08 06:51), `b1f52e9a4` "serve bounded-staleness reads" (23-08 08:37), `dd91eed70` (PreVote, raft probe) — none in our 9 commits |

An external review claimed this was unverifiable/hallucinated because
`P2-VERIFICATION.md` §1.3 says "0 match ReadIndex in nodedb-raft/src/" and
`P2-SOURCE-CODE.md` snapshots `461c3ad` (07-08). Both are STALE: the audit ran
before main added ReadIndex (23-08). Resolution tasks in Phase 2A item 4.

Conclusion unchanged: epic High #3 CLOSED by main ReadIndex + our check-quorum
step-down. Lease = dormant fast-path (perf follow-up), do NOT block P2.

**F3 — P3/P4/P6 review docs ride the branch.**
`P3-BACKUP-RESTORE-PITR-REVIEW.md` (299), `P4-MULTI-TENANT-REVIEW.md` (152),
`P6-CONFORMANCE-REVIEW.md` (296) are other phases' reviews carried by the original
P2 commits. Decision: split into a docs-only PR so the P2 PR stays code-focused.

### 1.3 External review resolution (2026-08-24)

An independent review of this plan raised 7 points. Resolution:

| #   | Reviewer point                                           | Verdict                | Evidence / action                                                                                                                                                           |
| --- | -------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R1  | F2 ReadIndex claim unverifiable, possibly hallucinated   | REJECTED               | 3 files exist + 1 production caller + 3 origin commits (main, 23-08). Reviewer read stale docs (P2-SOURCE-CODE @ 461c3ad 07-08)                                             |
| R2  | Review methodology: source given is pre-fix              | ACCEPTED               | P2-SOURCE-CODE.md snapshots 461c3ad; fixes not visible. Task 2A-4: refresh/annotate                                                                                         |
| R3  | #161 "all mutation paths persist" unproven               | REJECTED (partial)     | 9469296c2 + call sites: consensus.rs:23 (inbound vote), dispatch_outbound.rs:88/129/176 (outbound), snapshot_dispatch.rs:95, core.rs:251                                    |
| R4  | check-quorum `last-1` floor heuristic may false-negative | ACCEPTED (noted, safe) | False negative = safe step-down + re-elect; no-op ack establishes contact in one round-trip; test `leader_with_recent_quorum_contact_stays_leader` pins healthy-leader case |
| R5  | Crash recovery tests missing                             | ACCEPTED               | Task 2A-3: step-down-then-rejoin + config-change-crash tests                                                                                                                |
| R6  | Docs (.rdx) updates missing from deliverables            | ACCEPTED               | Task 2A-5: consistency.rdx + multi-raft.rdx                                                                                                                                 |
| R7  | Multi-group safety not addressed                         | ACCEPTED (monitor)     | Note added, Phase 2E item 14                                                                                                                                                |

## 2. Epic Mapping (#165)

| Epic item                            | Status                                                                                                        | By        |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------- | --------- |
| #161 HardState persist               | DONE — main 9469296c2 + our proof test                                                                        | main + P2 |
| #162 InstallSnapshot order           | DONE — main finalize.rs + our 2 proof tests                                                                   | main + P2 |
| High: deposed leader stale reads     | DONE — main ReadIndex quorum-confirm + our check-quorum step-down                                             | main + P2 |
| High: BoundedStaleness lag-vs-leader | DONE — main staleness.rs (`last_applied < leader_commit → Behind`)                                            | main      |
| High: wire version rolling upgrade   | OPEN — `wire_version.rs:51` still MIN == WIRE                                                                 | —         |
| High: fencing tokens                 | PARTIAL — control (12-07), CRDT (23-07), control dispatch (23-08), lease fence token in apply_committed.rs:89 | main      |
| Med: scatter-gather guard            | DONE — b037bab06 (reject unexpected/duplicate, 30s timeout)                                                   | main      |
| Med: descriptor-lease wall clock     | PARTIAL — now `Hlc` expiry (metadata_group/descriptors/lease.rs); skew bound not explicit                     | main      |
| Med: SWIM fast-restart rejoin        | OPEN — bootstrap.rs:128 still Incarnation::ZERO                                                               | —         |
| Med: pre-vote + leadership transfer  | DONE — main (TimeoutNow ae45049bf, pre_vote)                                                                  | main      |
| Low: snapshot GC + ShutdownWatch     | PARTIAL — sweep_orphans now startup + periodic 60s; ShutdownWatch still absent (registry.rs:126)              | main      |

## 3. Full Plan

### Phase 2A — Final fixes (local, 1–2 commits)

1. F1: lease init at win-time in `become_leader()` + comments + test flip
2. F2: docs-only note — "lease fast-path = follow-up perf work"; ReadIndex already guarantees safety
3. F3: split P3/P4/P6 docs → `docs/phase-reviews` PR, keep P2 PR code-focused
4. Crash recovery tests (reviewer R5):
   - `leader_steps_down_then_rejoins_quorum` — step-down on quorum loss, rejoin as follower, catch up from new leader
   - `config_change_propose_then_crash_before_commit` — restart, uncommitted conf-change entry, no double apply
5. Docs staleness fix (reviewer R2): annotate P2-SOURCE-CODE.md as pre-fix snapshot @ 461c3ad; update P2-VERIFICATION.md §1.3 — ReadIndex exists now (main f9910444b/b1f52e9a4, 23-08), audit grep was pre-refactor
6. Docs deliverable (reviewer R6): consistency.rdx (Strong = leader read gated by quorum lease/ReadIndex) + multi-raft.rdx (check-quorum + lease description)
7. Gate: heavy test (3× full loop) + clippy `-D warnings` → 0 errors before push

### Phase 2B — Push + PR + CI

5. Push `p2-rebased-v2` → fork EnRaiha/nodedb
6. PR → NodeDB-Lab/nodedb, body: commit→epic mapping + test evidence (7 runs, 1015, clippy 0)
7. Copilot reviewer loop: fix → reply with SHA → second round (skill: rust-workspace-contribution)

### Phase 2C — Epic sync

8. Comment on #165: full status table (5 DONE / 4 PARTIAL / 2 OPEN) + PR link + P2-VERIFICATION.md reference

### Phase 2D — Merge → unblock nodedb-lite

9. After merge: rerun CI on nodedb-lite PRs #17/#18/#20 (Farhan intel: lite CI problem tied to nodedb main state)
10. If green: resume review loop + merge pending lite PRs

### Phase 2E — Remaining epic items (maintainer decisions)

11. Wire version rolling upgrade (High #5) — release-policy decision, needs habibtalib input
12. SWIM rejoin fix (Medium #9) — small scope, can self-drive
13. Fencing coverage + lease HLC skew bound — monitor main
14. Multi-group safety (reviewer R7) — cross-group txn during leadership transition; monitor, covered by P2-POST-ROADMAP TEST-3

### Phase 2F — Post-P2 roadmap (P3+/v0.7+, NOT a P2 blocker)

See `P2/P2-POST-ROADMAP.md` — perf + test architecture evolution:

| Area        | Items                                                                                                                                                                    | Top priority                                                     |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Performance | PERF-1 per-group lock (DashMap), PERF-2 commit index O(k log k), PERF-3 per-tick allocs, PERF-4 Ready pooling, PERF-5 ConfigChange binary, PERF-6 group commit fsync     | PERF-2 (S effort, ~10,000x hot path), PERF-1 (M, linear scaling) |
| Testing     | TEST-1 deterministic simulation harness (SimClock), TEST-2 proptest invariants (state machine / election safety / log matching), TEST-3 Jepsen black-box linearizability | TEST-1 (foundation) → TEST-2 → TEST-3                            |

Suggested order: TEST-1 → TEST-2 → PERF-2 → PERF-1 → TEST-3 → PERF-6 → PERF-3 → PERF-4 → PERF-5

## 4. Verification Gates

- [x] Heavy test: 7 full runs, 0 failures (2026-08-24)
- [x] Cluster all-features: 1015/1015
- [x] Clippy `-D warnings`: 0
- [ ] 2A fixes re-verified: 3× loop + clippy
- [ ] PR CI green (NodeDB-Lab CI, not fork)
- [ ] Epic #165 comment posted

## 5. Open Decisions

| #   | Question                  | Options                         | Recommendation                           |
| --- | ------------------------- | ------------------------------- | ---------------------------------------- |
| D1  | F2: wire lease now?       | wire fast-path / docs follow-up | docs follow-up — ReadIndex covers safety |
| D2  | F3: split docs?           | split / keep                    | split — code-focused PR                  |
| D3  | 2E items: in P2 or later? | P2.5 / defer to P3              | wire version: defer (policy); SWIM: P2.5 |
