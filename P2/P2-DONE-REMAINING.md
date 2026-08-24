# P2 — Done vs Remaining (Epic #165, Cluster Consensus Safety)

Status penuh: 24-08-2026. Repo: NodeDB-Lab/nodedb. Base: `54fe575c0`.
Sumber: epic #165, drills 24-08, PR #243-#247, heavy-test verification.

---

## ✅ DONE — 11/11 epic items addressed

| Item                                  | Status                | Evidence                                                                                                                                                                            |
| ------------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Critical #161 — HardState persist     | ✅ DONE               | main `9469296c2` persist-before-reply + RED→GREEN proof test `da49bf0f2` (PR #247)                                                                                                  |
| Critical #162 — InstallSnapshot order | ✅ DONE               | main finalize.rs:106-122 apply-data-before-advance + 2 proof tests (PR #247)                                                                                                        |
| High — deposed leader stale reads     | ✅ DONE               | main ReadIndex quorum-confirm (`f9910444b`, `b1f52e9a4`) + check-quorum `052f2fa64` + leader lease `cf2cd5c24` + seed commit_index `dca42ac89` + learner gate `b215d4bba` (PR #247) |
| High — BoundedStaleness lag-vs-leader | ✅ DONE (main)        | staleness.rs:40-57 — `last_applied < leader_commit` → Behind                                                                                                                        |
| High — rolling upgrade wire bump      | ✅ DONE               | PR #244 `e60a853ae` — window [MIN=1, WIRE=2], range join gate, ClusterVersionView move, restart re-stamp                                                                            |
| High — fencing tokens                 | ✅ DONE (core)        | PR #245 `4f929593c` — cluster-epoch fence di parse_frame (exempt JOIN/PING/PONG) + main: KV RMW `020863783`, CRDT `2530e0c8f`/`3a37ae64b`, control `6023d2aa0`, lease fence token   |
| Med — scatter-gather guard            | ✅ DONE (main)        | `b037bab06` + DEFAULT_GATHER_TIMEOUT 30s                                                                                                                                            |
| Med — descriptor-lease expiry         | ✅ DONE (crash-wedge) | PR #246 `16d99164a` — drain filter + Leave hook + periodic sweep; Hlc expiry (main)                                                                                                 |
| Med — SWIM fast-restart rejoin        | ✅ DONE               | PR #243 `888684628` — persist incarnation (catalog), deterministic refutation echo, ping=liveness, suspicion-timer cancel                                                           |
| Med — pre-vote + leadership transfer  | ✅ DONE (main)        | pre_vote state.rs:131 + TimeoutNow `ae45049bf`                                                                                                                                      |
| Low — snapshot GC + decommission      | ✅ DONE               | main startup + periodic ~60s GC; decommission → ShutdownWatch::signal via consensus (0f625a6a2, 7056676cc)                                                                          |

**PRs:** #243 (SWIM), #244 (wire), #245 (epoch fence), #246 (lease GC), #247 (P2 raft core — closes #161, #162).
**Verification:** build workspace 0, raft 122/122, cluster all-features 1044, nodedb lib 6218, clippy -D warnings 0, maya-gate L1 clean, heavy-test loop hijau. Gabungan integrasi: `phase2-fixes`.

---

## ⏳ REMAINING — follow-ups (none blocking)

1. **Fencing hardening (optional)** — lease-grant fence vs catalog version; thread cluster_epoch ke snapshot-install + SWIM (raft core term checks kekal). Rekod: P2-FIX-PLAN Fix 3 §5.
2. **Descriptor-lease skew bound** — MAX_SKEW clamp + SWIM-Dead → on_node_crash release hook (LeaseManager consolidation). Refactor code: P2-REPORT.md §4.5.
3. **SWIM Left landmine** — `MemberState::Left` tiada production sender; bila graceful-leave wujud, restart mesti resume di atas TerminalLeft atau clean-shutdown restart akan stick kekal.
4. **Wire phase-2 (post-1.0)** — JoinResponse window echo via envelope-versioned structs (rkyv constraint), fail-fast retry hardening, two-binary CI test. Rekod: P2-FIX-PLAN Fix 2 §2.5/§3.3.
5. **Pre-existing rustdoc** — 32 broken intra-doc links dalam fail yang tak disentuh (applied_watcher, auth/bundle, calvin/sequencer, forward, mirror, raft_loop builder/hooks).
6. **Combined integration test** (Fix Plan 5) — rolling upgrade + node crash + epoch bump + lease GC dalam satu harness.
7. **Post-P2 perf/test roadmap** (P2-POST-ROADMAP.md) — PERF-1..6 (per-group lock, commit-index O(k log k), cached targets), TEST-1..3 (SimCluster harness, proptests state-machine/election/log-matching, Jepsen). Priority: TEST-1 → TEST-2 → PERF-2 → PERF-1 → TEST-3.
8. **Refactor post-merge** (code penuh dalam P2-REPORT.md §4) — IncarnationTracker, VersionWindow, EpochFence, LeaseManager, PersistentState, PERF-2 commit index.

---

## Rekod berkaitan

- `P2-REPORT.md` — report penuh + refactor code (§4.1-4.6) + urutan implementasi
- `P2-UNSOLVED-ISSUES.md` — drill asal 4 isu + RESOLUTION banner
- `P2-FIX-PLAN.md` — 4 fix plan + Fix Plan 5 (combined test) + IMPLEMENTED banner
- `P2-GLM53-REVIEW-RESOLUTION.md` — 13 review points + 4 improved solutions + RESOLVED banner
- `P2-POST-ROADMAP.md` — post-P2 performance & test architecture roadmap
- Epic #165 comments: status awal (issuecomment-5388799588) + DONE-vs-REMAINING (issuecomment-5389393433)
