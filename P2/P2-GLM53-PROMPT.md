# P2 GLM 5.3 Prompt — What Else Should Phase 2 Include?

Salin teks di bawah ke chat.z.ai (GLM 5.3). Soalan: apa LAGI yang phase 2 (Cluster Consensus Safety) patut include yang audit + review sedia ada tak cover.

---

Saya maintain NodeDB (NodeDB-Lab) — distributed database dalam Rust, multi-group Raft. Phase 2 (v0.6) = Cluster Consensus Safety. Goal: restart/partition tak boleh hasilkan dua leader, committed entries yang divergen, atau silent state-machine divergence; linearizable reads betul-betul linearizable.

Audit semasa (dengan code evidence) ada item ni:

- #161: Raft HardState (current_term/voted_for) — audit kata tak pernah persist; code semasa dah ada persist-before-reply (consensus.rs handle_append_entries_rpc/handle_request_vote_rpc → persist_group_hard_state → save_hard_state), tapi belum dibuktikan semua mutation path (become_follower/candidate, vote grant) set dirty flag, dan takde test "vote A, crash, restart, mesti tak grant vote B dalam term sama", dan takde proof fsync dalam nodedb-wal backend
- #162: InstallSnapshot advance index tanpa data → silent divergence (receiver dah ada chunk/offset/CRC validation, tapi atomic index-advance-after-apply belum dibuktikan)
- Deposed/partitioned leader serves stale linearizable reads — follower_read.rs Strong = is_leader_fn je, takde ReadIndex heartbeat atau leader lease
- BoundedStaleness ukur time-since-last-apply, bukan lag-vs-leader (closed_timestamp.rs is_fresh_enough)
- Takde fencing tokens dalam raft/cluster
- Descriptor-lease expiry guna raw wall clock, takde skew bound
- SWIM fast-restart rejoin boleh stick (incarnation 0)
- Takde pre-vote; partition-heal term inflation paksa healthy leader step down
- Snapshot GC sweep_orphans startup-only
- Multi-group raft + scatter-gather distributed queries

Soalan: **Apa LAGI yang phase 2 patut include yang audit dan review sedia ada tak cover?** Fokus consensus-safety gaps: Raft invariants, durability ordering, leader lease/ReadIndex, snapshot safety, membership changes, clock/epoch, crash scenarios. Jangan ulang item yang dah ada. Bagi: title — kenapa penting untuk NodeDB-style multi-group raft — di mana ia mungkin hidup dalam codebase — priority (must-have/should-have/nice-to-have untuk v0.6).

---

## Dokumen berkaitan (dalam repo ini)

| File                                | Kandungan                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------- |
| `../P2-CLUSTER-CONSENSUS-REVIEW.md` | Review DeepSeek V4 Pro penuh — verdict per item audit P2 + fix/test design |
| `../P4-MULTI-TENANT-REVIEW.md`      | Review P4 (v0.8) — kait dengan resource governance, bukan P2               |
| `../P6-CONFORMANCE-REVIEW.md`       | Review P6 (v0.9) — A1 EdgeId critical, kait dengan graph semantics         |
| Epic #165                           | https://github.com/NodeDB-Lab/nodedb/issues/165 — senarai penuh item P2    |
| Epic #161                           | https://github.com/NodeDB-Lab/nodedb/issues/161 — HardState persist        |
| Epic #162                           | https://github.com/NodeDB-Lab/nodedb/issues/162 — InstallSnapshot index    |

## Rujukan code (lokasi dalam repo)

- `nodedb-cluster/src/raft_loop/handle_rpc/consensus.rs` — RPC handlers + persist-before-reply
- `nodedb-cluster/src/multi_raft/rpc_dispatch.rs:76-81` — persist_group_hard_state
- `nodedb-raft/src/node/core.rs:196-202` — persist_hard_state_if_dirty
- `nodedb-raft/src/node/internal.rs:368-370` — persist_hard_state (staging)
- `nodedb-raft/src/storage.rs:27-31` — save_hard_state trait
- `nodedb-cluster/src/install_snapshot/receiver.rs` — chunked snapshot receiver
- `nodedb-cluster/src/follower_read.rs:59-70` — can_serve_locally (Strong/BoundedStaleness/Eventual)
- `nodedb-cluster/src/closed_timestamp.rs:104-110` — is_fresh_enough
- `nodedb-cluster/src/swim/bootstrap.rs:115-140` — SWIM seed/incarnation
- `nodedb-cluster/src/install_snapshot/gc.rs` — sweep_orphans
- `nodedb-types/src/wire_version.rs` — wire format version (documented WONTFIX pre-1.0)
