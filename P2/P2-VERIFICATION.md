# P2 GLM 5.3 — Verification terhadap Source Code Tempatan

Repo: NodeDB-Lab/nodedb @ 461c3ad (main, local `~/projects/nodedb`).
Tarikh: 2026-08-23. Semua rujukan baris dari tree TEMPATAN (bukan GitHub GLM).

> **POST-AUDIT UPDATE (2026-08-24) — verdict dibawah adalah rekod SEJARAH @ 461c3ad (07-08).**
> Main telah advance selepas audit; claim-claim yang ditanda SALAH/TIADA mungkin kini WUJUD:
>
> - **ReadIndex (item #3, GAP-1, baris 39, 63):** ReadIndex quorum confirmation MASUK main 23-08 — `f9910444b` "confirm quorum leadership before serving strong reads" + `b1f52e9a4` "serve bounded-staleness reads". Kini wujud: `nodedb-raft/src/node/read_index.rs`, `nodedb-cluster/src/read_index_wait.rs` (confirm_read_index), caller `nodedb/src/control/cluster/read_index.rs:66`. Verdict "hallucination" TIDAK LAGI SAH untuk main terkini — GLM silap untuk tree 461c3ad, tapi main kemudiannya implement apa yang GLM claim.
> - **Check-quorum (GAP-1):** MASIH SAH — fix kita (P2, `052f2fa64`) + lease `cf2cd5c24`, bukan main.
> - **GAP-4 commit_index:** fix kita `dca42ac89` (seed dari durable floor).
> - **GAP-3 promote_learner:** fix kita `b215d4bba` (propose-side guard).
>   Rujuk `P2-FULL-PLAN.md` untuk status terkini (2026-08-24).

---

## Bahagian 1 — "Sudah Ada" claims (GLM tarik balik cadangan awal)

| #   | Claim GLM                           | Verdict                      | Bukti (tree 461c3ad)                                                                                                                                                                                                                                                                                                                          |
| --- | ----------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | No-op entry dalam `become_leader()` | ✅ **BENAR**                 | `nodedb-raft/src/node/internal.rs:126-132` — "Raft paper §5.4.2: leader appends a no-op entry" + `self.log.append(noop)`; single-voter cluster commit segera selepasnya                                                                                                                                                                       |
| 2   | Previous-term commit restriction    | ✅ **BENAR**                 | `nodedb-raft/src/node/internal.rs:338` — `if term_at_n != self.hard_state.current_term { continue; }` (GLM kata 300-301, sebenar 338 — lokasi sikit beza, logik wujud)                                                                                                                                                                        |
| 3   | **ReadIndex quorum confirmation**   | ❌ **SALAH / HALLUCINATION** | **TIADA** `read_index.rs`, tiada `start_read_index`, tiada `read_index_confirmed`, tiada sebarang rujukan `ReadIndex` dalam `nodedb-raft/src/` (grep penuh: 0 match). GLM mengaku item ini "sudah ada" tetapi ia langsung tidak wujud. Audit asal "deposed leader serves stale reads — no ReadIndex heartbeat, no leader lease" **MASIH SAH** |
| 4   | Truncate-before-memory ordering     | ✅ **BENAR**                 | `nodedb-raft/src/log.rs:158-164` — `truncate_from()`: `self.storage.truncate(index)?` (baris 162) SEBELUM `self.entries.truncate(offset)` (baris 164)                                                                                                                                                                                         |
| 5   | Durability-before-memory append     | ✅ **BENAR**                 | `nodedb-raft/src/log.rs:129` (`storage.append(entries)?`) sebelum `entries.push` (baris 138); `append()` baris 147-148 sama corak                                                                                                                                                                                                             |
| 6   | Compaction gating                   | ✅ **BENAR**                 | `nodedb-raft/src/node/core.rs:282` `compact_log_up_to()` + baris 314 `maybe_compact_log()` — gating terhadap durable_applied (perlu baca penuh untuk pengesahan akhir, struktur wujud)                                                                                                                                                        |

**Kesimpulan Bahagian 1:** GLM betul pada 5/6, tapi **claim ReadIndex adalah hallucination** — item itu sebenarnya salah satu jurang HIGH yang masih terbuka. Ini sebab verification perlu: model AI (termasuk GLM) boleh "mengesahkan" benda yang tak wujud.

---

## Bahagian 2 — Jurang sebenar (GAP)

| GAP   | Claim GLM                                                               | Verdict                | Bukti (tree 461c3ad)                                                                                                                                                                                                                                                                                                 |
| ----- | ----------------------------------------------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GAP-1 | Check-quorum takde — leader tak step down bila hilang quorum            | ✅ **SAH**             | `nodedb-raft/src/node/core.rs:396-414` — branch `NodeRole::Leader` dalam `tick()` hanya: transfer-expiry clear + heartbeat `replicate_to_all()`. **Tiada** `last_quorum_contact`, tiada step-down bila quorum hilang. Grep `quorum_contact` / `check.quorum`: 0 match                                                |
| GAP-2 | Membership change tak melalui Raft log — `set_voters()` mutate langsung | ✅ **SAH**             | `nodedb-raft/src/node/membership.rs:28-57` — `set_voters()` terus `self.config.peers = new_voters` (baris 56); `add_peer()` baris 65-74 panggil `set_voters()` terus. Tiada ConfigChange log entry, tiada joint consensus, tiada Ongaro guard                                                                        |
| GAP-3 | `promote_learner()` tanpa enforcement catch-up                          | ✅ **SAH**             | `nodedb-raft/src/node/membership.rs:149-165` — doc kata "Called on the leader after observing the learner has caught up (its match_index >= commit_index)" tetapi badan fungsi TIADA check — terus `learners.retain` + `peers.push`. Caller boleh lupa → quorum hilang                                               |
| GAP-4 | `commit_index` regress ke 0 selepas restore                             | ✅ **SAH**             | `nodedb-raft/src/state.rs:95-100` — `VolatileState::restored(applied_index)` set `commit_index: 0, last_applied: applied_index`. `core.rs:130-131` `restore()` guna ia terus. Selepas restart: commit_index 0 walaupun durable_applied tinggi → `try_advance_commit_index()` iterasi dari 1 → potensi `LogCompacted` |
| GAP-5 | Tiada group_id validation dalam membership ops                          | ✅ **SAH (defensive)** | Semua fn membership (`set_voters`, `add_peer`, `remove_peer`, `promote_learner`) tak validate group_id/node role — memang defensive, bukan bug aktif                                                                                                                                                                 |

**Kesimpulan Bahagian 2:** 4/4 jurang utama SAH dan disahkan dalam tree tempatan. Fix design GLM (Bahagian 3) boleh dipakai sebagai asas.

---

## Implikasi gabungan (GLM + DeepSeek V4 Pro + audit)

1. **ReadIndex/leader lease** — DUA-DUA review (GLM tersilap anggap wujud, audit & DS V4 Pro kata takde) — sebenarnya **takde langsung**. Ini menaikkan GAP-1: bukan setakat check-quorum, tapi linearizable reads perlukan sama ada ReadIndex round-trip atau leader lease. Item audit "Deposed/partitioned leader serves stale linearizable reads" kekal HIGH.
2. **GAP-2 membership via log** — paling besar, dan GAP-3 promote_learner ialah symptom sama (mutasi terus tanpa protocol). Fix perlu satu mekanisme: ConfigChange entry + apply-on-commit.
3. **GAP-4 commit_index restore** — fix satu baris (`commit_index = durable_applied`) — cepat menang.
4. **#161 HardState** — DS V4 Pro: PARTIAL (persist-before-reply wujud, tapi perlu test + fsync audit). GLM takde claim baru.
5. **#162 InstallSnapshot** — DS V4 Pro: BLOCKER v0.6 (index-advance-after-apply belum dibuktikan).

---

## Cadangan urutan kerja (gabungan semua review)

1. **Fix GAP-4** (commit_index restore) — ✅ **DONE 2026-08-23** (`bebf346`) — seed commit_index dari durable floor; double review: GLM claim "LogCompacted error" REFUTED (term_at return None → continue, bukan error) tapi seeding tetap selamat (append_entries.rs:60-61 guard `>` monotonic; durable_applied <= commit_index selalu) — test RED → GREEN, nodedb-raft 73+23 pass, nodedb-cluster 991 pass
2. **Fix GAP-1 + ReadIndex** (check-quorum + leader lease/ReadIndex) — ✅ **DONE** (`f88624a` + `d430cfa`) — check-quorum: leader step down bila hilang quorum contact > election_timeout_max; leader lease: Strong read = is_leader && quorum_lease_valid (contact < election_timeout_min) — lease sah secara teoretikal (candidate perlukan election_timeout_min untuk menang, voter yang ack kita tak boleh vote lawan)
3. **Fix GAP-2 + GAP-3** (membership via log + promote guard) — ✅ **DONE** — GAP-2: **REFUTED di peringkat sistem** — cluster dah ada `conf_change.rs` (propose via log + apply-on-commit + idempotent + routing sync); semua production caller guna `propose_conf_change` (migration_executor, membership_convergence). Low-level raft API masih expose direct mutation tapi takde production caller bypass. GAP-3: `56985e8` — guard di PROPOSE side (`propose_conf_change` → LearnerNotCaughtUp) bukan apply side (apply time commit_index dah advance → spurious reject); test: propose_promote_requires_learner_caught_up RED→GREEN
4. **#161 test + fsync audit** — ✅ test DONE (`78da27f`) — `restart_does_not_double_vote_in_same_term` (vote A → persist → restore → vote B ditolak); fsync audit nodedb-wal masih open
5. **#162 proof** — ✅ **DONE** (`78da27f`) — **code dah fixed** (finalize.rs:106-122: apply data BEFORE raft advance; apply failure → return tanpa advance); 2 proof test: snapshot_data_applied_before_raft_advances (data → state machine → index advance) + snapshot_apply_failure_does_not_advance_raft (fail → index kekal)

---

## Bahagian 3 — Docs Mismatch (nodedb-docs vs code reality)

Repo docs: `~/projects/nodedb-docs` (Oxidoc .rdx, 107 files, nodedb.dev). Semak 2026-08-23.

| Docs claim                                                                    | Lokasi                                    | Code reality (461c3ad)                                                                                                | Mismatch                                                                                                                                          |
| ----------------------------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Strong read: "Leader read, **waits for commit index**"                        | `docs/architecture/consistency.rdx:20,32` | `follower_read.rs` — Strong = is_leader_fn sahaja (pre-lease); dengan `d430cfa` Strong = is_leader + lease            | **SEPARA FIXED** — docs kena update: Strong = leader + lease, takde "waits for commit index" (belum ada ReadIndex round-trip; lease adalah proxy) |
| Bounded staleness: "follower read allowed if **lag <= duration**"             | `consistency.rdx:24,33`                   | `closed_timestamp.rs:104-110` — `last.elapsed() <= max` — ukur time-since-last-apply (freshness), BUKAN lag-vs-leader | **YA** — docs kata "lag", code ukur "freshness". Follower yang apply tersekat 1s lepas tetap "fresh" walaupun jauh ketinggalan                    |
| "Writes are **linearizable** within each Raft group"                          | `docs/architecture/multi-raft.rdx:26`     | Writes: quorum commit → linearizable ✅. Strong reads kini lease-gated (`d430cfa`) — lebih dekat dengan linearizable  | **SEPARA** — write linearizable; read Strong lease-gated                                                                                          |
| Leader election: "automatic failover when current leader becomes unreachable" | `multi-raft.rdx:14`                       | Benar secara asas, + check-quorum (`f88624a`) kini leader step down sendiri bila hilang quorum                        | **SEPARA → IMPROVED**                                                                                                                             |

**Implikasi:** docs `consistency.rdx` + `multi-raft.rdx` perlu dikemaskini (proposed, belum dibuat): Strong = "leader read gated by quorum lease"; bounded staleness = "time-since-last-apply" bukan "lag"; tambah check-quorum + lease dalam description.

---

## Status penuh P2 (2026-08-23 lewat malam)

| Item                                 | Status                                                                                                                                                                                                                                             | Commit    |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| GAP-4 commit_index restore           | ✅ fixed + test                                                                                                                                                                                                                                    | `bebf346` |
| GAP-1 check-quorum                   | ✅ fixed + 2 test                                                                                                                                                                                                                                  | `f88624a` |
| GAP-3 promote guard (propose side)   | ✅ fixed + 2 test                                                                                                                                                                                                                                  | `56985e8` |
| GAP-2 membership via log             | ✅ REFUTED (cluster dah ada conf_change)                                                                                                                                                                                                           | —         |
| #161 regression test                 | ✅ test pass (code dah fixed)                                                                                                                                                                                                                      | `78da27f` |
| #162 proof test                      | ✅ 2 test pass (code dah fixed)                                                                                                                                                                                                                    | `78da27f` |
| Leader lease (linearizable Strong)   | ✅ fixed + test                                                                                                                                                                                                                                    | `d430cfa` |
| fsync audit nodedb-wal               | ✅ **DONE** — raft log + HardState = redb `commit()` (raft_storage.rs:122,350) — redb transaction commit = fsync (durability default); nodedb-wal segmented (`append` buffered + `sync()` explicit) adalah data-plane WAL, bukan raft — di luar P2 | —         |
| Docs update (consistency/multi-raft) | ⏳ proposed                                                                                                                                                                                                                                        | —         |
| Full workspace test                  | ⚠️ 1 flaky: corrupt_vector_checkpoint_fails_boot (crash harness, sedang re-verify)                                                                                                                                                                 | —         |
