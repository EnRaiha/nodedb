# P2 GLM 5.3 — Additional Consensus-Safety Gaps + Fix Implementation

Sumber: web chat chat.z.ai (GLM 5.3), prompt `P2-GLM53-PROMPT.md` + upload `P2-SOURCE-CODE.md`.
Tarikh: 2026-08-23. Repo rujukan: NodeDB-Lab/nodedb (GLM baca dari GitHub; verification tempatan di `../P2-VERIFICATION.md`).

---

## Bahagian 1 — Analisis Awal (sebelum baca source, kemudian DITARIK BALIK)

### 1. Leader No-Op Entry untuk Committing Previous-Term Entries

- **Kenapa:** Raft hanya benarkan komit entri dari term semasa melalui pengiraan majoriti. Pemimpin baru mesti append no-op entry dalam term sendiri sebelum boleh implisit commit entri dari term sebelumnya. Tanpa ini, entri yang nampak committed boleh di-overwrite oleh pemimpin baru — melanggar State Machine Safety.
- **Lokasi (audit awal):** `become_leader()` dalam `nodedb-raft/src/node/internal.rs`
- **Priority awal:** MUST-HAVE v0.6
- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `internal.rs:109-115`, `become_leader()`:
  ```rust
  // Raft paper §5.4.2: leader appends a no-op entry.
  let noop = LogEntry { term: self.hard_state.current_term, index: self.log.last_index() + 1, data: Vec::new() };
  let _ = self.log.append(noop);
  ```

### 2. Previous-Term Commit Restriction

- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `internal.rs:300-301`, `try_advance_commit_index()`:
  ```rust
  if term_at_n != self.hard_state.current_term { continue; }
  ```

### 3. ReadIndex Quorum Confirmation

- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `read_index.rs:25-61`, `start_read_index()` + `read_index_confirmed()` dengan pengiraan quorum betul.

### 4. Truncate-Before-Memory Ordering

- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `log.rs:137-151`, `truncate_from()` panggil `self.storage.truncate(index)` SEBELUM `self.entries.truncate(offset)` — kalau storage gagal, memori kekal konsisten.

### 5. Durability-Before-Memory dalam append_entries

- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `log.rs:94-117` — storage append sebelum in-memory mutation, dengan dokumentasi.

### 6. Compaction Gating

- **STATUS AKHIR GLM:** ✅ SUDAH ADA — `core.rs:254-263`, `compact_log_up_to()` menolak jika `up_to_index > self.durable_applied`.

---

## Bahagian 2 — Jurang SEBENAR (selepas baca source)

### GAP-1: Check-Quorum — Leader tidak step down bila kehilangan quorum — MUST-HAVE

- **Lokasi:** `core.rs:347-382` `tick()`, branch `NodeRole::Leader` — hanya ada heartbeat + transfer expiry check, TIADA check "masih boleh reach quorum?"
- **Kesan:** Pemimpin ter-partition kekal leader selama-lamanya — terima proposal (masuk log tapi tak replicate), buang CPU/memory, serve stale reads. Multi-group memburukkan: node boleh jadi leader group A (quorum OK) tapi deposed group B.
- **Fix cadangan GLM:** Track `last_quorum_contact: Instant`; update bila quorum respond (dalam `replicate_to_all()` / `handle_append_entries_response()`); dalam `tick()` jika `now - last_quorum_contact > election_timeout_max` → `become_follower(self.current_term())`.

### GAP-2: Membership Changes Tidak Melalui Raft Log — MUST-HAVE

- **Lokasi:** `membership.rs:22-49` `set_voters()` — terus mutate `self.config.peers`, TIDAK direplikasi, TIDAK crash-safe, tiada joint consensus / single-server protocol.
- **Kesan:** Leader crash selepas `add_peer()` → follower lain tak kenal node baru → split-brain membership.
- **Fix cadangan GLM:** ConfigChange sebagai LogEntry khas; apply bila COMMIT; joint consensus (C_old,new) atau single-server change dengan Ongaro guard.

### GAP-3: `promote_learner()` Tanpa Enforcement Catch-Up — MUST-HAVE

- **Lokasi:** `membership.rs:133-148` — doc kata "Called on the leader after observing the learner has caught up" tapi TIADA check dalam code.
- **Kesan:** 3-voter + 1 learner belum catch-up → promote → quorum jadi 3/4 tapi learner tak boleh ack → tiada commit → cluster stuck.
- **Fix cadangan GLM:** `if leader.match_index_for(peer) < self.volatile.commit_index { return false; }`

### GAP-4: Commit Index Boleh Regress Selepas Restore — SHOULD-HAVE

- **Lokasi:** `core.rs:119-126` `restore()` — `volatile.commit_index` = 0 selepas restart walaupun `durable_applied` tinggi.
- **Kesan:** Bukan data loss (durable_applied halang double-apply), tapi `collect_committed_entries()` boleh cuba baca range yang dah compacted → `LogCompacted` error; latency naik selepas restart.
- **Fix cadangan GLM:** `volatile.commit_index = self.durable_applied` sebagai starting point.

### GAP-5: Tiada group_id Validation dalam Membership Operations — NICE-TO-HAVE

- Defensive programming: caller silap panggil `add_peer()` pada group/node yang salah → silent inconsistent state.

### Ringkasan prioriti GLM

| Priority     | Item                          |
| ------------ | ----------------------------- |
| MUST-HAVE    | GAP-1 Check-Quorum            |
| MUST-HAVE    | GAP-2 Membership via Raft Log |
| MUST-HAVE    | GAP-3 Learner Promotion Guard |
| SHOULD-HAVE  | GAP-4 Commit Index Restore    |
| NICE-TO-HAVE | GAP-5 Group-ID Validation     |

---

## Bahagian 3 — Fix Implementation (code penuh dari GLM)

### Fix 1: Check-Quorum (MUST-HAVE) — `nodedb-raft/src/node/core.rs`

- Field baru `last_quorum_contact: Option<Instant>` dalam `RaftNode`
- Init `None` dalam `new()`; `tick()` branch Leader: jika gap >= `election_timeout_max` → warn + `become_follower(current_term)` + return; `None` (baru leader) → set ke now (tunggu first round-trip)
- `internal.rs` `try_advance_commit_index()`: track quorum contact — kira peers dengan `match_index >= last_index-1`, jika >= quorum → `last_quorum_contact = Some(now)`
- `internal.rs` `become_follower()`: reset `last_quorum_contact = None`

### Fix 2: Membership via Raft Log (MUST-HAVE)

- `message.rs`: enum `ConfigChange` (AddVoter/RemoveVoter/AddLearner/RemoveLearner/PromoteLearner/EnterJointConsensus/LeaveJointConsensus), enum `EntryPayload` (Command/Config), `LogEntry::as_config_change()` (discriminator 0x00/0x01 + serde_json), `encode_config()`
- `membership.rs`: `propose_config_change()` — guna `self.propose(data)`; guard Ongaro (`has_committed_in_current_term()`); validate PromoteLearner catch-up; `apply_config_change()` — satu-satunya tempat mutate peers/learners; joint consensus quorum = majority BOTH sets
- `config.rs`: field `joint_consensus: Option<(Vec<u64>, Vec<u64>)>`
- `error.rs`: `ConfigChangeBeforeCommit`, `LearnerNotCaughtUp`, `NotALearner`, `JointConsensusInProgress`
- `raft_loop/apply.rs`: `process_committed_entries()` — config entries route ke `apply_config_change()`, command ke state machine

### Fix 3: Commit Index Restore (SHOULD-HAVE) — `core.rs` `restore()`

```rust
self.volatile.commit_index = self.durable_applied;
```

### Urutan implementasi disyorkan GLM

1. Fix 4 (error types) — asas
2. Fix 3 (commit index restore) — paling mudah
3. Fix 1 (check-quorum) — critical safety
4. Fix 2 (membership via log) — paling besar, perlu migration path

---

## Nota

- Bahagian 1 = cadangan awal yang GLM TARIK BALIK selepas baca source (semua "sudah ada")
- Bahagian 2-3 = jurang sebenar + fix code
- Verification terhadap tree tempatan: lihat `P2-VERIFICATION.md`
