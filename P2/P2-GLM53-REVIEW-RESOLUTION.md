# P2 — GLM 5.3 Review Resolution (2026-08-24)

> ## ✅ RESOLVED (24-08-2026) — SEMUA 4 FIX TELAH DIIMPLEMENT
>
> Fix 1 `888684628` (PR #243), Fix 2 `e60a853ae` (PR #244), Fix 3 `4f929593c`
> (PR #245), Fix 4 `16d99164a` (PR #246). 4 improved solutions (IncarnationTracker,
> VersionWindow, EpochFence, LeaseManager) + PersistentState kekal sebagai
> refactor post-merge — code penuh dalam `P2-REPORT.md` §4.

Review GLM 5.3 terhadap `P2-UNSOLVED-ISSUES.md` + `P2-FIX-PLAN.md`. Semua point diselesaikan di sini. Setiap point: verdict (accept/reject) + tindakan.

## 0. Document Completeness Correction

**Claim GLM:** "Fix Plan 3 truncated, Fix Plan 4 missing entirely."

**REJECTED sebagai isu dokumen** — `P2-FIX-PLAN.md` lengkap 1,737 baris:

- Fix Plan 3 (cluster_epoch): line 724–1047 (termasuk verification, 9 unit tests, risk notes)
- Fix Plan 4 (lease GC): line 1048–1737 (690 baris, 6 section, unit + integrasi + manual smoke)

GLM menerima paste separa sahaja. Untuk review penuh: hantar fail lengkap, atau guna index ini sebagai panduan.

## Review Points + Resolutions

| #   | Point GLM                                                                                           | Verdict               | Tindakan                                                                                                                                     |
| --- | --------------------------------------------------------------------------------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| R1  | F4 (persist incarnation) = primary fix; F1/F2 = defense-in-depth; F3 = bunuh re-kill race           | ACCEPT                | Plan 1 §1 update: nyatakan hierarki ini secara eksplisit                                                                                     |
| R2  | `apply_and_notify` jadi async — risiko deadlock bila dipanggil dari sync context                    | ACCEPT                | Tambah doc comment keras + panik guard (`tokio::runtime::Handle::try_current().expect(...)`) dalam plan                                      |
| R3  | `IncarnationSink` sync dalam async task — redb write boleh stall probe tick                         | ACCEPT (nice-to-have) | Plan 1 §F4: balut save dalam `tokio::spawn` fire-and-forget; persist hanya pada SelfRefute bump (rare)                                       |
| R4  | Missing test: suspicion timer fire ANTARA refutation sampai dan apply_and_notify jalan              | ACCEPT                | Tambah test `suspicion_timer_fires_during_refutation_window` dalam plan                                                                      |
| R5  | Wire: restart path `NodeInfo.wire_version` re-stamp — GLM kata MUST-FIX sebelum 1.0, bukan optional | ACCEPT (dinaik taraf) | Plan 2 §4.8: status tukar dari "cosmetic pre-1.0" → "MUST-FIX sebelum 1.0" (cluster view laporkan min_version salah → feature gate tak flip) |
| R6  | Wire: test lain mungkin assume exact equality — `git grep` tak cukup                                | ACCEPT                | Verification plan 2 tambah: `cargo test --workspace 2>&1 \| grep -i wire`                                                                    |
| R7  | Sequencing: Fix 3 JANGAN land sebelum Fix 2 (epoch strict enforcement + mixed-version cluster)      | ACCEPT                | Order tukar: **Fix 1 → Fix 2 → Fix 3 → Fix 4**                                                                                               |
| R8  | Fence rejection: sender dapat StalePeerEpoch → apa? Tanpa recovery path, partition kekal            | ACCEPT                | Guna GLM Improvement 3: recovery path auto-rejoin (join exempt → JoinResponse bawa epoch → observe → fence lift)                             |
| R9  | `RPC_PING_REQ` (indirect ping relay) status exemption                                               | ACCEPT                | Explicit: indirect ping relay EXEMPT — liveness channel mesti jalan untuk discovery                                                          |
| R10 | Fix 1: self-advertise setiap ping → queue flooding                                                  | ACCEPT                | Guna GLM Improvement 1: `should_advertise()` rate-limit 500ms                                                                                |
| R11 | Combined integration test semua 4 fix                                                               | ACCEPT                | Tambah seksyen "Combined Integration Test" dalam FIX-PLAN (rolling upgrade + crash + epoch bump + lease GC)                                  |
| R12 | Fix 2: `handle_join_request` signature change — check public API re-export                          | ACCEPT                | Plan 2 tambah langkah: `git grep handle_join_request` semua callers + semak re-export dalam lib.rs                                           |
| R13 | Fence: exempt `RPC_INSTALL_SNAPSHOT` (snapshot install self-healing — follower reset)               | ACCEPT                | Tambah ke exemption list (GLM Improvement 3: `SnapshotTransfer`)                                                                             |

## GLM Improved Solutions — Status

GLM sediakan 4 refactored implementations + 1 cross-cutting trait. Disimpan sebagai REFERENCE DIRECTION (bukan final code — beberapa rujuk API yang belum wujud dan perlu adaptasi):

| Improvement                                             | Nilai                                                                                                             | Status                                                                                     |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| 1. `IncarnationTracker` (incarnation.rs rewrite)        | Consolidate state scattered (detector/sink/config) + rate-limit + persist atomic + MemIncarnationStore untuk test | ADOPT sebagai direction untuk Fix 1 — ganti F1-F4 scattered changes dengan tracker tunggal |
| 2. `VersionWindow` type (wire_version.rs rewrite)       | Type-safe window + union/with_floor/contains + ClusterVersionView integrate ke topology                           | ADOPT — lebih bersih dari bare function                                                    |
| 3. `EpochFence` middleware (cluster_epoch.rs)           | Recovery path auto-rejoin + FenceStats + typed exemptions (enum) + snapshot exempt                                | ADOPT — jawab R8/R13                                                                       |
| 4. `LeaseManager` (metadata_group/lease_manager.rs new) | Centralize GC: topology-change + sweep + SWIM Dead hook + skew clamp + drainable filter                           | ADOPT sebagai direction — konsolidasi 8+ fail; integrate SWIM Dead → on_node_crash         |
| Cross-cutting. `PersistentState` trait                  | Satu pattern untuk 4 jenis persisted state (incarnation/epoch/lease/operator floor)                               | DEFER — refactor minggu 3; bagus tapi bukan blocker                                        |

**Caveat adaptasi (penting untuk implementer):**

- `CatalogIncarnationStore` rujuk `catalog.save_swim_incarnation` — fungsi TAK wujud; perlu ikut corak `save_cluster_epoch` sedia ada (u64 LE dalam METADATA_TABLE)
- `LeaseManager` rujuk `crate::hlc::HlcClock` dan `propose_lease_grant/release` — perlu map ke API sebenar (hlc_clock dalam nodedb-types; propose path host-side dalam `nodedb/src/control/lease/`)
- `PersistentState::KEY` pattern rujuk `catalog.load_metadata/save_metadata` — perlu define format bytes
- Semua code GLM = pseudo-directional; line numbers + signatures SEBENAR dalam P2-FIX-PLAN.md yang menang bila konflik

## Revised Sequencing + Effort

```
Fix 1 (SWIM)      → 2.5–3 hari (IncarnationTracker direction)
Fix 2 (Wire)      → 1.5 hari core + 1 hari optional (VersionWindow + restart re-stamp MUST-FIX)
Fix 3 (Epoch)     → 1.5–2 hari (EpochFence + recovery path) — SELEPAS Fix 2
Fix 4 (Lease GC)  → 3.5 hari fix + 2-3 hari refactor berasingan (LeaseManager direction)
Combined integ test → +1 hari
Total: ~10–11.5 hari
```

Priority P2 vs defer (GLM cadangan diterima):

- P2: Fix 1 + Fix 2 + Fix 3 (safety-critical, sequencing 1→2→3)
- P2.5: Fix 4 (availability, bukan correctness)
- P3: PersistentState refactor + LeaseManager konsolidasi penuh
