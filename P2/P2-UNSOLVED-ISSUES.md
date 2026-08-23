# P2 — Unsolved Issues Drill

Main terkini: `origin/main` @ `54fe575c0` (23-08). Method: 5 parallel drill (read-only via `git show origin/main:<path>`). Tarikh: 2026-08-24.

Epic #165 checklist: 11 item → **7 solved oleh main + P2 branch, 4 masih perlu kerja** (2 open penuh, 2 enforcement gap). Fail ini = rekod penuh isu yang BELUM selesai, dengan evidence, fix design, effort, dan refactor suggestion.

---

## 1. SWIM Fast-Restart Rejoin Stick — OPEN (bug real)

**Verdict: OPEN** — bug boleh demo pada main; tiada fix berkesan (config knob = test plumbing sahaja).

### Mekanisme (kenapa boleh stick kekal)

1. Node A crash → peers tandakan `Dead(A, N)`. A restart → `initial_incarnation = Incarnation::ZERO` → announce `Alive(A, 0)` **SEKALI sahaja** (dissemination queue primed sekali; entry auto-drop selepas ~7 sends ≈ 7s window)
2. Satu-satunya heal path fragile + probabilistik: `Alive(0)` A mesti sampai ke peer yang simpan `Dead(A,N)` semasa dalam queue A → peer `Refute` (lexicographic `(0,0) < (N,2)`) → refutation mesti balik ke A via **direct ack** sahaja (indirect relay boleh drop — equal-tuple `Ignore`) → A `SelfRefute` → `Alive(N+1)` → heal
3. Window tertutup tanpa round-trip lengkap → **zero retry triggers** → divergence kekal

### Bukti (origin/main)

- `swim/bootstrap.rs:128` — `incarnation: Incarnation::ZERO` (production wiring `SwimConfig::default()` di `bootstrap/start.rs:73`)
- `swim/detector/runner.rs` — `ProbeOutcome::Acked{incarnation}` discarded; tiada periodic self-alive
- `swim/detector/probe_round.rs` / `handle_ping` — **ignore** `Ping.from`/`Ping.incarnation` (doc kata "Receiver uses this for merge logic"; code tidak)
- `swim/detector/suspicion.rs` — `SuspicionTimer::cancel` wujud tapi **never called in production** (unit test sahaja) → re-kill race: stale timer fire `Dead` pada incarnation refuted → re-kill
- `swim/incarnation.rs` — `Incarnation::bump()` ("on rejoin after a suspected restart") = **dead API**, 0 production call sites
- `swim/bootstrap.rs:114,132` — `cfg.initial_incarnation` plumbed tapi documented "Always 0 in production; exposed for deterministic unit tests" (`config.rs:43,66`)
- `ProbeScheduler` probe **Alive members only** → takde sesiapa hantar apa-apa kepada Dead A

### Fix design (~2.5–3 hari, TIADA wire change)

1. **Persist incarnation** (primer, ~1 hari): simpan incarnation dalam catalog/data-dir; bootstrap guna `persisted.bump()` sebagai `initial_incarnation` (`bootstrap/restart.rs` + `start.rs`); persist pada setiap `SelfRefute` bump + shutdown. Restart announce `Alive(N+1)` → dominate terus, tiada round-trip diperlukan
2. **Ping = liveness evidence** (~0.5 hari): `handle_ping` apply `Alive(from, ping.incarnation)` sebelum ack (safe — transport MAC-authenticated); juga apply `ack.incarnation` pada `ProbeOutcome::Acked`
3. **Cancel suspicion timer on Alive apply** (~0.5 hari): dalam runner selepas `apply_and_notify`, `suspicion.cancel(&node_id)` bila Alive applies — bunuh re-kill race
4. **Regression tests** (~0.5 hari): in-memory fast-restart test (kill → Dead(N) → respawn → converge Alive bounded rounds) + mid-suspicion-restart race test

### Refactor suggestion

- `MemberState::Left` tiada production sender (test sahaja) — bila graceful-leave diaktifkan kemudian, ia akan jadi landmine (clean-shutdown restart → TerminalLeft → stick kekal). Cadangan: sebelum/semasa menambah Left announcement, gabungkan dengan persist-incarnation supaya restart tak terperangkap TerminalLeft.

---

## 2. Wire Version Rolling Upgrade Block — OPEN

**Verdict: OPEN** — root cause confirmed: `nodedb-types/src/wire_version.rs:52` MIN == WIRE + satu-satunya gate exact-equality di `handle_join.rs:51`.

### Tiga sistem versi (epic campur; block hanya pada satu)

**A. Cluster schema version (`WIRE_FORMAT_VERSION`)** — epic target:

- `MIN_WIRE_FORMAT_VERSION` = **dead code**, 0 references luar fail sendiri
- Satu-satunya enforcement: `nodedb-cluster/src/bootstrap/handle_join.rs:51` — exact-equality `req.wire_version != CLUSTER_WIRE_FORMAT_VERSION` → reject
- Rejection TIDAK silent (warn! + structured fields + success:false) tapi message mengelirukan: "rolling upgrade is required before this node can join" — upgrade yang diminta mustahil (kontradiksi operator-hostile)
- `NodeInfo.wire_version` (topology.rs:118) stamped tapi never compared di mana-mana selain join check itu

**B. Transport/frame handshake (`nodedb-cluster/src/wire_version/`)** — SUDAH rolling-capable:

- QUIC handshake: `VersionHandshake {range, capabilities}` → `VersionHandshakeAck {agreed}`; `negotiate()` pilih highest common; disjoint → close 0x01 + reason
- Range diiklankan `[WireVersion(1), WIRE_VERSION=2]` — versi frame/envelope, berbeza dari cluster schema version
- `RPC_FRAME_VERSION = 3` exact-match tapi covered oleh handshake range — OK

**C. `ClusterSettings.min_wire_version`** (catalog/cluster_settings.rs:63) — knob ketiga, **unenforced**

### Apa yang pecah mid-upgrade

Bump `WIRE_FORMAT_VERSION` → node yang di-upgrade+restart ditolak join cluster lama (dan sebaliknya). Rolling upgrade = restart node satu-satu → setiap node tersekat luar topology → raft groups hilang voters → risiko quorum. Transport boleh bawa mixed traffic; join gate = satu-satunya hard partition.

### Fix design (5 langkah, ~1–2 hari foundational)

1. **Buka window pada masa bump** (bukan sekarang): kekal MIN == WIRE pre-1.0; pada bump pertama post-1.0 set `MIN_WIRE_FORMAT_VERSION = 1` sementara `WIRE_FORMAT_VERSION = 2`. Constants + compile-time asserts dah ada — trivial
2. **Range check di `handle_join.rs`**: ganti exact equality dengan `(MIN..=WIRE).contains(&req.wire_version)`; optional include server `[min,max]` dalam JoinResponse untuk diagnostics
3. **Bina `ClusterVersionView`** (modul yang doc rujuk tapi TAK WUJUD): scan live topology → min/max/mixed. Ini jadikan feature gates `wire_version >= V` hidup, dan hook keselamatan per-feature untuk mixed-version RPC dispatch
4. **Rejection path**: message nyatakan kedua-dua window bounds; structured error code; joiner log satu baris jelas "build vX outside cluster window [Y..Z]"
5. **Cleanup**: wire `ClusterSettings.min_wire_version` ke check sama ATAU delete; betulkan doc-comment stale (`control::rolling_upgrade::view::ClusterVersionView` — modul tak wujud)

### Effort

- Decouple constants + doc: 0.5–1 jam
- Join range check + unit tests: 1–2 jam
- ClusterVersionView + tests: 2–4 jam
- JoinResponse window echo + client validation: 1–2 jam
- Mixed-version integration test (2 builds): 2–4 jam
- Total: ~1–2 hari + per-feature 1–3 jam untuk setiap RPC baru yang perlu consult view

### Risiko

- Rendah sekarang, sederhana kemudian: relax check pre-1.0 kosong (tiada deployed cluster); risiko sebenar pada bump pertama — setiap RPC feature baru mesti gate pada cluster view, kalau tidak mixed cluster pecah senyap
- Raft membership/log encoding perlu care bila format v2-only diperkenal (hari ini log encoding versioned berasingan via `WireVersion::CURRENT`)

### Refactor suggestion

- Tiga sistem versi berasingan (cluster schema, frame handshake, ClusterSettings knob) patut disatukan ke satu sumber kebenaran. Cadangan: pindah `MIN_WIRE_FORMAT_VERSION`/`WIRE_FORMAT_VERSION` ke modul `nodedb-cluster/src/wire_version/` yang sama dengan frame negotiation (mereka memang berkaitan), dan jadikan `ClusterSettings.min_wire_version` sebagai dynamic override di atas constants — bukan sistem ketiga yang berasingan.

---

## 3. Fencing — cluster_epoch Enforcement Gap

**Verdict: PARTIAL** — infrastruktur landed, enforcement tak lengkap. Claim epic "no fencing tokens anywhere" kini SALAH untuk nodedb-cluster (cluster_epoch, DdlPrepared, SyncProducerFence wujud), tapi headline token (cluster_epoch) tiada enforcement path.

### Yang WUJUD (origin/main)

- `cluster_epoch.rs` — cluster generation/epoch fence token: leader-bumped pada metadata-group leadership acquisition (`raft_loop/tick/apply_committed.rs:89-119`), persisted via catalog (`catalog/core.rs:114-131`), stamped pada SETIAP outbound RPC frame header (`rpc_codec/header.rs:41-46`), observed inbound via `fetch_max` (`header.rs:112-114`)
- `metadata_group/entry.rs:80-95` — `DdlPrepared{token}` + Acquire/Release: replicated descriptor-preparation lease
- `metadata_group/entry.rs:186-198` — `SyncProducerFence{lite_id,new_epoch}`: replicated fencing epoch untuk Lite producers, max-wins/idempotent
- CRDT admission fence (2530e0c8f + 3a37ae64b), KV RMW fence (020863783), COMMIT-time buffered-write fence (a18c74b38, retryable 40001 SchemaChanged), event-trigger re-fence (5445f8bf9), sync epoch floor

### GAP

1. **cluster_epoch TIDAK di-enforce** — paling besar: token stamped + observed (`fetch_max`) tapi **tiada code reject** frame dari peer pada epoch lama. Doc `cluster_epoch.rs:12` claim "reject (or quarantine) frames from peers stuck on a strictly older epoch" — enforcement tu TIDAK WUJUD. Fence hiasan
2. **Descriptor lease grant** — documented gap: lease grant tak banding versi vs catalog (`version_check.rs` comments). Mitigated oleh re-fence dispatch/COMMIT, tapi lease sendiri unfenced
3. **nodedb-raft core** — 0 fence hits; elections/snapshot guna standard Raft term checks (protocol-correct, tak perlu fence explicit)
4. **install_snapshot** — CRC quarantine hook sahaja; tiada cluster-epoch check
5. **SWIM** — incarnation-based (SWIM-standard), tak wired ke cluster_epoch
6. **Calvin sequencer** — batch epoch sendiri (sequencer/validator.rs:319), tak fenced terhadap descriptor/catalog; safety bergantung admission fences baru

### Fix design

- **Enforce cluster_epoch pada inbound frames** (S, 1–2 hari): dalam decode path, reject (typed error) frames `peer_epoch < local_epoch`, dengan bootstrap/join exemption
- **Fence descriptor-lease grant terhadap catalog version** (M, 2–3 hari): reject grants untuk versi superseded, bukan bergantung downstream re-fences
- Optional hardening: thread cluster_epoch ke snapshot-install + SWIM (M-L, 2–5 hari); raft core term checks defensible as-is

### Refactor suggestion

- Enforcement patut duduk dalam `rpc_codec/header.rs` decode path (tempat `fetch_max` dah observe) — satu fungsi `validate_peer_epoch(peer_epoch) -> Result<(), ClusterError::StalePeer>` yang dipanggil sebelum dispatch. Ini elak scattering checks di setiap handler.

---

## 4. Descriptor-Lease — Crash-Wedge + Skew Bound

**Verdict: PARTIAL** — correctness asal epic SOLVED (same-node expiry + explicit-clear drain + 2 pinned regression tests), tapi 3 gap terbuka: crash-wedge (availability), tiada skew bound, tiada fencing token.

### Yang SOLVED (main)

- `DescriptorLease { descriptor_id, version, node_id, expires_at: Hlc }` — replicated via raft Grant/Release; semua node simpan SEMUA lease dalam `MetadataCache.leases`
- Semua expiry check same-node sahaja: `acquire_lease` vs `hlc_clock.now()`; renewal `collect_near_expiry` filter `node_id == self.node_id`
- Drain clear HANYA via `DescriptorDrainEnd` eksplisit — `is_draining` tak pernah baca `expires_at` (pin: `is_draining_stays_active_past_local_wall_clock_expiry` "Pins the fix for the cross-node clock-skew bug")
- Drain tunggu release eksplisit (refcount → 0 → raft release), timeout 35s → DDL FAIL (fail-closed)

### GAP

1. **CRASH-WEDGE (paling kritikal)**: tiada GC lease, tiada `leases.retain/clear` kecuali explicit release, tiada hook topology-change untuk purge lease node hilang. Node crash sambil pegang lease → setiap ALTER pada descriptor itu drain-wait 35s → timeout → fail, rekod kekal → setiap retry fail sama. Hanya operator/restart node boleh pulih. Availability gap, bukan correctness (fail-closed)
2. Lease expired berkumpul dalam replicated map selamanya (memori + view stale)
3. Tiada skew bound / NTP sanity check / confidence interval — benign hari ini (tiada cross-node consumer), fragile terhadap perubahan masa depan. Edge: `last_applied_hlc` watermark naik ke `expires_at` (masa depan) setiap grant apply — setakat ini hanya route debug HTTP; kalau wired ke fencing kemudian → skew amplifier
4. Renewal guna wall clock mentah vs stamp guna HLC — regresi selamat arah (panjangkan lifetime sahaja)

### Analisis skew (HLC derived dari physical clock)

- Node A laju +2 min: stamp expires_at 2 min depan relatif node lain; tiada cross-node reader → tiada kesan correctness; rekod hidup 2 min lama → drain tunggu lama sikit (dah dibound 35s)
- Node A lambat −2 min: A anggap lease valid 2 min lepas real expiry; MASIH fail-safe — bump versi tak boleh commit selagi rekod lease A wujud (release yang buang rekod, bukan expiry)

### Fix design

1. **Crashed-node lease GC** (S, 1–2 hari): pada topology change/member removal (+ sweep perlahan di leader), propose `DescriptorLeaseRelease` untuk lease dengan `node_id` bukan ahli; `poll_leases_drained` abaikan lease dari node bukan ahli. Tukar wedge → bounded 35s-then-success
2. **Skew clamp** (S, ~1 hari): clamp `expires_at` pada stamp (`peek + duration + skew_bound` configurable) dan/atau log/refuse bila |NTP offset| > bound
3. **Fencing token sebenar** (M, 3–5 hari, defer): lease epoch per descriptor-version bump. Benefit rendah — design sekarang fail-closed; hanya perlu kalau nak DDL proceed semasa partition (tradeoff availability)

### Refactor suggestion

- Lease lifecycle bertabur merentasi 8+ fail (acquire/renew/propose/drain/drain_propose/refcount/release/shutdown_release/methods_lease/cache). Cadangan: konsolidasi ke satu `LeaseManager` yang memiliki: stamp (dengan skew clamp), GC hook (topology-change + periodic sweep), dan release batching — hari ini setiap path implement separa sendiri.

---

## Solved (rekod — tak perlu kerja)

**ShutdownWatch decommission — SOLVED (main):**

- DecommissionCoordinator propose plan via metadata raft group (StartDecommission → LeadershipTransfer×N → RemoveMember×N → FinishDecommission → Leave); metadata group di-host setiap node → semua apply → cascade ke ClusterTopology + RoutingTable (consensus-based broadcast, bukan push)
- DecommissionObserver (poll 5s, scoped local_node_id) → RunningCluster::decommission_signal → spawn_decommission_shutdown_bridge → ShutdownWatch::signal → graceful drain (0f625a6a2)
- Peer-side: reconcile_placement (metadata leader, ~1s) — active_nodes() sahaja → target placement tanpa node decommission → SetPlacement
- sweep_orphans: startup + periodic ~60s tick (gc.rs)

---

## Ringkasan Contribution Priority

| #   | Item                        | Effort     | Nilai                                                   |
| --- | --------------------------- | ---------- | ------------------------------------------------------- |
| 1   | SWIM rejoin fix (4 langkah) | 2.5–3 hari | Bug real, tiada wire change, semua lokal/receiver-side  |
| 2   | cluster_epoch enforcement   | 1–2 hari   | Fence hiasan → berfungsi; satu fungsi dalam decode path |
| 3   | Wire version join range     | 1–2 hari   | Foundational untuk rolling upgrade v0.6+                |
| 4   | Lease crash-wedge GC        | 1–2 hari   | Pulih availability DDL selepas node crash               |

Semua = fix isu sedia ada, bukan feature baru. Setiap submission sertakan refactor suggestion (rule 2026-08-24).
